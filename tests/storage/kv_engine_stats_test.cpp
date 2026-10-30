// The write-amplification counters and the block cache figures in KVEngine::Stats.
#include <gtest/gtest.h>

#include <filesystem>
#include <memory>
#include <string>

#include "engine_test_util.h"
#include "storage/checkpoint.h"
#include "storage/kv_engine.h"

using namespace raftkv;
using raftkv::testutil::TempDir;
using raftkv::testutil::WaitFor;
namespace fs = std::filesystem;

namespace {

// A WAL record is crc(4) + length(4) + type(1) + key length(4) + key + value (wal.cpp).
uint64_t WalRecord(size_t key, size_t value) { return 13 + key + value; }

const std::string kIndexKey = std::string(kReservedKeyPrefix, sizeof(kReservedKeyPrefix) - 1) + "applied_index";

uint64_t SstBytesOnDisk(const std::string& dir) {
  uint64_t total = 0;
  for (const auto& de : fs::directory_iterator(dir)) {
    if (de.path().extension() == ".sst") total += fs::file_size(de.path());
  }
  return total;
}

KVEngine::Options Opts(const std::string& dir, size_t compaction_trigger = 0) {
  KVEngine::Options o;
  o.dir = dir;
  o.sync_wal = false;
  o.use_sstables = true;
  o.memtable_bytes = 2048;
  o.compaction_trigger = compaction_trigger;
  o.flush_retry_ms = 5;
  return o;
}

TEST(EngineStats, UserAndWalBytesCountEveryAcknowledgedWriteExactly) {
  TempDir dir;
  std::unique_ptr<KVEngine> e;
  ASSERT_EQ(KVEngine::Open(Opts(dir.path()), &e), Status::kOk);
  KVEngine::Stats s = e->GetStats();
  EXPECT_EQ(s.user_bytes_written, 0u);
  EXPECT_EQ(s.wal_bytes_written, 0u);

  ASSERT_EQ(e->Put("ab", "cde"), Status::kOk);
  ASSERT_EQ(e->Put("k", ""), Status::kOk);  // an empty value is still a write
  ASSERT_EQ(e->Delete("ab"), Status::kOk);  // a delete's logical payload is its key
  s = e->GetStats();
  EXPECT_EQ(s.user_bytes_written, 5u + 1u + 2u);
  EXPECT_EQ(s.wal_bytes_written, WalRecord(2, 3) + WalRecord(1, 0) + WalRecord(2, 0));

  // Refused writes change nothing.
  EXPECT_EQ(e->Put(kIndexKey, "x"), Status::kInvalidArgument);
  EXPECT_EQ(e->PutAt("a", "b", 0), Status::kInvalidArgument);
  const KVEngine::Stats after_refusals = e->GetStats();
  EXPECT_EQ(after_refusals.user_bytes_written, s.user_bytes_written);
  EXPECT_EQ(after_refusals.wal_bytes_written, s.wal_bytes_written);

  // PutAt: the user bytes are the pair only; the WAL also holds the applied-index record.
  ASSERT_EQ(e->PutAt("xy", "z", 1), Status::kOk);
  const KVEngine::Stats with_index = e->GetStats();
  EXPECT_EQ(with_index.user_bytes_written, s.user_bytes_written + 3u);
  EXPECT_EQ(with_index.wal_bytes_written, s.wal_bytes_written + WalRecord(2, 1) + WalRecord(kIndexKey.size(), 8));
}

TEST(EngineStats, WalBytesKeepCountingAcrossMemtableRotations) {
  TempDir dir;
  std::unique_ptr<KVEngine> e;
  ASSERT_EQ(KVEngine::Open(Opts(dir.path()), &e), Status::kOk);
  uint64_t expected = 0;
  for (int i = 0; i < 300; ++i) {
    const std::string k = "key" + std::to_string(1000 + i);
    ASSERT_EQ(e->Put(k, std::string(60, 'v')), Status::kOk);
    expected += WalRecord(k.size(), 60);
  }
  ASSERT_GT(e->GetStats().active_seq, 1u) << "the memtable rotated (new WAL files)";
  EXPECT_EQ(e->GetStats().wal_bytes_written, expected);
}

TEST(EngineStats, FlushBytesEqualTheSSTablesFlushedAndManifestBytesAreCounted) {
  TempDir dir;
  std::unique_ptr<KVEngine> e;
  ASSERT_EQ(KVEngine::Open(Opts(dir.path(), /*compaction_trigger=*/0), &e), Status::kOk);
  EXPECT_EQ(e->GetStats().flush_bytes_written, 0u);
  for (int i = 0; i < 300; ++i) {
    ASSERT_EQ(e->Put("key" + std::to_string(1000 + i), std::string(60, 'v')), Status::kOk);
  }
  ASSERT_TRUE(WaitFor([&] { return e->GetStats().frozen_memtables == 0; }));
  const KVEngine::Stats s = e->GetStats();
  ASSERT_GT(s.flushes_done, 0u);
  EXPECT_EQ(s.compaction_bytes_written, 0u);
  EXPECT_EQ(s.flush_bytes_written, SstBytesOnDisk(dir.path())) << "no compaction: every table on disk was flushed";
  EXPECT_GE(s.manifest_bytes_written, fs::file_size(dir.path() + "/MANIFEST"));
  EXPECT_GT(s.manifest_bytes_written, 0u);
  EXPECT_LT(s.flush_bytes_written, 3 * s.user_bytes_written) << "sanity: tables are not absurdly large";
}

TEST(EngineStats, CompactionBytesCoverTheTablesCompactionsWrote) {
  TempDir dir;
  std::unique_ptr<KVEngine> e;
  ASSERT_EQ(KVEngine::Open(Opts(dir.path(), /*compaction_trigger=*/2), &e), Status::kOk);
  for (int i = 0; i < 600; ++i) {
    ASSERT_EQ(e->Put("key" + std::to_string(1000 + i % 150), std::string(60, 'a' + i % 26)), Status::kOk);
  }
  ASSERT_TRUE(WaitFor([&] {
    const KVEngine::Stats s = e->GetStats();
    return s.frozen_memtables == 0 && s.sstables < 2 && s.compactions_done > 0;
  }));
  const KVEngine::Stats s = e->GetStats();
  EXPECT_GT(s.compaction_bytes_written, 0u);
  EXPECT_GE(s.flush_bytes_written + s.compaction_bytes_written, SstBytesOnDisk(dir.path()))
      << "every live table was written by a flush or a compaction";
}

TEST(EngineStats, ACheckpointInstallCountsItsTableSeparately) {
  TempDir dir;
  std::unique_ptr<KVEngine> e;
  ASSERT_EQ(KVEngine::Open(Opts(dir.path()), &e), Status::kOk);
  std::string body;
  AppendCheckpointEntry(&body, "a", "1");
  AppendCheckpointEntry(&body, "b", "2");
  ASSERT_EQ(e->InstallCheckpoint(CheckpointHeader(2) + body, 7), Status::kOk);
  const KVEngine::Stats s = e->GetStats();
  EXPECT_EQ(s.checkpoint_bytes_written, SstBytesOnDisk(dir.path()));
  EXPECT_EQ(s.flush_bytes_written, 0u);
  EXPECT_EQ(s.user_bytes_written, 0u) << "installed data is not a user write of this engine";
}

TEST(EngineStats, TheBlockCacheFiguresFollowTheOption) {
  TempDir dir;
  KVEngine::Options o = Opts(dir.path());
  std::unique_ptr<KVEngine> e;
  ASSERT_EQ(KVEngine::Open(o, &e), Status::kOk);
  EXPECT_EQ(e->GetStats().block_cache_capacity, 0u) << "off by default";
  EXPECT_EQ(e->GetStats().block_cache_usage, 0u);
  e.reset();

  o.block_cache_bytes = 1 << 20;
  ASSERT_EQ(KVEngine::Open(o, &e), Status::kOk);
  EXPECT_EQ(e->GetStats().block_cache_capacity, size_t{1} << 20);
  EXPECT_EQ(e->GetStats().block_cache_usage, 0u);
  for (int i = 0; i < 300; ++i) {
    ASSERT_EQ(e->Put("key" + std::to_string(1000 + i), std::string(60, 'v')), Status::kOk);
  }
  ASSERT_TRUE(WaitFor([&] { return e->GetStats().frozen_memtables == 0 && e->GetStats().sstables > 0; }));
  std::string v;
  ASSERT_EQ(e->Get("key1000", &v), Status::kOk);
  EXPECT_GT(e->GetStats().block_cache_usage, 0u) << "a read through an SSTable filled the cache";
  EXPECT_LE(e->GetStats().block_cache_usage, e->GetStats().block_cache_capacity);
}

TEST(EngineStats, LegacyModeHasNoCache) {
  TempDir dir;
  KVEngine::Options o = Opts(dir.path());
  o.use_sstables = false;
  o.block_cache_bytes = 1 << 20;  // ignored: there are no SSTables
  std::unique_ptr<KVEngine> e;
  ASSERT_EQ(KVEngine::Open(o, &e), Status::kOk);
  EXPECT_EQ(e->GetStats().block_cache_capacity, 0u);
}

}  // namespace
