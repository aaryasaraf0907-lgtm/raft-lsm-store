// An engine whose WAL is not fsynced on every write: a power loss can leave a hole in the middle of
// it, so recovery must be able to cut the log at the damage instead of refusing to start, and must
// never keep data that sits behind a hole.
#include <gtest/gtest.h>

#include <stdlib.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "storage/kv_engine.h"
#include "storage/wal.h"

using namespace raftkv;
using namespace std::chrono_literals;
namespace fs = std::filesystem;

namespace {

struct Dir {
  std::string path;
  Dir() {
    char tmpl[] = "/tmp/raftkv_async_XXXXXX";
    path = ::mkdtemp(tmpl);
  }
  ~Dir() {
    std::error_code ec;
    fs::remove_all(path, ec);
  }
};

KVEngine::Options Opts(const std::string& dir, bool tolerant, size_t memtable = 4 * 1024 * 1024) {
  KVEngine::Options o;
  o.dir = dir;
  o.sync_wal = false;
  o.use_sstables = true;
  o.compaction_trigger = 0;
  o.memtable_bytes = memtable;
  o.flush_retry_ms = 5;
  o.wal_truncate_on_corruption = tolerant;
  return o;
}

void FlipByte(const std::string& path, size_t pos) {
  std::string bytes;
  {
    std::ifstream in(path, std::ios::binary);
    bytes.assign(std::istreambuf_iterator<char>(in), {});
  }
  ASSERT_LT(pos, bytes.size());
  bytes[pos] ^= 0x40;
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  out << bytes;
}

std::string Key(uint64_t i) { return "key" + std::to_string(100000 + i); }

}  // namespace

TEST(WalReplay, ADamagedMiddleIsCutLikeATornTailOnlyWhenAskedTo) {
  Dir d;
  const std::string path = d.path + "/wal.log";
  {
    std::unique_ptr<WAL> w;
    ASSERT_EQ(WAL::Open(path, &w), Status::kOk);
    for (int i = 0; i < 20; ++i) ASSERT_EQ(w->AppendPut("k" + std::to_string(i), std::string(30, 'v'), false), Status::kOk);
  }
  const uint64_t size = fs::file_size(path);
  FlipByte(path, size / 2 + 20);  // a payload byte of a record in the middle (not a length field)
  ReplayStats strict;
  EXPECT_EQ(WAL::Replay(path, [](const Entry&) {}, &strict), Status::kCorruption) << "the default must still report it";
  EXPECT_EQ(fs::file_size(path), size) << "a refused replay must not modify the file";

  std::vector<std::string> keys;
  ReplayStats stats;
  ASSERT_EQ(WAL::Replay(path, [&](const Entry& e) { keys.push_back(e.key); }, &stats, /*truncate_mid_log_corruption=*/true), Status::kOk);
  ASSERT_GT(keys.size(), 0u);
  ASSERT_LT(keys.size(), 20u);
  for (size_t i = 0; i < keys.size(); ++i) EXPECT_EQ(keys[i], "k" + std::to_string(i)) << "only a clean prefix may survive";
  EXPECT_EQ(fs::file_size(path), stats.valid_bytes);
  EXPECT_GT(stats.discarded_bytes, 0u);
  std::unique_ptr<WAL> w;  // and the repaired log accepts new records
  ASSERT_EQ(WAL::Open(path, &w), Status::kOk);
  ASSERT_EQ(w->AppendPut("after", "repair", true), Status::kOk);
}

TEST(AsyncWalEngine, ADamagedMiddleLeavesAConsistentPrefixWhoseIndexMatchesItsData) {
  Dir d;
  {
    std::unique_ptr<KVEngine> e;
    ASSERT_EQ(KVEngine::Open(Opts(d.path, false), &e), Status::kOk);
    for (uint64_t i = 1; i <= 80; ++i) ASSERT_EQ(e->PutAt(Key(i), std::string(40, 'v'), i), Status::kOk);
  }
  const std::string wal = d.path + "/" + KVEngine::WalFileName(1);
  FlipByte(wal, fs::file_size(wal) / 2);
  {
    std::unique_ptr<KVEngine> e;
    EXPECT_EQ(KVEngine::Open(Opts(d.path, /*tolerant=*/false), &e), Status::kCorruption) << "strict mode must refuse";
  }
  std::unique_ptr<KVEngine> e;
  ASSERT_EQ(KVEngine::Open(Opts(d.path, /*tolerant=*/true), &e), Status::kOk);
  const uint64_t applied = e->AppliedIndex();
  ASSERT_GT(applied, 5u);
  ASSERT_LT(applied, 80u);
  std::string v;
  for (uint64_t i = 1; i <= applied; ++i) ASSERT_EQ(e->Get(Key(i), &v), Status::kOk) << "an entry the index claims is missing: " << i;
  // The data record can survive without its index record (the index lags by one), never more.
  for (uint64_t i = applied + 2; i <= 80; ++i) EXPECT_EQ(e->Get(Key(i), &v), Status::kNotFound) << "data after the hole survived: " << i;
  // The engine is usable again, and what it does next survives another restart.
  ASSERT_EQ(e->PutAt("after", "repair", 500), Status::kOk);
  e.reset();
  ASSERT_EQ(KVEngine::Open(Opts(d.path, true), &e), Status::kOk);
  EXPECT_EQ(e->AppliedIndex(), 500u);
  EXPECT_EQ(e->Get("after", &v), Status::kOk);
}

// Several WALs are unflushed (the flush "crashed"). If the OLDEST is damaged in the middle, the
// newer ones sit behind a hole: keeping them would leave an applied index that claims entries whose
// data was lost, and replay would skip exactly those entries.
TEST(AsyncWalEngine, DataBehindAHoleInAnOlderWalIsDroppedWithIt) {
  Dir d;
  std::atomic<bool> fired{false};
  {
    KVEngine::Options o = Opts(d.path, false, /*memtable=*/2048);
    o.test_crash_hook = [&](const char* p) { return std::string(p) == "flush:after_sst" && !fired.exchange(true); };
    std::unique_ptr<KVEngine> e;
    ASSERT_EQ(KVEngine::Open(o, &e), Status::kOk);
    for (uint64_t i = 1; i <= 400 && !fired; ++i) {
      while (!fired && e->PutAt(Key(i), std::string(100, 'v'), i) != Status::kOk) std::this_thread::sleep_for(1ms);
    }
    for (int i = 0; i < 2000 && !fired; ++i) std::this_thread::sleep_for(1ms);
    ASSERT_TRUE(fired);
  }
  std::set<uint64_t> wal_seqs;
  for (const auto& de : fs::directory_iterator(d.path)) {
    const std::string n = de.path().filename().string();
    if (n.rfind("wal-", 0) == 0) wal_seqs.insert(std::stoull(n.substr(4, 6)));
  }
  ASSERT_GE(wal_seqs.size(), 2u) << "the test needs several unflushed WALs";
  const std::string oldest = d.path + "/" + KVEngine::WalFileName(*wal_seqs.begin());
  FlipByte(oldest, fs::file_size(oldest) / 2);

  std::unique_ptr<KVEngine> e;
  ASSERT_EQ(KVEngine::Open(Opts(d.path, true, 2048), &e), Status::kOk);
  const uint64_t applied = e->AppliedIndex();
  ASSERT_GT(applied, 0u);
  std::string v;
  for (uint64_t i = 1; i <= applied; ++i) ASSERT_EQ(e->Get(Key(i), &v), Status::kOk) << "the index claims " << i << " but its data is gone";
  for (uint64_t i = applied + 2; i <= 400; ++i) ASSERT_EQ(e->Get(Key(i), &v), Status::kNotFound) << "data newer than the hole survived: " << i;
  for (uint64_t seq : wal_seqs) {
    if (seq != *wal_seqs.begin()) {
      EXPECT_FALSE(fs::exists(d.path + "/" + KVEngine::WalFileName(seq))) << "WAL " << seq << " sat behind the hole";
    }
  }
}
