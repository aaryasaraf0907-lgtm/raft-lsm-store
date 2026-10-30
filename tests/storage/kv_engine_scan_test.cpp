// KVEngine::Scan: range boundaries, limits, the merge of the active memtable, frozen memtables and
// SSTables (newest version wins, tombstones hide older values), reserved keys, concurrency with
// writes, flushes and compactions, checkpoint installs, errors and legacy mode.
#include <gtest/gtest.h>

#include <atomic>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "engine_test_util.h"
#include "storage/checkpoint.h"
#include "storage/kv_engine.h"

using namespace raftkv;
using raftkv::testutil::BuildTables;
using raftkv::testutil::TempDir;
using raftkv::testutil::WaitFor;
namespace fs = std::filesystem;

namespace {

using KVs = std::vector<std::pair<std::string, std::string>>;

class ScanTest : public ::testing::TestWithParam<size_t> {  // the parameter: block_cache_bytes
 protected:
  void TearDown() override { engine_.reset(); }

  KVEngine::Options Opts(size_t compaction_trigger = 0) {
    KVEngine::Options o;
    o.dir = dir_.path();
    o.sync_wal = false;
    o.memtable_bytes = 2048;
    o.use_sstables = true;
    o.compaction_trigger = compaction_trigger;
    o.flush_retry_ms = 5;
    o.block_cache_bytes = GetParam();
    return o;
  }
  void Open(const KVEngine::Options& o) {
    engine_.reset();
    ASSERT_EQ(KVEngine::Open(o, &engine_), Status::kOk);
  }
  KVs Scan(const std::string& start, const std::string& end, size_t limit = 0) {
    KVs out{{"stale", "output"}};  // must be replaced
    EXPECT_EQ(engine_->Scan(start, end, limit, &out), Status::kOk) << start << ".." << end;
    return out;
  }
  // Writes filler keys (outside the "k" range) until the active memtable has been frozen.
  void FreezeActive() {
    const uint64_t before = engine_->GetStats().active_seq;
    for (int i = 0; i < 400 && engine_->GetStats().active_seq == before; ++i) {
      char k[16];
      std::snprintf(k, sizeof(k), "z%04d", i);
      ASSERT_EQ(engine_->Put(k, std::string(100, 'z')), Status::kOk);
    }
    ASSERT_GT(engine_->GetStats().active_seq, before);
  }
  std::string Get(const std::string& key) {
    std::string v;
    const Status s = engine_->Get(key, &v);
    return s == Status::kOk ? v : (s == Status::kNotFound ? "<absent>" : "<error>");
  }

  TempDir dir_;
  std::unique_ptr<KVEngine> engine_;
};

INSTANTIATE_TEST_SUITE_P(CacheOffAndOn, ScanTest, ::testing::Values(size_t{0}, size_t{1} << 20),
                         [](const ::testing::TestParamInfo<size_t>& p) {
                           return p.param == 0 ? std::string("NoCache") : std::string("Cache");
                         });

TEST_P(ScanTest, StartIsInclusiveEndIsExclusiveAndAnEmptyEndIsUnbounded) {
  Open(Opts());
  for (const char* k : {"", "a", "b", "bb", "c"}) ASSERT_EQ(engine_->Put(k, std::string("v") + k), Status::kOk);
  ASSERT_EQ(engine_->Put("b", ""), Status::kOk);  // an empty value is a value, not a deletion
  const KVs all{{"", "v"}, {"a", "va"}, {"b", ""}, {"bb", "vbb"}, {"c", "vc"}};
  EXPECT_EQ(Scan("", ""), all);
  EXPECT_EQ(Scan("a", "c"), (KVs{{"a", "va"}, {"b", ""}, {"bb", "vbb"}}));
  EXPECT_EQ(Scan("b", "bb"), (KVs{{"b", ""}}));
  EXPECT_EQ(Scan("ab", ""), (KVs{{"b", ""}, {"bb", "vbb"}, {"c", "vc"}}));
  EXPECT_EQ(Scan("c", ""), (KVs{{"c", "vc"}}));
  EXPECT_EQ(Scan("d", ""), KVs{});
  EXPECT_EQ(Scan("b", "b"), KVs{}) << "start == end";
  EXPECT_EQ(Scan("c", "a"), KVs{}) << "start > end";
  EXPECT_EQ(Scan("", std::string(1, '\0')), (KVs{{"", "v"}})) << "only the empty key sorts before \"\\0\"";
}

TEST_P(ScanTest, LimitCountsOnlyReturnedKeysAndZeroMeansNoLimit) {
  Open(Opts());
  for (const char* k : {"a", "b", "c", "d"}) ASSERT_EQ(engine_->Put(k, k), Status::kOk);
  EXPECT_EQ(Scan("", "", 2), (KVs{{"a", "a"}, {"b", "b"}}));
  EXPECT_EQ(Scan("", "", 0).size(), 4u);
  EXPECT_EQ(Scan("", "", 100).size(), 4u);
  EXPECT_EQ(Scan("b", "", 1), (KVs{{"b", "b"}}));
  ASSERT_EQ(engine_->Delete("a"), Status::kOk);
  ASSERT_EQ(engine_->Delete("b"), Status::kOk);
  EXPECT_EQ(Scan("", "", 1), (KVs{{"c", "c"}})) << "deleted keys do not use up the limit";
}

TEST_P(ScanTest, ReservedKeysAreNeverReturned) {
  Open(Opts());
  ASSERT_EQ(engine_->PutAt("a", "1", 1), Status::kOk);  // also writes the applied-index key
  ASSERT_EQ(engine_->PutAt("b", "2", 2), Status::kOk);
  const KVs want{{"a", "1"}, {"b", "2"}};
  EXPECT_EQ(Scan("", ""), want);
  EXPECT_EQ(Scan(std::string(kReservedKeyPrefix, sizeof(kReservedKeyPrefix) - 1), ""), want);
  EXPECT_EQ(Scan(std::string(1, '\0'), "a"), KVs{});
  FreezeActive();
  ASSERT_TRUE(WaitFor([&] { return testutil::Settled(*engine_, 0); }));
  EXPECT_EQ(Scan("", "z"), want) << "also when the reserved key comes from an SSTable";
}

TEST_P(ScanTest, MergesActiveFrozenAndPersistedLayersNewestFirst) {
  // Two SSTables (older, newer), a frozen memtable the flusher cannot persist (the manifest store
  // is blocked), and the active memtable: every precedence case across the layers.
  BuildTables(dir_.path(), {
                               {{"k1", "sst1", false}, {"k2", "sst1", false}, {"k3", "sst1", false},
                                {"k5", "sst1", false}, {"k7", "sst1", false}},
                               {{"k2", "", true}, {"k3", "sst2", false}, {"k4", "", true}},
                           });
  KVEngine::Options o = Opts();
  o.flush_retry_ms = 20;
  Open(o);
  const std::string blocker = dir_.path() + "/MANIFEST.tmp";  // makes every manifest store fail
  fs::create_directories(blocker);
  { std::ofstream(blocker + "/keep") << "x"; }
  ASSERT_EQ(engine_->Put("k1", "frozen"), Status::kOk);
  ASSERT_EQ(engine_->Put("k4", "frozen"), Status::kOk);  // a newer value over an older tombstone
  ASSERT_EQ(engine_->Put("k6", "frozen"), Status::kOk);
  ASSERT_EQ(engine_->Delete("k7"), Status::kOk);         // a newer tombstone over an older value
  FreezeActive();
  ASSERT_EQ(engine_->Put("k1", "active"), Status::kOk);  // overwrites frozen and sst1
  ASSERT_EQ(engine_->Delete("k5"), Status::kOk);
  ASSERT_EQ(engine_->Put("k6", "active"), Status::kOk);
  ASSERT_TRUE(WaitFor([&] { return engine_->GetStats().flush_failing; }));
  ASSERT_GE(engine_->GetStats().frozen_memtables, 1u) << "the frozen layer must still be in memory";

  const KVs want{{"k1", "active"}, {"k3", "sst2"}, {"k4", "frozen"}, {"k6", "active"}};
  EXPECT_EQ(Scan("k", "l"), want);
  EXPECT_EQ(Scan("k2", "k5"), (KVs{{"k3", "sst2"}, {"k4", "frozen"}}));
  EXPECT_EQ(Scan("k", "l", 2), (KVs{{"k1", "active"}, {"k3", "sst2"}}));
  for (const auto& kv : want) EXPECT_EQ(Get(kv.first), kv.second) << "Get and Scan must agree";
  for (const char* k : {"k2", "k5", "k7"}) EXPECT_EQ(Get(k), "<absent>") << k;

  fs::remove_all(blocker);  // the flush completes: the frozen layer becomes an SSTable
  ASSERT_TRUE(WaitFor([&] { return testutil::Settled(*engine_, 0); }));
  EXPECT_EQ(Scan("k", "l"), want);
  Open(Opts(/*compaction_trigger=*/2));  // reopen (WAL replay), then compaction merges the tables
  ASSERT_TRUE(WaitFor([&] { return testutil::Settled(*engine_, 2); }));
  EXPECT_EQ(Scan("k", "l"), want);
  EXPECT_EQ(Scan("k", "l", 3), (KVs{{"k1", "active"}, {"k3", "sst2"}, {"k4", "frozen"}}));
}

TEST_P(ScanTest, ConcurrentWritesFlushesAndCompactionsNeverHideCompletedWrites) {
  // A writer appends keys w000000.. and overwrites "x" with its own progress; flushes and
  // compactions run all the time (tiny memtables, trigger 2). Every scan must contain every write
  // that completed before it started, in order, without errors; "x" can only be newer.
  Open(Opts(/*compaction_trigger=*/2));
  constexpr int kWrites = 1500;
  std::atomic<int> completed{0};
  std::atomic<bool> writer_failed{false};
  std::thread writer([&] {
    char k[16], x[16];
    for (int i = 0; i < kWrites; ++i) {
      std::snprintf(k, sizeof(k), "w%06d", i);
      std::snprintf(x, sizeof(x), "%010d", i);
      if (engine_->Put(k, std::string(40, 'v')) != Status::kOk || engine_->Put("x", x) != Status::kOk) {
        writer_failed = true;
        return;
      }
      completed.store(i + 1);
    }
  });
  int scans = 0;
  bool good = true;
  while (good && (completed.load() < kWrites || scans < 5) && !writer_failed) {
    const int before = completed.load();
    KVs out;
    good = engine_->Scan("", "", 0, &out) == Status::kOk;
    ASSERT_TRUE(good) << "scan failed";
    for (size_t i = 1; i < out.size(); ++i) ASSERT_LT(out[i - 1].first, out[i].first) << "sorted, unique";
    // Keys "w..." come first and form a prefix w000000..w(n-1) with n >= before.
    int n = 0;
    while (static_cast<size_t>(n) < out.size() && out[static_cast<size_t>(n)].first[0] == 'w') {
      char k[16];
      std::snprintf(k, sizeof(k), "w%06d", n);
      ASSERT_EQ(out[static_cast<size_t>(n)].first, k);
      ++n;
    }
    ASSERT_GE(n, before) << "a write that completed before the scan is missing";
    if (before > 0) {
      ASSERT_EQ(out.back().first, "x");
      ASSERT_GE(std::stoi(out.back().second), before - 1) << "an older value of x than one already written";
    }
    if (before > 300) {  // a bounded scan in the middle: Seek + limit
      KVs part;
      ASSERT_EQ(engine_->Scan("w000100", "w000300", 50, &part), Status::kOk);
      ASSERT_EQ(part.size(), 50u);
      EXPECT_EQ(part.front().first, "w000100");
      EXPECT_EQ(part.back().first, "w000149");
    }
    ++scans;
  }
  writer.join();
  ASSERT_FALSE(writer_failed);
  EXPECT_GT(engine_->GetStats().compactions_done, 0u) << "the test must overlap with compactions";
  ASSERT_TRUE(WaitFor([&] { return testutil::Settled(*engine_, 2); }));
  EXPECT_EQ(Scan("w", "x").size(), static_cast<size_t>(kWrites));
}

TEST_P(ScanTest, AfterACheckpointInstallOnlyTheInstalledDataIsVisible) {
  Open(Opts());
  ASSERT_EQ(engine_->Put("a", "old"), Status::kOk);
  ASSERT_EQ(engine_->Put("c", "old"), Status::kOk);
  FreezeActive();
  ASSERT_TRUE(WaitFor([&] { return testutil::Settled(*engine_, 0); }));
  std::string body;
  AppendCheckpointEntry(&body, "b", "installed");
  AppendCheckpointEntry(&body, "c", "installed");
  ASSERT_EQ(engine_->InstallCheckpoint(CheckpointHeader(2) + body, 10), Status::kOk);
  EXPECT_EQ(Scan("", ""), (KVs{{"b", "installed"}, {"c", "installed"}}));
  ASSERT_EQ(engine_->Put("d", "after"), Status::kOk);
  EXPECT_EQ(Scan("", ""), (KVs{{"b", "installed"}, {"c", "installed"}, {"d", "after"}}));
}

TEST_P(ScanTest, AFailStoppedEngineRefusesScans) {
  KVEngine::Options o = Opts();
  // The install's manifest store and its restore both "fail" after their renames: fail-stop.
  o.test_crash_hook = [](const char* p) { return std::string(p) == "install:fail_after_manifest_rename"; };
  Open(o);
  ASSERT_EQ(engine_->Put("a", "1"), Status::kOk);
  std::string body;
  AppendCheckpointEntry(&body, "b", "2");
  ASSERT_EQ(engine_->InstallCheckpoint(CheckpointHeader(1) + body, 5), Status::kIOError);
  ASSERT_TRUE(engine_->GetStats().fail_stopped);
  KVs out{{"stale", "output"}};
  EXPECT_EQ(engine_->Scan("", "", 0, &out), Status::kIOError);
  EXPECT_TRUE(out.empty());
}

TEST_P(ScanTest, ADamagedBlockFailsTheScanThatReadsIt) {
  std::vector<Entry> rows;
  for (int i = 0; i < 3000; ++i) {
    char k[16];
    std::snprintf(k, sizeof(k), "a%04d", i);
    rows.push_back(Entry{k, std::string(20, 'r'), false});
  }
  BuildTables(dir_.path(), {rows});
  const std::string sst = dir_.path() + "/" + KVEngine::SstFileName(1);
  {  // flip a byte in the middle of the data blocks (they make up most of the file)
    std::fstream f(sst, std::ios::in | std::ios::out | std::ios::binary);
    const std::streamoff at = static_cast<std::streamoff>(fs::file_size(sst) / 2);
    f.seekg(at);
    char c = 0;
    f.get(c);
    f.seekp(at);
    f.put(static_cast<char>(c ^ 1));
  }
  Open(Opts());  // opening does not read data blocks (sstable-format.md §10.1)
  KVs out{{"stale", "output"}};
  EXPECT_EQ(engine_->Scan("", "", 0, &out), Status::kCorruption);
  EXPECT_TRUE(out.empty()) << "an error is never a shorter result";
  EXPECT_EQ(engine_->Scan("a1000", "a2000", 0, &out), Status::kCorruption);
  EXPECT_TRUE(out.empty());
  KVs head = Scan("a0000", "a0010");  // the first block is intact
  EXPECT_EQ(head.size(), 10u);
}

TEST(ScanLegacyMode, WorksOverTheMemtablesAlone) {
  TempDir dir;
  KVEngine::Options o;
  o.dir = dir.path();
  o.sync_wal = false;
  std::unique_ptr<KVEngine> engine;
  ASSERT_EQ(KVEngine::Open(o, &engine), Status::kOk);
  ASSERT_EQ(engine->PutAt("a", "1", 1), Status::kOk);
  ASSERT_EQ(engine->PutAt("b", "2", 2), Status::kOk);
  ASSERT_EQ(engine->PutAt("c", "3", 3), Status::kOk);
  ASSERT_EQ(engine->DeleteAt("b", 4), Status::kOk);
  ASSERT_EQ(engine->Put("a", "1b"), Status::kOk);
  KVs out;
  ASSERT_EQ(engine->Scan("", "", 0, &out), Status::kOk);
  EXPECT_EQ(out, (KVs{{"a", "1b"}, {"c", "3"}}));
  engine.reset();
  ASSERT_EQ(KVEngine::Open(o, &engine), Status::kOk);  // after WAL replay
  ASSERT_EQ(engine->Scan("b", "", 0, &out), Status::kOk);
  EXPECT_EQ(out, (KVs{{"c", "3"}}));
}

}  // namespace
