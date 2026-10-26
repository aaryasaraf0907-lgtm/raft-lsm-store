// KVEngine in SSTable mode: manifest, flush into SSTables, reads through them, compaction, and
// recovery from a crash at every step of flush and compaction.
#include <gtest/gtest.h>

#include <stdlib.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "storage/kv_engine.h"
#include "storage/manifest.h"
#include "storage/sstable.h"

using namespace raftkv;
using namespace std::chrono_literals;
namespace fs = std::filesystem;

namespace {

const std::string kIndexKey = std::string(kReservedKeyPrefix, sizeof(kReservedKeyPrefix) - 1) + "applied_index";

bool WaitFor(const std::function<bool()>& pred, std::chrono::milliseconds timeout = 15000ms) {
  const auto end = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < end) {
    if (pred()) return true;
    std::this_thread::sleep_for(2ms);
  }
  return pred();
}

class LsmTest : public ::testing::Test {
 protected:
  void SetUp() override {
    char tmpl[] = "/tmp/raftkv_lsm_XXXXXX";
    dir_ = ::mkdtemp(tmpl);
  }
  void TearDown() override {
    engine_.reset();
    std::error_code ec;
    fs::remove_all(dir_, ec);
  }
  KVEngine::Options Opts(size_t compaction_trigger = 0) {
    KVEngine::Options o;
    o.dir = dir_;
    o.sync_wal = false;
    o.memtable_bytes = 2048;
    o.use_sstables = true;
    o.compaction_trigger = compaction_trigger;
    o.flush_retry_ms = 5;
    return o;
  }
  Status Open(const KVEngine::Options& o) {
    engine_.reset();
    return KVEngine::Open(o, &engine_);
  }
  std::string Get(const std::string& key) {
    std::string v;
    Status s = engine_->Get(key, &v);
    return s == Status::kOk ? v : (s == Status::kNotFound ? "<absent>" : "<error>");
  }
  // Put or delete, retrying while writers are briefly refused because flushing is behind.
  void Write(const std::string& key, const std::string& value, uint64_t index = 0, bool del = false) {
    for (int i = 0; i < 5000; ++i) {
      Status s = del ? (index ? engine_->DeleteAt(key, index) : engine_->Delete(key))
                     : (index ? engine_->PutAt(key, value, index) : engine_->Put(key, value));
      if (s == Status::kOk) return;
      ASSERT_NE(s, Status::kInvalidArgument);
      std::this_thread::sleep_for(2ms);
    }
    FAIL() << "write never succeeded";
  }
  // Like Write, but gives up quietly once a crash point has fired (the "process" is dead then).
  bool WriteUntil(const std::atomic<bool>& dead, const std::string& key, const std::string& value) {
    while (!dead) {
      if (engine_->Put(key, value) == Status::kOk) return true;
      std::this_thread::sleep_for(1ms);
    }
    return false;
  }
  // Writes until the engine has handed out a new WAL number (a memtable was frozen).
  void ForceAFreeze() {
    const uint64_t before = engine_->GetStats().active_seq;
    for (int i = 0; i < 400 && engine_->GetStats().active_seq == before; ++i) Write("freeze" + std::to_string(i), std::string(120, 'z'));
    ASSERT_GT(engine_->GetStats().active_seq, before);
  }
  void Settle() {  // every frozen memtable flushed
    ASSERT_TRUE(WaitFor([&] { return engine_->GetStats().frozen_memtables == 0; }));
  }
  ManifestState Manifest() {
    ManifestState m;
    bool found = false;
    EXPECT_EQ(LoadManifest(dir_, &m, &found), Status::kOk);
    EXPECT_TRUE(found);
    return m;
  }
  std::vector<std::string> Files(const std::string& suffix) {
    std::vector<std::string> out;
    for (const auto& de : fs::directory_iterator(dir_)) {
      const std::string n = de.path().filename().string();
      if (n.size() >= suffix.size() && n.compare(n.size() - suffix.size(), suffix.size(), suffix) == 0) out.push_back(n);
    }
    return out;
  }
  static std::string Key(int i) { return "key" + std::to_string(1000 + i); }

  std::string dir_;
  std::unique_ptr<KVEngine> engine_;
};

}  // namespace

TEST_F(LsmTest, FlushedDataIsReadableAndSurvivesARestart) {
  ASSERT_EQ(Open(Opts()), Status::kOk);
  for (int i = 0; i < 300; ++i) Write(Key(i), "value-" + std::to_string(i) + std::string(80, 'x'));
  Settle();
  EXPECT_GE(engine_->GetStats().sstables, 3u);
  for (int i = 0; i < 300; ++i) ASSERT_EQ(Get(Key(i)), "value-" + std::to_string(i) + std::string(80, 'x')) << i;
  EXPECT_EQ(Get("never-written"), "<absent>");
  ASSERT_EQ(Open(Opts()), Status::kOk);  // everything now comes from SSTables plus the unflushed tail
  for (int i = 0; i < 300; ++i) ASSERT_EQ(Get(Key(i)), "value-" + std::to_string(i) + std::string(80, 'x')) << i;
}

TEST_F(LsmTest, NewerVersionsAndTombstonesInNewerTablesHideOlderOnes) {
  ASSERT_EQ(Open(Opts()), Status::kOk);
  auto churn = [&](int tag) {  // forces a flush so the next write lands in a newer memtable
    for (int i = 0; i < 40; ++i) Write("filler" + std::to_string(tag) + "-" + std::to_string(i), std::string(100, 'f'));
    Settle();
  };
  Write("a", "old");
  Write("b", "keep");
  Write("c", "old");
  churn(1);
  Write("a", "new");   // overwritten in a newer table
  Write("c", "", 0, true);  // deleted in a newer table
  churn(2);
  EXPECT_EQ(Get("a"), "new");
  EXPECT_EQ(Get("b"), "keep");
  EXPECT_EQ(Get("c"), "<absent>") << "a tombstone in a newer SSTable must hide the older value";
  Write("c", "again");  // and a still newer write beats the tombstone
  EXPECT_EQ(Get("c"), "again");
  ASSERT_EQ(Open(Opts()), Status::kOk);
  EXPECT_EQ(Get("a"), "new");
  EXPECT_EQ(Get("c"), "again");
}

// Condition 1 of the manifest design: after a flush has retired the WAL, the applied index lives
// only in an SSTable, so the engine must install the SSTables BEFORE it reads the index at startup.
TEST_F(LsmTest, TheAppliedIndexSurvivesFlushAndRestart) {
  ASSERT_EQ(Open(Opts()), Status::kOk);
  for (uint64_t i = 1; i <= 200; ++i) Write(Key(static_cast<int>(i)), std::string(90, 'v'), i);
  Settle();
  ASSERT_GE(engine_->GetStats().sstables, 2u);
  ASSERT_EQ(engine_->AppliedIndex(), 200u);
  ASSERT_EQ(Open(Opts()), Status::kOk);
  EXPECT_EQ(engine_->AppliedIndex(), 200u) << "the index was lost: the reader was not in place when it was read";
  // The index key is also visible to the engine's own Get, like any key.
  EXPECT_NE(Get(kIndexKey), "<absent>");
  Write("next", "x", 201);
  EXPECT_EQ(engine_->AppliedIndex(), 201u);
}

// Condition 2: one memtable, one SSTable. A data record and its index record never end up in
// different tables, so each table's index equals the highest index of the data inside it.
TEST_F(LsmTest, EveryFlushedTableCarriesTheIndexOfItsOwnData) {
  ASSERT_EQ(Open(Opts()), Status::kOk);
  for (uint64_t i = 1; i <= 240; ++i) Write(Key(static_cast<int>(i)), std::string(90, 'v'), i);
  Settle();
  const ManifestState m = Manifest();
  ASSERT_GE(m.files.size(), 3u);
  for (const SSTableMeta& f : m.files) {
    std::unique_ptr<SSTable> t;
    ASSERT_EQ(SSTable::Open(dir_ + "/" + KVEngine::SstFileName(f.file_number), f.file_number, &t), Status::kOk);
    uint64_t highest_data = 0, stored_index = 0;
    for (auto it = t->NewIterator(); it->Valid(); it->Next()) {
      const Entry& e = it->Current();
      if (e.key == kIndexKey) {
        for (int b = 0; b < 8; ++b) stored_index |= static_cast<uint64_t>(static_cast<uint8_t>(e.value[b])) << (8 * b);
      } else {
        highest_data = std::max<uint64_t>(highest_data, std::stoull(e.key.substr(3)) - 1000);
      }
    }
    EXPECT_EQ(stored_index, highest_data) << "table " << f.file_number;
  }
}

TEST_F(LsmTest, TheManifestIsTheSourceOfTruth) {
  ASSERT_EQ(Open(Opts()), Status::kOk);
  for (int i = 0; i < 200; ++i) Write(Key(i), std::string(90, 'v'));
  Settle();
  const ManifestState m = Manifest();
  EXPECT_EQ(m.files.size(), engine_->GetStats().sstables);
  EXPECT_EQ(Files(".sst").size(), m.files.size());
  uint64_t highest_file = 0;
  for (const auto& f : m.files) highest_file = std::max(highest_file, f.file_number);
  EXPECT_EQ(m.flushed_through, highest_file) << "without compaction, memtable n is file n";
  EXPECT_GT(m.next_file_number, highest_file);
  EXPECT_GE(m.next_file_number, engine_->GetStats().active_seq) << "numbers already handed to WALs are covered";
}

// Crash AFTER the SSTable is published but BEFORE the manifest names it: the SSTable is an orphan,
// the WAL still has everything. Startup deletes the orphan and replays the WAL.
TEST_F(LsmTest, ACrashBeforeTheManifestEditLosesNothingAndLeavesNoOrphan) {
  std::atomic<bool> fired{false};
  KVEngine::Options o = Opts();
  o.test_crash_hook = [&](const char* point) {
    if (std::string(point) == "flush:after_sst" && !fired.exchange(true)) return true;
    return false;
  };
  ASSERT_EQ(Open(o), Status::kOk);
  int written = 0;
  for (; written < 200 && !fired; ++written) {
    if (!WriteUntil(fired, Key(written), "value" + std::string(90, 'x'))) break;
  }
  ASSERT_TRUE(WaitFor([&] { return fired.load(); }));
  engine_.reset();  // the process "died" there
  EXPECT_EQ(Files(".sst").size(), 1u) << "the orphan SSTable is on disk";
  EXPECT_TRUE(fs::exists(dir_ + "/" + KVEngine::WalFileName(1))) << "the WAL still holds the data";
  ASSERT_EQ(Open(Opts()), Status::kOk);
  for (int i = 0; i < written; ++i) ASSERT_EQ(Get(Key(i)), "value" + std::string(90, 'x')) << "acknowledged write " << i << " was lost";
  Settle();  // recovery flushes the replayed memtable again, cleanly this time
  EXPECT_EQ(Files(".sst").size(), Manifest().files.size()) << "no SSTable may exist that the manifest does not know";
  Write("fresh", "after-recovery");
  EXPECT_EQ(Get("fresh"), "after-recovery");
}

// Crash AFTER the manifest names the SSTable but BEFORE the WAL is deleted: the WAL is at or below
// flushed_through, so startup retires it instead of replaying it over newer data.
TEST_F(LsmTest, ACrashBeforeTheWalIsDeletedRetiresItInsteadOfReplayingIt) {
  std::atomic<bool> fired{false};
  KVEngine::Options o = Opts();
  o.test_crash_hook = [&](const char* point) {
    return std::string(point) == "flush:after_manifest" && !fired.exchange(true);
  };
  ASSERT_EQ(Open(o), Status::kOk);
  int written = 0;
  for (; written < 200 && !fired; ++written) {
    if (!WriteUntil(fired, Key(written), "value" + std::string(90, 'x'))) break;
  }
  ASSERT_TRUE(WaitFor([&] { return fired.load(); }));
  engine_.reset();
  const std::string wal1 = dir_ + "/" + KVEngine::WalFileName(1);
  ASSERT_TRUE(fs::exists(wal1)) << "the crash happened before the WAL was deleted";
  ASSERT_EQ(Open(Opts()), Status::kOk);
  EXPECT_FALSE(fs::exists(wal1)) << "a WAL at or below flushed_through must be retired";
  EXPECT_EQ(Manifest().flushed_through, 1u);
  for (int i = 0; i < written; ++i) ASSERT_EQ(Get(Key(i)), "value" + std::string(90, 'x')) << "write " << i;
}

TEST_F(LsmTest, CompactionMergesIntoOneTableAndDropsTombstones) {
  ASSERT_EQ(Open(Opts(/*compaction_trigger=*/4)), Status::kOk);
  std::map<std::string, std::string> model;
  for (int round = 0; round < 8; ++round) {  // overwrites and deletes spread over many flushes
    for (int i = 0; i < 50; ++i) {
      const std::string k = Key(i);
      if ((i + round) % 7 == 0) {
        Write(k, "", 0, true);
        model.erase(k);
      } else {
        const std::string v = "r" + std::to_string(round) + "-" + std::to_string(i) + std::string(60, 'p');
        Write(k, v);
        model[k] = v;
      }
    }
  }
  Settle();
  ASSERT_TRUE(WaitFor([&] { return engine_->GetStats().compactions_done >= 1; }));
  ASSERT_TRUE(WaitFor([&] { return engine_->GetStats().sstables < 4; }));
  for (int i = 0; i < 50; ++i) {
    auto it = model.find(Key(i));
    ASSERT_EQ(Get(Key(i)), it == model.end() ? "<absent>" : it->second) << Key(i);
  }
  // Wait until the live set is down to one table, then look inside it: no tombstone may remain.
  ASSERT_TRUE(WaitFor([&] {
    auto s = engine_->GetStats();
    return s.sstables == 1 || (s.sstables < 4 && s.frozen_memtables == 0 && s.compactions_done >= 2);
  }));
  const ManifestState m = Manifest();
  ASSERT_FALSE(m.files.empty());
  std::unique_ptr<SSTable> oldest;
  ASSERT_EQ(SSTable::Open(dir_ + "/" + KVEngine::SstFileName(m.files[0].file_number), m.files[0].file_number, &oldest), Status::kOk);
  for (auto it = oldest->NewIterator(); it->Valid(); it->Next()) EXPECT_FALSE(it->Current().tombstone) << it->Current().key;
  EXPECT_EQ(Files(".sst").size(), m.files.size()) << "compaction inputs must be deleted";
}

// Crash after the merged output is published but before the manifest swaps it in: the old files are
// still the live set; the output is an orphan.
TEST_F(LsmTest, ACrashAfterTheCompactionOutputKeepsTheOldFilesAndDeletesTheOrphan) {
  std::atomic<bool> fired{false};
  KVEngine::Options o = Opts(4);
  o.test_crash_hook = [&](const char* point) {
    return std::string(point) == "compact:after_output" && !fired.exchange(true);
  };
  ASSERT_EQ(Open(o), Status::kOk);
  std::map<std::string, std::string> model;
  for (int i = 0; i < 600 && !fired; ++i) {
    const std::string v = "v" + std::to_string(i) + std::string(90, 'x');
    if (!WriteUntil(fired, Key(i % 60), v)) break;
    model[Key(i % 60)] = v;
  }
  ASSERT_TRUE(WaitFor([&] { return fired.load(); }));
  engine_.reset();
  const size_t on_disk = Files(".sst").size();
  const size_t live = Manifest().files.size();
  EXPECT_EQ(on_disk, live + 1) << "the unreferenced compaction output is on disk";
  ASSERT_EQ(Open(Opts(0)), Status::kOk);
  EXPECT_EQ(Files(".sst").size(), live) << "startup must delete the orphan output";
  for (int i = 0; i < 60; ++i) {
    auto it = model.find(Key(i));
    if (it != model.end()) {
      ASSERT_EQ(Get(Key(i)), it->second) << i;
    }
  }
}

// Crash after the manifest swapped the output in but before the inputs were deleted.
TEST_F(LsmTest, ACrashAfterTheCompactionManifestEditDeletesTheStaleInputsAtStartup) {
  std::atomic<bool> fired{false};
  KVEngine::Options o = Opts(4);
  o.test_crash_hook = [&](const char* point) {
    return std::string(point) == "compact:after_manifest" && !fired.exchange(true);
  };
  ASSERT_EQ(Open(o), Status::kOk);
  std::map<std::string, std::string> model;
  for (int i = 0; i < 600 && !fired; ++i) {
    const std::string v = "v" + std::to_string(i) + std::string(90, 'x');
    if (!WriteUntil(fired, Key(i % 60), v)) break;
    model[Key(i % 60)] = v;
  }
  ASSERT_TRUE(WaitFor([&] { return fired.load(); }));
  engine_.reset();
  const size_t live = Manifest().files.size();
  EXPECT_GT(Files(".sst").size(), live) << "the replaced inputs are still on disk";
  ASSERT_EQ(Open(Opts(0)), Status::kOk);
  EXPECT_EQ(Files(".sst").size(), live);
  for (int i = 0; i < 60; ++i) {
    auto it = model.find(Key(i));
    if (it != model.end()) {
      ASSERT_EQ(Get(Key(i)), it->second) << i;
    }
  }
}

// Condition 4: numbers handed out are never reused, even when the highest-numbered file is gone.
TEST_F(LsmTest, FileNumbersAreNeverReusedAcrossCompactionAndRestart) {
  ASSERT_EQ(Open(Opts(4)), Status::kOk);
  for (int i = 0; i < 500; ++i) Write(Key(i % 40), "v" + std::to_string(i) + std::string(90, 'x'));
  Settle();
  ASSERT_TRUE(WaitFor([&] { return engine_->GetStats().compactions_done >= 1; }));
  ASSERT_TRUE(WaitFor([&] { return engine_->GetStats().sstables < 4; }));
  const ManifestState before = Manifest();
  uint64_t highest = 0;
  for (const auto& f : before.files) highest = std::max(highest, f.file_number);
  ASSERT_GE(before.next_file_number, highest + 1);
  engine_.reset();
  ASSERT_EQ(Open(Opts(0)), Status::kOk);
  ForceAFreeze();  // the next number handed out must not be one that was used before
  EXPECT_GE(engine_->GetStats().active_seq, before.next_file_number) << "a number below the persisted counter was reused";
}

TEST_F(LsmTest, LeftoverOrphansAndTempFilesAreCleanedAtStartup) {
  ASSERT_EQ(Open(Opts()), Status::kOk);
  for (int i = 0; i < 100; ++i) Write(Key(i), std::string(90, 'v'));
  Settle();
  engine_.reset();
  { std::ofstream(dir_ + "/000999.sst") << "an orphan"; }
  { std::ofstream(dir_ + "/000998.sst.tmp.AbCdEf") << "a temp file"; }
  { std::ofstream(dir_ + "/MANIFEST.tmp") << "a half-written manifest"; }
  const size_t live = Manifest().files.size();
  ASSERT_EQ(Open(Opts()), Status::kOk);
  EXPECT_EQ(Files(".sst").size(), live);
  EXPECT_FALSE(fs::exists(dir_ + "/MANIFEST.tmp"));
  EXPECT_FALSE(fs::exists(dir_ + "/000998.sst.tmp.AbCdEf"));
  for (int i = 0; i < 100; ++i) ASSERT_EQ(Get(Key(i)), std::string(90, 'v')) << i;
  ForceAFreeze();
  EXPECT_GE(engine_->GetStats().active_seq, 1000u) << "an orphan's number must still count as used";
}

TEST_F(LsmTest, DamagedOrMissingStateIsReportedNeverTreatedAsEmpty) {
  ASSERT_EQ(Open(Opts()), Status::kOk);
  for (int i = 0; i < 150; ++i) Write(Key(i), std::string(90, 'v'));
  Settle();
  engine_.reset();
  const ManifestState m = Manifest();
  ASSERT_GE(m.files.size(), 2u);
  const std::string victim = dir_ + "/" + KVEngine::SstFileName(m.files[0].file_number);
  const std::string saved = dir_ + "/saved.copy";
  fs::copy_file(victim, saved);

  fs::remove(victim);  // a live SSTable is missing
  EXPECT_EQ(Open(Opts()), Status::kCorruption);
  fs::copy_file(saved, victim);

  std::string bytes;  // a damaged footer
  { std::ifstream in(victim, std::ios::binary); bytes.assign(std::istreambuf_iterator<char>(in), {}); }
  std::string bad = bytes;
  bad[bad.size() - 20] ^= 0x04;
  { std::ofstream out(victim, std::ios::binary | std::ios::trunc); out << bad; }
  EXPECT_EQ(Open(Opts()), Status::kCorruption);

  bad = bytes;  // damage inside a data block: Open succeeds, but a read must report the damage
  bad[40] ^= 0x04;
  { std::ofstream out(victim, std::ios::binary | std::ios::trunc); out << bad; }
  if (Open(Opts()) == Status::kOk) {
    bool saw_error = false;
    for (int i = 0; i < 150 && !saw_error; ++i) {
      std::string v;
      Status s = engine_->Get(Key(i), &v);
      if (s == Status::kCorruption) saw_error = true;
      else EXPECT_TRUE(s == Status::kOk || s == Status::kNotFound);
    }
    EXPECT_TRUE(saw_error) << "a damaged block must surface as kCorruption, not as a missing key";
  }
  fs::copy_file(saved, victim, fs::copy_options::overwrite_existing);
  ASSERT_EQ(Open(Opts()), Status::kOk);
  EXPECT_EQ(Get(Key(3)), std::string(90, 'v'));
}

TEST_F(LsmTest, SstablesWithoutAManifestAreRefusedRatherThanGuessedAbout) {
  ASSERT_EQ(Open(Opts()), Status::kOk);
  for (int i = 0; i < 100; ++i) Write(Key(i), std::string(90, 'v'));
  Settle();
  engine_.reset();
  fs::remove(dir_ + "/MANIFEST");
  EXPECT_EQ(Open(Opts()), Status::kCorruption);
}

TEST_F(LsmTest, TheTwoFlushPathsAreMutuallyExclusive) {
  KVEngine::Options o = Opts();
  o.flush_writer = [](KVIterator&, const std::string&, uint64_t) { return Status::kOk; };
  EXPECT_EQ(Open(o), Status::kInvalidArgument);
}

// Reads are always consistent with the model, whatever flush and compaction are doing underneath.
TEST_F(LsmTest, RandomOperationsMatchAModelThroughFlushCompactionAndRestarts) {
  std::mt19937_64 g(31337);
  ASSERT_EQ(Open(Opts(3)), Status::kOk);
  std::map<std::string, std::string> model;
  uint64_t index = 0;
  auto check_all = [&](const char* when) {
    int bad = 0;
    for (int i = 0; i < 40; ++i) {
      auto it = model.find(Key(i));
      if (Get(Key(i)) != (it == model.end() ? "<absent>" : it->second)) ++bad;
    }
    ASSERT_EQ(bad, 0) << when;
    ASSERT_EQ(engine_->AppliedIndex(), index) << when;
  };
  for (int op = 1; op <= 5000; ++op) {
    const std::string k = Key(static_cast<int>(g() % 40));
    if (g() % 4 == 0) {
      Write(k, "", ++index, true);
      model.erase(k);
    } else {
      const std::string v = "v" + std::to_string(op) + std::string(g() % 120, 'x');
      Write(k, v, ++index);
      model[k] = v;
    }
    if (op % 50 == 0) check_all("during the run");
    if (op % 700 == 0) {
      ASSERT_EQ(Open(Opts(3)), Status::kOk);  // clean restart in the middle of it all
      check_all("after a restart");
    }
  }
  Settle();
  check_all("at the end");
  EXPECT_GE(engine_->GetStats().flushes_done + engine_->GetStats().compactions_done, 1u);
}

// While one thread writes ever-increasing values, readers must never see a value go backwards:
// that would mean a read saw a state in which a flush or compaction had hidden newer data.
TEST_F(LsmTest, ReadersNeverSeeTheStateGoBackwardsWhileFlushingAndCompacting) {
  ASSERT_EQ(Open(Opts(3)), Status::kOk);
  std::atomic<bool> stop{false};
  std::atomic<int> violations{0};
  std::atomic<uint64_t> version{0};
  std::vector<std::thread> readers;
  for (int r = 0; r < 3; ++r) {
    readers.emplace_back([&] {
      uint64_t seen = 0;
      while (!stop) {
        std::string v;
        if (engine_->Get("counter", &v) == Status::kOk) {
          const uint64_t n = std::stoull(v);
          if (n < seen) ++violations;
          seen = std::max(seen, n);
        }
      }
    });
  }
  for (uint64_t i = 1; i <= 1500; ++i) {
    Write("counter", std::to_string(i));
    Write("pad" + std::to_string(i % 30), std::string(120, 'p'));  // keep the memtables turning over
    version = i;
  }
  Settle();
  stop = true;
  for (auto& t : readers) t.join();
  EXPECT_EQ(violations.load(), 0);
  EXPECT_EQ(Get("counter"), "1500");
}
