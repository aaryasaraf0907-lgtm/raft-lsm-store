// KVEngine in SSTable mode: manifest, flush into SSTables, reads through them, compaction, and
// recovery from a crash at every step of flush and compaction.
#include <gtest/gtest.h>

#include <stdlib.h>
#include <sys/resource.h>
#include <sys/stat.h>

#include <atomic>
#include <csignal>
#include <cstdio>
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

#include "storage/checkpoint.h"
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

  // Lays out an exact live set by hand, as consecutive flushes would leave it: table i (from 1) is
  // "00000i.sst" with file number and sequence range {i, i, i}, and the manifest lists them all with
  // flushed_through = the last one. Each table's entries must be sorted by key. No WAL exists, so the
  // engine opened on this directory holds exactly these tables and an empty memtable.
  void BuildTables(const std::vector<std::vector<Entry>>& tables) {
    ManifestState m;
    for (size_t i = 0; i < tables.size(); ++i) {
      const uint64_t n = i + 1;
      const SSTableMeta meta{n, n, n};
      std::unique_ptr<SSTableBuilder> b;
      ASSERT_EQ(SSTableBuilder::Create(dir_ + "/" + KVEngine::SstFileName(n), meta, {}, &b), Status::kOk);
      for (const Entry& e : tables[i]) ASSERT_EQ(b->Add(e), Status::kOk);
      ASSERT_EQ(b->Finish(), Status::kOk);
      m.files.push_back(meta);
    }
    m.flushed_through = tables.size();
    m.next_file_number = tables.size() + 1;
    ASSERT_EQ(StoreManifest(dir_, m), Status::kOk);
  }
  void ResetDir() {
    engine_.reset();
    fs::remove_all(dir_);
    fs::create_directories(dir_);
  }
  // `count` puts "<prefix>0000", "<prefix>0001", ... (sorted), each with `value`.
  static std::vector<Entry> Rows(const std::string& prefix, int count, const std::string& value) {
    std::vector<Entry> out;
    for (int i = 0; i < count; ++i) {
      char key[16];
      std::snprintf(key, sizeof(key), "%04d", i);
      out.push_back(Entry{prefix + key, value, false});
    }
    return out;
  }
  // Looks `key` up in one SSTable on disk, bypassing the engine.
  SSTable::GetResult TableGet(uint64_t file_number, const std::string& key) {
    std::unique_ptr<SSTable> t;
    EXPECT_EQ(SSTable::Open(dir_ + "/" + KVEngine::SstFileName(file_number), file_number, &t), Status::kOk);
    SSTable::GetResult r = SSTable::GetResult::kAbsent;
    std::string v;
    if (t != nullptr) {
      EXPECT_EQ(t->Get(key, &r, &v), Status::kOk);
    }
    return r;
  }
  std::string FileBytes(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(in), {});
  }
  // No compaction is running or due: fewer live tables than the trigger, nothing left to flush,
  // and every replaced input already deleted from disk.
  void WaitUntilCompactionIdle(size_t trigger) {
    ASSERT_TRUE(WaitFor([&] {
      const auto s = engine_->GetStats();
      return s.sstables < trigger && s.frozen_memtables == 0 && Files(".sst").size() == s.sstables;
    }));
  }

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

TEST_F(LsmTest, CompactionKeepsReadsCorrectAndDeletesItsInputs) {
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
  // Let compaction settle (the compactor deletes a merge's inputs AFTER it installs the new live
  // set, so the files on disk only match the manifest once it is idle).
  WaitUntilCompactionIdle(4);
  const ManifestState m = Manifest();
  ASSERT_FALSE(m.files.empty());
  EXPECT_EQ(Files(".sst").size(), m.files.size()) << "compaction inputs must be deleted";
  if (m.files.size() == 1) {  // everything was merged into one table, so no tombstone may remain in it
    std::unique_ptr<SSTable> only;
    ASSERT_EQ(SSTable::Open(dir_ + "/" + KVEngine::SstFileName(m.files[0].file_number), m.files[0].file_number, &only), Status::kOk);
    for (auto it = only->NewIterator(); it->Valid(); it->Next()) EXPECT_FALSE(it->Current().tombstone) << it->Current().key;
  }
  for (int i = 0; i < 50; ++i) {  // and reads are still right after it settled
    auto it = model.find(Key(i));
    ASSERT_EQ(Get(Key(i)), it == model.end() ? "<absent>" : it->second) << Key(i);
  }
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

// ---------------------------------------------------------------------------------------------
// Compaction policy: which run of tables is merged. The live sets below are built by hand
// (BuildTables) so that every test knows the exact tables, sizes and sequence ranges involved.

// The common case the policy exists for: one large older table and a few small newer ones. Only the
// newer ones are merged; the large table is neither rewritten nor touched. The run does not start at
// the oldest table, so a tombstone in it must survive: it still hides the older value in table 1.
TEST_F(LsmTest, CompactionMergesTheSmallNewerTablesAndLeavesTheLargeOldOneAlone) {
  std::vector<Entry> t1 = Rows("a", 200, "old");  // 201 entries
  t1.push_back(Entry{"k", "old", false});
  std::vector<Entry> t2 = Rows("b", 10, "two");  // 11 entries
  t2.push_back(Entry{"k", "", true});            // deletes the value in table 1
  std::vector<Entry> t3 = {Entry{"a0005", "new", false}};  // 11 entries; overwrites table 1
  for (const Entry& e : Rows("c", 10, "three")) t3.push_back(e);
  BuildTables({t1, t2, t3, Rows("d", 10, "four")});  // sizes 201, 11, 11, 10
  const std::string table1 = dir_ + "/" + KVEngine::SstFileName(1);
  const std::string table1_bytes = FileBytes(table1);

  ASSERT_EQ(Open(Opts(4)), Status::kOk);
  ASSERT_TRUE(WaitFor([&] { return engine_->GetStats().compactions_done >= 1; }));
  WaitUntilCompactionIdle(4);
  std::this_thread::sleep_for(100ms);  // a policy that kept going would show up here
  EXPECT_EQ(engine_->GetStats().compactions_done, 1u) << "two live tables are below the trigger";

  const ManifestState m = Manifest();
  ASSERT_EQ(m.files.size(), 2u);
  EXPECT_EQ(m.files[0].file_number, 1u) << "the large old table is still live";
  EXPECT_EQ(m.files[0].min_seq, 1u);
  EXPECT_EQ(m.files[0].max_seq, 1u);
  EXPECT_EQ(m.files[1].min_seq, 2u) << "the output covers exactly the merged run 2..4";
  EXPECT_EQ(m.files[1].max_seq, 4u);
  for (uint64_t n = 1; n <= 4; ++n) EXPECT_NE(m.files[1].file_number, n) << "output numbers come from the shared counter";
  EXPECT_LT(m.files[1].file_number, m.next_file_number);
  EXPECT_EQ(FileBytes(table1), table1_bytes) << "the large table must not be rewritten";
  EXPECT_EQ(Files(".sst").size(), 2u) << "the three merged inputs are deleted";
  EXPECT_EQ(TableGet(m.files[1].file_number, "k"), SSTable::GetResult::kDeleted)
      << "the tombstone must be kept: the run does not start at the oldest table";

  auto check = [&](const char* when) {
    EXPECT_EQ(Get("k"), "<absent>") << when;
    EXPECT_EQ(Get("a0005"), "new") << when;
    EXPECT_EQ(Get("a0006"), "old") << when;
    EXPECT_EQ(Get("b0003"), "two") << when;
    EXPECT_EQ(Get("c0009"), "three") << when;
    EXPECT_EQ(Get("d0000"), "four") << when;
  };
  check("after compaction");
  ASSERT_EQ(Open(Opts(4)), Status::kOk);
  check("after a restart");
  EXPECT_EQ(Manifest().files.size(), 2u);
}

// Tables of similar size: no run of newer tables is as small as the table before it, so the policy
// merges everything. That run starts at the oldest table, so the tombstone and every older version
// of its key are dropped (C7).
TEST_F(LsmTest, CompactionOfAllTablesDropsTombstonesAndOlderVersions) {
  std::vector<Entry> t1 = Rows("a", 10, "one");
  t1.push_back(Entry{"k", "v1", false});
  std::vector<Entry> t2 = Rows("b", 10, "two");
  t2.push_back(Entry{"k", "v2", false});
  std::vector<Entry> t3 = Rows("c", 10, "three");
  t3.push_back(Entry{"k", "", true});
  BuildTables({t1, t2, t3, Rows("d", 10, "four")});

  ASSERT_EQ(Open(Opts(4)), Status::kOk);
  ASSERT_TRUE(WaitFor([&] { return engine_->GetStats().compactions_done >= 1; }));
  WaitUntilCompactionIdle(4);
  const ManifestState m = Manifest();
  ASSERT_EQ(m.files.size(), 1u);
  EXPECT_EQ(m.files[0].min_seq, 1u);
  EXPECT_EQ(m.files[0].max_seq, 4u);
  EXPECT_EQ(TableGet(m.files[0].file_number, "k"), SSTable::GetResult::kAbsent)
      << "with nothing older left, the tombstone and the values it hid are dropped";
  EXPECT_EQ(Get("k"), "<absent>");
  EXPECT_EQ(Get("a0000"), "one");
  EXPECT_EQ(Get("d0009"), "four");
  ASSERT_EQ(Open(Opts(4)), Status::kOk);
  EXPECT_EQ(Get("k"), "<absent>") << "a delete must stay deleted after compaction and restart";
  EXPECT_EQ(Get("b0005"), "two");
}

// The trigger: nothing happens below it, 0 means never, it is a threshold (not an exact count), and
// a value below 2 behaves as 2.
TEST_F(LsmTest, CompactionStartsOnlyAtTheTriggerAndZeroMeansNever) {
  BuildTables({Rows("a", 10, "1"), Rows("b", 10, "2"), Rows("c", 10, "3"), Rows("d", 10, "4"), Rows("e", 10, "5")});
  for (size_t trigger : {size_t{0}, size_t{6}}) {
    ASSERT_EQ(Open(Opts(trigger)), Status::kOk);
    std::this_thread::sleep_for(150ms);
    EXPECT_EQ(engine_->GetStats().compactions_done, 0u) << "trigger " << trigger;
    EXPECT_EQ(engine_->GetStats().sstables, 5u) << "trigger " << trigger;
    EXPECT_EQ(Manifest().files.size(), 5u) << "trigger " << trigger;
  }
  ASSERT_EQ(Open(Opts(5)), Status::kOk);  // exactly at the trigger
  ASSERT_TRUE(WaitFor([&] { return engine_->GetStats().compactions_done >= 1; }));
  WaitUntilCompactionIdle(5);
  EXPECT_EQ(Get("a0000"), "1");
  EXPECT_EQ(Get("e0009"), "5");

  ResetDir();
  BuildTables({Rows("a", 10, "1"), Rows("b", 10, "2")});
  ASSERT_EQ(Open(Opts(1)), Status::kOk);  // documented minimum is 2: two tables are enough
  ASSERT_TRUE(WaitFor([&] { return engine_->GetStats().compactions_done >= 1; }));
  WaitUntilCompactionIdle(2);
  EXPECT_EQ(Manifest().files.size(), 1u);
  EXPECT_EQ(Get("a0003"), "1");
  EXPECT_EQ(Get("b0003"), "2");
}

// The two crash points of compaction, for a run that does NOT include the oldest table. Before the
// manifest edit the old tables stay live and the output is an orphan; after it the output is live and
// the replaced inputs are orphans. Either way the large table is untouched and nothing is lost.
TEST_F(LsmTest, ACrashAtEitherStepOfAPartialCompactionRecoversTheRightTables) {
  for (const std::string point : {"compact:after_output", "compact:after_manifest"}) {
    SCOPED_TRACE(point);
    ResetDir();
    std::vector<Entry> t1 = Rows("a", 200, "old");
    t1.push_back(Entry{"k", "old", false});
    std::vector<Entry> t2 = Rows("b", 10, "two");
    t2.push_back(Entry{"k", "", true});
    BuildTables({t1, t2, Rows("c", 10, "three"), Rows("d", 10, "four")});
    const std::string table1_bytes = FileBytes(dir_ + "/" + KVEngine::SstFileName(1));

    std::atomic<bool> fired{false};
    KVEngine::Options o = Opts(4);
    o.test_crash_hook = [&](const char* p) { return std::string(p) == point && !fired.exchange(true); };
    ASSERT_EQ(Open(o), Status::kOk);
    ASSERT_TRUE(WaitFor([&] { return fired.load(); }));
    engine_.reset();  // the process "died" there

    const ManifestState m = Manifest();
    EXPECT_EQ(Files(".sst").size(), 5u) << "four inputs plus one output are on disk";
    if (point == "compact:after_output") {
      ASSERT_EQ(m.files.size(), 4u) << "the edit never happened: the old tables are still the live set";
    } else {
      ASSERT_EQ(m.files.size(), 2u) << "the edit happened: table 1 and the output";
      EXPECT_EQ(m.files[0].file_number, 1u);
      EXPECT_EQ(m.files[1].min_seq, 2u);
      EXPECT_EQ(m.files[1].max_seq, 4u);
    }
    ASSERT_EQ(Open(Opts(0)), Status::kOk);
    EXPECT_EQ(Files(".sst").size(), m.files.size()) << "startup deletes whatever the manifest does not list";
    EXPECT_EQ(FileBytes(dir_ + "/" + KVEngine::SstFileName(1)), table1_bytes);
    EXPECT_EQ(Get("k"), "<absent>");
    EXPECT_EQ(Get("a0100"), "old");
    EXPECT_EQ(Get("c0001"), "three");
    EXPECT_EQ(Get("d0009"), "four");
  }
}

// A compaction that keeps failing (here its output cannot be written: RLIMIT_FSIZE) must leave the
// live set, the manifest and every input as they were, publish nothing, keep counting failures and
// retrying, and succeed once writing works again.
TEST_F(LsmTest, AFailingCompactionKeepsItsInputsLiveAndPublishesNothing) {
  const std::string big(100, 'x');
  BuildTables({Rows("a", 400, big), Rows("b", 40, big), Rows("c", 40, big), Rows("d", 40, big)});
  const ManifestState before = Manifest();

  struct FileSizeLimit {  // restores the limit even if an assertion below fails
    struct rlimit original;
    explicit FileSizeLimit(rlim_t bytes) {
      std::signal(SIGXFSZ, SIG_IGN);  // exceeding the limit then fails write() with EFBIG
      getrlimit(RLIMIT_FSIZE, &original);
      struct rlimit tiny = {bytes, original.rlim_max};
      setrlimit(RLIMIT_FSIZE, &tiny);
    }
    void Lift() { setrlimit(RLIMIT_FSIZE, &original); }
    ~FileSizeLimit() { Lift(); }
  } limit(4096);  // far smaller than the ~12 KB output of merging tables 2..4

  ASSERT_EQ(Open(Opts(4)), Status::kOk);
  ASSERT_TRUE(WaitFor([&] { return engine_->GetStats().compaction_failures >= 3; }));
  const auto s = engine_->GetStats();
  EXPECT_EQ(s.compactions_done, 0u);
  EXPECT_EQ(s.sstables, 4u);
  const ManifestState during = Manifest();
  ASSERT_EQ(during.files.size(), before.files.size());
  for (size_t i = 0; i < before.files.size(); ++i) EXPECT_EQ(during.files[i].file_number, before.files[i].file_number);
  std::set<std::string> on_disk;
  for (const auto& n : Files(".sst")) on_disk.insert(n);
  EXPECT_EQ(on_disk, (std::set<std::string>{KVEngine::SstFileName(1), KVEngine::SstFileName(2),
                                            KVEngine::SstFileName(3), KVEngine::SstFileName(4)}))
      << "no output may be published, and no input may be deleted";
  EXPECT_EQ(Get("a0399"), big);
  EXPECT_EQ(Get("d0039"), big);

  limit.Lift();
  ASSERT_TRUE(WaitFor([&] { return engine_->GetStats().compactions_done >= 1; }));
  WaitUntilCompactionIdle(4);
  EXPECT_EQ(Manifest().files.size(), 2u);
  EXPECT_EQ(Manifest().files[0].file_number, 1u);
  EXPECT_EQ(Get("a0399"), big);
  EXPECT_EQ(Get("b0000"), big);
}

// The manifest store at the commit point fails BEFORE its rename (MANIFEST.tmp cannot be created: a
// non-empty directory has that name, which startup cleanup cannot remove either). The manifest in the
// directory is then still the old one and names no output, so the output is deleted rather than left
// behind by every retry. The live set, the read view, the manifest and the inputs stay as they were,
// and the compaction completes once storing works again. (A store that fails AFTER its rename cannot
// be injected here; the two states it can leave are the two crash-point states tested above.)
TEST_F(LsmTest, AManifestStoreThatFailsBeforeItsRenameKeepsEverythingAndLeavesNoOutput) {
  std::vector<Entry> t1 = Rows("a", 200, "old");
  t1.push_back(Entry{"k", "old", false});
  std::vector<Entry> t2 = Rows("b", 10, "two");
  t2.push_back(Entry{"k", "", true});
  BuildTables({t1, t2, Rows("c", 10, "three"), Rows("d", 10, "four")});  // sizes 201, 11, 10, 10
  const std::string manifest_before = FileBytes(dir_ + "/MANIFEST");
  const std::string blocker = dir_ + "/MANIFEST.tmp";
  fs::create_directories(blocker);
  { std::ofstream(blocker + "/keep") << "x"; }

  KVEngine::Options o = Opts(4);
  o.flush_retry_ms = 300;  // a quiet window after each failure in which nothing is in flight
  ASSERT_EQ(Open(o), Status::kOk);
  ASSERT_TRUE(WaitFor([&] { return engine_->GetStats().compaction_failures >= 2; }));
  // GetStats takes the engine lock, and a failure is counted and cleaned up under that lock, so the
  // second attempt is fully over here and the next one is still about 300 ms away.
  const auto s = engine_->GetStats();
  EXPECT_EQ(s.compactions_done, 0u);
  EXPECT_EQ(s.sstables, 4u) << "the live set must not change";
  EXPECT_EQ(FileBytes(dir_ + "/MANIFEST"), manifest_before) << "the old manifest is untouched";
  std::set<std::string> on_disk;
  for (const auto& n : Files(".sst")) on_disk.insert(n);
  EXPECT_EQ(on_disk, (std::set<std::string>{KVEngine::SstFileName(1), KVEngine::SstFileName(2),
                                            KVEngine::SstFileName(3), KVEngine::SstFileName(4)}))
      << "inputs kept; an output no manifest names is deleted, not accumulated by retries";
  EXPECT_EQ(Get("k"), "<absent>");
  EXPECT_EQ(Get("a0100"), "old");
  EXPECT_EQ(Get("d0009"), "four");

  fs::remove_all(blocker);
  ASSERT_TRUE(WaitFor([&] { return engine_->GetStats().compactions_done >= 1; }));
  WaitUntilCompactionIdle(4);
  const ManifestState m = Manifest();
  ASSERT_EQ(m.files.size(), 2u);
  EXPECT_EQ(m.files[0].file_number, 1u);
  EXPECT_EQ(Files(".sst").size(), 2u);
  EXPECT_EQ(Get("k"), "<absent>");
  ASSERT_EQ(Open(Opts(0)), Status::kOk);
  EXPECT_EQ(Get("k"), "<absent>");
  EXPECT_EQ(Get("c0005"), "three");
}

// The ambiguous case: the commit-point store reports failure AFTER the new manifest is in place
// (seam "compact:fail_after_manifest_rename", standing for a directory fsync that failed after the
// rename). The running engine must keep its old live set and view, keep every input, and keep the
// output, because the manifest in the directory names it. The retry then succeeds, and its durable
// manifest names neither the inputs nor the kept output, so both are deleted. This fails if the
// output were deleted after the failed store, or if the kept output were never cleaned up.
TEST_F(LsmTest, AnAmbiguousManifestStoreKeepsTheOutputItNamesAndCleansItUpAfterTheRetry) {
  std::vector<Entry> t1 = Rows("a", 200, "old");
  t1.push_back(Entry{"k", "old", false});
  std::vector<Entry> t2 = Rows("b", 10, "two");
  t2.push_back(Entry{"k", "", true});
  BuildTables({t1, t2, Rows("c", 10, "three"), Rows("d", 10, "four")});  // sizes 201, 11, 10, 10

  std::atomic<int> injected{0};
  KVEngine::Options o = Opts(4);
  o.flush_retry_ms = 300;  // a quiet window after the failure in which nothing is in flight
  o.test_crash_hook = [&](const char* p) {
    return std::string(p) == "compact:fail_after_manifest_rename" && injected.fetch_add(1) == 0;
  };
  ASSERT_EQ(Open(o), Status::kOk);
  ASSERT_TRUE(WaitFor([&] { return engine_->GetStats().compaction_failures >= 1; }));
  // A failure is counted and handled under the engine lock, which GetStats takes: it is over here.
  const auto s = engine_->GetStats();
  EXPECT_EQ(s.compactions_done, 0u);
  EXPECT_EQ(s.sstables, 4u) << "the in-memory live set must not take the failed edit";
  const ManifestState on_disk = Manifest();
  ASSERT_EQ(on_disk.files.size(), 2u) << "the failed store's manifest is in place: table 1 and the output";
  const uint64_t kept = on_disk.files[1].file_number;
  const std::string kept_path = dir_ + "/" + KVEngine::SstFileName(kept);
  EXPECT_TRUE(fs::exists(kept_path)) << "a manifest that may survive names the output: it must be kept";
  for (uint64_t n = 1; n <= 4; ++n) {
    EXPECT_TRUE(fs::exists(dir_ + "/" + KVEngine::SstFileName(n))) << "input " << n << " must be kept";
  }
  EXPECT_EQ(Get("k"), "<absent>");
  EXPECT_EQ(Get("a0100"), "old");
  EXPECT_EQ(Get("d0009"), "four");

  ASSERT_TRUE(WaitFor([&] { return engine_->GetStats().compactions_done >= 1; }));  // the retry
  ASSERT_TRUE(WaitFor([&] { return Files(".sst").size() == 2; }))
      << "after the successful retry the inputs and the kept output must all be deleted";
  EXPECT_FALSE(fs::exists(kept_path));
  const ManifestState m = Manifest();
  ASSERT_EQ(m.files.size(), 2u);
  EXPECT_EQ(m.files[0].file_number, 1u);
  EXPECT_NE(m.files[1].file_number, kept) << "the retry wrote a new output under a new number";
  EXPECT_EQ(Get("k"), "<absent>");
  ASSERT_EQ(Open(Opts(0)), Status::kOk);
  EXPECT_EQ(Files(".sst").size(), 2u);
  EXPECT_EQ(Get("k"), "<absent>");
  EXPECT_EQ(Get("a0100"), "old");
  EXPECT_EQ(Get("c0005"), "three");
}

// A crash right after the ambiguous store (here: during the retry, once its new output is written).
// After a real crash either manifest may be the one found, so both are tried:
//  - the new one (the rename survived): it is on disk as left;
//  - the old one (the rename was not yet durable): constructed by putting the old manifest back.
// Either way startup must open, keep exactly the tables that manifest lists, delete the rest, and
// return the right data. With the new manifest this needs the kept output: had it been deleted after
// the failed store, startup would find a listed table missing.
TEST_F(LsmTest, ACrashAfterAnAmbiguousManifestStoreRecoversWithEitherManifest) {
  for (const bool new_manifest_survives : {true, false}) {
    SCOPED_TRACE(new_manifest_survives ? "new manifest survives" : "old manifest survives");
    ResetDir();
    std::vector<Entry> t1 = Rows("a", 200, "old");
    t1.push_back(Entry{"k", "old", false});
    std::vector<Entry> t2 = Rows("b", 10, "two");
    t2.push_back(Entry{"k", "", true});
    BuildTables({t1, t2, Rows("c", 10, "three"), Rows("d", 10, "four")});
    const std::string old_manifest = FileBytes(dir_ + "/MANIFEST");

    std::atomic<int> stage{0};  // 0: before the store, 1: after the injected failure, 2: crashed
    KVEngine::Options o = Opts(4);
    o.test_crash_hook = [&](const char* p) {
      const std::string point(p);
      if (point == "compact:fail_after_manifest_rename" && stage == 0) {
        stage = 1;
        return true;  // the store "fails" after its rename
      }
      if (point == "compact:after_output" && stage == 1) {
        stage = 2;
        return true;  // crash during the retry, once its output is written
      }
      return false;
    };
    ASSERT_EQ(Open(o), Status::kOk);
    ASSERT_TRUE(WaitFor([&] { return stage.load() == 2; }));
    engine_.reset();  // the process "died" there

    const ManifestState left = Manifest();
    ASSERT_EQ(left.files.size(), 2u) << "the failed store's manifest is in the directory";
    const uint64_t kept = left.files[1].file_number;
    EXPECT_TRUE(fs::exists(dir_ + "/" + KVEngine::SstFileName(kept)));
    EXPECT_EQ(Files(".sst").size(), 6u) << "four inputs, the kept output and the retry's output";
    if (!new_manifest_survives) {
      std::ofstream out(dir_ + "/MANIFEST", std::ios::binary | std::ios::trunc);
      out << old_manifest;
    }

    ASSERT_EQ(Open(Opts(0)), Status::kOk);
    const ManifestState m = Manifest();
    if (new_manifest_survives) {
      ASSERT_EQ(m.files.size(), 2u);
      EXPECT_EQ(m.files[0].file_number, 1u);
      EXPECT_EQ(m.files[1].file_number, kept) << "the kept output is live";
    } else {
      ASSERT_EQ(m.files.size(), 4u) << "the four inputs are live";
      EXPECT_FALSE(fs::exists(dir_ + "/" + KVEngine::SstFileName(kept))) << "the output is an orphan";
    }
    EXPECT_EQ(Files(".sst").size(), m.files.size()) << "startup deletes every table the manifest does not list";
    EXPECT_EQ(Get("k"), "<absent>");
    EXPECT_EQ(Get("a0100"), "old");
    EXPECT_EQ(Get("b0003"), "two");
    EXPECT_EQ(Get("c0001"), "three");
    EXPECT_EQ(Get("d0009"), "four");
  }
}

// A compaction still merging when a checkpoint replaces the dataset must not install its output
// into the new dataset. The compactor is held at "compact:after_output" (the hook runs without the
// engine lock) while InstallCheckpoint runs, then released; it must see the epoch change, discard
// its output and leave the checkpoint's single table as the whole live set.
TEST_F(LsmTest, ACompactionOverlappingACheckpointInstallIsDiscarded) {
  BuildTables({Rows("a", 10, "1"), Rows("b", 10, "2"), Rows("c", 10, "3"), Rows("d", 10, "4")});
  std::atomic<bool> reached{false}, release{false};
  KVEngine::Options o = Opts(4);
  o.test_crash_hook = [&](const char* p) {
    if (std::string(p) == "compact:after_output" && !reached.exchange(true)) {
      while (!release) std::this_thread::sleep_for(1ms);
    }
    return false;  // never a crash
  };
  ASSERT_EQ(Open(o), Status::kOk);
  struct Release {  // never leave the compactor blocked, whatever happens below
    std::atomic<bool>& flag;
    ~Release() { flag = true; }
  } release_guard{release};
  ASSERT_TRUE(WaitFor([&] { return reached.load(); }));

  std::string body;
  AppendCheckpointEntry(&body, "ck", "cv");
  ASSERT_EQ(engine_->InstallCheckpoint(CheckpointHeader(1) + body, 77), Status::kOk);
  release = true;

  // The stale output is deleted once the compactor resumes; the checkpoint's table is all that is left.
  ASSERT_TRUE(WaitFor([&] { return Files(".sst").size() == 1; }));
  const ManifestState m = Manifest();
  ASSERT_EQ(m.files.size(), 1u);
  EXPECT_EQ(Files(".sst")[0], KVEngine::SstFileName(m.files[0].file_number));
  EXPECT_EQ(engine_->GetStats().compactions_done, 0u) << "the stale result must not be installed";
  EXPECT_EQ(engine_->GetStats().compaction_failures, 0u)
      << "the epoch check discards it as stale; it is not a failed compaction to be retried";
  EXPECT_EQ(engine_->GetStats().sstables, 1u);
  EXPECT_EQ(Get("ck"), "cv");
  EXPECT_EQ(Get("a0000"), "<absent>") << "the old dataset is gone";
  EXPECT_EQ(engine_->AppliedIndex(), 77u);
  ASSERT_EQ(Open(Opts(4)), Status::kOk);
  EXPECT_EQ(Manifest().files.size(), 1u);
  EXPECT_EQ(Get("ck"), "cv");
  EXPECT_EQ(Get("d0009"), "<absent>");
  EXPECT_EQ(engine_->AppliedIndex(), 77u);
}

// ---------------------------------------------------------------------------------------------
// Flush and checkpoint install: a manifest store that fails AFTER its rename (seams
// "flush:fail_after_manifest_rename" and "install:fail_after_manifest_rename", standing for a
// directory fsync that failed). The manifest then in the directory may be the one found after a
// crash, so no table it names may be deleted.

namespace {
// Identifies one particular file: a deleted and rewritten file can get the freed inode number back,
// but not the same modification time (nanosecond resolution).
std::string FileIdentity(const std::string& path) {
  struct stat st;
  if (::stat(path.c_str(), &st) != 0) return "<missing>";
  // std::filesystem, not st_mtim: that field is spelled st_mtimespec on macOS. The tick count is a
  // 128-bit integer in libc++ (macOS), which std::to_string cannot take, hence the cast.
  std::error_code ec;
  const auto mtime = std::filesystem::last_write_time(path, ec);
  return std::to_string(st.st_ino) + ":" +
         (ec ? std::string("?") : std::to_string(static_cast<long long>(mtime.time_since_epoch().count())));
}
}  // namespace

// The first flush's store fails after its rename. The engine keeps the frozen memtable and its WAL
// (as on any failed flush) and also keeps 000001.sst, which the manifest in the directory names. The
// retry reuses that same file (never deleted and rewritten) and completes the flush.
TEST_F(LsmTest, AnAmbiguousFlushManifestStoreKeepsTheTableAndTheRetryReusesIt) {
  std::atomic<int> injected{0};
  KVEngine::Options o = Opts();
  o.flush_retry_ms = 300;  // a quiet window after the failure
  o.test_crash_hook = [&](const char* p) {
    return std::string(p) == "flush:fail_after_manifest_rename" && injected.fetch_add(1) == 0;
  };
  ASSERT_EQ(Open(o), Status::kOk);
  int written = 0;
  while (engine_->GetStats().active_seq == 1 && written < 400) {  // until memtable 1 is frozen
    Write(Key(written), "v" + std::to_string(written) + std::string(100, 'f'));
    ++written;
  }
  ASSERT_GT(engine_->GetStats().active_seq, 1u);
  ASSERT_TRUE(WaitFor([&] { return engine_->GetStats().flush_failures >= 1; }));
  const auto s = engine_->GetStats();  // the failure is handled under the engine lock: it is over
  EXPECT_EQ(s.flushes_done, 0u);
  EXPECT_EQ(s.sstables, 0u) << "the in-memory live set must not take the failed edit";
  EXPECT_GE(s.frozen_memtables, 1u) << "the memtable stays until its flush is committed";
  const ManifestState on_disk = Manifest();
  ASSERT_EQ(on_disk.files.size(), 1u) << "the failed store's manifest is in place";
  EXPECT_EQ(on_disk.files[0].file_number, 1u);
  EXPECT_EQ(on_disk.flushed_through, 1u);
  const std::string table = dir_ + "/" + KVEngine::SstFileName(1);
  const std::string wal1 = dir_ + "/" + KVEngine::WalFileName(1);
  ASSERT_TRUE(fs::exists(table)) << "a manifest that may survive names this table: it must be kept";
  EXPECT_TRUE(fs::exists(wal1)) << "the WAL is kept until the flush is committed";
  const std::string identity = FileIdentity(table);
  for (int i = 0; i < written; ++i) ASSERT_EQ(Get(Key(i)), "v" + std::to_string(i) + std::string(100, 'f')) << i;

  ASSERT_TRUE(WaitFor([&] { return engine_->GetStats().flushes_done >= 1; }));  // the retry
  ASSERT_TRUE(WaitFor([&] { return !fs::exists(wal1); }));
  EXPECT_EQ(FileIdentity(table), identity) << "the retry must reuse the kept table, not delete and rewrite it";
  EXPECT_EQ(Manifest().files[0].file_number, 1u);
  ASSERT_EQ(Open(Opts()), Status::kOk);
  for (int i = 0; i < written; ++i) ASSERT_EQ(Get(Key(i)), "v" + std::to_string(i) + std::string(100, 'f')) << i;
}

// A crash right after the ambiguous flush store, before any retry. The crash state is taken as a copy
// of the directory while the engine waits to retry (a long retry delay keeps it idle), because the
// retry would rewrite the table and hide a deletion. Either manifest may then be the one found: the
// new one (as left on disk) or the old one, which for the very first flush means no manifest at all
// (constructed by removing it). Both must recover every acknowledged write: from the table with the
// new manifest (the WAL is retired), from the WAL without it (the table is an orphan). Had the table
// been deleted after the failed store, the new manifest would name a missing table and startup
// would refuse.
TEST_F(LsmTest, ACrashAfterAnAmbiguousFlushManifestStoreRecoversWithEitherManifest) {
  for (const bool new_manifest_survives : {true, false}) {
    SCOPED_TRACE(new_manifest_survives ? "new manifest survives" : "old (no) manifest survives");
    ResetDir();
    std::atomic<int> injected{0};
    KVEngine::Options o = Opts();
    o.flush_retry_ms = 5000;  // no retry while the crash state is copied
    o.test_crash_hook = [&](const char* p) {
      return std::string(p) == "flush:fail_after_manifest_rename" && injected.fetch_add(1) == 0;
    };
    ASSERT_EQ(Open(o), Status::kOk);
    int written = 0;
    while (engine_->GetStats().active_seq == 1 && written < 400) {
      Write(Key(written), "v" + std::to_string(written) + std::string(100, 'f'));
      ++written;
    }
    ASSERT_TRUE(WaitFor([&] { return engine_->GetStats().flush_failures >= 1; }));
    // The failure is handled under the engine lock, which GetStats takes: the files are settled.
    const std::string crashed = dir_ + "-crashed";
    fs::remove_all(crashed);
    fs::copy(dir_, crashed, fs::copy_options::recursive);
    engine_.reset();
    fs::remove_all(dir_);
    fs::rename(crashed, dir_);  // the process "died" right after the failed store

    const ManifestState left = Manifest();
    ASSERT_EQ(left.files.size(), 1u);
    EXPECT_EQ(left.flushed_through, 1u);
    EXPECT_TRUE(fs::exists(dir_ + "/" + KVEngine::SstFileName(1)));
    EXPECT_TRUE(fs::exists(dir_ + "/" + KVEngine::WalFileName(1)));
    if (!new_manifest_survives) fs::remove(dir_ + "/MANIFEST");

    ASSERT_EQ(Open(Opts()), Status::kOk);
    if (new_manifest_survives) {
      EXPECT_FALSE(fs::exists(dir_ + "/" + KVEngine::WalFileName(1))) << "the WAL is retired";
      EXPECT_EQ(Manifest().files.size(), 1u);
    } else {
      EXPECT_GT(engine_->recovered_records(), 0u) << "the WAL is replayed";
    }
    for (int i = 0; i < written; ++i) {
      ASSERT_EQ(Get(Key(i)), "v" + std::to_string(i) + std::string(100, 'f')) << "write " << i << " was lost";
    }
  }
}

// InstallCheckpoint's store fails after its rename, then putting the old state back succeeds. The
// install reports the error, the directory holds the old dataset's manifest again, the checkpoint's
// table and new WAL are gone, the old dataset is still served, and a later install works.
TEST_F(LsmTest, AnAmbiguousCheckpointInstallStoreIsUndoneByStoringTheOldStateAgain) {
  BuildTables({Rows("a", 10, "1"), Rows("b", 10, "2")});
  std::atomic<int> injected{0};
  KVEngine::Options o = Opts();
  o.test_crash_hook = [&](const char* p) {
    return std::string(p) == "install:fail_after_manifest_rename" && injected.fetch_add(1) == 0;
  };
  ASSERT_EQ(Open(o), Status::kOk);
  std::string body;
  AppendCheckpointEntry(&body, "ck", "cv");
  const std::string data = CheckpointHeader(1) + body;
  EXPECT_EQ(engine_->InstallCheckpoint(data, 77), Status::kIOError);
  EXPECT_EQ(injected.load(), 2) << "two stores: the install's (made to fail) and the restore (left alone)";
  const ManifestState m = Manifest();
  ASSERT_EQ(m.files.size(), 2u) << "the old dataset's manifest is back";
  EXPECT_EQ(m.files[0].file_number, 1u);
  EXPECT_EQ(m.files[1].file_number, 2u);
  EXPECT_EQ(m.flushed_through, 2u);
  std::set<std::string> ssts;
  for (const auto& n : Files(".sst")) ssts.insert(n);
  EXPECT_EQ(ssts, (std::set<std::string>{KVEngine::SstFileName(1), KVEngine::SstFileName(2)}))
      << "the checkpoint's table is unreferenced once the old state is durable again";
  EXPECT_EQ(Get("a0003"), "1");
  EXPECT_EQ(Get("ck"), "<absent>");
  EXPECT_EQ(engine_->AppliedIndex(), 0u);
  Write("after", "failed-install");
  ASSERT_EQ(engine_->InstallCheckpoint(data, 77), Status::kOk) << "a later install works";
  EXPECT_EQ(Get("ck"), "cv");
  ASSERT_EQ(Open(Opts()), Status::kOk);
  EXPECT_EQ(Get("ck"), "cv");
  EXPECT_EQ(Get("a0003"), "<absent>");
  EXPECT_EQ(engine_->AppliedIndex(), 77u);
}

// Both the install's store and the restore of the old state fail after their renames. Nothing that
// either manifest names may be deleted, so the checkpoint's table is kept, and after a crash either
// dataset is recovered whole: the old one (the manifest left on disk, which is the restored old
// state) or the checkpoint (constructed by putting back the install's manifest, captured in the hook).
TEST_F(LsmTest, ACheckpointInstallWhoseStoreAndRestoreBothFailRecoversEitherDataset) {
  for (const bool checkpoint_survives : {false, true}) {
    SCOPED_TRACE(checkpoint_survives ? "the install's manifest survives" : "the old manifest survives");
    ResetDir();
    BuildTables({Rows("a", 10, "1"), Rows("b", 10, "2")});
    std::string install_manifest;  // the install's manifest, as it was in the directory
    int calls = 0;
    KVEngine::Options o = Opts();
    o.test_crash_hook = [&](const char* p) {
      if (std::string(p) != "install:fail_after_manifest_rename") return false;
      if (++calls == 1) install_manifest = FileBytes(dir_ + "/MANIFEST");
      return true;  // the install's store and the restore both "fail" after their renames
    };
    ASSERT_EQ(Open(o), Status::kOk);
    std::string body;
    AppendCheckpointEntry(&body, "ck", "cv");
    EXPECT_EQ(engine_->InstallCheckpoint(CheckpointHeader(1) + body, 77), Status::kIOError);
    EXPECT_EQ(calls, 2) << "the install's store, then the restore";
    ManifestState installed;
    ASSERT_EQ(DecodeManifest(install_manifest, &installed), Status::kOk);
    ASSERT_EQ(installed.files.size(), 1u) << "the install's manifest names only the checkpoint's table";
    const std::string table = dir_ + "/" + KVEngine::SstFileName(installed.files[0].file_number);
    EXPECT_TRUE(fs::exists(table)) << "a manifest that may survive names it: it must be kept";
    EXPECT_EQ(Manifest().files.size(), 2u) << "the restore's rename put the old state back in the directory";
    std::string v;
    EXPECT_EQ(engine_->Get("a0003", &v), Status::kIOError) << "a fail-stopped engine refuses reads";
    engine_.reset();  // crash

    if (checkpoint_survives) {
      std::ofstream out(dir_ + "/MANIFEST", std::ios::binary | std::ios::trunc);
      out << install_manifest;
    }
    ASSERT_EQ(Open(Opts()), Status::kOk);
    EXPECT_EQ(Files(".sst").size(), Manifest().files.size()) << "every unlisted table is deleted";
    if (checkpoint_survives) {
      EXPECT_EQ(Get("ck"), "cv");
      EXPECT_EQ(Get("a0003"), "<absent>");
      EXPECT_EQ(engine_->AppliedIndex(), 77u);
    } else {
      EXPECT_EQ(Get("a0003"), "1");
      EXPECT_EQ(Get("b0009"), "2");
      EXPECT_EQ(Get("ck"), "<absent>");
      EXPECT_FALSE(fs::exists(table)) << "the checkpoint's table is an orphan";
    }
  }
}

// After the install's store AND the restore both fail, which dataset is durable is unknown, so the
// engine must stop for good. Reads work normally before the failure; afterwards every read, every
// write entry point and both checkpoint calls fail with kIOError (a read STARTED after the failure is
// never served, checked by a reader thread running across it), nothing is written, GetStats reports
// the stop, and a flush and a compaction that were already running when it stopped (held
// here at "flush:after_sst" and "compact:after_output", outside the engine lock) must not store a
// manifest when they resume. Shutting down must not hang. Reopening then recovers either dataset whole: the old one with every write
// acknowledged before the install (the manifest left on disk is the restored old state), or the
// checkpoint (the install's manifest, captured in the hook and put back).
TEST_F(LsmTest, ACheckpointInstallWhoseStoreAndRestoreBothFailStopsTheEngine) {
  for (const bool checkpoint_survives : {false, true}) {
    SCOPED_TRACE(checkpoint_survives ? "the install's manifest survives" : "the old manifest survives");
    ResetDir();
    BuildTables({Rows("a", 10, "1"), Rows("b", 10, "2"), Rows("c", 10, "3"), Rows("d", 10, "4")});
    std::string install_manifest;
    int install_stores = 0;
    std::atomic<bool> flush_held{false}, compaction_held{false}, release_workers{false};
    KVEngine::Options o = Opts(/*compaction_trigger=*/4);  // the four tables start a compaction at once
    o.test_crash_hook = [&](const char* p) {
      const std::string point(p);
      if (point == "install:fail_after_manifest_rename") {
        if (++install_stores == 1) install_manifest = FileBytes(dir_ + "/MANIFEST");
        return true;  // the install's store and the restore both "fail" after their renames
      }
      if (point == "flush:after_sst" && !flush_held.exchange(true)) {
        while (!release_workers) std::this_thread::sleep_for(1ms);  // a flush in flight
      }
      if (point == "compact:after_output" && !compaction_held.exchange(true)) {
        while (!release_workers) std::this_thread::sleep_for(1ms);  // a compaction in flight
      }
      return false;
    };
    ASSERT_EQ(Open(o), Status::kOk);
    struct Release {  // never leave a worker held, whatever happens below
      std::atomic<bool>& flag;
      ~Release() { flag = true; }
    } release_guard{release_workers};
    ASSERT_TRUE(WaitFor([&] { return compaction_held.load(); }));
    const uint64_t first_wal = engine_->GetStats().active_seq;
    int written = 0;
    while (engine_->GetStats().active_seq == first_wal && written < 400) {  // until a memtable is frozen
      Write(Key(written), "v" + std::to_string(written) + std::string(100, 'f'));
      ++written;
    }
    ASSERT_TRUE(WaitFor([&] { return flush_held.load(); }));
    // Reads work normally before the failure.
    EXPECT_EQ(Get("a0003"), "1");
    EXPECT_EQ(Get(Key(0)), "v0" + std::string(100, 'f'));
    EXPECT_EQ(Get("never-written"), "<absent>");
    EXPECT_FALSE(engine_->GetStats().fail_stopped);
    // A reader runs across the failure. Every Get it STARTS after the install has returned must
    // fail; Gets started earlier may return data (they are ordered before the failure).
    std::atomic<bool> install_returned{false}, stop_reader{false};
    std::atomic<int> late_reads{0}, late_reads_not_refused{0};
    std::thread reader([&] {
      while (!stop_reader) {
        const bool after = install_returned.load();
        std::string v;
        const Status s = engine_->Get("a0003", &v);
        if (after) {
          ++late_reads;
          if (s != Status::kIOError) ++late_reads_not_refused;
        } else {
          EXPECT_TRUE(s == Status::kOk || s == Status::kIOError) << "a read before the failure saw a broken state";
        }
      }
    });
    struct JoinReader {
      std::atomic<bool>& stop;
      std::thread& t;
      ~JoinReader() {
        stop = true;
        if (t.joinable()) t.join();
      }
    } join_reader{stop_reader, reader};

    std::string body;
    AppendCheckpointEntry(&body, "ck", "cv");
    const std::string data = CheckpointHeader(1) + body;
    ASSERT_EQ(engine_->InstallCheckpoint(data, 77), Status::kIOError);
    install_returned = true;
    ASSERT_EQ(install_stores, 2) << "the install's store, then the restore";
    ASSERT_TRUE(WaitFor([&] { return late_reads.load() >= 50; })) << "the reader kept running";
    stop_reader = true;
    reader.join();
    EXPECT_EQ(late_reads_not_refused.load(), 0) << "a read started after the fail-stop returned data";
    const std::string manifest_after = FileBytes(dir_ + "/MANIFEST");
    const std::string active_wal = dir_ + "/" + KVEngine::WalFileName(engine_->GetStats().active_seq);
    const uintmax_t wal_size = fs::file_size(active_wal);

    // Fail-stop: nothing is accepted, nothing is reported as done, nothing reaches the WAL.
    EXPECT_EQ(engine_->Put("late", "x"), Status::kIOError);
    EXPECT_EQ(engine_->Delete(Key(0)), Status::kIOError);
    EXPECT_EQ(engine_->PutAt("late", "x", 1000), Status::kIOError);
    EXPECT_EQ(engine_->DeleteAt(Key(0), 1001), Status::kIOError);
    std::string snapshot;
    uint64_t snapshot_index = 0;
    EXPECT_EQ(engine_->Checkpoint(&snapshot, &snapshot_index), Status::kIOError);
    EXPECT_EQ(engine_->InstallCheckpoint(data, 78), Status::kIOError);
    EXPECT_EQ(install_stores, 2) << "a refused install must not even try to store a manifest";
    EXPECT_EQ(fs::file_size(active_wal), wal_size) << "a refused write must not reach the WAL";
    EXPECT_EQ(engine_->AppliedIndex(), 0u);
    // Reads are refused too, for present and absent keys alike, and the stop is reported.
    std::string v;
    EXPECT_EQ(engine_->Get("a0003", &v), Status::kIOError);
    EXPECT_EQ(engine_->Get(Key(0), &v), Status::kIOError);
    EXPECT_EQ(engine_->Get("late", &v), Status::kIOError);
    EXPECT_EQ(engine_->Get("never-written", &v), Status::kIOError);
    EXPECT_TRUE(engine_->GetStats().fail_stopped);

    release_workers = true;
    engine_.reset();  // joins the flusher and the compactor, which resume now; must not hang
    EXPECT_EQ(FileBytes(dir_ + "/MANIFEST"), manifest_after)
        << "the flush and compaction that were running when the engine stopped must not store a manifest";

    if (checkpoint_survives) {
      std::ofstream out(dir_ + "/MANIFEST", std::ios::binary | std::ios::trunc);
      out << install_manifest;
    }
    ASSERT_EQ(Open(Opts()), Status::kOk);
    EXPECT_FALSE(engine_->GetStats().fail_stopped) << "reopening ends the fail-stop";
    EXPECT_EQ(Files(".sst").size(), Manifest().files.size()) << "every unlisted table is deleted";
    if (checkpoint_survives) {
      EXPECT_EQ(Get("ck"), "cv");
      EXPECT_EQ(Get("a0003"), "<absent>");
      EXPECT_EQ(Get(Key(0)), "<absent>") << "the checkpoint replaces the whole dataset";
      EXPECT_EQ(engine_->AppliedIndex(), 77u);
    } else {
      EXPECT_EQ(Get("a0003"), "1");
      EXPECT_EQ(Get("d0009"), "4");
      EXPECT_EQ(Get("ck"), "<absent>");
      for (int i = 0; i < written; ++i) {
        ASSERT_EQ(Get(Key(i)), "v" + std::to_string(i) + std::string(100, 'f')) << "acknowledged write " << i << " was lost";
      }
    }
    EXPECT_EQ(Get("late"), "<absent>") << "a refused write must not appear after recovery";
    Write("after-reopen", "ok");  // the reopened engine works normally
    EXPECT_EQ(Get("after-reopen"), "ok");
  }
}

// Many flush/compaction cycles through the real write path: reads always match a model, the live
// set on disk always matches the manifest, the engine settles below the trigger each time instead of
// spinning, and the applied index survives. Each compaction removes at least one table and each flush
// adds one, so since the last Open (which resets the counters and starts with at most trigger - 1 = 3
// tables, because the previous engine had settled) compactions can exceed flushes by at most 3.
TEST_F(LsmTest, RepeatedFlushAndCompactionCyclesStayCorrectAndSettle) {
  ASSERT_EQ(Open(Opts(4)), Status::kOk);
  std::map<std::string, std::string> model;
  uint64_t index = 0;
  auto check = [&](const std::string& when) {
    for (int i = 0; i < 80; ++i) {
      auto it = model.find(Key(i));
      ASSERT_EQ(Get(Key(i)), it == model.end() ? "<absent>" : it->second) << when << " " << Key(i);
    }
    ASSERT_EQ(engine_->AppliedIndex(), index) << when;
  };
  for (int round = 0; round < 25; ++round) {
    for (int i = 0; i < 40; ++i) {
      const int key = (round * 37 + i * 11) % 80;
      if ((round + i) % 9 == 0) {
        Write(Key(key), "", ++index, true);
        model.erase(Key(key));
      } else {
        const std::string v = "r" + std::to_string(round) + "-" + std::to_string(i) + std::string(70, 'v');
        Write(Key(key), v, ++index);
        model[Key(key)] = v;
      }
    }
    Settle();
    WaitUntilCompactionIdle(4);
    const auto s = engine_->GetStats();
    ASSERT_LE(s.compactions_done, s.flushes_done + 3) << "round " << round;
    ASSERT_EQ(Manifest().files.size(), s.sstables) << "round " << round;
    check("round " + std::to_string(round));
    if (round % 8 == 7) {
      ASSERT_EQ(Open(Opts(4)), Status::kOk);
      check("after a restart in round " + std::to_string(round));
    }
  }
  EXPECT_GE(engine_->GetStats().compactions_done + engine_->GetStats().flushes_done, 1u);
}
