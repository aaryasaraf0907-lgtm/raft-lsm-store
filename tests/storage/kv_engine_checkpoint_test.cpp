// Checkpoints: a consistent copy of an engine's whole dataset (for Raft snapshots) and the atomic
// replacement of a dataset by one.
#include <gtest/gtest.h>

#include <stdlib.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "storage/checkpoint.h"
#include "storage/kv_engine.h"
#include "storage/manifest.h"

using namespace raftkv;
using namespace std::chrono_literals;
namespace fs = std::filesystem;

namespace {

bool WaitFor(const std::function<bool()>& pred, std::chrono::milliseconds timeout = 15000ms) {
  const auto end = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < end) {
    if (pred()) return true;
    std::this_thread::sleep_for(2ms);
  }
  return pred();
}

struct Dir {
  std::string path;
  Dir() {
    char tmpl[] = "/tmp/raftkv_ckpt_XXXXXX";
    path = ::mkdtemp(tmpl);
  }
  ~Dir() {
    std::error_code ec;
    fs::remove_all(path, ec);
  }
};

KVEngine::Options Opts(const std::string& dir, size_t compaction_trigger = 0) {
  KVEngine::Options o;
  o.dir = dir;
  o.sync_wal = false;
  o.memtable_bytes = 2048;
  o.use_sstables = true;
  o.compaction_trigger = compaction_trigger;
  o.flush_retry_ms = 5;
  return o;
}

void Write(KVEngine* e, const std::string& k, const std::string& v, uint64_t index, bool del = false) {
  for (int i = 0; i < 5000; ++i) {
    Status s = del ? e->DeleteAt(k, index) : e->PutAt(k, v, index);
    if (s == Status::kOk) return;
    std::this_thread::sleep_for(2ms);
  }
  FAIL() << "write never succeeded";
}

std::map<std::string, std::string> Decode(const std::string& data) {
  std::vector<std::pair<std::string, std::string>> pairs;
  EXPECT_TRUE(ParseCheckpoint(data, &pairs));
  return std::map<std::string, std::string>(pairs.begin(), pairs.end());
}

std::string Key(int i) { return "key" + std::to_string(1000 + i); }

std::map<std::string, std::string> Dump(KVEngine* e, int keys) {
  std::map<std::string, std::string> m;
  for (int i = 0; i < keys; ++i) {
    std::string v;
    if (e->Get(Key(i), &v) == Status::kOk) m[Key(i)] = v;
  }
  return m;
}

}  // namespace

TEST(CheckpointFormat, RoundTripsAndRejectsEverythingMalformed) {
  std::string data = CheckpointHeader(3);
  AppendCheckpointEntry(&data, "a", "1");
  AppendCheckpointEntry(&data, std::string("b\0b", 3), std::string("\xff\0", 2));
  AppendCheckpointEntry(&data, "c", "");
  std::vector<std::pair<std::string, std::string>> out;
  ASSERT_TRUE(ParseCheckpoint(data, &out));
  ASSERT_EQ(out.size(), 3u);
  EXPECT_EQ(out[1].first, std::string("b\0b", 3));
  EXPECT_EQ(out[2].second, "");
  for (size_t len = 0; len < data.size(); ++len) EXPECT_FALSE(ParseCheckpoint(data.substr(0, len), &out)) << len;
  EXPECT_FALSE(ParseCheckpoint(data + "x", &out)) << "trailing bytes";
  std::string unordered = CheckpointHeader(2);
  AppendCheckpointEntry(&unordered, "b", "1");
  AppendCheckpointEntry(&unordered, "a", "2");
  EXPECT_FALSE(ParseCheckpoint(unordered, &out)) << "keys must increase";
  std::string reserved = CheckpointHeader(1);
  AppendCheckpointEntry(&reserved, std::string(kReservedKeyPrefix, sizeof(kReservedKeyPrefix) - 1) + "x", "1");
  EXPECT_FALSE(ParseCheckpoint(reserved, &out)) << "reserved keys must not appear";
}

// Every layer holds some of the data: SSTables (old), frozen memtables, the active memtable. The
// newest version of each key wins and deleted keys are absent.
TEST(Checkpoint, MergesEveryLayerNewestWinsAndLeavesOutDeletedAndReservedKeys) {
  Dir d;
  std::unique_ptr<KVEngine> e;
  ASSERT_EQ(KVEngine::Open(Opts(d.path), &e), Status::kOk);
  std::map<std::string, std::string> model;
  uint64_t index = 0;
  std::mt19937_64 g(5);
  for (int op = 0; op < 900; ++op) {
    const std::string k = Key(static_cast<int>(g() % 60));
    if (g() % 5 == 0) {
      Write(e.get(), k, "", ++index, true);
      model.erase(k);
    } else {
      const std::string v = "v" + std::to_string(op) + std::string(g() % 100, 'x');
      Write(e.get(), k, v, ++index);
      model[k] = v;
    }
  }
  ASSERT_GE(e->GetStats().sstables + e->GetStats().frozen_memtables, 2u) << "the data must span several layers";
  std::string data;
  uint64_t applied = 0;
  ASSERT_EQ(e->Checkpoint(&data, &applied), Status::kOk);
  EXPECT_EQ(applied, index);
  EXPECT_EQ(Decode(data), model);
}

TEST(Checkpoint, WritesAfterwardsDoNotChangeACheckpointAlreadyTaken) {
  Dir d;
  std::unique_ptr<KVEngine> e;
  ASSERT_EQ(KVEngine::Open(Opts(d.path), &e), Status::kOk);
  for (uint64_t i = 1; i <= 100; ++i) Write(e.get(), Key(static_cast<int>(i)), "old", i);
  std::string data;
  uint64_t applied = 0;
  ASSERT_EQ(e->Checkpoint(&data, &applied), Status::kOk);
  for (uint64_t i = 101; i <= 200; ++i) Write(e.get(), Key(static_cast<int>(i - 100)), "new", i);
  EXPECT_EQ(applied, 100u);
  auto m = Decode(data);
  EXPECT_EQ(m.size(), 100u);
  EXPECT_EQ(m[Key(5)], "old");
}

TEST(Checkpoint, AnEmptyEngineHasAnEmptyCheckpoint) {
  Dir d;
  std::unique_ptr<KVEngine> e;
  ASSERT_EQ(KVEngine::Open(Opts(d.path), &e), Status::kOk);
  std::string data;
  uint64_t applied = 99;
  ASSERT_EQ(e->Checkpoint(&data, &applied), Status::kOk);
  EXPECT_EQ(applied, 0u);
  EXPECT_TRUE(Decode(data).empty());
}

TEST(InstallCheckpoint, ReplacesTheWholeDatasetAndSurvivesARestart) {
  Dir src_dir, dst_dir;
  std::unique_ptr<KVEngine> src, dst;
  ASSERT_EQ(KVEngine::Open(Opts(src_dir.path), &src), Status::kOk);
  ASSERT_EQ(KVEngine::Open(Opts(dst_dir.path), &dst), Status::kOk);
  std::map<std::string, std::string> model;
  for (uint64_t i = 1; i <= 300; ++i) {
    Write(src.get(), Key(static_cast<int>(i % 70)), "src-" + std::to_string(i) + std::string(60, 's'), i);
    model[Key(static_cast<int>(i % 70))] = "src-" + std::to_string(i) + std::string(60, 's');
  }
  for (uint64_t i = 1; i <= 400; ++i) Write(dst.get(), "stale" + std::to_string(i % 90), std::string(80, 'd'), i);  // other data
  std::string data;
  uint64_t applied = 0;
  ASSERT_EQ(src->Checkpoint(&data, &applied), Status::kOk);
  ASSERT_EQ(dst->InstallCheckpoint(data, applied), Status::kOk);
  EXPECT_EQ(Dump(dst.get(), 70), model);
  std::string v;
  EXPECT_EQ(dst->Get("stale3", &v), Status::kNotFound) << "the old dataset must be gone";
  EXPECT_EQ(dst->AppliedIndex(), applied);
  // Writes continue normally after the install, with indexes above the snapshot's.
  Write(dst.get(), "after", "install", applied + 1);
  EXPECT_EQ(dst->Get("after", &v), Status::kOk);
  const uint64_t last = dst->AppliedIndex();
  dst.reset();
  ASSERT_EQ(KVEngine::Open(Opts(dst_dir.path), &dst), Status::kOk);
  EXPECT_EQ(dst->AppliedIndex(), last);
  EXPECT_EQ(Dump(dst.get(), 70), model);
  EXPECT_EQ(dst->Get("after", &v), Status::kOk);
  EXPECT_EQ(dst->Get("stale3", &v), Status::kNotFound);
  // No file of the old dataset is left behind: only the files the manifest names.
  ManifestState m;
  bool found = false;
  ASSERT_EQ(LoadManifest(dst_dir.path, &m, &found), Status::kOk);
  size_t ssts = 0;
  for (const auto& de : fs::directory_iterator(dst_dir.path)) {
    if (de.path().extension() == ".sst") ++ssts;
  }
  EXPECT_EQ(ssts, m.files.size());
}

TEST(InstallCheckpoint, RejectsMalformedDataAndLeavesTheDatasetAlone) {
  Dir d;
  std::unique_ptr<KVEngine> e;
  ASSERT_EQ(KVEngine::Open(Opts(d.path), &e), Status::kOk);
  Write(e.get(), "keep", "me", 1);
  std::string bad = CheckpointHeader(2);
  AppendCheckpointEntry(&bad, "b", "1");
  AppendCheckpointEntry(&bad, "a", "2");
  EXPECT_EQ(e->InstallCheckpoint(bad, 50), Status::kCorruption);
  EXPECT_EQ(e->InstallCheckpoint(CheckpointHeader(5), 50), Status::kCorruption);
  std::string v;
  EXPECT_EQ(e->Get("keep", &v), Status::kOk);
  EXPECT_EQ(e->AppliedIndex(), 1u);
  KVEngine::Options legacy;
  legacy.dir = d.path + "/legacy";
  legacy.sync_wal = false;
  std::unique_ptr<KVEngine> old_engine;
  ASSERT_EQ(KVEngine::Open(legacy, &old_engine), Status::kOk);
  EXPECT_EQ(old_engine->InstallCheckpoint(CheckpointHeader(0), 1), Status::kInvalidArgument) << "needs SSTable mode";
}

// A crash at each step of the replacement leaves the old dataset or the new one, never a mix.
TEST(InstallCheckpoint, ACrashLeavesTheOldDatasetOrTheNewOneNeverAMix) {
  for (const char* point : {"install:after_sst", "install:after_manifest"}) {
    SCOPED_TRACE(point);
    Dir src_dir, dst_dir;
    std::unique_ptr<KVEngine> src, dst;
    ASSERT_EQ(KVEngine::Open(Opts(src_dir.path), &src), Status::kOk);
    for (uint64_t i = 1; i <= 120; ++i) Write(src.get(), Key(static_cast<int>(i)), "new" + std::string(60, 'n'), i);
    std::string data;
    uint64_t applied = 0;
    ASSERT_EQ(src->Checkpoint(&data, &applied), Status::kOk);
    const std::map<std::string, std::string> new_model = Decode(data);

    std::atomic<bool> fired{false};
    KVEngine::Options o = Opts(dst_dir.path);
    o.test_crash_hook = [&](const char* p) { return std::string(p) == point && !fired.exchange(true); };
    ASSERT_EQ(KVEngine::Open(o, &dst), Status::kOk);
    std::map<std::string, std::string> old_model;
    for (uint64_t i = 1; i <= 150; ++i) {
      const std::string k = "old" + std::to_string(i % 40);
      Write(dst.get(), k, "old" + std::string(70, 'o'), i);
      old_model[k] = "old" + std::string(70, 'o');
    }
    ASSERT_TRUE(WaitFor([&] { return dst->GetStats().frozen_memtables == 0; }));
    EXPECT_EQ(dst->InstallCheckpoint(data, applied), Status::kIOError) << "the crash point fires";
    ASSERT_TRUE(fired);
    dst.reset();  // the process "died"

    ASSERT_EQ(KVEngine::Open(Opts(dst_dir.path), &dst), Status::kOk);
    const bool is_new = std::string(point) == "install:after_manifest";
    if (is_new) {  // the manifest edit committed: the new dataset, and nothing of the old one
      EXPECT_EQ(Dump(dst.get(), 130), new_model);
      std::string v;
      EXPECT_EQ(dst->Get("old3", &v), Status::kNotFound);
      EXPECT_EQ(dst->AppliedIndex(), applied);
    } else {  // not committed: exactly the old dataset
      for (const auto& kv : old_model) {
        std::string v;
        ASSERT_EQ(dst->Get(kv.first, &v), Status::kOk) << kv.first;
        EXPECT_EQ(v, kv.second);
      }
      std::string v;
      EXPECT_EQ(dst->Get(Key(7), &v), Status::kNotFound) << "none of the new data may leak in";
    }
    size_t ssts = 0;
    ManifestState m;
    bool found = false;
    ASSERT_EQ(LoadManifest(dst_dir.path, &m, &found), Status::kOk);
    ASSERT_TRUE(WaitFor([&] { return dst->GetStats().frozen_memtables == 0; }));
    for (const auto& de : fs::directory_iterator(dst_dir.path)) {
      if (de.path().extension() == ".sst") ++ssts;
    }
    EXPECT_EQ(ssts, LoadManifest(dst_dir.path, &m, &found) == Status::kOk ? m.files.size() : 0u) << "orphans must be cleaned";
  }
}

// An install while flushes, compactions and writes are running: whatever they were doing must not
// leak into, or corrupt, the new dataset.
TEST(InstallCheckpoint, WinsOverFlushesAndCompactionsThatAreRunning) {
  Dir src_dir, dst_dir;
  std::unique_ptr<KVEngine> src, dst;
  ASSERT_EQ(KVEngine::Open(Opts(src_dir.path), &src), Status::kOk);
  ASSERT_EQ(KVEngine::Open(Opts(dst_dir.path, /*compaction_trigger=*/3), &dst), Status::kOk);
  for (uint64_t i = 1; i <= 200; ++i) Write(src.get(), Key(static_cast<int>(i % 50)), "src" + std::string(50, 'a'), i);
  std::string data;
  uint64_t applied = 0;
  ASSERT_EQ(src->Checkpoint(&data, &applied), Status::kOk);
  const auto model = Decode(data);

  std::atomic<bool> stop{false};
  std::atomic<uint64_t> next_index{1};
  std::thread writer([&] {  // keeps the destination busy: memtables fill, flush and compact
    while (!stop) {
      const uint64_t i = next_index.fetch_add(1);
      if (dst->PutAt("busy" + std::to_string(i % 200), std::string(150, 'b'), i) != Status::kOk) {
        next_index.fetch_sub(1);
        std::this_thread::sleep_for(1ms);
      }
    }
  });
  ASSERT_TRUE(WaitFor([&] { return dst->GetStats().flushes_done >= 3; }));
  ASSERT_EQ(dst->InstallCheckpoint(data, applied + 100000), Status::kOk);  // index above anything the writer reached
  stop = true;
  writer.join();
  // Writes after the install fail until their index exceeds the installed one; the writer's indexes
  // are below it, so nothing of the old dataset can appear.
  EXPECT_EQ(Dump(dst.get(), 50), model);
  std::string v;
  EXPECT_EQ(dst->Get("busy7", &v), Status::kNotFound);
  ASSERT_TRUE(WaitFor([&] { return dst->GetStats().frozen_memtables == 0; }));
  dst.reset();
  ASSERT_EQ(KVEngine::Open(Opts(dst_dir.path), &dst), Status::kOk);
  EXPECT_EQ(Dump(dst.get(), 50), model);
  EXPECT_EQ(dst->Get("busy7", &v), Status::kNotFound);
}

// The applied-index record is stored under a reserved key that sorts among the user keys. Wherever
// it falls (before all of them, between them, after all of them, or with no user keys at all), the
// installed dataset must hold it, because it is the only place the engine's index survives.
TEST(InstallCheckpoint, StoresTheAppliedIndexWhereverItSortsAmongTheKeys) {
  const std::vector<std::vector<std::string>> key_sets = {
      {},
      {std::string("\0a", 2)},                                    // sorts BEFORE the reserved key
      {std::string("\0z", 2)},                                    // sorts AFTER it, with nothing before
      {std::string("\0a", 2), std::string("\0z", 2), "b"},       // on both sides of it
      {"b", "c"},                                                  // ordinary keys, all after it
  };
  for (size_t n = 0; n < key_sets.size(); ++n) {
    SCOPED_TRACE("key set " + std::to_string(n));
    Dir d;
    std::unique_ptr<KVEngine> e;
    ASSERT_EQ(KVEngine::Open(Opts(d.path), &e), Status::kOk);
    std::string data = CheckpointHeader(key_sets[n].size());
    for (const std::string& k : key_sets[n]) AppendCheckpointEntry(&data, k, "value-of-" + k);
    ASSERT_EQ(e->InstallCheckpoint(data, 77), Status::kOk);
    EXPECT_EQ(e->AppliedIndex(), 77u);
    e.reset();
    ASSERT_EQ(KVEngine::Open(Opts(d.path), &e), Status::kOk);
    EXPECT_EQ(e->AppliedIndex(), 77u) << "the index must survive a restart";
    for (const std::string& k : key_sets[n]) {
      std::string v;
      ASSERT_EQ(e->Get(k, &v), Status::kOk);
      EXPECT_EQ(v, "value-of-" + k);
    }
    std::string again;
    uint64_t applied = 0;
    ASSERT_EQ(e->Checkpoint(&again, &applied), Status::kOk);
    EXPECT_EQ(applied, 77u);
    EXPECT_EQ(again, data) << "a checkpoint of an installed checkpoint must be identical";
  }
}
