#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "storage/kv_engine.h"

using raftkv::Entry;
using raftkv::KVEngine;
using raftkv::KVIterator;
using raftkv::Status;
using raftkv::WAL;

namespace {

std::string Key(int i) {
  char buf[16];
  std::snprintf(buf, sizeof(buf), "k%05d", i);
  return buf;
}

bool WaitFor(const std::function<bool()>& pred, int timeout_ms = 5000) {
  for (int waited = 0; waited < timeout_ms; waited += 5) {
    if (pred()) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  return pred();
}

// Stand-in for the real SSTable writer: records what it was given and publishes
// atomically (temp file + rename), like the real one must. Can be blocked/failed.
class FakeSst {
 public:
  KVEngine::FlushWriter Writer() {
    return [this](KVIterator& it, const std::string& path, uint64_t seq) { return Write(it, path, seq); };
  }
  void Block(bool b) {
    std::lock_guard<std::mutex> lock(mu_);
    blocked_ = b;
    cv_.notify_all();
  }
  void Fail(bool f) {
    std::lock_guard<std::mutex> lock(mu_);
    fail_ = f;
  }
  std::map<uint64_t, std::vector<Entry>> Files() {
    std::lock_guard<std::mutex> lock(mu_);
    return files_;
  }
  std::vector<uint64_t> CallOrder() {
    std::lock_guard<std::mutex> lock(mu_);
    return calls_;
  }

 private:
  Status Write(KVIterator& it, const std::string& path, uint64_t seq) {
    {
      std::unique_lock<std::mutex> lock(mu_);
      calls_.push_back(seq);
      cv_.wait(lock, [&] { return !blocked_; });
      if (fail_) return Status::kIOError;
    }
    std::vector<Entry> entries;
    for (; it.Valid(); it.Next()) entries.push_back(it.Current());
    const std::string tmp = path + ".tmp";
    { std::ofstream(tmp) << "sst"; }
    std::filesystem::rename(tmp, path);
    std::lock_guard<std::mutex> lock(mu_);
    files_[seq] = std::move(entries);
    return Status::kOk;
  }

  std::mutex mu_;
  std::condition_variable cv_;
  bool blocked_ = false;
  bool fail_ = false;
  std::map<uint64_t, std::vector<Entry>> files_;
  std::vector<uint64_t> calls_;
};

class FlushTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::string tmpl = (std::filesystem::temp_directory_path() / "raftkv_flush_XXXXXX").string();
    ASSERT_NE(::mkdtemp(&tmpl[0]), nullptr);
    dir_ = tmpl;
  }
  void TearDown() override {
    sst_.Block(false);  // never leave the flush thread stuck in the writer
    sst2_.Block(false);
    engine_.reset();
    std::filesystem::remove_all(dir_);
  }

  KVEngine::Options Opts(bool with_writer = true, size_t threshold = 2048) {
    KVEngine::Options o;
    o.dir = dir_;
    o.memtable_bytes = threshold;
    o.sync_wal = false;  // keep the stress tests quick; durability is covered elsewhere
    o.flush_retry_ms = 10;
    if (with_writer) o.flush_writer = sst_.Writer();
    return o;
  }
  void Open(const KVEngine::Options& o) {
    engine_.reset();
    ASSERT_EQ(KVEngine::Open(o, &engine_), Status::kOk);
  }
  bool Exists(const std::string& name) { return std::filesystem::exists(dir_ + "/" + name); }
  std::string Wal(uint64_t s) { return KVEngine::WalFileName(s); }
  std::string Sst(uint64_t s) { return KVEngine::SstFileName(s); }
  bool Quiesced() { return engine_->GetStats().frozen_memtables == 0; }

  // What the read path will do once SSTables are wired in: memtables first, then
  // the flushed files newest-first (a tombstone ends the search).
  std::string Resolve(const std::string& k) {
    std::string v;
    if (engine_->Get(k, &v) == Status::kOk) return v;
    auto files = sst_.Files();
    for (auto it = files.rbegin(); it != files.rend(); ++it) {
      for (const Entry& e : it->second) {
        if (e.key == k) return e.tombstone ? "<none>" : e.value;
      }
    }
    return "<none>";
  }
  void WriteWalFile(uint64_t seq, const std::vector<std::pair<std::string, std::string>>& puts) {
    std::unique_ptr<WAL> w;
    ASSERT_EQ(WAL::Open(dir_ + "/" + Wal(seq), &w), Status::kOk);
    for (auto& kv : puts) ASSERT_EQ(w->AppendPut(kv.first, kv.second), Status::kOk);
  }

  std::string dir_;
  FakeSst sst_;
  FakeSst sst2_;  // writer for the engine opened after a simulated crash
  std::unique_ptr<KVEngine> engine_;
};

}  // namespace

TEST_F(FlushTest, WithoutAWriterTheEngineNeverFreezes) {
  Open(Opts(/*with_writer=*/false, 512));
  for (int i = 0; i < 300; ++i) ASSERT_EQ(engine_->Put(Key(i), std::string(40, 'v')), Status::kOk);
  EXPECT_EQ(engine_->GetStats().frozen_memtables, 0u);
  EXPECT_TRUE(Exists(Wal(1)));
  EXPECT_FALSE(Exists(Wal(2)));
  for (int i = 0; i < 300; ++i) ASSERT_EQ(Resolve(Key(i)), std::string(40, 'v'));
}

TEST_F(FlushTest, FullMemtableIsFrozenFlushedAndItsWalRetired) {
  Open(Opts());
  std::map<std::string, std::string> model;
  for (int i = 0; i < 300; ++i) {
    std::string v = "v" + std::to_string(i) + std::string(40, 'x');
    ASSERT_EQ(engine_->Put(Key(i), v), Status::kOk);
    model[Key(i)] = v;
    if (i % 7 == 0) {  // overwrites
      v += "-new";
      ASSERT_EQ(engine_->Put(Key(i), v), Status::kOk);
      model[Key(i)] = v;
    }
  }
  ASSERT_TRUE(WaitFor([&] { return Quiesced(); }));

  auto stats = engine_->GetStats();
  EXPECT_GE(stats.flushes_done, 5u);
  auto order = sst_.CallOrder();
  EXPECT_TRUE(std::is_sorted(order.begin(), order.end()));                         // flushed oldest first
  EXPECT_TRUE(std::adjacent_find(order.begin(), order.end()) == order.end());      // each exactly once
  for (auto& [seq, entries] : sst_.Files()) {
    for (size_t i = 1; i < entries.size(); ++i) ASSERT_LT(entries[i - 1].key, entries[i].key);  // sorted, unique
    EXPECT_TRUE(Exists(Sst(seq)));
    EXPECT_FALSE(Exists(Wal(seq))) << "WAL " << seq << " must be retired after its flush";
  }
  EXPECT_TRUE(Exists(Wal(stats.active_seq)));
  for (auto& [k, v] : model) ASSERT_EQ(Resolve(k), v) << k;
}

TEST_F(FlushTest, TombstonesAreHandedToTheWriter) {
  Open(Opts());
  ASSERT_EQ(engine_->Put("victim", "old"), Status::kOk);
  ASSERT_EQ(engine_->Delete("victim"), Status::kOk);
  for (int i = 0; engine_->GetStats().flushes_done == 0 && i < 500; ++i) {
    ASSERT_EQ(engine_->Put(Key(i), std::string(40, 'v')), Status::kOk);
    WaitFor([&] { return Quiesced(); }, 20);
  }
  ASSERT_TRUE(WaitFor([&] { return engine_->GetStats().flushes_done >= 1; }));
  bool found = false;
  for (auto& [seq, entries] : sst_.Files()) {
    for (const Entry& e : entries) {
      if (e.key == "victim") {
        found = true;
        EXPECT_TRUE(e.tombstone);
        EXPECT_TRUE(e.value.empty());
      }
    }
  }
  EXPECT_TRUE(found);
}

TEST_F(FlushTest, FrozenMemtableStaysReadableWhileItsFlushIsStuck) {
  Open(Opts());
  sst_.Block(true);
  std::map<std::string, std::string> model;
  for (int i = 0; engine_->GetStats().frozen_memtables == 0 && i < 500; ++i) {
    model[Key(i)] = "val" + std::to_string(i);
    ASSERT_EQ(engine_->Put(Key(i), model[Key(i)]), Status::kOk);
  }
  ASSERT_GE(engine_->GetStats().frozen_memtables, 1u);
  for (auto& [k, v] : model) {
    std::string got;
    ASSERT_EQ(engine_->Get(k, &got), Status::kOk) << k;
    ASSERT_EQ(got, v);
  }
  EXPECT_TRUE(Exists(Wal(1)));  // not retired before the SSTable is published
}

TEST_F(FlushTest, FailedFlushKeepsTheWalAndRetriesUntilItWorks) {
  Open(Opts());
  sst_.Fail(true);
  for (int i = 0; engine_->GetStats().frozen_memtables == 0 && i < 500; ++i)
    ASSERT_EQ(engine_->Put(Key(i), std::string(40, 'v')), Status::kOk);
  ASSERT_TRUE(WaitFor([&] { return engine_->GetStats().flush_failures >= 2; }));
  EXPECT_TRUE(engine_->GetStats().flush_failing);
  EXPECT_TRUE(Exists(Wal(1)));
  EXPECT_FALSE(Exists(Sst(1)));

  sst_.Fail(false);
  ASSERT_TRUE(WaitFor([&] { return Quiesced(); }));
  EXPECT_TRUE(Exists(Sst(1)));
  EXPECT_FALSE(Exists(Wal(1)));
  EXPECT_FALSE(engine_->GetStats().flush_failing);
}

TEST_F(FlushTest, WritersWaitWhenFlushingFallsBehindAndLoseNothing) {
  auto o = Opts(true, 1024);
  o.max_frozen = 1;
  Open(o);
  sst_.Block(true);
  std::atomic<int> done{0};
  constexpr int kN = 80;
  std::thread writer([&] {
    for (int i = 0; i < kN; ++i) {
      EXPECT_EQ(engine_->Put(Key(i), std::string(60, 'v')), Status::kOk);
      ++done;
    }
  });
  int last = -1;
  ASSERT_TRUE(WaitFor([&] {  // wait until the writer has stalled
    int now = done.load();
    bool stalled = now == last && now > 0 && now < kN;
    last = now;
    std::this_thread::sleep_for(std::chrono::milliseconds(60));
    return stalled;
  }, 8000));
  EXPECT_LT(done.load(), kN);
  sst_.Block(false);
  writer.join();
  ASSERT_TRUE(WaitFor([&] { return Quiesced(); }));
  for (int i = 0; i < kN; ++i) ASSERT_EQ(Resolve(Key(i)), std::string(60, 'v'));
}

TEST_F(FlushTest, WriteFailsFastInsteadOfHangingWhenFlushingIsBroken) {
  auto o = Opts(true, 1024);
  o.max_frozen = 1;
  Open(o);
  sst_.Fail(true);
  std::vector<std::string> acked;
  Status last = Status::kOk;
  for (int i = 0; i < 2000 && last == Status::kOk; ++i) {
    last = engine_->Put(Key(i), std::string(60, 'v'));
    if (last == Status::kOk) acked.push_back(Key(i));
  }
  EXPECT_EQ(last, Status::kIOError);  // reached without hanging
  EXPECT_FALSE(acked.empty());
  for (auto& k : acked) {  // everything acknowledged is still readable and in a WAL
    std::string v;
    ASSERT_EQ(engine_->Get(k, &v), Status::kOk) << k;
  }
  EXPECT_TRUE(Exists(Wal(1)));

  sst_.Fail(false);  // flushing heals, writes work again
  EXPECT_TRUE(WaitFor([&] { return engine_->Put("after", "x") == Status::kOk; }));
}

TEST_F(FlushTest, RestartReplaysUnflushedWalsThenFlushesThemInOrder) {
  auto o = Opts(true, 1024);
  o.max_frozen = 100;  // let it rotate many times without blocking
  Open(o);
  sst_.Fail(true);  // nothing ever gets flushed in the first run
  std::map<std::string, std::string> model;
  for (int i = 0; i < 120; ++i) {
    model[Key(i)] = "v" + std::to_string(i) + std::string(30, 'y');
    ASSERT_EQ(engine_->Put(Key(i), model[Key(i)]), Status::kOk);
  }
  const uint64_t seqs = engine_->GetStats().active_seq;
  ASSERT_GE(seqs, 5u);
  engine_.reset();  // "crash": destructor must return promptly with flushes failing

  sst2_.Block(true);  // hold flushing until the recovered data has been checked in memory
  auto o2 = Opts(false, 1024);
  o2.flush_writer = sst2_.Writer();
  Open(o2);
  for (auto& [k, v] : model) {  // all data back, from several WALs
    std::string got;
    ASSERT_EQ(engine_->Get(k, &got), Status::kOk) << k;
    ASSERT_EQ(got, v);
  }
  sst2_.Block(false);
  ASSERT_TRUE(WaitFor([&] { return Quiesced(); }));
  auto order = sst2_.CallOrder();
  EXPECT_FALSE(order.empty());
  EXPECT_TRUE(std::is_sorted(order.begin(), order.end()));
  for (uint64_t s : order) {
    EXPECT_TRUE(Exists(Sst(s)));
    EXPECT_FALSE(Exists(Wal(s)));
  }
}

TEST_F(FlushTest, WalWhoseSstableExistsIsRetiredNotReplayed) {
  WriteWalFile(1, {{"a", "flushed-already"}});
  { std::ofstream(dir_ + "/" + Sst(1)) << "sst"; }  // crash happened after publish, before WAL delete
  WriteWalFile(2, {{"b", "2"}});
  Open(Opts(false));
  EXPECT_FALSE(Exists(Wal(1)));
  EXPECT_TRUE(Exists(Wal(2)));
  EXPECT_EQ(engine_->recovered_records(), 1u);  // only WAL 2 was replayed
  std::string v;
  EXPECT_EQ(engine_->Get("b", &v), Status::kOk);
  EXPECT_EQ(engine_->Get("a", &v), Status::kNotFound);  // lives in 000001.sst; the reader arrives next PR
}

TEST_F(FlushTest, NewWalSequenceNeverReusesAnExistingNumber) {
  { std::ofstream(dir_ + "/" + Sst(7)) << "sst"; }
  Open(Opts(false));
  EXPECT_TRUE(Exists(Wal(8)));
  EXPECT_EQ(engine_->GetStats().active_seq, 8u);
}

TEST_F(FlushTest, EmptyOlderWalIsDeletedAndNewestKeepsReceivingWrites) {
  WriteWalFile(1, {});
  WriteWalFile(2, {{"b", "2"}});
  Open(Opts(false));
  EXPECT_FALSE(Exists(Wal(1)));
  EXPECT_EQ(engine_->GetStats().active_seq, 2u);
  ASSERT_EQ(engine_->Put("c", "3"), Status::kOk);
  engine_.reset();
  Open(Opts(false));
  std::string v;
  EXPECT_EQ(engine_->Get("b", &v), Status::kOk);
  EXPECT_EQ(engine_->Get("c", &v), Status::kOk);
}

TEST_F(FlushTest, DamagedOlderWalRefusesToOpen) {
  WriteWalFile(1, {{"a", "1"}, {"b", "2"}, {"c", "3"}});
  WriteWalFile(2, {{"d", "4"}});
  {
    std::fstream f(dir_ + "/" + Wal(1), std::ios::in | std::ios::out | std::ios::binary);
    f.seekp(8 + 1 + 4 + 2 + 8 + 1);  // inside the second record's payload
    f.put('\x7f');
  }
  std::unique_ptr<KVEngine> e;
  EXPECT_EQ(KVEngine::Open(Opts(false), &e), Status::kCorruption);
}

TEST_F(FlushTest, ConcurrentWritersAndReadersWhileFlushing) {
  auto o = Opts(true, 2048);
  Open(o);
  constexpr int kWriters = 4, kPer = 400;
  std::atomic<bool> stop{false}, bad{false};
  std::vector<std::thread> threads;
  for (int w = 0; w < kWriters; ++w) {
    threads.emplace_back([&, w] {
      for (int i = 0; i < kPer; ++i) {
        std::string k = Key(w * kPer + i);
        EXPECT_EQ(engine_->Put(k, "v" + k), Status::kOk);
      }
    });
  }
  std::vector<std::thread> readers;
  for (int r = 0; r < 2; ++r) {
    readers.emplace_back([&, r] {
      int i = r;
      std::string v;
      while (!stop.load()) {
        std::string k = Key(i++ % (kWriters * kPer));
        Status s = engine_->Get(k, &v);
        if (s == Status::kOk && v != "v" + k) bad = true;
      }
    });
  }
  for (auto& t : threads) t.join();
  stop = true;
  for (auto& t : readers) t.join();
  EXPECT_FALSE(bad.load());
  ASSERT_TRUE(WaitFor([&] { return Quiesced(); }));
  EXPECT_GE(engine_->GetStats().flushes_done, 5u);
  for (int i = 0; i < kWriters * kPer; ++i) ASSERT_EQ(Resolve(Key(i)), "v" + Key(i));
}