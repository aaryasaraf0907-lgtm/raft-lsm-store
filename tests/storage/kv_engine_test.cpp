#include "storage/kv_engine.h"

#include <gtest/gtest.h>
#ifndef _WIN32
#include <sys/resource.h>
#include <unistd.h>
#endif

#include <atomic>
#include <chrono>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

using raftkv::KVEngine;
using raftkv::KVStore;
using raftkv::Status;

class KVEngineTest : public ::testing::Test {
 protected:
  void SetUp() override {
    static std::atomic<uint64_t> next_id{0};
    const auto timestamp = std::chrono::high_resolution_clock::now()
                              .time_since_epoch()
                              .count();
    dir_ = (std::filesystem::temp_directory_path() /
            ("raftkv_eng_" + std::to_string(timestamp) + "_" +
             std::to_string(next_id++))).string();
    std::error_code ec;
    ASSERT_TRUE(std::filesystem::create_directories(dir_, ec));
    ASSERT_FALSE(ec);
  }
  void TearDown() override { std::filesystem::remove_all(dir_); }

  std::unique_ptr<KVEngine> Open(bool sync = true) {
    KVEngine::Options o;
    o.dir = dir_;
    o.sync_wal = sync;
    std::unique_ptr<KVEngine> e;
    EXPECT_EQ(KVEngine::Open(o, &e), Status::kOk);
    return e;
  }
  std::string Get(KVStore& s, const std::string& k) {
    std::string v;
    return s.Get(k, &v) == Status::kOk ? v : "<none>";
  }
  std::string WalPath() { return dir_ + "/wal.log"; }

  std::string dir_;
};

TEST_F(KVEngineTest, EmptyEngineHasNothing) {
  auto e = Open();
  std::string v = "untouched";
  EXPECT_EQ(e->Get("x", &v), Status::kNotFound);
  EXPECT_EQ(v, "untouched");  // contract: *value untouched on kNotFound
  EXPECT_EQ(e->recovered_records(), 0u);
}

TEST_F(KVEngineTest, CreatesMissingDirectory) {
  KVEngine::Options o;
  o.dir = dir_ + "/a/b/c";
  std::unique_ptr<KVEngine> e;
  ASSERT_EQ(KVEngine::Open(o, &e), Status::kOk);
  EXPECT_EQ(e->Put("k", "v"), Status::kOk);
}

TEST_F(KVEngineTest, OpenFailsWhenPathIsAFile) {
  { std::ofstream(dir_ + "/file") << "x"; }
  KVEngine::Options o;
  o.dir = dir_ + "/file";
  std::unique_ptr<KVEngine> e;
  EXPECT_EQ(KVEngine::Open(o, &e), Status::kIOError);
}

TEST_F(KVEngineTest, PutGetOverwriteDelete) {
  auto e = Open();
  EXPECT_EQ(e->Put("k", "v1"), Status::kOk);
  EXPECT_EQ(Get(*e, "k"), "v1");
  EXPECT_EQ(e->Put("k", "v2"), Status::kOk);
  EXPECT_EQ(Get(*e, "k"), "v2");
  EXPECT_EQ(e->Delete("k"), Status::kOk);
  EXPECT_EQ(Get(*e, "k"), "<none>");
  EXPECT_EQ(e->Put("k", "v3"), Status::kOk);  // resurrect after delete
  EXPECT_EQ(Get(*e, "k"), "v3");
}

TEST_F(KVEngineTest, DeleteOfAbsentKeyIsOk) {
  auto e = Open();
  EXPECT_EQ(e->Delete("ghost"), Status::kOk);
  EXPECT_EQ(Get(*e, "ghost"), "<none>");
}

TEST_F(KVEngineTest, WorksThroughKVStoreInterface) {
  std::unique_ptr<KVEngine> e = Open();
  KVStore& s = *e;
  EXPECT_EQ(s.Put("a", "1"), Status::kOk);
  EXPECT_EQ(Get(s, "a"), "1");
}

TEST_F(KVEngineTest, EmptyAndBinaryKeysAndValues) {
  auto e = Open();
  std::string k("\0k\xff", 3), v("\0\0v", 3);
  EXPECT_EQ(e->Put("", ""), Status::kOk);
  EXPECT_EQ(e->Put(k, v), Status::kOk);
  EXPECT_EQ(Get(*e, ""), "");
  EXPECT_EQ(Get(*e, k), v);
}

TEST_F(KVEngineTest, EveryWriteIsInTheWalBeforeItIsAcknowledged) {
  auto e = Open();
  ASSERT_EQ(e->Put("a", "1"), Status::kOk);
  ASSERT_EQ(e->Delete("a"), Status::kOk);
  std::vector<raftkv::Entry> log;  // read the log while the engine is still open
  ASSERT_EQ(raftkv::WAL::Replay(WalPath(), [&](const raftkv::Entry& x) { log.push_back(x); }), Status::kOk);
  ASSERT_EQ(log.size(), 2u);
  EXPECT_FALSE(log[0].tombstone);
  EXPECT_TRUE(log[1].tombstone);
}

TEST_F(KVEngineTest, RecoversPutsOverwritesAndDeletesAfterRestart) {
  {
    auto e = Open();
    e->Put("a", "1");
    e->Put("b", "2");
    e->Put("a", "1b");
    e->Delete("b");
    e->Put("c", "3");
  }
  auto e = Open();
  EXPECT_EQ(e->recovered_records(), 5u);
  EXPECT_EQ(Get(*e, "a"), "1b");
  EXPECT_EQ(Get(*e, "b"), "<none>");
  EXPECT_EQ(Get(*e, "c"), "3");
}

TEST_F(KVEngineTest, RestartTwiceKeepsAppending) {
  { Open()->Put("a", "1"); }
  { Open()->Put("b", "2"); }
  auto e = Open();
  EXPECT_EQ(Get(*e, "a"), "1");
  EXPECT_EQ(Get(*e, "b"), "2");
}

TEST_F(KVEngineTest, UnsyncedModeStillRecoversAfterCleanClose) {
  { Open(/*sync=*/false)->Put("a", "1"); }
  EXPECT_EQ(Get(*Open(), "a"), "1");
}

TEST_F(KVEngineTest, CrashWithTornLastRecordLosesOnlyThatWrite) {
  {
    auto e = Open();
    e->Put("a", "1");
    e->Put("b", "2");
  }
  std::filesystem::resize_file(WalPath(), std::filesystem::file_size(WalPath()) - 3);  // power cut
  {
    auto e = Open();
    EXPECT_EQ(Get(*e, "a"), "1");
    EXPECT_EQ(Get(*e, "b"), "<none>");
    EXPECT_EQ(e->Put("c", "3"), Status::kOk);  // new writes land after clean data
  }
  auto e = Open();
  EXPECT_EQ(Get(*e, "a"), "1");
  EXPECT_EQ(Get(*e, "c"), "3");
}

TEST_F(KVEngineTest, MidLogCorruptionRefusesToOpen) {
  {
    auto e = Open();
    e->Put("a", "1");
    e->Put("b", "2");
    e->Put("c", "3");
  }
  {
    std::fstream f(WalPath(), std::ios::in | std::ios::out | std::ios::binary);
    f.seekp(8 + 1 + 4 + 2 + 8 + 1);  // inside the second record's payload
    f.put('\x7f');
  }
  KVEngine::Options o;
  o.dir = dir_;
  std::unique_ptr<KVEngine> e;
  EXPECT_EQ(KVEngine::Open(o, &e), Status::kCorruption);
  EXPECT_EQ(e, nullptr);
}

// If the WAL write fails, the write must not become visible, and the engine
// must keep refusing writes (the WAL may end in a partial record).
TEST_F(KVEngineTest, FailedWalWriteIsNotAppliedToMemtable) {
#ifndef _WIN32
  std::signal(SIGXFSZ, SIG_IGN);
  auto e = Open();
  ASSERT_EQ(e->Put("a", "1"), Status::kOk);
  uint64_t size = std::filesystem::file_size(WalPath());

  struct rlimit original;
  ASSERT_EQ(getrlimit(RLIMIT_FSIZE, &original), 0);
  struct rlimit tiny = {static_cast<rlim_t>(size + 10), original.rlim_max};
  ASSERT_EQ(setrlimit(RLIMIT_FSIZE, &tiny), 0);
  Status failed = e->Put("b", "2");
  ASSERT_EQ(setrlimit(RLIMIT_FSIZE, &original), 0);

  EXPECT_EQ(failed, Status::kIOError);
  EXPECT_EQ(Get(*e, "b"), "<none>");
  EXPECT_EQ(e->Put("c", "3"), Status::kIOError);
  EXPECT_EQ(e->Delete("a"), Status::kIOError);
  EXPECT_EQ(Get(*e, "a"), "1");  // reads still work
#else
  GTEST_SKIP() << "KVEngine does not expose WAL I/O injection; "
                  "the WAL-level injected partial-write test covers this path";
#endif
}

TEST_F(KVEngineTest, ConcurrentWritersAndReaders) {
  auto e = Open(/*sync=*/false);
  constexpr int kWriters = 4, kPerWriter = 300;
  std::atomic<bool> stop{false};
  std::vector<std::thread> threads;
  for (int w = 0; w < kWriters; ++w) {
    threads.emplace_back([&, w] {
      for (int i = 0; i < kPerWriter; ++i) {
        std::string k = "w" + std::to_string(w) + "-" + std::to_string(i);
        EXPECT_EQ(e->Put(k, k), Status::kOk);
      }
    });
  }
  std::thread reader([&] {
    std::string v;
    while (!stop) e->Get("w0-1", &v);
  });
  for (auto& t : threads) t.join();
  stop = true;
  reader.join();
  for (int w = 0; w < kWriters; ++w)
    for (int i = 0; i < kPerWriter; ++i) {
      std::string k = "w" + std::to_string(w) + "-" + std::to_string(i);
      ASSERT_EQ(Get(*e, k), k);
    }
  e.reset();
  EXPECT_EQ(Open()->recovered_records(), static_cast<uint64_t>(kWriters * kPerWriter));
}