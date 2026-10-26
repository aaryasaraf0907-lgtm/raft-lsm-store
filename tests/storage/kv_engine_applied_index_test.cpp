// The engine's applied index: the Raft log index of the last entry applied through PutAt/DeleteAt,
// stored durably together with the data so that a restart can skip replaying what it already has.
#include <gtest/gtest.h>

#include <stdlib.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "storage/kv_engine.h"
#include "storage/wal.h"

using namespace raftkv;
namespace fs = std::filesystem;

namespace {

// One index record in the WAL: 8 bytes header + 1 type + 4 key length + key + 8 value bytes.
const std::string kIndexKey = std::string(kReservedKeyPrefix, sizeof(kReservedKeyPrefix) - 1) + "applied_index";
const size_t kIndexRecordBytes = 8 + 1 + 4 + kIndexKey.size() + 8;

class AppliedIndexTest : public ::testing::Test {
 protected:
  void SetUp() override {
    char tmpl[] = "/tmp/raftkv_idx_XXXXXX";
    dir_ = ::mkdtemp(tmpl);
  }
  void TearDown() override {
    engine_.reset();
    std::error_code ec;
    fs::remove_all(dir_, ec);
  }
  void Open(KVEngine::FlushWriter writer = nullptr) {
    engine_.reset();
    KVEngine::Options o;
    o.dir = dir_;
    o.sync_wal = false;
    o.memtable_bytes = writer ? 2048 : 4 * 1024 * 1024;
    o.flush_writer = writer;
    ASSERT_EQ(KVEngine::Open(o, &engine_), Status::kOk);
  }
  std::string Wal() const { return dir_ + "/" + KVEngine::WalFileName(1); }
  std::string Get(const std::string& key) {
    std::string v;
    return engine_->Get(key, &v) == Status::kOk ? v : "<absent>";
  }

  std::string dir_;
  std::unique_ptr<KVEngine> engine_;
};

}  // namespace

TEST_F(AppliedIndexTest, StartsAtZeroAndPutAtAndDeleteAtAdvanceIt) {
  Open();
  EXPECT_EQ(engine_->AppliedIndex(), 0u);
  ASSERT_EQ(engine_->PutAt("a", "1", 3), Status::kOk);
  EXPECT_EQ(engine_->AppliedIndex(), 3u);
  ASSERT_EQ(engine_->DeleteAt("a", 9), Status::kOk);
  EXPECT_EQ(engine_->AppliedIndex(), 9u);
  EXPECT_EQ(Get("a"), "<absent>");
  ASSERT_EQ(engine_->Put("plain", "x"), Status::kOk);  // a plain write does not touch the index
  EXPECT_EQ(engine_->AppliedIndex(), 9u);
}

TEST_F(AppliedIndexTest, IndexAndDataSurviveARestart) {
  Open();
  for (uint64_t i = 1; i <= 20; ++i) ASSERT_EQ(engine_->PutAt("k" + std::to_string(i), "v" + std::to_string(i), i * 2), Status::kOk);
  ASSERT_EQ(engine_->DeleteAt("k5", 41), Status::kOk);
  Open();  // a clean "crash": nothing but the files is kept
  EXPECT_EQ(engine_->AppliedIndex(), 41u);
  EXPECT_EQ(Get("k7"), "v7");
  EXPECT_EQ(Get("k5"), "<absent>");
}

// The order of the two records is what makes the index safe: it must never be ahead of the data.
TEST_F(AppliedIndexTest, TheWalHoldsTheDataRecordBeforeItsIndexRecord) {
  Open();
  ASSERT_EQ(engine_->PutAt("first", "1", 1), Status::kOk);
  ASSERT_EQ(engine_->DeleteAt("second", 2), Status::kOk);
  engine_.reset();
  std::vector<std::string> keys;
  ReplayStats stats;
  ASSERT_EQ(WAL::Replay(Wal(), [&](const Entry& e) { keys.push_back(e.key); }, &stats), Status::kOk);
  ASSERT_EQ(keys.size(), 4u);
  EXPECT_EQ(keys[0], "first");
  EXPECT_EQ(keys[1], kIndexKey) << "the index record came BEFORE the data it covers";
  EXPECT_EQ(keys[2], "second");
  EXPECT_EQ(keys[3], kIndexKey);
}

// A crash between the two records (or in the middle of the second): the data is there, the index
// lags by that one operation. Replaying that operation is harmless; skipping it would not be.
TEST_F(AppliedIndexTest, ACrashBeforeTheIndexRecordLeavesTheDataWithALaggingIndex) {
  for (size_t cut : {kIndexRecordBytes, kIndexRecordBytes - 15}) {  // whole record gone / torn in the middle
    SCOPED_TRACE("bytes cut from the end: " + std::to_string(cut));
    fs::remove_all(dir_);
    fs::create_directories(dir_);
    Open();
    ASSERT_EQ(engine_->PutAt("k1", "v1", 1), Status::kOk);
    ASSERT_EQ(engine_->PutAt("k2", "v2", 2), Status::kOk);
    engine_.reset();
    fs::resize_file(Wal(), fs::file_size(Wal()) - cut);
    Open();
    EXPECT_EQ(Get("k2"), "v2") << "the data record was durable";
    EXPECT_EQ(engine_->AppliedIndex(), 1u) << "the index must lag, not lead";
  }
}

TEST_F(AppliedIndexTest, RejectsAnIndexThatDoesNotIncreaseAndWritesNothing) {
  Open();
  ASSERT_EQ(engine_->PutAt("k", "v", 5), Status::kOk);
  EXPECT_EQ(engine_->PutAt("same", "x", 5), Status::kInvalidArgument);
  EXPECT_EQ(engine_->PutAt("older", "x", 3), Status::kInvalidArgument);
  EXPECT_EQ(engine_->DeleteAt("k", 4), Status::kInvalidArgument);
  EXPECT_EQ(engine_->PutAt("zero", "x", 0), Status::kInvalidArgument);
  EXPECT_EQ(Get("same"), "<absent>");
  EXPECT_EQ(Get("older"), "<absent>");
  EXPECT_EQ(Get("k"), "v") << "a rejected delete must not delete";
  EXPECT_EQ(engine_->AppliedIndex(), 5u);
}

TEST_F(AppliedIndexTest, TheReservedPrefixIsOffLimitsForWrites) {
  Open();
  const std::string reserved = std::string(kReservedKeyPrefix, sizeof(kReservedKeyPrefix) - 1) + "anything";
  EXPECT_EQ(engine_->Put(reserved, "x"), Status::kInvalidArgument);
  EXPECT_EQ(engine_->Delete(reserved), Status::kInvalidArgument);
  EXPECT_EQ(engine_->PutAt(reserved, "x", 1), Status::kInvalidArgument);
  EXPECT_EQ(engine_->DeleteAt(reserved, 2), Status::kInvalidArgument);
  EXPECT_EQ(engine_->Put(kIndexKey, "forged"), Status::kInvalidArgument) << "the applied index must not be forgeable";
  EXPECT_EQ(engine_->AppliedIndex(), 0u);
  // Near misses are ordinary keys.
  EXPECT_EQ(engine_->Put(std::string("x") + reserved, "ok"), Status::kOk);  // contains the prefix, does not start with it
  EXPECT_EQ(engine_->Put(reserved.substr(0, sizeof(kReservedKeyPrefix) - 2), "ok"), Status::kOk);  // one byte short
}

// Whatever memtable a write lands in, its index record lands in the same one, so a flushed table is
// self-describing: its index is the highest index of the data inside it.
TEST_F(AppliedIndexTest, EveryFlushedMemtableCarriesTheIndexOfItsOwnData) {
  std::mutex mu;
  std::map<uint64_t, std::map<std::string, std::string>> flushed;
  auto writer = [&](KVIterator& it, const std::string& path, uint64_t seq) -> Status {
    std::map<std::string, std::string> got;
    for (; it.Valid(); it.Next()) got[it.Current().key] = it.Current().value;
    { std::ofstream(path + ".tmp") << "sst"; }
    if (::rename((path + ".tmp").c_str(), path.c_str()) != 0) return Status::kIOError;
    std::lock_guard<std::mutex> lk(mu);
    flushed[seq] = got;
    return Status::kOk;
  };
  Open(writer);
  std::map<uint64_t, uint64_t> highest_index_in_seq;
  for (uint64_t i = 1; i <= 120; ++i) {
    Status s = engine_->PutAt("key" + std::to_string(i), std::string(100, 'x'), i);
    while (s != Status::kOk) {  // briefly refused while flushing catches up
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
      s = engine_->PutAt("key" + std::to_string(i), std::string(100, 'x'), i);
    }
    highest_index_in_seq[engine_->GetStats().active_seq] = i;
  }
  for (int i = 0; i < 400 && engine_->GetStats().frozen_memtables > 0; ++i) std::this_thread::sleep_for(std::chrono::milliseconds(5));
  std::lock_guard<std::mutex> lk(mu);
  ASSERT_GE(flushed.size(), 3u) << "the test needs several flushed memtables";
  for (const auto& f : flushed) {
    auto it = f.second.find(kIndexKey);
    ASSERT_NE(it, f.second.end()) << "memtable " << f.first << " was flushed without its index";
    uint64_t index = 0;
    for (int b = 0; b < 8; ++b) index |= static_cast<uint64_t>(static_cast<uint8_t>(it->second[b])) << (8 * b);
    EXPECT_EQ(index, highest_index_in_seq[f.first]) << "memtable " << f.first;
  }
}
