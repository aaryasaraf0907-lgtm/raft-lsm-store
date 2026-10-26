// A restarting node replays its whole Raft log through the state machine. With the real engine
// (which remembers the applied index) the replay must rebuild the client-session table but must NOT
// write again what the engine already holds.
#include <gtest/gtest.h>

#include <stdlib.h>

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "raft/raft_interface.h"
#include "server/kv_state_machine.h"
#include "storage/kv_engine.h"
#include "storage/wal.h"

using namespace raftkv;
namespace fs = std::filesystem;

namespace {

// Wraps the real engine and counts the writes that reach it.
class CountingIndexed : public IndexedKVStore {
 public:
  explicit CountingIndexed(KVEngine* e) : e_(e) {}
  Status Put(const std::string& k, const std::string& v) override { ++plain_writes; return e_->Put(k, v); }
  Status Get(const std::string& k, std::string* v) override { return e_->Get(k, v); }
  Status Delete(const std::string& k) override { ++plain_writes; return e_->Delete(k); }
  Status PutAt(const std::string& k, const std::string& v, uint64_t i) override { ++indexed_writes; return e_->PutAt(k, v, i); }
  Status DeleteAt(const std::string& k, uint64_t i) override { ++indexed_writes; return e_->DeleteAt(k, i); }
  uint64_t AppliedIndex() override { return e_->AppliedIndex(); }
  int indexed_writes = 0, plain_writes = 0;

 private:
  KVEngine* e_;
};

// The same engine seen only through the plain KVStore interface (what older code uses).
class PlainCounting : public KVStore {
 public:
  explicit PlainCounting(KVEngine* e) : e_(e) {}
  Status Put(const std::string& k, const std::string& v) override { ++writes; return e_->Put(k, v); }
  Status Get(const std::string& k, std::string* v) override { return e_->Get(k, v); }
  Status Delete(const std::string& k) override { ++writes; return e_->Delete(k); }
  int writes = 0;

 private:
  KVEngine* e_;
};

LogEntry Put(LogIndex index, const std::string& key, const std::string& value, uint64_t client = 0, uint64_t seq = 0) {
  Command c{CommandType::kPut, key, value};
  c.client_id = client;
  c.seq = seq;
  return LogEntry{1, index, c};
}
LogEntry Del(LogIndex index, const std::string& key, uint64_t client = 0, uint64_t seq = 0) {
  Command c{CommandType::kDelete, key, ""};
  c.client_id = client;
  c.seq = seq;
  return LogEntry{1, index, c};
}

class ReplaySkipTest : public ::testing::Test {
 protected:
  void SetUp() override {
    char tmpl[] = "/tmp/raftkv_replay_XXXXXX";
    dir_ = ::mkdtemp(tmpl);
  }
  void TearDown() override {
    std::error_code ec;
    fs::remove_all(dir_, ec);
  }
  std::unique_ptr<KVEngine> Open() {
    KVEngine::Options o;
    o.dir = dir_;
    o.sync_wal = false;
    std::unique_ptr<KVEngine> e;
    EXPECT_EQ(KVEngine::Open(o, &e), Status::kOk);
    return e;
  }
  std::string Get(KVEngine* e, const std::string& k) {
    std::string v;
    return e->Get(k, &v) == Status::kOk ? v : "<absent>";
  }
  std::string dir_;
};

}  // namespace

TEST_F(ReplaySkipTest, ReplayAfterARestartRebuildsSessionsButWritesNothingAgain) {
  std::vector<LogEntry> log;
  for (LogIndex i = 2; i <= 21; ++i) log.push_back(Put(i, "k" + std::to_string(i), "v" + std::to_string(i), 7, i - 1));
  {  // live: the node applies the log as it commits
    auto engine = Open();
    KVStateMachine machine(engine.get());
    for (const auto& e : log) machine.Apply(e);
    EXPECT_EQ(machine.ReplaySkipped(), 0u);
    EXPECT_EQ(engine->AppliedIndex(), 21u);
  }
  // restart: a brand-new state machine (empty session table) gets the whole log again
  auto engine = Open();
  CountingIndexed store(engine.get());
  KVStateMachine machine(&store);
  EXPECT_EQ(machine.ReplayFloor(), 21u);
  for (const auto& e : log) machine.Apply(e);
  EXPECT_EQ(store.indexed_writes, 0) << "the replay wrote entries the engine already had";
  EXPECT_EQ(machine.ReplaySkipped(), 20u);
  EXPECT_EQ(machine.Sessions().LastSeq(7), 20u) << "the session table must still be rebuilt";
  EXPECT_EQ(machine.Applied(), 20u);
  EXPECT_EQ(machine.LookupRequestOutcome(5), KVStateMachine::RequestOutcome::kApplied);
  EXPECT_EQ(Get(engine.get(), "k13"), "v13");

  // New entries after the replay are written, and the index moves on.
  machine.Apply(Put(22, "fresh", "yes", 7, 21));
  EXPECT_EQ(store.indexed_writes, 1);
  EXPECT_EQ(engine->AppliedIndex(), 22u);
  EXPECT_EQ(Get(engine.get(), "fresh"), "yes");
}

// Crash between the data record and the index record of the last entry: the engine's index is one
// behind its data. Replay must re-apply exactly that entry (harmless) and skip everything before.
TEST_F(ReplaySkipTest, ACrashBeforeTheLastIndexRecordReappliesOnlyTheLastEntry) {
  std::vector<LogEntry> log;
  for (LogIndex i = 2; i <= 11; ++i) log.push_back(Put(i, "k" + std::to_string(i), "v" + std::to_string(i), 9, i - 1));
  {
    auto engine = Open();
    KVStateMachine machine(engine.get());
    for (const auto& e : log) machine.Apply(e);
  }
  const std::string index_key = std::string(kReservedKeyPrefix, sizeof(kReservedKeyPrefix) - 1) + "applied_index";
  const std::string wal = dir_ + "/" + KVEngine::WalFileName(1);
  fs::resize_file(wal, fs::file_size(wal) - (8 + 1 + 4 + index_key.size() + 8));  // the last index record is lost
  auto engine = Open();
  CountingIndexed store(engine.get());
  KVStateMachine machine(&store);
  EXPECT_EQ(machine.ReplayFloor(), 10u);
  for (const auto& e : log) machine.Apply(e);
  EXPECT_EQ(store.indexed_writes, 1) << "only the entry whose index record was lost may be written again";
  EXPECT_EQ(machine.ReplaySkipped(), 9u);
  EXPECT_EQ(engine->AppliedIndex(), 11u);
  EXPECT_EQ(Get(engine.get(), "k11"), "v11");
  EXPECT_EQ(Get(engine.get(), "k5"), "v5");
}

// A client retry that was committed twice is applied once, live and during replay.
TEST_F(ReplaySkipTest, DuplicateRequestsInTheLogStillCountOnceDuringReplay) {
  std::vector<LogEntry> log = {Put(2, "a", "1", 7, 1), Put(3, "a", "OVERWRITE-IF-APPLIED-TWICE", 7, 1) /* the same request again */,
                               Put(4, "b", "2", 7, 2)};
  {
    auto engine = Open();
    KVStateMachine machine(engine.get());
    for (const auto& e : log) machine.Apply(e);
    EXPECT_EQ(machine.DuplicatesSkipped(), 1u);
    EXPECT_EQ(Get(engine.get(), "a"), "1");
  }
  auto engine = Open();
  CountingIndexed store(engine.get());
  KVStateMachine machine(&store);
  for (const auto& e : log) machine.Apply(e);
  EXPECT_EQ(store.indexed_writes, 0);
  EXPECT_EQ(machine.DuplicatesSkipped(), 1u);
  EXPECT_EQ(machine.Sessions().LastSeq(7), 2u);
  EXPECT_EQ(machine.LookupRequestOutcome(2), KVStateMachine::RequestOutcome::kApplied);
  EXPECT_EQ(machine.LookupRequestOutcome(3), KVStateMachine::RequestOutcome::kDuplicate);
  EXPECT_EQ(Get(engine.get(), "a"), "1");
}

TEST_F(ReplaySkipTest, DeletesAreIndexedAndSkippedToo) {
  std::vector<LogEntry> log = {Put(2, "k", "v", 7, 1), Del(3, "k", 7, 2), Del(4, "never-existed", 7, 3)};
  {
    auto engine = Open();
    KVStateMachine machine(engine.get());
    for (const auto& e : log) machine.Apply(e);
    EXPECT_EQ(engine->AppliedIndex(), 4u);
  }
  auto engine = Open();
  CountingIndexed store(engine.get());
  KVStateMachine machine(&store);
  for (const auto& e : log) machine.Apply(e);
  EXPECT_EQ(store.indexed_writes, 0);
  EXPECT_EQ(Get(engine.get(), "k"), "<absent>");
}

// Entries that need no store write (a no-op, a duplicate) do not move the index; that is fine.
TEST_F(ReplaySkipTest, ANoopAtTheEndOfTheLogChangesNothing) {
  std::vector<LogEntry> log = {Put(2, "k", "v", 7, 1), LogEntry{1, 3, Command{CommandType::kNoop, "", ""}}};
  auto engine = Open();
  KVStateMachine machine(engine.get());
  for (const auto& e : log) machine.Apply(e);
  EXPECT_EQ(engine->AppliedIndex(), 2u);
}

// Code that only knows the plain interface keeps working; it just rewrites on replay.
TEST_F(ReplaySkipTest, APlainStoreStillAppliesEverythingOnReplay) {
  std::vector<LogEntry> log;
  for (LogIndex i = 2; i <= 6; ++i) log.push_back(Put(i, "k" + std::to_string(i), "v", 7, i - 1));
  auto engine = Open();
  PlainCounting store(engine.get());
  KVStateMachine first(&store);
  for (const auto& e : log) first.Apply(e);
  EXPECT_EQ(store.writes, 5);
  KVStateMachine second(&store);  // a restart, with the plain interface
  for (const auto& e : log) second.Apply(e);
  EXPECT_EQ(store.writes, 10);
  EXPECT_EQ(second.ReplaySkipped(), 0u);
  EXPECT_EQ(second.ReplayFloor(), 0u);
}
