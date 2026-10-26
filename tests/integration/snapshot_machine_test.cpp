// Raft snapshots of the real engine through the state machine: the data and the client-session
// table travel together, and a node that already holds the state does not rewrite it.
#include <gtest/gtest.h>

#include <stdlib.h>

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "raft/raft_interface.h"
#include "server/kv_state_machine.h"
#include "storage/kv_engine.h"

using namespace raftkv;
namespace fs = std::filesystem;

namespace {

LogEntry Put(LogIndex index, const std::string& key, const std::string& value, uint64_t client, uint64_t seq) {
  Command c{CommandType::kPut, key, value};
  c.client_id = client;
  c.seq = seq;
  return LogEntry{1, index, c};
}

struct Node {
  std::string dir;
  std::unique_ptr<KVEngine> engine;
  std::unique_ptr<KVStateMachine> machine;
  explicit Node(const std::string& d) : dir(d) { Open(); }
  void Open() {
    machine.reset();
    engine.reset();
    KVEngine::Options o;
    o.dir = dir;
    o.sync_wal = false;
    o.use_sstables = true;
    o.memtable_bytes = 4096;
    o.compaction_trigger = 0;
    EXPECT_EQ(KVEngine::Open(o, &engine), Status::kOk);
    machine = std::make_unique<KVStateMachine>(engine.get());
  }
  std::string Get(const std::string& k) {
    std::string v;
    return engine->Get(k, &v) == Status::kOk ? v : "<absent>";
  }
};

class SnapshotMachineTest : public ::testing::Test {
 protected:
  void SetUp() override {
    char tmpl[] = "/tmp/raftkv_snapm_XXXXXX";
    root_ = ::mkdtemp(tmpl);
  }
  void TearDown() override {
    std::error_code ec;
    fs::remove_all(root_, ec);
  }
  std::string root_;
};

}  // namespace

TEST_F(SnapshotMachineTest, ASnapshotCarriesTheDataAndTheSessionTableToAFreshNode) {
  Node a(root_ + "/a"), b(root_ + "/b");
  for (LogIndex i = 2; i <= 81; ++i) a.machine->Apply(Put(i, "k" + std::to_string(i % 30), "v" + std::to_string(i) + std::string(80, 'x'), 7, i - 1));
  const std::string snapshot = a.machine->TakeSnapshot();

  b.machine->RestoreSnapshot(snapshot);
  for (int k = 0; k < 30; ++k) ASSERT_EQ(b.Get("k" + std::to_string(k)), a.Get("k" + std::to_string(k))) << k;
  EXPECT_EQ(b.engine->AppliedIndex(), a.engine->AppliedIndex());
  EXPECT_EQ(b.machine->Sessions().LastSeq(7), 80u) << "without the session table a retry would be applied twice";
  EXPECT_EQ(b.machine->ReplayFloor(), a.engine->AppliedIndex());

  // A retry of an old request, with a tampered value, is still filtered on the restored node.
  b.machine->Apply(Put(82, "k5", "TAMPERED", 7, 3));
  EXPECT_EQ(b.machine->DuplicatesSkipped(), 1u);
  EXPECT_NE(b.Get("k5"), "TAMPERED");
  // New work continues from the snapshot.
  b.machine->Apply(Put(83, "fresh", "yes", 7, 81));
  EXPECT_EQ(b.Get("fresh"), "yes");
  EXPECT_EQ(b.engine->AppliedIndex(), 83u);
}

TEST_F(SnapshotMachineTest, AnEmptyNodeSnapshotsAndRestoresToo) {
  Node a(root_ + "/a"), b(root_ + "/b");
  const std::string snapshot = a.machine->TakeSnapshot();
  b.machine->Apply(Put(2, "old", "data", 9, 1));
  b.machine->RestoreSnapshot(snapshot);  // b is ahead of an empty snapshot: nothing is replaced
  EXPECT_EQ(b.Get("old"), "data");
}

// A node restarting from its own snapshot has a store that is at or ahead of it. Replacing the
// store would throw away newer work and rewrite everything on every start, so it must be skipped.
TEST_F(SnapshotMachineTest, ARestoreIsSkippedWhenTheEngineAlreadyHoldsAtLeastThatState) {
  Node a(root_ + "/a");
  for (LogIndex i = 2; i <= 31; ++i) a.machine->Apply(Put(i, "k" + std::to_string(i), "early", 7, i - 1));
  const std::string snapshot = a.machine->TakeSnapshot();  // as of index 31
  for (LogIndex i = 32; i <= 60; ++i) a.machine->Apply(Put(i, "k" + std::to_string(i), "late", 7, i - 1));

  a.Open();  // a restart: a fresh state machine over the engine, which holds everything up to 60
  EXPECT_EQ(a.machine->ReplayFloor(), 60u);
  a.machine->RestoreSnapshot(snapshot);
  EXPECT_EQ(a.Get("k45"), "late") << "the newer data must survive the restore";
  EXPECT_EQ(a.engine->AppliedIndex(), 60u);
  EXPECT_EQ(a.machine->ReplayFloor(), 60u);
  EXPECT_EQ(a.machine->Sessions().LastSeq(7), 30u) << "sessions are as of the snapshot, then rebuilt by replay";
  // Raft now replays the entries after the snapshot: the store already has them.
  for (LogIndex i = 32; i <= 60; ++i) a.machine->Apply(Put(i, "k" + std::to_string(i), "late", 7, i - 1));
  EXPECT_EQ(a.machine->ReplaySkipped(), 29u);
  EXPECT_EQ(a.machine->Sessions().LastSeq(7), 59u);
}

TEST_F(SnapshotMachineTest, AMalformedSnapshotIsFatalNotSilentlyIgnored) {
  Node a(root_ + "/a");
  EXPECT_DEATH(a.machine->RestoreSnapshot("this is not a snapshot"), "snapshot");
  const std::string good = a.machine->TakeSnapshot();
  EXPECT_DEATH(a.machine->RestoreSnapshot(good.substr(0, good.size() - 3)), "snapshot");
}
