#include <gtest/gtest.h>

#include <chrono>
#include <map>
#include <string>
#include <thread>
#include <vector>

#include "server/kv_state_machine.h"
#include "test_cluster.h"

using namespace raftkv;
using namespace std::chrono_literals;
using raftkv::testing_util::Cluster;
using raftkv::testing_util::WaitFor;

namespace {

// A KVStore that just remembers what it was asked to do.
class FakeStore : public KVStore {
 public:
  Status Put(const std::string& key, const std::string& value) override {
    ++puts;
    data[key] = value;
    return Status::kOk;
  }
  Status Get(const std::string& key, std::string* value) override {
    auto it = data.find(key);
    if (it == data.end()) return Status::kNotFound;
    *value = it->second;
    return Status::kOk;
  }
  Status Delete(const std::string& key) override {
    ++deletes;
    data.erase(key);
    return Status::kNotFound;  // some stores report this for a missing key; it must be tolerated
  }
  std::map<std::string, std::string> data;
  int puts = 0;
  int deletes = 0;
};

Command Req(uint64_t client, uint64_t seq, CommandType type, const std::string& key,
            const std::string& value = "") {
  Command c{type, key, value};
  c.client_id = client;
  c.seq = seq;
  return c;
}

LogEntry Entry(LogIndex index, const Command& cmd) { return LogEntry{1, index, cmd}; }

// Runs entries through a fresh state machine, the way a node's apply callback would.
struct Replayed {
  FakeStore store;
  KVStateMachine machine{&store};
  explicit Replayed(const std::vector<LogEntry>& entries) {
    for (const auto& e : entries) machine.Apply(e);
  }
};

// Retries one request (always the same client_id and seq) until every node has applied it.
bool RetryUntilEverywhere(Cluster& c, const Command& cmd, std::chrono::milliseconds timeout) {
  auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    NodeId leader = c.FindLeader();
    if (leader != 0 && c.Node(leader)->ProposeAndWait(cmd, 1500ms).result == CommitResult::kApplied) {
      bool everywhere = WaitFor(
          [&] {
            for (auto& node : c.nodes) {
              bool found = false;
              for (const auto& e : c.Applied(node->Id())) {
                if (e.command.client_id == cmd.client_id && e.command.seq == cmd.seq) found = true;
              }
              if (!found) return false;
            }
            return true;
          },
          1500ms);
      if (everywhere) return true;
    }
    std::this_thread::sleep_for(50ms);
  }
  return false;
}

}  // namespace

// ---- the duplicate table on its own ----

TEST(ClientSessionTable, AppliesEachSequenceNumberOnce) {
  ClientSessionTable t;
  EXPECT_TRUE(t.ShouldApply(Req(7, 1, CommandType::kPut, "k")));
  EXPECT_FALSE(t.ShouldApply(Req(7, 1, CommandType::kPut, "k"))) << "same request again";
  EXPECT_TRUE(t.ShouldApply(Req(7, 2, CommandType::kPut, "k")));
  EXPECT_FALSE(t.ShouldApply(Req(7, 1, CommandType::kPut, "k"))) << "older request after newer";
  EXPECT_FALSE(t.ShouldApply(Req(7, 2, CommandType::kPut, "k")));
  EXPECT_TRUE(t.ShouldApply(Req(7, 10, CommandType::kPut, "k"))) << "gaps are fine";
  EXPECT_EQ(t.LastSeq(7), 10u);
}

TEST(ClientSessionTable, ClientsAreIndependent) {
  ClientSessionTable t;
  EXPECT_TRUE(t.ShouldApply(Req(1, 5, CommandType::kPut, "k")));
  EXPECT_TRUE(t.ShouldApply(Req(2, 5, CommandType::kPut, "k"))) << "same seq, other client";
  EXPECT_FALSE(t.ShouldApply(Req(1, 5, CommandType::kPut, "k")));
  EXPECT_EQ(t.ClientCount(), 2u);
}

TEST(ClientSessionTable, RequestsWithoutAnIdAreAlwaysApplied) {
  ClientSessionTable t;
  for (int i = 0; i < 3; ++i) {
    EXPECT_TRUE(t.ShouldApply(Req(0, 0, CommandType::kPut, "k")));
    EXPECT_TRUE(t.ShouldApply(Req(0, 5, CommandType::kPut, "k"))) << "no client id";
    EXPECT_TRUE(t.ShouldApply(Req(9, 0, CommandType::kPut, "k"))) << "seq 0 means no request id";
  }
  EXPECT_EQ(t.ClientCount(), 0u);
}

// ---- the state machine ----

TEST(KVStateMachine, AppliesPutsAndDeletesAndIgnoresNoops) {
  Replayed r({Entry(1, Req(0, 0, CommandType::kNoop, "")),
              Entry(2, Req(1, 1, CommandType::kPut, "a", "1")),
              Entry(3, Req(1, 2, CommandType::kPut, "b", "2")),
              Entry(4, Req(1, 3, CommandType::kDelete, "a")),
              Entry(5, Req(1, 4, CommandType::kDelete, "never-existed"))});
  EXPECT_EQ(r.store.data, (std::map<std::string, std::string>{{"b", "2"}}));
  EXPECT_EQ(r.store.puts, 2);
  EXPECT_EQ(r.store.deletes, 2);
  EXPECT_EQ(r.machine.Applied(), 4u);
  EXPECT_EQ(r.machine.DuplicatesSkipped(), 0u);
}

TEST(KVStateMachine, RetriedRequestIsAppliedOnlyOnce) {
  Replayed r({Entry(1, Req(7, 1, CommandType::kPut, "k", "v")),
              Entry(2, Req(7, 1, CommandType::kPut, "k", "v"))});  // the client's retry
  EXPECT_EQ(r.store.puts, 1);
  EXPECT_EQ(r.machine.DuplicatesSkipped(), 1u);
}

TEST(KVStateMachine, StaleRetryCannotOverwriteANewerWrite) {
  // The anomaly de-duplication prevents: the client wrote "a", then "b", and a delayed retry of
  // the first request shows up in the log afterwards. Without de-duplication "a" would win.
  Replayed r({Entry(1, Req(7, 1, CommandType::kPut, "k", "a")),
              Entry(2, Req(7, 2, CommandType::kPut, "k", "b")),
              Entry(3, Req(7, 1, CommandType::kPut, "k", "a"))});
  EXPECT_EQ(r.store.data["k"], "b");
  EXPECT_EQ(r.store.puts, 2);
}

TEST(KVStateMachine, ReplayingTheWholeLogAfterARestartGivesTheSameResult) {
  std::vector<LogEntry> log = {Entry(1, Req(1, 1, CommandType::kPut, "x", "1")),
                               Entry(2, Req(2, 1, CommandType::kPut, "y", "2")),
                               Entry(3, Req(1, 1, CommandType::kPut, "x", "1")),
                               Entry(4, Req(1, 2, CommandType::kDelete, "x"))};
  Replayed before(log);
  Replayed after_restart(log);  // fresh state machine, same log
  EXPECT_EQ(before.store.data, after_restart.store.data);
  EXPECT_EQ(before.machine.DuplicatesSkipped(), after_restart.machine.DuplicatesSkipped());
}

// ---- through a real Raft cluster ----

TEST(ExactlyOnce, RetryAfterATimeoutTakesEffectOnce) {
  Cluster c(3);
  ASSERT_TRUE(WaitFor([&] { return c.CountLeaders() == 1; }, 3000ms));
  NodeId leader = c.FindLeader();
  const Command request = Req(9, 1, CommandType::kPut, "x", "1");

  // The leader is cut off from both followers, so the client gets no answer: outcome unknown.
  NodeId f1 = (leader % 3) + 1;
  NodeId f2 = (f1 % 3) + 1;
  c.net.Disconnect(f1);
  c.net.Disconnect(f2);
  EXPECT_EQ(c.Node(leader)->ProposeAndWait(request, 400ms).result, CommitResult::kTimeout);

  // The network heals and the client retries the SAME request until it gets through. The first
  // attempt may or may not have survived the leadership change; either way it must count once.
  c.net.Reconnect(f1);
  c.net.Reconnect(f2);
  ASSERT_TRUE(RetryUntilEverywhere(c, request, 12000ms));

  ASSERT_TRUE(WaitFor(
      [&] { return c.Applied(1).size() == c.Applied(2).size() && c.Applied(2).size() == c.Applied(3).size(); },
      5000ms));
  for (NodeId id = 1; id <= 3; ++id) {
    Replayed r(c.Applied(id));
    EXPECT_EQ(r.store.data["x"], "1") << "node " << id;
    EXPECT_EQ(r.store.puts, 1) << "node " << id << " applied the request more than once";
  }
}

TEST(ExactlyOnce, DelayedDuplicateDoesNotUndoANewerWriteOnAnyNode) {
  Cluster c(3);
  ASSERT_TRUE(WaitFor([&] { return c.CountLeaders() == 1; }, 3000ms));
  NodeId leader = c.FindLeader();
  ASSERT_EQ(c.Node(leader)->ProposeAndWait(Req(5, 1, CommandType::kPut, "k", "old"), 5000ms).result,
            CommitResult::kApplied);
  ASSERT_EQ(c.Node(leader)->ProposeAndWait(Req(5, 2, CommandType::kPut, "k", "new"), 5000ms).result,
            CommitResult::kApplied);
  // A retry of request 1 that was stuck somewhere in the network finally reaches the leader.
  ASSERT_EQ(c.Node(leader)->ProposeAndWait(Req(5, 1, CommandType::kPut, "k", "old"), 5000ms).result,
            CommitResult::kApplied);

  for (NodeId id = 1; id <= 3; ++id) {
    ASSERT_TRUE(WaitFor([&] { return c.Applied(id).size() == 3; }, 5000ms)) << "node " << id;
    Replayed r(c.Applied(id));
    EXPECT_EQ(r.store.data["k"], "new") << "node " << id;
    EXPECT_EQ(r.machine.DuplicatesSkipped(), 1u) << "node " << id;
  }
}

TEST(ExactlyOnce, RequestIdsSurviveARestartOfTheWholeCluster) {
  Cluster c(3, /*persistent=*/true);
  ASSERT_TRUE(WaitFor([&] { return c.CountLeaders() == 1; }, 3000ms));
  NodeId leader = c.FindLeader();
  ASSERT_EQ(c.Node(leader)->ProposeAndWait(Req(0xABCDEF0123456789ull, 1, CommandType::kPut, "a", "1"), 5000ms).result,
            CommitResult::kApplied);
  ASSERT_EQ(c.Node(leader)->ProposeAndWait(Req(0xABCDEF0123456789ull, 2, CommandType::kPut, "b", "2"), 5000ms).result,
            CommitResult::kApplied);
  for (NodeId id = 1; id <= 3; ++id) {
    ASSERT_TRUE(WaitFor([&] { return c.Applied(id).size() == 2; }, 5000ms));
  }

  c.RestartAll();  // every node reloads its log from disk and replays it
  for (NodeId id = 1; id <= 3; ++id) {
    ASSERT_TRUE(WaitFor([&] { return c.Applied(id).size() == 2; }, 10000ms)) << "node " << id;
    auto entries = c.Applied(id);
    EXPECT_EQ(entries[0].command.client_id, 0xABCDEF0123456789ull);
    EXPECT_EQ(entries[0].command.seq, 1u);
    EXPECT_EQ(entries[1].command.seq, 2u);
  }
  // And a retry of request 2 sent after the restart is still recognised as a duplicate.
  NodeId new_leader = 0;
  ASSERT_TRUE(WaitFor([&] { new_leader = c.FindLeader(); return new_leader != 0; }, 5000ms));
  ASSERT_EQ(c.Node(new_leader)->ProposeAndWait(Req(0xABCDEF0123456789ull, 2, CommandType::kPut, "b", "2"), 5000ms).result,
            CommitResult::kApplied);
  ASSERT_TRUE(WaitFor([&] { return c.Applied(new_leader).size() == 3; }, 5000ms));
  Replayed r(c.Applied(new_leader));
  EXPECT_EQ(r.store.puts, 2);
  EXPECT_EQ(r.machine.DuplicatesSkipped(), 1u);
}
