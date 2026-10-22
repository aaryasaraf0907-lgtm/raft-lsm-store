#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "test_cluster.h"

using namespace raftkv;
using namespace std::chrono_literals;
using raftkv::testing_util::Cluster;
using raftkv::testing_util::PrefixConsistent;
using raftkv::testing_util::ProposeUntilApplied;
using raftkv::testing_util::Put;
using raftkv::testing_util::TempDir;
using raftkv::testing_util::WaitFor;

namespace {

// A node (id 1, peers 2 and 3) wired to a file in `dir` and never started. Destroying it and
// building another on the same directory is a crash followed by a reboot.
struct SoloNode {
  FakeNetwork net;
  std::unique_ptr<FakeTransport> transport;
  FileRaftStorage storage;
  RaftNodeImpl node;

  explicit SoloNode(const std::string& dir)
      : transport(net.MakeTransport(1)),
        storage(dir, /*sync=*/false),
        node(1, {2, 3}, transport.get(), RaftConfig(), &storage) {}
};

RequestVoteRequest Vote(Term term, NodeId candidate) {
  return RequestVoteRequest{term, candidate, 0, 0};
}

LogEntry Entry(LogIndex index, Term term, const std::string& key) {
  return LogEntry{term, index, Command{CommandType::kPut, key, "v"}};
}

AppendEntriesRequest Append(Term term, LogIndex prev_index, Term prev_term,
                            std::vector<LogEntry> entries) {
  return AppendEntriesRequest{term, 2, prev_index, prev_term, std::move(entries), 0};
}

}  // namespace

// ---- single-node behaviour, called directly (no threads, no timing) ----

TEST(RaftPersistence, NodeStartsFromSavedTermAndVote) {
  TempDir dir;
  {
    FileRaftStorage s(dir.path.string());
    PersistentState st;
    ASSERT_TRUE(s.Load(&st));
    ASSERT_TRUE(s.SaveHardState(7, 2));
  }
  SoloNode n(dir.path.string());
  EXPECT_EQ(n.node.CurrentTerm(), 7u);
  EXPECT_FALSE(n.node.HandleRequestVote(Vote(7, 3)).vote_granted) << "already voted for 2";
  EXPECT_TRUE(n.node.HandleRequestVote(Vote(7, 2)).vote_granted);
}

TEST(RaftPersistence, VoteSurvivesRestartSoNoDoubleVoting) {
  TempDir dir;
  {
    SoloNode n(dir.path.string());
    RequestVoteReply reply = n.node.HandleRequestVote(Vote(5, 2));
    EXPECT_TRUE(reply.vote_granted);
    EXPECT_EQ(reply.term, 5u);
  }  // crash
  SoloNode n(dir.path.string());
  EXPECT_EQ(n.node.CurrentTerm(), 5u);
  RequestVoteReply other = n.node.HandleRequestVote(Vote(5, 3));
  EXPECT_FALSE(other.vote_granted) << "voted twice in the same term after a restart";
  EXPECT_EQ(other.term, 5u);
  EXPECT_TRUE(n.node.HandleRequestVote(Vote(5, 2)).vote_granted);  // same candidate: still fine
  EXPECT_TRUE(n.node.HandleRequestVote(Vote(6, 3)).vote_granted);  // a newer term is a fresh vote
}

TEST(RaftPersistence, LogSurvivesRestart) {
  TempDir dir;
  {
    SoloNode n(dir.path.string());
    AppendEntriesReply r = n.node.HandleAppendEntries(Append(2, 0, 0, {Entry(1, 2, "a"), Entry(2, 2, "b")}));
    ASSERT_TRUE(r.success);
  }  // crash
  SoloNode n(dir.path.string());
  EXPECT_EQ(n.node.CurrentTerm(), 2u);
  // A heartbeat that refers to entry 2 only succeeds if the log was really restored.
  EXPECT_TRUE(n.node.HandleAppendEntries(Append(2, 2, 2, {})).success);
  EXPECT_FALSE(n.node.HandleAppendEntries(Append(2, 3, 2, {})).success) << "no entry 3";
}

TEST(RaftPersistence, TruncationSurvivesRestart) {
  TempDir dir;
  {
    SoloNode n(dir.path.string());
    ASSERT_TRUE(n.node.HandleAppendEntries(Append(2, 0, 0, {Entry(1, 2, "a"), Entry(2, 2, "b"), Entry(3, 2, "c")})).success);
    // A new leader in term 3 overwrites entries 2 and 3 with its own entry 2.
    ASSERT_TRUE(n.node.HandleAppendEntries(Append(3, 1, 2, {Entry(2, 3, "b2")})).success);
  }  // crash
  SoloNode n(dir.path.string());
  EXPECT_EQ(n.node.CurrentTerm(), 3u);
  EXPECT_TRUE(n.node.HandleAppendEntries(Append(3, 2, 3, {})).success) << "entry 2 should be the term-3 one";
  EXPECT_FALSE(n.node.HandleAppendEntries(Append(3, 2, 2, {})).success) << "old entry 2 must be gone";
  EXPECT_FALSE(n.node.HandleAppendEntries(Append(3, 3, 2, {})).success) << "old entry 3 must be gone";
}

// ---- whole clusters that crash and reboot from disk ----

TEST(RaftPersistence, RestartedFollowerReplaysTheCommittedLog) {
  Cluster c(3, /*persistent=*/true);
  ASSERT_TRUE(WaitFor([&] { return c.CountLeaders() == 1; }, 3000ms));
  NodeId leader = c.FindLeader();
  std::vector<std::string> expected;
  for (int i = 0; i < 5; ++i) {
    expected.push_back("k" + std::to_string(i));
    ASSERT_EQ(c.Node(leader)->Propose(Put(expected.back())).result, ProposeResult::kAccepted);
  }
  for (NodeId id = 1; id <= 3; ++id) {
    ASSERT_TRUE(WaitFor([&] { return c.Keys(id) == expected; }, 5000ms));
  }

  NodeId follower = (leader % 3) + 1;
  c.Restart(follower);  // its state machine is empty again; the log comes back from disk
  ASSERT_TRUE(WaitFor([&] { return c.Keys(follower) == expected; }, 8000ms))
      << "restarted follower did not recover the committed entries";

  ASSERT_TRUE(ProposeUntilApplied(c, "after", 8000ms));  // and it is a working member again
}

TEST(RaftPersistence, WholeClusterRestartLosesNothing) {
  Cluster c(3, /*persistent=*/true);
  ASSERT_TRUE(WaitFor([&] { return c.CountLeaders() == 1; }, 3000ms));
  NodeId leader = c.FindLeader();
  std::vector<std::string> expected;
  for (int i = 0; i < 8; ++i) {
    expected.push_back("k" + std::to_string(i));
    ASSERT_EQ(c.Node(leader)->Propose(Put(expected.back())).result, ProposeResult::kAccepted);
  }
  for (NodeId id = 1; id <= 3; ++id) {
    ASSERT_TRUE(WaitFor([&] { return c.Keys(id) == expected; }, 5000ms));
  }

  c.RestartAll();  // every node loses its memory at once
  ASSERT_TRUE(WaitFor([&] { return c.CountLeaders() == 1; }, 5000ms));
  for (NodeId id = 1; id <= 3; ++id) {
    ASSERT_TRUE(WaitFor([&] { return c.Keys(id) == expected; }, 8000ms))
        << "node " << id << " lost committed data across the restart";
  }
  ASSERT_TRUE(ProposeUntilApplied(c, "after", 8000ms));
}

TEST(RaftPersistence, RestartedLeaderRejoinsAndClusterKeepsWorking) {
  Cluster c(3, /*persistent=*/true);
  ASSERT_TRUE(WaitFor([&] { return c.CountLeaders() == 1; }, 3000ms));
  NodeId leader = c.FindLeader();
  ASSERT_EQ(c.Node(leader)->Propose(Put("x")).result, ProposeResult::kAccepted);
  for (NodeId id = 1; id <= 3; ++id) {
    ASSERT_TRUE(WaitFor([&] { return c.Keys(id) == std::vector<std::string>{"x"}; }, 5000ms));
  }

  c.Restart(leader);
  ASSERT_TRUE(ProposeUntilApplied(c, "y", 10000ms));
  std::vector<std::string> expected = {"x", "y"};
  for (NodeId id = 1; id <= 3; ++id) {
    ASSERT_TRUE(WaitFor([&] { return c.Keys(id) == expected; }, 8000ms)) << "node " << id;
  }
}

// Chaos test with crashes: partitions AND node restarts at random while writes flow. No two
// nodes may ever disagree about what is committed, and nothing may be applied twice.
TEST(RaftPersistence, RandomFaultsAndCrashesNeverMakeNodesDisagree) {
  Cluster c(5, /*persistent=*/true);
  std::mt19937 rng(777);
  std::set<std::string> proposed;
  int next_key = 0;

  auto check_consistency = [&] {
    std::vector<std::vector<std::string>> all;
    for (NodeId id = 1; id <= 5; ++id) all.push_back(c.Keys(id));
    for (size_t i = 0; i < all.size(); ++i) {
      for (size_t j = i + 1; j < all.size(); ++j) {
        ASSERT_TRUE(PrefixConsistent(all[i], all[j])) << "nodes " << i + 1 << " and " << j + 1
                                                      << " diverged";
      }
    }
  };

  auto deadline = std::chrono::steady_clock::now() + 4000ms;
  while (std::chrono::steady_clock::now() < deadline) {
    switch (rng() % 5) {
      case 0:
        c.net.Disconnect(static_cast<NodeId>(rng() % 5 + 1));
        break;
      case 1:
        c.net.Reconnect(static_cast<NodeId>(rng() % 5 + 1));
        break;
      case 2:  // crash and reboot a random node
        c.Restart(static_cast<NodeId>(rng() % 5 + 1));
        break;
      default: {
        NodeId leader = c.FindLeader();
        if (leader != 0) {
          std::string key = "w" + std::to_string(next_key++);
          if (c.Node(leader)->Propose(Put(key)).result == ProposeResult::kAccepted) {
            proposed.insert(key);
          }
        }
      }
    }
    check_consistency();
    if (HasFatalFailure()) return;
    std::this_thread::sleep_for(std::chrono::milliseconds(20 + rng() % 40));
  }

  for (NodeId id = 1; id <= 5; ++id) c.net.Reconnect(id);
  ASSERT_TRUE(ProposeUntilApplied(c, "final", 15000ms));
  ASSERT_TRUE(WaitFor(
      [&] {
        auto first = c.Keys(1);
        for (NodeId id = 2; id <= 5; ++id) {
          if (c.Keys(id) != first) return false;
        }
        return true;
      },
      8000ms));

  std::set<std::string> seen;
  for (const auto& k : c.Keys(1)) {
    EXPECT_TRUE(k == "final" || proposed.count(k) == 1) << "unknown entry " << k;
    EXPECT_TRUE(seen.insert(k).second) << "entry applied twice: " << k;
  }
}
