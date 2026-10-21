#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "test_cluster.h"

using namespace raftkv;
using namespace std::chrono_literals;
using raftkv::testing_util::Cluster;
using raftkv::testing_util::WaitFor;

namespace {

Command Put(const std::string& key) { return Command{CommandType::kPut, key, "v"}; }

bool Contains(const std::vector<std::string>& keys, const std::string& key) {
  for (const auto& k : keys) {
    if (k == key) return true;
  }
  return false;
}

// Keeps proposing `key` on whichever node is leader until every node has applied it.
bool ProposeUntilApplied(Cluster& c, const std::string& key, std::chrono::milliseconds timeout) {
  auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    NodeId leader = c.FindLeader();
    if (leader != 0 && c.Node(leader)->Propose(Put(key)).result == ProposeResult::kAccepted) {
      bool everywhere = WaitFor(
          [&] {
            for (auto& node : c.nodes) {
              if (!Contains(c.Keys(node->Id()), key)) return false;
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

TEST(RaftReplication, WriteReachesEveryNode) {
  Cluster c(3);
  ASSERT_TRUE(WaitFor([&] { return c.CountLeaders() == 1; }, 3000ms));
  NodeId leader = c.FindLeader();
  ASSERT_EQ(c.Node(leader)->Propose(Put("k1")).result, ProposeResult::kAccepted);
  for (NodeId id = 1; id <= 3; ++id) {
    ASSERT_TRUE(WaitFor([&] { return c.Keys(id) == std::vector<std::string>{"k1"}; }, 3000ms))
        << "node " << id << " did not apply the entry";
  }
}

TEST(RaftReplication, ManyWritesApplyInOrderEverywhere) {
  Cluster c(3);
  ASSERT_TRUE(WaitFor([&] { return c.CountLeaders() == 1; }, 3000ms));
  NodeId leader = c.FindLeader();
  std::vector<std::string> expected;
  for (int i = 0; i < 50; ++i) {
    expected.push_back("key" + std::to_string(i));
    ASSERT_EQ(c.Node(leader)->Propose(Put(expected.back())).result, ProposeResult::kAccepted);
  }
  for (NodeId id = 1; id <= 3; ++id) {
    ASSERT_TRUE(WaitFor([&] { return c.Keys(id) == expected; }, 5000ms)) << "node " << id;
  }
  // Log indexes handed to the callback must strictly increase.
  for (NodeId id = 1; id <= 3; ++id) {
    auto entries = c.Applied(id);
    for (size_t i = 1; i < entries.size(); ++i) {
      EXPECT_LT(entries[i - 1].index, entries[i].index);
    }
  }
}

TEST(RaftReplication, SingleNodeClusterCommitsImmediately) {
  Cluster c(1);
  ASSERT_TRUE(WaitFor([&] { return c.CountLeaders() == 1; }, 2000ms));
  ASSERT_EQ(c.Node(1)->Propose(Put("solo")).result, ProposeResult::kAccepted);
  ASSERT_TRUE(WaitFor([&] { return c.Keys(1) == std::vector<std::string>{"solo"}; }, 2000ms));
}

TEST(RaftReplication, NothingCommitsWithoutMajority) {
  Cluster c(3);
  ASSERT_TRUE(WaitFor([&] { return c.CountLeaders() == 1; }, 3000ms));
  NodeId leader = c.FindLeader();
  NodeId f1 = (leader % 3) + 1;
  NodeId f2 = (f1 % 3) + 1;

  c.net.Disconnect(f1);
  c.net.Disconnect(f2);
  ASSERT_EQ(c.Node(leader)->Propose(Put("unreplicated")).result, ProposeResult::kAccepted);
  std::this_thread::sleep_for(800ms);
  EXPECT_TRUE(c.Keys(leader).empty()) << "entry was applied without a majority";

  // Heal the partition. The uncommitted entry may legitimately be lost (another node can win
  // the election), but all nodes must end up with exactly the same applied sequence.
  c.net.Reconnect(f1);
  c.net.Reconnect(f2);
  ASSERT_TRUE(ProposeUntilApplied(c, "after", 8000ms));
  ASSERT_TRUE(WaitFor(
      [&] { return c.Keys(1) == c.Keys(2) && c.Keys(2) == c.Keys(3); }, 3000ms));
}

TEST(RaftReplication, DisconnectedFollowerCatchesUp) {
  Cluster c(3);
  ASSERT_TRUE(WaitFor([&] { return c.CountLeaders() == 1; }, 3000ms));
  NodeId leader = c.FindLeader();
  NodeId lagging = (leader % 3) + 1;
  NodeId healthy = (lagging % 3) + 1;

  c.net.Disconnect(lagging);
  std::vector<std::string> expected;
  for (int i = 0; i < 10; ++i) {
    expected.push_back("a" + std::to_string(i));
    ASSERT_EQ(c.Node(leader)->Propose(Put(expected.back())).result, ProposeResult::kAccepted);
  }
  ASSERT_TRUE(WaitFor([&] { return c.Keys(leader) == expected && c.Keys(healthy) == expected; },
                      5000ms));
  EXPECT_TRUE(c.Keys(lagging).empty());

  c.net.Reconnect(lagging);
  for (NodeId id = 1; id <= 3; ++id) {
    ASSERT_TRUE(WaitFor([&] { return c.Keys(id) == expected; }, 8000ms)) << "node " << id;
  }
}

TEST(RaftReplication, CommittedEntriesSurviveLeaderFailure) {
  Cluster c(3);
  ASSERT_TRUE(WaitFor([&] { return c.CountLeaders() == 1; }, 3000ms));
  NodeId old_leader = c.FindLeader();
  ASSERT_EQ(c.Node(old_leader)->Propose(Put("x")).result, ProposeResult::kAccepted);
  for (NodeId id = 1; id <= 3; ++id) {
    ASSERT_TRUE(WaitFor([&] { return c.Keys(id) == std::vector<std::string>{"x"}; }, 3000ms));
  }

  c.net.Disconnect(old_leader);  // kill the leader
  ASSERT_TRUE(WaitFor([&] { return c.CountLeaders({old_leader}) == 1; }, 3000ms));
  NodeId new_leader = c.FindLeader({old_leader});
  ASSERT_EQ(c.Node(new_leader)->Propose(Put("y")).result, ProposeResult::kAccepted);

  std::vector<std::string> expected = {"x", "y"};
  for (NodeId id = 1; id <= 3; ++id) {
    if (id == old_leader) continue;
    ASSERT_TRUE(WaitFor([&] { return c.Keys(id) == expected; }, 5000ms)) << "node " << id;
  }

  c.net.Reconnect(old_leader);  // the old leader rejoins and catches up
  ASSERT_TRUE(WaitFor([&] { return c.Keys(old_leader) == expected; }, 8000ms));
}

namespace {

// True if one sequence is a prefix of the other (nodes may lag, but must never disagree).
bool PrefixConsistent(const std::vector<std::string>& a, const std::vector<std::string>& b) {
  size_t n = std::min(a.size(), b.size());
  for (size_t i = 0; i < n; ++i) {
    if (a[i] != b[i]) return false;
  }
  return true;
}

}  // namespace

// Chaos test: 5 nodes, random disconnects/reconnects while writes flow. Whatever happens,
// no two nodes may ever apply different entries at the same position, and nothing may be
// applied twice. This is the core safety property of Raft.
TEST(RaftReplication, RandomFaultsNeverMakeNodesDisagree) {
  Cluster c(5);
  std::mt19937 rng(12345);
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

  auto deadline = std::chrono::steady_clock::now() + 3500ms;
  while (std::chrono::steady_clock::now() < deadline) {
    switch (rng() % 3) {
      case 0:  // cut a random node off
        c.net.Disconnect(static_cast<NodeId>(rng() % 5 + 1));
        break;
      case 1:  // bring a random node back
        c.net.Reconnect(static_cast<NodeId>(rng() % 5 + 1));
        break;
      default: {  // write through whoever currently thinks it is leader
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

  // Heal everything; the cluster must recover and converge.
  for (NodeId id = 1; id <= 5; ++id) c.net.Reconnect(id);
  ASSERT_TRUE(ProposeUntilApplied(c, "final", 12000ms));
  ASSERT_TRUE(WaitFor(
      [&] {
        auto first = c.Keys(1);
        for (NodeId id = 2; id <= 5; ++id) {
          if (c.Keys(id) != first) return false;
        }
        return true;
      },
      5000ms));

  // Every applied entry was really proposed, and none was applied twice.
  std::vector<std::string> keys = c.Keys(1);
  std::set<std::string> seen;
  for (const auto& k : keys) {
    EXPECT_TRUE(k == "final" || proposed.count(k) == 1) << "unknown entry " << k;
    EXPECT_TRUE(seen.insert(k).second) << "entry applied twice: " << k;
  }
}
