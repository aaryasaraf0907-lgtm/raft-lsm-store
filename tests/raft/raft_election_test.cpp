#include <gtest/gtest.h>

#include <chrono>
#include <functional>
#include <memory>
#include <set>
#include <thread>
#include <vector>

#include "raft/fake_transport.h"
#include "raft/raft_node.h"

using namespace raftkv;
using namespace std::chrono_literals;

namespace {

bool WaitFor(const std::function<bool()>& pred, std::chrono::milliseconds timeout) {
  auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (pred()) return true;
    std::this_thread::sleep_for(10ms);
  }
  return pred();
}

// n nodes (ids 1..n) on one FakeNetwork.
// Member order matters: nodes are destroyed before transports, transports before the network.
struct Cluster {
  FakeNetwork net;
  std::vector<std::unique_ptr<FakeTransport>> transports;
  std::vector<std::unique_ptr<RaftNodeImpl>> nodes;

  explicit Cluster(int n) {
    std::vector<NodeId> ids;
    for (int i = 1; i <= n; ++i) ids.push_back(static_cast<NodeId>(i));
    for (NodeId id : ids) {
      std::vector<NodeId> peers;
      for (NodeId p : ids) {
        if (p != id) peers.push_back(p);
      }
      transports.push_back(net.MakeTransport(id));
      RaftConfig cfg;
      cfg.random_seed = 42;
      nodes.push_back(std::make_unique<RaftNodeImpl>(id, peers, transports.back().get(), cfg));
    }
    for (auto& node : nodes) node->Start();
  }

  ~Cluster() {
    for (auto& node : nodes) node->Stop();
    net.Shutdown();
  }

  RaftNodeImpl* Node(NodeId id) { return nodes[id - 1].get(); }

  int CountLeaders(const std::set<NodeId>& exclude = {}) {
    int count = 0;
    for (auto& node : nodes) {
      if (exclude.count(node->Id()) == 0 && node->GetRole() == Role::kLeader) ++count;
    }
    return count;
  }

  NodeId FindLeader(const std::set<NodeId>& exclude = {}) {
    for (auto& node : nodes) {
      if (exclude.count(node->Id()) == 0 && node->GetRole() == Role::kLeader) return node->Id();
    }
    return 0;
  }
};

}  // namespace

TEST(RaftElection, ElectsExactlyOneLeader) {
  Cluster c(3);
  ASSERT_TRUE(WaitFor([&] { return c.CountLeaders() == 1; }, 3000ms));
  std::this_thread::sleep_for(500ms);  // leadership must be stable, not flapping
  EXPECT_EQ(c.CountLeaders(), 1);
}

TEST(RaftElection, ReelectsAfterLeaderIsDisconnected) {
  Cluster c(3);
  ASSERT_TRUE(WaitFor([&] { return c.CountLeaders() == 1; }, 3000ms));
  NodeId old_leader = c.FindLeader();
  Term old_term = c.Node(old_leader)->CurrentTerm();

  c.net.Disconnect(old_leader);  // "kill" the leader
  ASSERT_TRUE(WaitFor([&] { return c.CountLeaders({old_leader}) == 1; }, 3000ms));
  NodeId new_leader = c.FindLeader({old_leader});
  EXPECT_NE(new_leader, old_leader);
  EXPECT_GT(c.Node(new_leader)->CurrentTerm(), old_term);

  // The old leader comes back, sees the higher term and steps down.
  c.net.Reconnect(old_leader);
  ASSERT_TRUE(WaitFor(
      [&] { return c.CountLeaders() == 1 && !c.Node(old_leader)->IsLeader(); }, 3000ms));
}

TEST(RaftElection, NoLeaderWithoutMajority) {
  Cluster c(3);
  ASSERT_TRUE(WaitFor([&] { return c.CountLeaders() == 1; }, 3000ms));
  NodeId leader = c.FindLeader();
  NodeId other = (leader % 3) + 1;  // some other node
  NodeId survivor = 6 - leader - other;  // ids are 1+2+3 = 6

  c.net.Disconnect(leader);
  c.net.Disconnect(other);
  // The survivor is alone (1 of 3): it keeps trying but can never win.
  std::this_thread::sleep_for(1500ms);
  EXPECT_NE(c.Node(survivor)->GetRole(), Role::kLeader);
}

TEST(RaftElection, SingleNodeClusterElectsItself) {
  Cluster c(1);
  ASSERT_TRUE(WaitFor([&] { return c.CountLeaders() == 1; }, 2000ms));
}

TEST(RaftElection, FollowerRejectsProposeAndHintsLeader) {
  Cluster c(3);
  ASSERT_TRUE(WaitFor([&] { return c.CountLeaders() == 1; }, 3000ms));
  NodeId leader = c.FindLeader();
  NodeId follower = (leader % 3) + 1;
  // Wait until the follower has heard from the leader.
  Command cmd{CommandType::kPut, "k", "v"};
  ASSERT_TRUE(WaitFor([&] { return c.Node(follower)->Propose(cmd).leader_hint == leader; }, 2000ms));
  EXPECT_EQ(c.Node(follower)->Propose(cmd).result, ProposeResult::kNotLeader);
  EXPECT_EQ(c.Node(leader)->Propose(cmd).result, ProposeResult::kAccepted);
}
