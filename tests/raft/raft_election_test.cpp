#include <gtest/gtest.h>

#include <chrono>
#include <functional>
#include <memory>
#include <set>
#include <thread>
#include <vector>

#include "test_cluster.h"

using namespace raftkv;
using raftkv::testing_util::Cluster;
using raftkv::testing_util::WaitFor;
using namespace std::chrono_literals;


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
