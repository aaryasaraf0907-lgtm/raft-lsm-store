#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "test_cluster.h"

using namespace raftkv;
using namespace std::chrono_literals;
using raftkv::testing_util::Cluster;
using raftkv::testing_util::Put;
using raftkv::testing_util::WaitFor;

TEST(RaftCommitWait, ReturnsOnlyAfterTheWriteIsAppliedOnTheLeader) {
  Cluster c(3);
  ASSERT_TRUE(WaitFor([&] { return c.CountLeaders() == 1; }, 3000ms));
  NodeId leader = c.FindLeader();

  for (int i = 0; i < 5; ++i) {
    CommitResponse r = c.Node(leader)->ProposeAndWait(Put("k" + std::to_string(i)), 5000ms);
    ASSERT_EQ(r.result, CommitResult::kApplied);
    EXPECT_GT(r.index, 0u);
    // No waiting here on purpose: once the call returns, the leader's state machine must
    // already contain the write (and every earlier one, in order).
    auto keys = c.Keys(leader);
    ASSERT_EQ(keys.size(), static_cast<size_t>(i + 1));
    EXPECT_EQ(keys.back(), "k" + std::to_string(i));
  }
  // A majority had it, so the followers converge too.
  for (NodeId id = 1; id <= 3; ++id) {
    ASSERT_TRUE(WaitFor([&] { return c.Keys(id).size() == 5; }, 3000ms)) << "node " << id;
  }
}

TEST(RaftCommitWait, SingleNodeClusterAppliesImmediately) {
  Cluster c(1);
  ASSERT_TRUE(WaitFor([&] { return c.CountLeaders() == 1; }, 2000ms));
  CommitResponse r = c.Node(1)->ProposeAndWait(Put("solo"), 3000ms);
  EXPECT_EQ(r.result, CommitResult::kApplied);
  EXPECT_EQ(c.Keys(1), std::vector<std::string>{"solo"});
}

TEST(RaftCommitWait, FollowerSaysNotLeaderAndHintsTheLeader) {
  Cluster c(3);
  ASSERT_TRUE(WaitFor([&] { return c.CountLeaders() == 1; }, 3000ms));
  NodeId leader = c.FindLeader();
  NodeId follower = (leader % 3) + 1;
  ASSERT_TRUE(WaitFor(
      [&] { return c.Node(follower)->ProposeAndWait(Put("x"), 10ms).leader_hint == leader; },
      3000ms));
  CommitResponse r = c.Node(follower)->ProposeAndWait(Put("x"), 100ms);
  EXPECT_EQ(r.result, CommitResult::kNotLeader);
  EXPECT_EQ(r.leader_hint, leader);
  EXPECT_EQ(r.index, 0u);
}

TEST(RaftCommitWait, TimesOutWhenNoMajorityCanBeReached) {
  Cluster c(3);
  ASSERT_TRUE(WaitFor([&] { return c.CountLeaders() == 1; }, 3000ms));
  NodeId leader = c.FindLeader();
  c.net.Disconnect((leader % 3) + 1);
  c.net.Disconnect(((leader % 3) + 1) % 3 + 1);

  auto start = std::chrono::steady_clock::now();
  CommitResponse r = c.Node(leader)->ProposeAndWait(Put("nope"), 400ms);
  auto waited = std::chrono::steady_clock::now() - start;
  EXPECT_EQ(r.result, CommitResult::kTimeout);
  EXPECT_GE(waited, 350ms);
  EXPECT_LT(waited, 3000ms);
  EXPECT_TRUE(c.Keys(leader).empty()) << "an unreplicated write must not be applied";
}

TEST(RaftCommitWait, ReportsLostWhenAnotherLeaderOverwritesTheEntry) {
  Cluster c(3);
  ASSERT_TRUE(WaitFor([&] { return c.CountLeaders() == 1; }, 3000ms));
  NodeId old_leader = c.FindLeader();

  // The old leader is cut off but still believes it leads, so it accepts a write nobody else sees.
  c.net.Disconnect(old_leader);
  auto doomed = std::async(std::launch::async, [&] {
    return c.Node(old_leader)->ProposeAndWait(Put("doomed"), 20000ms);
  });

  // Meanwhile the other two elect a new leader and commit a different entry.
  ASSERT_TRUE(WaitFor([&] { return c.CountLeaders({old_leader}) == 1; }, 5000ms));
  NodeId new_leader = c.FindLeader({old_leader});
  ASSERT_EQ(c.Node(new_leader)->ProposeAndWait(Put("y"), 5000ms).result, CommitResult::kApplied);

  // When the old leader rejoins, its uncommitted entry is overwritten and the waiter is told so.
  c.net.Reconnect(old_leader);
  ASSERT_EQ(doomed.wait_for(10s), std::future_status::ready);
  CommitResponse r = doomed.get();
  EXPECT_EQ(r.result, CommitResult::kLost);

  // The lost write never shows up anywhere.
  for (NodeId id = 1; id <= 3; ++id) {
    ASSERT_TRUE(WaitFor([&] { return c.Keys(id) == std::vector<std::string>{"y"}; }, 5000ms))
        << "node " << id;
  }
}

TEST(RaftCommitWait, StoppingTheNodeReleasesBlockedCallers) {
  Cluster c(3);
  ASSERT_TRUE(WaitFor([&] { return c.CountLeaders() == 1; }, 3000ms));
  NodeId leader = c.FindLeader();
  c.net.Disconnect(leader);  // nothing can commit, so the call would block for a long time

  auto blocked = std::async(std::launch::async, [&] {
    return c.Node(leader)->ProposeAndWait(Put("stuck"), 30000ms);
  });
  std::this_thread::sleep_for(200ms);
  ASSERT_EQ(blocked.wait_for(0ms), std::future_status::timeout) << "should still be waiting";

  c.Node(leader)->Stop();
  ASSERT_EQ(blocked.wait_for(3s), std::future_status::ready) << "Stop() must wake the waiter";
  EXPECT_EQ(blocked.get().result, CommitResult::kStopped);
  EXPECT_EQ(c.Node(leader)->ProposeAndWait(Put("after"), 100ms).result, CommitResult::kStopped);
}

TEST(RaftCommitWait, ManyConcurrentWritersAllGetAnswered) {
  Cluster c(3);
  ASSERT_TRUE(WaitFor([&] { return c.CountLeaders() == 1; }, 3000ms));
  NodeId leader = c.FindLeader();

  constexpr int kThreads = 8;
  constexpr int kWritesEach = 10;
  std::atomic<int> failures{0};
  std::vector<std::thread> writers;
  for (int t = 0; t < kThreads; ++t) {
    writers.emplace_back([&, t] {
      for (int j = 0; j < kWritesEach; ++j) {
        std::string key = "t" + std::to_string(t) + "_" + std::to_string(j);
        if (c.Node(leader)->ProposeAndWait(Put(key), 10000ms).result != CommitResult::kApplied) {
          ++failures;
        }
      }
    });
  }
  for (auto& w : writers) w.join();
  EXPECT_EQ(failures.load(), 0);

  // Every write applied exactly once, and each writer's own writes stayed in order.
  for (NodeId id = 1; id <= 3; ++id) {
    ASSERT_TRUE(WaitFor([&] { return c.Keys(id).size() == kThreads * kWritesEach; }, 8000ms))
        << "node " << id;
  }
  auto keys = c.Keys(leader);
  EXPECT_EQ(std::set<std::string>(keys.begin(), keys.end()).size(), keys.size()) << "duplicates";
  for (int t = 0; t < kThreads; ++t) {
    int last = -1;
    for (const auto& k : keys) {
      std::string prefix = "t" + std::to_string(t) + "_";
      if (k.rfind(prefix, 0) == 0) {
        int j = std::stoi(k.substr(prefix.size()));
        EXPECT_GT(j, last) << "writer " << t << " applied out of order";
        last = j;
      }
    }
  }
  EXPECT_EQ(c.Keys(1), c.Keys(2));
  EXPECT_EQ(c.Keys(2), c.Keys(3));
}
