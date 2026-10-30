// Membership changes, one server at a time (Raft thesis, chapter 4).
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <functional>
#include <set>
#include <string>
#include <thread>

#include "test_cluster.h"

using namespace raftkv;
using namespace raftkv::testing_util;
using namespace std::chrono_literals;

namespace {

NodeId WaitForLeader(Cluster& c, const std::set<NodeId>& exclude = {}) {
  NodeId leader = 0;
  WaitFor([&] { return (leader = c.FindLeader(exclude)) != 0; }, 10000ms);
  return leader;
}

// Commits one write through whoever is leader, retrying across elections.
bool Write(Cluster& c, const std::string& key) {
  for (int attempt = 0; attempt < 60; ++attempt) {
    const NodeId leader = c.FindLeader();
    if (leader != 0 && c.Node(leader)->ProposeAndWait(Put(key), 2000ms).result == CommitResult::kApplied) return true;
    std::this_thread::sleep_for(50ms);
  }
  return false;
}

bool Has(Cluster& c, NodeId id, const std::string& key) {
  const auto keys = c.Keys(id);
  return std::find(keys.begin(), keys.end(), key) != keys.end();
}

std::set<NodeId> Members(Cluster& c, NodeId id) {
  std::set<NodeId> out;
  for (const auto& m : c.Node(id)->Configuration()) out.insert(m.first);
  return out;
}

// Applies a membership change, retrying while the leader answers kInProgress: a new leader refuses
// changes until it has committed an entry of its own term (Raft thesis 4.1), so a change asked for
// right after an election may have to wait a moment. KvClient retries the same way.
MembershipResult Change(const std::function<MembershipResponse()>& op) {
  const auto deadline = std::chrono::steady_clock::now() + 10000ms;
  MembershipResponse r = op();
  while (r.result == MembershipResult::kInProgress && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(20ms);
    r = op();
  }
  return r.result;
}

NodeId OtherThan(Cluster& c, std::set<NodeId> not_these) {
  for (NodeId id = 1; id <= static_cast<NodeId>(c.nodes.size()); ++id) {
    if (not_these.count(id) == 0) return id;
  }
  return 0;
}

}  // namespace

TEST(Membership, AnAddedServerCatchesUpAndBecomesAVoter) {
  Cluster c(3);
  const NodeId leader = WaitForLeader(c);
  ASSERT_NE(leader, 0u);
  for (int i = 0; i < 30; ++i) ASSERT_TRUE(Write(c, "before" + std::to_string(i)));
  const NodeId added = c.AddJoiningNode();
  EXPECT_FALSE(c.Node(added)->IsMember());
  ASSERT_EQ(Change([&] { return c.Node(leader)->AddServer(added, "", 10000ms); }), MembershipResult::kOk);
  for (NodeId id = 1; id <= added; ++id) {
    ASSERT_TRUE(WaitFor([&] { return Members(c, id) == std::set<NodeId>{1, 2, 3, 4}; }, 5000ms)) << "node " << id;
  }
  EXPECT_TRUE(c.Node(added)->IsMember());
  ASSERT_TRUE(WaitFor([&] { return Has(c, added, "before29"); }, 5000ms)) << "the new server did not catch up";
  ASSERT_TRUE(Write(c, "after"));
  EXPECT_TRUE(WaitFor([&] { return Has(c, added, "after"); }, 5000ms));
}

// With four members a majority is three. Cut off one original follower AND the new server: the
// leader plus one follower is 2 of 4 and must not commit (counted over the old 3 it would be 2 of 3).
TEST(Membership, AfterAnAddTheMajorityIsCountedOverTheNewConfiguration) {
  Cluster c(3);
  const NodeId leader = WaitForLeader(c);
  ASSERT_NE(leader, 0u);
  const NodeId added = c.AddJoiningNode();
  ASSERT_EQ(Change([&] { return c.Node(leader)->AddServer(added, "", 10000ms); }), MembershipResult::kOk);
  const NodeId a = OtherThan(c, {leader, added});
  c.net.Disconnect(a);
  c.net.Disconnect(added);
  EXPECT_NE(c.Node(leader)->ProposeAndWait(Put("blocked"), 800ms).result, CommitResult::kApplied)
      << "2 of 4 is not a majority (the new server must count)";
  c.net.HealAll();
  EXPECT_TRUE(Write(c, "unblocked"));
}

// The decisive check that a removed server no longer counts: it stays connected (and would
// acknowledge), the one remaining follower is cut off, and the leader must then NOT commit.
TEST(Membership, ARemovedFollowerNoLongerCountsAndDoesNotDisrupt) {
  Cluster c(3);
  const NodeId leader = WaitForLeader(c);
  ASSERT_NE(leader, 0u);
  ASSERT_TRUE(Write(c, "x"));
  const NodeId removed = OtherThan(c, {leader});
  const NodeId kept = OtherThan(c, {leader, removed});
  ASSERT_EQ(Change([&] { return c.Node(leader)->RemoveServer(removed, 5000ms); }), MembershipResult::kOk);
  EXPECT_EQ(Members(c, leader), (std::set<NodeId>{leader, kept}));
  const Term term = c.Node(leader)->CurrentTerm();

  c.net.Disconnect(kept);
  EXPECT_NE(c.Node(leader)->ProposeAndWait(Put("needs-kept"), 800ms).result, CommitResult::kApplied)
      << "the removed server still counted toward the majority";
  c.net.HealAll();
  ASSERT_TRUE(Write(c, "y"));

  std::this_thread::sleep_for(1500ms);  // many election timeouts for the removed server
  EXPECT_EQ(c.FindLeader(), leader) << "the removed server disrupted the cluster";
  EXPECT_EQ(c.Node(leader)->CurrentTerm(), term);
  EXPECT_NE(c.Node(removed)->GetRole(), Role::kLeader);
}

// Removing servers shrinks the majority. Five servers, two removed: three remain, so the leader plus
// one other is a majority even with a third server unreachable (counted over the old five, 2 of 5
// would not be).
TEST(Membership, RemovingServersShrinksTheMajority) {
  Cluster c(5);
  const NodeId leader = WaitForLeader(c);
  ASSERT_NE(leader, 0u);
  const NodeId r1 = OtherThan(c, {leader});
  const NodeId r2 = OtherThan(c, {leader, r1});
  ASSERT_EQ(Change([&] { return c.Node(leader)->RemoveServer(r1, 5000ms); }), MembershipResult::kOk);
  ASSERT_EQ(Change([&] { return c.Node(leader)->RemoveServer(r2, 5000ms); }), MembershipResult::kOk);
  const NodeId down = OtherThan(c, {leader, r1, r2});
  c.net.Disconnect(down);
  EXPECT_EQ(c.Node(leader)->ProposeAndWait(Put("two-of-three"), 3000ms).result, CommitResult::kApplied)
      << "with three members, the leader and one other are a majority";
}

TEST(Membership, TheLeaderCanRemoveItselfAndHandsOver) {
  Cluster c(3);
  const NodeId old_leader = WaitForLeader(c);
  ASSERT_NE(old_leader, 0u);
  ASSERT_TRUE(Write(c, "before"));
  ASSERT_EQ(Change([&] { return c.Node(old_leader)->RemoveServer(old_leader, 5000ms); }), MembershipResult::kOk);
  const NodeId new_leader = WaitForLeader(c, {old_leader});
  ASSERT_NE(new_leader, 0u) << "the remaining servers must elect a leader";
  EXPECT_FALSE(c.Node(old_leader)->IsMember());
  ASSERT_TRUE(Write(c, "after"));
  std::this_thread::sleep_for(1000ms);
  EXPECT_NE(c.Node(old_leader)->GetRole(), Role::kLeader) << "a removed server must never lead again";
}

// The scenario membership changes exist for: a machine dies and is replaced by a new one.
TEST(Membership, AFailedServerIsReplacedByANewOne) {
  Cluster c(3);
  const NodeId leader = WaitForLeader(c);
  ASSERT_NE(leader, 0u);
  for (int i = 0; i < 20; ++i) ASSERT_TRUE(Write(c, "k" + std::to_string(i)));
  const NodeId failed = OtherThan(c, {leader});
  c.net.Disconnect(failed);
  ASSERT_EQ(Change([&] { return c.Node(leader)->RemoveServer(failed, 5000ms); }), MembershipResult::kOk);
  const NodeId fresh = c.AddJoiningNode();
  ASSERT_EQ(Change([&] { return c.Node(leader)->AddServer(fresh, "", 10000ms); }), MembershipResult::kOk);
  const NodeId kept = OtherThan(c, {leader, failed, fresh});
  EXPECT_EQ(Members(c, leader), (std::set<NodeId>{leader, kept, fresh}));
  ASSERT_TRUE(Write(c, "after"));
  EXPECT_TRUE(WaitFor([&] { return Has(c, fresh, "k19") && Has(c, fresh, "after"); }, 5000ms));
}

// Raft thesis 4.1 (the erratum's fix): a new leader refuses membership changes until it has
// committed an entry of its own term. Every message is delayed 50 ms, so the no-op a new leader
// appends needs about 100 ms to commit; a change asked for in that window must be refused, and
// accepted once an entry of the term has committed.
TEST(Membership, ANewLeaderRefusesChangesUntilItHasCommittedAnEntryOfItsTerm) {
  Cluster c(3);
  FaultPlan plan;
  plan.min_delay_ms = 50;
  plan.max_delay_ms = 50;
  c.net.SetFaults(plan);
  const NodeId leader = WaitForLeader(c);
  ASSERT_NE(leader, 0u);
  EXPECT_EQ(c.Node(leader)->RemoveServer(OtherThan(c, {leader}), 1000ms).result, MembershipResult::kInProgress)
      << "a leader that has not committed an entry of its term changed the configuration";
  ASSERT_TRUE(Write(c, "x"));
  const NodeId now = WaitForLeader(c);
  EXPECT_EQ(Change([&] { return c.Node(now)->RemoveServer(OtherThan(c, {now}), 5000ms); }), MembershipResult::kOk);
}

TEST(Membership, OneChangeAtATimeAndAFailedCatchUpChangesNothing) {
  Cluster c(3);
  const NodeId leader = WaitForLeader(c);
  ASSERT_NE(leader, 0u);
  ASSERT_TRUE(Write(c, "x"));
  const NodeId unreachable = c.AddJoiningNode();
  c.net.Disconnect(unreachable);  // it can never catch up
  MembershipResponse add;
  std::thread adder([&] { add = c.Node(leader)->AddServer(unreachable, "", 1500ms); });
  std::this_thread::sleep_for(300ms);
  EXPECT_EQ(c.Node(leader)->RemoveServer(OtherThan(c, {leader, unreachable}), 1000ms).result,
            MembershipResult::kInProgress) << "a second change while one is in progress";
  adder.join();
  EXPECT_EQ(add.result, MembershipResult::kCatchUpFailed);
  EXPECT_EQ(Members(c, leader), (std::set<NodeId>{1, 2, 3})) << "a failed add must change nothing";
}

TEST(Membership, ANodeThatIsNotAMemberNeverStartsAnElection) {
  Cluster c(3);
  ASSERT_NE(WaitForLeader(c), 0u);
  const NodeId outsider = c.AddJoiningNode();
  std::this_thread::sleep_for(1500ms);
  EXPECT_EQ(c.Node(outsider)->CurrentTerm(), 0u) << "a non-member campaigned";
  EXPECT_EQ(c.Node(outsider)->GetRole(), Role::kFollower);
}

// The configuration must survive snapshots that drop the log entries carrying it, and restarts.
TEST(Membership, TheConfigurationSurvivesSnapshotsAndAFullRestart) {
  Cluster c(3, /*persistent=*/true, [](RaftConfig& cfg) {
    cfg.snapshot_threshold_entries = 20;
    cfg.snapshot_trailing_entries = 5;
  });
  NodeId leader = WaitForLeader(c);
  ASSERT_NE(leader, 0u);
  for (int i = 0; i < 30; ++i) ASSERT_TRUE(Write(c, "a" + std::to_string(i)));
  ASSERT_TRUE(WaitFor([&] { return c.Node(leader)->SnapshotIndex() > 0; }, 5000ms));
  const NodeId added = c.AddJoiningNode();  // must catch up from a snapshot of the OLD configuration
  ASSERT_EQ(Change([&] { return c.Node(leader)->AddServer(added, "", 10000ms); }), MembershipResult::kOk);
  for (int i = 0; i < 40; ++i) ASSERT_TRUE(Write(c, "b" + std::to_string(i)));
  for (NodeId id = 1; id <= added; ++id) {
    ASSERT_TRUE(WaitFor([&] { return c.Node(id)->SnapshotIndex() >= 60; }, 10000ms))
        << "node " << id << " never took a snapshot after the configuration change";
  }
  c.RestartAll();
  for (NodeId id = 1; id <= added; ++id) {
    EXPECT_EQ(Members(c, id), (std::set<NodeId>{1, 2, 3, 4})) << "node " << id << " lost the configuration";
  }
  ASSERT_NE(WaitForLeader(c), 0u);
  ASSERT_TRUE(Write(c, "after-restart"));
  EXPECT_TRUE(WaitFor([&] { return Has(c, added, "after-restart"); }, 5000ms));
}

TEST(Membership, ConfigurationEntriesNeverReachTheStateMachine) {
  Cluster c(3);
  const NodeId leader = WaitForLeader(c);
  ASSERT_NE(leader, 0u);
  const NodeId added = c.AddJoiningNode();
  ASSERT_EQ(Change([&] { return c.Node(leader)->AddServer(added, "", 10000ms); }), MembershipResult::kOk);
  ASSERT_TRUE(Write(c, "x"));
  for (NodeId id = 1; id <= added; ++id) {
    ASSERT_TRUE(WaitFor([&] { return Has(c, id, "x"); }, 5000ms));
    for (const LogEntry& e : c.Applied(id)) EXPECT_NE(e.command.type, CommandType::kConfig) << "node " << id;
  }
}
