// AppendEntries is capped by bytes, not only by entry count: a follower that missed a lot of
// large committed data must still catch up over a transport with a message size limit.
#include <gtest/gtest.h>

#include <set>

#include "raft/message_size.h"
#include "test_cluster.h"

using namespace raftkv;
using namespace raftkv::testing_util;
using namespace std::chrono_literals;

namespace {

constexpr size_t kMB = 1024 * 1024;
constexpr size_t kGrpcDefaultLimit = 4 * kMB;

Command BigPut(const std::string& key, size_t value_bytes) {
  return Command{CommandType::kPut, key, std::string(value_bytes, 'v')};
}

// Proposes on whichever node is leader, retrying if leadership moves (slow sanitizer builds can
// trigger an election). A retry may commit a key twice, so callers check key SETS, not counts.
bool ProposeCommitted(Cluster& c, const Command& cmd, const std::set<NodeId>& exclude = {}) {
  auto deadline = std::chrono::steady_clock::now() + 20000ms;
  while (std::chrono::steady_clock::now() < deadline) {
    NodeId leader = c.FindLeader(exclude);
    if (leader != 0 && c.Node(leader)->ProposeAndWait(cmd, 8000ms).result == CommitResult::kApplied) {
      return true;
    }
    std::this_thread::sleep_for(50ms);
  }
  return false;
}

bool HasAll(Cluster& c, NodeId id, const std::vector<std::string>& keys) {
  std::vector<std::string> have = c.Keys(id);
  std::set<std::string> s(have.begin(), have.end());
  for (const auto& k : keys) {
    if (s.count(k) == 0) return false;
  }
  return true;
}

NodeId WaitForLeader(Cluster& c) {
  EXPECT_TRUE(WaitFor([&] { return c.FindLeader() != 0; }, 5000ms));
  return c.FindLeader();
}

}  // namespace

TEST(RaftAppendBytes, FollowerMissingManyLargeEntriesCatchesUpUnderMessageLimit) {
  Cluster c(3);
  c.net.SetMaxMessageBytes(kGrpcDefaultLimit);
  NodeId leader = WaitForLeader(c);
  ASSERT_NE(leader, 0u);
  NodeId follower = leader % 3 + 1;
  c.net.Disconnect(follower);
  std::vector<std::string> keys;
  for (int i = 0; i < 7; ++i) {  // 7 MB committed while the follower is away
    keys.push_back("big" + std::to_string(i));
    ASSERT_TRUE(ProposeCommitted(c, BigPut(keys.back(), kMB), {follower}));
  }
  c.net.Reconnect(follower);
  EXPECT_TRUE(WaitFor([&] { return HasAll(c, follower, keys); }, 15000ms));
  EXPECT_EQ(c.net.OversizedDropped(), 0u);
  EXPECT_LE(c.net.MaxObservedMessageBytes(), kMB + kEntryOverheadBytes + kMessageOverheadBytes + 16);
}

TEST(RaftAppendBytes, MessagesStayUnderTheByteBudget) {
  Cluster c(3);
  NodeId leader = WaitForLeader(c);
  ASSERT_NE(leader, 0u);
  NodeId follower = leader % 3 + 1;
  c.net.Disconnect(follower);
  std::vector<std::string> keys;
  for (int i = 0; i < 12; ++i) {
    keys.push_back("m" + std::to_string(i));
    ASSERT_TRUE(ProposeCommitted(c, BigPut(keys.back(), 300 * 1024), {follower}));
  }
  c.net.Reconnect(follower);
  ASSERT_TRUE(WaitFor([&] { return HasAll(c, follower, keys); }, 15000ms));
  EXPECT_LE(c.net.MaxObservedMessageBytes(), kMB);  // 3 x 300 KB fit, a 4th would not
}

TEST(RaftAppendBytes, SingleEntryLargerThanBudgetIsStillSent) {
  Cluster c(3);
  c.net.SetMaxMessageBytes(kGrpcDefaultLimit);
  NodeId leader = WaitForLeader(c);
  ASSERT_NE(leader, 0u);
  ASSERT_TRUE(ProposeCommitted(c, BigPut("huge", 2 * kMB)));
  for (NodeId id = 1; id <= 3; ++id) {
    EXPECT_TRUE(WaitFor([&] { return HasAll(c, id, {"huge"}); }, 15000ms)) << "node " << id;
  }
}
