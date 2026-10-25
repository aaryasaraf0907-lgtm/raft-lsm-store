// ReadIndex: a linearizable read barrier that does not write to the log. The important property
// is that a leader that has been cut off (and replaced) can never serve a stale read.
#include <gtest/gtest.h>

#include <atomic>
#include <functional>
#include <future>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "test_cluster.h"

using namespace raftkv;
using namespace raftkv::testing_util;
using namespace std::chrono_literals;

namespace {
NodeId WaitForLeader(Cluster& c) {
  EXPECT_TRUE(WaitFor([&] { return c.FindLeader() != 0; }, 8000ms));
  return c.FindLeader();
}
}  // namespace

TEST(ReadIndex, SeesEveryAcknowledgedWrite) {
  Cluster c(3);
  NodeId leader = WaitForLeader(c);
  ASSERT_NE(leader, 0u);
  for (int i = 0; i < 40; ++i) {
    std::string key = "k" + std::to_string(i);
    CommitResponse w = c.Node(leader)->ProposeAndWait(Put(key), 5000ms);
    ASSERT_EQ(w.result, CommitResult::kApplied);
    CommitResponse r = c.Node(leader)->ReadIndex(5000ms);
    ASSERT_EQ(r.result, CommitResult::kApplied) << i;
    EXPECT_GE(r.index, w.index);
    EXPECT_GE(c.Node(leader)->LastApplied(), r.index);
    EXPECT_TRUE(Contains(c.Keys(leader), key)) << "the read missed an acknowledged write";
  }
}

TEST(ReadIndex, ReadsDoNotGrowTheLog) {
  Cluster c(3);
  NodeId leader = WaitForLeader(c);
  ASSERT_NE(leader, 0u);
  ASSERT_EQ(c.Node(leader)->ProposeAndWait(Put("a"), 5000ms).result, CommitResult::kApplied);
  const size_t before = c.Node(leader)->LogLength();
  for (int i = 0; i < 200; ++i) {
    ASSERT_EQ(c.Node(leader)->ReadIndex(5000ms).result, CommitResult::kApplied) << i;
  }
  EXPECT_EQ(c.Node(leader)->LogLength(), before) << "reads must not append log entries";
}

TEST(ReadIndex, FollowerRefusesAndPointsAtTheLeader) {
  Cluster c(3);
  NodeId leader = WaitForLeader(c);
  ASSERT_NE(leader, 0u);
  NodeId follower = leader % 3 + 1;
  ASSERT_TRUE(WaitFor([&] { return c.Node(follower)->GetRole() == Role::kFollower; }, 5000ms));
  CommitResponse r = c.Node(follower)->ReadIndex(500ms);
  EXPECT_EQ(r.result, CommitResult::kNotLeader);
}

TEST(ReadIndex, WorksInASingleNodeCluster) {
  Cluster c(1);
  ASSERT_TRUE(WaitFor([&] { return c.FindLeader() != 0; }, 5000ms));
  ASSERT_EQ(c.Node(1)->ProposeAndWait(Put("x"), 5000ms).result, CommitResult::kApplied);
  CommitResponse r = c.Node(1)->ReadIndex(2000ms);
  EXPECT_EQ(r.result, CommitResult::kApplied);
  EXPECT_TRUE(Contains(c.Keys(1), "x"));
}

TEST(ReadIndex, StoppedNodeReportsStopped) {
  Cluster c(3);
  NodeId leader = WaitForLeader(c);
  ASSERT_NE(leader, 0u);
  c.Node(leader)->Stop();
  EXPECT_EQ(c.Node(leader)->ReadIndex(200ms).result, CommitResult::kStopped);
}

// A leader that cannot reach a majority must not confirm anything.
TEST(ReadIndex, IsolatedLeaderCannotConfirmALeaderRead) {
  Cluster c(3);
  NodeId leader = WaitForLeader(c);
  ASSERT_NE(leader, 0u);
  ASSERT_EQ(c.Node(leader)->ProposeAndWait(Put("a"), 5000ms).result, CommitResult::kApplied);
  c.net.Disconnect(leader);
  CommitResponse r = c.Node(leader)->ReadIndex(400ms);
  EXPECT_NE(r.result, CommitResult::kApplied) << "an isolated leader served a read";
}

// The scenario ReadIndex exists for. The leader is partitioned away; the other two elect a new
// leader and keep acknowledging writes. A reader keeps asking EVERY node, including the stale
// leader, which still believes it leads. Any read that succeeds must see every write that was
// acknowledged before the read began.
TEST(ReadIndex, NeverServesAStaleReadFromAReplacedLeader) {
  Cluster c(3);
  NodeId old_leader = WaitForLeader(c);
  ASSERT_NE(old_leader, 0u);

  std::atomic<uint64_t> acked{0};
  std::atomic<NodeId> cut{0};
  std::atomic<bool> stop{false};
  std::atomic<int> stale{0}, served{0}, served_after_cut{0};

  std::thread writer([&] {
    for (uint64_t s = 1; !stop; ++s) {
      std::string key = "w" + std::to_string(s);
      bool done = false;
      while (!stop && !done) {
        NodeId l = c.FindLeader(cut.load() != 0 ? std::set<NodeId>{cut.load()} : std::set<NodeId>{});
        if (l != 0 && c.Node(l)->ProposeAndWait(Put(key), 400ms).result == CommitResult::kApplied) {
          done = true;
        } else {
          std::this_thread::sleep_for(10ms);
        }
      }
      if (done) acked = s;
    }
  });
  std::thread reader([&] {
    while (!stop) {
      for (NodeId id = 1; id <= 3 && !stop; ++id) {
        const uint64_t before = acked.load();
        if (c.Node(id)->ReadIndex(150ms).result != CommitResult::kApplied) continue;
        ++served;
        if (cut.load() != 0) ++served_after_cut;
        if (before > 0 && !Contains(c.Keys(id), "w" + std::to_string(before))) ++stale;
      }
    }
  });

  std::this_thread::sleep_for(300ms);
  cut = old_leader;
  c.net.Disconnect(old_leader);
  std::this_thread::sleep_for(1800ms);
  stop = true;
  writer.join();
  reader.join();

  EXPECT_EQ(stale.load(), 0) << "a read missed a write that was acknowledged before it began";
  EXPECT_GT(served.load(), 0);
  EXPECT_GT(served_after_cut.load(), 0) << "the new leader never served a read";
  EXPECT_GT(acked.load(), 5u) << "the new leader never took writes";
}

// Every read that follows an acknowledged write sees it, across a leadership change.
TEST(ReadIndex, ReadsStayLinearizableAcrossLeaderCrash) {
  Cluster c(3);
  NodeId first = WaitForLeader(c);
  ASSERT_NE(first, 0u);
  ASSERT_EQ(c.Node(first)->ProposeAndWait(Put("before"), 5000ms).result, CommitResult::kApplied);
  c.Node(first)->Stop();
  ASSERT_TRUE(WaitFor([&] { return c.FindLeader({first}) != 0; }, 8000ms));
  NodeId second = c.FindLeader({first});
  ASSERT_EQ(c.Node(second)->ProposeAndWait(Put("after"), 5000ms).result, CommitResult::kApplied);
  CommitResponse r = c.Node(second)->ReadIndex(5000ms);
  ASSERT_EQ(r.result, CommitResult::kApplied);
  EXPECT_TRUE(Contains(c.Keys(second), "before"));
  EXPECT_TRUE(Contains(c.Keys(second), "after"));
}

// Right after an election the new leader's commit index can lag behind writes its predecessor
// already acknowledged: here follower B holds the write (it was the one that made the majority)
// but was never told it committed, because the old leader crashed first. A read must wait until
// the new leader has committed an entry of its own term AND applied everything up to that point,
// or it would miss that write. The apply callback is slow here, as a real storage engine can be,
// which is what makes the stale window visible.
TEST(ReadIndex, NewLeaderReadSeesWritesTheOldLeaderAcknowledged) {
  for (int trial = 0; trial < 6; ++trial) {
    SCOPED_TRACE("trial=" + std::to_string(trial));
    Cluster c(3);
    NodeId first = WaitForLeader(c);
    ASSERT_NE(first, 0u);
    NodeId lagging = first % 3 + 1;  // never sees the write
    NodeId acker = lagging % 3 + 1;  // makes the majority with the old leader
    ASSERT_NE(acker, first);

    std::mutex mu;
    std::vector<std::string> seen;
    c.Node(acker)->SetApplyCallback([&](const LogEntry& e) {
      if (e.command.key == "acked") std::this_thread::sleep_for(120ms);
      std::lock_guard<std::mutex> lk(mu);
      seen.push_back(e.command.key);
    });

    c.net.Disconnect(lagging);
    ASSERT_EQ(c.Node(first)->ProposeAndWait(Put("acked"), 5000ms).result, CommitResult::kApplied);
    c.Node(first)->Stop();  // crash before `acker` hears that the write committed
    c.net.Reconnect(lagging);

    // Ask the future leader over and over, so the first read is sent the instant it wins.
    CommitResponse r{CommitResult::kNotLeader, 0, 0};
    auto deadline = std::chrono::steady_clock::now() + 8000ms;
    while (r.result == CommitResult::kNotLeader && std::chrono::steady_clock::now() < deadline) {
      r = c.Node(acker)->ReadIndex(5000ms);
      if (r.result == CommitResult::kNotLeader) std::this_thread::yield();
    }
    ASSERT_EQ(r.result, CommitResult::kApplied);
    std::lock_guard<std::mutex> lk(mu);
    EXPECT_TRUE(Contains(seen, "acked")) << "stale read right after an election";
  }
}

// ---- deterministic: a transport the test drives by hand ----

namespace {
class ManualTransport : public Transport {
 public:
  struct Vote {
    NodeId to;
    RequestVoteRequest req;
    std::function<void(const RequestVoteReply&)> cb;
  };
  struct Append {
    NodeId to;
    AppendEntriesRequest req;
    std::function<void(const AppendEntriesReply&)> cb;
  };
  void SendRequestVote(NodeId to, const RequestVoteRequest& req,
                       std::function<void(const RequestVoteReply&)> cb) override {
    std::lock_guard<std::mutex> lk(mu_);
    votes_.push_back({to, req, std::move(cb)});
  }
  void SendAppendEntries(NodeId to, const AppendEntriesRequest& req,
                         std::function<void(const AppendEntriesReply&)> cb) override {
    std::lock_guard<std::mutex> lk(mu_);
    appends_.push_back({to, req, std::move(cb)});
  }
  void SetHandler(RpcHandler*) override {}
  void Start() override {}
  void Stop() override {}
  std::vector<Vote> TakeVotes() {
    std::lock_guard<std::mutex> lk(mu_);
    return std::exchange(votes_, {});
  }
  std::vector<Append> TakeAppends() {
    std::lock_guard<std::mutex> lk(mu_);
    return std::exchange(appends_, {});
  }

 private:
  std::mutex mu_;
  std::vector<Vote> votes_;
  std::vector<Append> appends_;
};
}  // namespace

// A new leader may only confirm a read once an entry of ITS OWN term has committed. Here every
// message is answered with a rejection: that is enough for the majority to acknowledge the
// leader (they accepted its term), but nothing ever commits. The read must stay blocked until a
// message is finally accepted and the leader's first entry commits.
TEST(ReadIndex, LeaderWaitsForAnEntryOfItsOwnTermBeforeConfirming) {
  ManualTransport net;
  RaftConfig cfg;
  cfg.random_seed = 3;
  RaftNodeImpl node(1, {2, 3}, &net, cfg);
  node.SetApplyCallback([](const LogEntry&) {});
  // Peer 2, leader of term 1, gave us two entries; we never heard that they committed.
  AppendEntriesRequest seed{1, 2, 0, 0,
                            {LogEntry{1, 1, Command{CommandType::kNoop, "", ""}},
                             LogEntry{1, 2, Put("acked")}},
                            0};
  ASSERT_TRUE(node.HandleAppendEntries(seed).success);
  ASSERT_EQ(node.CommitIndex(), 0u);
  node.Start();

  ASSERT_TRUE(WaitFor(
      [&] {
        for (auto& v : net.TakeVotes()) {
          if (v.to == 2) {
            v.cb(RequestVoteReply{v.req.term, true});
            return true;
          }
        }
        return false;
      },
      5000ms));
  ASSERT_TRUE(WaitFor([&] { return node.GetRole() == Role::kLeader; }, 2000ms));

  auto read = std::async(std::launch::async, [&] { return node.ReadIndex(8000ms); });
  auto end = std::chrono::steady_clock::now() + 500ms;
  while (std::chrono::steady_clock::now() < end) {
    for (auto& a : net.TakeAppends()) a.cb(AppendEntriesReply{a.req.term, false, 0});  // reject
    std::this_thread::sleep_for(5ms);
  }
  EXPECT_EQ(read.wait_for(0ms), std::future_status::timeout)
      << "the read was confirmed before the leader committed an entry of its own term";

  CommitResponse r{CommitResult::kTimeout, 0, 0};
  auto stop_at = std::chrono::steady_clock::now() + 5000ms;
  while (read.wait_for(0ms) != std::future_status::ready && std::chrono::steady_clock::now() < stop_at) {
    for (auto& a : net.TakeAppends()) {
      a.cb(AppendEntriesReply{a.req.term, true, a.req.prev_log_index + a.req.entries.size()});
    }
    std::this_thread::sleep_for(5ms);
  }
  ASSERT_EQ(read.wait_for(0ms), std::future_status::ready);
  r = read.get();
  EXPECT_EQ(r.result, CommitResult::kApplied);
  EXPECT_GE(r.index, 3u);  // at or after the leader's own first entry
  node.Stop();
}
