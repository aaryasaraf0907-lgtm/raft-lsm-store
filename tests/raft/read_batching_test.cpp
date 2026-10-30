// Concurrent linearizable reads share confirmation rounds instead of each sending its own, without
// giving up linearizability.
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <random>
#include <shared_mutex>
#include <string>
#include <thread>
#include <vector>

#include "checker/linearizability.h"
#include "test_cluster.h"

using namespace raftkv;
using namespace raftkv::testing_util;
using namespace std::chrono_literals;

namespace {

NodeId WaitForLeader(Cluster& c) {
  NodeId leader = 0;
  WaitFor([&] { return (leader = c.FindLeader()) != 0; }, 10000ms);
  return leader;
}

// Runs `threads` x `each` ReadIndex calls on the leader at once; returns how many were confirmed.
int ConcurrentReads(RaftNodeImpl* leader, int threads, int each, std::chrono::milliseconds timeout) {
  std::atomic<int> ok{0};
  std::vector<std::thread> ts;
  for (int t = 0; t < threads; ++t) {
    ts.emplace_back([&] {
      for (int i = 0; i < each; ++i) {
        if (leader->ReadIndex(timeout).result == CommitResult::kApplied) ++ok;
      }
    });
  }
  for (auto& th : ts) th.join();
  return ok.load();
}

}  // namespace

TEST(ReadBatching, ConcurrentReadsShareRoundsInsteadOfEachSendingOne) {
  // Long election timeouts: 32 reader threads on a busy machine must not delay heartbeats into an
  // election in the middle of the measurement.
  Cluster c(3, false, [](RaftConfig& cfg) {
    cfg.election_timeout_min_ms = 1000;
    cfg.election_timeout_max_ms = 2000;
  });
  const NodeId id = WaitForLeader(c);
  ASSERT_NE(id, 0u);
  RaftNodeImpl* leader = c.nodes[id - 1].get();
  ASSERT_EQ(leader->ProposeAndWait(Put("x"), 5000ms).result, CommitResult::kApplied);  // an entry of this term
  const uint64_t before = leader->ReplicationMessagesSent();
  constexpr int kThreads = 32, kEach = 20;
  ASSERT_EQ(ConcurrentReads(leader, kThreads, kEach, 5000ms), kThreads * kEach);
  const uint64_t messages = leader->ReplicationMessagesSent() - before;
  std::printf("[ read batching ] %llu messages for %d reads (one round per read would be %d)\n",
              static_cast<unsigned long long>(messages), kThreads * kEach, 2 * kThreads * kEach);
  // A round is one message per follower (2 here), so one round per read sends at least 2 x reads
  // messages. Shared rounds measured 92-208 for 640 reads; the bound leaves plenty of room.
  EXPECT_LT(messages, static_cast<uint64_t>(kThreads * kEach)) << "reads are not sharing rounds";
}

// A read that arrives while a round is in flight cannot use that round; it must not then have to wait
// for the next heartbeat. Every message is delayed 100 ms, so the first read's round takes ~200 ms;
// a second read starts 50 ms in, during that round, and nothing else happens afterwards. Its own
// round must start as soon as the first one is acknowledged: ~350 ms in all. Waiting for the heartbeat
// instead (the bug this test exists for) takes ~2,100 ms. The limit sits well between the two, so a
// slow shared CI machine (the macOS runner once measured 850 ms) does not fail a correct build.
TEST(ReadBatching, AReadThatMissedTheInFlightRoundDoesNotWaitForAHeartbeat) {
  Cluster c(3, false, [](RaftConfig& cfg) {
    cfg.heartbeat_interval_ms = 2000;
    cfg.election_timeout_min_ms = 6000;
    cfg.election_timeout_max_ms = 8000;
  });
  const NodeId id = WaitForLeader(c);
  ASSERT_NE(id, 0u);
  RaftNodeImpl* leader = c.nodes[id - 1].get();
  FaultPlan plan;
  plan.min_delay_ms = 100;
  plan.max_delay_ms = 100;
  c.net.SetFaults(plan);
  ASSERT_EQ(leader->ProposeAndWait(Put("x"), 10000ms).result, CommitResult::kApplied);
  // Quiet start: the commit's round (about 200 ms) is over, and the next heartbeat is still ~1 s
  // away, so no round is in flight when the first read starts and none is due before the reads end.
  std::this_thread::sleep_for(1000ms);
  std::thread first([&] { EXPECT_EQ(leader->ReadIndex(5000ms).result, CommitResult::kApplied); });
  std::this_thread::sleep_for(50ms);
  const auto start = std::chrono::steady_clock::now();
  EXPECT_EQ(leader->ReadIndex(5000ms).result, CommitResult::kApplied);
  const auto took = std::chrono::steady_clock::now() - start;
  first.join();
  c.net.HealAll();
  EXPECT_LT(took, 1400ms) << "the second read waited for a heartbeat instead of the follow-up round";
}

TEST(ReadBatching, ReadsStillCompleteWithAFollowerDown) {
  Cluster c(3);
  const NodeId id = WaitForLeader(c);
  ASSERT_NE(id, 0u);
  RaftNodeImpl* leader = c.nodes[id - 1].get();
  ASSERT_EQ(leader->ProposeAndWait(Put("x"), 5000ms).result, CommitResult::kApplied);
  c.net.Disconnect(id % 3 + 1);  // a majority (leader + one follower) is still there
  EXPECT_EQ(ConcurrentReads(leader, 8, 10, 5000ms), 80);
}

// Lost messages can leave a round that is never acknowledged; reads must still complete (the next
// heartbeat starts a round they can use).
TEST(ReadBatching, ReadsStillCompleteWhenMessagesAreLost) {
  Cluster c(3);
  const NodeId id = WaitForLeader(c);
  ASSERT_NE(id, 0u);
  RaftNodeImpl* leader = c.nodes[id - 1].get();
  ASSERT_EQ(leader->ProposeAndWait(Put("x"), 5000ms).result, CommitResult::kApplied);
  FaultPlan plan;
  plan.drop_request = 0.3;
  plan.drop_reply = 0.3;
  c.net.SetFaultSeed(11);
  c.net.SetFaults(plan);
  const int ok = ConcurrentReads(leader, 8, 10, 8000ms);
  c.net.HealAll();
  if (c.FindLeader() == id) {
    EXPECT_EQ(ok, 80) << "with the leader unchanged, every read must eventually be confirmed";
  }
}

// The real safety check. Writers and readers run against whoever is leader while messages are
// delayed (so they overtake each other), dropped and duplicated, and the leader is crashed and
// restarted. Every operation is recorded and the whole history must be linearizable: no read may
// return a value older than a write that finished before it started.
TEST(ReadBatching, ReadsAndWritesStayLinearizableUnderDelaysLossAndLeaderCrashes) {
  Cluster c(3, /*persistent=*/true);
  ASSERT_NE(WaitForLeader(c), 0u);
  FaultPlan plan;
  plan.min_delay_ms = 0;
  plan.max_delay_ms = 15;
  plan.drop_request = 0.03;
  plan.drop_reply = 0.03;
  plan.duplicate = 0.03;
  c.net.SetFaultSeed(2026);
  c.net.SetFaults(plan);

  std::shared_mutex nodes_mu;  // restarts replace node objects: no operation may be using one then
  std::atomic<bool> restart_pending{false};  // workers step aside so a restart is not starved
  lin::HistoryRecorder rec;
  std::atomic<bool> stop{false};
  // Bounded, so the history stays checkable however fast operations complete (a bug that skips the
  // confirmation round makes reads nearly instant: hundreds of thousands of them in a few seconds).
  constexpr int kMaxOpsPerThread = 500;
  auto value_on = [&](NodeId id, const std::string& key, std::string* value) {
    std::lock_guard<std::mutex> lk(c.applied_mu);
    const auto& entries = c.applied[id - 1];
    for (auto it = entries.rbegin(); it != entries.rend(); ++it) {
      if (it->command.key == key && it->command.type == CommandType::kPut) {
        *value = it->command.value;
        return true;
      }
    }
    return false;
  };
  std::vector<std::thread> threads;
  for (int w = 0; w < 4; ++w) {
    threads.emplace_back([&, w] {
      std::mt19937_64 g(100 + static_cast<uint64_t>(w));
      for (int i = 0; !stop && i < kMaxOpsPerThread; ++i) {
        const std::string key = "k" + std::to_string(g() % 8);
        const std::string value = "w" + std::to_string(w) + "-" + std::to_string(i);
        while (restart_pending) std::this_thread::sleep_for(1ms);
        std::shared_lock<std::shared_mutex> lk(nodes_mu);
        const NodeId id = c.FindLeader();
        if (id == 0) {
          lk.unlock();
          std::this_thread::sleep_for(5ms);
          continue;
        }
        const size_t op = rec.Begin(w, lin::OpType::kPut, key, value);
        const CommitResponse r = c.nodes[id - 1]->ProposeAndWait(Command{CommandType::kPut, key, value}, 1500ms);
        if (r.result == CommitResult::kApplied) rec.EndWriteOk(op);
        else if (r.result == CommitResult::kNotLeader || r.result == CommitResult::kLost) rec.Drop(op);  // never committed
        else rec.EndWriteUnknown(op);  // timed out or stopped: it may still commit
      }
    });
  }
  for (int r = 0; r < 4; ++r) {
    threads.emplace_back([&, r] {
      std::mt19937_64 g(200 + static_cast<uint64_t>(r));
      for (int n = 0; !stop && n < kMaxOpsPerThread; ++n) {
        const std::string key = "k" + std::to_string(g() % 8);
        while (restart_pending) std::this_thread::sleep_for(1ms);
        std::shared_lock<std::shared_mutex> lk(nodes_mu);
        const NodeId id = c.FindLeader();
        if (id == 0) {
          lk.unlock();
          std::this_thread::sleep_for(5ms);
          continue;
        }
        const size_t op = rec.Begin(10 + r, lin::OpType::kGet, key);
        const CommitResponse resp = c.nodes[id - 1]->ReadIndex(1500ms);
        if (resp.result == CommitResult::kApplied) {
          std::string v;
          const bool found = value_on(id, key, &v);
          rec.EndGet(op, found, v);
        } else {
          rec.Drop(op);  // an unconfirmed read tells us nothing
        }
      }
    });
  }
  for (int round = 0; round < 4; ++round) {  // crash and restart the leader a few times
    std::this_thread::sleep_for(700ms);
    restart_pending = true;
    {
      std::unique_lock<std::shared_mutex> lk(nodes_mu);
      const NodeId id = c.FindLeader();
      if (id != 0) c.Restart(id);
    }
    restart_pending = false;
  }
  std::this_thread::sleep_for(500ms);
  stop = true;
  for (auto& th : threads) th.join();

  const std::vector<lin::Operation> history = rec.Snapshot();
  size_t reads = 0, writes = 0;
  for (const auto& o : history) (o.type == lin::OpType::kGet ? reads : writes)++;
  std::printf("[ linearizability ] %zu reads and %zu writes checked\n", reads, writes);
  // A linearizable history is confirmed in a few steps per operation. A violation can make the search
  // explore far more, so the budget is bounded: running out of it fails the test too.
  const lin::CheckResult result = lin::Check(history, /*max_steps=*/200000);
  ASSERT_FALSE(result.gave_up) << result.explanation;
  EXPECT_TRUE(result.ok) << result.explanation;
  EXPECT_GT(reads, 50u) << "too few reads for the check to mean much";
  EXPECT_GT(writes, 20u) << "too few writes for the check to mean much";
}
