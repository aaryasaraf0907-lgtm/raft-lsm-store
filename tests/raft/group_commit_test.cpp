// Concurrent proposals share log writes and fsyncs, without changing what a proposer is promised:
// distinct, contiguous log indexes, durable before the answer, and an answer for everyone.
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <map>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

#include "raft/raft_node.h"
#include "raft/raft_storage.h"
#include "test_cluster.h"

using namespace raftkv;
using raftkv::testing_util::Cluster;
using namespace std::chrono_literals;

namespace {

// In-memory storage whose AppendEntries takes `delay_ms` (a stand-in for an fsync) and is counted.
class CountingStorage : public RaftStorage {
 public:
  explicit CountingStorage(int delay_ms) : delay_ms_(delay_ms) {}
  bool Load(PersistentState* out) override {
    *out = PersistentState{};
    return true;
  }
  bool SaveHardState(Term, NodeId) override { return true; }
  bool AppendEntries(const std::vector<LogEntry>& entries) override {
    std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms_));
    std::lock_guard<std::mutex> lk(mu_);
    ++calls_;
    max_batch_ = std::max(max_batch_, entries.size());
    for (const LogEntry& e : entries) stored_.push_back(e);
    return true;
  }
  bool TruncateSuffix(LogIndex) override { return true; }
  bool SaveSnapshot(LogIndex, Term, const std::string&, LogIndex, Term, const std::vector<LogEntry>&) override { return true; }

  size_t calls() const { std::lock_guard<std::mutex> lk(mu_); return calls_; }
  size_t max_batch() const { std::lock_guard<std::mutex> lk(mu_); return max_batch_; }
  std::vector<LogEntry> stored() const { std::lock_guard<std::mutex> lk(mu_); return stored_; }
  bool HasIndex(LogIndex i) const {
    std::lock_guard<std::mutex> lk(mu_);
    for (const LogEntry& e : stored_) {
      if (e.index == i) return true;
    }
    return false;
  }

 private:
  int delay_ms_;
  mutable std::mutex mu_;
  size_t calls_ = 0, max_batch_ = 0;
  std::vector<LogEntry> stored_;
};

struct SingleNode {
  FakeNetwork net;
  std::unique_ptr<FakeTransport> transport;
  CountingStorage storage;
  std::mutex applied_mu;
  std::vector<LogEntry> applied;
  std::unique_ptr<RaftNodeImpl> node;

  explicit SingleNode(int delay_ms) : storage(delay_ms) {
    transport = net.MakeTransport(1);
    RaftConfig cfg;
    cfg.random_seed = 7;
    node = std::make_unique<RaftNodeImpl>(1, std::vector<NodeId>{}, transport.get(), cfg, &storage);
    node->SetApplyCallback([this](const LogEntry& e) {
      std::lock_guard<std::mutex> lk(applied_mu);
      applied.push_back(e);
    });
    node->Start();
    const auto end = std::chrono::steady_clock::now() + 10s;
    while (node->GetRole() != Role::kLeader && std::chrono::steady_clock::now() < end) std::this_thread::sleep_for(2ms);
  }
  ~SingleNode() {
    node->Stop();
    net.Shutdown();
  }
};

Command Put(const std::string& key) { return Command{CommandType::kPut, key, "v"}; }

}  // namespace

TEST(GroupCommit, ConcurrentProposersShareLogWritesAndEveryEntryIsStoredOnceInOrder) {
  SingleNode n(/*delay_ms=*/5);
  ASSERT_EQ(n.node->GetRole(), Role::kLeader);
  const size_t calls_before = n.storage.calls();
  constexpr int kThreads = 32, kEach = 10;
  std::atomic<int> bad{0}, not_durable{0};
  std::mutex m;
  std::set<LogIndex> indexes;
  // Scheduling pressure: two threads that burn CPU. Batching that only works when every thread wakes
  // promptly falls apart under this (it did on a laptop: 161 writes for 320 proposals).
  std::atomic<bool> burn{true};
  std::vector<std::thread> burners;
  for (int b = 0; b < 2; ++b) {
    burners.emplace_back([&] {
      volatile uint64_t sink = 0;
      while (burn.load()) {
        for (int i = 0; i < 2000; ++i) sink = sink + static_cast<uint64_t>(i);
      }
    });
  }
  std::vector<std::thread> threads;
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      for (int i = 0; i < kEach; ++i) {
        CommitResponse r = n.node->ProposeAndWait(Put("t" + std::to_string(t) + "-" + std::to_string(i)), 20s);
        if (r.result != CommitResult::kApplied) ++bad;
        if (!n.storage.HasIndex(r.index)) ++not_durable;  // an applied entry must already be in storage
        std::lock_guard<std::mutex> lk(m);
        if (!indexes.insert(r.index).second) ++bad;  // two proposals got the same index
      }
    });
  }
  for (auto& th : threads) th.join();
  burn = false;
  for (auto& th : burners) th.join();
  EXPECT_EQ(bad.load(), 0);
  EXPECT_EQ(not_durable.load(), 0);
  EXPECT_EQ(indexes.size(), static_cast<size_t>(kThreads * kEach));

  const size_t writes = n.storage.calls() - calls_before;
  std::printf("[ batching ] %zu log writes for %d proposals, largest batch %zu\n", writes, kThreads * kEach, n.storage.max_batch());
  // With the group-commit delay, 320 proposals take about a dozen writes whatever the scheduler does;
  // without it they took 40-260 depending on the machine. The bound is generous but meaningful.
  EXPECT_LT(writes, static_cast<size_t>(kThreads * kEach) / 8) << "proposals were not batched well: " << writes << " log writes";
  EXPECT_GE(n.storage.max_batch(), 8u);

  // The log in storage is exactly 1..N, in order, each index once, holding every proposed command once.
  const std::vector<LogEntry> stored = n.storage.stored();
  std::map<std::string, int> seen;
  for (size_t i = 0; i < stored.size(); ++i) {
    ASSERT_EQ(stored[i].index, i + 1) << "storage has a gap, a duplicate or a reordering at position " << i;
    if (stored[i].command.type == CommandType::kPut) ++seen[stored[i].command.key];
  }
  ASSERT_EQ(seen.size(), static_cast<size_t>(kThreads * kEach));
  for (const auto& kv : seen) EXPECT_EQ(kv.second, 1) << kv.first;
  std::lock_guard<std::mutex> lk(n.applied_mu);
  ASSERT_FALSE(n.applied.empty());
  for (size_t i = 0; i < n.applied.size(); ++i) EXPECT_EQ(n.applied[i].index, n.applied[0].index + i) << "applied out of order or with a gap";
}

TEST(GroupCommit, ALoneProposalCostsExactlyOneLogWrite) {
  SingleNode n(/*delay_ms=*/1);
  ASSERT_EQ(n.node->GetRole(), Role::kLeader);
  const size_t before = n.storage.calls();
  EXPECT_EQ(n.node->ProposeAndWait(Put("solo"), 10s).result, CommitResult::kApplied);
  EXPECT_EQ(n.storage.calls() - before, 1u);
}

// Propose (no waiting) must also not answer before the entry is durable.
TEST(GroupCommit, ProposeAnswersOnlyOnceTheEntryIsInStorage) {
  SingleNode n(/*delay_ms=*/20);
  ASSERT_EQ(n.node->GetRole(), Role::kLeader);
  for (int i = 0; i < 5; ++i) {
    ProposeResponse r = n.node->Propose(Put("p" + std::to_string(i)));
    ASSERT_EQ(r.result, ProposeResult::kAccepted);
    EXPECT_TRUE(n.storage.HasIndex(r.index)) << "the answer came before the entry was durable";
  }
}

TEST(GroupCommit, EveryProposalToAFollowerIsAnsweredAtOnceWithTheLeaderHint) {
  Cluster c(3);
  int leader = 0;
  for (int i = 0; i < 2000 && leader == 0; ++i) {
    leader = static_cast<int>(c.FindLeader());
    if (leader == 0) std::this_thread::sleep_for(5ms);
  }
  ASSERT_NE(leader, 0);
  const int follower = leader % 3 + 1;
  std::atomic<int> wrong{0};
  std::vector<std::thread> threads;
  const auto start = std::chrono::steady_clock::now();
  for (int t = 0; t < 16; ++t) {
    threads.emplace_back([&] {
      for (int i = 0; i < 5; ++i) {
        CommitResponse r = c.nodes[static_cast<size_t>(follower - 1)]->ProposeAndWait(Put("x"), 5s);
        if (r.result != CommitResult::kNotLeader || r.leader_hint != static_cast<NodeId>(leader)) ++wrong;
      }
    });
  }
  for (auto& th : threads) th.join();
  EXPECT_EQ(wrong.load(), 0);
  EXPECT_LT(std::chrono::steady_clock::now() - start, 3s) << "refusals must not wait";
}

// Stopping the node while proposers are queued behind a slow write must not leave anyone hanging.
TEST(GroupCommit, StoppingTheNodeWakesEveryQueuedProposer) {
  SingleNode n(/*delay_ms=*/200);
  ASSERT_EQ(n.node->GetRole(), Role::kLeader);
  std::atomic<int> finished{0};
  std::vector<std::thread> threads;
  for (int t = 0; t < 16; ++t) {
    threads.emplace_back([&, t] {
      CommitResponse r = n.node->ProposeAndWait(Put("s" + std::to_string(t)), 20s);
      EXPECT_TRUE(r.result == CommitResult::kApplied || r.result == CommitResult::kStopped || r.result == CommitResult::kNotLeader)
          << static_cast<int>(r.result);
      ++finished;
    });
  }
  std::this_thread::sleep_for(60ms);
  n.node->Stop();
  const auto end = std::chrono::steady_clock::now() + 10s;
  while (finished.load() < 16 && std::chrono::steady_clock::now() < end) std::this_thread::sleep_for(5ms);
  EXPECT_EQ(finished.load(), 16) << "a proposer is stuck after Stop()";
  for (auto& th : threads) th.join();
}
