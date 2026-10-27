// End-to-end: a 3-node Raft cluster where every node applies committed entries through
// KVStateMachine into a real KVEngine (WAL + memtable). A leader is killed mid-write; acknowledged
// writes must survive on every node and no write may be applied twice.
#include <gtest/gtest.h>

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "../raft/test_cluster.h"
#include "raft/fake_transport.h"
#include "raft/file_raft_storage.h"
#include "raft/raft_node.h"
#include "server/kv_state_machine.h"
#include "storage/kv_engine.h"

using namespace raftkv;
using namespace raftkv::testing_util;
using namespace std::chrono_literals;

namespace {

constexpr uint64_t kClient = 7;

// Wraps a store and counts how many times each key was written. If a retried request were
// applied twice the count would be 2.
class CountingStore : public KVStore {
 public:
  explicit CountingStore(KVStore* inner) : inner_(inner) {}
  Status Put(const std::string& k, const std::string& v) override {
    {
      std::lock_guard<std::mutex> lk(mu_);
      ++puts_[k];
    }
    return inner_->Put(k, v);
  }
  Status Get(const std::string& k, std::string* v) override { return inner_->Get(k, v); }
  Status Delete(const std::string& k) override { return inner_->Delete(k); }
  int PutsOf(const std::string& k) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = puts_.find(k);
    return it == puts_.end() ? 0 : it->second;
  }
  size_t TotalPuts() {
    std::lock_guard<std::mutex> lk(mu_);
    size_t n = 0;
    for (auto& p : puts_) n += static_cast<size_t>(p.second);
    return n;
  }

 private:
  KVStore* inner_;
  std::mutex mu_;
  std::map<std::string, int> puts_;
};

// n nodes; node i owns a Raft log directory and a KVEngine directory. Crash() stops a node;
// Restart() boots a new incarnation from disk (the engine reopens and replays its WAL, Raft
// replays its log into a fresh, empty session table).
class ReplicatedCluster {
 public:
  explicit ReplicatedCluster(int n) : size_(n), nodes_(n) {
    for (int id = 1; id <= n; ++id) transports_.push_back(net_.MakeTransport(static_cast<NodeId>(id)));
    for (int id = 1; id <= n; ++id) Boot(static_cast<NodeId>(id));
    for (auto& s : nodes_) s.node->Start();
  }
  ~ReplicatedCluster() {
    for (auto& s : nodes_) {
      if (s.node) s.node->Stop();
    }
    net_.Shutdown();
  }

  RaftNodeImpl* Node(NodeId id) { return nodes_[id - 1].node.get(); }
  CountingStore* Store(NodeId id) { return nodes_[id - 1].store.get(); }
  KVStateMachine* Machine(NodeId id) { return nodes_[id - 1].sm.get(); }

  NodeId Leader(const std::set<NodeId>& exclude = {}) {
    for (auto& s : nodes_) {
      if (exclude.count(s.node->Id()) == 0 && s.node->GetRole() == Role::kLeader) return s.node->Id();
    }
    return 0;
  }

  void Crash(NodeId id) { nodes_[id - 1].node->Stop(); }

  void Restart(NodeId id) {
    Slot& s = nodes_[id - 1];
    s.node->Stop();
    s.node.reset();
    s.sm.reset();
    s.store.reset();
    s.engine.reset();
    s.storage.reset();
    Boot(id);
    s.node->Start();
  }

  void RestartAll() {
    for (auto& s : nodes_) s.node->Stop();
    for (auto& s : nodes_) {
      s.node.reset();
      s.sm.reset();
      s.store.reset();
      s.engine.reset();
      s.storage.reset();
    }
    for (int id = 1; id <= size_; ++id) Boot(static_cast<NodeId>(id));
    for (auto& s : nodes_) s.node->Start();
  }

  bool HasValue(NodeId id, const std::string& key, const std::string& value) {
    std::string got;
    return Store(id)->Get(key, &got) == Status::kOk && got == value;
  }

 private:
  struct Slot {
    std::unique_ptr<FileRaftStorage> storage;
    std::unique_ptr<KVEngine> engine;
    std::unique_ptr<CountingStore> store;
    std::unique_ptr<KVStateMachine> sm;
    std::unique_ptr<RaftNodeImpl> node;
  };

  void Boot(NodeId id) {
    Slot& s = nodes_[id - 1];
    std::string base = (tmp_.path / ("node" + std::to_string(id))).string();
    s.storage = std::make_unique<FileRaftStorage>(base + "_raft", /*sync=*/false);
    KVEngine::Options opts;
    opts.dir = base + "_kv";
    opts.sync_wal = false;
    ASSERT_EQ(KVEngine::Open(opts, &s.engine), Status::kOk);
    s.store = std::make_unique<CountingStore>(s.engine.get());
    s.sm = std::make_unique<KVStateMachine>(s.store.get());
    std::vector<NodeId> peers;
    for (int p = 1; p <= size_; ++p) {
      if (static_cast<NodeId>(p) != id) peers.push_back(static_cast<NodeId>(p));
    }
    RaftConfig cfg;
    cfg.random_seed = 42;
    s.node = std::make_unique<RaftNodeImpl>(id, peers, transports_[id - 1].get(), cfg, s.storage.get());
    KVStateMachine* sm = s.sm.get();
    s.node->SetApplyCallback([sm](const LogEntry& e) { sm->Apply(e); });
  }

  int size_;
  TempDir tmp_;  // declared before everything that uses the directories
  FakeNetwork net_;
  std::vector<std::unique_ptr<FakeTransport>> transports_;
  std::vector<Slot> nodes_;
};

std::string KeyOf(uint64_t seq) { return "key" + std::to_string(seq); }
std::string ValueOf(uint64_t seq) { return "value" + std::to_string(seq); }

Command PutCmd(uint64_t seq) {
  Command c{CommandType::kPut, KeyOf(seq), ValueOf(seq)};
  c.client_id = kClient;
  c.seq = seq;
  return c;
}

// One client request, retried with the SAME seq until a leader acknowledges it. `down` is read on
// every attempt, because a stopped node can still report itself as leader.
bool Write(ReplicatedCluster& c, const Command& cmd, const std::atomic<NodeId>& down) {
  auto deadline = std::chrono::steady_clock::now() + 30000ms;
  while (std::chrono::steady_clock::now() < deadline) {
    std::set<NodeId> exclude;
    if (down.load() != 0) exclude.insert(down.load());
    NodeId leader = c.Leader(exclude);
    if (leader != 0 && c.Node(leader)->ProposeAndWait(cmd, 2000ms).result == CommitResult::kApplied) {
      return true;
    }
    std::this_thread::sleep_for(20ms);
  }
  return false;
}

void ExpectExactlyOnce(ReplicatedCluster& c, uint64_t n) {
  for (NodeId id = 1; id <= 3; ++id) {
    ASSERT_TRUE(WaitFor(
        [&] {
          // A restarted node's engine kept the data on disk, so the values are there at once; its
          // log is re-applied only after it hears the commit index from the leader. Wait for both.
          if (c.Machine(id)->Applied() < n) return false;
          for (uint64_t s = 1; s <= n; ++s) {
            if (!c.HasValue(id, KeyOf(s), ValueOf(s))) return false;
          }
          return true;
        },
        20000ms))
        << "node " << id << " is missing an acknowledged write or has not finished replaying its log";
  }
  for (NodeId id = 1; id <= 3; ++id) {
    EXPECT_EQ(c.Machine(id)->Sessions().LastSeq(kClient), n) << "node " << id;
    EXPECT_EQ(c.Machine(id)->Applied(), n) << "node " << id;
    EXPECT_EQ(c.Store(id)->TotalPuts(), n) << "node " << id << " wrote some key twice";
    for (uint64_t s = 1; s <= n; ++s) EXPECT_EQ(c.Store(id)->PutsOf(KeyOf(s)), 1) << KeyOf(s);
  }
}

}  // namespace

// The leader dies while a client is writing. The client retries each request with the same seq.
// Every acknowledged write must be on all 3 nodes, applied exactly once on each.
TEST(ReplicatedKV, LeaderKilledMidWriteLosesNothingAndDoublesNothing) {
  for (uint64_t kill_after : {3u, 17u, 40u}) {
    SCOPED_TRACE("kill_after=" + std::to_string(kill_after));
    ReplicatedCluster c(3);
    ASSERT_TRUE(WaitFor([&] { return c.Leader() != 0; }, 8000ms));
    constexpr uint64_t kN = 60;
    std::atomic<uint64_t> acked{0};
    std::atomic<NodeId> down{0};
    bool all_acked = true;
    std::thread client([&] {
      for (uint64_t s = 1; s <= kN; ++s) {
        if (!Write(c, PutCmd(s), down)) {
          all_acked = false;
          return;
        }
        acked = s;
      }
    });
    while (acked.load() < kill_after) std::this_thread::sleep_for(1ms);
    NodeId victim = 0;  // an election may be in progress at any given instant: wait for a leader
    ASSERT_TRUE(WaitFor([&] { return (victim = c.Leader()) != 0; }, 8000ms));
    down = victim;
    c.Crash(victim);  // mid-stream: the client is still sending
    client.join();
    ASSERT_TRUE(all_acked);
    ASSERT_TRUE(WaitFor([&] { return c.Leader({victim}) != 0; }, 10000ms)) << "no new leader";

    c.Restart(victim);  // reboots from its Raft log and KV directory
    ExpectExactlyOnce(c, kN);
  }
}

// A client whose ack was lost resends an already-committed request, possibly to a new leader.
// It must be acknowledged but not applied a second time.
TEST(ReplicatedKV, ResentRequestAfterLeaderChangeIsAppliedOnce) {
  ReplicatedCluster c(3);
  ASSERT_TRUE(WaitFor([&] { return c.Leader() != 0; }, 8000ms));
  std::atomic<NodeId> down{0};
  ASSERT_TRUE(Write(c, PutCmd(1), down));
  NodeId old_leader = c.Leader();
  down = old_leader;
  c.Crash(old_leader);
  ASSERT_TRUE(WaitFor([&] { return c.Leader({old_leader}) != 0; }, 8000ms));
  ASSERT_TRUE(Write(c, PutCmd(1), down));  // the same seq again
  ASSERT_TRUE(Write(c, PutCmd(1), down));  // and again
  ASSERT_TRUE(Write(c, PutCmd(2), down));
  c.Restart(old_leader);
  ExpectExactlyOnce(c, 2);
  uint64_t skipped = 0;
  for (NodeId id = 1; id <= 3; ++id) skipped += c.Machine(id)->DuplicatesSkipped();
  EXPECT_GE(skipped, 2u) << "the resends should have been filtered as duplicates";
}

// Power loss for the whole cluster: data survives in the engines, the session tables are rebuilt
// by replay, and a resent request after the reboot is still not applied twice.
TEST(ReplicatedKV, WholeClusterRestartKeepsDataAndDedup) {
  ReplicatedCluster c(3);
  ASSERT_TRUE(WaitFor([&] { return c.Leader() != 0; }, 8000ms));
  const std::atomic<NodeId> none{0};
  for (uint64_t s = 1; s <= 10; ++s) ASSERT_TRUE(Write(c, PutCmd(s), none));
  c.RestartAll();
  ASSERT_TRUE(WaitFor([&] { return c.Leader() != 0; }, 8000ms));
  ASSERT_TRUE(Write(c, PutCmd(10), none));  // resent after the reboot
  ASSERT_TRUE(Write(c, PutCmd(11), none));
  ExpectExactlyOnce(c, 11);
}
