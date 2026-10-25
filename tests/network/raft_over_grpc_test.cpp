// Raft nodes talking over REAL gRPC sockets on localhost (no fake network): elections with
// PreVote, a follower that reboots empty and catches up from a chunked snapshot, an unresponsive
// peer, and a node that is cut off. These are the scenarios that exposed real transport problems
// (the 4 MiB message limit, snapshot deadlines, worker starvation).
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "network/grpc_transport.h"
#include "raft/raft_node.h"

namespace raftkv {
namespace {

using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;

bool WaitFor(const std::function<bool()>& pred, std::chrono::milliseconds timeout) {
  const auto end = Clock::now() + timeout;
  while (Clock::now() < end) {
    if (pred()) return true;
    std::this_thread::sleep_for(10ms);
  }
  return pred();
}

// A "frozen process": accepts TCP connections in the kernel but never answers.
struct Tarpit {
  int fd = -1;
  int port = 0;
  Tarpit() {
    fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    a.sin_port = 0;
    ::bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a));
    ::listen(fd, 128);
    socklen_t len = sizeof(a);
    ::getsockname(fd, reinterpret_cast<sockaddr*>(&a), &len);
    port = ntohs(a.sin_port);
  }
  ~Tarpit() {
    if (fd >= 0) ::close(fd);
  }
};

void PutLen(std::string* s, uint64_t v) {
  for (int i = 0; i < 8; ++i) s->push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
}
uint64_t GetLen(const std::string& s, size_t* pos) {
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) v |= static_cast<uint64_t>(static_cast<uint8_t>(s[*pos + i])) << (8 * i);
  *pos += 8;
  return v;
}

// A key-value state machine with snapshot support.
struct MapStateMachine {
  std::mutex mu;
  std::map<std::string, std::string> kv;
  int restores = 0;

  void Apply(const LogEntry& e) {
    std::lock_guard<std::mutex> lk(mu);
    if (e.command.type == CommandType::kPut) kv[e.command.key] = e.command.value;
    if (e.command.type == CommandType::kDelete) kv.erase(e.command.key);
  }
  std::string Snapshot() {
    std::lock_guard<std::mutex> lk(mu);
    std::string out;
    PutLen(&out, kv.size());
    for (const auto& p : kv) {
      PutLen(&out, p.first.size());
      out += p.first;
      PutLen(&out, p.second.size());
      out += p.second;
    }
    return out;
  }
  void Restore(const std::string& bytes) {
    std::lock_guard<std::mutex> lk(mu);
    size_t pos = 0;
    uint64_t n = GetLen(bytes, &pos);
    kv.clear();
    for (uint64_t i = 0; i < n; ++i) {
      uint64_t kl = GetLen(bytes, &pos);
      std::string k = bytes.substr(pos, kl);
      pos += kl;
      uint64_t vl = GetLen(bytes, &pos);
      kv[k] = bytes.substr(pos, vl);
      pos += vl;
    }
    ++restores;
  }
  size_t Size() {
    std::lock_guard<std::mutex> lk(mu);
    return kv.size();
  }
  bool Has(const std::string& k, const std::string& v) {
    std::lock_guard<std::mutex> lk(mu);
    auto it = kv.find(k);
    return it != kv.end() && it->second == v;
  }
  int Restores() {
    std::lock_guard<std::mutex> lk(mu);
    return restores;
  }
};

Command Put(const std::string& k, const std::string& v) { return Command{CommandType::kPut, k, v}; }

// `real` Raft nodes on real gRPC transports, plus `total - real` frozen processes (tarpits).
class GrpcCluster {
 public:
  GrpcCluster(int total, int real, RaftConfig cfg) : total_(total), real_(real), cfg_(cfg), slots_(real) {
    for (int id = real + 1; id <= total; ++id) tarpits_.push_back(std::make_unique<Tarpit>());
    for (int id = 1; id <= real; ++id) Boot(id);
    for (auto& s : slots_) s.node->Start();  // starts each transport's server
    for (int id = 1; id <= real; ++id) WirePeers(id);
  }
  ~GrpcCluster() {
    for (auto& s : slots_) {
      if (s.node) s.node->Stop();
    }
  }

  RaftNodeImpl* Node(int id) { return slots_[id - 1].node.get(); }
  MapStateMachine* Sm(int id) { return slots_[id - 1].sm.get(); }
  GrpcTransport* Transport(int id) { return slots_[id - 1].transport.get(); }
  void Stop(int id) { slots_[id - 1].node->Stop(); }
  int Leader(int exclude = 0) {
    for (int id = 1; id <= real_; ++id) {
      if (id != exclude && slots_[id - 1].node->GetRole() == Role::kLeader) return id;
    }
    return 0;
  }

  // The node `id` crashes and comes back as a brand-new, EMPTY process on a new port; everyone
  // else is told where it is.
  void Reboot(int id) {
    Slot& s = slots_[id - 1];
    s.node->Stop();
    s.node.reset();
    s.transport.reset();
    s.sm.reset();
    Boot(id);
    s.node->Start();
    WirePeers(id);
    for (int p = 1; p <= real_; ++p) {
      if (p != id) Transport(p)->SetPeerAddress(id, Address(Transport(id)->ListeningPort()));
    }
  }

  // Cuts `id` off from everyone (both directions) by pointing every address at a closed port.
  void Isolate(int id) {
    for (int p = 1; p <= real_; ++p) {
      if (p == id) continue;
      Transport(p)->SetPeerAddress(id, "127.0.0.1:1");
      Transport(id)->SetPeerAddress(p, "127.0.0.1:1");
    }
  }
  void Heal(int id) {
    for (int p = 1; p <= real_; ++p) {
      if (p == id) continue;
      Transport(p)->SetPeerAddress(id, Address(Transport(id)->ListeningPort()));
      Transport(id)->SetPeerAddress(p, Address(Transport(p)->ListeningPort()));
    }
  }

 private:
  struct Slot {
    std::unique_ptr<GrpcTransport> transport;
    std::unique_ptr<MapStateMachine> sm;
    std::unique_ptr<RaftNodeImpl> node;
  };
  static std::string Address(int port) { return "127.0.0.1:" + std::to_string(port); }

  void Boot(int id) {
    Slot& s = slots_[id - 1];
    s.transport = std::make_unique<GrpcTransport>("127.0.0.1:0", std::map<NodeId, std::string>{});
    s.sm = std::make_unique<MapStateMachine>();
    std::vector<NodeId> peers;
    for (int p = 1; p <= total_; ++p) {
      if (p != id) peers.push_back(static_cast<NodeId>(p));
    }
    RaftConfig cfg = cfg_;
    cfg.random_seed = static_cast<uint32_t>(cfg_.random_seed + id);
    s.node = std::make_unique<RaftNodeImpl>(static_cast<NodeId>(id), peers, s.transport.get(), cfg);
    MapStateMachine* sm = s.sm.get();
    s.node->SetApplyCallback([sm](const LogEntry& e) { sm->Apply(e); });
    s.node->SetSnapshotCallbacks([sm] { return sm->Snapshot(); },
                                 [sm](const std::string& b) { sm->Restore(b); });
  }
  void WirePeers(int id) {
    for (int p = 1; p <= total_; ++p) {
      if (p == id) continue;
      int port = p <= real_ ? Transport(p)->ListeningPort() : tarpits_[p - real_ - 1]->port;
      Transport(id)->SetPeerAddress(static_cast<NodeId>(p), Address(port));
    }
  }

  int total_, real_;
  RaftConfig cfg_;
  std::vector<std::unique_ptr<Tarpit>> tarpits_;
  std::vector<Slot> slots_;
};

}  // namespace

TEST(RaftOverGrpc, ElectsWithPreVoteReplicatesAndSurvivesALeaderCrash) {
  GrpcCluster c(3, 3, RaftConfig());
  ASSERT_TRUE(WaitFor([&] { return c.Leader() != 0; }, 10000ms)) << "no leader over gRPC";
  const int leader = c.Leader();
  for (int i = 0; i < 30; ++i) {
    ASSERT_EQ(c.Node(leader)->ProposeAndWait(Put("k" + std::to_string(i), "v"), 5000ms).result,
              CommitResult::kApplied) << i;
  }
  for (int id = 1; id <= 3; ++id) ASSERT_TRUE(WaitFor([&] { return c.Sm(id)->Size() == 30; }, 5000ms)) << id;

  c.Stop(leader);  // crash the leader
  ASSERT_TRUE(WaitFor([&] { return c.Leader(leader) != 0; }, 10000ms)) << "no new leader";
  const int next = c.Leader(leader);
  ASSERT_EQ(c.Node(next)->ProposeAndWait(Put("after", "crash"), 5000ms).result, CommitResult::kApplied);
  for (int id = 1; id <= 3; ++id) {
    if (id == leader) continue;
    EXPECT_TRUE(WaitFor([&] { return c.Sm(id)->Has("after", "crash"); }, 5000ms)) << id;
  }
}

// The scenario that originally failed: a follower that missed more than 4 MiB of committed data
// could never catch up over gRPC. Now it is sent a chunked snapshot.
TEST(RaftOverGrpc, FollowerThatRebootsEmptyCatchesUpFromAChunkedSnapshot) {
  RaftConfig cfg;
  cfg.snapshot_threshold_entries = 3;
  GrpcCluster c(3, 3, cfg);
  ASSERT_TRUE(WaitFor([&] { return c.Leader() != 0; }, 10000ms));
  const int leader = c.Leader();
  const int victim = leader % 3 + 1;
  c.Stop(victim);
  for (int i = 0; i < 7; ++i) {  // 7 MiB committed while it is down
    ASSERT_EQ(c.Node(leader)
                  ->ProposeAndWait(Put("big" + std::to_string(i),
                                       std::string(1024 * 1024, static_cast<char>('a' + i))), 8000ms)
                  .result,
              CommitResult::kApplied) << i;
  }
  ASSERT_GT(c.Node(leader)->SnapshotIndex(), 0u) << "the leader should have compacted its log";

  c.Reboot(victim);
  ASSERT_TRUE(WaitFor([&] { return c.Sm(victim)->Size() == 7; }, 20000ms))
      << "the rebooted follower never caught up (" << c.Sm(victim)->Size() << " / 7)";
  for (int i = 0; i < 7; ++i) {
    EXPECT_TRUE(c.Sm(victim)->Has("big" + std::to_string(i),
                                  std::string(1024 * 1024, static_cast<char>('a' + i))))
        << "wrong data for big" << i;
  }
  EXPECT_GE(c.Sm(victim)->Restores(), 1) << "it should have been restored from a snapshot";
}

// One peer is a frozen process. The leader keeps trying to send it a snapshot, and that must not
// keep it from replicating to the healthy peer.
TEST(RaftOverGrpc, AnUnresponsivePeerDoesNotStallTheCluster) {
  RaftConfig cfg;
  cfg.snapshot_threshold_entries = 3;
  GrpcCluster c(3, 2, cfg);  // node 3 is a tarpit
  ASSERT_TRUE(WaitFor([&] { return c.Leader() != 0; }, 15000ms)) << "no leader with 2 of 3 alive";
  const int leader = c.Leader();
  int ok = 0, failed = 0;
  double worst = 0;
  for (int i = 0; i < 60; ++i) {
    auto t0 = Clock::now();
    auto r = c.Node(c.Leader() ? c.Leader() : leader)->ProposeAndWait(Put("f" + std::to_string(i), "v"), 4000ms);
    double ms = std::chrono::duration<double, std::milli>(Clock::now() - t0).count();
    if (r.result == CommitResult::kApplied) {
      ++ok;
      worst = std::max(worst, ms);
    } else {
      ++failed;
    }
  }
  EXPECT_EQ(failed, 0) << ok << " ok, " << failed << " failed";
  EXPECT_LT(worst, 2000.0) << "worst write took " << worst << " ms";
}

namespace {
struct Isolation {
  Term term_before = 0;
  Term isolated_term = 0;
  int leader_before = 0;
  int leader_after = 0;
  Term leader_term_after = 0;
};

Isolation RunIsolation(bool pre_vote) {
  Isolation r;
  RaftConfig cfg;
  cfg.enable_pre_vote = pre_vote;
  GrpcCluster c(3, 3, cfg);
  WaitFor([&] { return c.Leader() != 0; }, 10000ms);
  const int leader = c.Leader();
  const int follower = leader % 3 + 1;
  std::this_thread::sleep_for(400ms);
  r.leader_before = leader;
  r.term_before = c.Node(leader)->CurrentTerm();
  c.Isolate(follower);
  std::this_thread::sleep_for(2500ms);
  r.isolated_term = c.Node(follower)->CurrentTerm();
  c.Heal(follower);
  std::this_thread::sleep_for(2000ms);
  r.leader_after = c.Leader();
  if (r.leader_after != 0) r.leader_term_after = c.Node(r.leader_after)->CurrentTerm();
  return r;
}
}  // namespace

TEST(RaftOverGrpc, PreVoteKeepsAnIsolatedNodeFromDisruptingTheLeaderOnReturn) {
  Isolation r = RunIsolation(/*pre_vote=*/true);
  ASSERT_NE(r.leader_before, 0);
  EXPECT_EQ(r.isolated_term, r.term_before) << "a cut-off node raised its term";
  EXPECT_EQ(r.leader_after, r.leader_before) << "the returning node deposed the leader";
  EXPECT_EQ(r.leader_term_after, r.term_before) << "the returning node forced a new election";
}

TEST(RaftOverGrpc, WithoutPreVoteTheSameIsolationDisruptsTheCluster) {  // control
  Isolation r = RunIsolation(/*pre_vote=*/false);
  ASSERT_NE(r.leader_before, 0);
  EXPECT_GT(r.isolated_term, r.term_before) << "control: the cut-off node should inflate its term";
  EXPECT_GT(r.leader_term_after, r.term_before) << "control: its return should force an election";
}

}  // namespace raftkv
