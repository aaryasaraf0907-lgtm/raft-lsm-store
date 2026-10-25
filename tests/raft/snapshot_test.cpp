// Snapshots and log compaction: the log is bounded, a follower that fell behind the dropped log
// catches up from a (chunked) snapshot, restarts restore from the snapshot, and the exactly-once
// session table survives all of it.
#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "raft/client_sessions.h"
#include "raft/fake_transport.h"
#include "raft/file_raft_storage.h"
#include "raft/raft_node.h"
#include "test_cluster.h"

using namespace raftkv;
using namespace raftkv::testing_util;
using namespace std::chrono_literals;

namespace {

constexpr uint64_t kClient = 7;

void PutLen(std::string* s, uint64_t v) {
  for (int i = 0; i < 8; ++i) s->push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
}
uint64_t GetLen(const std::string& s, size_t* pos) {
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) v |= static_cast<uint64_t>(static_cast<uint8_t>(s[*pos + i])) << (8 * i);
  *pos += 8;
  return v;
}
void PutBlob(std::string* s, const std::string& b) {
  PutLen(s, b.size());
  s->append(b);
}
std::string GetBlob(const std::string& s, size_t* pos) {
  uint64_t n = GetLen(s, pos);
  std::string b = s.substr(*pos, n);
  *pos += n;
  return b;
}

// A tiny state machine with the same semantics as KVStateMachine (dedup, then apply), plus
// Snapshot/Restore. The snapshot holds the data AND the session table.
struct MapStateMachine {
  std::mutex mu;
  std::map<std::string, std::string> kv;
  ClientSessionTable sessions;
  std::map<std::string, int> puts;  // writes applied by this incarnation
  int restores = 0;
  uint64_t duplicates = 0;

  void Apply(const LogEntry& e) {
    std::lock_guard<std::mutex> lk(mu);
    if (!sessions.ShouldApply(e.command)) {
      ++duplicates;
      return;
    }
    if (e.command.type == CommandType::kPut) {
      kv[e.command.key] = e.command.value;
      ++puts[e.command.key];
    } else if (e.command.type == CommandType::kDelete) {
      kv.erase(e.command.key);
    }
  }
  std::string Snapshot() {
    std::lock_guard<std::mutex> lk(mu);
    std::string out;
    PutLen(&out, kv.size());
    for (const auto& p : kv) {
      PutBlob(&out, p.first);
      PutBlob(&out, p.second);
    }
    PutBlob(&out, sessions.Serialize());
    return out;
  }
  void Restore(const std::string& bytes) {
    std::lock_guard<std::mutex> lk(mu);
    size_t pos = 0;
    uint64_t n = GetLen(bytes, &pos);
    kv.clear();
    for (uint64_t i = 0; i < n; ++i) {
      std::string k = GetBlob(bytes, &pos);
      kv[k] = GetBlob(bytes, &pos);
    }
    EXPECT_TRUE(sessions.Restore(GetBlob(bytes, &pos)));
    ++restores;
  }
  int Restores() {
    std::lock_guard<std::mutex> lk(mu);
    return restores;
  }
  uint64_t Duplicates() {
    std::lock_guard<std::mutex> lk(mu);
    return duplicates;
  }
  bool Has(const std::string& k, const std::string& v) {
    std::lock_guard<std::mutex> lk(mu);
    auto it = kv.find(k);
    return it != kv.end() && it->second == v;
  }
  int PutsOf(const std::string& k) {
    std::lock_guard<std::mutex> lk(mu);
    auto it = puts.find(k);
    return it == puts.end() ? 0 : it->second;
  }
  std::map<std::string, std::string> Data() {
    std::lock_guard<std::mutex> lk(mu);
    return kv;
  }
};

struct Options {
  int n = 3;
  bool persistent = false;
  size_t threshold = 10;
  size_t max_bytes = 1 << 20;
};

class SnapCluster {
 public:
  explicit SnapCluster(Options o) : opt_(o), slots_(static_cast<size_t>(o.n)) {
    if (o.persistent) tmp_ = std::make_unique<TempDir>();
    for (int id = 1; id <= o.n; ++id) transports_.push_back(net_.MakeTransport(static_cast<NodeId>(id)));
    for (int id = 1; id <= o.n; ++id) Boot(static_cast<NodeId>(id));
    for (auto& s : slots_) s.node->Start();
  }
  ~SnapCluster() {
    for (auto& s : slots_) {
      if (s.node) s.node->Stop();
    }
    net_.Shutdown();
  }
  FakeNetwork& net() { return net_; }
  RaftNodeImpl* Node(NodeId id) { return slots_[id - 1].node.get(); }
  MapStateMachine* Sm(NodeId id) { return slots_[id - 1].sm.get(); }
  void Crash(NodeId id) { slots_[id - 1].node->Stop(); }
  void Restart(NodeId id) {
    Slot& s = slots_[id - 1];
    s.node->Stop();
    s.node.reset();
    s.sm.reset();
    s.storage.reset();
    Boot(id);
    s.node->Start();
  }
  void RestartAll() {
    for (auto& s : slots_) s.node->Stop();
    for (auto& s : slots_) {
      s.node.reset();
      s.sm.reset();
      s.storage.reset();
    }
    for (int id = 1; id <= opt_.n; ++id) Boot(static_cast<NodeId>(id));
    for (auto& s : slots_) s.node->Start();
  }
  std::vector<NodeId> Leaders(unsigned exclude_mask) {
    std::vector<NodeId> out;
    for (int id = 1; id <= opt_.n; ++id) {
      if ((exclude_mask >> id) & 1u) continue;
      if (Node(static_cast<NodeId>(id))->GetRole() == Role::kLeader) out.push_back(static_cast<NodeId>(id));
    }
    return out;
  }
  NodeId Leader(unsigned exclude_mask = 0) {
    auto v = Leaders(exclude_mask);
    return v.empty() ? 0 : v[0];
  }

 private:
  struct Slot {
    std::unique_ptr<FileRaftStorage> storage;
    std::unique_ptr<MapStateMachine> sm;
    std::unique_ptr<RaftNodeImpl> node;
  };
  void Boot(NodeId id) {
    Slot& s = slots_[id - 1];
    if (tmp_) {
      s.storage = std::make_unique<FileRaftStorage>((tmp_->path / ("node" + std::to_string(id))).string(),
                                                   /*sync=*/false);
    }
    s.sm = std::make_unique<MapStateMachine>();
    std::vector<NodeId> peers;
    for (int p = 1; p <= opt_.n; ++p) {
      if (static_cast<NodeId>(p) != id) peers.push_back(static_cast<NodeId>(p));
    }
    RaftConfig cfg;
    cfg.random_seed = 42;
    cfg.snapshot_threshold_entries = opt_.threshold;
    cfg.max_bytes_per_append = opt_.max_bytes;
    s.node = std::make_unique<RaftNodeImpl>(id, peers, transports_[id - 1].get(), cfg, s.storage.get());
    MapStateMachine* sm = s.sm.get();
    s.node->SetApplyCallback([sm](const LogEntry& e) { sm->Apply(e); });
    s.node->SetSnapshotCallbacks([sm] { return sm->Snapshot(); },
                                 [sm](const std::string& b) { sm->Restore(b); });
  }

  Options opt_;
  std::unique_ptr<TempDir> tmp_;
  FakeNetwork net_;
  std::vector<std::unique_ptr<FakeTransport>> transports_;
  std::vector<Slot> slots_;
};

std::string KeyOf(uint64_t s) { return "key" + std::to_string(s); }
std::string ValueOf(uint64_t s) { return "value-" + std::to_string(s) + std::string(40, 'x'); }

Command PutCmd(uint64_t seq) {
  Command c{CommandType::kPut, KeyOf(seq), ValueOf(seq)};
  c.client_id = kClient;
  c.seq = seq;
  return c;
}

// Retries one request (same seq) on whichever up node is leader until it is acknowledged.
bool Write(SnapCluster& c, const Command& cmd, const std::atomic<unsigned>& down) {
  auto deadline = std::chrono::steady_clock::now() + 40000ms;
  while (std::chrono::steady_clock::now() < deadline) {
    for (NodeId leader : c.Leaders(down.load())) {
      if (c.Node(leader)->ProposeAndWait(cmd, 1000ms).result == CommitResult::kApplied) return true;
    }
    std::this_thread::sleep_for(20ms);
  }
  return false;
}

bool AllHave(SnapCluster& c, uint64_t first, uint64_t last, const std::set<NodeId>& nodes) {
  for (NodeId id : nodes) {
    for (uint64_t s = first; s <= last; ++s) {
      if (!c.Sm(id)->Has(KeyOf(s), ValueOf(s))) return false;
    }
  }
  return true;
}

}  // namespace

TEST(RaftSnapshot, LogIsCompactedAndStateIsKept) {
  SnapCluster c({3, false, 10});
  ASSERT_TRUE(WaitFor([&] { return c.Leader() != 0; }, 8000ms));
  const std::atomic<unsigned> none{0};
  for (uint64_t s = 1; s <= 80; ++s) ASSERT_TRUE(Write(c, PutCmd(s), none));
  ASSERT_TRUE(WaitFor([&] { return AllHave(c, 1, 80, {1, 2, 3}); }, 10000ms));
  for (NodeId id = 1; id <= 3; ++id) {
    EXPECT_GT(c.Node(id)->SnapshotIndex(), 0u) << "node " << id;
    EXPECT_LT(c.Node(id)->LogLength(), 25u) << "node " << id << " never dropped its log";
  }
}

TEST(RaftSnapshot, ProposeAndWaitSucceedsWhileTheLogIsBeingCompacted) {
  SnapCluster c({3, false, 5});
  ASSERT_TRUE(WaitFor([&] { return c.Leader() != 0; }, 8000ms));
  NodeId leader = c.Leader();
  for (uint64_t s = 1; s <= 60; ++s) {
    ASSERT_EQ(c.Node(leader)->ProposeAndWait(PutCmd(s), 5000ms).result, CommitResult::kApplied) << s;
  }
  EXPECT_GT(c.Node(leader)->SnapshotIndex(), 0u);
}

TEST(RaftSnapshot, LaggingFollowerCatchesUpFromAMultiChunkSnapshot) {
  // Small messages force the snapshot to travel in several chunks, and the network drops any
  // message over 1 KB, so an unchunked snapshot could never arrive.
  SnapCluster c({3, false, 10, /*max_bytes=*/512});
  c.net().SetMaxMessageBytes(1024);
  ASSERT_TRUE(WaitFor([&] { return c.Leader() != 0; }, 8000ms));
  NodeId leader = c.Leader();
  NodeId follower = leader % 3 + 1;
  c.net().Disconnect(follower);
  std::atomic<unsigned> down{1u << follower};
  for (uint64_t s = 1; s <= 70; ++s) ASSERT_TRUE(Write(c, PutCmd(s), down));
  ASSERT_GT(c.Node(leader)->SnapshotIndex(), 0u);
  c.net().Reconnect(follower);
  ASSERT_TRUE(WaitFor([&] { return c.Sm(follower)->Restores() >= 1 && AllHave(c, 1, 70, {follower}); },
                      15000ms));
  EXPECT_EQ(c.net().OversizedDropped(), 0u);
  EXPECT_GT(c.Node(follower)->SnapshotIndex(), 0u);
}

TEST(RaftSnapshot, RestartRestoresFromTheSnapshotThenReplaysTheTail) {
  SnapCluster c({3, true, 10});
  ASSERT_TRUE(WaitFor([&] { return c.Leader() != 0; }, 8000ms));
  const std::atomic<unsigned> none{0};
  for (uint64_t s = 1; s <= 45; ++s) ASSERT_TRUE(Write(c, PutCmd(s), none));
  ASSERT_TRUE(WaitFor([&] { return AllHave(c, 1, 45, {1, 2, 3}); }, 10000ms));
  c.RestartAll();
  ASSERT_TRUE(WaitFor([&] { return c.Leader() != 0; }, 8000ms));
  ASSERT_TRUE(WaitFor([&] { return AllHave(c, 1, 45, {1, 2, 3}); }, 15000ms));
  for (NodeId id = 1; id <= 3; ++id) {
    EXPECT_GE(c.Sm(id)->Restores(), 1) << "node " << id << " did not restore from its snapshot";
    size_t reapplied = 0;
    for (uint64_t s = 1; s <= 45; ++s) reapplied += static_cast<size_t>(c.Sm(id)->PutsOf(KeyOf(s)));
    EXPECT_LT(reapplied, 45u) << "node " << id << " replayed the whole history";
  }
}

// Client A's requests end up ONLY inside the snapshot (client B's fill the log after it), so a
// follower that restored from the snapshot knows about A solely through the restored session
// table. A resent request of A (here with a tampered value, to make a re-apply visible) must still
// be skipped there, exactly as on the nodes that applied the original from the log.
TEST(RaftSnapshot, ExactlyOnceSurvivesSnapshotInstall) {
  SnapCluster c({3, false, 10});
  ASSERT_TRUE(WaitFor([&] { return c.Leader() != 0; }, 8000ms));
  NodeId leader = c.Leader();
  NodeId follower = leader % 3 + 1;
  c.net().Disconnect(follower);
  std::atomic<unsigned> down{1u << follower};
  for (uint64_t s = 1; s <= 30; ++s) ASSERT_TRUE(Write(c, PutCmd(s), down));  // client A
  auto b_put = [](uint64_t s) {
    Command cmd{CommandType::kPut, "b" + std::to_string(s), ValueOf(s)};
    cmd.client_id = kClient + 1;
    cmd.seq = s;
    return cmd;
  };
  for (uint64_t s = 1; s <= 40; ++s) ASSERT_TRUE(Write(c, b_put(s), down));  // client B
  c.net().Reconnect(follower);
  ASSERT_TRUE(WaitFor(
      [&] {
        return c.Sm(follower)->Restores() >= 1 && AllHave(c, 1, 30, {follower}) &&
               c.Sm(follower)->Has("b40", ValueOf(40));
      },
      15000ms));
  down = 0;
  Command resend = PutCmd(5);
  resend.value = "TAMPERED";
  ASSERT_TRUE(Write(c, resend, down));  // a resend of A's request 5
  ASSERT_TRUE(Write(c, b_put(41), down));
  ASSERT_TRUE(WaitFor([&] { return c.Sm(follower)->Has("b41", ValueOf(41)); }, 10000ms));
  for (NodeId id = 1; id <= 3; ++id) {
    EXPECT_TRUE(c.Sm(id)->Has(KeyOf(5), ValueOf(5))) << "node " << id << " re-applied request 5";
    EXPECT_EQ(c.Sm(id)->sessions.LastSeq(kClient), 30u) << "node " << id;
  }
  EXPECT_GE(c.Sm(follower)->Duplicates(), 1u);
}

// Random crashes and partitions while a client writes, with snapshots every 8 entries and tiny
// messages. Every acknowledged write must end up on every node, also after a final full restart.
TEST(RaftSnapshot, ChaosWithSnapshotsLosesNothing) {
  for (uint32_t seed : {1u, 2u, 3u}) {
    SCOPED_TRACE("seed=" + std::to_string(seed));
    SnapCluster c({3, true, 8, /*max_bytes=*/512});
    ASSERT_TRUE(WaitFor([&] { return c.Leader() != 0; }, 8000ms));
    constexpr uint64_t kN = 100;
    std::atomic<unsigned> down{0};
    std::atomic<bool> pause{false}, idle{false}, ok{true}, finished{false};
    std::thread writer([&] {
      for (uint64_t s = 1; s <= kN; ++s) {
        if (pause) {
          idle = true;
          while (pause) std::this_thread::sleep_for(1ms);
          idle = false;
        }
        if (!Write(c, PutCmd(s), down)) {
          ok = false;
          break;
        }
      }
      finished = true;
    });
    std::mt19937 rng(seed);
    std::set<NodeId> crashed;
    std::set<NodeId> cut;
    while (!finished) {
      std::this_thread::sleep_for(std::chrono::milliseconds(20 + rng() % 60));
      NodeId id = static_cast<NodeId>(1 + rng() % 3);
      int action = static_cast<int>(rng() % 4);
      if (action == 0 && crashed.empty() && cut.empty()) {
        c.Crash(id);
        crashed.insert(id);
        down = down.load() | (1u << id);
      } else if (action == 1 && crashed.empty() && cut.empty()) {
        c.net().Disconnect(id);
        cut.insert(id);
        down = down.load() | (1u << id);
      } else if (action >= 2) {
        for (NodeId n : cut) {
          c.net().Reconnect(n);
          down = down.load() & ~(1u << n);
        }
        cut.clear();
        if (!crashed.empty()) {
          pause = true;  // restarting destroys the node: let the writer finish its request first
          for (int i = 0; i < 20000 && !idle && !finished; ++i) std::this_thread::sleep_for(1ms);
          for (NodeId n : crashed) {
            c.Restart(n);
            down = down.load() & ~(1u << n);
          }
          crashed.clear();
          pause = false;
        }
      }
    }
    writer.join();
    ASSERT_TRUE(ok) << "a client request was never acknowledged";
    for (NodeId n : cut) c.net().Reconnect(n);
    for (NodeId n : crashed) c.Restart(n);
    ASSERT_TRUE(WaitFor([&] { return AllHave(c, 1, kN, {1, 2, 3}); }, 30000ms)) << "lost a write";
    unsigned max_snapshot = 0;
    for (NodeId id = 1; id <= 3; ++id) max_snapshot = std::max<unsigned>(max_snapshot, static_cast<unsigned>(c.Node(id)->SnapshotIndex()));
    EXPECT_GT(max_snapshot, 0u) << "the run never took a snapshot";

    c.RestartAll();
    ASSERT_TRUE(WaitFor([&] { return c.Leader() != 0; }, 10000ms));
    ASSERT_TRUE(WaitFor([&] { return AllHave(c, 1, kN, {1, 2, 3}); }, 30000ms))
        << "lost a write across a full restart";
  }
}

// ---- storage level ----

namespace {
LogEntry Entry(LogIndex i, Term t, const std::string& key) {
  return LogEntry{t, i, Command{CommandType::kPut, key, "v", 5, i}};
}
}  // namespace

TEST(FileRaftStorageSnapshot, RoundTripKeepsSnapshotSuffixAndHardState) {
  TempDir dir;
  {
    FileRaftStorage st(dir.path.string(), false);
    PersistentState ps;
    ASSERT_TRUE(st.Load(&ps));
    ASSERT_TRUE(st.SaveHardState(4, 2));
    std::vector<LogEntry> all;
    for (LogIndex i = 1; i <= 6; ++i) all.push_back(Entry(i, 3, "k" + std::to_string(i)));
    ASSERT_TRUE(st.AppendEntries(all));
    ASSERT_TRUE(st.SaveSnapshot(4, 3, std::string("snap\0data", 9), {all[4], all[5]}));
    ASSERT_TRUE(st.AppendEntries({Entry(7, 4, "k7")}));
    ASSERT_TRUE(st.TruncateSuffix(7));
    ASSERT_TRUE(st.AppendEntries({Entry(7, 5, "k7b")}));
  }
  FileRaftStorage st(dir.path.string(), false);
  PersistentState ps;
  ASSERT_TRUE(st.Load(&ps));
  EXPECT_EQ(ps.term, 4u);
  EXPECT_EQ(ps.voted_for, 2u);
  EXPECT_EQ(ps.snapshot_index, 4u);
  EXPECT_EQ(ps.snapshot_term, 3u);
  EXPECT_EQ(ps.snapshot_data, std::string("snap\0data", 9));
  ASSERT_EQ(ps.log.size(), 3u);
  EXPECT_EQ(ps.log[0].index, 5u);
  EXPECT_EQ(ps.log[1].index, 6u);
  EXPECT_EQ(ps.log[2].index, 7u);
  EXPECT_EQ(ps.log[2].term, 5u);
  EXPECT_EQ(ps.log[2].command.key, "k7b");
  EXPECT_EQ(ps.log[2].command.client_id, 5u);
}

TEST(FileRaftStorageSnapshot, LeftoversFromACrashedSnapshotAreIgnoredAndRemoved) {
  TempDir dir;
  {
    FileRaftStorage st(dir.path.string(), false);
    PersistentState ps;
    ASSERT_TRUE(st.Load(&ps));
    ASSERT_TRUE(st.AppendEntries({Entry(1, 1, "a"), Entry(2, 1, "b"), Entry(3, 1, "c")}));
    ASSERT_TRUE(st.SaveSnapshot(2, 1, "old", {Entry(3, 1, "c")}));
  }
  // A crash after writing the next snapshot file but before swapping the log in, plus a torn temp.
  { std::ofstream(dir.path / "snapshot-3.bin") << "new snapshot the log never pointed at"; }
  { std::ofstream(dir.path / "raft.log.tmp") << "half written"; }
  FileRaftStorage st(dir.path.string(), false);
  PersistentState ps;
  ASSERT_TRUE(st.Load(&ps));
  EXPECT_EQ(ps.snapshot_index, 2u);
  EXPECT_EQ(ps.snapshot_data, "old");
  ASSERT_EQ(ps.log.size(), 1u);
  EXPECT_FALSE(std::filesystem::exists(dir.path / "snapshot-3.bin"));
  EXPECT_FALSE(std::filesystem::exists(dir.path / "raft.log.tmp"));
}

TEST(FileRaftStorageSnapshot, CorruptSnapshotFileMakesLoadFail) {
  TempDir dir;
  {
    FileRaftStorage st(dir.path.string(), false);
    PersistentState ps;
    ASSERT_TRUE(st.Load(&ps));
    ASSERT_TRUE(st.AppendEntries({Entry(1, 1, "a"), Entry(2, 1, "b")}));
    ASSERT_TRUE(st.SaveSnapshot(2, 1, "payload-bytes", {}));
  }
  std::string path = (dir.path / "snapshot-2.bin").string();
  std::string bytes;
  { std::ifstream in(path, std::ios::binary); bytes.assign(std::istreambuf_iterator<char>(in), {}); }
  bytes[bytes.size() / 2] ^= 0x40;
  { std::ofstream out(path, std::ios::binary | std::ios::trunc); out << bytes; }
  FileRaftStorage st(dir.path.string(), false);
  PersistentState ps;
  EXPECT_FALSE(st.Load(&ps));
}
