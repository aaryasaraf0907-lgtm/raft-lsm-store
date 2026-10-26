// Whole nodes (durable Raft log, storage engine, state machine, both gRPC servers) in one process,
// driven through the real client library: the same wiring raftkv_node uses.
#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <stdlib.h>

#include <gtest/gtest.h>

#include "cli/kv_client.h"
#include "node/node_runtime.h"

namespace raftkv {
namespace {

using namespace std::chrono_literals;

bool WaitFor(const std::function<bool()>& pred, std::chrono::milliseconds timeout) {
  const auto end = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < end) {
    if (pred()) return true;
    std::this_thread::sleep_for(10ms);
  }
  return pred();
}

struct TempDir {
  std::string path;
  TempDir() {
    char tmpl[] = "/tmp/raftkv_node_test_XXXXXX";
    path = ::mkdtemp(tmpl);
  }
  ~TempDir() {
    std::error_code ec;
    std::filesystem::remove_all(path, ec);
  }
};

class NodeCluster {
 public:
  explicit NodeCluster(int n, size_t memtable_bytes = 0, size_t snapshot_threshold = 0)
      : n_(n), memtable_bytes_(memtable_bytes), snapshot_threshold_(snapshot_threshold), nodes_(n) {
    for (int id = 1; id <= n; ++id) Boot(id);
    WireAll();
    KvClientOptions options;
    for (int id = 1; id <= n; ++id) options.servers[id] = ClientAddress(id);
    options.op_timeout = 20s;
    client_ = std::make_unique<KvClient>(options);
  }
  ~NodeCluster() {
    client_.reset();
    for (auto& node : nodes_) {
      if (node) node->Stop();
    }
  }

  KvClient& client() { return *client_; }
  NodeRuntime* Node(int id) { return nodes_[id - 1].get(); }

  // Number of the node that currently reports itself leader (0 if none).
  int Leader() {
    for (int id = 1; id <= n_; ++id) {
      if (!nodes_[id - 1]) continue;
      network::proto::StatusResponse s;
      std::string error;
      if (client_->NodeStatus(id, &s, &error) && s.role() == "leader") return id;
    }
    return 0;
  }

  void Crash(int id) { nodes_[id - 1].reset(); }  // stops it; its data directory stays

  // Crashes the node, DELETES everything it had on disk, and boots it again: a replacement machine.
  void RestartEmpty(int id) {
    Crash(id);
    std::error_code ec;
    std::filesystem::remove_all(dir_.path + "/node" + std::to_string(id), ec);
    Restart(id);
  }

  // Boots node `id` again from its data directory (on new ports) and tells everyone.
  void Restart(int id) {
    Boot(id);
    WireAll();
    client_->SetServer(id, ClientAddress(id));
  }

 private:
  void Boot(int id) {
    NodeOptions options;
    options.id = id;
    for (int p = 1; p <= n_; ++p) options.cluster.push_back(p);
    options.data_dir = dir_.path + "/node" + std::to_string(id);
    options.sync = false;  // speed: crash safety is covered by the storage tests
    if (snapshot_threshold_ != 0) {
      options.snapshot_threshold_entries = snapshot_threshold_;
      options.snapshot_trailing_entries = 5;
    }
    if (memtable_bytes_ != 0) {
      options.memtable_bytes = memtable_bytes_;  // small: the engine flushes and compacts during the test
      options.compaction_trigger = 3;
    }
    nodes_[id - 1] = std::make_unique<NodeRuntime>(options);
    nodes_[id - 1]->Start();
  }
  void WireAll() {
    for (int a = 1; a <= n_; ++a) {
      if (!nodes_[a - 1]) continue;
      for (int b = 1; b <= n_; ++b) {
        if (a == b || !nodes_[b - 1]) continue;
        nodes_[a - 1]->SetPeerAddress(b, "127.0.0.1:" + std::to_string(nodes_[b - 1]->raft_port()));
      }
    }
  }
  std::string ClientAddress(int id) { return "127.0.0.1:" + std::to_string(nodes_[id - 1]->client_port()); }

  int n_;
  size_t memtable_bytes_;
  size_t snapshot_threshold_;
  TempDir dir_;
  std::vector<std::unique_ptr<NodeRuntime>> nodes_;
  std::unique_ptr<KvClient> client_;
};

std::string KeyOf(int i) { return "key" + std::to_string(i); }
std::string ValueOf(int i) { return "value-" + std::to_string(i); }

bool EngineHas(NodeRuntime* node, const std::string& key, const std::string& value) {
  std::string got;
  return node->store()->Get(key, &got) == Status::kOk && got == value;
}

}  // namespace

TEST(NodeClusterTest, PutGetDeleteAndTheSizeLimits) {
  NodeCluster c(3);
  EXPECT_EQ(c.client().Put("hello", "world").status, KvStatus::kOk);
  KvResult got = c.client().Get("hello");
  EXPECT_EQ(got.status, KvStatus::kOk);
  EXPECT_EQ(got.value, "world");
  EXPECT_EQ(c.client().Delete("hello").status, KvStatus::kOk);
  EXPECT_EQ(c.client().Get("hello").status, KvStatus::kNotFound);
  EXPECT_EQ(c.client().Get("never-written").status, KvStatus::kNotFound);

  // Binary keys and values survive: NULs and bytes that are not valid UTF-8.
  const std::string binary_key("a\0b\xff", 4), binary_value("\0\x01\xfe\xff\0", 5);
  EXPECT_EQ(c.client().Put(binary_key, binary_value).status, KvStatus::kOk);
  got = c.client().Get(binary_key);
  EXPECT_EQ(got.status, KvStatus::kOk);
  EXPECT_EQ(got.value, binary_value);

  EXPECT_EQ(c.client().Put("", "x").status, KvStatus::kInvalidArgument);
  EXPECT_EQ(c.client().Put("k", std::string(1024 * 1024, 'v')).status, KvStatus::kOk);
  EXPECT_EQ(c.client().Get("k").value.size(), 1024u * 1024u);
  EXPECT_EQ(c.client().Put("k", std::string(1024 * 1024 + 1, 'v')).status, KvStatus::kInvalidArgument);
  EXPECT_EQ(c.client().Put(std::string(4097, 'k'), "v").status, KvStatus::kInvalidArgument);
}

TEST(NodeClusterTest, StatusReportsExactlyOneLeader) {
  NodeCluster c(3);
  ASSERT_EQ(c.client().Put("warm", "up").status, KvStatus::kOk);
  ASSERT_TRUE(WaitFor(
      [&] {
        int leaders = 0, agree = 0, leader_id = 0;
        for (int id = 1; id <= 3; ++id) {
          network::proto::StatusResponse s;
          std::string error;
          if (!c.client().NodeStatus(id, &s, &error)) return false;
          if (s.role() == "leader") {
            ++leaders;
            leader_id = static_cast<int>(s.node_id());
          }
        }
        for (int id = 1; id <= 3; ++id) {
          network::proto::StatusResponse s;
          std::string error;
          c.client().NodeStatus(id, &s, &error);
          if (static_cast<int>(s.leader_id_hint()) == leader_id) ++agree;
        }
        return leaders == 1 && agree == 3;
      },
      10000ms)) << "the nodes do not agree on a single leader";
}

// The leader dies while a client is writing. The client retries each write with the SAME request
// number, through the election. Every write must be on every node, applied exactly once on the
// nodes that never restarted, and the crashed node must recover all of them from its own disk.
TEST(NodeClusterTest, LeaderCrashWhileAClientWritesLosesNothingAndAppliesEachWriteOnce) {
  NodeCluster c(3);
  ASSERT_EQ(c.client().Put("warm", "up").status, KvStatus::kOk);
  constexpr int kN = 60;
  std::atomic<int> acked{0};
  std::atomic<bool> failed{false};
  std::thread writer([&] {
    for (int i = 0; i < kN; ++i) {
      if (c.client().Put(KeyOf(i), ValueOf(i)).status != KvStatus::kOk) {
        failed = true;
        return;
      }
      acked = i + 1;
    }
  });
  while (acked.load() < 15 && !failed) std::this_thread::sleep_for(1ms);
  const int victim = c.Leader();
  ASSERT_NE(victim, 0);
  c.Crash(victim);  // mid-stream: the client is still writing
  writer.join();
  ASSERT_FALSE(failed) << "a write was never acknowledged";

  for (int id = 1; id <= 3; ++id) {
    if (id == victim) continue;
    ASSERT_TRUE(WaitFor(
        [&] {
          for (int i = 0; i < kN; ++i) {
            if (!EngineHas(c.Node(id), KeyOf(i), ValueOf(i))) return false;
          }
          return true;
        },
        15000ms)) << "node " << id << " is missing an acknowledged write";
    // +1 for the warm-up write. A retry that was applied twice would make this larger.
    EXPECT_EQ(c.Node(id)->state_machine()->Applied(), static_cast<uint64_t>(kN + 1)) << "node " << id;
  }

  c.Restart(victim);  // it recovers from its own Raft log and engine files
  ASSERT_TRUE(WaitFor(
      [&] {
        for (int i = 0; i < kN; ++i) {
          if (!EngineHas(c.Node(victim), KeyOf(i), ValueOf(i))) return false;
        }
        return c.Node(victim)->state_machine()->Applied() == static_cast<uint64_t>(kN + 1);
      },
      15000ms)) << "the restarted node did not recover";
}

TEST(NodeClusterTest, WholeClusterRestartKeepsTheData) {
  NodeCluster c(3);
  for (int i = 0; i < 30; ++i) ASSERT_EQ(c.client().Put(KeyOf(i), ValueOf(i)).status, KvStatus::kOk) << i;
  ASSERT_EQ(c.client().Delete(KeyOf(7)).status, KvStatus::kOk);
  for (int id = 1; id <= 3; ++id) c.Crash(id);
  for (int id = 1; id <= 3; ++id) c.Restart(id);
  for (int i = 0; i < 30; ++i) {
    KvResult got = c.client().Get(KeyOf(i));
    if (i == 7) {
      EXPECT_EQ(got.status, KvStatus::kNotFound);
    } else {
      ASSERT_EQ(got.status, KvStatus::kOk) << i;
      EXPECT_EQ(got.value, ValueOf(i));
    }
  }
  EXPECT_EQ(c.client().Put("after-restart", "ok").status, KvStatus::kOk);
}

// The real LSM engine under the cluster: tiny memtables so that flushes and compactions happen all
// the time while a client writes and the leader dies. Restarting replays the Raft log through the
// state machine, and the engine's applied index (kept in its SSTables) lets replay skip the writes
// the engine already holds.
TEST(NodeClusterTest, LeaderCrashWithFlushesAndCompactionsLosesNothing) {
  NodeCluster c(3, /*memtable_bytes=*/8192);
  ASSERT_EQ(c.client().Put("warm", "up").status, KvStatus::kOk);
  constexpr int kN = 150;
  const std::string pad(300, 'p');
  std::atomic<int> acked{0};
  std::atomic<bool> failed{false};
  std::thread writer([&] {
    for (int i = 0; i < kN; ++i) {
      if (c.client().Put(KeyOf(i), ValueOf(i) + pad).status != KvStatus::kOk) {
        failed = true;
        return;
      }
      acked = i + 1;
    }
  });
  while (acked.load() < 60 && !failed) std::this_thread::sleep_for(1ms);
  const int victim = c.Leader();
  ASSERT_NE(victim, 0);
  c.Crash(victim);
  writer.join();
  ASSERT_FALSE(failed);
  for (int id = 1; id <= 3; ++id) {
    if (id == victim) continue;
    ASSERT_TRUE(WaitFor(
        [&] {
          for (int i = 0; i < kN; ++i) {
            if (!EngineHas(c.Node(id), KeyOf(i), ValueOf(i) + pad)) return false;
          }
          return true;
        },
        20000ms)) << "node " << id << " is missing an acknowledged write";
  }
  c.Restart(victim);
  ASSERT_TRUE(WaitFor(
      [&] {
        for (int i = 0; i < kN; ++i) {
          if (!EngineHas(c.Node(victim), KeyOf(i), ValueOf(i) + pad)) return false;
        }
        return true;
      },
      20000ms)) << "the restarted node did not recover from its SSTables and WAL";
  EXPECT_GT(c.Node(victim)->state_machine()->ReplaySkipped(), 0u)
      << "replay should have skipped what the engine already held";
}

// With snapshots on, the Raft log stays bounded however much is written, and the data survives a
// restart of the whole cluster (which restores from snapshots instead of replaying a huge log).
TEST(NodeClusterTest, SnapshotsBoundTheRaftLogAndTheDataSurvivesAFullRestart) {
  NodeCluster c(3, /*memtable_bytes=*/16384, /*snapshot_threshold=*/25);
  const std::string pad(150, 'p');
  for (int i = 0; i < 300; ++i) ASSERT_EQ(c.client().Put(KeyOf(i), ValueOf(i) + pad).status, KvStatus::kOk) << i;
  for (int id = 1; id <= 3; ++id) {
    ASSERT_TRUE(WaitFor([&] { return c.Node(id)->node()->SnapshotIndex() > 0; }, 15000ms)) << "node " << id << " never snapshotted";
    EXPECT_LT(c.Node(id)->node()->LogLength(), 70u) << "node " << id << " keeps an unbounded Raft log";
  }
  for (int id = 1; id <= 3; ++id) c.Crash(id);
  for (int id = 1; id <= 3; ++id) c.Restart(id);
  for (int i = 0; i < 300; ++i) {
    KvResult got = c.client().Get(KeyOf(i));
    ASSERT_EQ(got.status, KvStatus::kOk) << i;
    ASSERT_EQ(got.value, ValueOf(i) + pad) << i;
  }
  EXPECT_EQ(c.client().Put("after", "restart").status, KvStatus::kOk);
}

// The scenario the whole snapshot machinery exists for: a node that was down while the leader
// compacted its Raft log past that node's position. The log entries it needs no longer exist, so the
// leader sends its snapshot over gRPC; the node installs it into its engine, REPLACING whatever the
// engine held, and carries on.
TEST(NodeClusterTest, ALaggingFollowerCatchesUpFromASnapshotThatReplacesItsOldData) {
  NodeCluster c(3, /*memtable_bytes=*/16384, /*snapshot_threshold=*/25);
  const std::string pad(150, 'p');
  ASSERT_EQ(c.client().Put("gone-later", "x").status, KvStatus::kOk);
  const int leader = c.Leader();
  ASSERT_NE(leader, 0);
  const int victim = leader % 3 + 1;
  ASSERT_TRUE(WaitFor([&] { return EngineHas(c.Node(victim), "gone-later", "x"); }, 10000ms));
  c.Crash(victim);  // down while the cluster moves on
  ASSERT_EQ(c.client().Delete("gone-later").status, KvStatus::kOk);
  for (int i = 0; i < 200; ++i) ASSERT_EQ(c.client().Put(KeyOf(i), ValueOf(i) + pad).status, KvStatus::kOk) << i;
  ASSERT_TRUE(WaitFor([&] { return c.Node(leader)->node()->SnapshotIndex() > 50; }, 15000ms))
      << "the leader should have dropped the part of its log the follower needs";
  c.Restart(victim);  // from its own, now stale, disk
  ASSERT_TRUE(WaitFor(
      [&] {
        for (int i = 0; i < 200; ++i) {
          if (!EngineHas(c.Node(victim), KeyOf(i), ValueOf(i) + pad)) return false;
        }
        return true;
      },
      30000ms)) << "the lagging node never caught up";
  EXPECT_GT(c.Node(victim)->node()->SnapshotIndex(), 0u) << "it should have been brought up to date by a snapshot";
  std::string v;
  EXPECT_EQ(c.Node(victim)->store()->Get("gone-later", &v), Status::kNotFound) << "the old dataset must have been replaced";
  ASSERT_EQ(c.client().Put("later", "still-works").status, KvStatus::kOk);
  EXPECT_TRUE(WaitFor([&] { return EngineHas(c.Node(victim), "later", "still-works"); }, 10000ms));
}

// A replacement machine with an empty disk, brought in while the cluster has only ever heard from
// it at the very start. (A node that is wiped AFTER it caught up cannot rejoin under the same id:
// Raft assumes a node keeps its persisted state, and replacing one needs a membership change.)
TEST(NodeClusterTest, AnEmptyReplacementThatNeverCaughtUpIsBroughtUpBySnapshot) {
  NodeCluster c(3, /*memtable_bytes=*/16384, /*snapshot_threshold=*/25);
  const std::string pad(150, 'p');
  ASSERT_EQ(c.client().Put("warm", "up").status, KvStatus::kOk);
  const int leader = c.Leader();
  ASSERT_NE(leader, 0);
  const int victim = leader % 3 + 1;
  c.Crash(victim);
  for (int i = 0; i < 200; ++i) ASSERT_EQ(c.client().Put(KeyOf(i), ValueOf(i) + pad).status, KvStatus::kOk) << i;
  ASSERT_TRUE(WaitFor([&] { return c.Node(leader)->node()->SnapshotIndex() > 50; }, 15000ms));
  std::error_code ec;
  c.RestartEmpty(victim);
  ASSERT_TRUE(WaitFor(
      [&] {
        for (int i = 0; i < 200; ++i) {
          if (!EngineHas(c.Node(victim), KeyOf(i), ValueOf(i) + pad)) return false;
        }
        return true;
      },
      30000ms)) << "the replacement node never caught up";
  EXPECT_GT(c.Node(victim)->node()->SnapshotIndex(), 0u);
}

}  // namespace raftkv
