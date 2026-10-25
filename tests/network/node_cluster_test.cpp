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
  explicit NodeCluster(int n) : n_(n), nodes_(n) {
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

}  // namespace raftkv
