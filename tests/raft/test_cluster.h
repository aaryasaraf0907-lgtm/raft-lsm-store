#pragma once
// Shared helpers for the Raft tests: a small cluster on a FakeNetwork.
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

#include "raft/fake_transport.h"
#include "raft/file_raft_storage.h"
#include "raft/raft_node.h"

namespace raftkv {
namespace testing_util {

inline bool WaitFor(const std::function<bool()>& pred, std::chrono::milliseconds timeout) {
  auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (pred()) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return pred();
}

inline Command Put(const std::string& key) { return Command{CommandType::kPut, key, "v"}; }

inline bool Contains(const std::vector<std::string>& keys, const std::string& key) {
  for (const auto& k : keys) {
    if (k == key) return true;
  }
  return false;
}

// True if one sequence is a prefix of the other (nodes may lag, but must never disagree).
inline bool PrefixConsistent(const std::vector<std::string>& a, const std::vector<std::string>& b) {
  size_t n = std::min(a.size(), b.size());
  for (size_t i = 0; i < n; ++i) {
    if (a[i] != b[i]) return false;
  }
  return true;
}

// A fresh temporary directory, removed when the object goes away.
struct TempDir {
  std::filesystem::path path;

  TempDir() {
    static std::atomic<int> counter{0};
    path = std::filesystem::temp_directory_path() /
           ("raftkv_test_" + std::to_string(::getpid()) + "_" + std::to_string(counter++));
    std::filesystem::create_directories(path);
  }
  ~TempDir() {
    std::error_code ec;
    std::filesystem::remove_all(path, ec);
  }
  TempDir(const TempDir&) = delete;
  TempDir& operator=(const TempDir&) = delete;
};

// n nodes (ids 1..n) on one FakeNetwork. Every committed entry a node applies is recorded.
// With persistent = true each node keeps its term/vote/log in a file (in a temp directory), so
// Restart()/RestartAll() simulate a crash followed by a reboot from disk.
// Member order matters: recordings, the temp dir and the network outlive transports and nodes.
struct Cluster {
  std::mutex applied_mu;
  std::vector<std::vector<LogEntry>> applied;  // applied[id-1] = entries in apply order
  std::unique_ptr<TempDir> tmp;                // only when persistent
  FakeNetwork net;
  std::vector<std::unique_ptr<FakeTransport>> transports;
  std::vector<std::unique_ptr<FileRaftStorage>> storages;  // null entries when not persistent
  std::vector<std::unique_ptr<RaftNodeImpl>> nodes;
  int size;

  explicit Cluster(int n, bool persistent = false)
      : applied(static_cast<size_t>(n)), size(n) {
    if (persistent) tmp = std::make_unique<TempDir>();
    storages.resize(static_cast<size_t>(n));
    for (int id = 1; id <= n; ++id) transports.push_back(net.MakeTransport(static_cast<NodeId>(id)));
    for (int id = 1; id <= n; ++id) nodes.push_back(MakeNode(static_cast<NodeId>(id)));
    for (auto& node : nodes) node->Start();
  }

  ~Cluster() {
    for (auto& node : nodes) {
      if (node) node->Stop();
    }
    net.Shutdown();
  }

  // Crash node `id` and boot a fresh one from whatever it had on disk. The node's applied
  // record is cleared: its state machine starts empty and is rebuilt by replaying the log.
  void Restart(NodeId id) {
    nodes[id - 1]->Stop();
    nodes[id - 1].reset();
    ClearApplied(id);
    nodes[id - 1] = MakeNode(id);
    nodes[id - 1]->Start();
  }

  void RestartAll() {
    for (auto& node : nodes) node->Stop();
    for (auto& node : nodes) node.reset();
    for (NodeId id = 1; id <= static_cast<NodeId>(size); ++id) {
      ClearApplied(id);
      nodes[id - 1] = MakeNode(id);
    }
    for (auto& node : nodes) node->Start();
  }

  RaftNodeImpl* Node(NodeId id) { return nodes[id - 1].get(); }

  int CountLeaders(const std::set<NodeId>& exclude = {}) {
    int count = 0;
    for (auto& node : nodes) {
      if (exclude.count(node->Id()) == 0 && node->GetRole() == Role::kLeader) ++count;
    }
    return count;
  }

  NodeId FindLeader(const std::set<NodeId>& exclude = {}) {
    for (auto& node : nodes) {
      if (exclude.count(node->Id()) == 0 && node->GetRole() == Role::kLeader) return node->Id();
    }
    return 0;
  }

  // Keys of the entries node `id` has applied, in order.
  std::vector<std::string> Keys(NodeId id) {
    std::lock_guard<std::mutex> lk(applied_mu);
    std::vector<std::string> keys;
    for (const LogEntry& e : applied[id - 1]) keys.push_back(e.command.key);
    return keys;
  }

  std::vector<LogEntry> Applied(NodeId id) {
    std::lock_guard<std::mutex> lk(applied_mu);
    return applied[id - 1];
  }

 private:
  void ClearApplied(NodeId id) {
    std::lock_guard<std::mutex> lk(applied_mu);
    applied[id - 1].clear();
  }

  std::unique_ptr<RaftNodeImpl> MakeNode(NodeId id) {
    std::vector<NodeId> peers;
    for (int p = 1; p <= size; ++p) {
      if (static_cast<NodeId>(p) != id) peers.push_back(static_cast<NodeId>(p));
    }
    RaftStorage* storage = nullptr;
    if (tmp) {  // reopening the same directory reloads what the previous incarnation saved
      std::string dir = (tmp->path / ("node" + std::to_string(id))).string();
      storages[id - 1] = std::make_unique<FileRaftStorage>(dir, /*sync=*/false);
      storage = storages[id - 1].get();
    }
    RaftConfig cfg;
    cfg.random_seed = 42;
    auto node = std::make_unique<RaftNodeImpl>(id, peers, transports[id - 1].get(), cfg, storage);
    node->SetApplyCallback([this, id](const LogEntry& e) {
      std::lock_guard<std::mutex> lk(applied_mu);
      applied[id - 1].push_back(e);
    });
    return node;
  }
};

// Keeps proposing `key` on whichever node is leader until every node has applied it.
inline bool ProposeUntilApplied(Cluster& c, const std::string& key,
                                std::chrono::milliseconds timeout) {
  using namespace std::chrono_literals;
  auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    NodeId leader = c.FindLeader();
    if (leader != 0 && c.Node(leader)->Propose(Put(key)).result == ProposeResult::kAccepted) {
      bool everywhere = WaitFor(
          [&] {
            for (auto& node : c.nodes) {
              if (!Contains(c.Keys(node->Id()), key)) return false;
            }
            return true;
          },
          1500ms);
      if (everywhere) return true;
    }
    std::this_thread::sleep_for(50ms);
  }
  return false;
}

}  // namespace testing_util
}  // namespace raftkv
