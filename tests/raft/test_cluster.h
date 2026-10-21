#pragma once
// Shared helpers for the Raft tests: a small cluster on a FakeNetwork.
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "raft/fake_transport.h"
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

// n nodes (ids 1..n) on one FakeNetwork. Every committed entry a node applies is recorded.
// Member order matters: recordings and the network outlive the transports and nodes.
struct Cluster {
  std::mutex applied_mu;
  std::vector<std::vector<LogEntry>> applied;  // applied[id-1] = entries in apply order
  FakeNetwork net;
  std::vector<std::unique_ptr<FakeTransport>> transports;
  std::vector<std::unique_ptr<RaftNodeImpl>> nodes;

  explicit Cluster(int n) : applied(static_cast<size_t>(n)) {
    std::vector<NodeId> ids;
    for (int i = 1; i <= n; ++i) ids.push_back(static_cast<NodeId>(i));
    for (NodeId id : ids) {
      std::vector<NodeId> peers;
      for (NodeId p : ids) {
        if (p != id) peers.push_back(p);
      }
      transports.push_back(net.MakeTransport(id));
      RaftConfig cfg;
      cfg.random_seed = 42;
      nodes.push_back(std::make_unique<RaftNodeImpl>(id, peers, transports.back().get(), cfg));
      nodes.back()->SetApplyCallback([this, id](const LogEntry& e) {
        std::lock_guard<std::mutex> lk(applied_mu);
        applied[id - 1].push_back(e);
      });
    }
    for (auto& node : nodes) node->Start();
  }

  ~Cluster() {
    for (auto& node : nodes) node->Stop();
    net.Shutdown();
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
};

}  // namespace testing_util
}  // namespace raftkv
