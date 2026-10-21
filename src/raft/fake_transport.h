#pragma once
#include <condition_variable>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <thread>
#include "raft/transport.h"

namespace raftkv {

class FakeNetwork;

// One node's endpoint on the fake network. Create it with FakeNetwork::MakeTransport.
class FakeTransport : public Transport {
 public:
  FakeTransport(NodeId id, FakeNetwork* net) : id_(id), net_(net) {}

  void SendRequestVote(NodeId to, const RequestVoteRequest& req,
                       std::function<void(const RequestVoteReply&)> on_reply) override;
  void SendAppendEntries(NodeId to, const AppendEntriesRequest& req,
                         std::function<void(const AppendEntriesReply&)> on_reply) override;
  void SetHandler(RpcHandler* handler) override;
  void Start() override {}
  void Stop() override;

 private:
  NodeId id_;
  FakeNetwork* net_;
};

// In-process "network" for testing Raft without sockets.
// A single worker thread delivers all messages, so tests are repeatable.
// Disconnect(id) drops every message to or from that node (a crash or partition).
//
// Shutdown order in tests: stop all nodes, then Shutdown() the network,
// then destroy the nodes. (Cluster in the tests does exactly this.)
class FakeNetwork {
 public:
  FakeNetwork();
  ~FakeNetwork();

  std::unique_ptr<FakeTransport> MakeTransport(NodeId id);
  void Disconnect(NodeId id);
  void Reconnect(NodeId id);
  void Shutdown();

 private:
  friend class FakeTransport;

  void Register(NodeId id, RpcHandler* handler);
  void SendRequestVote(NodeId from, NodeId to, const RequestVoteRequest& req,
                       std::function<void(const RequestVoteReply&)> on_reply);
  void SendAppendEntries(NodeId from, NodeId to, const AppendEntriesRequest& req,
                         std::function<void(const AppendEntriesReply&)> on_reply);

  bool Reachable(NodeId a, NodeId b);
  RpcHandler* Lookup(NodeId from, NodeId to);
  void Post(std::function<void()> task);
  void WorkerLoop();

  std::mutex mu_;
  std::condition_variable cv_;
  std::deque<std::function<void()>> queue_;
  std::map<NodeId, RpcHandler*> handlers_;
  std::set<NodeId> disconnected_;
  bool stop_ = false;
  std::thread worker_;
};

}  // namespace raftkv
