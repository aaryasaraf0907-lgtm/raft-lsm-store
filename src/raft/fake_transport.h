#pragma once
#include <atomic>
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
  void SendInstallSnapshot(NodeId to, const InstallSnapshotRequest& req,
                           std::function<void(const InstallSnapshotReply&)> on_reply) override;
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
// A node may be stopped and destroyed while the network keeps running (to simulate a crash and
// restart): unregistering a node waits until any delivery in progress has finished, and replies
// to a node that has since been stopped or replaced are dropped.
class FakeNetwork {
 public:
  FakeNetwork();
  ~FakeNetwork();

  std::unique_ptr<FakeTransport> MakeTransport(NodeId id);
  void Disconnect(NodeId id);
  void Reconnect(NodeId id);
  // Drop any AppendEntries or InstallSnapshot whose approximate size (message_size.h) exceeds `bytes`, like a real
  // transport with a message size limit (gRPC's default is 4 MB). 0 = no limit.
  void SetMaxMessageBytes(size_t bytes) { max_message_bytes_ = bytes; }
  // Size of the largest AppendEntries sent so far, and how many were dropped for being too big.
  size_t MaxObservedMessageBytes() const { return max_observed_bytes_; }
  size_t OversizedDropped() const { return oversized_dropped_; }
  void Shutdown();

 private:
  friend class FakeTransport;

  void Register(NodeId id, RpcHandler* handler);
  void SendRequestVote(NodeId from, NodeId to, const RequestVoteRequest& req,
                       std::function<void(const RequestVoteReply&)> on_reply);
  void SendAppendEntries(NodeId from, NodeId to, const AppendEntriesRequest& req,
                         std::function<void(const AppendEntriesReply&)> on_reply);

  void SendInstallSnapshot(NodeId from, NodeId to, const InstallSnapshotRequest& req,
                           std::function<void(const InstallSnapshotReply&)> on_reply);

  void NoteSize(size_t bytes);
  bool TooBig(size_t bytes);

  bool Reachable(NodeId a, NodeId b);
  RpcHandler* Lookup(NodeId from, NodeId to);
  uint64_t GenerationOf(NodeId id);
  bool SenderStillCurrent(NodeId id, uint64_t generation);
  void Post(std::function<void()> task);
  void WorkerLoop();

  std::mutex mu_;
  std::condition_variable cv_;
  std::condition_variable idle_cv_;  // signalled when the worker finishes a delivery
  std::deque<std::function<void()>> queue_;
  std::map<NodeId, uint64_t> generation_;  // bumped on every register/unregister of a node
  bool busy_ = false;                      // worker is running a delivery
  std::map<NodeId, RpcHandler*> handlers_;
  std::set<NodeId> disconnected_;
  bool stop_ = false;
  std::atomic<size_t> max_message_bytes_{0};
  std::atomic<size_t> max_observed_bytes_{0};
  std::atomic<size_t> oversized_dropped_{0};
  std::thread worker_;
};

}  // namespace raftkv
