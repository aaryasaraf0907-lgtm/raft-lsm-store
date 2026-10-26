#pragma once
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <random>
#include <set>
#include <thread>
#include <utility>
#include <vector>
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
  bool SupportsPreVote() const override { return true; }
  void SendPreVote(NodeId to, const PreVoteRequest& req,
                   std::function<void(const PreVoteReply&)> on_reply) override;
  void SetHandler(RpcHandler* handler) override;
  void Start() override {}
  void Stop() override;

 private:
  NodeId id_;
  FakeNetwork* net_;
};

// Faults the network can inject into every message (see FakeNetwork::SetFaults). All default to
// off; probabilities are in [0, 1].
struct FaultPlan {
  double drop_request = 0;  // the request never arrives
  double drop_reply = 0;    // the request is handled but the reply is lost
  double duplicate = 0;     // the request is delivered twice (the sender still gets ONE reply)
  // Every request and every reply is delayed by a random time in [min, max] ms. Different delays
  // make messages overtake each other, so this also reorders.
  int min_delay_ms = 0;
  int max_delay_ms = 0;
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

  // One-way link failure: messages sent from `from` to `to` are lost, while `to` can still reach
  // `from`. Replies travel the opposite way, so a one-way cut also kills replies in one direction.
  void BlockLink(NodeId from, NodeId to);
  void UnblockLink(NodeId from, NodeId to);
  // Random message faults. The random choices come from a generator seeded by SetFaultSeed, so a
  // run with the same seed makes the same decisions in the same order (the interleaving of threads
  // can still differ).
  void SetFaults(const FaultPlan& plan);
  void SetFaultSeed(uint64_t seed);
  // Reconnects everything, unblocks every link and turns all faults off.
  void HealAll();
  size_t DroppedByFaults() const { return fault_dropped_; }
  size_t DuplicatedByFaults() const { return fault_duplicated_; }
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
  void SendPreVote(NodeId from, NodeId to, const PreVoteRequest& req,
                   std::function<void(const PreVoteReply&)> on_reply);

  void NoteSize(size_t bytes);
  bool TooBig(size_t bytes);

  bool Reachable(NodeId a, NodeId b);
  RpcHandler* Lookup(NodeId from, NodeId to);
  uint64_t GenerationOf(NodeId id);
  bool SenderStillCurrent(NodeId id, uint64_t generation);
  struct Decision {  // what the fault plan says to do with one message
    bool drop = false;
    bool duplicate = false;
    int delay_ms = 0;
    int duplicate_delay_ms = 0;
  };
  Decision Decide(bool is_request);
  // Sends one request/reply exchange through the fault plan. `handle` runs the handler.
  template <typename Req, typename Reply, typename Handle>
  void SendRpc(NodeId from, NodeId to, const Req& req,
               std::function<void(const Reply&)> on_reply, Handle handle);
  void Post(std::function<void()> task) { PostAfter(0, std::move(task)); }
  void PostAfter(int delay_ms, std::function<void()> task);
  void WorkerLoop();

  std::mutex mu_;
  std::condition_variable cv_;
  std::condition_variable idle_cv_;  // signalled when the worker finishes a delivery
  using Clock = std::chrono::steady_clock;
  struct Item {
    Clock::time_point due;
    uint64_t seq;  // keeps messages that are due at the same moment in the order they were sent
    std::function<void()> task;
  };
  struct Later {
    bool operator()(const Item& a, const Item& b) const {
      return a.due != b.due ? a.due > b.due : a.seq > b.seq;
    }
  };
  std::priority_queue<Item, std::vector<Item>, Later> queue_;
  uint64_t next_seq_ = 0;
  std::set<std::pair<NodeId, NodeId>> blocked_links_;
  FaultPlan faults_;
  std::mt19937_64 fault_rng_{1};
  std::atomic<size_t> fault_dropped_{0};
  std::atomic<size_t> fault_duplicated_{0};
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
