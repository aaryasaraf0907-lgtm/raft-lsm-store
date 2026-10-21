#pragma once
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <random>
#include <thread>
#include <vector>
#include "raft/raft_interface.h"
#include "raft/transport.h"

namespace raftkv {

struct RaftConfig {
  int election_timeout_min_ms = 150;
  int election_timeout_max_ms = 300;
  int heartbeat_interval_ms = 50;
  uint32_t random_seed = 1;  // seed is combined with the node id
};

enum class Role { kFollower, kCandidate, kLeader };

// Raft node. This version implements leader election and heartbeats.
// Log replication, commit advancement and persistence come in the next PRs.
//
// Node ids must be >= 1 (0 is used internally to mean "nobody").
// Transport sends must not block (see transport.h): they are called with the node lock held.
class RaftNodeImpl : public RaftNode, public RpcHandler {
 public:
  RaftNodeImpl(NodeId id, std::vector<NodeId> peers, Transport* transport,
               RaftConfig cfg = RaftConfig());
  ~RaftNodeImpl() override;

  // RaftNode
  void Start() override;
  void Stop() override;
  ProposeResponse Propose(const Command& cmd) override;
  void SetApplyCallback(ApplyCallback cb) override;
  bool IsLeader() const override;
  Term CurrentTerm() const override;

  Role GetRole() const;
  NodeId Id() const { return id_; }

  // RpcHandler
  RequestVoteReply HandleRequestVote(const RequestVoteRequest& req) override;
  AppendEntriesReply HandleAppendEntries(const AppendEntriesRequest& req) override;

 private:
  using Clock = std::chrono::steady_clock;

  void TickerLoop();
  void StartElectionLocked();
  void BecomeFollowerLocked(Term term);
  void BecomeLeaderLocked();
  void SendHeartbeatsLocked();
  void ResetElectionDeadlineLocked();
  void OnRequestVoteReply(Term election_term, const RequestVoteReply& reply);
  void OnAppendEntriesReply(const AppendEntriesReply& reply);
  LogIndex LastLogIndexLocked() const;
  Term LastLogTermLocked() const;

  const NodeId id_;
  const std::vector<NodeId> peers_;
  Transport* transport_;
  const RaftConfig cfg_;
  std::mt19937 rng_;

  mutable std::mutex mu_;
  std::condition_variable cv_;
  std::thread ticker_;
  std::atomic<bool> running_{false};

  // Raft state (guarded by mu_)
  Role role_ = Role::kFollower;
  Term current_term_ = 0;
  NodeId voted_for_ = 0;
  NodeId leader_hint_ = 0;
  int votes_ = 0;
  std::vector<LogEntry> log_;  // log_[0] is a dummy entry so real indexes start at 1
  LogIndex commit_index_ = 0;
  ApplyCallback apply_cb_;

  Clock::time_point election_deadline_;
  Clock::time_point next_heartbeat_;
};

}  // namespace raftkv
