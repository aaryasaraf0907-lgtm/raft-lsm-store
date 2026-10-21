#pragma once
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <map>
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
  size_t max_entries_per_append = 64;  // cap on entries per AppendEntries message
  uint32_t random_seed = 1;            // seed is combined with the node id
};

enum class Role { kFollower, kCandidate, kLeader };

// Raft node: leader election, log replication, commit and in-order apply.
// Persistence (surviving restarts) and snapshots come in later PRs.
//
// Usage: construct, SetApplyCallback(...), then Start().
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
  // Leader only. Appends to the log and starts replicating. Returns once the entry is in
  // the leader's log, NOT when it is committed; commit shows up as the apply callback.
  ProposeResponse Propose(const Command& cmd) override;
  // Called once per committed entry, in log order, from a dedicated thread (never with the
  // node lock held, so it may call back into the node). kNoop entries are not delivered.
  // Set it before Start(); committed entries wait until a callback is set.
  void SetApplyCallback(ApplyCallback cb) override;
  bool IsLeader() const override;
  Term CurrentTerm() const override;

  Role GetRole() const;
  NodeId Id() const { return id_; }
  LogIndex CommitIndex() const;
  LogIndex LastApplied() const;

  // RpcHandler
  RequestVoteReply HandleRequestVote(const RequestVoteRequest& req) override;
  AppendEntriesReply HandleAppendEntries(const AppendEntriesRequest& req) override;

 private:
  using Clock = std::chrono::steady_clock;

  void TickerLoop();
  void ApplierLoop();
  void StartElectionLocked();
  void BecomeFollowerLocked(Term term);
  void BecomeLeaderLocked();
  void BroadcastAppendEntriesLocked();
  void SendAppendEntriesToLocked(NodeId peer);
  void AdvanceCommitIndexLocked();
  void ResetElectionDeadlineLocked();
  void OnRequestVoteReply(Term election_term, const RequestVoteReply& reply);
  void OnAppendEntriesReply(NodeId peer, Term sent_term, LogIndex prev_index, size_t n_entries,
                            const AppendEntriesReply& reply);
  LogIndex LastLogIndexLocked() const;
  Term LastLogTermLocked() const;

  const NodeId id_;
  const std::vector<NodeId> peers_;
  Transport* transport_;
  const RaftConfig cfg_;
  std::mt19937 rng_;

  mutable std::mutex mu_;
  std::condition_variable cv_;        // wakes the ticker on Stop
  std::condition_variable apply_cv_;  // wakes the applier on commit / callback / Stop
  std::thread ticker_;
  std::thread applier_;
  std::atomic<bool> running_{false};

  // Raft state (guarded by mu_)
  Role role_ = Role::kFollower;
  Term current_term_ = 0;
  NodeId voted_for_ = 0;
  NodeId leader_hint_ = 0;
  int votes_ = 0;
  std::vector<LogEntry> log_;  // log_[0] is a dummy entry so real indexes start at 1
  LogIndex commit_index_ = 0;
  LogIndex last_applied_ = 0;
  ApplyCallback apply_cb_;

  // Leader-only state, re-initialised on every election win.
  std::map<NodeId, LogIndex> next_index_;   // next entry to send to each follower
  std::map<NodeId, LogIndex> match_index_;  // highest entry known replicated on each follower

  Clock::time_point election_deadline_;
  Clock::time_point next_heartbeat_;
};

}  // namespace raftkv
