#pragma once
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <map>
#include <mutex>
#include <random>
#include <thread>
#include <vector>
#include "raft/raft_interface.h"
#include "raft/raft_storage.h"
#include "raft/transport.h"

namespace raftkv {

struct RaftConfig {
  int election_timeout_min_ms = 150;
  int election_timeout_max_ms = 300;
  int heartbeat_interval_ms = 50;
  size_t max_entries_per_append = 64;  // cap on entries per AppendEntries message
  // Soft cap on the size of one AppendEntries message (see message_size.h). A message always
  // carries at least one entry, so a single entry larger than this is still sent alone. Keep it
  // well below the transport's message limit (gRPC's default is 4 MB).
  size_t max_bytes_per_append = 1 << 20;
  uint32_t random_seed = 1;            // seed is combined with the node id
  // Take a snapshot and drop the log prefix once this many entries have been applied since the
  // last one. 0 = never (also needs SetSnapshotCallbacks).
  size_t snapshot_threshold_entries = 0;
};

enum class Role { kFollower, kCandidate, kLeader };

// Raft node: leader election, log replication, commit and in-order apply, with optional
// persistence, snapshots and log compaction.
//
// Snapshots: the state machine hands Raft an opaque byte string through SetSnapshotCallbacks.
// Once `snapshot_threshold_entries` entries were applied since the last snapshot, the applier
// thread asks for one (between applies, so it matches exactly the entries applied so far), saves
// it and drops the log up to that index. A follower that is behind the dropped prefix is sent the
// snapshot in chunks (InstallSnapshot) instead of log entries, and restores its state machine
// from it. The snapshot MUST contain everything the state machine needs to continue from that
// index, including the client session table used for exactly-once (ClientSessionTable::Serialize).
//
// Persistence: pass a RaftStorage and the node reloads its term, vote and log from it on
// construction, and saves them BEFORE answering any RPC that changed them (a vote, an appended
// or truncated log entry) and before a leader replicates a new entry. With no storage the node
// is volatile (used by the in-memory tests). A storage failure is fatal (the node aborts):
// carrying on without being able to remember a vote or an entry would break Raft's guarantees.
// After a restart commit_index/last_applied start at 0 and are re-learned from the leader, so
// the apply callback sees the committed log again; the state machine must tolerate replays
// (Put/Delete of the same entries in the same order give the same result).
//
// Usage: construct, SetApplyCallback(...), then Start().
// Node ids must be >= 1 (0 is used internally to mean "nobody").
// Transport sends must not block (see transport.h): they are called with the node lock held.
class RaftNodeImpl : public RaftNode, public RpcHandler {
 public:
  // `storage` is not owned and must outlive the node. nullptr = no persistence.
  RaftNodeImpl(NodeId id, std::vector<NodeId> peers, Transport* transport,
               RaftConfig cfg = RaftConfig(), RaftStorage* storage = nullptr);
  ~RaftNodeImpl() override;

  // RaftNode
  void Start() override;
  void Stop() override;
  // Leader only. Appends to the log and starts replicating. Returns once the entry is in
  // the leader's log, NOT when it is committed; commit shows up as the apply callback.
  ProposeResponse Propose(const Command& cmd) override;
  CommitResponse ProposeAndWait(const Command& cmd, std::chrono::milliseconds timeout) override;
  // Called once per committed entry, in log order, from a dedicated thread (never with the
  // node lock held, so it may call back into the node). kNoop entries are not delivered.
  // Set it before Start(); committed entries wait until a callback is set.
  void SetApplyCallback(ApplyCallback cb) override;
  // `take` returns the state machine's state as of the last entry delivered to the apply callback
  // (it is called on the apply thread, so nothing is applied meanwhile). `restore` replaces the
  // state machine's state with a snapshot (also on the apply thread) and is called on startup if
  // a snapshot was saved, and when a leader sends one. Set both before Start().
  using SnapshotTaker = std::function<std::string()>;
  using SnapshotRestorer = std::function<void(const std::string&)>;
  void SetSnapshotCallbacks(SnapshotTaker take, SnapshotRestorer restore);
  bool IsLeader() const override;
  Term CurrentTerm() const override;

  Role GetRole() const;
  NodeId Id() const { return id_; }
  LogIndex CommitIndex() const;
  LogIndex LastApplied() const;
  LogIndex SnapshotIndex() const;  // last log index covered by the snapshot (0 = none)
  size_t LogLength() const;        // log entries still stored (after the snapshot)

  // RpcHandler
  RequestVoteReply HandleRequestVote(const RequestVoteRequest& req) override;
  AppendEntriesReply HandleAppendEntries(const AppendEntriesRequest& req) override;
  InstallSnapshotReply HandleInstallSnapshot(const InstallSnapshotRequest& req) override;

 private:
  using Clock = std::chrono::steady_clock;

  void TickerLoop();
  void ApplierLoop();
  void StartElectionLocked();
  void BecomeFollowerLocked(Term term, bool persist = true);
  void BecomeLeaderLocked();
  void BroadcastAppendEntriesLocked();
  void SendAppendEntriesToLocked(NodeId peer);
  void SendSnapshotChunkLocked(NodeId peer);
  void OnInstallSnapshotReply(NodeId peer, Term sent_term, LogIndex snapshot_index,
                              const InstallSnapshotReply& reply);
  void InstallSnapshotLocked(LogIndex index, Term term, std::string data);
  void CompactLocked(LogIndex index, std::string data);
  LogEntry& EntryLocked(LogIndex index) { return log_[index - base_index_]; }
  const LogEntry& EntryLocked(LogIndex index) const { return log_[index - base_index_]; }
  void AdvanceCommitIndexLocked();
  LogIndex AppendAsLeaderLocked(const Command& cmd);
  void ResetElectionDeadlineLocked();
  void OnRequestVoteReply(Term election_term, const RequestVoteReply& reply);
  void OnAppendEntriesReply(NodeId peer, Term sent_term, LogIndex prev_index, size_t n_entries,
                            const AppendEntriesReply& reply);
  LogIndex LastLogIndexLocked() const;
  Term LastLogTermLocked() const;

  // Persistence helpers: no-ops without storage, abort if the storage fails.
  void PersistHardStateLocked();
  void PersistAppendLocked(const std::vector<LogEntry>& entries);
  void PersistTruncateLocked(LogIndex from_index);

  const NodeId id_;
  const std::vector<NodeId> peers_;
  Transport* transport_;
  const RaftConfig cfg_;
  RaftStorage* storage_;
  std::mt19937 rng_;

  mutable std::mutex mu_;
  std::condition_variable cv_;        // wakes the ticker on Stop
  std::condition_variable apply_cv_;  // wakes the applier on commit / callback / Stop
  std::condition_variable wait_cv_;   // wakes ProposeAndWait callers: applied / overwritten / Stop
  std::thread ticker_;
  std::thread applier_;
  std::atomic<bool> running_{false};

  // Raft state (guarded by mu_)
  Role role_ = Role::kFollower;
  Term current_term_ = 0;
  NodeId voted_for_ = 0;
  NodeId leader_hint_ = 0;
  int votes_ = 0;
  // log_[0] stands for the last entry covered by the snapshot (index base_index_, term
  // base_term_; both 0 when there is no snapshot), so the entry with index i is
  // log_[i - base_index_] and real entries start at base_index_ + 1.
  std::vector<LogEntry> log_;
  LogIndex base_index_ = 0;
  Term base_term_ = 0;
  std::string snapshot_data_;  // the snapshot at base_index_, kept to send to slow followers
  SnapshotTaker snap_taker_;
  SnapshotRestorer snap_restorer_;
  bool restore_pending_ = false;  // the state machine must be restored from snapshot_data_
  struct IncomingSnapshot {       // follower: a snapshot being received chunk by chunk
    bool active = false;
    LogIndex index = 0;
    Term term = 0;
    std::string data;
  } incoming_;
  LogIndex commit_index_ = 0;
  LogIndex last_applied_ = 0;
  ApplyCallback apply_cb_;

  // Leader-only state, re-initialised on every election win.
  std::map<NodeId, LogIndex> next_index_;   // next entry to send to each follower
  std::map<NodeId, LogIndex> match_index_;  // highest entry known replicated on each follower
  std::map<NodeId, uint64_t> snap_offset_;  // next snapshot byte to send to each follower

  Clock::time_point election_deadline_;
  Clock::time_point next_heartbeat_;
};

}  // namespace raftkv
