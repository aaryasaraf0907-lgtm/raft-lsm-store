#pragma once
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
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
  // After a snapshot at index A, keep the log entries from A - snapshot_trailing_entries + 1
  // onwards instead of dropping everything up to A. A follower that is behind by no more than
  // that is then caught up with plain log entries instead of a whole snapshot. 0 = drop them all.
  size_t snapshot_trailing_entries = 0;
  // Run a PreVote round before every real election (and only if the transport supports it, see
  // Transport::SupportsPreVote). A node that is cut off then cannot inflate its term while it is
  // away and force a healthy leader to step down when it returns.
  bool enable_pre_vote = true;
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
  // Linearizable read barrier without a log write (see RaftNode::ReadIndex). Leader only: fixes
  // the read index at its commit index (once an entry of its own term has committed), confirms it
  // is still leader by a heartbeat round that started AFTER that point acknowledged by a
  // majority, then waits until the state machine has applied up to the read index.
  CommitResponse ReadIndex(std::chrono::milliseconds timeout) override;
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
  NodeId LeaderHint() const;  // the leader this node last heard from (0 = unknown)
  LogIndex CommitIndex() const;
  LogIndex LastApplied() const;
  LogIndex SnapshotIndex() const;  // last log index covered by the snapshot (0 = none)
  size_t LogLength() const;
  // Replication messages (AppendEntries and InstallSnapshot) this node has sent as leader. For tests.
  uint64_t ReplicationMessagesSent() const;        // log entries still stored (after the snapshot)

  // RpcHandler
  RequestVoteReply HandleRequestVote(const RequestVoteRequest& req) override;
  AppendEntriesReply HandleAppendEntries(const AppendEntriesRequest& req) override;
  // Answers "would you vote for me in a real election at req.term?" WITHOUT changing any state:
  // no term update, no recorded vote, no timer reset, nothing persisted.
  PreVoteReply HandlePreVote(const PreVoteRequest& req) override;
  InstallSnapshotReply HandleInstallSnapshot(const InstallSnapshotRequest& req) override;

 private:
  using Clock = std::chrono::steady_clock;

  void TickerLoop();
  void ApplierLoop();
  void StartElectionLocked();
  void StartElectionOrPreVoteLocked();
  void StartPreVoteLocked();
  void OnPreVoteReply(uint64_t round, const PreVoteReply& reply);
  void NoteLeaderContactLocked();
  void BecomeFollowerLocked(Term term, bool persist = true);
  void BecomeLeaderLocked();
  void BroadcastAppendEntriesLocked();
  void SendAppendEntriesToLocked(NodeId peer);
  void SendSnapshotChunkLocked(NodeId peer);
  void OnInstallSnapshotReply(NodeId peer, Term sent_term, uint64_t seq, LogIndex snapshot_index,
                              const InstallSnapshotReply& reply);
  void InstallSnapshotLocked(LogIndex index, Term term, std::string data);
  void CompactLocked(LogIndex index, std::string data);
  LogEntry& EntryLocked(LogIndex index) { return log_[index - base_index_]; }
  const LogEntry& EntryLocked(LogIndex index) const { return log_[index - base_index_]; }
  void AdvanceCommitIndexLocked();
  // ---- group commit for proposals ----
  // Concurrent proposers queue their commands; one of them (the "batcher") appends everything that
  // is queued with ONE log write and ONE fsync, then wakes the rest. While one batch is being
  // fsynced the next one accumulates, so batches grow with the load. Every storage write still
  // happens under mu_, exactly as before: only the number of fsyncs per entry changes.
  struct PendingProposal {
    Command cmd;
    bool done = false;      // the batcher has handled it (guarded by proposal_mu_)
    bool accepted = false;  // appended to the log as leader
    bool stopped = false;   // the node was shut down
    LogIndex index = 0;     // valid if accepted
    Term term = 0;          // the term the entry was appended in (valid if accepted)
    NodeId leader_hint = 0;
  };
  void SubmitProposal(PendingProposal* p);
  static constexpr size_t kMaxProposalBatch = 256;
  std::mutex proposal_mu_;
  std::condition_variable proposal_cv_;
  std::deque<PendingProposal*> proposals_;
  bool batcher_active_ = false;
  // Group-commit delay. Proposers that just finished a batch are about to propose again; if the next
  // batcher wrote immediately it would catch only the stragglers and the group would fall apart into
  // tiny batches (badly so when threads wake slowly). So a batcher waits up to kBatchWait for as many
  // proposals to queue as there are proposers currently inside Propose/ProposeAndWait (queued, or
  // waiting for their commit). Self-correcting: if batches shrink, more proposers pile up inside, so
  // the wait grows. A lone client has one proposer inside, so it never waits.
  std::condition_variable batch_cv_;
  std::atomic<size_t> inflight_proposers_{0};
  static constexpr std::chrono::microseconds kBatchWait{2000};
  void ResetElectionDeadlineLocked();
  void OnRequestVoteReply(Term election_term, const RequestVoteReply& reply);
  void OnAppendEntriesReply(NodeId peer, Term sent_term, uint64_t seq, LogIndex prev_index,
                            size_t n_entries, const AppendEntriesReply& reply);
  void RecordAckLocked(NodeId peer, uint64_t seq);
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
  // log_[0] stands for the last entry that has been dropped from the log (index base_index_, term
  // base_term_; both 0 if nothing was dropped), so the entry with index i is
  // log_[i - base_index_] and real entries start at base_index_ + 1.
  std::vector<LogEntry> log_;
  LogIndex base_index_ = 0;
  Term base_term_ = 0;
  // The snapshot covers everything up to snapshot_index_ >= base_index_ (the log may keep a few
  // entries below it, see snapshot_trailing_entries). 0 = no snapshot.
  LogIndex snapshot_index_ = 0;
  Term snapshot_term_ = 0;
  std::string snapshot_data_;  // the snapshot at snapshot_index_, kept to send to slow followers
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
  // ReadIndex bookkeeping. Every message sent to a follower gets a number from send_seq_; when a
  // reply in our term comes back, the follower has accepted us as leader at that moment, and
  // acked_seq_[peer] records the highest such number. A read confirms leadership once a majority
  // acked a number above the send_seq_ value taken when its read index was fixed.
  uint64_t send_seq_ = 0;
  std::map<NodeId, uint64_t> acked_seq_;
  // Read batching. A read still needs a majority to acknowledge a message sent AFTER its read index
  // was fixed; what changes is how many rounds are sent. Every broadcast is a "round" (proposal
  // batches and heartbeats too). At most one round is in flight for reads: a read that arrives while
  // one is in flight cannot use it (its messages were sent before the read's index was fixed), so it
  // marks read_round_wanted_, and ONE new round for all such reads starts when the in-flight round is
  // acknowledged by a majority (or with the next heartbeat, if it never is).
  uint64_t read_round_mark_ = 0;     // send_seq_ just before the latest round was broadcast
  bool read_round_in_flight_ = false;
  bool read_round_wanted_ = false;
  bool MajorityAckedAfterLocked(uint64_t mark) const;  // a majority acknowledged a message numbered > mark
  LogIndex term_start_index_ = 0;  // index of the no-op this leader appended when elected

  // PreVote state (guarded by mu_). A round is identified by prevote_round_ so late replies from
  // an earlier round are ignored.
  bool prevote_active_ = false;
  uint64_t prevote_round_ = 0;
  int prevotes_ = 0;
  // When we last heard from a valid leader. A node that heard one within the minimum election
  // timeout refuses to pre-vote: the leader is alive, so nobody should be starting an election.
  bool have_leader_contact_ = false;
  Clock::time_point last_leader_contact_;

  Clock::time_point election_deadline_;
  Clock::time_point next_heartbeat_;
};

}  // namespace raftkv
