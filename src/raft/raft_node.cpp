#include "raft/raft_node.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <utility>

#include "raft/message_size.h"

namespace raftkv {

namespace {
// A node that cannot save its state must stop: answering RPCs without having remembered a
// vote or an entry could make it contradict itself after a restart.
void MustSucceed(bool ok, const char* what) {
  if (ok) return;
  std::fprintf(stderr, "raft: fatal storage error: %s\n", what);
  std::abort();
}
}  // namespace

RaftNodeImpl::RaftNodeImpl(NodeId id, std::vector<NodeId> peers, Transport* transport,
                           RaftConfig cfg, RaftStorage* storage)
    : id_(id),
      peers_(std::move(peers)),
      transport_(transport),
      cfg_(cfg),
      storage_(storage),
      rng_(cfg.random_seed + id) {
  log_.push_back(LogEntry{0, 0, Command{CommandType::kNoop, "", ""}});  // dummy entry at index 0
  if (storage_ != nullptr) {  // recover what we saved before the last crash
    PersistentState saved;
    MustSucceed(storage_->Load(&saved), "load persistent state");
    current_term_ = saved.term;
    voted_for_ = saved.voted_for;
    base_index_ = saved.log_base_index;
    base_term_ = saved.log_base_term;
    snapshot_index_ = saved.snapshot_index;
    snapshot_term_ = saved.snapshot_term;
    snapshot_data_ = std::move(saved.snapshot_data);
    log_[0] = LogEntry{base_term_, base_index_, Command{CommandType::kNoop, "", ""}};
    for (LogEntry& entry : saved.log) {
      MustSucceed(entry.index == log_.back().index + 1, "stored log is not contiguous");
      log_.push_back(std::move(entry));
    }
    if (snapshot_index_ > 0) {  // a snapshot only ever holds committed state
      commit_index_ = snapshot_index_;
      restore_pending_ = true;  // the applier restores the state machine before applying more
    }
  }
}

RaftNodeImpl::~RaftNodeImpl() { Stop(); }

void RaftNodeImpl::Start() {
  if (running_.exchange(true)) return;
  MustSucceed(!restore_pending_ || snap_restorer_ != nullptr,
              "a snapshot is stored but no snapshot restore callback was set");
  transport_->SetHandler(this);
  transport_->Start();
  {
    std::lock_guard<std::mutex> lk(mu_);
    ResetElectionDeadlineLocked();
  }
  ticker_ = std::thread(&RaftNodeImpl::TickerLoop, this);
  applier_ = std::thread(&RaftNodeImpl::ApplierLoop, this);
}

void RaftNodeImpl::Stop() {
  if (!running_.exchange(false)) return;
  {
    std::lock_guard<std::mutex> lk(mu_);  // makes sure no waiter misses the wake-up below
  }
  cv_.notify_all();
  apply_cv_.notify_all();
  wait_cv_.notify_all();  // release anyone blocked in ProposeAndWait
  if (ticker_.joinable()) ticker_.join();
  if (applier_.joinable()) applier_.join();
  transport_->Stop();
}

// Appends every queued proposal with one log write and one fsync (see the note in the header).
// The thread that finds no batcher running becomes the batcher; the others wait until their
// proposal has been handled. A proposal is handled exactly once: appended (accepted), refused
// because this node is not the leader, or refused because the node stopped.
void RaftNodeImpl::SubmitProposal(PendingProposal* p) {
  std::unique_lock<std::mutex> ql(proposal_mu_);
  proposals_.push_back(p);
  batch_cv_.notify_one();  // a batcher that is waiting for more proposals may now have enough
  while (!p->done) {
    if (batcher_active_) {
      proposal_cv_.wait(ql);
      continue;
    }
    batcher_active_ = true;
    const size_t want = std::min(inflight_proposers_.load(), kMaxProposalBatch);
    if (want > 1 && proposals_.size() < want) {
      batch_cv_.wait_for(ql, kBatchWait, [&] { return proposals_.size() >= want; });
    }
    std::vector<PendingProposal*> batch;
    while (!proposals_.empty() && batch.size() < kMaxProposalBatch) {
      batch.push_back(proposals_.front());
      proposals_.pop_front();
    }
    ql.unlock();  // proposers keep queueing while this batch is being written
    {
      std::lock_guard<std::mutex> lk(mu_);
      if (!running_) {
        for (PendingProposal* x : batch) {
          x->stopped = true;
          x->leader_hint = leader_hint_;
        }
      } else if (role_ != Role::kLeader) {
        for (PendingProposal* x : batch) x->leader_hint = leader_hint_;
      } else {
        std::vector<LogEntry> entries;
        entries.reserve(batch.size());
        LogIndex index = LastLogIndexLocked();
        for (PendingProposal* x : batch) {
          ++index;
          log_.push_back(LogEntry{current_term_, index, x->cmd});
          entries.push_back(log_.back());
          x->accepted = true;
          x->index = index;
          x->term = current_term_;
          x->leader_hint = id_;
        }
        PersistAppendLocked(entries);  // durable on the leader before we replicate or count it
        AdvanceCommitIndexLocked();    // a single-node cluster commits immediately
        BroadcastAppendEntriesLocked();  // replicate now instead of waiting for the next heartbeat
      }
    }
    ql.lock();
    for (PendingProposal* x : batch) x->done = true;
    batcher_active_ = false;
    proposal_cv_.notify_all();
  }
}

namespace {
struct InflightGuard {  // counts a proposer for as long as it is inside Propose/ProposeAndWait
  std::atomic<size_t>& n;
  explicit InflightGuard(std::atomic<size_t>& a) : n(a) { ++n; }
  ~InflightGuard() { --n; }
};
}  // namespace

ProposeResponse RaftNodeImpl::Propose(const Command& cmd) {
  InflightGuard inflight(inflight_proposers_);
  PendingProposal p;
  p.cmd = cmd;
  SubmitProposal(&p);
  if (!p.accepted) return {ProposeResult::kNotLeader, 0, p.leader_hint};
  return {ProposeResult::kAccepted, p.index, id_};
}

CommitResponse RaftNodeImpl::ProposeAndWait(const Command& cmd, std::chrono::milliseconds timeout) {
  InflightGuard inflight(inflight_proposers_);
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  PendingProposal p;
  p.cmd = cmd;
  SubmitProposal(&p);
  if (p.stopped) return {CommitResult::kStopped, 0, p.leader_hint};
  if (!p.accepted) return {CommitResult::kNotLeader, 0, p.leader_hint};

  const Term term = p.term;  // the entry is identified by (index, term)
  const LogIndex index = p.index;
  std::unique_lock<std::mutex> lk(mu_);
  CommitResult outcome = CommitResult::kTimeout;
  auto resolved = [&] {
    if (!running_) {
      outcome = CommitResult::kStopped;
      return true;
    }
    if (index <= base_index_) {
      // Compacted away while we waited, so it is committed and applied. We can only vouch that
      // it is OUR entry if we stayed leader in `term` (a leader never has its entries replaced);
      // otherwise the outcome is unknown and the client must retry with the same request id.
      outcome = (role_ == Role::kLeader && current_term_ == term) ? CommitResult::kApplied
                                                                  : CommitResult::kTimeout;
      return true;
    }
    // If a different leader replaced our entry at this index, ours can never commit.
    if (LastLogIndexLocked() < index || EntryLocked(index).term != term) {
      outcome = CommitResult::kLost;
      return true;
    }
    // Applied means the callback has returned for it, so the state machine already reflects it.
    if (last_applied_ >= index) {
      outcome = CommitResult::kApplied;
      return true;
    }
    return false;
  };
  wait_cv_.wait_until(lk, deadline, resolved);  // on timeout `outcome` stays kTimeout
  return {outcome, index, leader_hint_};
}

void RaftNodeImpl::SetApplyCallback(ApplyCallback cb) {
  {
    std::lock_guard<std::mutex> lk(mu_);
    apply_cb_ = std::move(cb);
  }
  apply_cv_.notify_all();
}

void RaftNodeImpl::RecordAckLocked(NodeId peer, uint64_t seq) {
  uint64_t& acked = acked_seq_[peer];
  if (seq > acked) {
    acked = seq;
    wait_cv_.notify_all();  // a ReadIndex waiter may now have its majority
    if (role_ == Role::kLeader && read_round_in_flight_ && MajorityAckedAfterLocked(read_round_mark_)) {
      read_round_in_flight_ = false;
      if (read_round_wanted_) BroadcastAppendEntriesLocked();  // one round for every read that waited
    }
  }
}

bool RaftNodeImpl::MajorityAckedAfterLocked(uint64_t mark) const {
  size_t acks = 1;  // ourselves
  for (NodeId peer : peers_) {
    auto it = acked_seq_.find(peer);
    if (it != acked_seq_.end() && it->second > mark) ++acks;
  }
  return acks > (peers_.size() + 1) / 2;
}

uint64_t RaftNodeImpl::ReplicationMessagesSent() const {
  std::lock_guard<std::mutex> lk(mu_);
  return send_seq_;
}

CommitResponse RaftNodeImpl::ReadIndex(std::chrono::milliseconds timeout) {
  std::unique_lock<std::mutex> lk(mu_);
  if (!running_) return {CommitResult::kStopped, 0, leader_hint_};
  if (role_ != Role::kLeader) return {CommitResult::kNotLeader, 0, leader_hint_};

  const Term term = current_term_;
  bool have_index = false;
  LogIndex read_index = 0;
  uint64_t seq_mark = 0;
  CommitResult outcome = CommitResult::kTimeout;
  auto resolved = [&] {
    if (!running_) {
      outcome = CommitResult::kStopped;
      return true;
    }
    if (role_ != Role::kLeader || current_term_ != term) {
      outcome = CommitResult::kNotLeader;
      return true;
    }
    if (!have_index) {
      // Until an entry of OUR term has committed, commit_index_ may lag behind entries that an
      // earlier leader already committed, so a read at it could be stale.
      if (commit_index_ < term_start_index_) return false;
      read_index = commit_index_;
      seq_mark = send_seq_;
      have_index = true;
      // We need a round that starts after seq_mark. If none is in flight, start one now; otherwise
      // the in-flight round is too old for us, so ask for the next one, which every read that
      // arrives meanwhile will share.
      if (!read_round_in_flight_) {
        BroadcastAppendEntriesLocked();
      } else {
        read_round_wanted_ = true;
      }
    }
    if (!MajorityAckedAfterLocked(seq_mark)) return false;  // not yet known to still be the leader
    if (last_applied_ < read_index) return false;  // state machine has not caught up
    outcome = CommitResult::kApplied;
    return true;
  };
  wait_cv_.wait_for(lk, timeout, resolved);  // on timeout `outcome` stays kTimeout
  return {outcome, read_index, leader_hint_};
}

void RaftNodeImpl::SetSnapshotCallbacks(SnapshotTaker take, SnapshotRestorer restore) {
  {
    std::lock_guard<std::mutex> lk(mu_);
    snap_taker_ = std::move(take);
    snap_restorer_ = std::move(restore);
  }
  apply_cv_.notify_all();
}

NodeId RaftNodeImpl::LeaderHint() const {
  std::lock_guard<std::mutex> lk(mu_);
  return role_ == Role::kLeader ? id_ : leader_hint_;
}

LogIndex RaftNodeImpl::SnapshotIndex() const {
  std::lock_guard<std::mutex> lk(mu_);
  return snapshot_index_;
}

size_t RaftNodeImpl::LogLength() const {
  std::lock_guard<std::mutex> lk(mu_);
  return log_.size() - 1;
}

bool RaftNodeImpl::IsLeader() const {
  std::lock_guard<std::mutex> lk(mu_);
  return role_ == Role::kLeader;
}

Term RaftNodeImpl::CurrentTerm() const {
  std::lock_guard<std::mutex> lk(mu_);
  return current_term_;
}

Role RaftNodeImpl::GetRole() const {
  std::lock_guard<std::mutex> lk(mu_);
  return role_;
}

LogIndex RaftNodeImpl::CommitIndex() const {
  std::lock_guard<std::mutex> lk(mu_);
  return commit_index_;
}

LogIndex RaftNodeImpl::LastApplied() const {
  std::lock_guard<std::mutex> lk(mu_);
  return last_applied_;
}

// ---- background threads ----

void RaftNodeImpl::TickerLoop() {
  std::unique_lock<std::mutex> lk(mu_);
  while (running_) {
    cv_.wait_for(lk, std::chrono::milliseconds(10), [this] { return !running_; });
    if (!running_) break;
    Clock::time_point now = Clock::now();
    if (role_ == Role::kLeader) {
      if (now >= next_heartbeat_) BroadcastAppendEntriesLocked();
    } else if (now >= election_deadline_) {
      StartElectionOrPreVoteLocked();
    }
  }
}

// Delivers committed entries to the apply callback, in order, without holding the lock. Also
// restores the state machine from a snapshot and takes new snapshots; doing all of it on this one
// thread means a snapshot always matches exactly the entries applied so far.
void RaftNodeImpl::ApplierLoop() {
  std::unique_lock<std::mutex> lk(mu_);
  while (running_) {
    apply_cv_.wait(lk, [this] {
      if (!running_) return true;
      if (restore_pending_) return snap_restorer_ != nullptr;
      return apply_cb_ != nullptr && last_applied_ < commit_index_;
    });
    if (!running_) break;

    if (restore_pending_) {
      restore_pending_ = false;  // a snapshot installed while we restore sets it again
      std::string data = snapshot_data_;
      const LogIndex index = snapshot_index_;
      SnapshotRestorer restore = snap_restorer_;
      lk.unlock();
      restore(data);
      lk.lock();
      last_applied_ = index;
      wait_cv_.notify_all();
      continue;
    }

    std::vector<LogEntry> batch;  // committed entries are never truncated, so copying is safe
    for (LogIndex i = last_applied_ + 1; i <= commit_index_; ++i) batch.push_back(EntryLocked(i));
    const LogIndex batch_end = commit_index_;
    ApplyCallback cb = apply_cb_;

    lk.unlock();
    for (const LogEntry& e : batch) {
      if (e.command.type != CommandType::kNoop) cb(e);
    }
    lk.lock();
    if (!restore_pending_) last_applied_ = batch_end;  // else the restore sets it, further ahead
    wait_cv_.notify_all();  // wake ProposeAndWait callers whose entry is now applied

    if (cfg_.snapshot_threshold_entries > 0 && snap_taker_ != nullptr && !restore_pending_ &&
        last_applied_ >= snapshot_index_ + cfg_.snapshot_threshold_entries) {
      const LogIndex at = last_applied_;
      SnapshotTaker take = snap_taker_;
      lk.unlock();
      std::string data = take();
      lk.lock();
      if (!restore_pending_ && last_applied_ == at && at > snapshot_index_) {
        CompactLocked(at, std::move(data));
      }
    }
  }
}

// ---- elections ----

void RaftNodeImpl::ResetElectionDeadlineLocked() {
  std::uniform_int_distribution<int> dist(cfg_.election_timeout_min_ms,
                                          cfg_.election_timeout_max_ms);
  election_deadline_ = Clock::now() + std::chrono::milliseconds(dist(rng_));
}

void RaftNodeImpl::BecomeFollowerLocked(Term term, bool persist) {
  bool term_changed = term > current_term_;
  if (term_changed) {
    current_term_ = term;
    voted_for_ = 0;  // a new term means we have not voted yet
  }
  role_ = Role::kFollower;
  prevote_active_ = false;  // whatever we were asking about is moot now
  ResetElectionDeadlineLocked();
  if (term_changed && persist) PersistHardStateLocked();
  wait_cv_.notify_all();  // a ReadIndex waiter must notice it is no longer leader
}

void RaftNodeImpl::NoteLeaderContactLocked() {
  have_leader_contact_ = true;
  last_leader_contact_ = Clock::now();
  prevote_active_ = false;  // a leader exists: abandon any pre-vote round in progress
}

// An election timer fired. With PreVote we first ask, without touching any state, whether a real
// election could be won; only a majority of "yes" turns into one.
void RaftNodeImpl::StartElectionOrPreVoteLocked() {
  if (cfg_.enable_pre_vote && !peers_.empty() && transport_->SupportsPreVote()) {
    StartPreVoteLocked();
  } else {
    StartElectionLocked();
  }
}

void RaftNodeImpl::StartPreVoteLocked() {
  role_ = Role::kFollower;  // a candidate whose election timed out starts over as a follower
  prevote_active_ = true;
  ++prevote_round_;
  prevotes_ = 1;  // ourselves
  leader_hint_ = 0;
  ResetElectionDeadlineLocked();  // if this round gets no majority, the next timeout retries
  // Nothing is persisted and our term is unchanged: only the PROPOSED term goes on the wire.
  PreVoteRequest req{current_term_ + 1, id_, LastLogIndexLocked(), LastLogTermLocked()};
  const uint64_t round = prevote_round_;
  for (NodeId peer : peers_) {
    transport_->SendPreVote(peer, req, [this, round](const PreVoteReply& reply) {
      OnPreVoteReply(round, reply);
    });
  }
}

void RaftNodeImpl::OnPreVoteReply(uint64_t round, const PreVoteReply& reply) {
  std::lock_guard<std::mutex> lk(mu_);
  if (!running_) return;
  if (reply.term > current_term_) {  // we are behind: adopt the newer term, no election for us
    BecomeFollowerLocked(reply.term);
    return;
  }
  if (!prevote_active_ || round != prevote_round_ || !reply.granted) return;
  ++prevotes_;
  const size_t cluster_size = peers_.size() + 1;
  if (static_cast<size_t>(prevotes_) > cluster_size / 2) {
    prevote_active_ = false;
    StartElectionLocked();  // the real thing: new term, persisted, RequestVote
  }
}

void RaftNodeImpl::StartElectionLocked() {
  role_ = Role::kCandidate;
  ++current_term_;
  voted_for_ = id_;  // vote for ourselves
  votes_ = 1;
  leader_hint_ = 0;
  ResetElectionDeadlineLocked();
  PersistHardStateLocked();  // our new term and our own vote must be durable before asking for votes

  if (peers_.empty()) {  // single-node cluster wins immediately
    BecomeLeaderLocked();
    return;
  }

  RequestVoteRequest req{current_term_, id_, LastLogIndexLocked(), LastLogTermLocked()};
  Term election_term = current_term_;
  for (NodeId peer : peers_) {
    transport_->SendRequestVote(peer, req, [this, election_term](const RequestVoteReply& reply) {
      OnRequestVoteReply(election_term, reply);
    });
  }
}

void RaftNodeImpl::OnRequestVoteReply(Term election_term, const RequestVoteReply& reply) {
  std::lock_guard<std::mutex> lk(mu_);
  if (!running_) return;
  if (reply.term > current_term_) {
    BecomeFollowerLocked(reply.term);
    return;
  }
  // Ignore stale replies from an older election, or if we are no longer a candidate.
  if (role_ != Role::kCandidate || current_term_ != election_term || !reply.vote_granted) return;

  ++votes_;
  size_t cluster_size = peers_.size() + 1;
  if (static_cast<size_t>(votes_) > cluster_size / 2) BecomeLeaderLocked();  // majority
}

void RaftNodeImpl::BecomeLeaderLocked() {
  role_ = Role::kLeader;
  leader_hint_ = id_;
  snap_offset_.clear();
  for (NodeId peer : peers_) {
    next_index_[peer] = LastLogIndexLocked() + 1;  // optimistic: assume followers are caught up
    match_index_[peer] = 0;                        // nothing is known to be replicated yet
  }
  // A leader may only commit entries from its OWN term by counting replicas. Appending a
  // no-op in the new term lets entries from earlier terms commit right after an election
  // (Raft paper, section 8) instead of waiting for the next client write.
  log_.push_back(LogEntry{current_term_, LastLogIndexLocked() + 1,
                          Command{CommandType::kNoop, "", ""}});
  PersistAppendLocked({log_.back()});
  acked_seq_.clear();
  term_start_index_ = log_.back().index;  // reads must wait until this entry is committed
  AdvanceCommitIndexLocked();  // a single-node cluster commits the no-op immediately
  BroadcastAppendEntriesLocked();
}

// ---- replication (leader side) ----

void RaftNodeImpl::BroadcastAppendEntriesLocked() {
  read_round_mark_ = send_seq_;  // every message of this round gets a larger number
  read_round_in_flight_ = true;
  read_round_wanted_ = false;    // this round serves every read that was waiting for one
  next_heartbeat_ = Clock::now() + std::chrono::milliseconds(cfg_.heartbeat_interval_ms);
  for (NodeId peer : peers_) SendAppendEntriesToLocked(peer);
}

// Sends the follower everything from next_index_ onwards (empty = plain heartbeat).
void RaftNodeImpl::SendAppendEntriesToLocked(NodeId peer) {
  LogIndex next = next_index_[peer];
  if (next <= base_index_) {  // the follower needs entries we no longer have: send the snapshot
    SendSnapshotChunkLocked(peer);
    return;
  }
  LogIndex prev = next - 1;  // next_index_ is never below 1
  AppendEntriesRequest req{current_term_, id_, prev, EntryLocked(prev).term, {}, commit_index_};
  LogIndex last = LastLogIndexLocked();
  size_t bytes = 0;
  for (LogIndex i = next; i <= last && req.entries.size() < cfg_.max_entries_per_append; ++i) {
    size_t entry_bytes = ApproxEntryBytes(EntryLocked(i));
    if (!req.entries.empty() && bytes + entry_bytes > cfg_.max_bytes_per_append) break;
    bytes += entry_bytes;
    req.entries.push_back(EntryLocked(i));
  }
  Term sent_term = current_term_;
  size_t n_entries = req.entries.size();
  const uint64_t seq = ++send_seq_;
  transport_->SendAppendEntries(
      peer, req,
      [this, peer, sent_term, seq, prev, n_entries](const AppendEntriesReply& reply) {
        OnAppendEntriesReply(peer, sent_term, seq, prev, n_entries, reply);
      });
}

// Sends the next chunk of the current snapshot to a follower (stop-and-wait: the reply to a chunk
// triggers the next; the heartbeat timer resends the current one if a message was lost).
void RaftNodeImpl::SendSnapshotChunkLocked(NodeId peer) {
  uint64_t offset = snap_offset_[peer];
  if (offset > snapshot_data_.size()) offset = 0;
  const size_t chunk =
      cfg_.max_bytes_per_append > 2 * kMessageOverheadBytes ? cfg_.max_bytes_per_append - 2 * kMessageOverheadBytes : 1;
  const size_t n = std::min<size_t>(chunk, snapshot_data_.size() - static_cast<size_t>(offset));
  InstallSnapshotRequest req{current_term_,
                             id_,
                             snapshot_index_,
                             snapshot_term_,
                             offset,
                             snapshot_data_.substr(static_cast<size_t>(offset), n),
                             offset + n == snapshot_data_.size()};
  const Term sent_term = current_term_;
  const LogIndex snapshot_index = snapshot_index_;
  const uint64_t seq = ++send_seq_;
  transport_->SendInstallSnapshot(
      peer, req, [this, peer, sent_term, seq, snapshot_index](const InstallSnapshotReply& reply) {
        OnInstallSnapshotReply(peer, sent_term, seq, snapshot_index, reply);
      });
}

void RaftNodeImpl::OnInstallSnapshotReply(NodeId peer, Term sent_term, uint64_t seq,
                                          LogIndex snapshot_index,
                                          const InstallSnapshotReply& reply) {
  std::lock_guard<std::mutex> lk(mu_);
  if (!running_) return;
  if (reply.term > current_term_) {
    BecomeFollowerLocked(reply.term);
    return;
  }
  if (role_ != Role::kLeader || current_term_ != sent_term) return;
  RecordAckLocked(peer, seq);
  if (snapshot_index != snapshot_index_) return;  // we have a newer snapshot now; heartbeat restarts
  if (reply.installed) {
    snap_offset_[peer] = 0;
    if (snapshot_index > match_index_[peer]) {
      match_index_[peer] = snapshot_index;
      AdvanceCommitIndexLocked();
    }
    next_index_[peer] = std::max(next_index_[peer], match_index_[peer] + 1);
    if (next_index_[peer] <= LastLogIndexLocked()) SendAppendEntriesToLocked(peer);
    return;
  }
  snap_offset_[peer] = reply.next_offset;
  if (reply.next_offset < snapshot_data_.size()) SendSnapshotChunkLocked(peer);
}

void RaftNodeImpl::OnAppendEntriesReply(NodeId peer, Term sent_term, uint64_t seq,
                                        LogIndex prev_index, size_t n_entries,
                                        const AppendEntriesReply& reply) {
  std::lock_guard<std::mutex> lk(mu_);
  if (!running_) return;
  if (reply.term > current_term_) {  // we are a stale leader
    BecomeFollowerLocked(reply.term);
    return;
  }
  // Ignore replies to requests sent in an older term, or if we are no longer the leader.
  if (role_ != Role::kLeader || current_term_ != sent_term) return;
  RecordAckLocked(peer, seq);  // even a rejection means the follower accepted us as leader

  if (reply.success) {
    LogIndex matched = prev_index + n_entries;
    if (matched > match_index_[peer]) {
      match_index_[peer] = matched;
      AdvanceCommitIndexLocked();
    }
    next_index_[peer] = std::max(next_index_[peer], match_index_[peer] + 1);
    if (next_index_[peer] <= LastLogIndexLocked()) SendAppendEntriesToLocked(peer);  // more to send
  } else {
    // The follower's log does not match at prev_index: back up (using its last index as a hint
    // to skip ahead when it is far behind) and retry straight away.
    LogIndex backed_up = next_index_[peer] > 1 ? next_index_[peer] - 1 : 1;
    LogIndex hinted = reply.match_index + 1;
    next_index_[peer] = std::max<LogIndex>(std::min(backed_up, hinted), match_index_[peer] + 1);
    SendAppendEntriesToLocked(peer);
  }
}

// Commit rule: an entry is committed once a majority stores it AND it is from the current
// term. Entries from earlier terms become committed indirectly, when a later one does.
void RaftNodeImpl::AdvanceCommitIndexLocked() {
  size_t cluster_size = peers_.size() + 1;
  for (LogIndex n = LastLogIndexLocked(); n > commit_index_; --n) {
    if (EntryLocked(n).term != current_term_) break;  // older terms cannot commit yet
    size_t replicas = 1;                       // the leader itself
    for (NodeId peer : peers_) {
      if (match_index_[peer] >= n) ++replicas;
    }
    if (replicas > cluster_size / 2) {
      commit_index_ = n;
      apply_cv_.notify_all();
      break;
    }
  }
}

// ---- incoming RPCs ----

RequestVoteReply RaftNodeImpl::HandleRequestVote(const RequestVoteRequest& req) {
  std::lock_guard<std::mutex> lk(mu_);
  bool state_changed = false;
  if (req.term > current_term_) {
    BecomeFollowerLocked(req.term, /*persist=*/false);  // saved once, below
    state_changed = true;
  }

  RequestVoteReply reply{current_term_, false};
  if (req.term < current_term_) return reply;  // candidate is behind

  bool can_vote = (voted_for_ == 0 || voted_for_ == req.candidate_id);
  // Only vote for a candidate whose log is at least as up to date as ours.
  bool log_ok = req.last_log_term > LastLogTermLocked() ||
                (req.last_log_term == LastLogTermLocked() &&
                 req.last_log_index >= LastLogIndexLocked());
  if (can_vote && log_ok) {
    voted_for_ = req.candidate_id;
    reply.vote_granted = true;
    state_changed = true;
    ResetElectionDeadlineLocked();  // granting a vote counts as hearing from a live candidate
  }
  // The vote (and any term change) must be on disk before the candidate hears about it, or a
  // restart could make us vote twice in the same term.
  if (state_changed) PersistHardStateLocked();
  return reply;
}

PreVoteReply RaftNodeImpl::HandlePreVote(const PreVoteRequest& req) {
  std::lock_guard<std::mutex> lk(mu_);
  PreVoteReply reply{current_term_, false};
  if (req.term <= current_term_) return reply;  // a real election at that term could not succeed
  if (role_ == Role::kLeader) return reply;     // there is a leader, and it is us
  // A leader we heard from recently is alive, so this node is probably just cut off (or slow);
  // letting it start an election would only disrupt the cluster.
  if (have_leader_contact_ &&
      Clock::now() - last_leader_contact_ < std::chrono::milliseconds(cfg_.election_timeout_min_ms)) {
    return reply;
  }
  // The same log test as for a real vote: only a candidate that is at least as up to date.
  reply.granted = req.last_log_term > LastLogTermLocked() ||
                  (req.last_log_term == LastLogTermLocked() && req.last_log_index >= LastLogIndexLocked());
  return reply;  // deliberately NO state change: no term, no vote, no timer, no storage write
}

AppendEntriesReply RaftNodeImpl::HandleAppendEntries(const AppendEntriesRequest& req) {
  std::lock_guard<std::mutex> lk(mu_);
  if (req.term < current_term_) return AppendEntriesReply{current_term_, false, 0};  // stale leader

  // A valid leader for this term exists: step down if needed and reset the timer.
  if (req.term > current_term_ || role_ != Role::kFollower) {
    BecomeFollowerLocked(req.term);
  } else {
    ResetElectionDeadlineLocked();
  }
  leader_hint_ = req.leader_id;
  NoteLeaderContactLocked();

  // Consistency check: our log must contain the entry just before the new ones.
  // On failure we report our last index so the leader can skip back quickly.
  // (An index inside our snapshot is committed, so it matches the leader's log by definition.)
  if (req.prev_log_index > LastLogIndexLocked() ||
      (req.prev_log_index >= base_index_ &&
       EntryLocked(req.prev_log_index).term != req.prev_log_term)) {
    return AppendEntriesReply{current_term_, false, LastLogIndexLocked()};
  }

  // Append the new entries. If an existing entry conflicts (same index, different term) it and
  // everything after it are dropped. Entries we already have are left alone, so duplicated or
  // reordered messages are harmless. Committed entries can never conflict.
  LogIndex index = req.prev_log_index;
  std::vector<LogEntry> appended;
  bool truncated = false;
  for (const LogEntry& entry : req.entries) {
    ++index;
    if (index <= base_index_) continue;  // already dropped from our log (committed)
    if (index <= LastLogIndexLocked()) {
      if (EntryLocked(index).term == entry.term) continue;
      log_.erase(log_.begin() + static_cast<std::ptrdiff_t>(index - base_index_), log_.end());
      PersistTruncateLocked(index);
      truncated = true;
    }
    log_.push_back(entry);
    appended.push_back(entry);
  }
  if (!appended.empty()) PersistAppendLocked(appended);  // durable before we acknowledge
  if (truncated) wait_cv_.notify_all();  // entries a waiter proposed may just have been replaced

  // Follow the leader's commit point, but never past what this message verified.
  LogIndex last_new = req.prev_log_index + req.entries.size();
  LogIndex new_commit = std::min(req.leader_commit, last_new);
  if (new_commit > commit_index_) {
    commit_index_ = new_commit;
    apply_cv_.notify_all();
  }
  return AppendEntriesReply{current_term_, true, last_new};
}

InstallSnapshotReply RaftNodeImpl::HandleInstallSnapshot(const InstallSnapshotRequest& req) {
  std::lock_guard<std::mutex> lk(mu_);
  if (req.term < current_term_) return InstallSnapshotReply{current_term_, 0, false};

  if (req.term > current_term_ || role_ != Role::kFollower) {
    BecomeFollowerLocked(req.term);
  } else {
    ResetElectionDeadlineLocked();
  }
  leader_hint_ = req.leader_id;
  NoteLeaderContactLocked();

  // We already hold everything this snapshot covers (committed entries match the leader's).
  if (req.last_included_index <= commit_index_) return InstallSnapshotReply{current_term_, 0, true};
  if (snap_restorer_ == nullptr) return InstallSnapshotReply{current_term_, 0, false};

  if (!incoming_.active || incoming_.index != req.last_included_index ||
      incoming_.term != req.last_included_term) {
    if (req.offset != 0) return InstallSnapshotReply{current_term_, 0, false};  // start over
    incoming_ = IncomingSnapshot{true, req.last_included_index, req.last_included_term, {}};
  }
  if (req.offset != incoming_.data.size()) {  // duplicate or out of order: say what we expect
    return InstallSnapshotReply{current_term_, incoming_.data.size(), false};
  }
  incoming_.data += req.data;
  if (!req.done) return InstallSnapshotReply{current_term_, incoming_.data.size(), false};

  const uint64_t received = incoming_.data.size();
  InstallSnapshotLocked(incoming_.index, incoming_.term, std::move(incoming_.data));
  incoming_ = IncomingSnapshot{};
  return InstallSnapshotReply{current_term_, received, true};
}

// Follower: adopt a complete snapshot from the leader. Our log entries after it are kept only if
// we hold the snapshot's last entry with the same term (then they agree with the leader's log).
void RaftNodeImpl::InstallSnapshotLocked(LogIndex index, Term term, std::string data) {
  std::vector<LogEntry> suffix;
  if (index >= base_index_ && index <= LastLogIndexLocked() && EntryLocked(index).term == term) {
    suffix.assign(log_.begin() + static_cast<std::ptrdiff_t>(index - base_index_ + 1), log_.end());
  }
  if (storage_ != nullptr) MustSucceed(storage_->SaveSnapshot(index, term, data, suffix), "save snapshot");
  log_.clear();
  log_.push_back(LogEntry{term, index, Command{CommandType::kNoop, "", ""}});
  for (LogEntry& e : suffix) log_.push_back(std::move(e));
  base_index_ = index;
  base_term_ = term;
  snapshot_index_ = index;
  snapshot_term_ = term;
  snapshot_data_ = std::move(data);
  commit_index_ = std::max(commit_index_, index);
  restore_pending_ = true;  // the applier restores the state machine, then continues after `index`
  apply_cv_.notify_all();
  wait_cv_.notify_all();
}

// Applier thread: the state machine has applied exactly the entries up to `index` and `data` is
// its serialized state. Save it and drop the log, except for the last snapshot_trailing_entries
// entries up to `index`, which stay so that a slightly slow follower needs no snapshot.
void RaftNodeImpl::CompactLocked(LogIndex index, std::string data) {
  const Term term = EntryLocked(index).term;
  LogIndex new_base = index > cfg_.snapshot_trailing_entries ? index - cfg_.snapshot_trailing_entries : 0;
  new_base = std::max(new_base, base_index_);  // the log never grows backwards
  const Term new_base_term = EntryLocked(new_base).term;
  if (storage_ != nullptr) {
    std::vector<LogEntry> kept(log_.begin() + static_cast<std::ptrdiff_t>(new_base - base_index_ + 1),
                               log_.end());
    MustSucceed(storage_->SaveSnapshot(index, term, data, new_base, new_base_term, kept), "save snapshot");
  }
  log_.erase(log_.begin(), log_.begin() + static_cast<std::ptrdiff_t>(new_base - base_index_));
  log_[0] = LogEntry{new_base_term, new_base, Command{CommandType::kNoop, "", ""}};
  base_index_ = new_base;
  base_term_ = new_base_term;
  snapshot_index_ = index;
  snapshot_term_ = term;
  snapshot_data_ = std::move(data);
  snap_offset_.clear();  // any transfer in progress was of the older snapshot
}

void RaftNodeImpl::PersistHardStateLocked() {
  if (storage_ != nullptr) MustSucceed(storage_->SaveHardState(current_term_, voted_for_), "save term/vote");
}

void RaftNodeImpl::PersistAppendLocked(const std::vector<LogEntry>& entries) {
  if (storage_ != nullptr) MustSucceed(storage_->AppendEntries(entries), "append log entries");
}

void RaftNodeImpl::PersistTruncateLocked(LogIndex from_index) {
  if (storage_ != nullptr) MustSucceed(storage_->TruncateSuffix(from_index), "truncate log");
}

LogIndex RaftNodeImpl::LastLogIndexLocked() const { return log_.back().index; }

Term RaftNodeImpl::LastLogTermLocked() const { return log_.back().term; }

}  // namespace raftkv
