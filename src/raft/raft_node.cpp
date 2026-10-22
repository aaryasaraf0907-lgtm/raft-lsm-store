#include "raft/raft_node.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <utility>

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
    for (LogEntry& entry : saved.log) {
      MustSucceed(entry.index == log_.size(), "stored log is not contiguous");
      log_.push_back(std::move(entry));
    }
  }
}

RaftNodeImpl::~RaftNodeImpl() { Stop(); }

void RaftNodeImpl::Start() {
  if (running_.exchange(true)) return;
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
  if (ticker_.joinable()) ticker_.join();
  if (applier_.joinable()) applier_.join();
  transport_->Stop();
}

ProposeResponse RaftNodeImpl::Propose(const Command& cmd) {
  std::lock_guard<std::mutex> lk(mu_);
  if (role_ != Role::kLeader) return {ProposeResult::kNotLeader, 0, leader_hint_};
  LogIndex index = LastLogIndexLocked() + 1;
  log_.push_back(LogEntry{current_term_, index, cmd});
  PersistAppendLocked({log_.back()});  // durable on the leader before we replicate or count it
  AdvanceCommitIndexLocked();       // a single-node cluster commits immediately
  BroadcastAppendEntriesLocked();   // replicate now instead of waiting for the next heartbeat
  return {ProposeResult::kAccepted, index, id_};
}

void RaftNodeImpl::SetApplyCallback(ApplyCallback cb) {
  {
    std::lock_guard<std::mutex> lk(mu_);
    apply_cb_ = std::move(cb);
  }
  apply_cv_.notify_all();
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
      StartElectionLocked();
    }
  }
}

// Delivers committed entries to the apply callback, in order, without holding the lock.
void RaftNodeImpl::ApplierLoop() {
  std::unique_lock<std::mutex> lk(mu_);
  while (running_) {
    apply_cv_.wait(lk, [this] {
      return !running_ || (apply_cb_ != nullptr && last_applied_ < commit_index_);
    });
    if (!running_) break;

    std::vector<LogEntry> batch;  // committed entries are never truncated, so copying is safe
    for (LogIndex i = last_applied_ + 1; i <= commit_index_; ++i) batch.push_back(log_[i]);
    ApplyCallback cb = apply_cb_;

    lk.unlock();
    for (const LogEntry& e : batch) {
      if (e.command.type != CommandType::kNoop) cb(e);
    }
    lk.lock();
    last_applied_ += batch.size();
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
  ResetElectionDeadlineLocked();
  if (term_changed && persist) PersistHardStateLocked();
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
  AdvanceCommitIndexLocked();  // a single-node cluster commits the no-op immediately
  BroadcastAppendEntriesLocked();
}

// ---- replication (leader side) ----

void RaftNodeImpl::BroadcastAppendEntriesLocked() {
  next_heartbeat_ = Clock::now() + std::chrono::milliseconds(cfg_.heartbeat_interval_ms);
  for (NodeId peer : peers_) SendAppendEntriesToLocked(peer);
}

// Sends the follower everything from next_index_ onwards (empty = plain heartbeat).
void RaftNodeImpl::SendAppendEntriesToLocked(NodeId peer) {
  LogIndex next = next_index_[peer];
  LogIndex prev = next - 1;  // next_index_ is never below 1
  AppendEntriesRequest req{current_term_, id_, prev, log_[prev].term, {}, commit_index_};
  LogIndex last = LastLogIndexLocked();
  for (LogIndex i = next; i <= last && req.entries.size() < cfg_.max_entries_per_append; ++i) {
    req.entries.push_back(log_[i]);
  }
  Term sent_term = current_term_;
  size_t n_entries = req.entries.size();
  transport_->SendAppendEntries(
      peer, req,
      [this, peer, sent_term, prev, n_entries](const AppendEntriesReply& reply) {
        OnAppendEntriesReply(peer, sent_term, prev, n_entries, reply);
      });
}

void RaftNodeImpl::OnAppendEntriesReply(NodeId peer, Term sent_term, LogIndex prev_index,
                                        size_t n_entries, const AppendEntriesReply& reply) {
  std::lock_guard<std::mutex> lk(mu_);
  if (!running_) return;
  if (reply.term > current_term_) {  // we are a stale leader
    BecomeFollowerLocked(reply.term);
    return;
  }
  // Ignore replies to requests sent in an older term, or if we are no longer the leader.
  if (role_ != Role::kLeader || current_term_ != sent_term) return;

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
    if (log_[n].term != current_term_) break;  // older terms below this point cannot commit yet
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

  // Consistency check: our log must contain the entry just before the new ones.
  // On failure we report our last index so the leader can skip back quickly.
  if (req.prev_log_index > LastLogIndexLocked() ||
      log_[req.prev_log_index].term != req.prev_log_term) {
    return AppendEntriesReply{current_term_, false, LastLogIndexLocked()};
  }

  // Append the new entries. If an existing entry conflicts (same index, different term) it and
  // everything after it are dropped. Entries we already have are left alone, so duplicated or
  // reordered messages are harmless. Committed entries can never conflict.
  LogIndex index = req.prev_log_index;
  std::vector<LogEntry> appended;
  for (const LogEntry& entry : req.entries) {
    ++index;
    if (index <= LastLogIndexLocked()) {
      if (log_[index].term == entry.term) continue;
      log_.erase(log_.begin() + static_cast<std::ptrdiff_t>(index), log_.end());
      PersistTruncateLocked(index);
    }
    log_.push_back(entry);
    appended.push_back(entry);
  }
  if (!appended.empty()) PersistAppendLocked(appended);  // durable before we acknowledge

  // Follow the leader's commit point, but never past what this message verified.
  LogIndex last_new = req.prev_log_index + req.entries.size();
  LogIndex new_commit = std::min(req.leader_commit, last_new);
  if (new_commit > commit_index_) {
    commit_index_ = new_commit;
    apply_cv_.notify_all();
  }
  return AppendEntriesReply{current_term_, true, last_new};
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
