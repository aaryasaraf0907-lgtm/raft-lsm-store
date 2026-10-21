#include "raft/raft_node.h"

#include <utility>

namespace raftkv {

RaftNodeImpl::RaftNodeImpl(NodeId id, std::vector<NodeId> peers, Transport* transport,
                           RaftConfig cfg)
    : id_(id),
      peers_(std::move(peers)),
      transport_(transport),
      cfg_(cfg),
      rng_(cfg.random_seed + id) {
  log_.push_back(LogEntry{0, 0, Command{CommandType::kPut, "", ""}});  // dummy entry at index 0
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
}

void RaftNodeImpl::Stop() {
  if (!running_.exchange(false)) return;
  cv_.notify_all();
  if (ticker_.joinable()) ticker_.join();
  transport_->Stop();
}

ProposeResponse RaftNodeImpl::Propose(const Command& cmd) {
  std::lock_guard<std::mutex> lk(mu_);
  if (role_ != Role::kLeader) return {ProposeResult::kNotLeader, 0, leader_hint_};
  // Appends locally only for now. Replication and commit come in the next PR.
  LogIndex index = LastLogIndexLocked() + 1;
  log_.push_back(LogEntry{current_term_, index, cmd});
  return {ProposeResult::kAccepted, index, id_};
}

void RaftNodeImpl::SetApplyCallback(ApplyCallback cb) {
  std::lock_guard<std::mutex> lk(mu_);
  apply_cb_ = std::move(cb);
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

// ---- background timer ----

void RaftNodeImpl::TickerLoop() {
  std::unique_lock<std::mutex> lk(mu_);
  while (running_) {
    cv_.wait_for(lk, std::chrono::milliseconds(10), [this] { return !running_; });
    if (!running_) break;
    Clock::time_point now = Clock::now();
    if (role_ == Role::kLeader) {
      if (now >= next_heartbeat_) SendHeartbeatsLocked();
    } else if (now >= election_deadline_) {
      StartElectionLocked();
    }
  }
}

// ---- elections ----

void RaftNodeImpl::ResetElectionDeadlineLocked() {
  std::uniform_int_distribution<int> dist(cfg_.election_timeout_min_ms,
                                          cfg_.election_timeout_max_ms);
  election_deadline_ = Clock::now() + std::chrono::milliseconds(dist(rng_));
}

void RaftNodeImpl::BecomeFollowerLocked(Term term) {
  if (term > current_term_) {
    current_term_ = term;
    voted_for_ = 0;  // a new term means we have not voted yet
  }
  role_ = Role::kFollower;
  ResetElectionDeadlineLocked();
}

void RaftNodeImpl::StartElectionLocked() {
  role_ = Role::kCandidate;
  ++current_term_;
  voted_for_ = id_;  // vote for ourselves
  votes_ = 1;
  leader_hint_ = 0;
  ResetElectionDeadlineLocked();

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
  SendHeartbeatsLocked();  // announce leadership right away
}

void RaftNodeImpl::SendHeartbeatsLocked() {
  next_heartbeat_ = Clock::now() + std::chrono::milliseconds(cfg_.heartbeat_interval_ms);
  // Heartbeats carry no entries. prev_log_index/term are 0 until replication is added.
  AppendEntriesRequest req{current_term_, id_, 0, 0, {}, commit_index_};
  for (NodeId peer : peers_) {
    transport_->SendAppendEntries(
        peer, req, [this](const AppendEntriesReply& reply) { OnAppendEntriesReply(reply); });
  }
}

void RaftNodeImpl::OnAppendEntriesReply(const AppendEntriesReply& reply) {
  std::lock_guard<std::mutex> lk(mu_);
  if (!running_) return;
  if (reply.term > current_term_) BecomeFollowerLocked(reply.term);  // we are a stale leader
}

// ---- incoming RPCs ----

RequestVoteReply RaftNodeImpl::HandleRequestVote(const RequestVoteRequest& req) {
  std::lock_guard<std::mutex> lk(mu_);
  if (req.term > current_term_) BecomeFollowerLocked(req.term);

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
    ResetElectionDeadlineLocked();  // granting a vote counts as hearing from a live candidate
  }
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
  if (req.prev_log_index > LastLogIndexLocked() ||
      log_[req.prev_log_index].term != req.prev_log_term) {
    return AppendEntriesReply{current_term_, false, 0};
  }
  // Appending entries and advancing commit_index come in the next PR.
  return AppendEntriesReply{current_term_, true, req.prev_log_index};
}

LogIndex RaftNodeImpl::LastLogIndexLocked() const { return log_.back().index; }

Term RaftNodeImpl::LastLogTermLocked() const { return log_.back().term; }

}  // namespace raftkv
