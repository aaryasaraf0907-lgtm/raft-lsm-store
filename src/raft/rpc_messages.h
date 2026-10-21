#pragma once
#include <vector>
#include "raft/raft_interface.h"

namespace raftkv {

struct RequestVoteRequest {
  Term term;
  NodeId candidate_id;
  LogIndex last_log_index;
  Term last_log_term;
};

struct RequestVoteReply {
  Term term;
  bool vote_granted;
};

struct AppendEntriesRequest {
  Term term;
  NodeId leader_id;
  LogIndex prev_log_index;
  Term prev_log_term;
  std::vector<LogEntry> entries;  // empty for heartbeats
  LogIndex leader_commit;
};

struct AppendEntriesReply {
  Term term;
  bool success;
  LogIndex match_index;  // highest index known to match, helps the leader back up quickly
};

}  // namespace raftkv
