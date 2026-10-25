#pragma once
#include <cstdint>
#include <string>
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

// A snapshot can be far bigger than one message, so the leader sends it in chunks, one at a
// time, each acknowledged before the next. `offset` is where `data` starts in the snapshot.
struct InstallSnapshotRequest {
  Term term;
  NodeId leader_id;
  LogIndex last_included_index;  // the snapshot replaces every log entry up to and including this
  Term last_included_term;
  uint64_t offset;
  std::string data;
  bool done;  // true on the last chunk
};

struct InstallSnapshotReply {
  Term term;
  uint64_t next_offset;  // the byte the follower expects next (0 = start over)
  bool installed;        // the follower now has (or already had) the state this snapshot covers
};

}  // namespace raftkv
