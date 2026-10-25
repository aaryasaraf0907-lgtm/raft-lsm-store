#pragma once
#include <string>
#include <vector>
#include "raft/raft_interface.h"

namespace raftkv {

// Everything Raft must never forget across a crash: the current term, who we voted for in it,
// and the log. (commit_index and last_applied are NOT stored: they are re-learned from the
// leader, and the state machine re-applies the committed log after a restart.)
struct PersistentState {
  Term term = 0;
  NodeId voted_for = 0;       // 0 = nobody
  // The latest snapshot, if any (snapshot_index == 0 means none). It stands for every log entry
  // up to and including snapshot_index.
  LogIndex snapshot_index = 0;
  Term snapshot_term = 0;
  std::string snapshot_data;
  // The stored log may start BEFORE the snapshot: a few entries are kept below it so a slightly
  // slow follower can still be sent log entries. Invariant: log_base_index <= snapshot_index.
  LogIndex log_base_index = 0;
  Term log_base_term = 0;
  std::vector<LogEntry> log;  // entries log_base_index+1 .. N in order; the dummy entry is not stored
};

// Durable storage for a Raft node. Every mutating call must be durable (on disk, fsynced)
// when it returns true: Raft answers RPCs only after its state is saved.
class RaftStorage {
 public:
  virtual ~RaftStorage() = default;

  // Reads everything back. Must be called once, before any write.
  virtual bool Load(PersistentState* out) = 0;
  virtual bool SaveHardState(Term term, NodeId voted_for) = 0;
  // Appends entries; their indexes must continue directly after the stored log.
  virtual bool AppendEntries(const std::vector<LogEntry>& entries) = 0;
  // Removes entry `from_index` and everything after it.
  virtual bool TruncateSuffix(LogIndex from_index) = 0;
  // Durably stores a snapshot (the state machine as of `snapshot_index`) and drops every log entry
  // up to `log_base_index` (<= snapshot_index). `entries_after_base` are the log entries after
  // log_base_index to keep, including those up to the snapshot (possibly none). Crash safe: after
  // a crash Load() returns either the state from before this call or the state after it.
  virtual bool SaveSnapshot(LogIndex snapshot_index, Term snapshot_term, const std::string& data,
                            LogIndex log_base_index, Term log_base_term,
                            const std::vector<LogEntry>& entries_after_base) = 0;
  // Common case: the log starts exactly at the snapshot (nothing is kept below it).
  bool SaveSnapshot(LogIndex index, Term term, const std::string& data,
                    const std::vector<LogEntry>& entries_after) {
    return SaveSnapshot(index, term, data, index, term, entries_after);
  }
};

}  // namespace raftkv
