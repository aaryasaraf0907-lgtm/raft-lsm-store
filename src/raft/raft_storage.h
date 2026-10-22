#pragma once
#include <vector>
#include "raft/raft_interface.h"

namespace raftkv {

// Everything Raft must never forget across a crash: the current term, who we voted for in it,
// and the log. (commit_index and last_applied are NOT stored: they are re-learned from the
// leader, and the state machine re-applies the committed log after a restart.)
struct PersistentState {
  Term term = 0;
  NodeId voted_for = 0;       // 0 = nobody
  std::vector<LogEntry> log;  // entries 1..N in order; the dummy entry 0 is not stored
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
};

}  // namespace raftkv
