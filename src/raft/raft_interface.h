#pragma once
#include <chrono>
#include <cstdint>
#include <functional>
#include <string>

namespace raftkv {

using Term = uint64_t;
using LogIndex = uint64_t;
using NodeId = uint32_t;

enum class CommandType : uint8_t { kPut, kDelete, kNoop };

struct Command {
  CommandType type;
  std::string key;
  std::string value;  // empty for kDelete
  // Identifies the client request, for exactly-once writes. A client picks a random non-zero
  // client_id and numbers its requests 1, 2, 3, ...; a retry reuses the same seq. The state
  // machine skips a (client_id, seq) it has already applied. client_id == 0 or seq == 0 means
  // "no request id": the command is always applied (used for internal entries).
  uint64_t client_id = 0;
  uint64_t seq = 0;
};

struct LogEntry {
  Term term;
  LogIndex index;
  Command command;
};

enum class ProposeResult { kAccepted, kNotLeader };

struct ProposeResponse {
  ProposeResult result;
  LogIndex index;       // log index assigned to the entry (valid if kAccepted)
  NodeId leader_hint;   // best-known leader (useful if kNotLeader)
};

// Outcome of RaftNode::ProposeAndWait.
enum class CommitResult {
  kApplied,    // committed by a majority AND already applied to the state machine
  kNotLeader,  // this node is not the leader; try leader_hint
  kLost,       // another leader overwrote the entry: it was NOT committed, so retrying is safe
  kTimeout,    // outcome unknown: the entry may still commit later (e.g. leader cut off)
  kStopped,    // the node shut down while waiting
};

struct CommitResponse {
  CommitResult result;
  LogIndex index;      // log index assigned to the entry (0 if it was never proposed)
  NodeId leader_hint;  // best-known leader (useful if kNotLeader)
};

// Called once per entry, in log order, only after the entry is COMMITTED
// (majority replicated). The server wires this to KVStore::Put/Delete.
using ApplyCallback = std::function<void(const LogEntry&)>;

class RaftNode {
 public:
  virtual ~RaftNode() = default;

  virtual void Start() = 0;
  virtual void Stop() = 0;

  // Leader only: appends to the Raft log and starts replication.
  // Returns when the entry is accepted into the log, NOT when committed.
  // Commit is signalled later through the ApplyCallback.
  virtual ProposeResponse Propose(const Command& cmd) = 0;

  // Leader only. Like Propose, but blocks until the entry has been committed AND applied (the
  // apply callback has returned for it), the entry is known to be lost, or `timeout` passes.
  // This is what a server calls to answer a client write.
  //   kApplied : the write is durable on a majority and visible in the state machine.
  //   kLost    : leadership changed and the entry was overwritten: it never committed.
  //   kTimeout : unknown. A retry of a non-idempotent command could apply it twice, so a real
  //              client needs request ids for de-duplication (a later piece of work).
  virtual CommitResponse ProposeAndWait(const Command& cmd,
                                        std::chrono::milliseconds timeout) = 0;

  // Linearizable read barrier. Blocks until it is safe to read this node's state machine and
  // see every write that was acknowledged before this call, then returns kApplied with `index`
  // = the log index the state machine has caught up to. Unlike a no-op through ProposeAndWait it
  // writes NOTHING to the log: the leader only confirms with a majority that it is still the
  // leader and waits for the state machine to reach its commit index.
  //   kApplied   : read the store now.
  //   kNotLeader : this node is not (or is no longer) the leader; try leader_hint.
  //   kTimeout   : leadership could not be confirmed in time (e.g. the leader is cut off).
  //   kStopped   : the node shut down.
  // The default is a no-op through the log, the same barrier as before, so an implementation
  // that does not override it keeps working. RaftNodeImpl overrides it with the real thing.
  virtual CommitResponse ReadIndex(std::chrono::milliseconds timeout) {
    return ProposeAndWait(Command{CommandType::kNoop, "", ""}, timeout);
  }

  virtual void SetApplyCallback(ApplyCallback cb) = 0;
  virtual bool IsLeader() const = 0;
  virtual Term CurrentTerm() const = 0;
};

}  // namespace raftkv
