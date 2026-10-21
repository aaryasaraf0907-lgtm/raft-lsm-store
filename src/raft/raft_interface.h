#pragma once
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

  virtual void SetApplyCallback(ApplyCallback cb) = 0;
  virtual bool IsLeader() const = 0;
  virtual Term CurrentTerm() const = 0;
};

}  // namespace raftkv
