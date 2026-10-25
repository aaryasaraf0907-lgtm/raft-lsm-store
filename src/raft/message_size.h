#pragma once
// Approximate wire size of Raft messages, used to cap AppendEntries by bytes (and by the fake
// network to mimic a transport message limit). Not an exact protobuf size: it is the payload
// plus a fixed per-entry allowance for term, index, type, client_id, seq and field tags.
#include <cstddef>

#include "raft/raft_interface.h"
#include "raft/rpc_messages.h"

namespace raftkv {

constexpr size_t kEntryOverheadBytes = 64;
constexpr size_t kMessageOverheadBytes = 64;

inline size_t ApproxEntryBytes(const LogEntry& e) {
  return kEntryOverheadBytes + e.command.key.size() + e.command.value.size();
}

inline size_t ApproxAppendBytes(const AppendEntriesRequest& req) {
  size_t total = kMessageOverheadBytes;
  for (const LogEntry& e : req.entries) total += ApproxEntryBytes(e);
  return total;
}

}  // namespace raftkv
