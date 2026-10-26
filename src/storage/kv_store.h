#pragma once
#include <cstddef>
#include <cstdint>
#include <string>

namespace raftkv {

// kInvalidArgument: the request itself is unacceptable (for example a key or value over the
// size limit). It is never retryable and must be rejected BEFORE anything is replicated or
// logged. Always add new values at the end: the numeric values must not change.
enum class Status { kOk, kNotFound, kIOError, kCorruption, kInvalidArgument };

// Contract for the local LSM storage engine.
// Raft's apply callback calls Put/Delete only AFTER an entry is committed.
// Implementations must be safe to call from multiple threads.
class KVStore {
 public:
  virtual ~KVStore() = default;

  virtual Status Put(const std::string& key, const std::string& value) = 0;
  // On kOk, *value holds the result. On kNotFound, *value is untouched.
  virtual Status Get(const std::string& key, std::string* value) = 0;
  virtual Status Delete(const std::string& key) = 0;
};

// Keys that start with this prefix belong to the storage engine itself (it keeps its own
// bookkeeping, such as the applied Raft index, under such keys). Users may not read or write them:
// the client service and the engine reject them with kInvalidArgument.
inline constexpr const char kReservedKeyPrefix[] = "\x00raftkv\x00";
inline bool HasReservedKeyPrefix(const std::string& key) {
  const size_t n = sizeof(kReservedKeyPrefix) - 1;
  return key.size() >= n && key.compare(0, n, kReservedKeyPrefix, n) == 0;
}

// A store that remembers, durably and together with the data, the Raft log index of the last
// entry applied through PutAt/DeleteAt. After a restart the Raft log is replayed through the state
// machine; entries at or below AppliedIndex() are already in the store, so replay can skip their
// writes. The index is written after the data it covers, in the same sync, so it can lag the data
// by one operation after a crash (replaying that one operation is harmless: Put and Delete are
// idempotent) but never runs ahead of it. PutAt/DeleteAt require raft_index > AppliedIndex().
class IndexedKVStore : public KVStore {
 public:
  virtual Status PutAt(const std::string& key, const std::string& value, uint64_t raft_index) = 0;
  virtual Status DeleteAt(const std::string& key, uint64_t raft_index) = 0;
  // 0 if nothing has been applied yet.
  virtual uint64_t AppliedIndex() = 0;
};

}  // namespace raftkv
