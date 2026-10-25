#pragma once
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

}  // namespace raftkv
