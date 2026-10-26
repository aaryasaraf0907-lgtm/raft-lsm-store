// A checkpoint is a consistent copy of an engine's whole dataset, used for Raft snapshots: a node
// that fell far behind is sent the leader's checkpoint instead of the log entries it missed.
#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "storage/kv_store.h"

namespace raftkv {

// Layout of checkpoint data (little endian):
//   u64 count, then count times:  u32 key_length | key | u32 value_length | value
// Keys are strictly increasing, are never under the reserved prefix, and only live keys appear
// (no tombstones: a checkpoint replaces the whole dataset, so there is nothing older to hide).
inline void AppendCheckpointEntry(std::string* out, const std::string& key, const std::string& value) {
  auto put32 = [&](uint32_t v) { for (int i = 0; i < 4; ++i) out->push_back(static_cast<char>((v >> (8 * i)) & 0xFF)); };
  put32(static_cast<uint32_t>(key.size()));
  out->append(key);
  put32(static_cast<uint32_t>(value.size()));
  out->append(value);
}

inline std::string CheckpointHeader(uint64_t count) {
  std::string out;
  for (int i = 0; i < 8; ++i) out.push_back(static_cast<char>((count >> (8 * i)) & 0xFF));
  return out;
}

// False for anything malformed: truncated, trailing bytes, keys not strictly increasing, or a
// reserved key.
inline bool ParseCheckpoint(const std::string& data, std::vector<std::pair<std::string, std::string>>* entries) {
  size_t pos = 0;
  auto get = [&](int n, uint64_t* v) {
    if (data.size() - pos < static_cast<size_t>(n)) return false;
    uint64_t r = 0;
    for (int i = 0; i < n; ++i) r |= static_cast<uint64_t>(static_cast<uint8_t>(data[pos + static_cast<size_t>(i)])) << (8 * i);
    pos += static_cast<size_t>(n);
    *v = r;
    return true;
  };
  uint64_t count = 0;
  if (!get(8, &count)) return false;
  entries->clear();
  for (uint64_t i = 0; i < count; ++i) {
    uint64_t klen = 0, vlen = 0;
    if (!get(4, &klen) || data.size() - pos < klen) return false;
    std::string key = data.substr(pos, static_cast<size_t>(klen));
    pos += static_cast<size_t>(klen);
    if (!get(4, &vlen) || data.size() - pos < vlen) return false;
    std::string value = data.substr(pos, static_cast<size_t>(vlen));
    pos += static_cast<size_t>(vlen);
    if (HasReservedKeyPrefix(key)) return false;
    if (!entries->empty() && !(entries->back().first < key)) return false;
    entries->emplace_back(std::move(key), std::move(value));
  }
  return pos == data.size();
}

class CheckpointableStore : public IndexedKVStore {
 public:
  // A consistent copy of every live key and the applied index it corresponds to. Writes may go on
  // while it runs; they are simply not in this copy.
  virtual Status Checkpoint(std::string* data, uint64_t* applied_index) = 0;
  // Replaces the WHOLE dataset with `data`, atomically and durably: after a crash the old dataset
  // or the new one is there, never a mix. AppliedIndex() becomes `applied_index`. Anything written
  // concurrently is discarded along with the old data.
  virtual Status InstallCheckpoint(const std::string& data, uint64_t applied_index) = 0;
};

}  // namespace raftkv
