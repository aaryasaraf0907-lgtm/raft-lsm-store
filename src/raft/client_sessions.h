#pragma once
#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>
#include <mutex>
#include <unordered_map>

#include "raft/raft_interface.h"

namespace raftkv {

// Remembers, for every client, the highest request number already applied, so that a request
// the client retried (same client_id and seq) takes effect only once.
//
// Assumptions (the usual Raft client model, Raft paper section 8): a client has one request in
// flight at a time and numbers its requests upwards (1, 2, 3, ...). A request whose seq is not
// above the last applied one for its client is a duplicate.
//
// This table is part of the replicated state machine. It must be updated only in log-apply
// order, so that every replica makes the same decisions; after a restart the log is replayed
// in order, which rebuilds it. (Once snapshots exist it has to be saved inside them.)
// It grows by one small entry per client; expiring idle clients is future work.
class ClientSessionTable {
 public:
  // Returns true if `cmd` should be applied and records it; false if it is a duplicate.
  // Commands without a request id (client_id == 0 or seq == 0) are always applied.
  bool ShouldApply(const Command& cmd) {
    if (cmd.client_id == 0 || cmd.seq == 0) return true;
    std::lock_guard<std::mutex> lk(mu_);
    uint64_t& last = last_seq_[cmd.client_id];  // starts at 0 for a new client
    if (cmd.seq <= last) return false;
    last = cmd.seq;
    return true;
  }

  // The table as bytes, for snapshots: [u64 count] then (u64 client_id, u64 last_seq) pairs sorted
  // by client_id, so every replica produces identical bytes. Restore replaces the whole table and
  // returns false (leaving it unchanged) if the bytes are malformed.
  std::string Serialize() const {
    std::lock_guard<std::mutex> lk(mu_);
    std::vector<std::pair<uint64_t, uint64_t>> v(last_seq_.begin(), last_seq_.end());
    std::sort(v.begin(), v.end());
    std::string out;
    PutU64(&out, v.size());
    for (const auto& p : v) {
      PutU64(&out, p.first);
      PutU64(&out, p.second);
    }
    return out;
  }

  bool Restore(const std::string& bytes) {
    if (bytes.size() < 8) return false;
    uint64_t count = GetU64(bytes.data());
    if (count > (bytes.size() - 8) / 16 || bytes.size() != 8 + count * 16) return false;
    std::unordered_map<uint64_t, uint64_t> fresh;
    for (uint64_t i = 0; i < count; ++i) {
      const char* p = bytes.data() + 8 + i * 16;
      fresh[GetU64(p)] = GetU64(p + 8);
    }
    std::lock_guard<std::mutex> lk(mu_);
    last_seq_ = std::move(fresh);
    return true;
  }

  uint64_t LastSeq(uint64_t client_id) const {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = last_seq_.find(client_id);
    return it == last_seq_.end() ? 0 : it->second;
  }

  size_t ClientCount() const {
    std::lock_guard<std::mutex> lk(mu_);
    return last_seq_.size();
  }

 private:
  static void PutU64(std::string* s, uint64_t v) {
    for (int i = 0; i < 8; ++i) s->push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
  }
  static uint64_t GetU64(const char* p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v |= static_cast<uint64_t>(static_cast<uint8_t>(p[i])) << (8 * i);
    return v;
  }

  mutable std::mutex mu_;
  std::unordered_map<uint64_t, uint64_t> last_seq_;  // client_id -> highest seq applied
};

}  // namespace raftkv
