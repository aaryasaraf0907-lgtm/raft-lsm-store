#pragma once
#include <cstdint>
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
  mutable std::mutex mu_;
  std::unordered_map<uint64_t, uint64_t> last_seq_;  // client_id -> highest seq applied
};

}  // namespace raftkv
