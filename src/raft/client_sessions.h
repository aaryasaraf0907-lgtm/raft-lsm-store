#pragma once
#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>
#include <mutex>
#include <set>
#include <unordered_map>

#include "raft/raft_interface.h"

namespace raftkv {

// How long sessions are remembered. Both limits count LOG ENTRIES, not time: every replica applies
// the same entries in the same order, so every replica forgets the same sessions at the same entry,
// whether it replayed the log or restored a snapshot. They must be the same on every node.
struct SessionLimits {
  size_t max_sessions = 100000;     // beyond this, the least recently active session is forgotten
  uint64_t idle_entries = 1000000;  // a session not used for this many log entries is forgotten
};

enum class SessionDecision {
  kApply,           // a new request: apply it
  kDuplicate,       // seen before (same or older seq): do not apply
  kSessionExpired,  // the client's session may have been forgotten: do not apply (see below)
};

// Remembers, for every client, the highest request number already applied, so that a request
// the client retried (same client_id and seq) takes effect only once.
//
// Assumptions (the usual Raft client model, Raft paper section 8): a client has one request in
// flight at a time and numbers its requests upwards (1, 2, 3, ...). A request whose seq is not
// above the last applied one for its client is a duplicate.
//
// This table is part of the replicated state machine. It must be updated only in log-apply
// order, so that every replica makes the same decisions; it is saved inside snapshots.
//
// Sessions are forgotten (SessionLimits), or the table would grow forever. Forgetting is the
// dangerous part: a retry of a request that was applied before its session was forgotten would
// look new. So once ANY session has been forgotten, a request from an unknown client is applied
// only if it is that client's first request (seq 1); otherwise it might be such a retry, and it is
// refused with kSessionExpired (the client then starts a new session). Until the first session is
// forgotten nothing can be such a retry, and unknown clients are accepted with any seq, as before.
class ClientSessionTable {
 public:
  explicit ClientSessionTable(SessionLimits limits = SessionLimits{}) : limits_(limits) {}

  void SetLimits(SessionLimits limits) {
    std::lock_guard<std::mutex> lk(mu_);
    limits_ = limits;
  }

  // Decides what to do with `cmd`, the command of the log entry at `index`, and records it. Must
  // be called in log order. Commands without a request id (client_id == 0 or seq == 0) are always
  // applied.
  SessionDecision Decide(const Command& cmd, LogIndex index) {
    std::lock_guard<std::mutex> lk(mu_);
    ExpireIdleLocked(index);
    if (cmd.client_id == 0 || cmd.seq == 0) return SessionDecision::kApply;
    auto it = sessions_.find(cmd.client_id);
    if (it == sessions_.end()) {
      if (forgotten_any_ && cmd.seq != 1) return SessionDecision::kSessionExpired;
      sessions_[cmd.client_id] = Session{cmd.seq, index};
      by_activity_.insert({index, cmd.client_id});
      EvictForCapacityLocked();
      return SessionDecision::kApply;
    }
    Session& s = it->second;
    by_activity_.erase({s.last_index, cmd.client_id});  // a retry is activity too
    s.last_index = index;
    by_activity_.insert({index, cmd.client_id});
    if (cmd.seq <= s.last_seq) return SessionDecision::kDuplicate;
    s.last_seq = cmd.seq;
    return SessionDecision::kApply;
  }

  // Older interface, kept for tests that do not track log indexes: each call counts as the next entry.
  bool ShouldApply(const Command& cmd) {
    LogIndex index;
    {
      std::lock_guard<std::mutex> lk(mu_);
      index = ++implicit_index_;
    }
    return Decide(cmd, index) == SessionDecision::kApply;
  }

  // The table as bytes, for snapshots. Every replica produces identical bytes (sorted by client id):
  //   "RKVSESS2" | u8 forgotten_any | u64 count | count x (u64 client_id, u64 last_seq, u64 last_index)
  // Restore also accepts the first format ([u64 count] then (client_id, last_seq) pairs), whose
  // sessions get last_index 0. Restore replaces the whole table, or returns false and changes nothing.
  std::string Serialize() const {
    std::lock_guard<std::mutex> lk(mu_);
    std::vector<std::pair<uint64_t, Session>> v(sessions_.begin(), sessions_.end());
    std::sort(v.begin(), v.end(), [](const auto& a, const auto& b) { return a.first < b.first; });
    std::string out = kMagic;
    out.push_back(forgotten_any_ ? 1 : 0);
    PutU64(&out, v.size());
    for (const auto& p : v) {
      PutU64(&out, p.first);
      PutU64(&out, p.second.last_seq);
      PutU64(&out, p.second.last_index);
    }
    return out;
  }
  bool Restore(const std::string& bytes) {
    std::unordered_map<uint64_t, Session> fresh;
    bool forgotten = false;
    if (bytes.size() >= 8 && bytes.compare(0, 8, kMagic) == 0) {
      if (bytes.size() < 17) return false;
      const unsigned char flag = static_cast<unsigned char>(bytes[8]);
      if (flag > 1) return false;
      forgotten = flag == 1;
      const uint64_t count = GetU64(bytes.data() + 9);
      if (count > (bytes.size() - 17) / 24 || bytes.size() != 17 + count * 24) return false;
      for (uint64_t i = 0; i < count; ++i) {
        const char* p = bytes.data() + 17 + i * 24;
        if (!fresh.emplace(GetU64(p), Session{GetU64(p + 8), GetU64(p + 16)}).second) return false;
      }
    } else {
      if (bytes.size() < 8) return false;
      const uint64_t count = GetU64(bytes.data());
      if (count > (bytes.size() - 8) / 16 || bytes.size() != 8 + count * 16) return false;
      for (uint64_t i = 0; i < count; ++i) {
        const char* p = bytes.data() + 8 + i * 16;
        if (!fresh.emplace(GetU64(p), Session{GetU64(p + 8), 0}).second) return false;
      }
    }
    std::lock_guard<std::mutex> lk(mu_);
    sessions_ = std::move(fresh);
    by_activity_.clear();
    for (const auto& kv : sessions_) by_activity_.insert({kv.second.last_index, kv.first});
    forgotten_any_ = forgotten;
    return true;
  }

  uint64_t LastSeq(uint64_t client_id) const {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = sessions_.find(client_id);
    return it == sessions_.end() ? 0 : it->second.last_seq;
  }
  size_t ClientCount() const {
    std::lock_guard<std::mutex> lk(mu_);
    return sessions_.size();
  }
  // Whether any session has ever been forgotten (part of the replicated state; see above).
  bool ForgottenAny() const {
    std::lock_guard<std::mutex> lk(mu_);
    return forgotten_any_;
  }

 private:
  struct Session {
    uint64_t last_seq = 0;    // highest seq applied for this client
    uint64_t last_index = 0;  // log index of this client's most recent command
  };
  static constexpr const char* kMagic = "RKVSESS2";

  void ForgetLocked(std::set<std::pair<uint64_t, uint64_t>>::iterator it) {
    sessions_.erase(it->second);
    by_activity_.erase(it);
    forgotten_any_ = true;
  }
  void ExpireIdleLocked(LogIndex index) {
    while (!by_activity_.empty() && index > by_activity_.begin()->first &&
           index - by_activity_.begin()->first > limits_.idle_entries) {
      ForgetLocked(by_activity_.begin());
    }
  }
  void EvictForCapacityLocked() {
    while (sessions_.size() > limits_.max_sessions && !by_activity_.empty()) ForgetLocked(by_activity_.begin());
  }
  static void PutU64(std::string* s, uint64_t v) {
    for (int i = 0; i < 8; ++i) s->push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
  }
  static uint64_t GetU64(const char* p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v |= static_cast<uint64_t>(static_cast<uint8_t>(p[i])) << (8 * i);
    return v;
  }

  mutable std::mutex mu_;
  SessionLimits limits_;
  std::unordered_map<uint64_t, Session> sessions_;
  std::set<std::pair<uint64_t, uint64_t>> by_activity_;  // (last_index, client_id), oldest first
  bool forgotten_any_ = false;
  LogIndex implicit_index_ = 0;  // only for ShouldApply
};

}  // namespace raftkv
