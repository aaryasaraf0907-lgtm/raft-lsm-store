#pragma once
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <mutex>

#include "raft/client_sessions.h"
#include "raft/raft_interface.h"
#include "storage/checkpoint.h"
#include "storage/kv_store.h"

namespace raftkv {

// The state machine behind Raft: turns committed log entries into KVStore operations, applying
// each client request at most once.
//
// Wire it up as the node's apply callback:
//   KVStateMachine machine(&store);
//   node.SetApplyCallback(machine.AsCallback());
//
// Raft calls it from one thread, in log order, so every replica takes the same decisions.
// After a restart the whole committed log is replayed through it, which also rebuilds the
// duplicate table. Put and Delete are idempotent for the same entries in the same order.
//
// Replay after a restart. If the store is an IndexedKVStore (the real engine is), the state machine
// writes every entry through PutAt/DeleteAt so the store records the log index it has applied. When
// the node restarts and the log is replayed from the beginning, entries at or below the index the
// store reports at startup are already in the store: their client-session effects are still
// replayed (the table has to be rebuilt) but their store writes are skipped. With a plain KVStore
// every replayed entry is written again, which is harmless (the writes are idempotent) but slow.
//
// A committed entry that the store cannot apply must not be skipped (replicas would diverge),
// so a storage failure is fatal.
class KVStateMachine {
 public:
  enum class RequestOutcome { kApplied, kDuplicate, kUnknown, kSessionExpired };

  explicit KVStateMachine(KVStore* store) : store_(store) {}
  // Preferred for an indexed store; overload resolution picks it automatically for a KVEngine.
  explicit KVStateMachine(IndexedKVStore* store)
      : store_(store), indexed_(store), replay_floor_(store->AppliedIndex()) {}
  // A store that can also checkpoint (the real engine): enables TakeSnapshot / RestoreSnapshot.
  explicit KVStateMachine(CheckpointableStore* store)
      : store_(store), indexed_(store), replay_floor_(store->AppliedIndex()), checkpointable_(store) {}

  void Apply(const LogEntry& entry) {
    const Command& cmd = entry.command;
    const SessionDecision decision = sessions_.Decide(cmd, entry.index);
    if (decision == SessionDecision::kDuplicate) {
      ++duplicates_skipped_;
      RecordRequestOutcome(entry.index, RequestOutcome::kDuplicate);
      return;
    }
    if (decision == SessionDecision::kSessionExpired) {  // might be a retry of a forgotten request
      ++sessions_expired_;
      RecordRequestOutcome(entry.index, RequestOutcome::kSessionExpired);
      return;
    }
    switch (cmd.type) {
      case CommandType::kPut:
        if (indexed_ == nullptr) {
          MustSucceed(store_->Put(cmd.key, cmd.value) == Status::kOk, "put");
        } else if (entry.index > replay_floor_) {
          MustSucceed(indexed_->PutAt(cmd.key, cmd.value, entry.index) == Status::kOk, "put");
        } else {
          ++replay_skipped_;  // already in the store from before the restart
        }
        ++applied_;
        RecordRequestOutcome(entry.index, RequestOutcome::kApplied);
        break;
      case CommandType::kDelete: {
        if (indexed_ == nullptr) {
          Status s = store_->Delete(cmd.key);  // deleting a missing key is not an error
          MustSucceed(s == Status::kOk || s == Status::kNotFound, "delete");
        } else if (entry.index > replay_floor_) {
          Status s = indexed_->DeleteAt(cmd.key, entry.index);
          MustSucceed(s == Status::kOk || s == Status::kNotFound, "delete");
        } else {
          ++replay_skipped_;
        }
        ++applied_;
        RecordRequestOutcome(entry.index, RequestOutcome::kApplied);
        break;
      }
      case CommandType::kNoop:
      case CommandType::kConfig:  // never delivered by the Raft node; nothing to do
        break;
    }
  }

  ApplyCallback AsCallback() {
    return [this](const LogEntry& entry) { Apply(entry); };
  }

  uint64_t Applied() const { return applied_.load(); }
  // ---- Raft snapshots (RaftNodeImpl::SetSnapshotCallbacks) ----
  // A snapshot is the store's whole dataset plus the client-session table (without it a node
  // restored from a snapshot would apply retried writes a second time):
  //   "RKVSNAP1" | u64 store_applied_index | u64 sessions_length | sessions | u64 data_length | data
  // Both are called on the Raft apply thread, between applies, so they see a state in which exactly
  // the entries up to the snapshot index have been applied. Failure is fatal, like any apply failure.
  std::string TakeSnapshot() {
    MustSucceed(checkpointable_ != nullptr, "this store cannot snapshot");
    std::string data;
    uint64_t index = 0;
    MustSucceed(checkpointable_->Checkpoint(&data, &index) == Status::kOk, "checkpoint");
    const std::string sessions = sessions_.Serialize();
    std::string out = "RKVSNAP1";
    PutU64(&out, index);
    PutU64(&out, sessions.size());
    out += sessions;
    PutU64(&out, data.size());
    out += data;
    return out;
  }

  // Makes this state machine at least as new as the snapshot. The session table is always replaced.
  // The store is only replaced if it is BEHIND the snapshot: a node restarting from its own
  // snapshot already holds that state in its engine (and usually more), and must not rewrite its
  // whole dataset on every start.
  void RestoreSnapshot(const std::string& bytes) {
    MustSucceed(checkpointable_ != nullptr, "this store cannot restore a snapshot");
    size_t pos = 8;
    MustSucceed(bytes.size() >= 8 && bytes.compare(0, 8, "RKVSNAP1") == 0, "snapshot header");
    auto get64 = [&]() {
      MustSucceed(bytes.size() - pos >= 8, "snapshot truncated");
      uint64_t v = 0;
      for (int i = 0; i < 8; ++i) v |= static_cast<uint64_t>(static_cast<uint8_t>(bytes[pos + static_cast<size_t>(i)])) << (8 * i);
      pos += 8;
      return v;
    };
    const uint64_t index = get64();
    const uint64_t sessions_len = get64();
    MustSucceed(bytes.size() - pos >= sessions_len, "snapshot sessions truncated");
    const std::string sessions = bytes.substr(pos, static_cast<size_t>(sessions_len));
    pos += static_cast<size_t>(sessions_len);
    const uint64_t data_len = get64();
    MustSucceed(bytes.size() - pos == data_len, "snapshot data length");
    MustSucceed(sessions_.Restore(sessions), "snapshot sessions");
    if (checkpointable_->AppliedIndex() < index) {
      MustSucceed(checkpointable_->InstallCheckpoint(bytes.substr(pos), index) == Status::kOk, "install checkpoint");
    }
    replay_floor_ = checkpointable_->AppliedIndex();  // entries up to here are in the store already
  }

  uint64_t DuplicatesSkipped() const { return duplicates_skipped_.load(); }
  // Requests refused because their client's session may have been forgotten.
  uint64_t SessionsExpired() const { return sessions_expired_.load(); }
  // Tests only: shorter session limits, to exercise expiry without millions of entries.
  void SetSessionLimits(SessionLimits limits) { sessions_.SetLimits(limits); }
  // Store writes skipped because the store already held them (see the replay note above).
  uint64_t ReplaySkipped() const { return replay_skipped_.load(); }
  // The store's applied index when this state machine was created (0 for a plain store).
  uint64_t ReplayFloor() const { return replay_floor_; }
  const ClientSessionTable& Sessions() const { return sessions_; }

  // Returns the result for an applied log index, if it is still in the bounded history.
  // A missing result must be treated as ambiguous, never as proof of a successful write.
  RequestOutcome LookupRequestOutcome(LogIndex index) const {
    std::lock_guard<std::mutex> lock(outcomes_mutex_);
    const auto found = request_outcomes_.find(index);
    return found == request_outcomes_.end() ? RequestOutcome::kUnknown
                                            : found->second;
  }

 private:
  static constexpr std::size_t kRequestOutcomeHistoryLimit = 4096;

  void RecordRequestOutcome(LogIndex index, RequestOutcome outcome) {
    std::lock_guard<std::mutex> lock(outcomes_mutex_);
    request_outcomes_[index] = outcome;
    while (request_outcomes_.size() > kRequestOutcomeHistoryLimit) {
      request_outcomes_.erase(request_outcomes_.begin());
    }
  }

  static void MustSucceed(bool ok, const char* what) {
    if (ok) return;
    std::fprintf(stderr, "kv state machine: fatal: could not apply a committed %s\n", what);
    std::abort();
  }

  KVStore* store_;
  IndexedKVStore* indexed_ = nullptr;  // store_ again, if it is indexed
  uint64_t replay_floor_ = 0;
  CheckpointableStore* checkpointable_ = nullptr;
  ClientSessionTable sessions_;
  mutable std::mutex outcomes_mutex_;
  std::map<LogIndex, RequestOutcome> request_outcomes_;
  std::atomic<uint64_t> applied_{0};
  std::atomic<uint64_t> duplicates_skipped_{0};
  std::atomic<uint64_t> sessions_expired_{0};
  std::atomic<uint64_t> replay_skipped_{0};

  static void PutU64(std::string* s, uint64_t v) {
    for (int i = 0; i < 8; ++i) s->push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
  }
};

}  // namespace raftkv
