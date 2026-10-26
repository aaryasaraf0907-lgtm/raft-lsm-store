#include "storage/kv_engine.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <system_error>

namespace raftkv {
namespace {

// Parses "<prefix><digits><suffix>"; false for anything else (temp files, etc).
bool ParseName(const std::string& name, const std::string& prefix, const std::string& suffix, uint64_t* out) {
  if (name.size() <= prefix.size() + suffix.size()) return false;
  if (name.compare(0, prefix.size(), prefix) != 0) return false;
  if (name.compare(name.size() - suffix.size(), suffix.size(), suffix) != 0) return false;
  const std::string digits = name.substr(prefix.size(), name.size() - prefix.size() - suffix.size());
  if (digits.size() > 18) return false;
  uint64_t v = 0;
  for (char c : digits) {
    if (c < '0' || c > '9') return false;
    v = v * 10 + static_cast<uint64_t>(c - '0');
  }
  *out = v;
  return true;
}

// The engine keeps the applied Raft index as an ordinary key under the reserved prefix, so flush,
// compaction and recovery need no special cases for it.
std::string AppliedIndexKey() {
  return std::string(kReservedKeyPrefix, sizeof(kReservedKeyPrefix) - 1) + "applied_index";
}

std::string EncodeIndex(uint64_t index) {
  std::string out(8, '\0');
  for (int i = 0; i < 8; ++i) out[static_cast<size_t>(i)] = static_cast<char>((index >> (8 * i)) & 0xFF);
  return out;
}

bool DecodeIndex(const std::string& bytes, uint64_t* index) {
  if (bytes.size() != 8) return false;
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) v |= static_cast<uint64_t>(static_cast<uint8_t>(bytes[static_cast<size_t>(i)])) << (8 * i);
  *index = v;
  return true;
}

}  // namespace

std::string KVEngine::WalFileName(uint64_t seq) {
  char buf[40];
  std::snprintf(buf, sizeof(buf), "wal-%06llu.log", static_cast<unsigned long long>(seq));
  return buf;
}

std::string KVEngine::SstFileName(uint64_t seq) {
  char buf[40];
  std::snprintf(buf, sizeof(buf), "%06llu.sst", static_cast<unsigned long long>(seq));
  return buf;
}

std::string KVEngine::Path(const std::string& name) const {
  return (std::filesystem::path(options_.dir) / name).string();
}

bool KVEngine::SyncDir() const {  // make file creations/removals in the directory durable
  int fd = ::open(options_.dir.c_str(), O_RDONLY);
  if (fd < 0) return false;
  const bool ok = ::fsync(fd) == 0;
  ::close(fd);
  return ok;
}

Status KVEngine::Open(const Options& options, std::unique_ptr<KVEngine>* out) {
  std::error_code ec;
  std::filesystem::create_directories(options.dir, ec);
  if (ec || !std::filesystem::is_directory(options.dir, ec)) return Status::kIOError;

  std::unique_ptr<KVEngine> engine(new KVEngine(options));
  Status s = engine->Recover();
  if (s != Status::kOk) return s;
  engine->PublishView();
  {
    std::string stored;  // the applied index that survived in the WAL (0 if there is none)
    uint64_t index = 0;
    if (engine->Get(AppliedIndexKey(), &stored) == Status::kOk && DecodeIndex(stored, &index)) {
      engine->applied_index_.store(index);
    }
  }
  if (options.flush_writer) engine->flusher_ = std::thread([e = engine.get()] { e->FlushLoop(); });
  *out = std::move(engine);
  return Status::kOk;
}

Status KVEngine::Recover() {
  std::error_code ec;
  std::vector<uint64_t> wal_seqs;
  uint64_t max_seq = 0;
  for (const auto& de : std::filesystem::directory_iterator(options_.dir, ec)) {
    const std::string name = de.path().filename().string();
    uint64_t n = 0;
    if (ParseName(name, "wal-", ".log", &n)) {
      wal_seqs.push_back(n);
      max_seq = std::max(max_seq, n);
    } else if (ParseName(name, "", ".sst", &n)) {
      max_seq = std::max(max_seq, n);  // sequence numbers are never reused
    }
  }
  if (ec) return Status::kIOError;
  std::sort(wal_seqs.begin(), wal_seqs.end());
  next_seq_ = max_seq + 1;

  struct Pending {
    uint64_t seq;
    std::shared_ptr<MemTable> mem;
  };
  std::vector<Pending> pending;  // unflushed WALs, oldest first
  for (uint64_t seq : wal_seqs) {
    const std::string wal_path = Path(WalFileName(seq));
    if (std::filesystem::exists(Path(SstFileName(seq)), ec)) {
      std::filesystem::remove(wal_path, ec);  // already flushed: retire, do not replay
      continue;
    }
    auto mem = std::make_shared<MemTable>(options_.memtable_bytes);
    ReplayStats stats;
    Status s = WAL::Replay(
        wal_path,
        [&](const Entry& e) {
          if (e.tombstone) {
            mem->Delete(e.key);
          } else {
            mem->Put(e.key, e.value);
          }
        },
        &stats);
    if (s != Status::kOk) return s;
    recovered_records_ += stats.records;
    pending.push_back({seq, std::move(mem)});
  }

  if (pending.empty()) {
    active_seq_ = next_seq_++;
    active_ = std::make_shared<MemTable>(options_.memtable_bytes);
  } else {  // the newest unflushed WAL keeps receiving writes
    active_seq_ = pending.back().seq;
    active_ = pending.back().mem;
    pending.pop_back();
    for (auto& p : pending) {  // older ones wait for their flush
      if (p.mem->Empty()) {
        std::filesystem::remove(Path(WalFileName(p.seq)), ec);
        continue;
      }
      frozen_.push_front({p.seq, p.mem});  // oldest pushed first => newest ends up in front
    }
  }
  Status s = WAL::Open(Path(WalFileName(active_seq_)), &wal_);
  if (s != Status::kOk) return s;
  if (!SyncDir()) return Status::kIOError;
  return Status::kOk;
}

KVEngine::~KVEngine() {
  {
    std::lock_guard<std::mutex> lock(write_mu_);
    stop_ = true;
  }
  cv_.notify_all();
  if (flusher_.joinable()) flusher_.join();
}

void KVEngine::PublishView() {
  auto v = std::make_shared<View>();
  v->active = active_;
  for (const Frozen& f : frozen_) v->frozen.push_back(f.mem);
  std::lock_guard<std::mutex> guard(view_mu_);
  view_ = std::move(v);
}

KVEngine::Stats KVEngine::GetStats() const {
  std::lock_guard<std::mutex> lock(write_mu_);
  Stats s;
  s.active_seq = active_seq_;
  s.frozen_memtables = frozen_.size();
  s.flushes_done = flushes_done_;
  s.flush_failures = flush_failures_;
  s.flush_failing = flush_failing_;
  return s;
}

Status KVEngine::Put(const std::string& key, const std::string& value) {
  if (HasReservedKeyPrefix(key)) return Status::kInvalidArgument;
  return Write(false, key, value, 0);
}

Status KVEngine::Delete(const std::string& key) {
  if (HasReservedKeyPrefix(key)) return Status::kInvalidArgument;
  return Write(true, key, std::string(), 0);
}

Status KVEngine::PutAt(const std::string& key, const std::string& value, uint64_t raft_index) {
  if (HasReservedKeyPrefix(key) || raft_index == 0) return Status::kInvalidArgument;
  return Write(false, key, value, raft_index);
}

Status KVEngine::DeleteAt(const std::string& key, uint64_t raft_index) {
  if (HasReservedKeyPrefix(key) || raft_index == 0) return Status::kInvalidArgument;
  return Write(true, key, std::string(), raft_index);
}

Status KVEngine::Write(bool tombstone, const std::string& key, const std::string& value,
                       uint64_t raft_index) {
  std::unique_lock<std::mutex> lock(write_mu_);
  const bool with_index = raft_index != 0;
  // The Raft state machine applies in increasing index order; anything else is a caller bug.
  if (with_index && raft_index <= applied_index_.load()) return Status::kInvalidArgument;
  Status s = EnsureRoom(lock);  // data and index record must land in the same WAL and memtable
  if (s != Status::kOk) return s;
  // Durable first, visible second: never expose a write that could be lost. With an index, the
  // data record is appended first, then the index record, and ONE sync makes both durable, so after
  // a crash the index can only be missing (lagging by this operation), never ahead of the data.
  const bool sync_data = options_.sync_wal && !with_index;
  s = tombstone ? wal_->AppendDelete(key, sync_data) : wal_->AppendPut(key, value, sync_data);
  if (s != Status::kOk) return Status::kIOError;
  std::string index_bytes;
  if (with_index) {
    index_bytes = EncodeIndex(raft_index);
    s = wal_->AppendPut(AppliedIndexKey(), index_bytes, options_.sync_wal);
    if (s != Status::kOk) return Status::kIOError;
  }
  if (tombstone) {
    active_->Delete(key);
  } else {
    active_->Put(key, value);
  }
  if (with_index) {
    active_->Put(AppliedIndexKey(), index_bytes);
    applied_index_.store(raft_index);
  }
  return Status::kOk;
}

// Called before every write. Freezes a full memtable; if too many frozen ones are
// already waiting, blocks until one is flushed, or fails fast if flushing is broken.
Status KVEngine::EnsureRoom(std::unique_lock<std::mutex>& lock) {
  for (;;) {
    if (!options_.flush_writer || !active_->ShouldFlush()) return Status::kOk;
    if (frozen_.size() < options_.max_frozen) return Freeze();
    if (flush_failing_ || stop_) return Status::kIOError;
    cv_.wait(lock);
  }
}

Status KVEngine::Freeze() {
  if (wal_->Sync() != Status::kOk) return Status::kIOError;  // old WAL fully durable first
  const uint64_t new_seq = next_seq_;
  std::unique_ptr<WAL> new_wal;
  if (WAL::Open(Path(WalFileName(new_seq)), &new_wal) != Status::kOk) return Status::kIOError;
  if (!SyncDir()) {
    new_wal.reset();
    std::error_code ec;
    std::filesystem::remove(Path(WalFileName(new_seq)), ec);
    return Status::kIOError;
  }
  frozen_.push_front({active_seq_, active_});
  active_ = std::make_shared<MemTable>(options_.memtable_bytes);
  active_seq_ = new_seq;
  ++next_seq_;
  wal_ = std::move(new_wal);
  PublishView();
  cv_.notify_all();
  return Status::kOk;
}

Status KVEngine::Get(const std::string& key, std::string* value) {
  std::shared_ptr<const View> view;
  {
    std::lock_guard<std::mutex> guard(view_mu_);
    view = view_;
  }
  auto probe = [&](const MemTable& m, Status* result) {
    switch (m.Get(key, value)) {
      case MemTable::GetResult::kFound:
        *result = Status::kOk;
        return true;
      case MemTable::GetResult::kDeleted:  // a tombstone hides every older version
        *result = Status::kNotFound;
        return true;
      case MemTable::GetResult::kNotFound:
        break;
    }
    return false;
  };
  Status result = Status::kNotFound;
  if (probe(*view->active, &result)) return result;
  for (const auto& f : view->frozen) {
    if (probe(*f, &result)) return result;
  }
  return Status::kNotFound;  // the SSTable lookup goes here (next PR)
}

void KVEngine::FlushLoop() {
  std::unique_lock<std::mutex> lock(write_mu_);
  for (;;) {
    cv_.wait(lock, [&] { return stop_ || !frozen_.empty(); });
    if (stop_) return;
    const Frozen f = frozen_.back();  // oldest first, so SSTables appear in sequence order
    lock.unlock();

    auto it = f.mem->NewIterator();
    Status s = options_.flush_writer(it, Path(SstFileName(f.seq)), f.seq);
    if (s == Status::kOk) {  // published and durable: the WAL is no longer needed
      std::error_code ec;
      std::filesystem::remove(Path(WalFileName(f.seq)), ec);  // if this fails, startup retires it
    }

    lock.lock();
    if (s == Status::kOk) {
      frozen_.pop_back();
      ++flushes_done_;
      flush_failing_ = false;
      PublishView();
      cv_.notify_all();
    } else {  // keep the memtable and its WAL; try again shortly
      ++flush_failures_;
      flush_failing_ = true;
      cv_.notify_all();  // wake writers waiting for room so they can fail fast
      cv_.wait_for(lock, std::chrono::milliseconds(options_.flush_retry_ms), [&] { return stop_; });
    }
  }
}

}  // namespace raftkv