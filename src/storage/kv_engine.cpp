#include "storage/kv_engine.h"

#include "storage/compaction.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <queue>
#include <set>
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
  if (options.use_sstables && options.flush_writer) return Status::kInvalidArgument;  // one flush path only
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
  if (options.flush_writer || options.use_sstables) engine->flusher_ = std::thread([e = engine.get()] { e->FlushLoop(); });
  if (options.use_sstables && options.compaction_trigger > 0) {
    engine->compactor_ = std::thread([e = engine.get()] { e->CompactLoop(); });
  }
  *out = std::move(engine);
  return Status::kOk;
}

bool KVEngine::CrashPoint(const char* point) {
  if (options_.test_crash_hook && options_.test_crash_hook(point)) {
    dead_ = true;
    cv_.notify_all();
    return true;
  }
  return false;
}

// SSTable mode, first step of recovery. The manifest is the truth about which SSTables are live.
// Anything else in the directory that looks like ours is the leftover of an operation that never
// committed (a flush or compaction output whose manifest edit did not happen, or an input that a
// finished compaction had not yet deleted) and is removed. SSTables without any manifest are
// refused rather than guessed about.
Status KVEngine::LoadLsmState() {
  std::error_code ec;
  bool found = false;
  Status s = LoadManifest(options_.dir, &manifest_, &found);
  if (s != Status::kOk) return s;
  std::set<uint64_t> live;
  for (const SSTableMeta& f : manifest_.files) live.insert(f.file_number);
  std::vector<std::filesystem::path> doomed;
  std::vector<uint64_t> sst_numbers;
  bool any_sst = false;
  for (const auto& de : std::filesystem::directory_iterator(options_.dir, ec)) {
    const std::string name = de.path().filename().string();
    uint64_t n = 0;
    if (name.find(".tmp") != std::string::npos) {
      doomed.push_back(de.path());  // a temp file of a write that never finished
    } else if (ParseName(name, "", ".sst", &n)) {
      any_sst = true;
      sst_numbers.push_back(n);
      lsm_highest_number_ = std::max(lsm_highest_number_, n);
      if (live.count(n) == 0) doomed.push_back(de.path());
    }
  }
  if (ec) return Status::kIOError;
  if (!found && any_sst) {
    // No manifest means no flush ever committed (the manifest edit comes BEFORE a WAL is retired),
    // so every SSTable here is an orphan of the very first flush and its WAL must still exist; the
    // SSTable is redundant. If some SSTable's WAL is gone, the data lives only in that SSTable and
    // the manifest has been lost: refuse rather than delete or guess.
    for (uint64_t n : sst_numbers) {
      if (!std::filesystem::exists(Path(WalFileName(n)), ec)) return Status::kCorruption;
    }
  }
  for (const auto& p : doomed) std::filesystem::remove(p, ec);
  for (const SSTableMeta& f : manifest_.files) {
    std::unique_ptr<SSTable> t;
    s = SSTable::Open(Path(SstFileName(f.file_number)), f.file_number, &t);
    if (s == Status::kIOError) return Status::kCorruption;  // a live file is missing or unreadable
    if (s != Status::kOk) return s;
    if (t->meta().min_seq != f.min_seq || t->meta().max_seq != f.max_seq) return Status::kCorruption;
    sstables_.push_back(std::shared_ptr<SSTable>(std::move(t)));
  }
  return Status::kOk;
}

Status KVEngine::Recover() {
  std::error_code ec;
  if (options_.use_sstables) {
    Status s = LoadLsmState();
    if (s != Status::kOk) return s;
  }
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
  next_seq_ = std::max({max_seq, lsm_highest_number_, manifest_.next_file_number - 1}) + 1;  // never reuse a number

  struct Pending {
    uint64_t seq;
    std::shared_ptr<MemTable> mem;
  };
  std::vector<Pending> pending;  // unflushed WALs, oldest first
  for (uint64_t seq : wal_seqs) {
    const std::string wal_path = Path(WalFileName(seq));
    // Already flushed: retire the WAL, do not replay it. SSTable mode asks the manifest; the
    // original mode asks whether <seq>.sst exists.
    const bool flushed = options_.use_sstables ? seq <= manifest_.flushed_through
                                               : std::filesystem::exists(Path(SstFileName(seq)), ec);
    if (flushed) {
      std::filesystem::remove(wal_path, ec);
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
  if (compactor_.joinable()) compactor_.join();
}

void KVEngine::PublishView() {
  auto v = std::make_shared<View>();
  v->active = active_;
  for (const Frozen& f : frozen_) v->frozen.push_back(f.mem);
  for (auto it = sstables_.rbegin(); it != sstables_.rend(); ++it) v->sstables.push_back(*it);
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
  s.sstables = sstables_.size();
  s.compactions_done = compactions_done_;
  s.compaction_failures = compaction_failures_;
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
    if (dead_) return Status::kIOError;
    if (!(options_.flush_writer || options_.use_sstables) || !active_->ShouldFlush()) return Status::kOk;
    if (frozen_.size() < options_.max_frozen) return Freeze();
    if (flush_failing_ || stop_) return Status::kIOError;
    cv_.wait(lock);
  }
}

namespace {

class VectorIterator : public KVIterator {
 public:
  explicit VectorIterator(std::vector<Entry> v) : v_(std::move(v)) {}
  bool Valid() const override { return i_ < v_.size(); }
  void Next() override { ++i_; }
  const Entry& Current() const override { return v_[i_]; }

 private:
  std::vector<Entry> v_;
  size_t i_ = 0;
};

}  // namespace

// Merges every memtable and SSTable, newest version of each key wins, tombstones and the engine's
// own bookkeeping keys are left out. The active memtable is copied under the write lock so that
// the copy and the applied index describe the same instant; everything else is immutable.
Status KVEngine::Checkpoint(std::string* data, uint64_t* applied_index) {
  std::vector<Entry> active_copy;
  std::shared_ptr<const View> view;
  uint64_t index = 0;
  {
    std::lock_guard<std::mutex> lock(write_mu_);
    if (dead_) return Status::kIOError;
    {
      std::lock_guard<std::mutex> guard(view_mu_);
      view = view_;
    }
    for (auto it = active_->NewIterator(); it.Valid(); it.Next()) active_copy.push_back(it.Current());
    index = applied_index_.load();
  }
  std::vector<std::unique_ptr<KVIterator>> sources;  // newest first
  sources.push_back(std::make_unique<VectorIterator>(std::move(active_copy)));
  for (const auto& m : view->frozen) sources.push_back(std::make_unique<MemTable::Iterator>(m->NewIterator()));
  for (const auto& t : view->sstables) sources.push_back(t->NewIterator());
  // Only SSTable iterators can fail (a damaged block); a memtable cannot.
  auto status_of = [&](size_t i) {
    auto* sst = dynamic_cast<SSTable::Iterator*>(sources[i].get());
    return sst != nullptr ? sst->status() : Status::kOk;
  };
  for (size_t i = 0; i < sources.size(); ++i) {
    if (!sources[i]->Valid() && status_of(i) != Status::kOk) return status_of(i);
  }
  auto later = [&](size_t x, size_t y) {  // true if x should come out AFTER y
    const std::string& kx = sources[x]->Current().key;
    const std::string& ky = sources[y]->Current().key;
    return kx != ky ? kx > ky : x > y;  // equal keys: the newer source (lower index) first
  };
  std::priority_queue<size_t, std::vector<size_t>, decltype(later)> heap(later);
  for (size_t i = 0; i < sources.size(); ++i) {
    if (sources[i]->Valid()) heap.push(i);
  }
  uint64_t count = 0;
  std::string body;
  while (!heap.empty()) {
    const size_t top = heap.top();
    heap.pop();
    const Entry newest = sources[top]->Current();
    auto advance = [&](size_t i) {
      sources[i]->Next();
      if (sources[i]->Valid()) heap.push(i);
      return status_of(i);
    };
    Status s = advance(top);
    while (s == Status::kOk && !heap.empty() && sources[heap.top()]->Current().key == newest.key) {
      const size_t older = heap.top();
      heap.pop();
      s = advance(older);  // an older version of the same key: discard
    }
    if (s != Status::kOk) return s;
    if (!newest.tombstone && !HasReservedKeyPrefix(newest.key)) {
      AppendCheckpointEntry(&body, newest.key, newest.value);
      ++count;
    }
  }
  *data = CheckpointHeader(count) + body;
  *applied_index = index;
  return Status::kOk;
}

// Replaces the dataset. Order (the manifest edit is the commit point):
//   1. write the new SSTable (all live keys + the applied index) -> an orphan until step 3
//   2. create the new, empty WAL
//   3. store the manifest: ONLY the new file, flushed_through above every old WAL
//   4. switch the in-memory state, then delete the old WALs and SSTables
// A crash before 3: startup deletes the orphan and keeps the old dataset. A crash after 3: startup
// sees the new manifest, retires every old WAL (<= flushed_through) and deletes the old SSTables
// (not listed). Flushes and compactions that were running notice the epoch change and discard
// their results instead of installing them into the new dataset.
Status KVEngine::InstallCheckpoint(const std::string& data, uint64_t applied_index) {
  if (!options_.use_sstables) return Status::kInvalidArgument;
  std::vector<std::pair<std::string, std::string>> pairs;
  if (!ParseCheckpoint(data, &pairs)) return Status::kCorruption;
  std::vector<Entry> entries;
  entries.reserve(pairs.size() + 1);
  const std::string index_key = AppliedIndexKey();
  bool index_placed = false;
  for (auto& p : pairs) {
    if (!index_placed && index_key < p.first) {
      entries.push_back(Entry{index_key, EncodeIndex(applied_index), false});
      index_placed = true;
    }
    entries.push_back(Entry{std::move(p.first), std::move(p.second), false});
  }
  if (!index_placed) entries.push_back(Entry{index_key, EncodeIndex(applied_index), false});

  std::unique_lock<std::mutex> lock(write_mu_);
  if (dead_ || stop_) return Status::kIOError;
  const uint64_t number = next_seq_++;
  const SSTableMeta meta{number, number, number};
  SSTableBuilderOptions bo;
  bo.bits_per_key = options_.bloom_bits_per_key;
  std::error_code ec;
  std::filesystem::remove(Path(SstFileName(number)), ec);
  VectorIterator source(std::move(entries));
  Status s = WriteSSTable(Path(SstFileName(number)), meta, bo, &source);
  std::unique_ptr<SSTable> opened;
  if (s == Status::kOk) s = SSTable::Open(Path(SstFileName(number)), number, &opened);
  if (s != Status::kOk) {
    std::filesystem::remove(Path(SstFileName(number)), ec);
    return s;
  }
  if (CrashPoint("install:after_sst")) return Status::kIOError;
  const uint64_t wal_seq = next_seq_++;
  std::unique_ptr<WAL> new_wal;
  if (WAL::Open(Path(WalFileName(wal_seq)), &new_wal) != Status::kOk || !SyncDir()) {
    new_wal.reset();
    std::filesystem::remove(Path(WalFileName(wal_seq)), ec);
    std::filesystem::remove(Path(SstFileName(number)), ec);
    return Status::kIOError;
  }
  ManifestState next;
  next.files = {meta};
  next.flushed_through = number;  // every old WAL has a smaller number
  next.next_file_number = next_seq_;
  s = StoreManifest(options_.dir, next);
  if (s != Status::kOk) {
    new_wal.reset();
    std::filesystem::remove(Path(WalFileName(wal_seq)), ec);
    std::filesystem::remove(Path(SstFileName(number)), ec);
    return s;
  }
  if (CrashPoint("install:after_manifest")) return Status::kIOError;

  std::vector<std::string> stale;
  stale.push_back(Path(WalFileName(active_seq_)));
  for (const Frozen& f : frozen_) stale.push_back(Path(WalFileName(f.seq)));
  for (const auto& t : sstables_) stale.push_back(Path(SstFileName(t->meta().file_number)));
  manifest_ = std::move(next);
  sstables_.clear();
  sstables_.push_back(std::shared_ptr<SSTable>(std::move(opened)));
  frozen_.clear();
  active_ = std::make_shared<MemTable>(options_.memtable_bytes);
  active_seq_ = wal_seq;
  wal_ = std::move(new_wal);
  applied_index_.store(applied_index);
  ++epoch_;
  PublishView();
  cv_.notify_all();
  for (const std::string& path : stale) std::filesystem::remove(path, ec);  // if this fails, startup cleans up
  return Status::kOk;
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
  for (const auto& table : view->sstables) {  // newest SSTable first; a tombstone stops the search
    SSTable::GetResult found = SSTable::GetResult::kAbsent;
    std::string stored;
    Status s = table->Get(key, &found, &stored);
    if (s != Status::kOk) return s;  // a damaged table is an error, never "not found"
    if (found == SSTable::GetResult::kFound) {
      *value = std::move(stored);
      return Status::kOk;
    }
    if (found == SSTable::GetResult::kDeleted) return Status::kNotFound;
  }
  return Status::kNotFound;
}

// Flushes frozen memtables, oldest first, so SSTables appear in sequence order.
//
// SSTable mode, per memtable (the order is what makes a crash at any point safe):
//   1. write <seq>.sst  (published atomically by SSTableBuilder)
//   2. store the manifest with that file added and flushed_through = seq     <- the commit point
//   3. delete wal-<seq>.log
// A crash before 2 leaves an SSTable the manifest does not know: startup deletes it and replays the
// WAL, which still has everything. A crash between 2 and 3 leaves a WAL at or below flushed_through:
// startup retires it. A memtable and its applied-index record always go into the same SSTable.
void KVEngine::FlushLoop() {
  std::unique_lock<std::mutex> lock(write_mu_);
  for (;;) {
    cv_.wait(lock, [&] { return stop_ || dead_ || !frozen_.empty(); });
    if (stop_ || dead_) return;
    const Frozen f = frozen_.back();  // oldest first, so SSTables appear in sequence order
    const uint64_t epoch = epoch_;
    lock.unlock();

    Status s = Status::kOk;
    std::shared_ptr<SSTable> table;
    const SSTableMeta meta{f.seq, f.seq, f.seq};
    if (options_.use_sstables) {
      std::error_code ec;
      std::filesystem::remove(Path(SstFileName(f.seq)), ec);  // an orphan of an earlier failed attempt
      auto it = f.mem->NewIterator();
      SSTableBuilderOptions bo;
      bo.bits_per_key = options_.bloom_bits_per_key;
      s = WriteSSTable(Path(SstFileName(f.seq)), meta, bo, &it);
      std::unique_ptr<SSTable> opened;
      if (s == Status::kOk) s = SSTable::Open(Path(SstFileName(f.seq)), f.seq, &opened);
      if (s == Status::kOk) table = std::shared_ptr<SSTable>(std::move(opened));
      if (s == Status::kOk && CrashPoint("flush:after_sst")) return;
    } else {
      auto it = f.mem->NewIterator();
      s = options_.flush_writer(it, Path(SstFileName(f.seq)), f.seq);
      if (s == Status::kOk) {  // published and durable: the WAL is no longer needed
        std::error_code ec;
        std::filesystem::remove(Path(WalFileName(f.seq)), ec);  // if this fails, startup retires it
      }
    }

    lock.lock();
    if (options_.use_sstables && epoch_ != epoch) {  // the dataset was replaced while we flushed: ours is stale
      std::error_code ec;
      std::filesystem::remove(Path(SstFileName(f.seq)), ec);
      continue;
    }
    if (options_.use_sstables && s == Status::kOk) {
      ManifestState next = manifest_;
      next.files.push_back(meta);
      next.flushed_through = f.seq;
      next.next_file_number = std::max(next.next_file_number, next_seq_);
      s = StoreManifest(options_.dir, next);
      if (s == Status::kOk) {
        manifest_ = std::move(next);
        sstables_.push_back(table);
        if (CrashPoint("flush:after_manifest")) return;  // WAL not deleted yet
        std::error_code ec;
        std::filesystem::remove(Path(WalFileName(f.seq)), ec);  // if this fails, startup retires it
      } else {
        std::error_code ec;
        std::filesystem::remove(Path(SstFileName(f.seq)), ec);  // never committed
      }
    }
    if (s == Status::kOk) {
      frozen_.pop_back();
      ++flushes_done_;
      flush_failing_ = false;
      PublishView();  // the memtable disappears and its SSTable appears in one step
      cv_.notify_all();
    } else {  // keep the memtable and its WAL; try again shortly
      ++flush_failures_;
      flush_failing_ = true;
      cv_.notify_all();  // wake writers waiting for room so they can fail fast
      cv_.wait_for(lock, std::chrono::milliseconds(options_.flush_retry_ms), [&] { return stop_ || dead_; });
    }
  }
}

// Merges the live SSTables into one when there are too many (SSTable mode).
//   1. Compact() writes the merged output (published atomically); inputs are untouched
//   2. store the manifest with the inputs removed and the output added   <- the commit point
//   3. delete the input files (readers that already hold them keep reading them)
// A crash before 2 leaves an output the manifest does not know (startup deletes it); a crash between
// 2 and 3 leaves input files the manifest no longer lists (startup deletes them). The run always
// starts at the oldest live file, so dropping tombstones is safe (rule C7): nothing older exists.
void KVEngine::CompactLoop() {
  const size_t trigger = std::max<size_t>(2, options_.compaction_trigger);
  std::unique_lock<std::mutex> lock(write_mu_);
  for (;;) {
    cv_.wait(lock, [&] { return stop_ || dead_ || sstables_.size() >= trigger; });
    if (stop_ || dead_) return;
    const size_t n = sstables_.size();
    std::vector<std::shared_ptr<SSTable>> inputs(sstables_.begin(), sstables_.begin() + static_cast<std::ptrdiff_t>(n));
    const std::vector<SSTableMeta> live = manifest_.files;
    const uint64_t out_number = next_seq_++;  // same counter as the WAL sequence numbers
    const uint64_t epoch = epoch_;
    lock.unlock();

    std::vector<const SSTable*> raw;
    for (const auto& t : inputs) raw.push_back(t.get());
    CompactionOptions co;
    co.drop_tombstones = true;
    co.builder.bits_per_key = options_.bloom_bits_per_key;
    CompactionResult result;
    const std::string out_path = Path(SstFileName(out_number));
    Status s = Compact(raw, live, out_path, out_number, co, &result);
    if (s == Status::kOk && CrashPoint("compact:after_output")) return;
    std::shared_ptr<SSTable> output;
    if (s == Status::kOk) {
      std::unique_ptr<SSTable> opened;
      s = SSTable::Open(out_path, out_number, &opened);
      if (s == Status::kOk) output = std::shared_ptr<SSTable>(std::move(opened));
    }

    lock.lock();
    if (epoch_ != epoch) {  // the dataset was replaced while we merged: the output is stale
      std::error_code ec;
      std::filesystem::remove(out_path, ec);
      continue;
    }
    bool installed = false;
    if (s == Status::kOk) {
      ManifestState next = manifest_;
      next.files.erase(next.files.begin(), next.files.begin() + static_cast<std::ptrdiff_t>(n));  // the oldest n
      next.files.insert(next.files.begin(), result.output);
      next.next_file_number = std::max(next.next_file_number, next_seq_);
      s = StoreManifest(options_.dir, next);
      if (s == Status::kOk) {
        manifest_ = std::move(next);
        sstables_.erase(sstables_.begin(), sstables_.begin() + static_cast<std::ptrdiff_t>(n));
        sstables_.insert(sstables_.begin(), output);
        ++compactions_done_;
        installed = true;
        PublishView();
        cv_.notify_all();
        if (CrashPoint("compact:after_manifest")) return;  // inputs not deleted yet
      }
    }
    if (installed) {
      lock.unlock();
      std::error_code ec;
      for (const auto& t : inputs) std::filesystem::remove(Path(SstFileName(t->meta().file_number)), ec);
      lock.lock();
    } else {
      ++compaction_failures_;
      std::error_code ec;
      std::filesystem::remove(out_path, ec);  // an output that never committed
      cv_.wait_for(lock, std::chrono::milliseconds(options_.flush_retry_ms), [&] { return stop_ || dead_; });
    }
  }
}

}  // namespace raftkv
