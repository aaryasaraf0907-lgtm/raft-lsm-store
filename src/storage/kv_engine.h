// Local storage engine: WAL + memtables, with optional background flush.
//
// Write path:  Put/Delete -> WAL append (fsync) -> active memtable.  A write that
//              fails to reach the WAL is NOT applied to the memtable.
// Flush:       when the active memtable is full it is frozen (read-only), a new
//              memtable + a new WAL file take over, and a background thread hands
//              the frozen memtable to Options::flush_writer. Once the SSTable is
//              published the frozen memtable's WAL is deleted.
// Files:       wal-<seq>.log  (one per memtable)   <seq>.sst  (flushed memtable)
//              A WAL whose <seq>.sst exists was flushed: it is deleted on startup,
//              never replayed. Every other WAL is replayed. Crashing at any point
//              therefore loses nothing and never replays old data over new data.
// Reads:       active memtable, then frozen memtables newest-first. A tombstone
//              stops the search.
//
// NOTE (4a): flushed data is NOT readable through Get yet. The SSTable reader is
// wired in by the next PR, so flush_writer must stay unset until then. With it
// unset the engine never freezes and behaves exactly like the WAL+memtable engine.
//
// Thread safety: writes are serialised by one mutex (keeps WAL order == memtable
// order). Gets take no engine lock: they copy a shared_ptr to an immutable view
// under a tiny mutex, then read lock-free from the concurrent memtables.
#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "storage/iterator.h"
#include "storage/checkpoint.h"
#include "storage/kv_store.h"
#include "storage/manifest.h"
#include "storage/memtable.h"
#include "storage/sstable.h"
#include "storage/wal.h"

namespace raftkv {

class KVEngine : public CheckpointableStore {
 public:
  // Writes `entries` (sorted by key, tombstones included) as the SSTable for flush
  // sequence `seq` at `path`. Must return kOk only once the complete file is durable
  // under `path` (publish atomically); any other status means "not published".
  // The real adapter passes seq as file_number, min_seq and max_seq.
  using FlushWriter = std::function<Status(KVIterator& entries, const std::string& path, uint64_t seq)>;

  struct Options {
    std::string dir;                          // data directory, created if missing
    size_t memtable_bytes = 4 * 1024 * 1024;  // freeze threshold
    bool sync_wal = true;                     // fsync every write before acknowledging it
    FlushWriter flush_writer;                 // unset => never freeze/flush
    size_t max_frozen = 2;                    // writers wait (or fail) beyond this many
    int flush_retry_ms = 200;                 // delay before retrying a failed flush

    // SSTable mode: the real LSM tree. A manifest (docs/sstable-format.md A.4) records which
    // SSTables are live and how far flushing has got; the engine flushes a full memtable into an
    // SSTable by itself (flush_writer must be unset); Get searches the SSTables newest first; and a
    // background thread merges a run of the newest live SSTables when there are too many (which
    // run: see CompactLoop). Off by default, which
    // keeps the original WAL + memtable behaviour (and the flush_writer test seam) unchanged.
    bool use_sstables = false;
    size_t compaction_trigger = 4;  // merge when this many SSTables are live (min 2; 0 = never)
    uint32_t bloom_bits_per_key = BloomFilter::kDefaultBitsPerKey;
    // Tests only: called at named steps of flush and compaction ("flush:after_sst",
    // "flush:after_manifest", "compact:after_output", "compact:after_manifest"). Returning true
    // makes the engine stop right there, as if the process had died, leaving files as they are.
    std::function<bool(const char* point)> test_crash_hook;
  };

  struct Stats {
    uint64_t active_seq = 0;
    size_t frozen_memtables = 0;
    uint64_t flushes_done = 0;
    uint64_t flush_failures = 0;
    bool flush_failing = false;
    size_t sstables = 0;  // live SSTables (SSTable mode)
    uint64_t compactions_done = 0;
    uint64_t compaction_failures = 0;
  };

  // kOk; kIOError (cannot create dir / open WAL); kCorruption (a WAL is damaged
  // before its last record, so recovery would silently lose data).
  static Status Open(const Options& options, std::unique_ptr<KVEngine>* out);
  ~KVEngine() override;  // frozen memtables not yet flushed stay in their WALs

  Status Put(const std::string& key, const std::string& value) override;
  Status Get(const std::string& key, std::string* value) override;
  Status Delete(const std::string& key) override;  // writes a tombstone, even if key is absent

  // Like Put/Delete, but also record `raft_index` as the applied index (see IndexedKVStore). The data
  // record and the index record are appended in that order and made durable by ONE sync, and
  // neither becomes visible to readers until both are in the WAL. raft_index must be greater than
  // AppliedIndex() (else kInvalidArgument, and nothing is written).
  Status PutAt(const std::string& key, const std::string& value, uint64_t raft_index) override;
  Status DeleteAt(const std::string& key, uint64_t raft_index) override;
  uint64_t AppliedIndex() override { return applied_index_.load(); }

  // Raft snapshots of the data (see checkpoint.h). Checkpoint works in both modes; InstallCheckpoint
  // needs SSTable mode (kInvalidArgument otherwise): it replaces everything with ONE new SSTable and
  // one manifest edit, so a crash leaves the old dataset or the new one, never a mix.
  Status Checkpoint(std::string* data, uint64_t* applied_index) override;
  Status InstallCheckpoint(const std::string& data, uint64_t applied_index) override;

  uint64_t recovered_records() const { return recovered_records_; }
  Stats GetStats() const;

  static std::string WalFileName(uint64_t seq);  // "wal-000001.log"
  static std::string SstFileName(uint64_t seq);  // "000001.sst"

 private:
  struct Frozen {
    uint64_t seq;
    std::shared_ptr<MemTable> mem;
  };
  struct View {  // immutable snapshot of what a read must search, newest data first
    std::shared_ptr<MemTable> active;
    std::vector<std::shared_ptr<MemTable>> frozen;  // newest first
    std::vector<std::shared_ptr<SSTable>> sstables;  // newest first (a reader keeps a file open
                                                     // even after compaction has deleted it)
  };

  explicit KVEngine(const Options& options) : options_(options) {}

  Status Recover();
  Status LoadLsmState();  // SSTable mode: manifest, orphan cleanup, open the live tables
  bool CrashPoint(const char* point);
  Status Write(bool tombstone, const std::string& key, const std::string& value, uint64_t raft_index);
  Status EnsureRoom(std::unique_lock<std::mutex>& lock);  // write_mu_ held
  Status Freeze();                                        // write_mu_ held
  void PublishView();                                     // write_mu_ held
  void FlushLoop();
  void CompactLoop();
  bool SyncDir() const;
  std::string Path(const std::string& name) const;

  const Options options_;
  uint64_t recovered_records_ = 0;  // set before any thread starts
  std::atomic<uint64_t> applied_index_{0};  // written under write_mu_, read anywhere

  mutable std::mutex write_mu_;  // guards everything below until view_mu_
  std::condition_variable cv_;
  std::shared_ptr<MemTable> active_;
  uint64_t active_seq_ = 0;
  uint64_t next_seq_ = 1;
  std::unique_ptr<WAL> wal_;
  std::deque<Frozen> frozen_;  // front = newest, back = oldest
  bool stop_ = false;
  bool flush_failing_ = false;
  uint64_t flushes_done_ = 0;
  uint64_t flush_failures_ = 0;
  // SSTable mode (guarded by write_mu_). sstables_ is oldest first and parallel to manifest_.files.
  ManifestState manifest_;
  std::vector<std::shared_ptr<SSTable>> sstables_;
  uint64_t lsm_highest_number_ = 0;  // highest file number seen on disk at startup, orphans included
  uint64_t compactions_done_ = 0;
  uint64_t compaction_failures_ = 0;
  uint64_t epoch_ = 0;  // bumped when InstallCheckpoint replaces the dataset (guarded by write_mu_)
  std::atomic<bool> dead_{false};  // a test crash point fired: every background thread stops

  mutable std::mutex view_mu_;  // innermost lock; only guards the pointer copy
  std::shared_ptr<const View> view_;

  std::thread flusher_;
  std::thread compactor_;
};

}  // namespace raftkv