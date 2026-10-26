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
#include "storage/kv_store.h"
#include "storage/memtable.h"
#include "storage/wal.h"

namespace raftkv {

class KVEngine : public KVStore {
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
  };

  struct Stats {
    uint64_t active_seq = 0;
    size_t frozen_memtables = 0;
    uint64_t flushes_done = 0;
    uint64_t flush_failures = 0;
    bool flush_failing = false;
  };

  // kOk; kIOError (cannot create dir / open WAL); kCorruption (a WAL is damaged
  // before its last record, so recovery would silently lose data).
  static Status Open(const Options& options, std::unique_ptr<KVEngine>* out);
  ~KVEngine() override;  // frozen memtables not yet flushed stay in their WALs

  Status Put(const std::string& key, const std::string& value) override;
  Status Get(const std::string& key, std::string* value) override;
  Status Delete(const std::string& key) override;  // writes a tombstone, even if key is absent

  uint64_t recovered_records() const { return recovered_records_; }
  Stats GetStats() const;

  static std::string WalFileName(uint64_t seq);  // "wal-000001.log"
  static std::string SstFileName(uint64_t seq);  // "000001.sst"

 private:
  struct Frozen {
    uint64_t seq;
    std::shared_ptr<MemTable> mem;
  };
  struct View {  // immutable snapshot of which memtables exist
    std::shared_ptr<MemTable> active;
    std::vector<std::shared_ptr<MemTable>> frozen;  // newest first
  };

  explicit KVEngine(const Options& options) : options_(options) {}

  Status Recover();
  Status Write(bool tombstone, const std::string& key, const std::string& value);
  Status EnsureRoom(std::unique_lock<std::mutex>& lock);  // write_mu_ held
  Status Freeze();                                        // write_mu_ held
  void PublishView();                                     // write_mu_ held
  void FlushLoop();
  bool SyncDir() const;
  std::string Path(const std::string& name) const;

  const Options options_;
  uint64_t recovered_records_ = 0;  // set before any thread starts

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

  mutable std::mutex view_mu_;  // innermost lock; only guards the pointer copy
  std::shared_ptr<const View> view_;

  std::thread flusher_;
};

}  // namespace raftkv