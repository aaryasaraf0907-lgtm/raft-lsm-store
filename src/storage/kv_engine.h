// Local storage engine: WAL + memtable (no SSTables / flush yet).
//
// Write path:  Put/Delete -> WAL append (fsync) -> memtable.   A write that
//              fails to reach the WAL is NOT applied to the memtable.
// Read path:   Get -> memtable only (until SSTables exist).
// Startup:     WAL is replayed into the memtable.
//
// Raft's apply callback calls Put/Delete only after an entry is committed.
//
// Thread safety: one shared_mutex. Gets run in parallel; writes are exclusive,
// which also keeps the WAL single-writer. This is the interim design; the
// concurrent memtable (next PR) removes the lock from the read path.
#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <string>

#include "storage/kv_store.h"
#include "storage/memtable.h"
#include "storage/wal.h"

namespace raftkv {

class KVEngine : public KVStore {
 public:
  struct Options {
    std::string dir;                        // data directory, created if missing
    size_t memtable_bytes = 4 * 1024 * 1024;  // flush threshold (used once flush exists)
    bool sync_wal = true;                   // fsync every write before acknowledging it
  };

  // kOk; kIOError (cannot create dir / open WAL); kCorruption (WAL damaged
  // before its last record, so recovery would silently lose data).
  static Status Open(const Options& options, std::unique_ptr<KVEngine>* out);

  Status Put(const std::string& key, const std::string& value) override;
  Status Get(const std::string& key, std::string* value) override;
  Status Delete(const std::string& key) override;  // writes a tombstone, even if key is absent

  uint64_t recovered_records() const { return recovered_records_; }

 private:
  explicit KVEngine(const Options& options) : options_(options), mem_(options.memtable_bytes) {}

  Options options_;
  MemTable mem_;
  std::unique_ptr<WAL> wal_;
  uint64_t recovered_records_ = 0;
  mutable std::shared_mutex mu_;
};

}  // namespace raftkv