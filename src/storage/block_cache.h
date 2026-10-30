// LRU cache of decoded SSTable data blocks, shared by the SSTables of one engine.
//
// Only blocks that passed the full §10.2 validation (docs/sstable-format.md) are inserted, so a hit
// serves an already-validated copy and never touches the disk; a block that failed to read or
// validate is never cached. An SSTable file is immutable once published and one SSTable object
// reads one file, so the block at (table id, block index) never changes while that object lives.
//
// Identity: every SSTable object takes a fresh id from NewTableId() (a process-wide counter), not
// its file number, so tables of different directories, a reopened table or a table installed by a
// checkpoint never share entries. ~SSTable erases its own entries (EraseTable).
//
// Lifetime: a block is handed out as a shared_ptr, so evicting or erasing it only drops the
// cache's reference; a reader still holding it keeps it alive. Memory in use can therefore exceed
// the capacity by the blocks that readers hold at that moment.
//
// Thread safety: every method may be called concurrently; one mutex guards the LRU list and the
// map, and no I/O happens under it.
#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "storage/iterator.h"

namespace raftkv {

class BlockCache {
 public:
  using Block = std::shared_ptr<const std::vector<Entry>>;

  // capacity_bytes 0 caches nothing (every Insert is dropped).
  explicit BlockCache(size_t capacity_bytes) : capacity_(capacity_bytes) {}
  BlockCache(const BlockCache&) = delete;
  BlockCache& operator=(const BlockCache&) = delete;

  // The cached block, now the most recently used, or nullptr.
  Block Lookup(uint64_t table_id, uint64_t block_index);
  // Caches `block` (which must have passed validation) unless it is already cached or alone is
  // larger than the capacity, then evicts least recently used blocks until usage <= capacity.
  void Insert(uint64_t table_id, uint64_t block_index, Block block);
  // Drops every block of table `table_id` (block indices [0, block_count)).
  void EraseTable(uint64_t table_id, uint64_t block_count);

  size_t capacity() const { return capacity_; }
  size_t usage() const;   // sum of Charge() over the cached blocks
  size_t size() const;    // number of cached blocks

  // What a block costs against the capacity: its key and value bytes plus a fixed overhead per
  // entry and per block (approximate, like the memtable's accounting).
  static size_t Charge(const std::vector<Entry>& block);
  static uint64_t NewTableId();

 private:
  struct Key {
    uint64_t table;
    uint64_t block;
    bool operator==(const Key& o) const { return table == o.table && block == o.block; }
  };
  struct KeyHash {
    size_t operator()(const Key& k) const {
      return std::hash<uint64_t>()(k.table * 0x9E3779B97F4A7C15ull ^ k.block);
    }
  };
  struct Item {
    Key key;
    Block block;
    size_t charge;
  };

  void EraseLocked(std::list<Item>::iterator it);

  const size_t capacity_;
  mutable std::mutex mu_;
  std::list<Item> lru_;  // front = most recently used
  std::unordered_map<Key, std::list<Item>::iterator, KeyHash> map_;
  size_t usage_ = 0;
};

}  // namespace raftkv
