// In-memory sorted write buffer backed by a skip list.
// Delete() writes a tombstone; keys are never physically removed, so that
// flush/compaction can shadow older values in lower levels.
#pragma once

#include <cstddef>
#include <string>

#include "storage/iterator.h"
#include "storage/skiplist.h"

namespace raftkv {

class MemTable {
 public:
  enum class GetResult { kFound, kDeleted, kNotFound };

  explicit MemTable(size_t flush_threshold_bytes = 4 * 1024 * 1024)
      : threshold_(flush_threshold_bytes) {}

  void Put(const std::string& key, const std::string& value);
  void Delete(const std::string& key);

  // kDeleted means a tombstone is present here: callers must NOT look in older
  // SSTables. kNotFound means this memtable knows nothing about the key.
  GetResult Get(const std::string& key, std::string* value) const;

  size_t ApproximateBytes() const { return list_.ApproximateBytes(); }
  size_t Count() const { return list_.Count(); }  // includes tombstones
  bool Empty() const { return list_.Count() == 0; }
  bool ShouldFlush() const { return list_.ApproximateBytes() >= threshold_; }

  // Sorted iterator over all entries, tombstones included. Starts positioned
  // at the first entry (or at the first key >= `start` if given).
  class Iterator : public KVIterator {
   public:
    explicit Iterator(const MemTable* mt) : it_(&mt->list_) { it_.SeekToFirst(); }
    Iterator(const MemTable* mt, const std::string& start) : it_(&mt->list_) { it_.Seek(start); }
    bool Valid() const override { return it_.Valid(); }
    void Next() override { it_.Next(); }
    const Entry& Current() const override { return it_.entry(); }

   private:
    SkipList::Iterator it_;
  };
  Iterator NewIterator() const { return Iterator(this); }
  Iterator NewIterator(const std::string& start) const { return Iterator(this, start); }

 private:
  SkipList list_;
  size_t threshold_;
};

}  // namespace raftkv