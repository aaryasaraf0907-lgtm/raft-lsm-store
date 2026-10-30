// k-way merge of sorted sources into one sorted stream with one entry per key: the newest version.
//
// Sources are given newest first: when several hold the same key, the one with the lowest index
// wins and the others' versions of that key are skipped. Tombstones are yielded like any other
// entry (a tombstone is the newest version of its key); callers decide what to show. Used by
// KVEngine::Checkpoint (every layer) and KVEngine::Scan (a key range).
//
// Errors: only an SSTable::Iterator can fail (a damaged block); other sources cannot report
// errors. A source that is already failed when the merge is built, or that fails while the merge
// advances it (including while skipping an older version), ends the merge: Valid() becomes false
// and status() returns that error, never a clean end. Errors are sticky.
//
// Current() is a copy owned by the merge (a memtable source may publish a newer version of the
// same key at any time; the merge reads it once), valid until the next Next(). Not thread-safe.
#pragma once

#include <cstddef>
#include <memory>
#include <queue>
#include <vector>

#include "storage/iterator.h"
#include "storage/kv_store.h"

namespace raftkv {

class MergingIterator : public KVIterator {
 public:
  explicit MergingIterator(std::vector<std::unique_ptr<KVIterator>> sources);

  bool Valid() const override { return status_ == Status::kOk && valid_; }
  void Next() override;
  const Entry& Current() const override { return current_; }
  Status status() const { return status_; }

 private:
  // Orders source indices by (key ascending, source index ascending): the heap's top is the
  // smallest key, and for equal keys the newest source.
  struct Later {
    const std::vector<std::unique_ptr<KVIterator>>* sources;
    bool operator()(size_t x, size_t y) const {
      const std::string& kx = (*sources)[x]->Current().key;
      const std::string& ky = (*sources)[y]->Current().key;
      return kx != ky ? kx > ky : x > y;
    }
  };

  void Advance();                      // takes the next key from the heap into current_
  Status AdvanceSource(size_t i);      // Next() on source i, re-queues it, returns its status
  Status StatusOf(size_t i) const;

  std::vector<std::unique_ptr<KVIterator>> sources_;
  std::priority_queue<size_t, std::vector<size_t>, Later> heap_;
  Entry current_;
  bool valid_ = false;
  Status status_ = Status::kOk;
};

}  // namespace raftkv
