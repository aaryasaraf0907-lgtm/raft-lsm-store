#pragma once
#include <string>

namespace raftkv {

// One key/value record. A delete is stored as a "tombstone" (not removed),
// because older SSTables may still hold the key. Compaction drops tombstones later.
struct Entry {
  std::string key;
  std::string value;       // empty when tombstone is true
  bool tombstone = false;
};

// Sorted-by-key iterator. Implemented by the memtable (Aansh) and by SSTable
// readers (Aditya Y.); consumed by SSTable flush and by compaction.
class KVIterator {
 public:
  virtual ~KVIterator() = default;
  virtual bool Valid() const = 0;
  virtual void Next() = 0;
  virtual const Entry& Current() const = 0;  // only valid if Valid()
};

}  // namespace raftkv
