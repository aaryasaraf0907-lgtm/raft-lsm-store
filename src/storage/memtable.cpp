#include "storage/memtable.h"

namespace raftkv {

void MemTable::Put(const std::string& key, const std::string& value) {
  list_.Upsert(key, value, /*tombstone=*/false);
}

void MemTable::Delete(const std::string& key) {
  list_.Upsert(key, std::string(), /*tombstone=*/true);
}

MemTable::GetResult MemTable::Get(const std::string& key, std::string* value) const {
  const Entry* e = list_.Find(key);
  if (e == nullptr) return GetResult::kNotFound;
  if (e->tombstone) return GetResult::kDeleted;
  if (value != nullptr) *value = e->value;
  return GetResult::kFound;
}

}  // namespace raftkv