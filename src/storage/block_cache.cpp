#include "storage/block_cache.h"

#include <atomic>
#include <iterator>
#include <utility>

namespace raftkv {

namespace {
constexpr size_t kBlockOverhead = 64;  // the vector, the shared_ptr control block, the LRU node
}  // namespace

BlockCache::Block BlockCache::Lookup(uint64_t table_id, uint64_t block_index) {
  std::lock_guard<std::mutex> lock(mu_);
  auto it = map_.find(Key{table_id, block_index});
  if (it == map_.end()) return nullptr;
  lru_.splice(lru_.begin(), lru_, it->second);  // now the most recently used
  return it->second->block;
}

void BlockCache::Insert(uint64_t table_id, uint64_t block_index, Block block) {
  const size_t charge = Charge(*block);
  if (charge > capacity_) return;  // also covers capacity 0
  std::lock_guard<std::mutex> lock(mu_);
  const Key key{table_id, block_index};
  if (map_.count(key) != 0) return;  // another reader cached it first: the content is identical
  lru_.push_front(Item{key, std::move(block), charge});
  map_.emplace(key, lru_.begin());
  usage_ += charge;
  while (usage_ > capacity_) EraseLocked(std::prev(lru_.end()));
}

void BlockCache::EraseTable(uint64_t table_id, uint64_t block_count) {
  std::lock_guard<std::mutex> lock(mu_);
  for (uint64_t b = 0; b < block_count && !map_.empty(); ++b) {
    auto it = map_.find(Key{table_id, b});
    if (it != map_.end()) EraseLocked(it->second);
  }
}

size_t BlockCache::usage() const {
  std::lock_guard<std::mutex> lock(mu_);
  return usage_;
}

size_t BlockCache::size() const {
  std::lock_guard<std::mutex> lock(mu_);
  return map_.size();
}

size_t BlockCache::Charge(const std::vector<Entry>& block) {
  size_t bytes = kBlockOverhead;
  for (const Entry& e : block) bytes += sizeof(Entry) + e.key.size() + e.value.size();
  return bytes;
}

uint64_t BlockCache::NewTableId() {
  static std::atomic<uint64_t> next{1};
  return next.fetch_add(1, std::memory_order_relaxed);
}

void BlockCache::EraseLocked(std::list<Item>::iterator it) {
  usage_ -= it->charge;
  map_.erase(it->key);
  lru_.erase(it);
}

}  // namespace raftkv
