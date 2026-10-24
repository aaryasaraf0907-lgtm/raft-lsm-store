// Concurrent ordered skip list used by the memtable.
//
// Concurrency model (insert-only, like LevelDB/RocksDB memtables):
//  * Nodes are never removed or freed until the list is destroyed. A delete is
//    just a newer tombstone version, so no reader can touch freed memory.
//  * A new key is linked with compare-and-swap (CAS), bottom level first.
//    Readers use acquire loads and never take a lock.
//  * Overwriting a key pushes a new immutable Version onto that node's version
//    chain with a CAS; the newest version wins. Old versions are kept until
//    destruction, so ApproximateBytes only grows.
//  * Destruction is NOT thread-safe: stop all users first.
#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "storage/iterator.h"

namespace raftkv {

class SkipList {
 private:
  struct Version {  // immutable once published
    Entry entry;
    const Version* older = nullptr;
    Version(const std::string& k, const std::string& v, bool t) : entry{k, v, t} {}
  };

  struct Node {
    const std::string key;
    const int height;
    std::atomic<const Version*> newest{nullptr};
    std::unique_ptr<std::atomic<Node*>[]> next;
    Node(const std::string& k, int h) : key(k), height(h), next(new std::atomic<Node*>[h]) {
      for (int i = 0; i < h; ++i) next[i].store(nullptr, std::memory_order_relaxed);
    }
    Node* Next(int level) const { return next[level].load(std::memory_order_acquire); }
  };

 public:
  static constexpr int kMaxHeight = 12;
  static constexpr size_t kNodeOverhead = 48;     // rough per-node bookkeeping cost
  static constexpr size_t kVersionOverhead = 48;  // rough per-version bookkeeping cost

  explicit SkipList(uint32_t seed = 0xC0FFEE) : head_(new Node(std::string(), kMaxHeight)), rng_state_(seed) {}

  SkipList(const SkipList&) = delete;
  SkipList& operator=(const SkipList&) = delete;

  ~SkipList() {  // iterative: a recursive delete could overflow the stack
    Node* n = head_->next[0].load(std::memory_order_relaxed);
    delete head_;
    while (n != nullptr) {
      Node* nx = n->next[0].load(std::memory_order_relaxed);
      const Version* v = n->newest.load(std::memory_order_relaxed);
      while (v != nullptr) {
        const Version* older = v->older;
        delete v;
        v = older;
      }
      delete n;
      n = nx;
    }
  }

  // Inserts the key or adds a newer version of it. Safe to call from many
  // threads. Returns true if this call created the key's node, false if the
  // key already existed (exactly one concurrent caller gets true per key).
  bool Upsert(const std::string& key, const std::string& value, bool tombstone) {
    Version* version = new Version(key, value, tombstone);
    const size_t version_bytes = key.size() + value.size() + kVersionOverhead;
    Node* preds[kMaxHeight];
    Node* succs[kMaxHeight];
    Node* node = nullptr;  // our new node, allocated lazily and never shared until linked

    for (;;) {
      Node* found = FindPreds(key, preds, succs);
      if (found != nullptr) {  // key exists (maybe inserted by another thread meanwhile)
        delete node;
        PushVersion(found, version);
        bytes_.fetch_add(version_bytes, std::memory_order_relaxed);
        return false;
      }
      if (node == nullptr) {
        node = new Node(key, RandomHeight());
        node->newest.store(version, std::memory_order_relaxed);
      }
      for (int i = 0; i < node->height; ++i) node->next[i].store(succs[i], std::memory_order_relaxed);
      Node* expected = succs[0];
      if (preds[0]->next[0].compare_exchange_strong(expected, node, std::memory_order_release,
                                                    std::memory_order_relaxed)) {
        break;  // visible to readers from this point on
      }
    }

    // Link the upper levels. The node is already findable at level 0, so a
    // failure here only means re-finding our neighbours and trying again.
    for (int i = 1; i < node->height; ++i) {
      for (;;) {
        Node* expected = succs[i];
        node->next[i].store(expected, std::memory_order_relaxed);
        if (preds[i]->next[i].compare_exchange_strong(expected, node, std::memory_order_release,
                                                      std::memory_order_relaxed)) {
          break;
        }
        FindPreds(key, preds, succs);
      }
    }

    int cur = max_height_.load(std::memory_order_relaxed);
    while (cur < node->height &&
           !max_height_.compare_exchange_weak(cur, node->height, std::memory_order_relaxed)) {
    }
    count_.fetch_add(1, std::memory_order_relaxed);
    bytes_.fetch_add(version_bytes + key.size() + kNodeOverhead + node->height * sizeof(Node*),
                     std::memory_order_relaxed);
    return true;
  }

  // Newest entry for `key`, or nullptr. Tombstones are returned as-is. The
  // pointer stays valid for the lifetime of the list and its contents never change.
  const Entry* Find(const std::string& key) const {
    const Node* n = FindGreaterOrEqual(key);
    if (n == nullptr || n->key != key) return nullptr;
    return &n->newest.load(std::memory_order_acquire)->entry;
  }

  size_t Count() const { return count_.load(std::memory_order_relaxed); }  // distinct keys
  size_t ApproximateBytes() const { return bytes_.load(std::memory_order_relaxed); }

  // Forward iterator in ascending key order. Not a snapshot: it may or may not
  // see inserts that happen while it runs, but keys always come out sorted.
  class Iterator {
   public:
    explicit Iterator(const SkipList* list) : list_(list), node_(nullptr) {}
    bool Valid() const { return node_ != nullptr; }
    void SeekToFirst() { node_ = list_->head_->Next(0); }
    void Seek(const std::string& target) { node_ = list_->FindGreaterOrEqual(target); }  // first key >= target
    void Next() { node_ = node_->Next(0); }
    const Entry& entry() const { return node_->newest.load(std::memory_order_acquire)->entry; }

   private:
    const SkipList* list_;
    const Node* node_;
  };

 private:
  static void PushVersion(Node* n, Version* v) {
    const Version* old = n->newest.load(std::memory_order_relaxed);
    do {
      v->older = old;
    } while (!n->newest.compare_exchange_weak(old, v, std::memory_order_release, std::memory_order_relaxed));
  }

  int RandomHeight() {  // lock-free splitmix64 stream, p = 1/4 per extra level
    uint64_t z = rng_state_.fetch_add(0x9E3779B97F4A7C15ull, std::memory_order_relaxed) + 0x9E3779B97F4A7C15ull;
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
    z ^= z >> 31;
    int h = 1;
    while (h < kMaxHeight && (z & 3) == 0) {
      ++h;
      z >>= 2;
    }
    return h;
  }

  Node* FindGreaterOrEqual(const std::string& key) const {
    Node* x = head_;
    for (int level = max_height_.load(std::memory_order_relaxed) - 1; level >= 0; --level) {
      for (Node* next = x->Next(level); next != nullptr && next->key < key; next = x->Next(level)) x = next;
    }
    return x->Next(0);
  }

  // Fills, for every level, the last node with key < `key` (preds) and the node
  // after it (succs). Returns the node holding `key` if it exists, else nullptr.
  Node* FindPreds(const std::string& key, Node** preds, Node** succs) const {
    Node* x = head_;
    const int top = max_height_.load(std::memory_order_relaxed);
    for (int level = kMaxHeight - 1; level >= top; --level) {
      preds[level] = head_;
      succs[level] = head_->Next(level);
    }
    for (int level = top - 1; level >= 0; --level) {
      Node* next = x->Next(level);
      while (next != nullptr && next->key < key) {
        x = next;
        next = x->Next(level);
      }
      preds[level] = x;
      succs[level] = next;
    }
    Node* cand = succs[0];
    return (cand != nullptr && cand->key == key) ? cand : nullptr;
  }

  Node* head_;
  std::atomic<int> max_height_{1};
  std::atomic<size_t> count_{0};
  std::atomic<size_t> bytes_{0};
  std::atomic<uint64_t> rng_state_;
};

}  // namespace raftkv