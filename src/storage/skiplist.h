// Ordered skip list used by the memtable. Single-threaded for now (PR 1);
// PR 3 adds concurrency. Keys are unique: Upsert overwrites in place.
#pragma once

#include <cstddef>
#include <cstdint>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "storage/iterator.h"

namespace raftkv {

class SkipList {
 public:
  static constexpr int kMaxHeight = 12;
  static constexpr size_t kNodeOverhead = 48;  // rough per-node bookkeeping cost

  struct Node {
    Entry entry;              // key, value, tombstone flag
    std::vector<Node*> next;  // next.size() == height of this node
    Node(std::string k, std::string v, bool t, int height) : entry{std::move(k), std::move(v), t}, next(height, nullptr) {}
  };

  explicit SkipList(uint32_t seed = 0xC0FFEE)
      : head_("", "", false, kMaxHeight), rng_(seed) {}

  SkipList(const SkipList&) = delete;
  SkipList& operator=(const SkipList&) = delete;

  ~SkipList() {  // iterative: a recursive delete could overflow the stack
    Node* n = head_.next[0];
    while (n != nullptr) {
      Node* nx = n->next[0];
      delete n;
      n = nx;
    }
  }

  // Inserts the key or overwrites its value/tombstone flag. Returns true if a
  // new node was created, false if an existing one was updated.
  bool Upsert(const std::string& key, const std::string& value, bool tombstone) {
    Node* prev[kMaxHeight];
    Node* found = FindGreaterOrEqual(key, prev);
    if (found != nullptr && found->entry.key == key) {
      bytes_ = bytes_ - found->entry.value.size() + value.size();
      found->entry.value = value;
      found->entry.tombstone = tombstone;
      return false;
    }
    int h = RandomHeight();
    if (h > height_) {
      for (int i = height_; i < h; ++i) prev[i] = &head_;
      height_ = h;
    }
    Node* n = new Node(key, value, tombstone, h);
    for (int i = 0; i < h; ++i) {
      n->next[i] = prev[i]->next[i];
      prev[i]->next[i] = n;
    }
    bytes_ += key.size() + value.size() + kNodeOverhead + h * sizeof(Node*);
    ++count_;
    return true;
  }

  // Returns the entry for `key`, or nullptr. Tombstones are returned as-is.
  const Entry* Find(const std::string& key) const {
    const Node* n = FindGreaterOrEqual(key, nullptr);
    return (n != nullptr && n->entry.key == key) ? &n->entry : nullptr;
  }

  size_t Count() const { return count_; }
  size_t ApproximateBytes() const { return bytes_; }

  // Forward iterator in ascending key order.
  class Iterator {
   public:
    explicit Iterator(const SkipList* list) : list_(list), node_(nullptr) {}
    bool Valid() const { return node_ != nullptr; }
    void SeekToFirst() { node_ = list_->head_.next[0]; }
    void Seek(const std::string& target) {  // first key >= target
      node_ = list_->FindGreaterOrEqual(target, nullptr);
    }
    void Next() { node_ = node_->next[0]; }
    const Entry& entry() const { return node_->entry; }

   private:
    const SkipList* list_;
    const Node* node_;
  };

 private:
  int RandomHeight() {
    int h = 1;
    while (h < kMaxHeight && (rng_() & 3) == 0) ++h;  // p = 1/4
    return h;
  }

  // First node with key >= `key`; fills prev[] per level when non-null.
  Node* FindGreaterOrEqual(const std::string& key, Node** prev) const {
    Node* x = const_cast<Node*>(&head_);
    for (int level = height_ - 1; level >= 0; --level) {
      while (x->next[level] != nullptr && x->next[level]->entry.key < key) x = x->next[level];
      if (prev != nullptr) prev[level] = x;
    }
    if (prev != nullptr) {
      for (int level = height_; level < kMaxHeight; ++level) prev[level] = const_cast<Node*>(&head_);
    }
    return x->next[0];
  }

  Node head_;
  int height_ = 1;
  size_t count_ = 0;
  size_t bytes_ = 0;
  std::mt19937 rng_;
};

}  // namespace raftkv