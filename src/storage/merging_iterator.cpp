#include "storage/merging_iterator.h"

#include <utility>

#include "storage/sstable.h"

namespace raftkv {

MergingIterator::MergingIterator(std::vector<std::unique_ptr<KVIterator>> sources)
    : sources_(std::move(sources)), heap_(Later{&sources_}) {
  for (size_t i = 0; i < sources_.size(); ++i) {
    if (sources_[i]->Valid()) {
      heap_.push(i);
    } else if (StatusOf(i) != Status::kOk) {
      status_ = StatusOf(i);  // failed before producing anything: never a clean (empty) end
      return;
    }
  }
  Advance();
}

void MergingIterator::Next() {
  if (!Valid()) return;
  Advance();
}

void MergingIterator::Advance() {
  valid_ = false;
  if (heap_.empty()) return;
  const size_t top = heap_.top();
  heap_.pop();
  current_ = sources_[top]->Current();  // the newest version of the smallest key
  Status s = AdvanceSource(top);
  while (s == Status::kOk && !heap_.empty() && sources_[heap_.top()]->Current().key == current_.key) {
    const size_t older = heap_.top();
    heap_.pop();
    s = AdvanceSource(older);  // an older version of the same key: discard it
  }
  if (s != Status::kOk) {
    status_ = s;
    return;
  }
  valid_ = true;
}

Status MergingIterator::AdvanceSource(size_t i) {
  sources_[i]->Next();
  if (sources_[i]->Valid()) heap_.push(i);
  return StatusOf(i);
}

Status MergingIterator::StatusOf(size_t i) const {
  const auto* sst = dynamic_cast<const SSTable::Iterator*>(sources_[i].get());
  return sst != nullptr ? sst->status() : Status::kOk;
}

}  // namespace raftkv
