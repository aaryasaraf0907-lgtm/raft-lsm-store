#include "storage/compaction.h"

#include <algorithm>
#include <memory>
#include <queue>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace raftkv {

Status ValidateCompaction(const std::vector<const SSTable*>& inputs,
                          const std::vector<SSTableMeta>& live_files,
                          uint64_t output_file_number, bool drop_tombstones, SSTableMeta* output) {
  if (output == nullptr) return Status::kInvalidArgument;

  // C1: unique file numbers; min_seq <= max_seq for every live file.
  std::unordered_map<uint64_t, size_t> by_number;  // file_number -> index into live_files
  for (size_t i = 0; i < live_files.size(); ++i) {
    const SSTableMeta& f = live_files[i];
    if (f.min_seq > f.max_seq) return Status::kInvalidArgument;
    if (!by_number.emplace(f.file_number, i).second) return Status::kInvalidArgument;
  }

  // C2: S = live_files sorted by min_seq; ranges pairwise disjoint.
  std::vector<size_t> order(live_files.size());
  for (size_t i = 0; i < order.size(); ++i) order[i] = i;
  std::sort(order.begin(), order.end(), [&](size_t x, size_t y) {
    return live_files[x].min_seq < live_files[y].min_seq;
  });
  for (size_t j = 0; j + 1 < order.size(); ++j) {
    if (!(live_files[order[j]].max_seq < live_files[order[j + 1]].min_seq)) {
      return Status::kInvalidArgument;
    }
  }
  std::vector<size_t> position(live_files.size());  // live_files index -> position in S
  for (size_t j = 0; j < order.size(); ++j) position[order[j]] = j;

  // C3: at least one input.
  if (inputs.empty()) return Status::kInvalidArgument;

  // C4: each input's footer {file_number, min_seq, max_seq} equals exactly one live entry; no
  // input appears twice.
  std::vector<size_t> run;  // positions in S of the inputs
  std::unordered_set<uint64_t> seen;
  for (const SSTable* t : inputs) {
    if (t == nullptr) return Status::kInvalidArgument;
    const SSTableMeta& m = t->meta();
    auto it = by_number.find(m.file_number);
    if (it == by_number.end()) return Status::kInvalidArgument;
    const SSTableMeta& live = live_files[it->second];
    if (live.min_seq != m.min_seq || live.max_seq != m.max_seq) return Status::kInvalidArgument;
    if (!seen.insert(m.file_number).second) return Status::kInvalidArgument;
    run.push_back(position[it->second]);
  }

  // C5: the inputs' positions in S form one consecutive run S[a..b].
  std::sort(run.begin(), run.end());
  for (size_t i = 0; i + 1 < run.size(); ++i) {
    if (run[i + 1] != run[i] + 1) return Status::kInvalidArgument;
  }
  const size_t a = run.front();
  const size_t b = run.back();

  // C6: the output file number is not a live file.
  if (by_number.count(output_file_number) != 0) return Status::kInvalidArgument;

  // C7: dropping tombstones requires the run to start at the oldest live file.
  if (drop_tombstones && a != 0) return Status::kInvalidArgument;

  // §13.3: [lo, hi] = [S[a].min_seq, S[b].max_seq].
  *output = SSTableMeta{output_file_number, live_files[order[a]].min_seq,
                        live_files[order[b]].max_seq};
  return Status::kOk;
}

Status Compact(const std::vector<const SSTable*>& inputs,
               const std::vector<SSTableMeta>& live_files, const std::string& output_path,
               uint64_t output_file_number, const CompactionOptions& options,
               CompactionResult* result) {
  SSTableMeta out_meta;
  Status s = ValidateCompaction(inputs, live_files, output_file_number, options.drop_tombstones,
                                &out_meta);
  if (s != Status::kOk) return s;

  // Create every input iterator and check it immediately (§13.4), before any output file exists.
  std::vector<std::unique_ptr<SSTable::Iterator>> iters;
  iters.reserve(inputs.size());
  for (const SSTable* t : inputs) {
    iters.push_back(t->NewIterator());
    // An invalid iterator with kOk is an empty input; anything else is an error (§11.3).
    if (!iters.back()->Valid() && iters.back()->status() != Status::kOk) {
      return iters.back()->status();
    }
  }

  // Min-heap of input indices ordered by (key ascending, max_seq descending). Input ranges are
  // disjoint (C2), so max_seq values are distinct and the order is total.
  auto later = [&](size_t x, size_t y) {  // true if x should come out of the heap after y
    const std::string& kx = iters[x]->Current().key;
    const std::string& ky = iters[y]->Current().key;
    if (kx != ky) return kx > ky;
    return inputs[x]->meta().max_seq < inputs[y]->meta().max_seq;
  };
  std::priority_queue<size_t, std::vector<size_t>, decltype(later)> heap(later);
  for (size_t i = 0; i < iters.size(); ++i) {
    if (iters[i]->Valid()) heap.push(i);
  }

  std::unique_ptr<SSTableBuilder> builder;
  s = SSTableBuilder::Create(output_path, out_meta, options.builder, &builder);
  if (s != Status::kOk) return s;  // nothing was created

  // Advances input i past its current entry and re-queues it. Returns the input's error, if any.
  auto advance = [&](size_t i) -> Status {
    iters[i]->Next();
    if (iters[i]->Valid()) {
      heap.push(i);
      return Status::kOk;
    }
    return iters[i]->status();  // kOk: clean end of this input
  };

  uint64_t written = 0;
  while (!heap.empty()) {
    const size_t top = heap.top();
    heap.pop();
    // The top entry is the newest version of the smallest remaining key.
    const Entry& newest = iters[top]->Current();
    const std::string key = newest.key;  // Current() is invalidated by Next()
    if (!(newest.tombstone && options.drop_tombstones)) {
      s = builder->Add(newest);
      if (s != Status::kOk) {
        builder->Abandon();
        return s;
      }
      ++written;
    }
    s = advance(top);
    // Discard every older version of the same key (each input holds a key at most once).
    while (s == Status::kOk && !heap.empty() && iters[heap.top()]->Current().key == key) {
      const size_t older = heap.top();
      heap.pop();
      s = advance(older);
    }
    if (s != Status::kOk) {
      builder->Abandon();  // corruption is never treated as end of input
      return s;
    }
  }

  s = builder->Finish();
  if (s != Status::kOk) return s;  // Per §12.2, treat failed Finish as unpublished; any leftover output is garbage for startup cleanup (Appendix A.5).
  if (result != nullptr) *result = CompactionResult{out_meta, written};
  return Status::kOk;
}

}  // namespace raftkv
