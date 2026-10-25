// SSTable compaction: docs/sstable-format.md §13.
//
// Merges a contiguous run of live SSTables into one new SSTable. Versions of a key are ordered by
// the max_seq of the file that holds them (entries carry no sequence numbers of their own; within
// one file each key appears at most once). The newest version wins; tombstones are kept unless
// dropping them is explicitly requested and permitted (C7).
//
// Compaction only reads its inputs and publishes one output through SSTableBuilder (§12). It never
// modifies or deletes inputs and never touches a manifest: installing the result (manifest edit,
// live-list swap, deleting inputs) is the caller's job (Appendix A.4).
//
// Status mapping (§10.3, §13.2, §13.4):
//   kInvalidArgument  C1–C7 violations and other invalid caller arguments (null input pointer,
//                     invalid builder options); always returned before any file is created
//   kCorruption /     reported by an input's iterator (passed through unchanged);
//   kIOError          kIOError also from publishing the output (§12.2)
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "storage/kv_store.h"
#include "storage/sstable.h"

namespace raftkv {

struct CompactionOptions {
  // Omit a key whose newest version is a tombstone (and all its older versions). Permitted only
  // when the inputs start at the oldest live file (C7); otherwise the call is rejected.
  bool drop_tombstones = false;
  SSTableBuilderOptions builder;  // options for the output table
};

struct CompactionResult {
  SSTableMeta output;           // {output_file_number, lo, hi} as written to the output footer
  uint64_t entries_written = 0;  // records in the output, tombstones included
};

// Checks C1–C7 (§13.2) for `inputs` against `live_files` and computes the output metadata
// {output_file_number, S[a].min_seq, S[b].max_seq} (§13.3). Pure: no I/O. `inputs` may be given
// in any order. Returns kInvalidArgument on any violation (and for a null input pointer or a null
// `output`); on error *output is unchanged.
Status ValidateCompaction(const std::vector<const SSTable*>& inputs,
                          const std::vector<SSTableMeta>& live_files,
                          uint64_t output_file_number, bool drop_tombstones, SSTableMeta* output);

// Validates (C1–C7), merges the inputs with a k-way min-heap (§13.4) and publishes the result at
// `output_path` through SSTableBuilder. An output is written even if it has no entries. On any
// error the output is abandoned and not published; inputs are never modified. `result` may be
// null; on error *result is unchanged.
Status Compact(const std::vector<const SSTable*>& inputs,
               const std::vector<SSTableMeta>& live_files, const std::string& output_path,
               uint64_t output_file_number, const CompactionOptions& options,
               CompactionResult* result);

}  // namespace raftkv
