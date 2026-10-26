// The manifest: which SSTables are live, and how far flushing has got. docs/sstable-format.md
// Appendix A.4 says the caller owns this; the engine uses it (KVEngine with Options::use_sstables).
//
// File "MANIFEST" in the engine directory. It always holds the COMPLETE state and is replaced as a
// whole by temp file + fsync + rename + directory fsync, so any edit, however many files it adds and
// removes, is all-or-nothing: after a crash the old state or the new state is there, never a mix.
//
// Layout (little endian):
//   header   u64 magic "RKVMANI1" | u32 format_version | u32 reserved (0)
//   records  repeated: u16 type | u16 flags | u32 payload_length | payload
//   trailer  u32 CRC-32 of every byte before it
// Records:
//   1  next_file_number   u64   highest file/WAL number ever handed out, plus one. WAL sequence
//                               numbers and SSTable file numbers come from this one counter.
//   2  flushed_through    u64   every memtable (WAL) with sequence <= this is in a live-or-merged
//                               SSTable: its WAL is retired. Never derived from the applied index.
//   3  sstable            u64 file_number | u64 min_seq | u64 max_seq   (one per live file)
// Forward compatibility: a record with an unknown type is skipped if flags bit 0 (REQUIRED) is
// clear and refuses the whole file if it is set; a format_version above kManifestFormatVersion
// is refused.
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "storage/kv_store.h"
#include "storage/sstable.h"

namespace raftkv {

constexpr uint64_t kManifestMagic = 0x31494E414D564B52ULL;  // "RKVMANI1"
constexpr uint32_t kManifestFormatVersion = 1;
constexpr uint16_t kManifestRequired = 1;  // record flag: readers that do not know the type must refuse

enum ManifestRecordType : uint16_t { kManifestNextFileNumber = 1, kManifestFlushedThrough = 2, kManifestSstable = 3 };

struct ManifestState {
  uint64_t next_file_number = 1;
  uint64_t flushed_through = 0;
  std::vector<SSTableMeta> files;  // live SSTables, oldest first (ascending, disjoint [min_seq, max_seq])
};

// "MANIFEST" inside `dir`.
std::string ManifestPath(const std::string& dir);

// Serialises / parses a manifest. Decode returns kCorruption for anything it cannot trust: bad
// magic, bad checksum, truncation, a newer format, an unknown REQUIRED record, or a state that
// breaks the invariants (duplicate file numbers, overlapping ranges, min_seq > max_seq, a file
// number >= next_file_number).
std::string EncodeManifest(const ManifestState& state);
Status DecodeManifest(const std::string& bytes, ManifestState* out);

// kOk with *found = false if there is no manifest (a fresh directory); kCorruption if it is damaged.
Status LoadManifest(const std::string& dir, ManifestState* out, bool* found);
// Atomically replaces the manifest. kIOError on failure; the previous manifest is then still valid.
Status StoreManifest(const std::string& dir, const ManifestState& state);

}  // namespace raftkv
