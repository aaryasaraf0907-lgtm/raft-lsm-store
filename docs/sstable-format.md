# RaftKV SSTable Format — Version 1

| | |
|---|---|
| Status | Draft for review (PR 1, branch `docs/sstable-format`) |
| Owner | Aditya Y. (storage read path) |
| Reviewers | Aansh (primary — write path / flush), Aarya (second — Raft) |
| Implements | `src/storage/bloom_filter.{h,cpp}`, `src/storage/sstable.{h,cpp}`, `src/storage/compaction.{h,cpp}` |

Sections 1–14 are **normative**: the writer, the reader and compaction MUST follow them
exactly, and a file that violates them MUST be rejected. Appendix A is **informational**: it
records integration dependencies (manifest, engine, WAL lifecycle, Raft recovery) that these
modules rely on but do **not** implement. Appendix B gives reference values for tests.

The key words MUST, MUST NOT, SHOULD and MAY are used in their usual specification sense.

---

## 1. Terms

| Term | Meaning |
|---|---|
| Entry | `raftkv::Entry` from `src/storage/iterator.h`: `key`, `value`, `tombstone`. |
| Record | The on-disk encoding of one entry (§4). |
| Data block | A run of whole records followed by a 4-byte CRC trailer (§5). |
| Record bytes | The bytes of a block excluding its trailer. The index field `size` counts only these. |
| Data region | Bytes `[0, filter_offset)`: all data blocks, back to back. |
| Filter section | Serialized Bloom filter (§7). |
| Index section | One entry per data block (§6). |
| Footer | Fixed 100-byte trailer at the end of the file (§8). |
| `file_number` | Unique identity of an SSTable. Supplied by the caller; never interpreted as recency. |
| Sequence range | `[min_seq, max_seq]`: the flush sequences whose data the file contains. Defines recency between files. |
| Live file list | The caller-supplied list of `{file_number, min_seq, max_seq}` of every live SSTable (§13). |
| Publish | Making a completely written file durable under its final name (§12). |

Notation: `u8/u16/u32/u64` are unsigned integers. `[a, b)` is a half-open byte range. Every
offset in this document is an **absolute** byte offset from the start of the file unless
explicitly called block-relative (only §4's record parsing is block-relative).

## 2. Encoding conventions

- **Byte order.** Every multi-byte integer is little-endian with the fixed width stated.
  Integers are encoded and decoded byte by byte; no struct is ever `memcpy`'d to or from disk.
- **CRC.** `raftkv::Crc32` from `src/storage/wal.h`: IEEE CRC-32 (reflected polynomial
  `0xEDB88320`, initial value `0xFFFFFFFF`, final XOR `0xFFFFFFFF`). Check value:
  `Crc32("123456789") == 0xCBF43926`. The CRC of zero bytes is `0`, so the rule "a section's
  CRC is `Crc32` of its bytes" also holds for empty sections. CRCs detect accidental damage
  (torn writes, bit rot, truncation); they are not a defence against deliberate tampering.
- **Magic.** The 8 bytes `52 4B 56 53 53 54 30 31` (ASCII `RKVSST01`) in file order. The
  magic is compared as a byte string, never as an integer.
- **Key order.** Unsigned bytewise lexicographic order, exactly `std::string::compare`
  (which compares as `unsigned char`). A proper prefix sorts first; the empty key is the
  smallest key. This is the order of the existing `SkipList`/`MemTable`. No other comparator
  is permitted.
- **Arithmetic.** All bounds checks use `u64` and are written so they cannot overflow: a check
  of `a + b <= limit` is performed as `a <= limit && b <= limit - a`; products are bounded by
  division (`n <= max / m`) before multiplying.

## 3. File layout

```
offset 0
+-----------------------------------------------+
| data block 0  : record bytes | crc32 (4)      |
| data block 1  : record bytes | crc32 (4)      |
| ...                                           |
| data block n-1: record bytes | crc32 (4)      |
+-----------------------------------------------+ filter_offset  (= end of data region)
| filter section                (filter_len)    |
+-----------------------------------------------+ index_offset
| index section                 (index_len)     |
+-----------------------------------------------+ file_size - 100
| footer                        (100 bytes)     |
+-----------------------------------------------+ file_size
```

Layout invariants (every one is checked by the reader, §10):

- **L1** `file_size >= 100`.
- **L2** `filter_offset + filter_len == index_offset`.
- **L3** `index_offset + index_len == file_size - 100`.
- **L4** Data blocks are contiguous from offset 0 and the last block's trailer ends exactly at
  `filter_offset` (§5.3). There are no gaps or padding anywhere in the file.

## 4. Records

A record is block-relative: it starts where the previous record in the same block ended.

| Field | Width | Rule |
|---|---|---|
| `flags` | u8 | Bit 0 = tombstone. Bits 1–7 MUST be 0. |
| `key_len` | u32 | Length of `key` in bytes. 0 is valid (empty key). |
| `value_len` | u32 | Length of `value` in bytes. MUST be 0 when the tombstone bit is set. |
| `key` | `key_len` bytes | Arbitrary bytes, including NUL. |
| `value` | `value_len` bytes | Arbitrary bytes, including NUL. |

The record header is 9 bytes; the smallest record (empty key, empty value) is 9 bytes.

**Size limit.** `kMaxKeyValueBytes = 2^26` (67,108,864). Every record MUST satisfy
`key_len + value_len <= kMaxKeyValueBytes`. Relationship to the WAL (`wal.cpp`): `WAL::Append`
rejects `1 + 4 + key + value > WAL::kMaxPayloadBytes (2^26)`, i.e. the WAL accepts
`key + value <= 2^26 - 5`. Therefore every entry that reached the MemTable through the WAL fits
in an SSTable record. `MemTable::Put` itself has no limit; an entry inserted without the WAL that
exceeds `kMaxKeyValueBytes` is rejected by the writer with `kInvalidArgument` (§12.1, W3).

**Deletion semantics.** The tombstone bit is the only deletion marker:

| `flags` bit 0 | `value_len` | Meaning |
|---|---|---|
| 0 | 0 | Key present with an **empty value** (a valid stored value, not a delete). |
| 0 | > 0 | Key present with that value. |
| 1 | 0 | Tombstone: key deleted. |
| 1 | > 0 | Invalid — writer rejects the entry with `kInvalidArgument` (W2); reader reports `kCorruption`. |

Within one file each key appears at most once (§5.2).

## 5. Data blocks

### 5.1 Constants and cutting rule

- `kBlockTargetBytes = 4096`.
- `kMaxBlockPayload = 2^26 + 8192` (67,117,056; fits in the u32 `size` field).

The writer appends whole records to the current block. **Before** appending a record, if the
current block's record bytes are `>= kBlockTargetBytes`, the writer closes that block and starts
a new one. Consequently: a block is never empty; a record never spans blocks; a block holds
fewer than 4096 record bytes before its last record, so its size is at most
`4095 + 9 + 2^26 = 2^26 + 4104 <= kMaxBlockPayload`. A single large record produces one
oversized block. The reader accepts any block size in `[9, kMaxBlockPayload]` regardless of how
the writer cut it.

Rationale for 4 KiB: matches the common page size, keeps the in-memory index at roughly one
entry per 4 KiB of data, and makes a linear scan of one block cheap enough that no intra-block
restart points are needed. Point lookup cost: `O(log block_count)` index comparisons plus one
block read and scan.

### 5.2 Block contents

For block `i` with `size_i` record bytes:

- Record bytes occupy `[offset_i, offset_i + size_i)`.
- The trailer is a u32 at `[offset_i + size_i, offset_i + size_i + 4)` and equals
  `Crc32(record bytes)`.
- Records parse exactly to `size_i`: every header and payload lies inside the record bytes and
  the last record ends at `size_i` with no leftover bytes.
- Keys are strictly increasing within the block, and the first key of block `i+1` is strictly
  greater than the last key of block `i`. Hence keys are strictly increasing across the file
  and unique.
- The last key of block `i` equals the index entry's `last_key` for block `i`.

### 5.3 Block offsets

```
offset_0     = 0
offset_{i+1} = offset_i + size_i + 4          (4 = trailer)
offset_{n-1} + size_{n-1} + 4 == filter_offset
```

`n = block_count`. The index stores `offset_i` explicitly; the reader recomputes the expected
value and MUST reject any mismatch.

## 6. Index section

`block_count` entries, in block order, back to back:

| Field | Width | Rule |
|---|---|---|
| `offset` | u64 | MUST equal `offset_i` from §5.3. |
| `size` | u32 | Record bytes of the block, **excluding** the 4-byte trailer. `9 <= size <= kMaxBlockPayload`. |
| `last_key_len` | u32 | `<= kMaxKeyValueBytes`. |
| `last_key` | `last_key_len` bytes | Last key stored in the block. |

Each entry is `16 + last_key_len` bytes. The entries MUST consume exactly `index_len` bytes.
`last_key` values MUST be strictly increasing. `index_crc == Crc32(index section bytes)`.
The index has no count field; `block_count` lives only in the footer.

**Lookup.** For target key `t`, binary-search for the first entry with `last_key >= t`. If none
exists, `t` is absent from the file. Otherwise `t` can only be in that block.

## 7. Bloom filter

### 7.1 Hash

```
u64 Fnv1a64(bytes):                 // FNV-1a, 64-bit
  h = 0xcbf29ce484222325
  for each byte b: h = h XOR b; h = h * 0x100000001b3   (mod 2^64)
  return h

u64 Fmix64(h):                      // MurmurHash3 64-bit finalizer
  h ^= h >> 33;  h *= 0xff51afd7ed558ccd
  h ^= h >> 33;  h *= 0xc4ceb9fe1a85ec53
  h ^= h >> 33
  return h

BloomHash(key) = Fmix64(Fnv1a64(key bytes))
```

`hash_kind = 1` identifies exactly this function. The hash covers the full key bytes only; the
tombstone flag and value are not hashed.

### 7.2 Probes

```
h  = BloomHash(key)
h1 = h & 0xFFFFFFFF
h2 = (h >> 32) | 1                  // odd, never 0
for i in 0 .. k-1:
  bit_i = (h1 + i * h2) mod m       // computed in u64; max < 2^32 + 29*2^32 < 2^37
```

`k = num_probes`, `m = num_bits`.

### 7.3 Bit ordering

Bit `b` is stored in byte `b >> 3` of the bit array, at bit position `b & 7`, where position 0
is the least-significant bit: `set: bytes[b >> 3] |= 1 << (b & 7)`. Bytes are stored in
ascending index order.

### 7.4 Building (writer)

- `bits_per_key` is a writer option in `[1, 64]`; default 10. Other values are rejected with
  `kInvalidArgument`.
- `k = clamp(bits_per_key * 69 / 100, 1, 30)` using integer division (default → 6).
- For `n >= 1` keys: `m = max(64, round_up_to_multiple_of_8(n * bits_per_key))`. The writer
  MUST check `n <= (2^64 - 1 - 7) / bits_per_key` before multiplying and otherwise fail with
  `kInvalidArgument` (likewise if the `m / 8`-byte bit array cannot be addressed by `size_t`).
- Every key written to the file is inserted, **tombstones included** (a tombstone must be found
  so that it shadows older files).
- The writer keeps one u64 `BloomHash` per key while writing (8 bytes of memory per key) and
  builds the filter at `Finish()`, because a single-pass iterator does not reveal `n` in advance.
- The Bloom module MAY build a filter for `n = 0` (64 zero bits; every query returns "not
  present") for unit testing. An SSTable never stores such a filter (§9).

### 7.5 Serialization

| Field | Width | Rule |
|---|---|---|
| `hash_kind` | u8 | MUST be 1. |
| `num_probes` | u8 | `1 <= k <= 30`. |
| `reserved` | u16 | MUST be 0. |
| `num_bits` | u64 | `m >= 64`, `m % 8 == 0`. |
| `bits` | `m / 8` bytes | Bit array (§7.3). |

Serialized length is `12 + m / 8`. `filter_crc == Crc32(filter section bytes)`.

### 7.6 Validation (reader)

With `filter_len` and `entry_count` already bounded by the file size (§10):

1. `filter_len >= 20`.
2. `hash_kind == 1`, `1 <= num_probes <= 30`, `reserved == 0`.
3. `num_bits % 8 == 0`, `num_bits >= 64`, and `num_bits / 8 == filter_len - 12`
   (division form; never compute `8 * filter_len`).
4. `num_bits / 64 + (num_bits % 64 != 0 ? 1 : 0) <= max(1, entry_count)`
   (equivalent to `num_bits <= 64 * max(1, entry_count)` without overflow).

### 7.7 Query semantics

`MayContain(key) == false` means the key is **definitely not** in the file, and the reader MAY
skip the file — only after the filter passed §7.6 and its CRC matched, which binds it to this
file. `MayContain(key) == true` means "possibly present"; the reader MUST consult the index.
A filter never produces a false negative for a key that was inserted.

## 8. Footer (100 bytes)

Located at `[file_size - 100, file_size)`.

| Offset | Width | Field | Rule |
|---|---|---|---|
| 0 | u32 | `version` | MUST be 1. |
| 4 | u32 | `reserved` | MUST be 0. |
| 8 | u64 | `file_number` | Caller-supplied identity. |
| 16 | u64 | `min_seq` | Caller-supplied. |
| 24 | u64 | `max_seq` | `min_seq <= max_seq`. |
| 32 | u64 | `entry_count` | Number of records (tombstones included). |
| 40 | u64 | `block_count` | Number of data blocks and index entries. |
| 48 | u64 | `filter_offset` | Start of filter section = end of data region. |
| 56 | u64 | `filter_len` | |
| 64 | u64 | `index_offset` | |
| 72 | u64 | `index_len` | |
| 80 | u32 | `filter_crc` | `Crc32` of the filter section. |
| 84 | u32 | `index_crc` | `Crc32` of the index section. |
| 88 | u32 | `footer_crc` | `Crc32` of footer bytes `[0, 88)`. |
| 92 | 8 bytes | `magic` | `52 4B 56 53 53 54 30 31`. |

Total: `4 + 4 + 9×8 + 3×4 + 8 = 100`. Magic is last so that truncation or appended bytes are
detected first.

## 9. Empty SSTable

A file with no entries is valid. It is exactly 100 bytes (the footer only) with
`entry_count = block_count = filter_offset = filter_len = index_offset = index_len = 0` and
`filter_crc = index_crc = 0`. `file_number`, `min_seq` and `max_seq` carry their normal meaning
(a compaction output may be empty and still covers its sequence range).

- If `entry_count == 0`, then `block_count`, `filter_offset`, `filter_len`, `index_offset` and
  `index_len` MUST all be 0. If `entry_count > 0`, then `block_count >= 1` and `filter_len > 0`
  (a non-empty file always has a filter).
- Reader on an empty file: `Get` returns absent without consulting a filter or index; a new
  iterator is immediately `!Valid()` with `status() == kOk`.
- A zero-byte file, or any file shorter than 100 bytes, is `kCorruption`, not an empty table.

Example (`file_number = 1`, `min_seq = max_seq = 1`) — see Appendix B.2.

## 10. Validation and error mapping

### 10.1 Open-time validation (in this order)

1. `fstat`: `file_size >= 100` (L1). The file size is taken once at open and all later checks
   use it.
2. Read the footer. Check, in order: magic; `footer_crc`; `version == 1`; `reserved == 0`;
   `file_number == expected_file_number`; `min_seq <= max_seq`.
3. Region bounds (overflow-safe): L2 and L3; therefore every section lies within
   `[0, file_size - 100)`.
4. Empty-table rules (§9). For a non-empty table: `block_count <= filter_offset / 13`
   (minimum block = 9 + 4 bytes) and `entry_count <= (filter_offset - 4 * block_count) / 9`
   (minimum record = 9 bytes); `index_len >= 16 * block_count` is checked as
   `block_count <= index_len / 16`.
5. Read the index section (`index_len` bytes, already bounded by the file size); check
   `index_crc`; parse exactly `block_count` entries with the §6 rules, recomputing offsets per
   §5.3 with `offset_i + size_i + 4 <= filter_offset` at each step; after the last entry the
   parse position MUST equal `index_len` and the next expected offset MUST equal
   `filter_offset`.
6. Read the filter section; check `filter_crc`; validate per §7.6.

No allocation is made from a length that has not first been bounded by the file size. Data
blocks are **not** read at open time.

### 10.2 Block-read validation (every time a block is read)

1. `pread` exactly `size_i + 4` bytes at `offset_i` (both already validated).
2. Trailer equals `Crc32(record bytes)`.
3. Each record header (9 bytes) and its `key_len + value_len` fit in the remaining record bytes;
   `key_len + value_len <= kMaxKeyValueBytes`; flags and tombstone rules of §4.
4. Keys strictly increasing; the first key is greater than the previous block's `last_key`
   (from index entry `i - 1`); the last key equals this block's `last_key`; records end exactly
   at `size_i`.

A full scan (iterator reaching the end) additionally checks that the number of records read
equals `entry_count`.

### 10.3 Error mapping (existing `raftkv::Status`)

| Condition | Status |
|---|---|
| Key absent or deleted (lookup outcome, §11.2) | not an error; reported separately |
| `open`, `fstat`, `pread`, `write`, `fsync`, `mkstemp`, `link`, `unlink` system-call failure (incl. missing file); `EINTR` is retried | `kIOError` |
| Any structural, CRC, limit, ordering or count violation; unsupported `version`; non-zero reserved field | `kCorruption` |
| `pread` returning fewer bytes than a range already validated against the open-time file size | `kCorruption` |
| Invalid API arguments or caller-supplied metadata, detected before any file data is trusted: Bloom `bits_per_key` or sizing overflow (§7.4); builder parameters and `Add` rules W1–W4 (§12.1); compaction checks C1–C7, including zero inputs (§13.2) | `kInvalidArgument` |

`kNotFound` is never used for a file-level problem. The split between the last row and the
`kCorruption` rows is by **where the bad value comes from**: a value supplied by the caller
through the API is `kInvalidArgument`; a value read from a file (footer, index, filter or block
bytes) is `kCorruption`, even when the caller wrote that file earlier. `kInvalidArgument` is never
retryable with the same arguments (see `kv_store.h`).

## 11. Reader and iterator

### 11.1 Reader

- `Open(path, expected_file_number)` performs §10.1 and on success holds the file descriptor,
  footer, index and filter in memory (bounded by the file size). Data is read on demand with
  `pread`.
- After `Open` the reader is immutable; concurrent `Get` calls and concurrent iterators on the
  same reader are safe. A single iterator object is not thread-safe.
- The reader MUST outlive all iterators created from it. The file descriptor is closed when the
  reader is destroyed.

### 11.2 Point lookup

`Get(key)` yields a `Status` plus, when the status is `kOk`, one of:

| Outcome | Meaning | Engine action |
|---|---|---|
| Found(value) | Record present, tombstone bit 0 (value may be empty). | Return value; stop searching. |
| Deleted | Record present, tombstone bit 1. | Report not found; **stop** searching older files. |
| Absent | Key not in this file. | Continue to the next older file. |

Algorithm: empty table → Absent. Bloom `MayContain == false` → Absent. Index search (§6); no
candidate → Absent. Read and validate the candidate block (§10.2) and scan it. Any error is
returned as the status; an error is never reported as Absent.

### 11.3 Iterator

The SSTable iterator implements `raftkv::KVIterator` unchanged and adds one method on the
concrete class only: `Status status() const`.

- Holds at most one decoded block in memory; reads blocks in order with §10.2 validation.
- `Current()` is valid only while `Valid()`; the returned reference remains valid until the next
  `Next()` or destruction.
- When `Valid()` becomes false, `status()` says why:

| `Valid()` | `status()` | Meaning |
|---|---|---|
| true | `kOk` | Positioned on an entry. |
| false | `kOk` | Clean end: every record read and count equals `entry_count`. |
| false | `kIOError` / `kCorruption` | Iteration stopped by an error. |

- Errors are sticky. `Next()` on an invalid iterator is a no-op. Consumers that must not treat
  corruption as end-of-data (compaction) MUST check `status()` whenever `Valid()` is false.

## 12. Writer and publication

### 12.1 Builder input

The builder is created with `final_path`, `file_number`, `min_seq`, `max_seq` (`min_seq <=
max_seq`, else rejected with `kInvalidArgument`) and `bits_per_key` (§7.4). It exposes `Add(entry)`, `Finish()` and
`Abandon()`; destroying an unfinished builder is equivalent to `Abandon()`. A convenience
function MAY drain a `KVIterator` into a builder (used by flush; `MemTable::Iterator` cannot fail).

`Add` returns `kInvalidArgument`, and puts the builder into a failed state, when:

- **W1** the key is not strictly greater than the previous key (duplicates included);
- **W2** `tombstone == true` and `value` is non-empty;
- **W3** `key.size() + value.size() > kMaxKeyValueBytes`;
- **W4** the builder has already failed, finished or been abandoned.

After any failure every later call fails and `Finish()` MUST NOT publish. A later call rejected
under W4 returns `kInvalidArgument`; the original failure (including any `kIOError`) was already
returned by the call that caused it.

### 12.2 Publication protocol

| Step | Action | On failure |
|---|---|---|
| P0 | If `final_path` already exists, fail. (Advisory only; P4 is the real guarantee.) | `kIOError`; nothing created. |
| P1 | `mkstemp("<final_path>.tmp.XXXXXX")`. The template is `final_path` plus the suffix `.tmp.` and ends in the six `X` characters POSIX `mkstemp()` requires, so the temp file is in the same directory as `final_path`; `mkstemp` creates it exclusively under a unique name with mode 0600. On success the builder **owns** the temp name. Then set `FD_CLOEXEC` with `fcntl` and `fchmod` the descriptor to 0644. | If `mkstemp` fails: `kIOError`; the builder created no file. If `mkstemp` succeeded but `fcntl` or `fchmod` fails: `close` the descriptor, then `unlink(temp)`; if that `unlink` fails, the temp file is left behind (best-effort cleanup, garbage per Appendix A.5); `kIOError`. |
| P2 | Write all blocks, filter, index, footer to the temp file. | Close; `unlink(temp)`; `kIOError`. |
| P3 | Full sync of the temp file (`fsync`; `F_FULLFSYNC` on macOS, as the WAL does) and `close`. | `unlink(temp)`; `kIOError`. |
| P4 | `link(temp, final_path)`. Atomic and fails if `final_path` exists. Success means the builder **created and owns** `final_path`. | `unlink(temp)` only; `final_path` is **not** touched (the builder did not create it); `kIOError`. |
| P5 | `unlink(temp)`. | `unlink(final_path)` (owned since P4), retry `unlink(temp)` once; `kIOError`. |
| P6 | Full sync of the directory (open the directory with `O_RDONLY` and `O_DIRECTORY`, then `fsync`). | `unlink(final_path)` (owned since P4); `kIOError`. |
| P7 | Return `kOk`. | — |

Rules:

- The builder never uses `rename`, never opens an existing file for writing, and never
  truncates or unlinks a name it did not create (it creates exactly two names: the temp name in
  P1 and `final_path` in a successful P4).
- `kOk` means the complete file is durable under `final_path`. It does **not** make the file
  live; only the manifest does (Appendix A.3).
- `kIOError` means the caller MUST treat the file as unpublished: it MUST NOT be added to the
  manifest. Cleanup is best-effort: after a failed `fsync` or `unlink` the on-disk state is
  uncertain, so a temp file and/or `final_path` may remain, or reappear after a crash. Such
  files are unreferenced by the manifest and are garbage (Appendix A.5). The caller SHOULD retry
  with a new `file_number`, since a leftover `final_path` would make the same number fail at P4.
- Two builders can never share a temp name (P1). Two builders given the same `final_path` (an
  engine bug) cannot both publish: the second fails at P4 and the first file is untouched.
- Platform requirement: `link()` and directory `fsync` must be supported (ext4, XFS, APFS do).
  If `link()` fails (e.g. `EPERM`, `ENOTSUP`), the builder fails with `kIOError`; it does not
  fall back to `rename`.
- Assumption: no other actor renames or creates files named `*.sst` in the data directory
  (Appendix A.5); this is what makes "the name created at P4 is still the builder's at P5/P6"
  hold.

## 13. Compaction

### 13.1 Inputs

- `inputs`: open SSTable readers (not bare `KVIterator`s, so `status()` is always available).
- `live_files`: every live SSTable as `{file_number, min_seq, max_seq}`, supplied by the caller
  from its authoritative live list (Appendix A.3).
- `output_path`, `output_file_number`.
- `drop_tombstones`: default `false`.

Compaction never modifies, deletes or references the manifest, and never deletes inputs.

### 13.2 Metadata validation (before reading any data)

Let `S` be `live_files` sorted by `min_seq`.

- **C1** `live_files` file numbers are unique; every entry has `min_seq <= max_seq`.
- **C2** Ranges in `S` are pairwise disjoint: `S[j].max_seq < S[j+1].min_seq` for all `j`.
- **C3** `inputs` is non-empty (zero inputs are rejected).
- **C4** Input identity: each input's footer `{file_number, min_seq, max_seq}` equals exactly one
  entry of `live_files` (matched by `file_number`, all three fields compared). No input appears
  twice.
- **C5** Contiguity: the positions of the inputs in `S` form one consecutive run
  `S[a..b]` with no non-input entry between them.
- **C6** `output_file_number` is not in `live_files`.
- **C7** If `drop_tombstones`, then `a == 0` (the run starts at the oldest live file).
  Otherwise the call is rejected; it is never silently downgraded to retaining tombstones.

Any violation returns `kInvalidArgument` before any file is created. These checks compare
caller-supplied metadata (`live_files`, `output_file_number`, `drop_tombstones`, the set of
inputs) with footers that `Open` has already validated; corruption in an input file is reported
as `kCorruption` by `Open` or by the input's iterator (§10, §13.4), never by these checks.

### 13.3 Output sequence range

The output range is `[lo, hi] = [S[a].min_seq, S[b].max_seq]`. Given C1–C5:

- **O1** `lo <= hi` — from C1 and the ordering of `S`.
- **O2** The output range covers every input range exactly: by C2 and C5 the inputs' ranges are
  the disjoint pieces of `S[a..b]`, all inside `[lo, hi]`.
- **O3** The output range is disjoint from every non-input live file: any non-input `S[j]` has
  `j < a` (so `S[j].max_seq < S[a].min_seq = lo`) or `j > b` (so `S[j].min_seq > S[b].max_seq =
  hi`).
- **O4** For every non-input live file `F` and every input `I`, `F` is older than the output iff
  `F` is older than `I`. Replacing `S[a..b]` with the output therefore keeps the live list
  disjoint (C2) and does not change the relative recency of any other file.

These are the only properties that make the output range valid; compaction relies on the
caller's `live_files` being accurate and stable for the duration (Appendix A.2, R4).

### 13.4 Merge

- Min-heap ordered by `(key ascending, max_seq descending)`. Because input ranges are disjoint,
  `max_seq` values are distinct, so the order is total and the result deterministic.
- For each key, the entry from the input with the highest `max_seq` (the newest) is selected;
  all other versions of that key are discarded.
- If the selected entry is a tombstone: it is written when `drop_tombstones == false`; it and all
  older versions are omitted when `drop_tombstones == true` (permitted only under C7).
- Empty values are ordinary values and are always kept.
- Output is fed to a builder (§12) with `[lo, hi]`; W1 provides a final ordering check.
- An output is written even if it has no entries (a 100-byte file, §9).
- Errors: after constructing each input iterator and after every `Next()`, if `Valid()` is false
  and `status() != kOk`, compaction calls `Abandon()` and returns that status. Corruption is
  never treated as end-of-input, and a partial output is never published.
- Cost: `O(N log k)` comparisons for `N` input records and `k` inputs; memory is `k` heap entries
  plus one block per input, plus the builder's per-key hash and index.

## 14. Versioning

This document defines format version 1. Any change to a field's width, position or meaning,
or any use of a reserved field, requires a new version number. A reader MUST reject versions it
does not implement (`kCorruption`). Files are never upgraded in place; a newer format is reached
by rewriting through compaction.

---

## Appendix A — Integration dependencies (informational, not implemented by these modules)

### A.1 Ownership

| Implemented by the storage read-path PRs | Integration dependency (other owners) |
|---|---|
| This format | `KVStore` engine: lookup order MemTable → SSTables newest-first |
| `BloomFilter` | Manifest and the authoritative live file list |
| SSTable builder, reader, iterator | Allocation of `file_number` and sequence numbers |
| Compaction merge and §13.2 checks | WAL per MemTable, WAL retirement, MemTable freezing (Aansh) |
| Unit tests | Compaction scheduling and concurrency; data-directory lock; garbage cleanup |
| | Raft restart replay handling; gating reads during replay (Aarya, Aditya K.) |

### A.2 Rules the engine must maintain

- **R1** Flush order: one MemTable accepts writes at a time and is frozen atomically; every write
  in flush `s` happens before every write in flush `s + 1`; sequence numbers increase in freeze
  order.
- **R2** Live sequence ranges are pairwise disjoint.
- **R3** Compaction inputs are contiguous in the live list (enforced by C5 against the supplied list).
- **R4** A file is an input to at most one in-flight compaction, and the live list passed to
  compaction is not changed concurrently in a way that invalidates C1–C7.
- **R5** The manifest is authoritative: a file is live if and only if the manifest lists it.

### A.3 Recommended manifest

An append-only log using the WAL's framing (`[crc32:4][len:4][payload]`), so a torn final record
is detected and discarded. Each record is one atomic edit: add files
`{file_number, min_seq, max_seq, file_size}`, remove files, next `file_number`, retired WAL, and
(if the Raft integration needs it) the applied Raft index covered by a flush. On startup the
live list is exactly the manifest's; a listed file that is missing or whose footer disagrees
with its record is `kCorruption` and startup stops. Directory scanning alone is **not** a safe
substitute: a footer's sequence range does not prove that the file correctly incorporates the
files whose ranges it contains, and it cannot tell which WALs were already flushed.

### A.4 Crash walkthroughs (with a manifest)

Compaction: (1) build and publish output (§12); (2) append and sync manifest edit {add output,
remove inputs}; (3) swap in-memory list; (4) delete inputs.

| Crash point | After restart |
|---|---|
| During (1) | Output unreferenced → garbage; inputs live. |
| After (1), before (2) | Same; retry uses a new `file_number`. |
| During (2), torn record | Record discarded; state before compaction. |
| After (2) | Output live; leftover inputs unreferenced → garbage. |

Flush: (1) publish SSTable; (2) manifest edit {add file, retire WAL `s`}; (3) delete WAL `s`.

| Crash point | After restart |
|---|---|
| After (1), before (2) | SSTable unreferenced → garbage; WAL `s` replayed. |
| After (2), before (3) | WAL `s` retired → deleted without replay. |

### A.5 Data-directory ownership

A single process owns a data directory (lock file). Only builders create `*.sst` names, and only
via §12. At startup, before any builder runs, the engine deletes builder temporary files (names of the form `<final_path>.tmp.XXXXXX`, i.e. a final SSTable path followed by the `.tmp.` segment and the six characters generated by `mkstemp`) and `*.sst` files
not listed in the manifest. Within this exclusively owned data directory, the `.tmp.` namespace
(names of the form `<final_path>.tmp.XXXXXX`) is reserved for SSTable-builder temporary files;
no other component may create files in it. Startup cleanup MUST delete only files that match
this temporary-file form or are unlisted `*.sst` files, and MUST NOT delete any other file in
the directory (for example the manifest, WAL files or the lock file).

### A.6 Tombstone reclamation

`drop_tombstones` is safe only when C7 holds against an accurate live list, the manifest edit
removes all inputs atomically, and unreferenced input files are never re-added. MemTables are
always newer than every SSTable and are unaffected.

### A.7 Raft restart replay

After a restart Raft re-applies the committed log from index 1. The final state converges, but
reads during replay can observe older values, and SSTables flushed mid-replay hold logically old
writes under newer sequence numbers. Resolution (persisting the applied index in the manifest,
or gating reads until replay completes) belongs to the Raft/engine integration. Format version 1
deliberately carries no applied-index field.

### A.8 Open decisions

- **M2** Record limit `key + value <= 2^26` (§4).
- **M3** *Resolved.* `Status::kInvalidArgument` now exists in `kv_store.h`. Invalid API
  arguments and caller-supplied metadata (Bloom parameters, writer input W1–W4, compaction
  checks C1–C7 including zero inputs) return `kInvalidArgument`; malformed or corrupted
  serialized SSTable data remains `kCorruption` (§10.3).
- **M4** Reuse `raftkv::Crc32` from `wal.h`; keep a private full-sync helper in the SSTable code
  (`FullSync` in `wal.cpp` is file-local) or extract a shared helper.
- **M5** Publication protocol of §12.2, and that flush/engine code never opens builder temporary files (`<final_path>.tmp.XXXXXX`, identified by the `.tmp.` segment after the final SSTable path).
- **M6** Compaction interface and checks of §13.
- Engine owner, manifest owner and format, counters, compaction scheduling (before integration,
  not before the module PRs).

---

## Appendix B — Reference values

Computed with a Python reference implementation of §2 and §7 (not with the C++ code, which does
not exist yet). Implementations MUST reproduce them in tests.

### B.1 Bloom hash

| Key (bytes) | `Fnv1a64` | `BloomHash` | `h1` | `h2` | First 6 probes for `m = 64` |
|---|---|---|---|---|---|
| `""` | `0xcbf29ce484222325` | `0xefd01f60ba992926` | `0xba992926` | `0xefd01f61` | 38, 7, 40, 9, 42, 11 |
| `"a"` | `0xaf63dc4c8601ec8c` | `0x82a2a958a9bece5b` | `0xa9bece5b` | `0x82a2a959` | 27, 52, 13, 38, 63, 24 |
| `"raftkv"` | `0xb262c55919df192b` | `0x8147b66b2790f68d` | `0x2790f68d` | `0x8147b66b` | 13, 56, 35, 14, 57, 36 |
| `61 00 62` | `0xe5d29919042666b2` | `0xab78f5eca36d0e2b` | `0xa36d0e2b` | `0xab78f5ed` | 43, 24, 5, 50, 31, 12 |

### B.2 Empty SSTable (`file_number = 1`, `min_seq = max_seq = 1`)

100 bytes; `footer_crc = 0x919ef640`. Hex:

```
01000000 00000000 01000000 00000000 01000000 00000000 01000000 00000000
00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000
00000000 00000000 00000000 00000000 00000000 00000000 40f69e91 524b5653
53543031
```

### B.3 One data block holding key `"a"` with an empty value

Record bytes `00 01000000 00000000 61` (9 + 1 = 10 bytes), trailer `Crc32 = 0xce442dfb`
(stored `fb 2d 44 ce`). Full block: `00010000000000000061fb2d44ce`.
