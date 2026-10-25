// Immutable sorted string table (SSTable), format version 1: docs/sstable-format.md.
//
// SSTableBuilder writes a table from strictly increasing entries and publishes it atomically
// (temp file, fsync, link, directory fsync; §12). SSTable opens a published table, validates its
// structure (§10.1) and serves point lookups (§11.2) and ordered scans (§11.3). Data blocks are
// read on demand with pread.
//
// Status mapping (§10.3):
//   kInvalidArgument  invalid caller arguments and builder rules W1–W4 (§12.1)
//   kCorruption       malformed or damaged file bytes, including a footer whose file_number
//                     differs from the expected one (§10.1 step 2)
//   kIOError          failed system calls, including publishing over an existing file (§12.2)
#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "storage/bloom_filter.h"
#include "storage/iterator.h"
#include "storage/kv_store.h"

namespace raftkv {

// Footer metadata supplied by the caller (§8). Not interpreted by the SSTable code beyond
// min_seq <= max_seq.
struct SSTableMeta {
  uint64_t file_number = 0;
  uint64_t min_seq = 0;
  uint64_t max_seq = 0;
};

struct SSTableBuilderOptions {
  uint32_t bits_per_key = BloomFilter::kDefaultBitsPerKey;  // §7.4, must be in [1, 64]
};

// Format constants (§4, §5.1, §8).
struct SSTableFormat {
  static constexpr uint32_t kVersion = 1;
  static constexpr uint64_t kMaxKeyValueBytes = uint64_t{1} << 26;
  static constexpr size_t kBlockTargetBytes = 4096;
  static constexpr uint64_t kMaxBlockPayload = (uint64_t{1} << 26) + 8192;
  static constexpr size_t kRecordHeaderBytes = 9;
  static constexpr size_t kBlockTrailerBytes = 4;
  static constexpr size_t kIndexEntryHeaderBytes = 16;
  static constexpr size_t kFooterBytes = 100;
};

// Writes one SSTable. Not thread-safe. Destroying an unfinished builder abandons it.
class SSTableBuilder {
 public:
  // Validates the arguments (kInvalidArgument: empty final_path, min_seq > max_seq,
  // bits_per_key outside [1, 64]), then fails with kIOError if final_path already exists, then
  // creates the temporary file "<final_path>.tmp.XXXXXX" (§12.2 P0–P1). On error nothing is left
  // behind and *out is unchanged.
  static Status Create(const std::string& final_path, const SSTableMeta& meta,
                       const SSTableBuilderOptions& options, std::unique_ptr<SSTableBuilder>* out);

  ~SSTableBuilder();
  SSTableBuilder(const SSTableBuilder&) = delete;
  SSTableBuilder& operator=(const SSTableBuilder&) = delete;

  // Appends one entry. kInvalidArgument for W1 (key not strictly greater than the previous key),
  // W2 (tombstone with a value), W3 (key + value > 2^26) and W4 (builder already failed, finished
  // or abandoned). kIOError if writing a completed block fails. Any error except W4 moves the
  // builder to the failed state and deletes its temporary file.
  Status Add(const Entry& entry);

  // Writes the filter, index and footer, then publishes final_path (§12.2 P2–P7). kOk means the
  // complete file is durable under final_path. On error final_path is not published by this
  // builder (best-effort cleanup, §12.2) and the builder is failed. W4 applies.
  Status Finish();

  // Closes and deletes the temporary file. Idempotent; a no-op after a successful Finish().
  void Abandon();

  uint64_t entry_count() const { return entry_count_; }

 private:
  enum class State { kOpen, kFailed, kFinished, kAbandoned };

  SSTableBuilder(std::string final_path, std::string temp_path, int fd, const SSTableMeta& meta,
                 uint32_t bits_per_key);
  Status FlushBlock();
  Status WriteBytes(const std::string& bytes);
  Status Fail(Status s);  // enter kFailed, close and delete the temp file, return s
  void CloseFd();

  const std::string final_path_;
  const std::string temp_path_;
  int fd_;
  const SSTableMeta meta_;
  const uint32_t bits_per_key_;
  State state_ = State::kOpen;
  bool temp_exists_ = true;  // the builder created temp_path_ and has not removed it

  std::string block_;      // record bytes of the block being built
  std::string index_;      // serialized index entries of completed blocks
  std::vector<uint64_t> key_hashes_;
  std::string last_key_;
  uint64_t offset_ = 0;    // bytes written to the temp file so far
  uint64_t entry_count_ = 0;
  uint64_t block_count_ = 0;
};

// Convenience for flush: Create, Add every entry of `it` in order, Finish. `it` must be a
// non-failing iterator (e.g. MemTable::Iterator); KVIterator cannot report errors.
Status WriteSSTable(const std::string& final_path, const SSTableMeta& meta,
                    const SSTableBuilderOptions& options, KVIterator* it);

// A published, validated SSTable. Immutable after Open: Get may be called concurrently.
class SSTable {
 public:
  enum class GetResult { kFound, kDeleted, kAbsent };  // §11.2

  // Opens and validates `path` in the order of §10.1. kIOError if the file cannot be opened or
  // read; kCorruption for any structural violation, including a footer file_number different
  // from expected_file_number. On error *out is unchanged.
  static Status Open(const std::string& path, uint64_t expected_file_number,
                     std::unique_ptr<SSTable>* out);

  ~SSTable();
  SSTable(const SSTable&) = delete;
  SSTable& operator=(const SSTable&) = delete;

  // On kOk sets *result; *value is set only for kFound (the value may be empty). The Bloom filter
  // is used only to answer kAbsent. On error (kIOError / kCorruption from reading or validating
  // the candidate block) *result and *value are unchanged; an error is never reported as kAbsent.
  Status Get(const std::string& key, GetResult* result, std::string* value) const;

  const SSTableMeta& meta() const { return meta_; }
  uint64_t entry_count() const { return entry_count_; }
  uint64_t block_count() const { return index_.size(); }

  // Ordered scan over every entry, tombstones included (§11.3). Holds one decoded block at a
  // time and validates each block per §10.2. When Valid() becomes false, status() says why:
  // kOk is a clean end (every record read and the count equals entry_count); kIOError or
  // kCorruption means the scan stopped on an error. Errors are sticky; Next() on an invalid
  // iterator is a no-op. Current() is valid only while Valid(), until the next Next().
  // The SSTable must outlive the iterator. A single iterator is not thread-safe.
  class Iterator : public KVIterator {
   public:
    bool Valid() const override { return status_ == Status::kOk && pos_ < entries_.size(); }
    void Next() override;
    const Entry& Current() const override { return entries_[pos_]; }
    Status status() const { return status_; }

   private:
    friend class SSTable;
    explicit Iterator(const SSTable* table);
    void LoadNextBlock();

    const SSTable* table_;
    size_t next_block_ = 0;
    std::vector<Entry> entries_;  // the current block
    size_t pos_ = 0;
    uint64_t records_seen_ = 0;
    Status status_ = Status::kOk;
  };
  std::unique_ptr<Iterator> NewIterator() const;

 private:
  struct IndexEntry {
    uint64_t offset;
    uint32_t size;  // record bytes, excluding the 4-byte trailer
    std::string last_key;
  };

  SSTable() = default;
  // Reads block `i` and validates it per §10.2, decoding its records into *entries.
  Status ReadBlock(size_t i, std::vector<Entry>* entries) const;

  int fd_ = -1;
  SSTableMeta meta_;
  uint64_t entry_count_ = 0;
  std::vector<IndexEntry> index_;
  BloomFilter filter_;
};

}  // namespace raftkv
