#include "storage/sstable.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <limits>
#include <string_view>
#include <utility>

#include "storage/wal.h"  // raftkv::Crc32 (IEEE CRC-32, spec §2)

namespace raftkv {
namespace {

using F = SSTableFormat;

constexpr char kMagic[8] = {'R', 'K', 'V', 'S', 'S', 'T', '0', '1'};
constexpr size_t kFooterCrcOffset = 88;
constexpr size_t kMagicOffset = 92;
constexpr uint8_t kTombstoneFlag = 0x01;

void PutU32(std::string* s, uint32_t v) {
  for (int i = 0; i < 4; ++i) s->push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
}

void PutU64(std::string* s, uint64_t v) {
  for (int i = 0; i < 8; ++i) s->push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
}

uint32_t GetU32(const char* p) {
  uint32_t v = 0;
  for (int i = 0; i < 4; ++i) v |= static_cast<uint32_t>(static_cast<unsigned char>(p[i])) << (8 * i);
  return v;
}

uint64_t GetU64(const char* p) {
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) v |= static_cast<uint64_t>(static_cast<unsigned char>(p[i])) << (8 * i);
  return v;
}

uint32_t Crc(const std::string& s) { return Crc32(s.data(), s.size()); }

// Walks the record bytes of one block (raw[0, size); the trailer has already been checked) and
// applies every per-record rule of §10.2 steps 3-4: header and payload in bounds, flags, tombstone
// without a value, strictly increasing keys (the first one greater than prev_last_key, if any),
// records ending exactly at `size`, at least one record, and a last key equal to `last_key`.
// Calls visit(tombstone, key, value) for every record, in order; the views point into `raw`.
// Validation never stops early, whatever the visitor is looking for.
template <typename Visit>
Status ParseBlock(const std::string& raw, size_t size, const std::string* prev_last_key,
                  const std::string& last_key, Visit&& visit) {
  size_t pos = 0;
  std::string_view prev;
  bool have_prev = prev_last_key != nullptr;
  if (have_prev) prev = *prev_last_key;
  while (pos < size) {
    if (size - pos < F::kRecordHeaderBytes) return Status::kCorruption;
    const uint8_t flags = static_cast<uint8_t>(raw[pos]);
    const uint32_t key_len = GetU32(&raw[pos + 1]);
    const uint32_t value_len = GetU32(&raw[pos + 5]);
    pos += F::kRecordHeaderBytes;
    if ((flags & ~kTombstoneFlag) != 0) return Status::kCorruption;
    const bool tombstone = (flags & kTombstoneFlag) != 0;
    if (tombstone && value_len != 0) return Status::kCorruption;
    const uint64_t kv = uint64_t{key_len} + value_len;
    if (kv > F::kMaxKeyValueBytes || kv > size - pos) return Status::kCorruption;
    const std::string_view key(raw.data() + pos, key_len);
    const std::string_view value(raw.data() + pos + key_len, value_len);
    pos += static_cast<size_t>(kv);
    if (have_prev && !(key > prev)) return Status::kCorruption;
    visit(tombstone, key, value);
    prev = key;
    have_prev = true;
  }
  // A block holds at least one record (size >= 9) and its last key must match the index. With no
  // record at all, `prev` is still the previous block's last key (or nothing): reject explicitly.
  if (size == 0 || prev != last_key) return Status::kCorruption;
  return Status::kOk;
}

// Same durability rule as the WAL (§12.2 P3): F_FULLFSYNC on macOS, fsync elsewhere.
bool FullSync(int fd) {
#ifdef __APPLE__
  if (::fcntl(fd, F_FULLFSYNC) == 0) return true;
#endif
  return ::fsync(fd) == 0;
}

bool WriteAll(int fd, const char* p, size_t n) {
  while (n > 0) {
    ssize_t w = ::write(fd, p, n);
    if (w < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    if (w == 0) return false;
    p += w;
    n -= static_cast<size_t>(w);
  }
  return true;
}

// Reads exactly n bytes at `offset`. kIOError on a failed read; kCorruption if the file ends
// early (every range read here was validated against the open-time file size, §10.3).
Status PreadExact(int fd, uint64_t offset, size_t n, std::string* out) {
  out->assign(n, '\0');
  size_t done = 0;
  while (done < n) {
    ssize_t r = ::pread(fd, &(*out)[done], n - done, static_cast<off_t>(offset + done));
    if (r < 0) {
      if (errno == EINTR) continue;
      return Status::kIOError;
    }
    if (r == 0) return Status::kCorruption;
    done += static_cast<size_t>(r);
  }
  return Status::kOk;
}

std::string DirectoryOf(const std::string& path) {
  const size_t slash = path.find_last_of('/');
  if (slash == std::string::npos) return ".";
  if (slash == 0) return "/";
  return path.substr(0, slash);
}

bool SyncDirectory(const std::string& dir) {
  int fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (fd < 0) return false;
  const bool ok = FullSync(fd);
  return ::close(fd) == 0 && ok;
}

}  // namespace

// ---------------------------------------------------------------------------------------------
// Builder

Status SSTableBuilder::Create(const std::string& final_path, const SSTableMeta& meta,
                              const SSTableBuilderOptions& options,
                              std::unique_ptr<SSTableBuilder>* out) {
  if (final_path.empty() || meta.min_seq > meta.max_seq ||
      options.bits_per_key < BloomFilter::kMinBitsPerKey ||
      options.bits_per_key > BloomFilter::kMaxBitsPerKey) {
    return Status::kInvalidArgument;
  }
  // P0 (advisory): refuse an existing final_path. P4's link() is the real guarantee.
  struct stat st;
  if (::lstat(final_path.c_str(), &st) == 0 || errno != ENOENT) return Status::kIOError;

  // P1: unique temp file in the same directory; the template must end in "XXXXXX".
  std::string temp = final_path + ".tmp.XXXXXX";
  int fd = ::mkstemp(&temp[0]);
  if (fd < 0) return Status::kIOError;  // the builder created no file
  const int flags = ::fcntl(fd, F_GETFD);
  if (flags < 0 || ::fcntl(fd, F_SETFD, flags | FD_CLOEXEC) != 0 || ::fchmod(fd, 0644) != 0) {
    ::close(fd);
    ::unlink(temp.c_str());  // best effort: a leftover temp file is garbage (Appendix A.5)
    return Status::kIOError;
  }
  out->reset(new SSTableBuilder(final_path, std::move(temp), fd, meta, options.bits_per_key));
  return Status::kOk;
}

SSTableBuilder::SSTableBuilder(std::string final_path, std::string temp_path, int fd,
                               const SSTableMeta& meta, uint32_t bits_per_key)
    : final_path_(std::move(final_path)),
      temp_path_(std::move(temp_path)),
      fd_(fd),
      meta_(meta),
      bits_per_key_(bits_per_key) {}

SSTableBuilder::~SSTableBuilder() { Abandon(); }

void SSTableBuilder::CloseFd() {
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
}

Status SSTableBuilder::Fail(Status s) {
  state_ = State::kFailed;
  CloseFd();
  if (temp_exists_) {
    ::unlink(temp_path_.c_str());  // best effort (§12.2 P2/P3)
    temp_exists_ = false;
  }
  return s;
}

void SSTableBuilder::Abandon() {
  if (state_ == State::kFinished) return;
  Fail(Status::kOk);  // closes the fd and retries deleting a temp file that still exists
  state_ = State::kAbandoned;
}

Status SSTableBuilder::WriteBytes(const std::string& bytes) {
  if (!WriteAll(fd_, bytes.data(), bytes.size())) return Fail(Status::kIOError);
  offset_ += bytes.size();
  return Status::kOk;
}

Status SSTableBuilder::FlushBlock() {
  if (block_.empty()) return Status::kOk;
  // Block = record bytes + Crc32(record bytes) (§5.2). The index entry records the absolute
  // offset of the block and its size excluding the trailer (§6).
  const uint64_t block_offset = offset_;
  const uint32_t size = static_cast<uint32_t>(block_.size());  // <= kMaxBlockPayload (§5.1)
  PutU32(&block_, Crc(block_));
  Status s = WriteBytes(block_);
  if (s != Status::kOk) return s;
  PutU64(&index_, block_offset);
  PutU32(&index_, size);
  PutU32(&index_, static_cast<uint32_t>(last_key_.size()));
  index_ += last_key_;
  ++block_count_;
  block_.clear();
  return Status::kOk;
}

Status SSTableBuilder::Add(const Entry& entry) {
  if (state_ != State::kOpen) return Status::kInvalidArgument;                         // W4
  if (entry_count_ > 0 && !(entry.key > last_key_)) return Fail(Status::kInvalidArgument);  // W1
  if (entry.tombstone && !entry.value.empty()) return Fail(Status::kInvalidArgument);  // W2
  if (entry.key.size() > F::kMaxKeyValueBytes ||
      entry.value.size() > F::kMaxKeyValueBytes - entry.key.size()) {
    return Fail(Status::kInvalidArgument);  // W3
  }
  // Cutting rule (§5.1): close the current block before appending if it already holds >= 4096
  // record bytes. Blocks are never empty and records never span blocks.
  if (block_.size() >= F::kBlockTargetBytes) {
    Status s = FlushBlock();
    if (s != Status::kOk) return s;
  }
  block_.push_back(static_cast<char>(entry.tombstone ? kTombstoneFlag : 0));
  PutU32(&block_, static_cast<uint32_t>(entry.key.size()));
  PutU32(&block_, static_cast<uint32_t>(entry.value.size()));
  block_ += entry.key;
  block_ += entry.value;
  last_key_ = entry.key;
  key_hashes_.push_back(BloomFilter::Hash(entry.key));  // tombstones included (§7.4)
  ++entry_count_;
  return Status::kOk;
}

Status SSTableBuilder::Finish() {
  if (state_ != State::kOpen) return Status::kInvalidArgument;  // W4
  Status s = FlushBlock();
  if (s != Status::kOk) return s;

  // P2: filter section, index section, footer. An empty table has neither section (§9).
  const uint64_t filter_offset = offset_;
  uint64_t filter_len = 0;
  uint32_t filter_crc = Crc32(nullptr, 0);
  if (entry_count_ > 0) {
    BloomFilter filter;
    s = BloomFilter::Build(key_hashes_, bits_per_key_, &filter);
    if (s != Status::kOk) return Fail(s);
    const std::string bytes = filter.Serialize();
    filter_len = bytes.size();
    filter_crc = Crc(bytes);
    if ((s = WriteBytes(bytes)) != Status::kOk) return s;
  }
  const uint64_t index_offset = offset_;
  const uint64_t index_len = index_.size();
  const uint32_t index_crc = Crc(index_);
  if ((s = WriteBytes(index_)) != Status::kOk) return s;

  std::string footer;
  footer.reserve(F::kFooterBytes);
  PutU32(&footer, F::kVersion);
  PutU32(&footer, 0);  // reserved
  PutU64(&footer, meta_.file_number);
  PutU64(&footer, meta_.min_seq);
  PutU64(&footer, meta_.max_seq);
  PutU64(&footer, entry_count_);
  PutU64(&footer, block_count_);
  PutU64(&footer, filter_offset);
  PutU64(&footer, filter_len);
  PutU64(&footer, index_offset);
  PutU64(&footer, index_len);
  PutU32(&footer, filter_crc);
  PutU32(&footer, index_crc);
  PutU32(&footer, Crc(footer));  // footer_crc covers bytes [0, 88)
  footer.append(kMagic, sizeof(kMagic));
  if ((s = WriteBytes(footer)) != Status::kOk) return s;

  // P3: full sync and close the temp file.
  const bool synced = FullSync(fd_);
  const bool closed = ::close(fd_) == 0;
  fd_ = -1;
  if (!synced || !closed) return Fail(Status::kIOError);

  // P4: link() never replaces an existing file. On failure final_path is not ours: only the
  // temp file is removed.
  if (::link(temp_path_.c_str(), final_path_.c_str()) != 0) return Fail(Status::kIOError);

  // P5: remove the temp name. On failure remove final_path (created by us at P4) and retry once.
  if (::unlink(temp_path_.c_str()) != 0) {
    ::unlink(final_path_.c_str());
    if (::unlink(temp_path_.c_str()) == 0) temp_exists_ = false;
    state_ = State::kFailed;
    return Status::kIOError;
  }
  temp_exists_ = false;

  // P6: make the new directory entry durable. On failure remove final_path (ours since P4).
  if (!SyncDirectory(DirectoryOf(final_path_))) {
    ::unlink(final_path_.c_str());
    state_ = State::kFailed;
    return Status::kIOError;
  }
  state_ = State::kFinished;  // P7
  return Status::kOk;
}

Status WriteSSTable(const std::string& final_path, const SSTableMeta& meta,
                    const SSTableBuilderOptions& options, KVIterator* it) {
  if (it == nullptr) return Status::kInvalidArgument;
  std::unique_ptr<SSTableBuilder> builder;
  Status s = SSTableBuilder::Create(final_path, meta, options, &builder);
  if (s != Status::kOk) return s;
  for (; it->Valid(); it->Next()) {
    if ((s = builder->Add(it->Current())) != Status::kOk) return s;
  }
  return builder->Finish();
}

// ---------------------------------------------------------------------------------------------
// Reader

SSTable::~SSTable() {
  if (cache_) cache_->EraseTable(cache_id_, index_.size());
  if (fd_ >= 0) ::close(fd_);
}

Status SSTable::Open(const std::string& path, uint64_t expected_file_number,
                     std::unique_ptr<SSTable>* out, std::shared_ptr<BlockCache> cache) {
  std::unique_ptr<SSTable> t(new SSTable());
  t->fd_ = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (t->fd_ < 0) return Status::kIOError;
  const int fd = t->fd_;

  // Step 1: the file size is taken once and bounds every later check.
  struct stat st;
  if (::fstat(fd, &st) != 0) return Status::kIOError;
  if (st.st_size < 0) return Status::kCorruption;
  const uint64_t file_size = static_cast<uint64_t>(st.st_size);
  if (file_size < F::kFooterBytes) return Status::kCorruption;  // L1
  const uint64_t body = file_size - F::kFooterBytes;

  // Step 2: footer, in the order magic, footer_crc, version, reserved, file_number, seq range.
  std::string footer;
  Status s = PreadExact(fd, body, F::kFooterBytes, &footer);
  if (s != Status::kOk) return s;
  const char* f = footer.data();
  if (!std::equal(kMagic, kMagic + sizeof(kMagic), f + kMagicOffset)) return Status::kCorruption;
  if (GetU32(f + kFooterCrcOffset) != Crc32(f, kFooterCrcOffset)) return Status::kCorruption;
  if (GetU32(f + 0) != F::kVersion) return Status::kCorruption;
  if (GetU32(f + 4) != 0) return Status::kCorruption;
  const uint64_t file_number = GetU64(f + 8);
  const uint64_t min_seq = GetU64(f + 16);
  const uint64_t max_seq = GetU64(f + 24);
  if (file_number != expected_file_number) return Status::kCorruption;
  if (min_seq > max_seq) return Status::kCorruption;
  const uint64_t entry_count = GetU64(f + 32);
  const uint64_t block_count = GetU64(f + 40);
  const uint64_t filter_offset = GetU64(f + 48);
  const uint64_t filter_len = GetU64(f + 56);
  const uint64_t index_offset = GetU64(f + 64);
  const uint64_t index_len = GetU64(f + 72);
  const uint32_t filter_crc = GetU32(f + 80);
  const uint32_t index_crc = GetU32(f + 84);

  // Step 3: L2 (filter_offset + filter_len == index_offset) and L3 (index_offset + index_len ==
  // file_size - 100), written so they cannot overflow.
  if (filter_len > index_offset || filter_offset != index_offset - filter_len) {
    return Status::kCorruption;
  }
  if (index_offset > body || index_len != body - index_offset) return Status::kCorruption;

  // Step 4: empty-table rules (§9) and count bounds.
  if (entry_count == 0) {
    if (block_count != 0 || filter_offset != 0 || filter_len != 0 || index_offset != 0 ||
        index_len != 0) {
      return Status::kCorruption;
    }
  } else {
    if (block_count < 1 || filter_len == 0) return Status::kCorruption;
    if (block_count > filter_offset / 13) return Status::kCorruption;
    if (entry_count > (filter_offset - 4 * block_count) / 9) return Status::kCorruption;
    if (block_count > index_len / F::kIndexEntryHeaderBytes) return Status::kCorruption;
  }
  if (index_len > std::numeric_limits<size_t>::max() ||
      filter_len > std::numeric_limits<size_t>::max()) {
    return Status::kCorruption;
  }

  // Step 5: index section.
  std::string index;
  if ((s = PreadExact(fd, index_offset, static_cast<size_t>(index_len), &index)) != Status::kOk) {
    return s;
  }
  if (Crc(index) != index_crc) return Status::kCorruption;
  std::vector<IndexEntry> entries;
  entries.reserve(static_cast<size_t>(block_count));  // block_count <= index_len / 16
  size_t pos = 0;
  uint64_t expected_offset = 0;
  for (uint64_t i = 0; i < block_count; ++i) {
    if (index.size() - pos < F::kIndexEntryHeaderBytes) return Status::kCorruption;
    IndexEntry e;
    e.offset = GetU64(&index[pos]);
    e.size = GetU32(&index[pos + 8]);
    const uint32_t key_len = GetU32(&index[pos + 12]);
    pos += F::kIndexEntryHeaderBytes;
    if (e.offset != expected_offset) return Status::kCorruption;
    if (e.size < F::kRecordHeaderBytes || e.size > F::kMaxBlockPayload) return Status::kCorruption;
    if (key_len > F::kMaxKeyValueBytes || key_len > index.size() - pos) return Status::kCorruption;
    e.last_key.assign(&index[pos], key_len);
    pos += key_len;
    if (i > 0 && !(e.last_key > entries.back().last_key)) return Status::kCorruption;
    // offset_i + size_i + 4 <= filter_offset, then offset_{i+1} = offset_i + size_i + 4 (§5.3).
    const uint64_t block_bytes = uint64_t{e.size} + F::kBlockTrailerBytes;
    if (block_bytes > filter_offset - expected_offset) return Status::kCorruption;
    expected_offset += block_bytes;
    entries.push_back(std::move(e));
  }
  if (pos != index.size() || expected_offset != filter_offset) return Status::kCorruption;

  // Step 6: filter section.
  std::string filter_bytes;
  s = PreadExact(fd, filter_offset, static_cast<size_t>(filter_len), &filter_bytes);
  if (s != Status::kOk) return s;
  if (Crc(filter_bytes) != filter_crc) return Status::kCorruption;
  if (entry_count > 0) {
    s = BloomFilter::Parse(filter_bytes.data(), filter_bytes.size(), entry_count, &t->filter_);
    if (s != Status::kOk) return s;
  }

  t->meta_ = SSTableMeta{file_number, min_seq, max_seq};
  t->entry_count_ = entry_count;
  t->index_ = std::move(entries);
  if (cache) {
    t->cache_ = std::move(cache);
    t->cache_id_ = BlockCache::NewTableId();
  }
  *out = std::move(t);
  return Status::kOk;
}

Status SSTable::ReadRawBlock(size_t i, std::string* raw) const {
  const IndexEntry& ie = index_[i];
  Status s = PreadExact(fd_, ie.offset, size_t{ie.size} + F::kBlockTrailerBytes, raw);  // §10.2.1
  if (s != Status::kOk) return s;
  if (GetU32(&(*raw)[ie.size]) != Crc32(raw->data(), ie.size)) return Status::kCorruption;  // .2
  return Status::kOk;
}

Status SSTable::ReadBlock(size_t i, std::vector<Entry>* entries) const {
  std::string raw;
  Status s = ReadRawBlock(i, &raw);
  if (s != Status::kOk) return s;
  entries->clear();
  return ParseBlock(raw, index_[i].size, i > 0 ? &index_[i - 1].last_key : nullptr, index_[i].last_key,
                    [&](bool tombstone, std::string_view key, std::string_view value) {
                      entries->push_back(Entry{std::string(key), std::string(value), tombstone});
                    });
}

Status SSTable::LoadBlock(size_t i, bool use_cache, BlockCache::Block* out) const {
  const bool cached = use_cache && cache_ != nullptr;
  if (cached) {
    BlockCache::Block hit = cache_->Lookup(cache_id_, i);
    if (hit != nullptr) {
      *out = std::move(hit);
      return Status::kOk;
    }
  }
  auto block = std::make_shared<std::vector<Entry>>();
  Status s = ReadBlock(i, block.get());
  if (s != Status::kOk) return s;  // a block that failed to read or validate is never cached
  if (cached) cache_->Insert(cache_id_, i, block);
  *out = std::move(block);
  return Status::kOk;
}

std::unique_ptr<SSTable::Iterator> SSTable::NewIterator() const {
  std::unique_ptr<Iterator> it(new Iterator(this, /*use_cache=*/false));
  it->LoadNextBlock();
  return it;
}

std::unique_ptr<SSTable::Iterator> SSTable::NewIterator(const std::string& start) const {
  std::unique_ptr<Iterator> it(new Iterator(this, /*use_cache=*/true));
  it->Seek(start);
  return it;
}

void SSTable::Iterator::LoadNextBlock() {
  block_.reset();
  pos_ = 0;
  if (next_block_ == table_->index_.size()) {
    // End of the data. From the first record on, a clean end only if the record count matches
    // the footer (§10.2); after a Seek the blocks before the start were never read.
    if (!seeked_ && records_seen_ != table_->entry_count_) status_ = Status::kCorruption;
    return;
  }
  BlockCache::Block block;
  Status s = table_->LoadBlock(next_block_++, use_cache_, &block);
  if (s != Status::kOk) {
    status_ = s;  // sticky: never reported as a normal end
    return;
  }
  block_ = std::move(block);
  records_seen_ += block_->size();
}

void SSTable::Iterator::Next() {
  if (!Valid()) return;
  if (++pos_ == block_->size()) LoadNextBlock();
}

void SSTable::Iterator::Seek(const std::string& target) {
  if (status_ != Status::kOk) return;  // errors are sticky
  seeked_ = true;
  const auto& index = table_->index_;
  // The first block whose last key is >= target holds the first entry >= target (§6).
  auto it = std::lower_bound(index.begin(), index.end(), target,
                             [](const IndexEntry& e, const std::string& k) { return e.last_key < k; });
  next_block_ = static_cast<size_t>(it - index.begin());
  LoadNextBlock();
  if (!Valid()) return;
  // The block's last key equals the index's last_key >= target, so this finds an entry.
  pos_ = static_cast<size_t>(std::lower_bound(block_->begin(), block_->end(), target,
                                              [](const Entry& e, const std::string& k) { return e.key < k; }) -
                             block_->begin());
}

Status SSTable::Get(const std::string& key, GetResult* result, std::string* value) const {
  if (entry_count_ == 0 || !filter_.MayContain(key)) {
    *result = GetResult::kAbsent;
    return Status::kOk;
  }
  // First block whose last key >= key (§6 lookup).
  auto it = std::lower_bound(index_.begin(), index_.end(), key,
                             [](const IndexEntry& e, const std::string& k) { return e.last_key < k; });
  if (it == index_.end()) {
    *result = GetResult::kAbsent;
    return Status::kOk;
  }
  const size_t i = static_cast<size_t>(it - index_.begin());
  if (cache_ != nullptr) {
    BlockCache::Block block;
    Status s = LoadBlock(i, /*use_cache=*/true, &block);
    if (s != Status::kOk) return s;
    auto e = std::lower_bound(block->begin(), block->end(), key,
                              [](const Entry& x, const std::string& k) { return x.key < k; });
    if (e == block->end() || e->key != key) {
      *result = GetResult::kAbsent;
    } else if (e->tombstone) {
      *result = GetResult::kDeleted;
    } else {
      *value = e->value;
      *result = GetResult::kFound;
    }
    return Status::kOk;
  }
  std::string raw;
  Status s = ReadRawBlock(i, &raw);
  if (s != Status::kOk) return s;
  // Search the encoded records in place: only the matching value is copied, and only after the
  // WHOLE block has passed validation, so damage after the key is still reported (§10.2).
  bool hit = false;
  bool deleted = false;
  std::string_view found;
  s = ParseBlock(raw, it->size, i > 0 ? &index_[i - 1].last_key : nullptr, it->last_key,
                 [&](bool tombstone, std::string_view k, std::string_view v) {
                   if (k == key) {
                     hit = true;
                     deleted = tombstone;
                     found = v;
                   }
                 });
  if (s != Status::kOk) return s;
  if (!hit) {
    *result = GetResult::kAbsent;
  } else if (deleted) {
    *result = GetResult::kDeleted;
  } else {
    value->assign(found.data(), found.size());
    *result = GetResult::kFound;
  }
  return Status::kOk;
}

}  // namespace raftkv
