#include "raft/file_raft_storage.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <cstdio>
#include <filesystem>
#include <utility>

namespace raftkv {

namespace {

constexpr uint8_t kRecHardState = 1;
constexpr uint8_t kRecEntry = 2;
constexpr uint8_t kRecTruncate = 3;
constexpr uint8_t kRecEntryV2 = 4;  // entry + client_id + seq (kRecEntry is the older layout)
constexpr uint8_t kRecBase = 5;     // snapshot index + term: the log starts right after it
constexpr uint8_t kRecBaseV2 = 6;   // log base and snapshot separately (log may start earlier)
constexpr uint32_t kSnapshotMagic = 0x504E5352u;  // "RSNP"
constexpr uint32_t kMaxRecordBytes = 64u * 1024u * 1024u;  // sanity limit when reading

uint32_t Crc32(const char* data, size_t n) {
  static const std::array<uint32_t, 256> table = [] {
    std::array<uint32_t, 256> t{};
    for (uint32_t i = 0; i < 256; ++i) {
      uint32_t c = i;
      for (int k = 0; k < 8; ++k) c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
      t[i] = c;
    }
    return t;
  }();
  uint32_t crc = 0xFFFFFFFFu;
  for (size_t i = 0; i < n; ++i) {
    crc = table[(crc ^ static_cast<uint8_t>(data[i])) & 0xFFu] ^ (crc >> 8);
  }
  return crc ^ 0xFFFFFFFFu;
}

// fsync that really reaches the disk. On macOS plain fsync only reaches the drive's cache.
bool SyncFd(int fd) {
#ifdef F_FULLFSYNC
  if (fcntl(fd, F_FULLFSYNC) == 0) return true;
#endif
  return fsync(fd) == 0;
}

// ---- encoding helpers (little endian) ----

void PutU8(std::string* s, uint8_t v) { s->push_back(static_cast<char>(v)); }

void PutU32(std::string* s, uint32_t v) {
  for (int i = 0; i < 4; ++i) s->push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
}

void PutU64(std::string* s, uint64_t v) {
  for (int i = 0; i < 8; ++i) s->push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
}

void PutStr(std::string* s, const std::string& v) {
  PutU32(s, static_cast<uint32_t>(v.size()));
  s->append(v);
}

// Bounds-checked reader: any read past the end flips ok to false.
struct Reader {
  const char* p;
  size_t n;
  bool ok = true;

  Reader(const char* data, size_t size) : p(data), n(size) {}

  uint64_t Uint(int bytes) {
    if (!ok || n < static_cast<size_t>(bytes)) {
      ok = false;
      return 0;
    }
    uint64_t v = 0;
    for (int i = 0; i < bytes; ++i) v |= static_cast<uint64_t>(static_cast<uint8_t>(p[i])) << (8 * i);
    p += bytes;
    n -= static_cast<size_t>(bytes);
    return v;
  }
  uint8_t U8() { return static_cast<uint8_t>(Uint(1)); }
  uint32_t U32() { return static_cast<uint32_t>(Uint(4)); }
  uint64_t U64() { return Uint(8); }
  std::string Str() {
    uint32_t len = U32();
    if (!ok || n < len) {
      ok = false;
      return std::string();
    }
    std::string s(p, len);
    p += len;
    n -= len;
    return s;
  }
};

// Wraps a payload in  [len][crc][payload].
void AppendFrame(std::string* out, const std::string& payload) {
  PutU32(out, static_cast<uint32_t>(payload.size()));
  PutU32(out, Crc32(payload.data(), payload.size()));
  out->append(payload);
}

// Applies one checksum-valid record to the state being rebuilt.
// Returns false if the record is nonsense (which means real corruption, not a torn tail).
bool ApplyRecord(const char* payload, size_t len, PersistentState* st) {
  Reader r(payload, len);
  uint8_t type = r.U8();
  switch (type) {
    case kRecHardState: {
      st->term = r.U64();
      st->voted_for = r.U32();
      return r.ok;
    }
    case kRecEntry:    // older files: no client request id
    case kRecEntryV2: {
      LogEntry e;
      e.index = r.U64();
      e.term = r.U64();
      uint8_t cmd = r.U8();
      e.command.key = r.Str();
      e.command.value = r.Str();
      if (type == kRecEntryV2) {
        e.command.client_id = r.U64();
        e.command.seq = r.U64();
      }
      if (!r.ok || cmd > static_cast<uint8_t>(CommandType::kConfig)) return false;
      e.command.type = static_cast<CommandType>(cmd);
      if (e.index != st->log_base_index + st->log.size() + 1) return false;  // contiguous
      st->log.push_back(std::move(e));
      return true;
    }
    case kRecTruncate: {
      uint64_t from = r.U64();
      if (!r.ok || from <= st->log_base_index || from > st->log_base_index + st->log.size() + 1) {
        return false;
      }
      st->log.erase(st->log.begin() + static_cast<std::ptrdiff_t>(from - st->log_base_index - 1),
                    st->log.end());
      return true;
    }
    case kRecBase: {
      st->snapshot_index = st->log_base_index = r.U64();
      st->snapshot_term = st->log_base_term = r.U64();
      st->log.clear();
      return r.ok && st->snapshot_index > 0;
    }
    case kRecBaseV2: {
      st->log_base_index = r.U64();
      st->log_base_term = r.U64();
      st->snapshot_index = r.U64();
      st->snapshot_term = r.U64();
      st->log.clear();
      return r.ok && st->snapshot_index > 0 && st->log_base_index <= st->snapshot_index;
    }
    default:
      return false;
  }
}


void EncodeEntryFrame(std::string* out, const LogEntry& e) {
  std::string payload;
  PutU8(&payload, kRecEntryV2);
  PutU64(&payload, e.index);
  PutU64(&payload, e.term);
  PutU8(&payload, static_cast<uint8_t>(e.command.type));
  PutStr(&payload, e.command.key);
  PutStr(&payload, e.command.value);
  PutU64(&payload, e.command.client_id);
  PutU64(&payload, e.command.seq);
  AppendFrame(out, payload);
}

void SyncDir(const std::string& dir) {
  int dfd = ::open(dir.c_str(), O_RDONLY);
  if (dfd >= 0) {
    SyncFd(dfd);
    ::close(dfd);
  }
}

std::string SnapshotFileName(uint64_t index) { return "snapshot-" + std::to_string(index) + ".bin"; }

// Writes `bytes` to a temp file, fsyncs it, renames it over `path` and fsyncs the directory, so
// `path` is always either the complete old file or the complete new one.
bool WriteFileAtomic(const std::string& dir, const std::string& path, const std::string& bytes,
                     bool sync) {
  std::string tmp = path + ".tmp";
  int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) return false;
  bool ok = true;
  size_t done = 0;
  while (done < bytes.size()) {
    ssize_t w = ::write(fd, bytes.data() + done, bytes.size() - done);
    if (w < 0 && errno == EINTR) continue;
    if (w <= 0) {
      ok = false;
      break;
    }
    done += static_cast<size_t>(w);
  }
  if (ok && sync) ok = SyncFd(fd);
  ::close(fd);
  if (!ok || ::rename(tmp.c_str(), path.c_str()) != 0) {
    ::unlink(tmp.c_str());
    return false;
  }
  if (sync) SyncDir(dir);
  return true;
}

bool ReadWholeFile(const std::string& path, std::string* out) {
  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) return false;
  struct stat sb;
  if (fstat(fd, &sb) != 0) {
    ::close(fd);
    return false;
  }
  out->assign(static_cast<size_t>(sb.st_size), '\0');
  size_t got = 0;
  while (got < out->size()) {
    ssize_t r = ::pread(fd, &(*out)[got], out->size() - got, static_cast<off_t>(got));
    if (r < 0 && errno == EINTR) continue;
    if (r <= 0) {
      ::close(fd);
      return false;
    }
    got += static_cast<size_t>(r);
  }
  ::close(fd);
  return true;
}

// snapshot file = [u32 magic][u64 index][u64 term][u64 length][data][u32 crc of all before]
std::string EncodeSnapshotFile(uint64_t index, uint64_t term, const std::string& data) {
  std::string f;
  PutU32(&f, kSnapshotMagic);
  PutU64(&f, index);
  PutU64(&f, term);
  PutU64(&f, data.size());
  f.append(data);
  PutU32(&f, Crc32(f.data(), f.size()));
  return f;
}

bool DecodeSnapshotFile(const std::string& f, uint64_t want_index, uint64_t want_term,
                        std::string* data) {
  constexpr size_t kHeader = 4 + 8 + 8 + 8;
  if (f.size() < kHeader + 4) return false;
  Reader tail(f.data() + f.size() - 4, 4);
  if (Crc32(f.data(), f.size() - 4) != tail.U32()) return false;
  Reader r(f.data(), kHeader);
  if (r.U32() != kSnapshotMagic) return false;
  uint64_t index = r.U64(), term = r.U64(), len = r.U64();
  if (!r.ok || index != want_index || term != want_term) return false;
  if (len != f.size() - kHeader - 4) return false;
  data->assign(f.data() + kHeader, static_cast<size_t>(len));
  return true;
}

// Deletes snapshot files (and temp files) left by a crashed or superseded SaveSnapshot.
void RemoveStaleFiles(const std::string& dir, uint64_t keep_index) {
  const std::string keep = keep_index > 0 ? SnapshotFileName(keep_index) : std::string();
  std::error_code ec;
  for (const auto& entry : std::filesystem::directory_iterator(dir, ec)) {
    std::string name = entry.path().filename().string();
    bool snapshot = name.rfind("snapshot-", 0) == 0;
    bool temp = name.size() > 4 && name.compare(name.size() - 4, 4, ".tmp") == 0;
    if ((snapshot || temp) && name != keep) std::filesystem::remove(entry.path(), ec);
  }
}

}  // namespace

FileRaftStorage::FileRaftStorage(std::string dir, bool sync) : dir_(std::move(dir)), sync_(sync) {
  std::error_code ec;
  std::filesystem::create_directories(dir_, ec);
  if (ec) return;
  path_ = dir_ + "/raft.log";
  fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
  if (fd_ < 0) return;
  if (sync_) {  // make the new file's directory entry durable too
    int dfd = ::open(dir_.c_str(), O_RDONLY);
    if (dfd >= 0) {
      SyncFd(dfd);
      ::close(dfd);
    }
  }
}

FileRaftStorage::~FileRaftStorage() {
  if (fd_ >= 0) ::close(fd_);
}

bool FileRaftStorage::Load(PersistentState* out) {
  if (fd_ < 0) return false;
  *out = PersistentState();

  struct stat sb;
  if (fstat(fd_, &sb) != 0) return false;
  std::string buf(static_cast<size_t>(sb.st_size), '\0');
  size_t got = 0;
  while (got < buf.size()) {
    ssize_t r = ::pread(fd_, &buf[got], buf.size() - got, static_cast<off_t>(got));
    if (r < 0 && errno == EINTR) continue;
    if (r <= 0) return false;
    got += static_cast<size_t>(r);
  }

  size_t pos = 0;
  size_t good_end = 0;  // end of the last complete, valid record
  while (buf.size() - pos >= 8) {
    Reader header(buf.data() + pos, 8);
    uint32_t len = header.U32();
    uint32_t crc = header.U32();
    if (len == 0 || len > kMaxRecordBytes || buf.size() - pos - 8 < len) break;  // torn tail
    const char* payload = buf.data() + pos + 8;
    if (Crc32(payload, len) != crc) break;                                        // corrupt tail
    if (!ApplyRecord(payload, len, out)) return false;
    pos += 8 + len;
    good_end = pos;
  }

  if (good_end < buf.size()) {  // cut off the incomplete record so new writes follow clean data
    if (ftruncate(fd_, static_cast<off_t>(good_end)) != 0) return false;
    if (sync_ && !SyncFd(fd_)) return false;
  }

  if (out->snapshot_index > 0) {  // the log base points at a snapshot file that must be intact
    std::string file;
    if (!ReadWholeFile(dir_ + "/" + SnapshotFileName(out->snapshot_index), &file)) return false;
    if (!DecodeSnapshotFile(file, out->snapshot_index, out->snapshot_term, &out->snapshot_data)) {
      return false;
    }
  }
  RemoveStaleFiles(dir_, out->snapshot_index);
  term_ = out->term;
  voted_ = out->voted_for;
  return true;
}

bool FileRaftStorage::WriteRecords(const std::string& bytes, bool sync) {
  if (fd_ < 0) return false;
  size_t done = 0;
  while (done < bytes.size()) {
    ssize_t w = ::write(fd_, bytes.data() + done, bytes.size() - done);
    if (w < 0 && errno == EINTR) continue;
    if (w <= 0) return false;
    done += static_cast<size_t>(w);
  }
  return !sync_ || !sync || SyncFd(fd_);
}

bool FileRaftStorage::SaveHardState(Term term, NodeId voted_for) {
  std::string payload, frame;
  PutU8(&payload, kRecHardState);
  PutU64(&payload, term);
  PutU32(&payload, voted_for);
  AppendFrame(&frame, payload);
  if (!WriteRecords(frame)) return false;
  term_ = term;
  voted_ = voted_for;
  return true;
}

bool FileRaftStorage::AppendEntries(const std::vector<LogEntry>& entries) {
  std::string frames;  // all entries go out in one write + one fsync
  for (const LogEntry& e : entries) EncodeEntryFrame(&frames, e);
  return WriteRecords(frames);
}

bool FileRaftStorage::AppendEntriesNoSync(const std::vector<LogEntry>& entries) {
  std::string frames;
  for (const LogEntry& e : entries) EncodeEntryFrame(&frames, e);
  return WriteRecords(frames, /*sync=*/false);
}

std::function<bool()> FileRaftStorage::PrepareSync() {
  if (!sync_) return [] { return true; };
  if (fd_ < 0) return [] { return false; };
  const int d = ::dup(fd_);
  if (d < 0) return [] { return false; };
  return [d] {
    const bool ok = SyncFd(d);
    ::close(d);
    return ok;
  };
}

bool FileRaftStorage::TruncateSuffix(LogIndex from_index) {
  std::string payload, frame;
  PutU8(&payload, kRecTruncate);
  PutU64(&payload, from_index);
  AppendFrame(&frame, payload);
  return WriteRecords(frame);
}

bool FileRaftStorage::SaveSnapshot(LogIndex snapshot_index, Term snapshot_term,
                                   const std::string& data, LogIndex log_base_index,
                                   Term log_base_term,
                                   const std::vector<LogEntry>& entries_after_base) {
  if (fd_ < 0 || snapshot_index == 0 || log_base_index > snapshot_index) return false;
  // Step 1: the snapshot file. The current log does not mention it yet, so a crash here is
  // harmless: Load() ignores and deletes it.
  if (!WriteFileAtomic(dir_, dir_ + "/" + SnapshotFileName(snapshot_index),
                       EncodeSnapshotFile(snapshot_index, snapshot_term, data), sync_)) {
    return false;
  }
  // Step 2: the new log, [hard state][base][kept entries]. Renaming it over raft.log is the
  // commit point.
  std::string log, payload;
  PutU8(&payload, kRecHardState);
  PutU64(&payload, term_);
  PutU32(&payload, voted_);
  AppendFrame(&log, payload);
  payload.clear();
  PutU8(&payload, kRecBaseV2);
  PutU64(&payload, log_base_index);
  PutU64(&payload, log_base_term);
  PutU64(&payload, snapshot_index);
  PutU64(&payload, snapshot_term);
  AppendFrame(&log, payload);
  for (const LogEntry& e : entries_after_base) EncodeEntryFrame(&log, e);
  if (!WriteFileAtomic(dir_, path_, log, sync_)) return false;
  ::close(fd_);
  fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
  if (fd_ < 0) return false;
  RemoveStaleFiles(dir_, snapshot_index);  // the previous snapshot is no longer needed
  return true;
}

}  // namespace raftkv
