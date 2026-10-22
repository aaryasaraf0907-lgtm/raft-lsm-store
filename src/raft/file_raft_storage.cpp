#include "raft/file_raft_storage.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <utility>

namespace raftkv {

namespace {

constexpr uint8_t kRecHardState = 1;
constexpr uint8_t kRecEntry = 2;
constexpr uint8_t kRecTruncate = 3;
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
    case kRecEntry: {
      LogEntry e;
      e.index = r.U64();
      e.term = r.U64();
      uint8_t cmd = r.U8();
      e.command.key = r.Str();
      e.command.value = r.Str();
      if (!r.ok || cmd > static_cast<uint8_t>(CommandType::kNoop)) return false;
      e.command.type = static_cast<CommandType>(cmd);
      if (e.index != st->log.size() + 1) return false;  // log must be contiguous
      st->log.push_back(std::move(e));
      return true;
    }
    case kRecTruncate: {
      uint64_t from = r.U64();
      if (!r.ok || from < 1 || from > st->log.size() + 1) return false;
      st->log.erase(st->log.begin() + static_cast<std::ptrdiff_t>(from - 1), st->log.end());
      return true;
    }
    default:
      return false;
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
  return true;
}

bool FileRaftStorage::WriteRecords(const std::string& bytes) {
  if (fd_ < 0) return false;
  size_t done = 0;
  while (done < bytes.size()) {
    ssize_t w = ::write(fd_, bytes.data() + done, bytes.size() - done);
    if (w < 0 && errno == EINTR) continue;
    if (w <= 0) return false;
    done += static_cast<size_t>(w);
  }
  return !sync_ || SyncFd(fd_);
}

bool FileRaftStorage::SaveHardState(Term term, NodeId voted_for) {
  std::string payload, frame;
  PutU8(&payload, kRecHardState);
  PutU64(&payload, term);
  PutU32(&payload, voted_for);
  AppendFrame(&frame, payload);
  return WriteRecords(frame);
}

bool FileRaftStorage::AppendEntries(const std::vector<LogEntry>& entries) {
  std::string frames;  // all entries go out in one write + one fsync
  for (const LogEntry& e : entries) {
    std::string payload;
    PutU8(&payload, kRecEntry);
    PutU64(&payload, e.index);
    PutU64(&payload, e.term);
    PutU8(&payload, static_cast<uint8_t>(e.command.type));
    PutStr(&payload, e.command.key);
    PutStr(&payload, e.command.value);
    AppendFrame(&frames, payload);
  }
  return WriteRecords(frames);
}

bool FileRaftStorage::TruncateSuffix(LogIndex from_index) {
  std::string payload, frame;
  PutU8(&payload, kRecTruncate);
  PutU64(&payload, from_index);
  AppendFrame(&frame, payload);
  return WriteRecords(frame);
}

}  // namespace raftkv
