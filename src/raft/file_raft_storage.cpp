#include "raft/file_raft_storage.h"

#include <fcntl.h>
#ifdef _WIN32
#include <io.h>
#include <sys/stat.h>
#include <windows.h>
#else
#include <sys/stat.h>
#include <unistd.h>
#endif

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <limits>
#include <utility>

namespace raftkv {

namespace {

constexpr uint8_t kRecHardState = 1;
constexpr uint8_t kRecEntry = 2;
constexpr uint8_t kRecTruncate = 3;
constexpr uint8_t kRecEntryV2 = 4;  // entry + client_id + seq (kRecEntry is the older layout)
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
#ifdef _WIN32
  const intptr_t raw_handle = ::_get_osfhandle(fd);
  if (raw_handle == -1) return false;
  return ::FlushFileBuffers(reinterpret_cast<HANDLE>(raw_handle)) != 0;
#else
  return fsync(fd) == 0;
#endif
}

int TruncateFd(int fd, uint64_t length) {
#ifdef _WIN32
  if (length > static_cast<uint64_t>(std::numeric_limits<__int64>::max())) {
    errno = EOVERFLOW;
    return -1;
  }
  return ::_chsize_s(fd, static_cast<__int64>(length)) == 0 ? 0 : -1;
#else
  if (length > static_cast<uint64_t>(std::numeric_limits<off_t>::max())) {
    errno = EOVERFLOW;
    return -1;
  }
  return ::ftruncate(fd, static_cast<off_t>(length));
#endif
}

// Reads at most min(size, SSIZE_MAX) bytes. A short positive result is valid and
// the caller must advance both its buffer and offset; zero means EOF, and -1 means
// an error with errno set. Windows uses the shared file pointer, so callers must
// hold io_mutex_ across this call and all other operations on this descriptor.
ssize_t ReadAt(int fd, char* buffer, size_t size, uint64_t offset) {
  if (size == 0) return 0;
#ifdef _WIN32
  if (offset > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
    errno = EOVERFLOW;
    return -1;
  }
  const intptr_t raw_handle = ::_get_osfhandle(fd);
  if (raw_handle == -1) {
    errno = EBADF;
    return -1;
  }
  LARGE_INTEGER position;
  position.QuadPart = static_cast<LONGLONG>(offset);
  if (!::SetFilePointerEx(reinterpret_cast<HANDLE>(raw_handle), position, nullptr,
                          FILE_BEGIN)) {
    switch (::GetLastError()) {
      case ERROR_INVALID_HANDLE:
        errno = EBADF;
        break;
      case ERROR_INVALID_PARAMETER:
        errno = EINVAL;
        break;
      case ERROR_OPERATION_ABORTED:
        errno = EINTR;
        break;
      default:
        errno = EIO;
        break;
    }
    return -1;
  }
  const DWORD requested = static_cast<DWORD>(
      std::min<size_t>(size, std::min<size_t>(
                                std::numeric_limits<DWORD>::max(),
                                static_cast<size_t>(
                                    std::numeric_limits<ssize_t>::max()))));
  DWORD received = 0;
  if (!::ReadFile(reinterpret_cast<HANDLE>(raw_handle), buffer, requested,
                  &received, nullptr)) {
    const DWORD error = ::GetLastError();
    if (error == ERROR_HANDLE_EOF) return 0;
    errno = error == ERROR_OPERATION_ABORTED ? EINTR : EIO;
    return -1;
  }
  return static_cast<ssize_t>(received);
#else
  if (offset > static_cast<uint64_t>(std::numeric_limits<off_t>::max())) {
    errno = EOVERFLOW;
    return -1;
  }
  const size_t requested = std::min<size_t>(
      size, static_cast<size_t>(std::numeric_limits<ssize_t>::max()));
  return ::pread(fd, buffer, requested, static_cast<off_t>(offset));
#endif
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
  int file_flags = O_RDWR | O_CREAT | O_APPEND;
#ifdef _WIN32
  file_flags |= O_BINARY;
#endif
  fd_ = ::open(path_.c_str(), file_flags, 0644);
  if (fd_ < 0) return;
  if (sync_) {
#ifndef _WIN32
    // Best-effort directory-entry durability where directory fsync is supported.
    int directory_flags = O_RDONLY;
    int dfd = ::open(dir_.c_str(), directory_flags);
    if (dfd >= 0) {
      SyncFd(dfd);
      ::close(dfd);
    }
#else
    // FlushFileBuffers on the log file does not guarantee its directory entry is durable.
#endif
  }
}

FileRaftStorage::~FileRaftStorage() {
  if (fd_ >= 0) ::close(fd_);
}

bool FileRaftStorage::Load(PersistentState* out) {
  std::lock_guard<std::mutex> lock(io_mutex_);
  if (fd_ < 0) return false;
  *out = PersistentState();

#ifdef _WIN32
  struct _stat64 sb;
  if (::_fstat64(fd_, &sb) != 0) return false;
#else
  struct stat sb;
  if (::fstat(fd_, &sb) != 0) return false;
#endif
  if (sb.st_size < 0 ||
      static_cast<uint64_t>(sb.st_size) >
          static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
    return false;
  }
  std::string buf(static_cast<size_t>(sb.st_size), '\0');
  size_t got = 0;
  while (got < buf.size()) {
    ssize_t r = ReadAt(fd_, &buf[got], buf.size() - got, got);
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
    if (TruncateFd(fd_, static_cast<uint64_t>(good_end)) != 0) return false;
    if (sync_ && !SyncFd(fd_)) return false;
  }
  return true;
}

bool FileRaftStorage::WriteRecords(const std::string& bytes) {
  std::lock_guard<std::mutex> lock(io_mutex_);
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
    PutU8(&payload, kRecEntryV2);
    PutU64(&payload, e.index);
    PutU64(&payload, e.term);
    PutU8(&payload, static_cast<uint8_t>(e.command.type));
    PutStr(&payload, e.command.key);
    PutStr(&payload, e.command.value);
    PutU64(&payload, e.command.client_id);
    PutU64(&payload, e.command.seq);
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
