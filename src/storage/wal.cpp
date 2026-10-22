#include "storage/wal.h"

#include <fcntl.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstring>

namespace raftkv {
namespace {

constexpr uint8_t kPut = 1;
constexpr uint8_t kDelete = 2;
constexpr size_t kHeaderBytes = 8;  // crc + payload_len

void PutU32(std::string* s, uint32_t v) {
  for (int i = 0; i < 4; ++i) s->push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
}

uint32_t GetU32(const char* p) {
  uint32_t v = 0;
  for (int i = 0; i < 4; ++i) v |= static_cast<uint32_t>(static_cast<unsigned char>(p[i])) << (8 * i);
  return v;
}

bool WriteAll(int fd, const char* p, size_t n) {
  while (n > 0) {
    ssize_t w = ::write(fd, p, n);
    if (w < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    p += w;
    n -= static_cast<size_t>(w);
  }
  return true;
}

bool FullSync(int fd) {
#ifdef __APPLE__
  // On macOS fsync() does not force data out of the drive cache; F_FULLFSYNC does.
  if (::fcntl(fd, F_FULLFSYNC) == 0) return true;
#endif
  return ::fsync(fd) == 0;
}

}  // namespace

uint32_t Crc32(const void* data, size_t n) {
  static const std::array<uint32_t, 256> table = [] {
    std::array<uint32_t, 256> t{};
    for (uint32_t i = 0; i < 256; ++i) {
      uint32_t c = i;
      for (int k = 0; k < 8; ++k) c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
      t[i] = c;
    }
    return t;
  }();
  const unsigned char* p = static_cast<const unsigned char*>(data);
  uint32_t c = 0xFFFFFFFFu;
  for (size_t i = 0; i < n; ++i) c = table[(c ^ p[i]) & 0xFF] ^ (c >> 8);
  return c ^ 0xFFFFFFFFu;
}

Status WAL::Open(const std::string& path, std::unique_ptr<WAL>* out) {
  int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
  if (fd < 0) return Status::kIOError;
  out->reset(new WAL(fd));
  return Status::kOk;
}

WAL::~WAL() {
  if (fd_ >= 0) ::close(fd_);
}

Status WAL::Sync() {
  if (failed_) return Status::kIOError;
  if (!FullSync(fd_)) {  // after a failed fsync the state of the file is unknown: stop using it
    failed_ = true;
    return Status::kIOError;
  }
  return Status::kOk;
}

Status WAL::AppendPut(const std::string& key, const std::string& value, bool sync) {
  return Append(kPut, key, value, sync);
}

Status WAL::AppendDelete(const std::string& key, bool sync) {
  return Append(kDelete, key, std::string(), sync);
}

Status WAL::Append(uint8_t type, const std::string& key, const std::string& value, bool sync) {
  if (failed_) return Status::kIOError;  // an earlier failure may have left a partial record
  const size_t payload_len = 1 + 4 + key.size() + value.size();
  if (payload_len > kMaxPayloadBytes) return Status::kIOError;

  std::string payload;
  payload.reserve(payload_len);
  payload.push_back(static_cast<char>(type));
  PutU32(&payload, static_cast<uint32_t>(key.size()));
  payload += key;
  payload += value;

  std::string record;
  record.reserve(kHeaderBytes + payload_len);
  PutU32(&record, Crc32(payload.data(), payload.size()));
  PutU32(&record, static_cast<uint32_t>(payload.size()));
  record += payload;

  // One write() per record keeps a torn write confined to the tail.
  // If this fails midway the file ends in a partial record. Later appends would sit behind it
  // and be thrown away by Replay even though they were reported as durable, so stop here.
  if (!WriteAll(fd_, record.data(), record.size()) || (sync && !FullSync(fd_))) {
    failed_ = true;
    return Status::kIOError;
  }
  return Status::kOk;
}

Status WAL::Replay(const std::string& path, const std::function<void(const Entry&)>& apply,
                   ReplayStats* stats) {
  ReplayStats local;
  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    if (errno == ENOENT) {
      if (stats != nullptr) *stats = local;
      return Status::kOk;  // no log yet == empty log
    }
    return Status::kIOError;
  }
  std::string data;
  char buf[1 << 16];
  for (;;) {
    ssize_t r = ::read(fd, buf, sizeof(buf));
    if (r < 0) {
      if (errno == EINTR) continue;
      ::close(fd);
      return Status::kIOError;
    }
    if (r == 0) break;
    data.append(buf, static_cast<size_t>(r));
  }
  ::close(fd);

  size_t pos = 0;
  while (data.size() - pos >= kHeaderBytes) {
    const uint32_t crc = GetU32(data.data() + pos);
    const uint32_t len = GetU32(data.data() + pos + 4);
    if (len < 5 || len > kMaxPayloadBytes || len > data.size() - pos - kHeaderBytes) break;
    const char* payload = data.data() + pos + kHeaderBytes;
    // A crash can only damage the LAST record. A complete record that fails its checksum but is
    // followed by more data means the middle of the log is corrupt: report it and do not delete
    // the (possibly intact) records after it.
    const bool is_last = pos + kHeaderBytes + len == data.size();
    if (Crc32(payload, len) != crc) {
      if (!is_last) return Status::kCorruption;
      break;
    }

    const uint8_t type = static_cast<uint8_t>(payload[0]);
    const uint32_t key_len = GetU32(payload + 1);
    if (key_len > len - 5 || (type != kPut && type != kDelete)) break;
    const size_t value_len = len - 5 - key_len;
    if (type == kDelete && value_len != 0) break;

    Entry e;
    e.key.assign(payload + 5, key_len);
    e.value.assign(payload + 5 + key_len, value_len);
    e.tombstone = (type == kDelete);
    apply(e);
    ++local.records;
    pos += kHeaderBytes + len;
  }

  local.valid_bytes = pos;
  local.discarded_bytes = data.size() - pos;
  if (local.discarded_bytes > 0) {  // drop the torn tail so new appends follow clean data
    int wfd = ::open(path.c_str(), O_WRONLY);
    if (wfd < 0) return Status::kIOError;
    bool ok = ::ftruncate(wfd, static_cast<off_t>(pos)) == 0 && FullSync(wfd);
    ::close(wfd);
    if (!ok) return Status::kIOError;
  }
  if (stats != nullptr) *stats = local;
  return Status::kOk;
}

}  // namespace raftkv