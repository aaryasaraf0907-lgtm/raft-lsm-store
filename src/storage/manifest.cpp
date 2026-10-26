#include "storage/manifest.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <set>

#include "storage/wal.h"  // Crc32

namespace raftkv {
namespace {

void PutU16(std::string* s, uint16_t v) { for (int i = 0; i < 2; ++i) s->push_back(static_cast<char>((v >> (8 * i)) & 0xFF)); }
void PutU32(std::string* s, uint32_t v) { for (int i = 0; i < 4; ++i) s->push_back(static_cast<char>((v >> (8 * i)) & 0xFF)); }
void PutU64(std::string* s, uint64_t v) { for (int i = 0; i < 8; ++i) s->push_back(static_cast<char>((v >> (8 * i)) & 0xFF)); }
uint64_t GetLe(const std::string& s, size_t pos, int n) {
  uint64_t v = 0;
  for (int i = 0; i < n; ++i) v |= static_cast<uint64_t>(static_cast<uint8_t>(s[pos + static_cast<size_t>(i)])) << (8 * i);
  return v;
}

void PutRecord(std::string* out, uint16_t type, uint16_t flags, const std::string& payload) {
  PutU16(out, type);
  PutU16(out, flags);
  PutU32(out, static_cast<uint32_t>(payload.size()));
  out->append(payload);
}

bool SyncPath(const std::string& path, int flags) {
  int fd = ::open(path.c_str(), flags);
  if (fd < 0) return false;
  const bool ok = ::fsync(fd) == 0;
  ::close(fd);
  return ok;
}

}  // namespace

std::string ManifestPath(const std::string& dir) { return (std::filesystem::path(dir) / "MANIFEST").string(); }

std::string EncodeManifest(const ManifestState& state) {
  std::string out;
  PutU64(&out, kManifestMagic);
  PutU32(&out, kManifestFormatVersion);
  PutU32(&out, 0);
  std::string payload;
  PutU64(&payload, state.next_file_number);
  PutRecord(&out, kManifestNextFileNumber, kManifestRequired, payload);
  payload.clear();
  PutU64(&payload, state.flushed_through);
  PutRecord(&out, kManifestFlushedThrough, kManifestRequired, payload);
  for (const SSTableMeta& f : state.files) {
    payload.clear();
    PutU64(&payload, f.file_number);
    PutU64(&payload, f.min_seq);
    PutU64(&payload, f.max_seq);
    PutRecord(&out, kManifestSstable, kManifestRequired, payload);
  }
  PutU32(&out, Crc32(out.data(), out.size()));
  return out;
}

Status DecodeManifest(const std::string& bytes, ManifestState* out) {
  constexpr size_t kHeader = 16, kRecordHeader = 8;
  if (bytes.size() < kHeader + 4) return Status::kCorruption;
  const size_t body = bytes.size() - 4;
  if (GetLe(bytes, body, 4) != Crc32(bytes.data(), body)) return Status::kCorruption;
  if (GetLe(bytes, 0, 8) != kManifestMagic) return Status::kCorruption;
  if (GetLe(bytes, 8, 4) > kManifestFormatVersion) return Status::kCorruption;  // written by a newer version
  ManifestState s;
  bool have_next = false, have_flushed = false;
  size_t pos = kHeader;
  while (pos < body) {
    if (body - pos < kRecordHeader) return Status::kCorruption;
    const uint16_t type = static_cast<uint16_t>(GetLe(bytes, pos, 2));
    const uint16_t flags = static_cast<uint16_t>(GetLe(bytes, pos + 2, 2));
    const uint64_t len = GetLe(bytes, pos + 4, 4);
    pos += kRecordHeader;
    if (len > body - pos) return Status::kCorruption;
    switch (type) {
      case kManifestNextFileNumber:
        if (len != 8 || have_next) return Status::kCorruption;
        s.next_file_number = GetLe(bytes, pos, 8);
        have_next = true;
        break;
      case kManifestFlushedThrough:
        if (len != 8 || have_flushed) return Status::kCorruption;
        s.flushed_through = GetLe(bytes, pos, 8);
        have_flushed = true;
        break;
      case kManifestSstable:
        if (len != 24) return Status::kCorruption;
        s.files.push_back(SSTableMeta{GetLe(bytes, pos, 8), GetLe(bytes, pos + 8, 8), GetLe(bytes, pos + 16, 8)});
        break;
      default:
        if ((flags & kManifestRequired) != 0) return Status::kCorruption;  // cannot be understood: refuse
        break;                                                              // optional: skip
    }
    pos += static_cast<size_t>(len);
  }
  if (!have_next || !have_flushed) return Status::kCorruption;
  // Invariants of a live file list (docs §13.2 C1, C2) and of the counter.
  std::set<uint64_t> numbers;
  for (size_t i = 0; i < s.files.size(); ++i) {
    const SSTableMeta& f = s.files[i];
    if (f.min_seq > f.max_seq || !numbers.insert(f.file_number).second) return Status::kCorruption;
    if (f.file_number >= s.next_file_number) return Status::kCorruption;
    if (i > 0 && !(s.files[i - 1].max_seq < f.min_seq)) return Status::kCorruption;
  }
  *out = std::move(s);
  return Status::kOk;
}

Status LoadManifest(const std::string& dir, ManifestState* out, bool* found) {
  const std::string path = ManifestPath(dir);
  std::error_code ec;
  if (!std::filesystem::exists(path, ec)) {
    *found = false;
    return Status::kOk;
  }
  std::ifstream in(path, std::ios::binary);
  if (!in) return Status::kIOError;
  std::string bytes((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
  Status s = DecodeManifest(bytes, out);
  if (s != Status::kOk) return s;
  *found = true;
  return Status::kOk;
}

Status StoreManifest(const std::string& dir, const ManifestState& state,
                     const std::function<bool()>& fail_after_rename) {
  const std::string path = ManifestPath(dir);
  const std::string tmp = path + ".tmp";
  const std::string bytes = EncodeManifest(state);
  int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) return Status::kIOError;
  size_t done = 0;
  bool ok = true;
  while (done < bytes.size()) {
    ssize_t w = ::write(fd, bytes.data() + done, bytes.size() - done);
    if (w < 0 && errno == EINTR) continue;
    if (w <= 0) { ok = false; break; }
    done += static_cast<size_t>(w);
  }
  if (ok && ::fsync(fd) != 0) ok = false;
  ::close(fd);
  if (!ok || ::rename(tmp.c_str(), path.c_str()) != 0) {
    ::unlink(tmp.c_str());
    return Status::kIOError;
  }
  if (fail_after_rename && fail_after_rename()) return Status::kIOError;  // tests: the directory fsync "fails"
  if (!SyncPath(dir, O_RDONLY)) return Status::kIOError;  // make the rename itself durable
  return Status::kOk;
}

}  // namespace raftkv
