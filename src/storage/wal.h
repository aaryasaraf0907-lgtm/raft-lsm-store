// Write-ahead log. Every committed write is appended here (and fsynced)
// BEFORE it is applied to the memtable, so a crash can be recovered by replay.
//
// On-disk record (little-endian):
//   [crc32 : 4][payload_len : 4][payload : payload_len]
//   payload = [type : 1][key_len : 4][key][value]      type: 1 = put, 2 = delete
// The CRC covers the payload. Replay stops at the first record that is
// incomplete or fails its checksum (a torn write from a crash) and truncates
// the file there, so later appends never sit behind garbage.
#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "storage/iterator.h"
#include "storage/kv_store.h"

namespace raftkv {

uint32_t Crc32(const void* data, size_t n);  // IEEE CRC-32, exposed for tests

struct ReplayStats {
  uint64_t records = 0;          // valid records applied
  uint64_t valid_bytes = 0;      // file length after recovery
  uint64_t discarded_bytes = 0;  // torn/corrupt tail that was truncated away
};

class WAL {
 public:
  using WriteFunction = std::function<std::ptrdiff_t(int, const char*, size_t)>;
  static constexpr uint32_t kMaxPayloadBytes = 64u << 20;  // sanity cap

  // Opens (creating if needed) the log for appending.
  static Status Open(const std::string& path, std::unique_ptr<WAL>* out);
  // Opens with an injected writer for tests that need to simulate a partial write failure.
  static Status OpenForTesting(const std::string& path, std::unique_ptr<WAL>* out,
                               WriteFunction write_function);
  ~WAL();
  WAL(const WAL&) = delete;
  WAL& operator=(const WAL&) = delete;

  // sync=true: the record is on stable storage when this returns OK.
  Status AppendPut(const std::string& key, const std::string& value, bool sync = true);
  Status AppendDelete(const std::string& key, bool sync = true);
  Status Sync();  // flush earlier sync=false appends

  // Calls `apply` for each valid record in order (a delete arrives as an Entry
  // with tombstone=true). A missing file is an empty log. Repairs a torn tail.
  static Status Replay(const std::string& path, const std::function<void(const Entry&)>& apply,
                       ReplayStats* stats = nullptr);

 private:
  explicit WAL(int fd, WriteFunction write_function = {})
      : fd_(fd), write_function_(std::move(write_function)) {}
  Status Append(uint8_t type, const std::string& key, const std::string& value, bool sync);
  int fd_;
  WriteFunction write_function_;
  bool failed_ = false;  // set after any failed write/fsync; the log may end in a partial record
};

}  // namespace raftkv