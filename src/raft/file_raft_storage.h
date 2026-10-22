#pragma once
#include <string>
#include "raft/raft_storage.h"

namespace raftkv {

// RaftStorage backed by one append-only file, <dir>/raft.log.
//
// File format: a sequence of records, each  [u32 length][u32 crc32][payload]  (little endian)
// where payload[0] is the record type:
//   1 = hard state {u64 term, u32 voted_for}     the last one wins
//   2 = log entry  {u64 index, u64 term, u8 command type, string key, string value}
//   3 = truncate   {u64 from_index}              drops that entry and all after it
// Load() replays the records. A torn or corrupt record at the end (a crash in the middle of a
// write) is detected by its length/checksum and cut off, so the node restarts from the last
// complete record.
//
// The file only grows for now; snapshots/compaction will bound it later.
class FileRaftStorage : public RaftStorage {
 public:
  // `sync` = fsync after every write. Keep it true in real use; tests turn it off for speed.
  explicit FileRaftStorage(std::string dir, bool sync = true);
  ~FileRaftStorage() override;
  FileRaftStorage(const FileRaftStorage&) = delete;
  FileRaftStorage& operator=(const FileRaftStorage&) = delete;

  bool ok() const { return fd_ >= 0; }  // false if the directory/file could not be opened

  bool Load(PersistentState* out) override;
  bool SaveHardState(Term term, NodeId voted_for) override;
  bool AppendEntries(const std::vector<LogEntry>& entries) override;
  bool TruncateSuffix(LogIndex from_index) override;

 private:
  bool WriteRecords(const std::string& bytes);

  std::string dir_;
  std::string path_;
  bool sync_;
  int fd_ = -1;
};

}  // namespace raftkv
