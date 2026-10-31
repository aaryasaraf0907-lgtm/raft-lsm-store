// An exclusive advisory lock on a data directory, so that two engines never work on the same files.
//
// The lock is flock(LOCK_EX | LOCK_NB) on a file named "LOCK" in the directory. flock is tied to the
// open file description, so it is released when the DirLock is destroyed (the descriptor is closed)
// and, just as reliably, when the process dies for any reason: a crashed engine never leaves a
// stale lock behind. It conflicts between two descriptors of the same process as well as between
// processes, which is what makes the same-process tests meaningful.
//
// The LOCK file is created on first use and is never deleted or replaced: removing a lock file while
// others may be opening it would let one process lock the old inode and another the new one. It is
// empty and is not part of the engine's data (recovery ignores it).
//
// Platform: flock exists on Linux and macOS (the platforms RaftKV supports). Like all advisory
// locks it only stops cooperating processes, and flock on a network filesystem (NFS) may be emulated
// or unreliable: keep the data directory on a local filesystem.
#pragma once

#include <memory>
#include <string>

#include "storage/kv_store.h"

namespace raftkv {

class DirLock {
 public:
  static constexpr const char* kFileName = "LOCK";

  // Locks `dir` (which must exist). kOk and *out set; kIOError if the LOCK file cannot be opened or
  // another holder has the lock (errno EWOULDBLOCK) or flock fails for another reason. On any error
  // *out is unchanged and no file descriptor stays open.
  static Status Acquire(const std::string& dir, std::unique_ptr<DirLock>* out);

  ~DirLock();  // releases the lock; leaves the LOCK file in place
  DirLock(const DirLock&) = delete;
  DirLock& operator=(const DirLock&) = delete;

 private:
  explicit DirLock(int fd) : fd_(fd) {}
  int fd_;
};

}  // namespace raftkv
