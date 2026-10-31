#include "storage/dir_lock.h"

#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

#include <cerrno>

namespace raftkv {

Status DirLock::Acquire(const std::string& dir, std::unique_ptr<DirLock>* out) {
  const std::string path = dir + "/" + kFileName;
  int fd;
  do {
    fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
  } while (fd < 0 && errno == EINTR);
  if (fd < 0) return Status::kIOError;
  int rc;
  do {
    rc = ::flock(fd, LOCK_EX | LOCK_NB);
  } while (rc != 0 && errno == EINTR);
  if (rc != 0) {  // EWOULDBLOCK: another engine holds it; anything else: the lock cannot be taken
    ::close(fd);
    return Status::kIOError;
  }
  out->reset(new DirLock(fd));
  return Status::kOk;
}

DirLock::~DirLock() {
  if (fd_ >= 0) ::close(fd_);  // closing the descriptor releases the flock
}

}  // namespace raftkv
