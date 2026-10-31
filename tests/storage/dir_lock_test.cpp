// The data directory's LOCK: one engine per directory, in this process and across processes.
#include "storage/dir_lock.h"

#include <gtest/gtest.h>
#include <poll.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <set>
#include <string>
#include <utility>

#include "../test_deadline.h"
#include "engine_test_util.h"
#include "storage/kv_engine.h"

using namespace raftkv;
using namespace std::chrono_literals;
using raftkv::testutil::RunWithDeadline;
using raftkv::testutil::TempDir;
using raftkv::testutil::WaitFor;
namespace fs = std::filesystem;

namespace {

KVEngine::Options Opts(const std::string& dir, bool sstables = true) {
  KVEngine::Options o;
  o.dir = dir;
  o.sync_wal = false;
  o.use_sstables = sstables;
  o.memtable_bytes = 2048;
  o.flush_retry_ms = 5;
  return o;
}

// Every Open below runs against a deadline. A refused Open returns at once (the lock is LOCK_NB); if
// that ever became a blocking flock, an unguarded test would hang the whole CI job instead of
// failing. The deadlines are generous for the machine and tiny for the bug.
constexpr std::chrono::milliseconds kRefusalDeadline = 2000ms;  // a refused Open
constexpr std::chrono::milliseconds kOpenDeadline = 30000ms;    // an Open that succeeds: recovery, threads
constexpr std::chrono::milliseconds kProcessDeadline = 30000ms;  // a child process to report or to exit

struct Opened {
  Status status = Status::kOk;
  std::unique_ptr<KVEngine> engine;
};

// KVEngine::Open on its own thread. Returns false, after recording a test failure, if it does not
// return within `limit`. (The thread is detached and owns copies of its inputs.)
bool OpenWithin(std::chrono::milliseconds limit, const KVEngine::Options& options, Opened* out) {
  auto result = RunWithDeadline<Opened>(limit, [options] {
    Opened r;
    r.status = KVEngine::Open(options, &r.engine);
    return r;
  });
  if (!result) {
    ADD_FAILURE() << "KVEngine::Open did not return within " << limit.count()
                  << " ms: the directory lock must not block (flock needs LOCK_NB)";
    return false;
  }
  *out = std::move(*result);
  return true;
}

std::set<std::string> Names(const std::string& dir) {
  std::set<std::string> out;
  for (const auto& de : fs::directory_iterator(dir)) out.insert(de.path().filename().string());
  return out;
}

class DirLockTest : public ::testing::TestWithParam<bool> {  // the parameter: SSTable mode
 protected:
  KVEngine::Options O(const std::string& dir) { return Opts(dir, GetParam()); }
  TempDir dir_;
};

INSTANTIATE_TEST_SUITE_P(Modes, DirLockTest, ::testing::Values(true, false),
                         [](const ::testing::TestParamInfo<bool>& p) {
                           return p.param ? std::string("SSTableMode") : std::string("LegacyMode");
                         });

TEST_P(DirLockTest, ASecondEngineCannotOpenADirectoryThatIsInUse) {
  Opened first_open;
  ASSERT_TRUE(OpenWithin(kOpenDeadline, O(dir_.path()), &first_open));
  ASSERT_EQ(first_open.status, Status::kOk);
  KVEngine* first = first_open.engine.get();
  ASSERT_EQ(first->Put("a", "1"), Status::kOk);

  Opened second;
  ASSERT_TRUE(OpenWithin(kRefusalDeadline, O(dir_.path()), &second));
  EXPECT_EQ(second.status, Status::kIOError);
  EXPECT_EQ(second.engine, nullptr) << "a failed Open leaves no engine";

  // The first engine is unaffected: it reads, writes and sees its own data.
  std::string v;
  ASSERT_EQ(first->Get("a", &v), Status::kOk);
  EXPECT_EQ(v, "1");
  ASSERT_EQ(first->Put("b", "2"), Status::kOk);
  ASSERT_EQ(first->Get("b", &v), Status::kOk);
  EXPECT_EQ(v, "2");
  Opened third;
  ASSERT_TRUE(OpenWithin(kRefusalDeadline, O(dir_.path()), &third));
  EXPECT_EQ(third.status, Status::kIOError) << "still locked";
}

TEST_P(DirLockTest, TheLockIsReleasedWhenTheEngineIsDestroyedAndTheDirectoryCanBeReopened) {
  Opened opened;
  ASSERT_TRUE(OpenWithin(kOpenDeadline, O(dir_.path()), &opened));
  ASSERT_EQ(opened.status, Status::kOk);
  ASSERT_EQ(opened.engine->Put("k", "v"), Status::kOk);
  struct stat before;
  ASSERT_EQ(::stat((dir_.path() + "/LOCK").c_str(), &before), 0) << "the LOCK file exists while open";
  opened.engine.reset();

  struct stat after;
  ASSERT_EQ(::stat((dir_.path() + "/LOCK").c_str(), &after), 0) << "the LOCK file is not deleted on close";
  Opened reopened;
  ASSERT_TRUE(OpenWithin(kOpenDeadline, O(dir_.path()), &reopened));
  ASSERT_EQ(reopened.status, Status::kOk);
  std::string v;
  ASSERT_EQ(reopened.engine->Get("k", &v), Status::kOk);
  EXPECT_EQ(v, "v") << "the data written by the first engine is recovered";
  struct stat again;
  ASSERT_EQ(::stat((dir_.path() + "/LOCK").c_str(), &again), 0);
  EXPECT_EQ(again.st_ino, before.st_ino) << "the same LOCK file is reused, never replaced";
  EXPECT_EQ(again.st_ino, after.st_ino);
}

TEST_P(DirLockTest, DifferentDirectoriesAreIndependent) {
  TempDir other;
  Opened a, b;
  ASSERT_TRUE(OpenWithin(kOpenDeadline, O(dir_.path()), &a));
  ASSERT_TRUE(OpenWithin(kOpenDeadline, O(other.path()), &b));
  ASSERT_EQ(a.status, Status::kOk);
  ASSERT_EQ(b.status, Status::kOk);
  ASSERT_EQ(a.engine->Put("k", "a"), Status::kOk);
  ASSERT_EQ(b.engine->Put("k", "b"), Status::kOk);
  std::string v;
  ASSERT_EQ(a.engine->Get("k", &v), Status::kOk);
  EXPECT_EQ(v, "a");
  ASSERT_EQ(b.engine->Get("k", &v), Status::kOk);
  EXPECT_EQ(v, "b");
}

TEST_P(DirLockTest, ARejectedOpenDoesNotRecoverOrChangeAnyFile) {
  // Recovery deletes orphan files and truncates WALs. A second engine that is turned away must not
  // have done any of that: the lock comes first. An unlisted .sst is exactly what recovery deletes.
  KVEngine::Options quiet = O(dir_.path());
  quiet.compaction_trigger = 0;  // the first engine's own background work must not change the files
  Opened first_open;
  ASSERT_TRUE(OpenWithin(kOpenDeadline, quiet, &first_open));
  ASSERT_EQ(first_open.status, Status::kOk);
  std::unique_ptr<KVEngine>& first = first_open.engine;
  ASSERT_EQ(first->Put("k", "v"), Status::kOk);
  if (GetParam()) {  // flush at least one memtable, so that a manifest exists and the orphan is plain
    for (int i = 0; i < 400 && first->GetStats().flushes_done == 0; ++i) {
      ASSERT_EQ(first->Put("fill" + std::to_string(i), std::string(100, 'f')), Status::kOk);
    }
    ASSERT_TRUE(testutil::WaitFor([&] {
      const KVEngine::Stats st = first->GetStats();
      return st.flushes_done >= 1 && st.frozen_memtables == 0;
    }));
  }
  { std::ofstream(dir_.path() + "/999999.sst") << "an orphan that recovery would delete"; }
  { std::ofstream(dir_.path() + "/leftover.tmp.abc") << "a temp file that recovery would delete"; }
  const std::set<std::string> before = Names(dir_.path());

  Opened second;
  ASSERT_TRUE(OpenWithin(kRefusalDeadline, quiet, &second));
  EXPECT_EQ(second.status, Status::kIOError);
  EXPECT_EQ(Names(dir_.path()), before) << "no file was created or deleted by the rejected open";

  first.reset();
  Opened third;
  ASSERT_TRUE(OpenWithin(kOpenDeadline, quiet, &third));
  ASSERT_EQ(third.status, Status::kOk);
  if (GetParam()) {
    EXPECT_FALSE(fs::exists(dir_.path() + "/999999.sst")) << "a real open still cleans up";
    EXPECT_FALSE(fs::exists(dir_.path() + "/leftover.tmp.abc"));
  }
}

TEST(DirLock, AnOpenThatFailsAfterTakingTheLockReleasesIt) {
  // An undecodable manifest makes recovery fail (SSTable mode only: the legacy engine has no
  // manifest) after the lock was taken.
  TempDir dir;
  { std::ofstream(dir.path() + "/MANIFEST") << "this is not a manifest"; }
  Opened failed;
  ASSERT_TRUE(OpenWithin(kOpenDeadline, Opts(dir.path()), &failed));
  ASSERT_EQ(failed.status, Status::kCorruption);
  EXPECT_EQ(failed.engine, nullptr);
  // The lock must be free again: removing the bad file lets the next open succeed (a held lock
  // would make this fail with kIOError).
  fs::remove(dir.path() + "/MANIFEST");
  Opened retry;
  ASSERT_TRUE(OpenWithin(kRefusalDeadline, Opts(dir.path()), &retry));
  ASSERT_EQ(retry.status, Status::kOk);
}

TEST(DirLock, TheLockIsHeldForAsLongAsTheDirLockObjectLives) {
  TempDir dir;
  using Acquired = std::pair<Status, std::unique_ptr<DirLock>>;
  auto acquire = [&dir](std::chrono::milliseconds limit) {
    const std::string path = dir.path();
    auto r = RunWithDeadline<Acquired>(limit, [path] {
      Acquired x;
      x.first = DirLock::Acquire(path, &x.second);
      return x;
    });
    EXPECT_TRUE(r.has_value()) << "DirLock::Acquire did not return within " << limit.count()
                               << " ms: it must not block (flock needs LOCK_NB)";
    return r ? std::move(*r) : Acquired(Status::kIOError, nullptr);
  };
  Acquired a = acquire(kOpenDeadline);
  ASSERT_EQ(a.first, Status::kOk);
  Acquired b = acquire(kRefusalDeadline);
  EXPECT_EQ(b.first, Status::kIOError);
  EXPECT_EQ(b.second, nullptr);
  a.second.reset();
  b = acquire(kRefusalDeadline);
  EXPECT_EQ(b.first, Status::kOk);
  EXPECT_TRUE(fs::exists(dir.path() + "/LOCK"));
}

TEST(DirLock, AMissingDirectoryIsAnErrorNotACreatedOne) {
  TempDir dir;
  std::unique_ptr<DirLock> lock;
  EXPECT_EQ(DirLock::Acquire(dir.path() + "/does/not/exist", &lock), Status::kIOError);
  EXPECT_EQ(lock, nullptr);
  EXPECT_FALSE(fs::exists(dir.path() + "/does"));
}

// ---------------------------------------------------------------------------------------------
// Another PROCESS. The child opens the engine, tells the parent, and waits; the parent must be
// refused. The child is forked before the parent starts any thread of its own.
//
// Nothing here may block without a limit: a child that never reports, never exits or is left behind
// by a failed assertion would otherwise hang the test (and keep the directory locked). Reads use
// poll() with a deadline, waiting for the exit polls waitpid(WNOHANG) under the same deadline, a
// child that overruns is killed and reaped, the guard does the same when the test ends early, and
// the child has an alarm() as a last resort should the parent itself disappear.

class ChildProcess {
 public:
  // Forks a child that opens `options`, writes 'o' (or 'f' if that failed) to the parent, waits for
  // a byte back, closes the engine and exits 0.
  static ChildProcess HoldingTheEngine(const KVEngine::Options& options) {
    int up[2], down[2];
    EXPECT_EQ(::pipe(up), 0);
    EXPECT_EQ(::pipe(down), 0);
    const pid_t pid = ::fork();
    if (pid == 0) {  // child: no gtest assertions, only _exit codes
      ::alarm(120);  // dies of SIGALRM if everything else goes wrong
      ::close(up[0]);
      ::close(down[1]);
      std::unique_ptr<KVEngine> engine;
      const bool ok = KVEngine::Open(options, &engine) == Status::kOk && engine->Put("child", "1") == Status::kOk;
      const char tell = ok ? 'o' : 'f';
      if (::write(up[1], &tell, 1) != 1) ::_exit(3);
      if (!ok) ::_exit(2);
      char go;
      const ssize_t n = ::read(down[0], &go, 1);  // until told, or until the parent is gone (EOF)
      (void)n;
      engine.reset();  // a clean shutdown releases the lock
      ::_exit(0);
    }
    ::close(up[1]);
    ::close(down[0]);
    return ChildProcess(pid, down[1], up[0]);
  }

  ChildProcess(ChildProcess&& o) noexcept : pid_(o.pid_), to_child_(o.to_child_), from_child_(o.from_child_) {
    o.pid_ = -1;
    o.to_child_ = o.from_child_ = -1;
  }
  ChildProcess(const ChildProcess&) = delete;
  ChildProcess& operator=(const ChildProcess&) = delete;
  ~ChildProcess() {
    Kill();
    CloseFds();
  }

  bool started() const { return pid_ > 0; }

  // The child's report: 'o' (it holds the engine) or 'f'. False if nothing arrived in time.
  bool ReadReport(char* out) {
    struct pollfd p = {from_child_, POLLIN, 0};
    const int ready = ::poll(&p, 1, static_cast<int>(kProcessDeadline.count()));
    if (ready != 1) {
      ADD_FAILURE() << "the child process did not report within " << kProcessDeadline.count() << " ms";
      return false;
    }
    return ::read(from_child_, out, 1) == 1;
  }

  // Tells the child to close the engine and exit.
  bool Release() {
    const char go = 'x';
    return ::write(to_child_, &go, 1) == 1;
  }

  void KillNow() { EXPECT_EQ(::kill(pid_, SIGKILL), 0) << "kill() failed"; }

  // Waits for the child to exit; false (after killing and reaping it) if it does not in time.
  bool WaitForExit(int* status) {
    if (reaped_) return false;
    const bool exited = WaitFor([&] {
      const pid_t r = ::waitpid(pid_, status, WNOHANG);
      if (r == pid_) reaped_ = true;
      return r == pid_ || r < 0;
    }, kProcessDeadline);
    if (!exited || !reaped_) {
      ADD_FAILURE() << "the child process did not exit within " << kProcessDeadline.count() << " ms";
      Kill();
      return false;
    }
    return true;
  }

 private:
  ChildProcess(pid_t pid, int to_child, int from_child) : pid_(pid), to_child_(to_child), from_child_(from_child) {}
  void Kill() {
    if (pid_ > 0 && !reaped_) {
      ::kill(pid_, SIGKILL);
      int status = 0;
      ::waitpid(pid_, &status, 0);  // a SIGKILLed process is reaped at once
      reaped_ = true;
    }
  }
  void CloseFds() {
    if (to_child_ >= 0) ::close(to_child_);
    if (from_child_ >= 0) ::close(from_child_);
    to_child_ = from_child_ = -1;
  }

  pid_t pid_;
  int to_child_;
  int from_child_;
  bool reaped_ = false;
};

TEST_P(DirLockTest, AnotherProcessHoldingTheDirectoryExcludesThisOneUntilItExits) {
  ChildProcess child = ChildProcess::HoldingTheEngine(O(dir_.path()));
  ASSERT_TRUE(child.started());
  char said = 0;
  ASSERT_TRUE(child.ReadReport(&said));
  ASSERT_EQ(said, 'o') << "the child could not open the directory";

  Opened refused;
  ASSERT_TRUE(OpenWithin(kRefusalDeadline, O(dir_.path()), &refused));
  EXPECT_EQ(refused.status, Status::kIOError) << "locked by another process";
  EXPECT_EQ(refused.engine, nullptr);

  ASSERT_TRUE(child.Release());
  int status = 0;
  ASSERT_TRUE(child.WaitForExit(&status));
  ASSERT_TRUE(WIFEXITED(status));
  EXPECT_EQ(WEXITSTATUS(status), 0);

  Opened engine;
  ASSERT_TRUE(OpenWithin(kOpenDeadline, O(dir_.path()), &engine));
  ASSERT_EQ(engine.status, Status::kOk) << "the child's exit released the lock";
  std::string v;
  ASSERT_EQ(engine.engine->Get("child", &v), Status::kOk) << "and its data is recovered";
  EXPECT_EQ(v, "1");
}

TEST_P(DirLockTest, ACrashedProcessLeavesNoStaleLock) {
  ChildProcess child = ChildProcess::HoldingTheEngine(O(dir_.path()));
  ASSERT_TRUE(child.started());
  char said = 0;
  ASSERT_TRUE(child.ReadReport(&said));
  ASSERT_EQ(said, 'o');
  Opened refused;
  ASSERT_TRUE(OpenWithin(kRefusalDeadline, O(dir_.path()), &refused));
  ASSERT_EQ(refused.status, Status::kIOError);

  child.KillNow();  // no destructor, no clean shutdown
  int status = 0;
  ASSERT_TRUE(child.WaitForExit(&status));
  ASSERT_TRUE(WIFSIGNALED(status));

  ASSERT_TRUE(fs::exists(dir_.path() + "/LOCK")) << "the file stays, but nobody holds it";
  Opened engine;
  ASSERT_TRUE(OpenWithin(kOpenDeadline, O(dir_.path()), &engine));
  ASSERT_EQ(engine.status, Status::kOk) << "the kernel released the lock";
  std::string v;
  EXPECT_EQ(engine.engine->Get("child", &v), Status::kOk) << "the child's acknowledged write survives its crash";
}

}  // namespace
