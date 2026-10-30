// The data directory's LOCK: one engine per directory, in this process and across processes.
#include "storage/dir_lock.h"

#include <gtest/gtest.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <memory>
#include <set>
#include <string>

#include "engine_test_util.h"
#include "storage/kv_engine.h"

using namespace raftkv;
using raftkv::testutil::TempDir;
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
  std::unique_ptr<KVEngine> first;
  ASSERT_EQ(KVEngine::Open(O(dir_.path()), &first), Status::kOk);
  ASSERT_EQ(first->Put("a", "1"), Status::kOk);

  std::unique_ptr<KVEngine> second;
  EXPECT_EQ(KVEngine::Open(O(dir_.path()), &second), Status::kIOError);
  EXPECT_EQ(second, nullptr) << "a failed Open leaves no engine";

  // The first engine is unaffected: it reads, writes and sees its own data.
  std::string v;
  ASSERT_EQ(first->Get("a", &v), Status::kOk);
  EXPECT_EQ(v, "1");
  ASSERT_EQ(first->Put("b", "2"), Status::kOk);
  ASSERT_EQ(first->Get("b", &v), Status::kOk);
  EXPECT_EQ(v, "2");
  EXPECT_EQ(KVEngine::Open(O(dir_.path()), &second), Status::kIOError) << "still locked";
}

TEST_P(DirLockTest, TheLockIsReleasedWhenTheEngineIsDestroyedAndTheDirectoryCanBeReopened) {
  std::unique_ptr<KVEngine> engine;
  ASSERT_EQ(KVEngine::Open(O(dir_.path()), &engine), Status::kOk);
  ASSERT_EQ(engine->Put("k", "v"), Status::kOk);
  struct stat before;
  ASSERT_EQ(::stat((dir_.path() + "/LOCK").c_str(), &before), 0) << "the LOCK file exists while open";
  engine.reset();

  struct stat after;
  ASSERT_EQ(::stat((dir_.path() + "/LOCK").c_str(), &after), 0) << "the LOCK file is not deleted on close";
  ASSERT_EQ(KVEngine::Open(O(dir_.path()), &engine), Status::kOk);
  std::string v;
  ASSERT_EQ(engine->Get("k", &v), Status::kOk);
  EXPECT_EQ(v, "v") << "the data written by the first engine is recovered";
  struct stat again;
  ASSERT_EQ(::stat((dir_.path() + "/LOCK").c_str(), &again), 0);
  EXPECT_EQ(again.st_ino, before.st_ino) << "the same LOCK file is reused, never replaced";
  EXPECT_EQ(again.st_ino, after.st_ino);
}

TEST_P(DirLockTest, DifferentDirectoriesAreIndependent) {
  TempDir other;
  std::unique_ptr<KVEngine> a, b;
  ASSERT_EQ(KVEngine::Open(O(dir_.path()), &a), Status::kOk);
  ASSERT_EQ(KVEngine::Open(O(other.path()), &b), Status::kOk);
  ASSERT_EQ(a->Put("k", "a"), Status::kOk);
  ASSERT_EQ(b->Put("k", "b"), Status::kOk);
  std::string v;
  ASSERT_EQ(a->Get("k", &v), Status::kOk);
  EXPECT_EQ(v, "a");
  ASSERT_EQ(b->Get("k", &v), Status::kOk);
  EXPECT_EQ(v, "b");
}

TEST_P(DirLockTest, ARejectedOpenDoesNotRecoverOrChangeAnyFile) {
  // Recovery deletes orphan files and truncates WALs. A second engine that is turned away must not
  // have done any of that: the lock comes first. An unlisted .sst is exactly what recovery deletes.
  std::unique_ptr<KVEngine> first;
  KVEngine::Options quiet = O(dir_.path());
  quiet.compaction_trigger = 0;  // the first engine's own background work must not change the files
  ASSERT_EQ(KVEngine::Open(quiet, &first), Status::kOk);
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

  std::unique_ptr<KVEngine> second;
  EXPECT_EQ(KVEngine::Open(quiet, &second), Status::kIOError);
  EXPECT_EQ(Names(dir_.path()), before) << "no file was created or deleted by the rejected open";

  first.reset();
  ASSERT_EQ(KVEngine::Open(quiet, &second), Status::kOk);
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
  std::unique_ptr<KVEngine> engine;
  ASSERT_EQ(KVEngine::Open(Opts(dir.path()), &engine), Status::kCorruption);
  EXPECT_EQ(engine, nullptr);
  // The lock must be free again: removing the bad file lets the next open succeed (a held lock
  // would make this fail with kIOError).
  fs::remove(dir.path() + "/MANIFEST");
  ASSERT_EQ(KVEngine::Open(Opts(dir.path()), &engine), Status::kOk);
}

TEST(DirLock, TheLockIsHeldForAsLongAsTheDirLockObjectLives) {
  TempDir dir;
  std::unique_ptr<DirLock> a, b;
  ASSERT_EQ(DirLock::Acquire(dir.path(), &a), Status::kOk);
  EXPECT_EQ(DirLock::Acquire(dir.path(), &b), Status::kIOError);
  EXPECT_EQ(b, nullptr);
  a.reset();
  EXPECT_EQ(DirLock::Acquire(dir.path(), &b), Status::kOk);
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

struct Child {
  pid_t pid = -1;
  int to_child = -1;    // the parent writes a byte to tell the child to close the engine and exit
  int from_child = -1;  // the child writes 'o' once it holds the engine, or 'f' if its Open failed
};

Child StartChildHoldingTheEngine(const KVEngine::Options& options) {
  int up[2], down[2];
  EXPECT_EQ(::pipe(up), 0);
  EXPECT_EQ(::pipe(down), 0);
  const pid_t pid = ::fork();
  if (pid == 0) {  // child: no gtest assertions, only _exit codes
    ::close(up[0]);
    ::close(down[1]);
    std::unique_ptr<KVEngine> engine;
    const bool ok = KVEngine::Open(options, &engine) == Status::kOk && engine->Put("child", "1") == Status::kOk;
    const char tell = ok ? 'o' : 'f';
    if (::write(up[1], &tell, 1) != 1) ::_exit(3);
    if (!ok) ::_exit(2);
    char go;
    const ssize_t n = ::read(down[0], &go, 1);  // blocks until told (or the parent exits)
    (void)n;
    engine.reset();  // a clean shutdown releases the lock
    ::_exit(0);
  }
  ::close(up[1]);
  ::close(down[0]);
  return Child{pid, down[1], up[0]};
}

TEST_P(DirLockTest, AnotherProcessHoldingTheDirectoryExcludesThisOneUntilItExits) {
  Child child = StartChildHoldingTheEngine(O(dir_.path()));
  ASSERT_GT(child.pid, 0);
  char said = 0;
  ASSERT_EQ(::read(child.from_child, &said, 1), 1);
  ASSERT_EQ(said, 'o') << "the child could not open the directory";

  std::unique_ptr<KVEngine> engine;
  EXPECT_EQ(KVEngine::Open(O(dir_.path()), &engine), Status::kIOError) << "locked by another process";
  EXPECT_EQ(engine, nullptr);

  const char go = 'x';
  ASSERT_EQ(::write(child.to_child, &go, 1), 1);
  int status = 0;
  ASSERT_EQ(::waitpid(child.pid, &status, 0), child.pid);
  ASSERT_TRUE(WIFEXITED(status));
  EXPECT_EQ(WEXITSTATUS(status), 0);
  ::close(child.to_child);
  ::close(child.from_child);

  ASSERT_EQ(KVEngine::Open(O(dir_.path()), &engine), Status::kOk) << "the child's exit released the lock";
  std::string v;
  ASSERT_EQ(engine->Get("child", &v), Status::kOk) << "and its data is recovered";
  EXPECT_EQ(v, "1");
}

TEST_P(DirLockTest, ACrashedProcessLeavesNoStaleLock) {
  Child child = StartChildHoldingTheEngine(O(dir_.path()));
  ASSERT_GT(child.pid, 0);
  char said = 0;
  ASSERT_EQ(::read(child.from_child, &said, 1), 1);
  ASSERT_EQ(said, 'o');
  std::unique_ptr<KVEngine> engine;
  ASSERT_EQ(KVEngine::Open(O(dir_.path()), &engine), Status::kIOError);

  ASSERT_EQ(::kill(child.pid, SIGKILL), 0);  // no destructor, no clean shutdown
  int status = 0;
  ASSERT_EQ(::waitpid(child.pid, &status, 0), child.pid);
  ASSERT_TRUE(WIFSIGNALED(status));
  ::close(child.to_child);
  ::close(child.from_child);

  ASSERT_TRUE(fs::exists(dir_.path() + "/LOCK")) << "the file stays, but nobody holds it";
  ASSERT_EQ(KVEngine::Open(O(dir_.path()), &engine), Status::kOk) << "the kernel released the lock";
  std::string v;
  EXPECT_EQ(engine->Get("child", &v), Status::kOk) << "the child's acknowledged write survives its crash";
}

}  // namespace
