// raftkv_node --block-cache-bytes: strict parsing, the default, and that the configured size
// reaches the storage engine.
#include <fcntl.h>
#include <gtest/gtest.h>
#include <signal.h>
#include <spawn.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <stdexcept>
#include <memory>
#include <string>
#include <vector>

#include "../test_deadline.h"
#include "node/node_runtime.h"
#include "node/option_parsing.h"
#include "storage/kv_engine.h"

extern char** environ;

namespace raftkv {
namespace {

using namespace std::chrono_literals;
using testutil::RunWithDeadline;

// A refused Start returns at once; a Start that succeeds opens the engine and two gRPC servers.
constexpr std::chrono::milliseconds kRefusalDeadline = 2000ms;
constexpr std::chrono::milliseconds kStartDeadline = 30000ms;
// raftkv_node refusing its command line, or printing its help, exits immediately; the limit is for a
// node that wrongly starts (and would otherwise run until a signal that never comes).
constexpr std::chrono::milliseconds kProgramDeadline = 30000ms;

struct TempDir {
  std::string path;
  TempDir() {
    char tmpl[] = "/tmp/raftkv_node_opts_XXXXXX";
    path = ::mkdtemp(tmpl);
  }
  ~TempDir() {
    std::error_code ec;
    std::filesystem::remove_all(path, ec);
  }
};

// ---- ParseByteCount ----------------------------------------------------------------------------

size_t Parse(const char* text, bool* ok) {
  size_t value = 12345;  // must stay untouched on failure
  *ok = ParseByteCount(text, &value);
  if (!*ok) {
    EXPECT_EQ(value, 12345u) << "a rejected value must not be stored: " << (text ? text : "(null)");
  }
  return value;
}

TEST(ParseByteCount, AcceptsPlainDecimalNumbers) {
  bool ok = false;
  EXPECT_EQ(Parse("0", &ok), 0u);
  EXPECT_TRUE(ok);
  EXPECT_EQ(Parse("1", &ok), 1u);
  EXPECT_TRUE(ok);
  EXPECT_EQ(Parse("4194304", &ok), 4194304u);
  EXPECT_TRUE(ok);
  EXPECT_EQ(Parse("00012", &ok), 12u) << "leading zeros are still decimal";
  EXPECT_TRUE(ok);
  EXPECT_EQ(Parse("1073741824", &ok), 1073741824u);
  EXPECT_TRUE(ok);
}

TEST(ParseByteCount, TheLargestRepresentableValueIsAcceptedAndOneMoreIsNot) {
  constexpr size_t kMax = std::numeric_limits<size_t>::max();
  bool ok = false;
  EXPECT_EQ(Parse(std::to_string(kMax).c_str(), &ok), kMax);
  EXPECT_TRUE(ok);
  EXPECT_EQ(Parse(std::to_string(kMax - 1).c_str(), &ok), kMax - 1);
  EXPECT_TRUE(ok);
  // max + 1, written out in decimal for the size of size_t on this platform.
  const std::string one_more = sizeof(size_t) == 8 ? "18446744073709551616" : "4294967296";
  Parse(one_more.c_str(), &ok);
  EXPECT_FALSE(ok);
  // Overflow only in the last digit step, and far beyond it.
  Parse((std::to_string(kMax) + "0").c_str(), &ok);
  EXPECT_FALSE(ok);
  Parse("99999999999999999999999999999999999999", &ok);
  EXPECT_FALSE(ok);
  Parse(std::string(1000, '9').c_str(), &ok);
  EXPECT_FALSE(ok);
}

TEST(ParseByteCount, RejectsEverythingThatIsNotNonNegativeDigits) {
  bool ok = true;
  for (const char* bad : {"", "-1", "-0", "+1", " 1", "1 ", "1 2", "1k", "10KB", "1e6", "0x10", "1.5", "1,000",
                          "abc", "--1", "1\n", "١٢٣" /* Arabic-Indic digits */, "-18446744073709551615"}) {
    ok = true;
    Parse(bad, &ok);
    EXPECT_FALSE(ok) << "should be rejected: \"" << bad << "\"";
  }
  size_t value = 7;
  EXPECT_FALSE(ParseByteCount(nullptr, &value));
  EXPECT_EQ(value, 7u);
}

// ---- The option reaches the engine ----------------------------------------------------------------

class NodeBlockCacheTest : public ::testing::Test {
 protected:
  NodeOptions Options() {
    NodeOptions o;
    o.id = 1;
    o.cluster = {1};
    o.data_dir = dir_.path;
    o.sync = false;
    o.memtable_bytes = 2048;
    return o;
  }
  // Starts a one-node cluster and returns the storage engine's statistics.
  KVEngine::Stats StartAndStats(const NodeOptions& options) {
    node_ = std::make_unique<NodeRuntime>(options);
    node_->Start();
    auto* engine = dynamic_cast<KVEngine*>(node_->store());
    EXPECT_NE(engine, nullptr);
    return engine != nullptr ? engine->GetStats() : KVEngine::Stats{};
  }
  TempDir dir_;
  std::unique_ptr<NodeRuntime> node_;
};

TEST_F(NodeBlockCacheTest, TheDefaultIsNoCacheAndNothingIsAllocatedUnlessAskedFor) {
  EXPECT_EQ(NodeOptions().block_cache_bytes, 0u);
  EXPECT_EQ(StartAndStats(Options()).block_cache_capacity, 0u);
}

TEST_F(NodeBlockCacheTest, AConfiguredSizeReachesTheEngine) {
  NodeOptions o = Options();
  o.block_cache_bytes = 1 << 20;
  EXPECT_EQ(StartAndStats(o).block_cache_capacity, size_t{1} << 20);
}

TEST_F(NodeBlockCacheTest, ZeroMeansNoCache) {
  NodeOptions o = Options();
  o.block_cache_bytes = 0;
  EXPECT_EQ(StartAndStats(o).block_cache_capacity, 0u);
}

TEST_F(NodeBlockCacheTest, AHugeSizeIsPassedThroughAsIs) {
  NodeOptions o = Options();
  o.block_cache_bytes = std::numeric_limits<size_t>::max();  // the cache allocates nothing up front
  EXPECT_EQ(StartAndStats(o).block_cache_capacity, std::numeric_limits<size_t>::max());
}

TEST_F(NodeBlockCacheTest, TheOldEngineHasNoCacheWhateverTheOptionSays) {
  NodeOptions o = Options();
  o.use_sstables = false;
  o.block_cache_bytes = 1 << 20;
  EXPECT_EQ(StartAndStats(o).block_cache_capacity, 0u);
}

TEST_F(NodeBlockCacheTest, ANodeWithACacheServesReadsThroughIt) {
  NodeOptions o = Options();
  o.block_cache_bytes = 1 << 20;
  StartAndStats(o);
  auto* engine = dynamic_cast<KVEngine*>(node_->store());
  ASSERT_NE(engine, nullptr);
  for (int i = 0; i < 300; ++i) {
    for (int attempt = 0; attempt < 5000; ++attempt) {
      if (engine->Put("key" + std::to_string(1000 + i), std::string(60, 'v')) == Status::kOk) break;
    }
  }
  for (int i = 0; i < 4000 && (engine->GetStats().frozen_memtables != 0 || engine->GetStats().sstables == 0); ++i) {
    ::usleep(5000);
  }
  std::string v;
  ASSERT_EQ(engine->Get("key1000", &v), Status::kOk);
  EXPECT_EQ(v, std::string(60, 'v'));
  EXPECT_GT(engine->GetStats().block_cache_usage, 0u);
}

TEST_F(NodeBlockCacheTest, ASecondNodeOnTheSameDataDirectoryCannotStartAndTheFirstKeepsWorking) {
  StartAndStats(Options());
  auto* engine = dynamic_cast<KVEngine*>(node_->store());
  ASSERT_NE(engine, nullptr);
  ASSERT_EQ(engine->Put("a", "1"), Status::kOk);

  // Start() on its own thread with a deadline: it must be refused at once, never wait for the lock.
  // The result is the exception text, "" if Start succeeded.
  auto start_node = [](NodeOptions options, std::chrono::milliseconds limit) {
    return RunWithDeadline<std::string>(limit, [options] {
      NodeRuntime node(options);
      try {
        node.Start();
        return std::string();
      } catch (const std::runtime_error& e) {
        return std::string(e.what());
      }
    });
  };
  const auto refused = start_node(Options(), kRefusalDeadline);
  ASSERT_TRUE(refused.has_value()) << "Start() did not return within " << kRefusalDeadline.count()
                                   << " ms: the directory lock must not block";
  EXPECT_FALSE(refused->empty()) << "a second node started on a data directory that is in use";
  EXPECT_NE(refused->find("storage engine"), std::string::npos) << *refused;
  EXPECT_NE(refused->find("another raftkv_node"), std::string::npos) << *refused;
  std::string v;
  ASSERT_EQ(engine->Get("a", &v), Status::kOk);
  EXPECT_EQ(v, "1");

  node_.reset();  // the first node stops: its lock is released
  const auto third = start_node(Options(), kStartDeadline);
  ASSERT_TRUE(third.has_value()) << "Start() did not return within " << kStartDeadline.count() << " ms";
  EXPECT_EQ(*third, "") << "the directory should be free again";
}

// ---- The raftkv_node program ----------------------------------------------------------------------

#ifdef RAFTKV_NODE_BIN
struct ProgramResult {
  bool timed_out = false;  // killed after kProgramDeadline
  int code = -1;           // the exit status, -1 if it did not exit normally
  std::string out, err;    // what it wrote to stdout and to stderr
};

std::string ReadFile(const std::string& path) {
  std::ifstream in(path, std::ios::binary);
  return std::string(std::istreambuf_iterator<char>(in), {});
}

// Runs raftkv_node with `args` (no shell: every element is one argument, as given), stdin from
// /dev/null and stdout/stderr captured. A node that has not exited after kProgramDeadline is killed
// and reaped, so a node that wrongly starts cannot hang the test run.
ProgramResult RunNode(const std::vector<std::string>& args) {
  TempDir scratch;
  const std::string out_path = scratch.path + "/stdout", err_path = scratch.path + "/stderr";
  posix_spawn_file_actions_t actions;
  posix_spawn_file_actions_init(&actions);
  posix_spawn_file_actions_addopen(&actions, 0, "/dev/null", O_RDONLY, 0);
  posix_spawn_file_actions_addopen(&actions, 1, out_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
  posix_spawn_file_actions_addopen(&actions, 2, err_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
  std::vector<std::string> storage = {RAFTKV_NODE_BIN};
  storage.insert(storage.end(), args.begin(), args.end());
  std::vector<char*> argv;
  for (std::string& a : storage) argv.push_back(&a[0]);
  argv.push_back(nullptr);

  ProgramResult r;
  pid_t pid = -1;
  const int rc = ::posix_spawn(&pid, RAFTKV_NODE_BIN, &actions, nullptr, argv.data(), environ);
  posix_spawn_file_actions_destroy(&actions);
  if (rc != 0) {
    ADD_FAILURE() << "cannot start " << RAFTKV_NODE_BIN << ": " << std::strerror(rc);
    return r;
  }
  int status = 0;
  const auto deadline = std::chrono::steady_clock::now() + kProgramDeadline;
  pid_t done = 0;
  while ((done = ::waitpid(pid, &status, WNOHANG)) == 0 && std::chrono::steady_clock::now() < deadline) {
    ::usleep(2000);  // poll for the exit; the limit above is what bounds the wait
  }
  if (done == 0) {
    r.timed_out = true;
    ::kill(pid, SIGKILL);
    ::waitpid(pid, &status, 0);
    ADD_FAILURE() << "raftkv_node did not exit within " << kProgramDeadline.count() << " ms; killed";
  } else if (WIFEXITED(status)) {
    r.code = WEXITSTATUS(status);
  }
  r.out = ReadFile(out_path);
  r.err = ReadFile(err_path);
  return r;
}

TEST(NodeProgram, TheFlagIsDocumentedInTheHelp) {
  const ProgramResult r = RunNode({"--help"});
  ASSERT_FALSE(r.timed_out);
  EXPECT_EQ(r.code, 0);
  EXPECT_NE(r.out.find("--block-cache-bytes"), std::string::npos);
}

TEST(NodeProgram, InvalidBlockCacheValuesAreRefusedBeforeAnythingStarts) {
  TempDir data;
  const std::vector<std::string> base = {"--id", "1", "--peers", "1=127.0.0.1:1", "--client-addr", "127.0.0.1:2",
                                         "--data-dir", data.path};
  auto with = [&](std::vector<std::string> extra) {
    std::vector<std::string> a = base;
    a.insert(a.end(), extra.begin(), extra.end());
    return a;
  };
  for (const char* bad : {"-1", "abc", "1k", "1e6", "0x10", "1.5", "+5", "18446744073709551616",
                          "99999999999999999999999999"}) {
    const ProgramResult r = RunNode(with({"--block-cache-bytes", bad}));
    EXPECT_EQ(r.code, 2) << bad;
    EXPECT_NE(r.err.find("--block-cache-bytes"), std::string::npos) << bad << ": " << r.err;
  }
  const ProgramResult missing = RunNode(with({"--block-cache-bytes"}));
  EXPECT_EQ(missing.code, 2) << "a missing value";
  EXPECT_NE(missing.err.find("needs a value"), std::string::npos) << missing.err;
  EXPECT_EQ(RunNode(with({"--block-cache-bytes", ""})).code, 2) << "an empty value";
  EXPECT_FALSE(std::filesystem::exists(data.path + "/kv")) << "a refused command line touched no data";
}
#endif

}  // namespace
}  // namespace raftkv
