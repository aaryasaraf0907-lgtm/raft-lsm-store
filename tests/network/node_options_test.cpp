// raftkv_node --block-cache-bytes: strict parsing, the default, and that the configured size
// reaches the storage engine.
#include <gtest/gtest.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstdio>
#include <filesystem>
#include <limits>
#include <stdexcept>
#include <memory>
#include <string>

#include "node/node_runtime.h"
#include "node/option_parsing.h"
#include "storage/kv_engine.h"

namespace raftkv {
namespace {

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

  NodeRuntime second(Options());
  try {
    second.Start();
    ADD_FAILURE() << "a second node started on a data directory that is in use";
  } catch (const std::runtime_error& e) {
    EXPECT_NE(std::string(e.what()).find("storage engine"), std::string::npos) << e.what();
    EXPECT_NE(std::string(e.what()).find("another raftkv_node"), std::string::npos) << e.what();
  }
  std::string v;
  ASSERT_EQ(engine->Get("a", &v), Status::kOk);
  EXPECT_EQ(v, "1");

  node_.reset();  // the first node stops: its lock is released
  NodeRuntime third(Options());
  EXPECT_NO_THROW(third.Start());
}

// ---- The raftkv_node program ----------------------------------------------------------------------

#ifdef RAFTKV_NODE_BIN
// Runs raftkv_node with `args`, discarding its output; returns the exit status (or -1).
int RunNode(const std::string& args, std::string* stderr_text = nullptr) {
  TempDir scratch;
  const std::string err = scratch.path + "/stderr";
  const std::string cmd = std::string(RAFTKV_NODE_BIN) + " " + args + " >/dev/null 2>" + err;
  const int rc = std::system(cmd.c_str());
  if (stderr_text != nullptr) {
    FILE* f = std::fopen(err.c_str(), "r");
    char buf[512];
    size_t n = f != nullptr ? std::fread(buf, 1, sizeof(buf) - 1, f) : 0;
    buf[n] = '\0';
    if (f != nullptr) std::fclose(f);
    *stderr_text = buf;
  }
  return WIFEXITED(rc) ? WEXITSTATUS(rc) : -1;
}

TEST(NodeProgram, TheFlagIsDocumentedInTheHelp) {
  TempDir scratch;
  const std::string out = scratch.path + "/help";
  ASSERT_EQ(std::system((std::string(RAFTKV_NODE_BIN) + " --help >" + out).c_str()), 0);
  FILE* f = std::fopen(out.c_str(), "r");
  ASSERT_NE(f, nullptr);
  std::string text;
  char buf[256];
  size_t n;
  while ((n = std::fread(buf, 1, sizeof(buf), f)) > 0) text.append(buf, n);
  std::fclose(f);
  EXPECT_NE(text.find("--block-cache-bytes"), std::string::npos);
}

TEST(NodeProgram, InvalidBlockCacheValuesAreRefusedBeforeAnythingStarts) {
  TempDir data;
  const std::string base = "--id 1 --peers 1=127.0.0.1:1 --client-addr 127.0.0.1:2 --data-dir " + data.path;
  for (const char* bad : {"-1", "abc", "1k", "1e6", "0x10", "1.5", "+5", "18446744073709551616",
                          "99999999999999999999999999"}) {
    std::string err;
    EXPECT_EQ(RunNode(base + " --block-cache-bytes '" + bad + "'", &err), 2) << bad;
    EXPECT_NE(err.find("--block-cache-bytes"), std::string::npos) << bad << ": " << err;
  }
  std::string err;
  EXPECT_EQ(RunNode(base + " --block-cache-bytes", &err), 2) << "a missing value";
  EXPECT_NE(err.find("needs a value"), std::string::npos) << err;
  EXPECT_EQ(RunNode(base + " --block-cache-bytes ''"), 2) << "an empty value";
  EXPECT_FALSE(std::filesystem::exists(data.path + "/kv")) << "a refused command line touched no data";
}
#endif

}  // namespace
}  // namespace raftkv
