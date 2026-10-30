// Model-based test: random Put / Delete / overwrite traffic (with and without applied indices),
// flushes, compactions and reopens, checked against a std::map after every phase. The engine's Get
// and Scan must agree with the model exactly; no writes run during a check, so even the
// per-key consistency of Scan must give the exact model state. Fixed seeds: a failure replays.
#include <gtest/gtest.h>

#include <cstdio>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "engine_test_util.h"
#include "storage/kv_engine.h"

using namespace raftkv;
using raftkv::testutil::TempDir;
using raftkv::testutil::WaitFor;

namespace {

using KVs = std::vector<std::pair<std::string, std::string>>;

struct ModelCase {
  uint64_t seed;
  size_t cache_bytes;
};

class EngineModelTest : public ::testing::TestWithParam<ModelCase> {};

std::string Key(uint64_t n) {
  char k[16];
  std::snprintf(k, sizeof(k), "k%04llu", static_cast<unsigned long long>(n));
  return k;
}

KVs ModelScan(const std::map<std::string, std::string>& model, const std::string& start,
              const std::string& end, size_t limit) {
  KVs out;
  if (!end.empty() && start >= end) return out;
  for (auto it = model.lower_bound(start); it != model.end(); ++it) {
    if (!end.empty() && it->first >= end) break;
    out.push_back(*it);
    if (limit != 0 && out.size() == limit) break;
  }
  return out;
}

TEST_P(EngineModelTest, GetAndScanAgreeWithAStdMap) {
  const ModelCase c = GetParam();
  TempDir dir;
  KVEngine::Options o;
  o.dir = dir.path();
  o.sync_wal = false;
  o.use_sstables = true;
  o.memtable_bytes = 2048;    // a flush every few dozen writes
  o.compaction_trigger = 3;   // and compactions all the time
  o.flush_retry_ms = 5;
  o.block_cache_bytes = c.cache_bytes;
  std::unique_ptr<KVEngine> engine;
  ASSERT_EQ(KVEngine::Open(o, &engine), Status::kOk);

  std::mt19937_64 rng(c.seed);
  auto uniform = [&](uint64_t n) { return std::uniform_int_distribution<uint64_t>(0, n - 1)(rng); };
  constexpr uint64_t kKeySpace = 300;  // small: plenty of overwrites and deletes of live keys
  std::map<std::string, std::string> model;
  uint64_t raft_index = 0;

  auto check = [&](int phase) {
    SCOPED_TRACE("seed " + std::to_string(c.seed) + ", phase " + std::to_string(phase));
    for (int i = 0; i < 60; ++i) {
      const std::string k = Key(uniform(kKeySpace + 20));  // some keys are never written
      std::string v;
      const Status s = engine->Get(k, &v);
      auto it = model.find(k);
      if (it == model.end()) {
        ASSERT_EQ(s, Status::kNotFound) << k;
      } else {
        ASSERT_EQ(s, Status::kOk) << k;
        ASSERT_EQ(v, it->second) << k;
      }
    }
    for (int i = 0; i < 40; ++i) {
      // Bounds are keys, keys with a suffix, or empty; limits 0 (none) to 20.
      auto bound = [&]() -> std::string {
        switch (uniform(4)) {
          case 0: return "";
          case 1: return Key(uniform(kKeySpace + 20));
          case 2: return Key(uniform(kKeySpace + 20)) + "5";
          default: return "k" + std::to_string(uniform(10));
        }
      };
      const std::string start = bound(), end = bound();
      const size_t limit = static_cast<size_t>(uniform(21));
      KVs got{{"stale", "output"}};
      ASSERT_EQ(engine->Scan(start, end, limit, &got), Status::kOk) << start << ".." << end << " limit " << limit;
      ASSERT_EQ(got, ModelScan(model, start, end, limit)) << start << ".." << end << " limit " << limit;
    }
    KVs all;
    ASSERT_EQ(engine->Scan("", "", 0, &all), Status::kOk);
    ASSERT_EQ(all, ModelScan(model, "", "", 0));
  };

  for (int phase = 0; phase < 12; ++phase) {
    for (int op = 0; op < 400; ++op) {
      const std::string k = Key(uniform(kKeySpace));
      const bool indexed = uniform(2) == 0;  // half the writes also record an applied index
      if (uniform(4) == 0) {
        ASSERT_EQ(indexed ? engine->DeleteAt(k, ++raft_index) : engine->Delete(k), Status::kOk);
        model.erase(k);
      } else {
        // Values of 0..80 bytes; the empty value is a legal value, not a deletion.
        std::string v = std::to_string(op) + ":" + std::string(static_cast<size_t>(uniform(80)), 'v');
        if (uniform(10) == 0) v.clear();
        ASSERT_EQ(indexed ? engine->PutAt(k, v, ++raft_index) : engine->Put(k, v), Status::kOk);
        model[k] = v;
      }
    }
    check(phase);  // flushes and compactions may still be running: results must not depend on it
    if (phase % 3 == 1) {
      ASSERT_TRUE(WaitFor([&] { return testutil::Settled(*engine, o.compaction_trigger); }));
      check(phase);  // settled: most data in SSTables
    }
    if (phase % 4 == 3) {
      engine.reset();  // reopen: WAL replay, manifest, orphan cleanup
      ASSERT_EQ(KVEngine::Open(o, &engine), Status::kOk);
      ASSERT_EQ(engine->AppliedIndex(), raft_index);
      check(phase);
    }
  }
  EXPECT_GT(engine->GetStats().sstables + engine->GetStats().compactions_done, 0u);
}

INSTANTIATE_TEST_SUITE_P(Seeds, EngineModelTest,
                         ::testing::Values(ModelCase{1, 0}, ModelCase{2, 0}, ModelCase{3, 0},
                                           ModelCase{1, 64 * 1024}, ModelCase{4, 64 * 1024},
                                           ModelCase{5, 4 * 1024}),
                         [](const ::testing::TestParamInfo<ModelCase>& p) {
                           return "Seed" + std::to_string(p.param.seed) + "Cache" + std::to_string(p.param.cache_bytes);
                         });

}  // namespace
