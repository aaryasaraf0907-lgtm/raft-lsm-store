// Point-read and range-scan benchmark for KVEngine (SSTable mode). Fill, overwrite and write
// amplification are measured by storage_write_bench.cpp. Built only with
// -DRAFTKV_BUILD_BENCHMARKS=ON; use a Release build:
//
//   cmake -S . -B build-bench -DCMAKE_BUILD_TYPE=Release -DRAFTKV_BUILD_BENCHMARKS=ON
//   cmake --build build-bench -j8 --target storage_read_bench
//   ./build-bench/benchmarks/storage_read_bench [--keys N] [--reads N] [--runs N]
//       [--trigger N] [--cache-bytes N] [--dir PATH]
//
// The dataset is fixed by the arguments (fixed seed): `keys` keys of 16 bytes ("k" + 15 digits,
// even numbers only, so odd numbers are guaranteed misses that still fall inside the key range),
// 100-byte values, inserted in shuffled order with a 1 MiB memtable. compaction_trigger 0 (the
// default here) never compacts, so the number of SSTables depends only on the arguments.
//
// Each workload runs once as a warm-up, then `runs` times; the report is the median, minimum and
// maximum nanoseconds per operation over those runs.
//
// Compile-time switches, so the same file also builds against older engines for a baseline:
//   RAFTKV_BENCH_NO_SCAN   KVEngine::Scan does not exist: skip the scan workloads
//   RAFTKV_BENCH_NO_CACHE  Options::block_cache_bytes does not exist: --cache-bytes must be 0
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <functional>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "bench_common.h"
#include "storage/kv_engine.h"

namespace {

using raftkv::KVEngine;
using raftkv::Status;

struct Args {
  uint64_t keys = 200000;
  uint64_t reads = 200000;
  int runs = 5;
  size_t trigger = 0;
  size_t cache_bytes = 0;
  std::string dir;
};

using raftkv::bench::Key;
using raftkv::bench::Value;

[[noreturn]] void Die(const char* what) { raftkv::bench::Die("storage_read_bench", what); }

Args Parse(int argc, char** argv) {
  Args a;
  for (int i = 1; i < argc; ++i) {
    auto next = [&]() -> const char* {
      if (i + 1 >= argc) Die("missing argument value");
      return argv[++i];
    };
    if (std::strcmp(argv[i], "--keys") == 0) {
      a.keys = std::strtoull(next(), nullptr, 10);
    } else if (std::strcmp(argv[i], "--reads") == 0) {
      a.reads = std::strtoull(next(), nullptr, 10);
    } else if (std::strcmp(argv[i], "--runs") == 0) {
      a.runs = std::atoi(next());
    } else if (std::strcmp(argv[i], "--trigger") == 0) {
      a.trigger = std::strtoull(next(), nullptr, 10);
    } else if (std::strcmp(argv[i], "--cache-bytes") == 0) {
      a.cache_bytes = std::strtoull(next(), nullptr, 10);
    } else if (std::strcmp(argv[i], "--dir") == 0) {
      a.dir = next();
    } else {
      Die("unknown argument (see the comment at the top of the file)");
    }
  }
  if (a.keys < 100 || a.reads == 0 || a.runs < 1) Die("need --keys >= 100, --reads >= 1, --runs >= 1");
  if (a.dir.empty()) {
    a.dir = (std::filesystem::temp_directory_path() / "raftkv-read-bench").string();
  }
  return a;
}

// Times `op` over `n` operations per run; prints median/min/max ns per operation.
void Measure(const char* name, uint64_t n, int runs, const std::function<void()>& op) {
  std::vector<double> ns;
  for (int r = -1; r < runs; ++r) {  // r == -1: warm-up, not reported
    const auto t0 = std::chrono::steady_clock::now();
    for (uint64_t i = 0; i < n; ++i) op();
    const auto t1 = std::chrono::steady_clock::now();
    if (r >= 0) ns.push_back(std::chrono::duration<double, std::nano>(t1 - t0).count() / static_cast<double>(n));
  }
  const raftkv::bench::Spread sp = raftkv::bench::Summarize(ns);
  std::printf("%-22s %12.1f %12.1f %12.1f\n", name, sp.median, sp.min, sp.max);
}

}  // namespace

int main(int argc, char** argv) {
  const Args args = Parse(argc, argv);
  std::error_code ec;
  std::filesystem::remove_all(args.dir, ec);

  KVEngine::Options o;
  o.dir = args.dir;
  o.use_sstables = true;
  o.sync_wal = false;  // the fill is not what is measured
  o.memtable_bytes = 1 << 20;
  o.compaction_trigger = args.trigger;
#ifdef RAFTKV_BENCH_NO_CACHE
  if (args.cache_bytes != 0) Die("this build has no block cache (RAFTKV_BENCH_NO_CACHE)");
#else
  o.block_cache_bytes = args.cache_bytes;
#endif
  std::unique_ptr<KVEngine> db;
  if (KVEngine::Open(o, &db) != Status::kOk) Die("open failed");

  std::mt19937_64 rng(42);
  std::vector<uint64_t> order(args.keys);
  for (uint64_t i = 0; i < args.keys; ++i) order[i] = 2 * i;  // present keys are even
  std::shuffle(order.begin(), order.end(), rng);
  for (uint64_t n : order) {
    if (db->Put(Key(n), Value(n)) != Status::kOk) Die("put failed");
  }
  // Wait until every frozen memtable is flushed (and, with a trigger, compactions are done).
  if (!raftkv::bench::WaitUntilIdle(*db, args.trigger)) Die("flush failing");
  const KVEngine::Stats s = db->GetStats();
  raftkv::bench::PrintEnvironment("storage_read_bench");
  std::printf("keys %llu, reads/run %llu, runs %d, trigger %zu, cache_bytes %zu, sstables %zu\n",
              static_cast<unsigned long long>(args.keys), static_cast<unsigned long long>(args.reads),
              args.runs, args.trigger, args.cache_bytes, s.sstables);
  std::printf("%-22s %12s %12s %12s\n", "workload", "median ns/op", "min", "max");

  std::uniform_int_distribution<uint64_t> any(0, args.keys - 1);
  std::uniform_int_distribution<uint64_t> hot(0, std::max<uint64_t>(1, args.keys / 100) - 1);
  std::string value;
  uint64_t found = 0;
  Measure("get hit (uniform)", args.reads, args.runs, [&] {
    found += db->Get(Key(order[any(rng)]), &value) == Status::kOk;
  });
  Measure("get miss (uniform)", args.reads, args.runs, [&] {
    found += db->Get(Key(2 * any(rng) + 1), &value) == Status::kOk;
  });
  Measure("get hit (hot 1%)", args.reads, args.runs, [&] {
    found += db->Get(Key(order[hot(rng)]), &value) == Status::kOk;
  });
#ifndef RAFTKV_BENCH_NO_SCAN
  std::vector<std::pair<std::string, std::string>> out;
  const uint64_t scans = std::max<uint64_t>(1, args.reads / 100);
  Measure("scan 100 (uniform)", scans, args.runs, [&] {
    if (db->Scan(Key(2 * any(rng)), "", 100, &out) != Status::kOk) Die("scan failed");
    found += out.size();
  });
  Measure("scan all", 1, args.runs, [&] {
    if (db->Scan("", "", 0, &out) != Status::kOk) Die("scan failed");
    if (out.size() != args.keys) Die("full scan returned the wrong number of keys");
  });
#endif
  std::printf("(checksum %llu)\n", static_cast<unsigned long long>(found));  // keeps the reads live
  db.reset();
  std::filesystem::remove_all(args.dir, ec);
  return 0;
}
