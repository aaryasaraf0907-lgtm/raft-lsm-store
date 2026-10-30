// Write-path benchmark for KVEngine (SSTable mode): fill throughput, overwrite throughput and write
// amplification. Point reads and scans are measured by storage_read_bench.cpp. Built only with
// -DRAFTKV_BUILD_BENCHMARKS=ON; use a Release build (the program says so in its header if not):
//
//   cmake -S . -B build-bench -DCMAKE_BUILD_TYPE=Release -DRAFTKV_BUILD_BENCHMARKS=ON
//   cmake --build build-bench -j8 --target storage_write_bench
//   ./build-bench/benchmarks/storage_write_bench [--keys N] [--value-bytes N] [--overwrites N]
//       [--runs N] [--trigger N] [--memtable-bytes N] [--sync] [--workloads LIST] [--dir PATH]
//
// Workloads (each run starts from an empty directory; --workloads picks a comma-separated subset):
//   fill-seq     `keys` distinct keys, inserted in ascending order
//   fill-random  the same keys in a fixed shuffled order
//   overwrite    fill-random first (not measured), then wait for the engine to go idle, then
//                `overwrites` x `keys` overwrites of uniformly random existing keys (measured)
// Keys are 16 bytes ("k" + 15 digits). Values are --value-bytes bytes. All randomness comes from
// std::mt19937_64 with a fixed seed, so every run writes the same bytes in the same order.
//
// Per run the benchmark measures (1) the time of the write phase, from the first Put to the return
// of the last, and (2) the engine's own byte counters (KVEngine::Stats) over the phase, read after
// the engine has gone idle (every frozen memtable flushed, no compaction waiting). The counters and
// the write-amplification definitions are in docs/storage-design.md section 6.4. Each workload is
// run `runs` times and reported as median [min-max]; throughput does not include the final idle
// wait, write amplification does (it counts the compactions the phase caused).
//
// What it does not count: data still in the active memtable at the end (at most --memtable-bytes,
// whose table write has not happened yet), file system journal and metadata writes, and device-level
// write amplification. With --sync (fsync every WAL append) the throughput is dominated by the disk.
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include "bench_common.h"
#include "storage/kv_engine.h"

namespace {

using raftkv::KVEngine;
using raftkv::Status;
using raftkv::bench::Key;
using raftkv::bench::Spread;
using raftkv::bench::Summarize;
using raftkv::bench::Value;

constexpr uint64_t kSeed = 42;
constexpr size_t kKeyBytes = 16;

[[noreturn]] void Die(const char* what) { raftkv::bench::Die("storage_write_bench", what); }

struct Args {
  uint64_t keys = 200000;
  size_t value_bytes = 100;
  uint64_t overwrites = 2;  // overwrite phase: this many times `keys` operations
  int runs = 5;
  size_t trigger = 4;
  size_t memtable_bytes = 1 << 20;
  bool sync = false;
  std::vector<std::string> workloads = {"fill-seq", "fill-random", "overwrite"};
  std::string dir;
};

Args Parse(int argc, char** argv) {
  Args a;
  for (int i = 1; i < argc; ++i) {
    auto next = [&]() -> const char* {
      if (i + 1 >= argc) Die("missing argument value");
      return argv[++i];
    };
    if (std::strcmp(argv[i], "--keys") == 0) {
      a.keys = std::strtoull(next(), nullptr, 10);
    } else if (std::strcmp(argv[i], "--value-bytes") == 0) {
      a.value_bytes = std::strtoull(next(), nullptr, 10);
    } else if (std::strcmp(argv[i], "--overwrites") == 0) {
      a.overwrites = std::strtoull(next(), nullptr, 10);
    } else if (std::strcmp(argv[i], "--runs") == 0) {
      a.runs = std::atoi(next());
    } else if (std::strcmp(argv[i], "--trigger") == 0) {
      a.trigger = std::strtoull(next(), nullptr, 10);
    } else if (std::strcmp(argv[i], "--memtable-bytes") == 0) {
      a.memtable_bytes = std::strtoull(next(), nullptr, 10);
    } else if (std::strcmp(argv[i], "--sync") == 0) {
      a.sync = true;
    } else if (std::strcmp(argv[i], "--workloads") == 0) {
      a.workloads.clear();
      std::stringstream ss(next());
      std::string w;
      while (std::getline(ss, w, ',')) a.workloads.push_back(w);
    } else if (std::strcmp(argv[i], "--dir") == 0) {
      a.dir = next();
    } else {
      Die("unknown argument (see the comment at the top of the file)");
    }
  }
  if (a.keys < 100 || a.runs < 1 || a.overwrites < 1 || a.workloads.empty() || a.memtable_bytes < 1024) {
    Die("need --keys >= 100, --runs >= 1, --overwrites >= 1, --memtable-bytes >= 1024 and a workload");
  }
  for (const std::string& w : a.workloads) {
    if (w != "fill-seq" && w != "fill-random" && w != "overwrite") Die("unknown workload");
  }
  if (a.dir.empty()) a.dir = (std::filesystem::temp_directory_path() / "raftkv-write-bench").string();
  return a;
}

struct RunResult {
  double seconds = 0;     // the write phase
  uint64_t ops = 0;
  uint64_t user_bytes = 0;
  double wa_wal = 0, wa_flush = 0, wa_compaction = 0, wa_lsm = 0, wa_total = 0;
  size_t sstables = 0;
  uint64_t compactions = 0;
};

KVEngine::Options EngineOptions(const Args& a, const std::string& dir) {
  KVEngine::Options o;
  o.dir = dir;
  o.use_sstables = true;
  o.sync_wal = a.sync;
  o.memtable_bytes = a.memtable_bytes;
  o.compaction_trigger = a.trigger;
  return o;
}

double Ratio(uint64_t bytes, uint64_t user) { return user == 0 ? 0.0 : static_cast<double>(bytes) / static_cast<double>(user); }

// Counters of `after` minus `before` as write-amplification ratios over the user bytes written.
void FillAmplification(const KVEngine::Stats& before, const KVEngine::Stats& after, RunResult* r) {
  const uint64_t user = after.user_bytes_written - before.user_bytes_written;
  const uint64_t wal = after.wal_bytes_written - before.wal_bytes_written;
  const uint64_t flush = after.flush_bytes_written - before.flush_bytes_written;
  const uint64_t comp = after.compaction_bytes_written - before.compaction_bytes_written;
  const uint64_t manifest = after.manifest_bytes_written - before.manifest_bytes_written;
  const uint64_t ckpt = after.checkpoint_bytes_written - before.checkpoint_bytes_written;
  r->user_bytes = user;
  r->wa_wal = Ratio(wal, user);
  r->wa_flush = Ratio(flush, user);
  r->wa_compaction = Ratio(comp, user);
  r->wa_lsm = Ratio(flush + comp, user);
  r->wa_total = Ratio(wal + flush + comp + manifest + ckpt, user);
  r->sstables = after.sstables;
  r->compactions = after.compactions_done - before.compactions_done;
}

// Runs one workload on a fresh directory.
RunResult RunOnce(const Args& a, const std::string& workload) {
  std::error_code ec;
  std::filesystem::remove_all(a.dir, ec);
  std::unique_ptr<KVEngine> db;
  if (KVEngine::Open(EngineOptions(a, a.dir), &db) != Status::kOk) Die("open failed");

  std::mt19937_64 rng(kSeed);
  std::vector<uint64_t> order(a.keys);
  for (uint64_t i = 0; i < a.keys; ++i) order[i] = i;
  if (workload != "fill-seq") std::shuffle(order.begin(), order.end(), rng);

  if (workload == "overwrite") {  // the set-up: not measured
    for (uint64_t n : order) {
      if (db->Put(Key(n), Value(n, a.value_bytes)) != Status::kOk) Die("put failed");
    }
    if (!raftkv::bench::WaitUntilIdle(*db, a.trigger)) Die("flush failing");
  }
  const KVEngine::Stats before = db->GetStats();

  uint64_t ops = 0;
  const auto t0 = std::chrono::steady_clock::now();
  if (workload == "overwrite") {
    std::uniform_int_distribution<uint64_t> pick(0, a.keys - 1);
    const uint64_t total = a.overwrites * a.keys;
    for (uint64_t i = 0; i < total; ++i) {
      const uint64_t n = pick(rng);
      // A value that differs on every overwrite, so no write is a repeat of an earlier one.
      if (db->Put(Key(n), Value(n + (i + 1) * 7919, a.value_bytes)) != Status::kOk) Die("put failed");
    }
    ops = total;
  } else {
    for (uint64_t n : order) {
      if (db->Put(Key(n), Value(n, a.value_bytes)) != Status::kOk) Die("put failed");
    }
    ops = a.keys;
  }
  const auto t1 = std::chrono::steady_clock::now();

  if (!raftkv::bench::WaitUntilIdle(*db, a.trigger)) Die("flush failing");
  const KVEngine::Stats after = db->GetStats();

  // Sanity: the engine under test must actually hold the data (a fill that lost keys proves nothing).
  std::string v;
  for (int i = 0; i < 2000; ++i) {
    const uint64_t n = order[(static_cast<uint64_t>(i) * 2654435761ull) % a.keys];
    if (db->Get(Key(n), &v) != Status::kOk || v.size() != a.value_bytes) Die("verification read failed");
  }

  RunResult r;
  r.seconds = std::chrono::duration<double>(t1 - t0).count();
  r.ops = ops;
  FillAmplification(before, after, &r);
  db.reset();
  std::filesystem::remove_all(a.dir, ec);
  return r;
}

}  // namespace

int main(int argc, char** argv) {
  const Args a = Parse(argc, argv);
  raftkv::bench::PrintEnvironment("storage_write_bench");
  std::printf("# keys %llu, key bytes %zu, value bytes %zu (%zu logical bytes per Put), overwrites %llux, "
              "runs %d, seed %llu\n",
              static_cast<unsigned long long>(a.keys), kKeyBytes, a.value_bytes, kKeyBytes + a.value_bytes,
              static_cast<unsigned long long>(a.overwrites), a.runs, static_cast<unsigned long long>(kSeed));
  std::printf("# engine: SSTable mode, memtable %zu bytes, compaction trigger %zu, WAL fsync %s, "
              "block cache off, data dir %s\n",
              a.memtable_bytes, a.trigger, a.sync ? "ON" : "off", a.dir.c_str());
  std::printf("# throughput = write phase only; WA = engine byte counters over the phase and the idle "
              "wait that follows, divided by the logical bytes written (docs/storage-design.md 6.4)\n");
  std::printf("%-12s %-26s %9s | %-5s %-6s %-6s | %-5s %-6s | %6s %6s\n", "workload", "ops/s median [min-max]", "user MB/s",
              "wal", "flush", "compct", "lsm", "total", "tables", "compct");

  for (const std::string& workload : a.workloads) {
    RunOnce(a, workload);  // warm-up: not reported
    std::vector<double> ops_s, mb_s, wal, flush, comp, lsm, total, tables, compactions;
    for (int r = 0; r < a.runs; ++r) {
      const RunResult x = RunOnce(a, workload);
      ops_s.push_back(static_cast<double>(x.ops) / x.seconds);
      mb_s.push_back(static_cast<double>(x.user_bytes) / x.seconds / 1e6);
      wal.push_back(x.wa_wal);
      flush.push_back(x.wa_flush);
      comp.push_back(x.wa_compaction);
      lsm.push_back(x.wa_lsm);
      total.push_back(x.wa_total);
      tables.push_back(static_cast<double>(x.sstables));
      compactions.push_back(static_cast<double>(x.compactions));
    }
    const Spread ops = Summarize(ops_s);
    char range[64];
    std::snprintf(range, sizeof(range), "%.0f [%.0f-%.0f]", ops.median, ops.min, ops.max);
    std::printf("%-12s %-26s %9.1f | %5.2f %6.2f %6.2f | %5.2f %6.2f | %6.0f %6.0f\n", workload.c_str(), range,
                Summarize(mb_s).median, Summarize(wal).median, Summarize(flush).median, Summarize(comp).median,
                Summarize(lsm).median, Summarize(total).median, Summarize(tables).median,
                Summarize(compactions).median);
    std::printf("#   WA spread over the runs (min-max): lsm %.2f-%.2f, total %.2f-%.2f\n", Summarize(lsm).min,
                Summarize(lsm).max, Summarize(total).min, Summarize(total).max);
  }
  return 0;
}
