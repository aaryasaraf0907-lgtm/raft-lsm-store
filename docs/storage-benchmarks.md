# Storage benchmarks

Two opt-in programs measure the storage engine (`KVEngine`, SSTable mode) on its own, without Raft or the
network. They are not run by `ctest` or by CI and have no pass/fail thresholds: they print numbers.

| Program | Measures |
|---|---|
| `benchmarks/storage_read_bench.cpp` | point reads (hits, misses, a hot 1%), range scans, with and without the block cache |
| `benchmarks/storage_write_bench.cpp` | fill throughput (ascending and random keys), overwrite throughput, and write amplification |

Shared helpers (key and value generation, median/min/max, the environment header, waiting for the engine
to go idle) are in `benchmarks/bench_common.h`. The cluster-level benchmark (three nodes, over gRPC) is a
different tool, `scripts/bench_cluster.sh`, with its results in [`benchmark-results.md`](benchmark-results.md).

## Running them

Use a Release build in its own directory. Every program prints its environment first (CPU, OS, compiler,
build type) and says so in the header if it was not built with `NDEBUG`: do not compare such numbers.

```
cmake -S . -B build-bench -DCMAKE_BUILD_TYPE=Release -DRAFTKV_BUILD_BENCHMARKS=ON
cmake --build build-bench -j8 --target storage_read_bench storage_write_bench
./build-bench/benchmarks/storage_write_bench
./build-bench/benchmarks/storage_read_bench --trigger 4
```

`RAFTKV_BUILD_BENCHMARKS` is `OFF` by default; the benchmarks compile with the project's warning flags
(`-Wall -Wextra -Wpedantic -Werror`). Run them on an otherwise idle machine, with the data directory (`--dir`,
default a temporary directory) on the file system you want to measure.

`storage_write_bench` options: `--keys N` (200000), `--value-bytes N` (100), `--overwrites N` (2: the
overwrite phase writes N times `keys` Puts), `--runs N` (5), `--trigger N` (compaction trigger, 4; 0 = never),
`--memtable-bytes N` (1048576), `--sync` (fsync every WAL append; off by default), `--workloads LIST`
(`fill-seq,fill-random,overwrite`), `--dir PATH`. `storage_read_bench` options are listed at the top of its
source file.

## Workloads and methodology

- **Data.** Keys are 16 bytes (`k` and 15 digits), values `--value-bytes` bytes (100): 116 logical bytes per
  Put. All randomness is `std::mt19937_64` with a fixed seed, so every run writes the same bytes in the same
  order. No compression exists in the engine, so value content does not affect size.
- **fill-seq / fill-random.** `keys` distinct keys into an empty engine, in ascending or in a fixed shuffled
  order.
- **overwrite.** fill-random first (not measured), wait until the engine is idle, then `overwrites x keys`
  Puts of uniformly random existing keys, each with a value that differs from every earlier one. Only this
  phase is measured.
- **Repetition.** Each workload runs once as a warm-up (not reported), then `--runs` times, every run on a
  fresh directory. Throughput and the amplification ratios are reported as the median over the runs, with
  the minimum and maximum of throughput and a min-max line for amplification.
- **Throughput** is the time from the first Put to the return of the last, in ops/s and in logical MB/s. It
  does not include the wait for background work that follows.
- **Write amplification** comes from the engine's own byte counters (`KVEngine::Stats`), taken before the
  phase and after the engine has gone idle (every frozen memtable flushed, no compaction waiting), so a
  phase is charged for the compactions it caused. The counters and the exact definitions are in
  [`storage-design.md`](storage-design.md) section 6.4: WAL, flush, compaction, LSM = (flush + compaction)
  / user, and total (adds the manifest). Nothing is estimated.
- **Check.** After each run 2,000 keys are read back; a run that lost data aborts instead of reporting.
- The benchmark does not change the compaction policy (`NewestRunToCompact`, section 8.1 of the storage
  design); it measures what the policy does. Results from before and after a change to the policy are only
  comparable when measured in the same session (see Results).

## Limitations

- One machine, one file system, one run of the matrix: results characterise that setup, not the engine in
  general. Results from different machines, compilers or build types must not be compared with each other.
- Compaction runs on a background thread, so *when* it runs relative to the writes varies from run to run;
  that is why the compaction share of amplification has a visible spread even with a fixed workload.
- The data still in the active memtable when a workload ends (at most `--memtable-bytes`) has not been
  written to a table; flush and LSM amplification are slightly low for small data sets (here the data set is
  23 MB against a 1 MiB memtable, up to about 5%).
- The WAL is not fsynced by default (as in the cluster node, where the Raft log is the durable copy), so the
  throughput is that of the in-memory write path plus `write()` calls. Use `--sync` for fsync-bound numbers.
- Write amplification excludes file system journal and metadata writes and device-level amplification.
- The data is mostly in the OS page cache; the read benchmark in particular does not measure cold disk reads.

## Results

Measured 2026-10-31 on an Intel Core i5-13450HX (16 hardware threads), 7.6 GiB visible to WSL2, Ubuntu
26.04.1 under WSL2 (kernel 6.18.54.1), g++ 15.2.0, CMake `Release` (`-O3 -DNDEBUG`), data directory under
`/tmp` (the WSL2 virtual disk). Settings: 200,000 keys, 16 + 100 bytes, memtable 1 MiB, WAL not fsynced,
no block cache, 2x overwrites. Each invocation runs 5 measured runs after a warm-up and was repeated in 3
rounds, so every cell is the **median of the 3 round medians** (15 runs); the ranges are the minimum and
maximum over all 15 runs. `tables` and `compactions` are the number of live SSTables at the end and of
compactions in the phase.

These are measurements of the compaction policy with tables weighed by their **file size in bytes**
(`SSTable::file_bytes()`), the policy as it is now. Compaction sizes were entry counts until this was
changed, so the second table compares the two. Both were built from the same compiler and flags
(`main` before the change, and the change), and run **interleaved in one session**: in each round both
builds ran at each trigger, and the order alternated between rounds (before first, after first, before
first). Sessions are not comparable with each other on this machine. This one replaces the table of
2026-10-30, which measured the entry-count policy only; today's "before" columns reproduce it within the
run-to-run spread (LSM amplification, 2026-10-30 against today's before, trigger 2: 4.25, 4.46, 3.60 against
3.86, 4.70, 3.64; trigger 4: 3.02, 3.72, 3.02 against 3.04, 3.47, 3.14; trigger 8: identical, 3.45, 3.09,
2.80), but not the throughput: with compaction on, both builds wrote 0.4-0.7 million Puts/s today against
0.8-0.95 million then, while with compaction off the two sessions are close (0.80-0.84 million today, 0.82-0.95 million then). The cause was
not investigated.

| trigger | workload | Puts/s median [min-max] | logical MB/s | WAL | flush | compaction | LSM | total | tables | compactions |
|---|---|---|---|---|---|---|---|---|---|---|
| 0 | fill-seq | 841,115 [700,082-964,294] | 97.6 | 1.11 | 1.09 | 0.00 | 1.09 | 2.20 | 45 | 0 |
| 0 | fill-random | 808,607 [306,977-830,983] | 93.8 | 1.11 | 1.09 | 0.00 | 1.09 | 2.20 | 45 | 0 |
| 0 | overwrite | 802,116 [672,516-826,362] | 93.0 | 1.11 | 1.09 | 0.00 | 1.09 | 2.21 | 136 | 0 |
| 2 | fill-seq | 567,373 [124,827-574,038] | 65.8 | 1.11 | 1.09 | 3.59 | 4.68 | 5.79 | 1 | 15 |
| 2 | fill-random | 503,029 [206,355-775,055] | 58.4 | 1.11 | 1.09 | 3.47 | 4.56 | 5.67 | 1 | 16 |
| 2 | overwrite | 502,075 [365,662-724,022] | 58.2 | 1.11 | 1.09 | 2.54 | 3.63 | 4.74 | 1 | 11 |
| 4 | fill-seq | 541,624 [355,121-639,858] | 62.8 | 1.11 | 1.09 | 1.91 | 2.99 | 4.10 | 3 | 10 |
| 4 | fill-random | 492,682 [376,444-706,094] | 57.2 | 1.11 | 1.09 | 2.05 | 3.14 | 4.25 | 3 | 12 |
| 4 | overwrite | 481,006 [106,113-632,481] | 55.8 | 1.11 | 1.09 | 2.15 | 3.24 | 4.36 | 2 | 14 |
| 8 | fill-seq | 567,172 [360,386-570,724] | 65.8 | 1.11 | 1.09 | 1.93 | 3.02 | 4.13 | 2 | 9 |
| 8 | fill-random | 494,755 [356,379-503,819] | 57.4 | 1.11 | 1.09 | 1.93 | 3.02 | 4.13 | 2 | 9 |
| 8 | overwrite | 495,936 [384,368-501,813] | 57.5 | 1.11 | 1.09 | 1.72 | 2.81 | 3.92 | 2 | 13 |

Before and after the change of the compaction size from entry counts to bytes (same session):

| trigger | workload | LSM before [min-max] | LSM after [min-max] | change | compactions before | after | tables before | after |
|---|---|---|---|---|---|---|---|---|
| 2 | fill-seq | 3.86 [3.81-4.61] | 4.68 [3.76-4.80] | +0.82 | 18 | 15 | 1 | 1 |
| 2 | fill-random | 4.70 [4.08-4.97] | 4.56 [3.98-4.75] | -0.14 | 19 | 16 | 1 | 1 |
| 2 | overwrite | 3.64 [3.57-3.93] | 3.63 [3.58-3.68] | -0.01 | 11 | 11 | 1 | 1 |
| 4 | fill-seq | 3.04 [2.92-3.72] | 2.99 [2.92-3.14] | -0.05 | 16 | 10 | 2 | 3 |
| 4 | fill-random | 3.47 [3.09-3.86] | 3.14 [3.09-4.05] | -0.33 | 17 | 12 | 2 | 3 |
| 4 | overwrite | 3.14 [2.92-3.48] | 3.24 [2.95-3.63] | +0.10 | 17 | 14 | 2 | 2 |
| 8 | fill-seq | 3.45 [3.45-3.45] | 3.02 [3.02-3.02] | -0.43 | 15 | 9 | 1 | 2 |
| 8 | fill-random | 3.09 [3.09-3.09] | 3.02 [3.02-3.02] | -0.07 | 10 | 9 | 2 | 2 |
| 8 | overwrite | 2.80 [2.79-2.85] | 2.81 [2.79-2.81] | +0.01 | 13 | 13 | 2 | 2 |

Reading them:

- **With compaction off** (trigger 0) every byte is written once to the WAL (1.11x: 13 bytes of header on
  every 116-byte pair) and once to a table (1.09x: the table adds a 9-byte record header, the Bloom filter
  and the index), and the overwrite workload leaves 136 tables because overwrites inside a 1 MiB memtable
  barely collapse. The two builds are identical here (the policy is not involved): 45, 45 and 136 tables,
  the same amplification.
- **Switching compaction on** adds about 1.7-3.6 further writes of the data to tables, depending on the
  trigger and on the workload: a lower trigger merges more often and writes more (trigger 2: LSM 3.6-4.7
  against 2.8-3.0 at trigger 8), at the price of far fewer tables to read. The tiered policy also merges
  tables whose key ranges do not overlap (an ascending fill still costs 3.0 at trigger 4), which is a
  property of the policy, not of this benchmark.
- **Bytes against entry counts** did not change the amplification across the board. Overwrites are
  unchanged (within 0.1). The fill workloads at triggers 4 and 8 compact fewer times (16 to 10 and 17 to 12
  at trigger 4; 15 to 9 and 10 to 9 at trigger 8) and write a little less (LSM -0.05 to -0.43), and at
  trigger 2 the ascending fill writes more (3.86 to 4.68: higher in each of the three rounds, though the
  minimum-maximum ranges over all runs overlap), while the random fill is within the spread. Why a few tables are weighed differently by bytes was not investigated.
  For uniform 116-byte records the two measures are close, so the benchmark is a poor discriminator
  between them; `LsmTest.CompactionWeighsTablesByBytes*` (in `tests/storage/kv_engine_lsm_test.cpp`) are the tests that
  pin the difference down.
- **Throughput** is not a result of this change: the write path is not touched by it, and the round
  medians of one build on one workload differ by up to a factor of three (for example 184, 571 and 567
  thousand Puts/s), so no difference between the builds can be claimed. The benchmark does not record stalls, so whether the writer was held up by flushing
  and compaction, which run on other threads, is not known; with `--sync` the disk would dominate instead.

Read results (hits, misses, hot keys, scans, with and without the block cache) are in
[`storage-design.md`](storage-design.md) section 6.3.
