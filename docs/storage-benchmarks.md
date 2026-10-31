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
  design); it measures what the policy does.

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

Measured 2026-10-30 on an Intel Core i5-13450HX (16 hardware threads), 7.6 GiB visible to WSL2, Ubuntu
26.04.1 under WSL2 (kernel 6.18.54.1), g++ 15.2.0, CMake `Release` (`-O3 -DNDEBUG`), data directory under
`/tmp` (the WSL2 virtual disk). Settings: 200,000 keys, 16 + 100 bytes, memtable 1 MiB, WAL not fsynced,
no block cache, 2x overwrites, 5 runs after a warm-up. Throughput is the median [min-max] of the 5 runs in
Puts per second; the amplification columns are medians over the runs; `tables` and `compactions` are the
number of live SSTables at the end and of compactions in the phase.

| trigger | workload | Puts/s median [min-max] | logical MB/s | WAL | flush | compaction | LSM | total | tables | compactions |
|---|---|---|---|---|---|---|---|---|---|---|
| 0 | fill-seq | 947,800 [941,584-969,854] | 109.9 | 1.11 | 1.09 | 0.00 | 1.09 | 2.20 | 45 | 0 |
| 0 | fill-random | 848,088 [822,097-859,616] | 98.4 | 1.11 | 1.09 | 0.00 | 1.09 | 2.20 | 45 | 0 |
| 0 | overwrite | 820,714 [813,243-822,032] | 95.2 | 1.11 | 1.09 | 0.00 | 1.09 | 2.21 | 136 | 0 |
| 2 | fill-seq | 945,200 [881,816-957,592] | 109.6 | 1.11 | 1.09 | 3.16 | 4.25 | 5.36 | 1 | 17 |
| 2 | fill-random | 814,302 [812,476-836,955] | 94.5 | 1.11 | 1.09 | 3.38 | 4.46 | 5.58 | 1 | 18 |
| 2 | overwrite | 831,980 [808,797-850,037] | 96.5 | 1.11 | 1.09 | 2.51 | 3.60 | 4.72 | 1 | 10 |
| 4 | fill-seq | 936,318 [914,682-946,479] | 108.6 | 1.11 | 1.09 | 1.93 | 3.02 | 4.13 | 3 | 15 |
| 4 | fill-random | 821,775 [802,895-836,804] | 95.3 | 1.11 | 1.09 | 2.63 | 3.72 | 4.83 | 2 | 19 |
| 4 | overwrite | 823,502 [816,247-838,587] | 95.5 | 1.11 | 1.09 | 1.93 | 3.02 | 4.14 | 2 | 18 |
| 8 | fill-seq | 943,333 [929,258-945,968] | 109.4 | 1.11 | 1.09 | 2.36 | 3.45 | 4.56 | 1 | 15 |
| 8 | fill-random | 846,289 [840,897-847,255] | 98.2 | 1.11 | 1.09 | 2.00 | 3.09 | 4.20 | 2 | 10 |
| 8 | overwrite | 840,616 [833,597-851,537] | 97.5 | 1.11 | 1.09 | 1.71 | 2.80 | 3.91 | 2 | 13 |

Run-to-run spread of the LSM amplification (min-max) at trigger 4: fill-seq 2.92-3.84, fill-random 3.43-3.91, overwrite 2.92-3.35.

Reading them: with compaction off every byte is written once to the WAL (1.11x: 13 bytes of header on every 116-byte pair) and once to a table (1.09x: the table adds a 9-byte record header, the Bloom filter and the
index), and the overwrite workload leaves 136 tables because overwrites inside a 1 MiB memtable barely
collapse. Switching compaction on adds about 1.7-3.4 further writes of the data to tables, depending on the trigger
and on the workload: a lower trigger merges more often and writes more (trigger 2: LSM 3.6-4.5 against 2.8-3.5
at trigger 8), at the price of far fewer tables to read. The tiered policy also merges tables whose key
ranges do not overlap (an ascending fill still costs 3.0 at trigger 4), which is a property of the policy,
not of this benchmark. Write throughput is almost the same in every configuration (medians between 0.81 and 0.95 million
Puts/s), which suggests that the writer was not held up by flushing and compaction, which run on other
threads (the benchmark does not record stalls, so this is an inference); with `--sync` the disk would
dominate instead.

Read results (hits, misses, hot keys, scans, with and without the block cache) are in
[`storage-design.md`](storage-design.md) section 6.3.
