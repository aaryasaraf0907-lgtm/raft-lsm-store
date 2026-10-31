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
no block cache, 2x overwrites.

Compaction chose by **entry count** until it was changed to weigh tables by their **file size in bytes**
(`SSTable::file_bytes()`), so every comparison below is of the build before the change (`main` at the
time) with the build after it, built from the same compiler and flags and **run interleaved in one session**
(both builds back to back at each trigger and workload, the order alternating between rounds), because
sessions are not comparable with each other on this machine. One *invocation* is one run of the benchmark
program: a warm-up and 5 runs (3 runs in the second batch), reported as that invocation's median. The fill
workloads at triggers 2, 4 and 8 have 9 invocations per build (3 of 5 runs and 6 of 3 runs); the overwrite
workloads and trigger 0 have 3 of 5 runs, because overwrite was not repeated in the second batch. Every
cell is the median over invocations, and the ranges are the minimum and maximum over all runs.

Results with the byte-sized policy (the policy as it is now); `tables` and `compactions` are the number of
live SSTables at the end and of compactions in the phase:

| trigger | workload | invocations | Puts/s median [min-max] | logical MB/s | WAL | flush | compaction | LSM | total | tables | compactions |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | fill-seq | 3 | 841,115 [700,082-964,294] | 97.6 | 1.11 | 1.09 | 0.00 | 1.09 | 2.20 | 45 | 0 |
| 0 | fill-random | 3 | 808,607 [306,977-830,983] | 93.8 | 1.11 | 1.09 | 0.00 | 1.09 | 2.20 | 45 | 0 |
| 0 | overwrite | 3 | 802,116 [672,516-826,362] | 93.0 | 1.11 | 1.09 | 0.00 | 1.09 | 2.21 | 136 | 0 |
| 2 | fill-seq | 9 | 942,883 [124,827-967,208] | 109.4 | 1.11 | 1.09 | 3.55 | 4.63 | 5.74 | 1 | 15 |
| 2 | fill-random | 9 | 808,805 [206,355-853,833] | 93.8 | 1.11 | 1.09 | 3.45 | 4.54 | 5.65 | 1 | 15 |
| 2 | overwrite | 3 | 502,075 [365,662-724,022] | 58.2 | 1.11 | 1.09 | 2.54 | 3.63 | 4.74 | 1 | 11 |
| 4 | fill-seq | 9 | 927,966 [355,121-966,716] | 107.6 | 1.11 | 1.09 | 1.91 | 2.99 | 4.10 | 3 | 11 |
| 4 | fill-random | 9 | 814,385 [376,444-848,396] | 94.5 | 1.11 | 1.09 | 2.00 | 3.09 | 4.20 | 3 | 12 |
| 4 | overwrite | 3 | 481,006 [106,113-632,481] | 55.8 | 1.11 | 1.09 | 2.15 | 3.24 | 4.36 | 2 | 14 |
| 8 | fill-seq | 9 | 925,642 [360,386-958,754] | 107.4 | 1.11 | 1.09 | 1.93 | 3.02 | 4.13 | 2 | 9 |
| 8 | fill-random | 9 | 758,017 [356,379-854,550] | 87.9 | 1.11 | 1.09 | 1.93 | 3.02 | 4.13 | 2 | 9 |
| 8 | overwrite | 3 | 495,936 [384,368-501,813] | 57.5 | 1.11 | 1.09 | 1.72 | 2.81 | 3.92 | 2 | 13 |

Before and after the change, same session. "Pairs" compares every invocation of one build with every
invocation of the other (9 x 9 = 81 pairs for the fill workloads, 3 x 3 = 9 for overwrite) and counts how
often the build after the change had the higher and the lower LSM write amplification:

| trigger | workload | invocations | LSM before [min-max] | LSM after [min-max] | change | pairs after higher / lower | compactions before | after | tables before | after |
|---|---|---|---|---|---|---|---|---|---|---|
| 2 | fill-seq | 9 | 4.25 [3.81-4.66] | 4.63 [3.76-4.80] | +0.38 | 63 / 16 of 81 | 18 | 15 | 1 | 1 |
| 2 | fill-random | 9 | 4.68 [3.96-4.99] | 4.54 [3.96-4.78] | -0.14 | 28 / 51 of 81 | 20 | 15 | 1 | 1 |
| 2 | overwrite | 3 | 3.64 [3.57-3.93] | 3.63 [3.58-3.68] | -0.01 | 2 / 5 of 9 | 11 | 11 | 1 | 1 |
| 4 | fill-seq | 9 | 3.04 [2.92-3.93] | 2.99 [2.92-3.14] | -0.05 | 9 / 72 of 81 | 16 | 11 | 2 | 3 |
| 4 | fill-random | 9 | 3.52 [3.09-3.88] | 3.09 [2.90-4.05] | -0.43 | 0 / 81 of 81 | 16 | 12 | 2 | 3 |
| 4 | overwrite | 3 | 3.14 [2.92-3.48] | 3.24 [2.95-3.63] | +0.10 | 7 / 2 of 9 | 17 | 14 | 2 | 2 |
| 8 | fill-seq | 9 | 3.45 [3.28-3.45] | 3.02 [3.02-3.02] | -0.43 | 0 / 81 of 81 | 15 | 9 | 1 | 2 |
| 8 | fill-random | 9 | 3.09 [3.09-3.45] | 3.02 [3.02-3.02] | -0.07 | 0 / 81 of 81 | 10 | 9 | 2 | 2 |
| 8 | overwrite | 3 | 2.80 [2.79-2.85] | 2.81 [2.79-2.81] | +0.01 | 9 / 0 of 9 | 13 | 13 | 2 | 2 |

Reading them:

- **With compaction off** (trigger 0) every byte is written once to the WAL (1.11x: 13 bytes of header on
  every 116-byte pair) and once to a table (1.09x: the table adds a 9-byte record header, the Bloom filter
  and the index), and the overwrite workload leaves 136 tables because overwrites inside a 1 MiB memtable
  barely collapse. The policy is not involved here, and the two builds are identical: 45, 45 and 136
  tables, the same amplification.
- **Switching compaction on** adds about 1.7-3.6 further writes of the data to tables, depending on the
  trigger and on the workload: a lower trigger merges more often and writes more (trigger 2: LSM 3.6-4.6
  against 2.8-3.0 at trigger 8), at the price of far fewer tables to read. The tiered policy also merges
  tables whose key ranges do not overlap (an ascending fill still costs 3.0 at trigger 4), which is a
  property of the policy, not of this benchmark.
- **Bytes against entry counts changed the amplification in both directions.** Lower with the byte-sized
  policy, in every pair: the random fill at trigger 4 (3.52 to 3.09) and both fills at trigger 8 (3.45 to
  3.02 and 3.09 to 3.02). Higher: the **ascending fill at trigger 2, 4.25 to 4.63 (+9%)**, in 63 of 81
  pairs (and lower in 16); the overwrite workload at trigger 4 by 0.10. Within the noise: the other
  cells. The number of compactions went down in all fill configurations (for example 16 to 11 and 16 to 12
  at trigger 4, 15 to 9 at trigger 8), i.e. fewer but larger merges. The effect is therefore not a plain
  improvement, and at trigger 2 with an ascending fill it is a regression.
- **Throughput** is not a result of this change: the write path is not touched by it, and one build on one
  workload swings between invocations by more than a factor of two (the ascending fill at trigger 2:
  184 to 958 thousand Puts/s), so no difference between the builds can be claimed. The benchmark does not
  record stalls, so whether the writer was held up by flushing and compaction, which run on other threads,
  is not known; with `--sync` the disk would dominate instead.
- **Against the table of 2026-10-30**, which measured the entry-count policy only and which this replaces:
  today's "before" columns reproduce its LSM amplification within the run-to-run spread (trigger 2: 4.25,
  4.46, 3.60 then against 4.25, 4.68, 3.64 now; trigger 4: 3.02, 3.72, 3.02 against 3.04, 3.52, 3.14;
  trigger 8: identical, 3.45, 3.09, 2.80). The throughput reproduces for the fill workloads (medians of
  0.76-0.94 million Puts/s now, 0.81-0.95 then) and for trigger 0 (0.80-0.84 against 0.82-0.95), but not
  for the overwrite workload, which was measured only in the first batch (0.48-0.50 million against
  0.82-0.84 then); within the session the speed also varied between batches. The cause was not
  investigated.

### Why the two policies differ: a tie in entry counts is not a tie in bytes

To find out what changed, a scratch build (not part of the repository) logged, at every compaction
decision, the entry count and file size of every live table together with the run that each of the two
measures would have chosen from that same state. Four engines (a warm-up and 3 runs) per cell, 9 cells
(three workloads, three triggers) for each policy: 1,290 decisions, in which a disagreement is possible only
when three or more tables are live.

- **File sizes are proportional to entry counts, with a fixed cost per file.** The flushed tables hold 4,395
  entries and 559,805 bytes (127.37 bytes per entry). A table merged from two of them holds 8,790 entries
  and 1,119,462 bytes, which is **148 bytes less** than the 1,119,610 of its two inputs, from three 296
  less, from four 445 less, from five 593 less: about 148 bytes saved per extra input (100 of them the
  footer; the other 48 were not decomposed).
- **The two measures disagreed in 211 of the 1,290 decisions, and all 211 are exact ties in entry
  counts.** Entry counts of the tables a policy compares are multiples of the flush size, so "the newest
  run is no larger than the table before it" is often an equality, and `<=` resolves it in favour of the
  short run. In bytes the merged table is smaller than the sum of its inputs, so the same comparison fails
  by about 148 bytes, the short run is rejected, and the policy goes on to a longer one. Example, state
  `13185, 8790, 4395` entries (`1,679,119`, `1,119,462`, `559,805` bytes): by entries 8790 + 4395 = 13185 is
  not more than 13185, so the two newest tables are merged; by bytes 1,119,462 + 559,805 = 1,679,267
  exceeds 1,679,119, so all three are merged.
- **In every disagreement the byte-sized policy merges more tables, never fewer (211 of 211), and in all 71
  disagreements of its own runs it merged all the live tables.** Over the same four engines it rewrote
  between 0.75 and 1.13 times as many entries as the entry-count policy, depending on trigger and
  workload (trigger 2: fill-seq 1.13x, fill-random 0.94x, overwrite 0.94x; trigger 4: 1.03x, 0.75x, 1.00x;
  trigger 8: 0.82x, 0.96x, 0.99x), and made fewer compactions (561 against 729). These ratios have the same
  sign as the measured change in write amplification in seven of the nine cells; in the other two, trigger 4
  fill-seq (1.03x rewritten, measured -0.05) and trigger 8 overwrite (0.99x, measured +0.01), both are
  within noise. In the fill workloads
  34-41% of what the byte-sized policy rewrote came from tie decisions that the entry-count policy would
  have decided differently; in the overwrite workloads 8-11%, which is why they hardly change.
- **So it is not a defect of `file_bytes()` or of the selection rule,** which is unchanged: the size is the
  real size of the file. It is a consequence of weighing tables by a measure that is not exactly additive
  under merging, in a benchmark whose flushed tables are all the same size, which is what makes the entry
  counts tie. Whether the extra merging pays off depends on the trigger and on timing (it made merges fewer
  and larger), and the sign of the net effect could not be derived: an idealized replay of the policy on
  these sizes did not reproduce the measured directions, so the arrival times of flushes relative to
  compactions matter. How often exact ties occur in a workload with varying record sizes was not measured.
- A common remedy is a tolerance in the comparison (treat tables within some factor of each other as the
  same size, as size-tiered policies usually do). It would remove the sensitivity, but it is a new
  threshold and a different policy, so it was not made; `LsmTest.CompactionBreaksATieInEntryCounts...`
  documents the current behaviour.

Read results (hits, misses, hot keys, scans, with and without the block cache) are in
[`storage-design.md`](storage-design.md) section 6.3.
