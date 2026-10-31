# RaftKV

A replicated key-value store written from scratch in C++17: a **Raft** consensus layer on top of an
**LSM-tree** storage engine, served over **gRPC**. Every node keeps a full copy of the data; a write
is acknowledged once a majority has it on disk, and reads are linearizable.

Built as a data structures and algorithms course project at VIT Pune. It runs on macOS and Linux.

## What it does

**Consensus (Raft)**
- Leader election with **PreVote** (a partitioned node cannot disrupt the cluster when it returns),
  log replication with conflict backtracking, and the commit rule from the Raft paper (a leader only
  counts replicas for entries of its own term).
- **Linearizable reads** with ReadIndex: a read is answered only after the leader confirms it is
  still the leader, without writing anything to the log.
- **Exactly-once writes**: each client request carries a client id and a sequence number, so a retry
  after a timeout or a leader change is applied at most once.
- **Snapshots**: the log is compacted once it grows past a threshold, and a node that has fallen
  behind the compacted log is sent the leader's snapshot in chunks.
- **Group commit**: concurrent proposals share one log write and one fsync, with an adaptive batching
  delay so that batching survives slow thread wake-ups.
- **Membership changes**: nodes are added and removed one at a time while the cluster keeps serving
  (Raft thesis, chapter 4); a new node catches up as a non-voting learner before it counts toward any
  majority, and the configuration survives log compaction inside snapshots.
- **Range scans**: `raftkv_cli scan START END` returns the keys in a range in order. A scan is
  linearizable like `get` (it goes through ReadIndex) and is paged: at most 1,000 keys or about 1 MiB
  per reply, and the CLI continues where a reply stopped.

**Storage (LSM tree)**
- A concurrent **skip-list memtable** behind a checksummed **write-ahead log**.
- Immutable **SSTables** with per-block checksums and **bloom filters**, flushed in the background.
- A **tiered compaction policy** that merges small new tables instead of rewriting big old ones, and
  drops tombstones only when that is safe.
- An atomically replaced **manifest** recording the live tables. Every step of flush, compaction and
  snapshot install is crash-safe, including a manifest write that fails after its rename.
- The Raft log is the durable copy of recent writes, so the engine's own WAL is not fsynced: after a
  crash the engine is rebuilt from the Raft log or a snapshot.

## Architecture

```mermaid
flowchart TB
  C["raftkv_cli / KvClient<br/>(retries, exactly-once)"] -- gRPC --> S["ClientService<br/>(Put / Get / Delete / NodeStatus)"]
  subgraph N["raftkv_node (one per machine)"]
    S --> R["Raft node<br/>election, replication, ReadIndex,<br/>snapshots, group commit"]
    R -- "committed entries" --> M["KV state machine<br/>(client sessions)"]
    M --> E["KVEngine (LSM)<br/>WAL, memtables, SSTables,<br/>manifest, compaction"]
    R --- L["Raft log + snapshot<br/>(fsynced)"]
  end
  R <-- "AppendEntries, RequestVote,<br/>InstallSnapshot (gRPC)" --> P["the other nodes"]
```

The design documents go into detail:

| Document | Covers |
|---|---|
| [`docs/raft-design.md`](docs/raft-design.md) | the Raft layer: state, replication, ReadIndex, snapshots, PreVote, with diagrams |
| [`docs/storage-design.md`](docs/storage-design.md) | the storage engine: write path, flush, manifest, compaction, recovery, checkpoints, known limitations |
| [`docs/sstable-format.md`](docs/sstable-format.md) | the on-disk SSTable format and the compaction rules |
| [`docs/running-a-cluster.md`](docs/running-a-cluster.md) | the node and CLI, all flags, and operational limits |
| [`docs/fault-injection.md`](docs/fault-injection.md) | the fault-injection harness used to test Raft |
| [`docs/benchmark-results.md`](docs/benchmark-results.md) | the full benchmark matrix |
| [`docs/storage-benchmarks.md`](docs/storage-benchmarks.md) | the storage benchmarks (reads, fills, overwrites, write amplification): how to run them, methodology, limits, results |

## Building

Requirements: CMake 3.16 or newer and a C++17 compiler. The network layer (the node, the client and
the cluster tests) also needs gRPC and protobuf.

```
brew install cmake grpc protobuf
```
On Ubuntu or Debian:
```
sudo apt-get install cmake g++ libgrpc++-dev libprotobuf-dev protobuf-compiler-grpc
```

The core library and its tests, without gRPC:
```
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug -DCMAKE_GTEST_DISCOVER_TESTS_DISCOVERY_MODE=PRE_TEST
cmake --build build -j8
ctest --test-dir build --output-on-failure
```

Everything, including the node and the client:
```
cmake -S . -B build-net -DCMAKE_BUILD_TYPE=Debug -DRAFTKV_BUILD_NETWORK=ON -DCMAKE_GTEST_DISCOVER_TESTS_DISCOVERY_MODE=PRE_TEST
cmake --build build-net -j8
ctest --test-dir build-net --output-on-failure
```
Run the tests serially, as above: many Raft tests are timer based. GoogleTest is downloaded by CMake.

## Running a cluster

`scripts/run_cluster.sh` runs a cluster of local processes, one per node:
```
scripts/run_cluster.sh start 3
scripts/run_cluster.sh cli put hello world
scripts/run_cluster.sh cli get hello
scripts/run_cluster.sh status
```
`status` shows each node's role, term and log positions. To see fail-over, kill the leader and watch
a new one take over:
```
scripts/run_cluster.sh kill "$(scripts/run_cluster.sh leader)"
scripts/run_cluster.sh status
scripts/run_cluster.sh cli get hello
```
Then bring it back, and clean up when done:
```
scripts/run_cluster.sh restart 1
scripts/run_cluster.sh stop
scripts/run_cluster.sh clean
```
Grow or shrink a running cluster (each node's `status` line ends with its members):
```
scripts/run_cluster.sh start 3
scripts/run_cluster.sh add 4
scripts/run_cluster.sh remove 2
scripts/run_cluster.sh status
```
Every flag of `raftkv_node` and `raftkv_cli` is described in
[`docs/running-a-cluster.md`](docs/running-a-cluster.md).

## Performance

Measured with `scripts/bench_cluster.sh` on an Apple M1 Pro laptop: three nodes on the one machine,
800 operations per run, 100-byte values, a Debug build. Selected rows; the full matrix is in
[`docs/benchmark-results.md`](docs/benchmark-results.md).

| Configuration | Operation | Client threads | Ops/s | p50 latency |
|---|---|---|---|---|
| durable (fsync on) | put | 1 | 67 | 14.9 ms |
| durable (fsync on) | put | 32 | 707 | 46.7 ms |
| no fsync | put | 1 | 2,268 | 0.43 ms |
| no fsync | put | 32 | 10,234 | 2.48 ms |
| durable | get (linearizable) | 1 | 2,636 | 0.36 ms |
| durable | get (linearizable) | 4 | 5,156 | 0.63 ms |
| durable | get (linearizable) | 32 | 10,534 | 1.44 ms |

Single runs on a laptop vary by 10-20%; the comparisons below are between runs on this machine.

What the numbers show:
- **Durable writes are bounded by the disk.** With fsync off, writes scale from 2,268 to 10,234 ops/s
  as clients are added, so the consensus and storage code paths are not the limit. With fsync on, a
  write needs the leader's and a follower's `F_FULLFSYNC`.
- **The leader's fsync overlaps the followers'** (`docs/raft-design.md`, group commit): the leader now
  syncs while the followers do, instead of before replicating. Against the previous run, durable writes
  improved by 31-43% in throughput from 4 to 32 clients (32 writers: 526 to 707 ops/s, p50 67 to 47 ms).
  A single client improved by about 10%, within run-to-run variation.
- **Why a single client gains little here.** All three nodes share one SSD, and `F_FULLFSYNC` flushes
  the whole drive cache, so the nodes' flushes still queue behind each other at the drive. On separate
  machines they would run in parallel and a write would wait for about one flush instead of two; with
  simulated disks the test suite measures exactly that halving (`FsyncOverlap.AWriteWaitsForOneFsyncNotTwo`).
  It has not been measured on separate machines.
- **Group commit is what makes concurrency help**: concurrent proposals share one log write and
  fsync, so 32 durable writers get about ten times the throughput of one.
- **Reads scale with clients.** Reads used to peak at four clients and then fall (3,287 ops/s and
  8.5 ms p50 at 32 clients), because every read sent its own confirmation round. Concurrent reads now
  share rounds (`docs/raft-design.md`, "Reads: ReadIndex"): at 32 clients, 10,534 ops/s at 1.44 ms p50
  in this run (11,785 in the previous one), about 3.2-3.6x the old figure.

## Testing

More than 400 test cases (one of them disabled by default), run on every pull request by CI on Linux
and macOS:

- **Unit and integration tests** for every component, including real three-node clusters over gRPC
  that kill and restart leaders while clients write, and then check that no acknowledged write was
  lost or applied twice.
- **Fault injection** for Raft: a simulated network that drops, duplicates, delays and reorders
  messages and cuts links, while checkers verify election safety, log agreement and that committed
  entries survive ([`docs/fault-injection.md`](docs/fault-injection.md)).
- **Crash tests** for the storage engine: hooks stop the engine at every step of flush, compaction
  and snapshot install, and recovery must produce either the old state or the new one, never a mix.
- **A linearizability checker** (`src/checker/linearizability.h`, Wing & Gong search with
  memoization) that decides whether a recorded client history could have come from a single copy of
  the store. Writes whose outcome is unknown are handled as "may or may not have happened".
- **Sanitizers in CI**: AddressSanitizer with UndefinedBehaviorSanitizer over everything, and
  ThreadSanitizer over the concurrency-heavy core tests. Linux builds use `-Werror`.
- During development, core changes were also checked by **deliberately breaking** the rule being
  implemented and confirming that a test fails.

## Limitations

- **Membership changes one server at a time** (no joint consensus), and a member must keep its disk:
  to replace a machine, remove its id and add the new node (`run_cluster.sh remove`, then `add`).
- **Snapshots are built in memory**, so a snapshot must fit in RAM.
- **macOS and Linux only.** Windows users can use WSL2.
- **One known open issue**: a cluster-level linearizability test is disabled because it occasionally
  stalls inside a gRPC call for longer than the call's deadline; the cause has not been found yet.

The storage engine's own limitations are listed in section 12 of
[`docs/storage-design.md`](docs/storage-design.md).

## Repository layout

```
src/raft/       Raft: node, storage of the log, client sessions, the in-process test network
src/storage/    the LSM engine: WAL, skip list, memtable, SSTables, bloom filter, compaction, manifest
src/server/     the replicated state machine and the gRPC client service
src/network/    gRPC transport between nodes, protobuf definitions
src/node/       raftkv_node: one cluster member
src/cli/        raftkv_cli and the KvClient library
src/checker/    the linearizability checker
tests/          raft, storage, integration and network tests
scripts/        run_cluster.sh, bench_cluster.sh
docs/           design documents and benchmark results
```

## Team

| Member | Area |
|---|---|
| Aarya | Raft core, replication and snapshots, integration |
| Aansh | storage write path: WAL, memtable, engine flush |
| Aditya Y. | SSTables, bloom filter, compaction, manifest hardening, storage design document |
| Aditya K. | gRPC networking, client, node and cluster tooling |

## License

MIT. See [`LICENSE`](LICENSE).
