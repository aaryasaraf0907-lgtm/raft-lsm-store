# Running a RaftKV cluster

Everything below runs on one machine (Linux or macOS; on Windows use WSL2 Ubuntu).

## Build

The node and the CLI need the gRPC layer:

```
cmake -S . -B build-net -DCMAKE_BUILD_TYPE=Release -DRAFTKV_BUILD_NETWORK=ON
cmake --build build-net -j8
```

This produces `build-net/bin/raftkv_node` (one cluster node) and `build-net/bin/raftkv_cli` (client).
On macOS, `brew install grpc protobuf cmake` first. On Ubuntu, `sudo apt install libgrpc++-dev
libprotobuf-dev protobuf-compiler-grpc`.

## Start a cluster

```
scripts/run_cluster.sh start 3
scripts/run_cluster.sh status
```

`status` shows each node's role, term and log positions, for example:

```
node 1  leader    term 1  leader 1  commit 5  applied 5  log 5  snapshot 0
node 2  follower  term 1  leader 1  commit 5  applied 5  log 5  snapshot 0
node 3  follower  term 1  leader 1  commit 5  applied 5  log 5  snapshot 0
```

Node `i` listens for other nodes on port `7000 + i` and for clients on `7100 + i`. Data and logs are
under `/tmp/raftkv-cluster` (`RAFTKV_DIR` changes it). `RAFTKV_NODE_FLAGS=--no-sync` makes a fast demo
that is **not** crash safe; leave it off to see real durability.

## Use it

```
scripts/run_cluster.sh cli put hello world
scripts/run_cluster.sh cli get hello
scripts/run_cluster.sh cli delete hello
scripts/run_cluster.sh cli --hex put 00ff10 deadbeef
scripts/run_cluster.sh cli repl
```

`get` is linearizable: the leader confirms it is still the leader with a majority (ReadIndex) before
answering, and a read writes nothing to the log. The CLI finds the leader by itself and follows
redirects. Exit status: 0 success, 1 failure, 2 usage error, 3 key not found.

Limits: keys up to 4096 bytes (not empty), values up to 1 MiB. Larger requests are rejected before
anything is replicated.

## Break it on purpose

```
scripts/run_cluster.sh leader
scripts/run_cluster.sh kill 1
scripts/run_cluster.sh cli put still-works yes
scripts/run_cluster.sh status
scripts/run_cluster.sh restart 1
scripts/run_cluster.sh status
```

`kill` is a SIGKILL: the node gets no chance to clean up. A client write issued during the election is
retried automatically with the same request number, so it takes effect exactly once. After `restart`,
the node rebuilds its state from its own disk and catches up from the leader.

With 3 nodes the cluster keeps working while any 2 are up. Kill 2 and writes (and reads) wait until a
majority is back; they never return stale data.

## Recover a node whose storage engine will not open

A node whose engine finds damaged data refuses to start rather than serve it. It exits with
`raftkv_node: cannot open the storage engine in <data-dir>/kv (status 3)`; status 3 is
`kCorruption`. Each node's data directory holds two parts: `raft/` (its term, vote, Raft log and
latest snapshot) and `kv/` (the storage engine). The engine's contents can be rebuilt from `raft/`,
so for that one node:

1. Make sure the node is stopped, and that the other nodes are up with a leader
   (`scripts/run_cluster.sh status`).
2. Move that node's `kv/` aside, for example
   `mv /tmp/raftkv-cluster/node2/kv /tmp/raftkv-cluster/node2/kv.damaged`
   (with `run_cluster.sh` the data directory is `$RAFTKV_DIR/node<ID>`, by default
   `/tmp/raftkv-cluster/node<ID>`). Leave `raft/` where it is.
3. Restart the node (`scripts/run_cluster.sh restart 2`). It starts with an empty engine, restores
   its stored snapshot if one exists, applies subsequent committed Raft log entries, and catches up
   from the leader. If the node has fallen too far behind for the retained log entries, the leader
   can send it a snapshot.
4. Check with `status` that the node's `applied` reaches the others' before deleting `kv.damaged`.

This is what `NodeClusterTest.ANodeWhoseEngineDirectoryIsGoneRebuildsFromItsSnapshotAndLog` does.
Be careful:

- Moving `kv/` away removes that node's local engine state. The node gets its data back only from
  the Raft log entries and snapshot that are still available, in its own `raft/` or from the leader.
  If they are not (for example `raft/` was also lost or damaged, or a `--no-sync` node lost writes in
  a power failure and no other node has them), recovery is not guaranteed. Keep `kv.damaged` until
  the node has caught up.
- Do this for the affected node's `kv/` only. Never move or delete its `raft/`, another node's
  directory, or a whole data directory: a node that returns without its Raft state is the unsupported
  case described under Known limitations.

## Measure

```
scripts/run_cluster.sh cli bench put 1000 100 1
scripts/run_cluster.sh cli bench put 1000 100 4
scripts/run_cluster.sh cli bench get 1000
```

Arguments: operation, number of operations, value size in bytes, number of client threads. It prints
throughput and p50/p95/p99 latency. Numbers depend heavily on fsync: compare with and without
`RAFTKV_NODE_FLAGS=--no-sync`.

## Flags of `raftkv_node`

| Flag | Meaning |
|---|---|
| `--id N` | this node's id (>= 1) |
| `--peers 1=H:P,2=H:P,...` | Raft address of every node, this one included |
| `--client-addr H:P` | where clients connect |
| `--data-dir DIR` | Raft log and storage engine files |
| `--no-sync` | no fsync (testing only) |
| `--memtable-bytes N`, `--compaction-trigger N` | flush size of a memtable (default 4 MiB) and how many SSTables trigger a merge (default 4; 0 = never) |
| `--snapshot-threshold N`, `--snapshot-trailing N` | snapshot the data and drop the Raft log every N applied entries (default 10000; 0 = never) and how many log entries to keep below it (default 100) |
| `--sync-engine-wal` | also fsync the engine's own WAL on every write. By default it is not: the Raft log is durable and the engine is rebuilt from it after a crash |
| `--no-sstables` | use the old WAL + memtable engine instead of the LSM engine (the default) |
| `--no-prevote` | turn PreVote off |
| `--heartbeat-ms N`, `--election-ms MIN,MAX` | timing (defaults 50 and 150,300) |

## Known limitations

- **Snapshots need the LSM engine.** With `--no-sstables` the node cannot snapshot, so its Raft log
  grows without bound. With the default engine, every `--snapshot-threshold` applied entries the node
  snapshots its data (a merged copy of every live key plus the client-session table) and drops the
  Raft log. A node that has no data, or is far behind, is sent the leader's snapshot in chunks. A
  snapshot is built in memory, so its size is bounded by the node's RAM.
- The cluster membership is fixed when the nodes start. In particular a node must keep its disk: one
  that is wiped and returns under the same id breaks Raft's assumption that persisted state survives,
  and a leader that believes the node had caught up cannot bring it back. Replacing a machine needs a
  membership change (remove the old id, add a new one), which is not implemented.
- Communication is unencrypted and unauthenticated: run it on a trusted network only.
