# Fault injection for the Raft layer

Raft is only interesting when things go wrong, and most bugs hide in rare interleavings that a
hand-written test never produces. This harness makes the interleavings happen: a seeded random
"nemesis" keeps breaking the network and the nodes while clients write, and checkers verify the
properties Raft promises no matter what.

Everything runs in one process on `FakeNetwork`, with no sockets: fast, and every random choice
comes from a seeded generator.

## What can be injected

`FakeNetwork` (`src/raft/fake_transport.h`) has these knobs. All are off by default, and with
everything off it behaves exactly like a perfect, ordered network.

| Knob | Effect |
|---|---|
| `SetFaults({drop_request, ...})` | probability that a request never arrives |
| `drop_reply` | the handler runs, but the reply is lost |
| `duplicate` | the request is delivered twice; the handler runs twice, the sender gets one reply |
| `min_delay_ms`, `max_delay_ms` | each message is delayed by a random time; different delays make messages overtake each other, so this also **reorders** |
| `Disconnect(id)` / `Reconnect(id)` | isolate a node completely |
| `BlockLink(a, b)` | one-way failure: `a -> b` is lost while `b -> a` works (replies travel the other way, so this also kills replies in one direction) |
| `HealAll()` | repair everything |
| `SetFaultSeed(seed)` | seed for all random decisions |

Nodes can also be crashed and rebooted from their disk (`Cluster::Restart`), which loses all memory
and keeps only what `FileRaftStorage` saved.

## The nemesis

`Nemesis` (`tests/raft/fault_injection.h`) wakes every 10 to 60 ms and applies a random action: new
random fault rates, isolate a node, reconnect, cut or restore a one-way link, crash a node (never
more than a minority at once), or reboot a crashed node. Its schedule is recorded and printed when a
check fails.

## What is checked

| Property | Checker |
|---|---|
| **Election safety**: at most one leader per term | `Monitor` samples every node's (term, role) every 2 ms into `ElectionSafetyChecker` |
| **State machine safety**: no two nodes apply different entries at the same position, and positions only increase | `CheckPrefixAgreement` over every node's applied sequence |
| **Durability**: every acknowledged write is applied on every node | `CheckOutcome`, after the nemesis has healed everything |
| **Progress after healing**: the cluster converges to one state | `CheckOutcome` waits up to 30 s |

The checkers are themselves tested: a deliberately wrong history must be rejected.

## Running it

```
ctest --test-dir build --output-on-failure -R "RaftFaultInjection|FakeNetworkFaults|FaultInjectionCheckers"
RAFTKV_FI_VERBOSE=1 build/tests/raft_tests --gtest_filter='RaftFaultInjection.*'
```

With `RAFTKV_FI_VERBOSE=1` every run prints how much it injected, for example:

```
[fault-injection] seed 2: 2471 writes acknowledged, 4454 messages lost, 1451 duplicated, 46 nemesis actions
```

Three scenarios run, three seeds each (2 seconds of continuous writing per run):

1. a lossy, duplicating, reordering network (3 nodes);
2. partitions and one-way link cuts (5 nodes);
3. crashes and reboots from disk on top of a lossy network (3 nodes, persistent).

To reproduce or explore, change the seeds in `fault_injection_test.cpp`. A failing run prints the
nemesis schedule; the same seed produces the same random choices (the interleaving of threads can still
differ, so a failure may need a few attempts to recur).

## Does the harness actually catch bugs?

A test that cannot fail proves nothing, so each safety rule was broken on purpose and the scenarios
were run. See the pull request for the results of those mutation runs.

## Adding a failure

Add a field to `FaultPlan` (or a method on `FakeNetwork`), apply it in `FakeNetwork::SendRpc`, give it
an action in `Nemesis::Act`, and add a unit test next to the existing ones in
`fault_injection_test.cpp`.
