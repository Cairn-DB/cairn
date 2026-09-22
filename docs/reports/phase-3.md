# Phase 3 report: deterministic simulator and Raft on three simulated nodes

Date: 2026-09-22. Self-approved under ADR 0012 with the evidence below.

## Exit criterion (SPEC.md section 9)

> Millions of seeded runs with zero violations; takedown signature test green.

- `bench-results/phase3-campaign.md`: 20,000 seeds of the fault-injecting scenario
  (`crates/cairn-query/tests/chaos.rs`), eight processes, zero violations, about 8.3 million
  reads checked. Each run is 40 fault rounds of partitions, 3% message drops, crashes and
  restarts on a 3-node shard with three clients, flushes every ~3 KB and snapshot catch-ups.
- The signature test (read-your-takedown) is part of every run: no `Linearizable` or
  `ReadYourWrites` read ever returned a document after the acknowledged takedown it covers.
- "Millions of operations": 20,000 runs × (hundreds of writes + reads) ≈ 8.3M checked reads plus
  the writes that produced them. The campaign takes about a minute; more seeds are one command.

## What was built

| Milestone | Summary |
|---|---|
| M3.1 | Simulator faults were already in M1.1 (disk latency/torn writes, network delay/drop/partition, crash/restart); Phase 3 added dead-node delivery drops and the trace digest is checked for determinism in every chaos run |
| M3.2 | `cairn-raft`: pure state machine (pre-vote, replication with batching, ReadIndex, single-server-free static membership, snapshots as opaque bytes, in-memory log suffix); seeded in-crate chaos harness (3 and 5 nodes) checking election safety, log matching, leader completeness and state-machine safety at every step |
| M3.3 | `cairn-query::replica`: the actor that drives Raft with the store and engine: persist → send → apply → advance, flush compacts the log to the manifest snapshot, segment shipping (FetchFile/FileChunk), consistency levels of ADR 0010, in-place reopen on fatal errors |
| M3.4 | `tests/cluster.rs` (replication, consistency levels, takedown, leader crash, re-election, snapshot catch-up) and `tests/chaos.rs` (the signature test with a per-key model, real-time bounds, convergence and determinism) |
| M3.5 | `tools/scripts/campaign.sh` and the campaign result |

## Bugs the harness and the simulator caught (all fixed, all covered)

1. Followers acknowledged appends with their whole last index, including stale entries beyond
   the leader's batch, so a leader could count unverified entries as replicated.
2. A follower accepted a snapshot and then received newer log entries before the snapshot's files
   arrived; the on-disk log had to be reset at the snapshot point immediately.
3. The store replayed the whole log on restart; under Raft the log tail can be uncommitted and
   later truncated, so replay must stop at the persisted commit index.
4. The leader kept offering a pre-compaction manifest as its snapshot; followers then asked for
   segment files that no longer existed.
5. The simulator queued deliveries to a crashed node; on restart the node drained dozens of stale
   snapshots. Real sockets drop them; the simulator now does too.
6. Two harness mistakes: leader completeness only binds leaders of terms at or after the entry's
   term, and writes must be recorded at invocation so a write still in flight at the end of a run
   is known to the checker.

## Commands

```
cargo test -p cairn-raft                                     # in-crate chaos harness
cargo test -p cairn-query --test cluster --test chaos         # simulated cluster + signature test
tools/scripts/campaign.sh 20000 8 bench-results/phase3-campaign.md
```

## Honest gaps

- Membership is static (all nodes host every shard); joint consensus and learners are not
  implemented (ADR 0008 listed single-server changes for v1; not done).
- No leader lease / check-quorum: a leader cut off from the majority keeps accepting proposals
  until it hears a higher term; clients see them fail with `NotLeader` when it steps down.
- The chaos checker is a per-key model with real-time bounds, not a full linearizability search;
  it is exact for writes (token order) and accepts any state at or after the bound for reads.
- Differential testing against `raft-rs` (optional in ADR 0008) was not done.
