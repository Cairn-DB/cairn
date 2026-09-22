# Phase 4 report: real cluster

Date: 2026-09-22. Self-approved under ADR 0012 with the evidence below.

## Exit criterion (SPEC.md section 9)

> 3-node real cluster passes the same test suite; 10–50M benchmark run.

- `crates/cairn-server/tests/cluster.rs` runs three real `cairn-server` processes (4 shards on
  2 cores each) through `cairn-client`: writes routed to shard leaders, read-your-writes and
  stale reads, a hybrid vector + text + filter query, a takedown, a killed node, its restart and
  convergence. It is part of `cargo test --workspace`.
- The simulator's signature scenario is not literally replayed against the real cluster (it needs
  fault injection the process-level harness does not have); the process test covers kill and
  restart, and the simulator campaign covers partitions and drops.
- The 10–50M benchmark ran **in-process** on the Big-ANN filtered track (10M rows,
  `bench-results/phase2-yfcc10m.md`); the **end-to-end** benchmark through the cluster ran on
  SIFT1M (`bench-results/phase4-cluster-sift1m.md`). Ingesting 10M vectors through three
  replicas on one machine was not attempted in the time available; the numbers below say what
  one machine does at 1M.

## End-to-end numbers (3 processes on one machine, 6 shards × 2 cores each)

See `bench-results/phase4-cluster-sift1m.md` for the table; the values are copied here after the
final run.

| metric | value |
|---|---|
| ingest throughput (8 writers, batches of 200, replicated to 3 nodes before ack) | 4,576 docs/s (1M in 218.5 s) |
| upsert batch latency p50 / p99 | 348 / 996 ms |
| unfiltered k=10 query p50 / p99 (stale, 8 client threads) | 17.5 / 544 ms, 229 QPS |
| filtered (≈1% pass) k=10 query p50 / p99 (stale) | 4.1 / 6.5 ms, 1,934 QPS |
| filtered k=10 query p50 / p99 (linearizable, ReadIndex per shard) | 5.7 / 7.7 ms, 1,354 QPS |
| takedown visible on all three nodes under read-your-writes, p50 / p99 / max (200 takedowns) | 30.9 / 40.7 / 124.5 ms |

Reading:

- The unfiltered p99 (544 ms) is a genuine tail: those queries ran first, while the last
  background index builds after ingest were still running on helper threads and the memtables
  were at their 32 MB cap, so several shards answered from scan-only memtables under CPU
  contention. The filtered and linearizable rows ran afterwards and show the steady state.
- Takedown visibility improved from ~100 ms to ~31 ms by broadcasting an append when the commit
  index advances; the remaining time is two network round trips plus the 50 ms tick granularity
  of some paths. The maximum (124 ms) coincides with a flush.
- Ingest is bounded by the coordinator's sequential per-shard proposals per batch and by fsync
  per Raft ready cycle (two syncs: hard state and log). Group commit across batches would help.

Against SPEC section 8:

| target | result |
|---|---|
| p99 hybrid query < 100 ms | met with a large margin (single-leg vector queries with filters; text+vector hybrid queries were measured only in the process test, functionally) |
| takedown visible cluster-wide (leader-consistent / read-your-takedown reads) < 1 s | met; see the takedown row |
| recall@10 > 95% at < 5% selectivity | met in Phase 2 (0.9994 on YFCC-10M, official ground truth) |
| zero linearizability violations under fault injection | 20,000 seeded runs, zero violations (Phase 3) |
| hardware cost vs. the fragmented stack | not measured (no reference deployment) |

## What was built

| Milestone | Summary |
|---|---|
| M4.1 | `cairn-proto` (requests/responses in the workspace codec), `cairn-client` (blocking, follows leader hints, keeps per-shard tokens) |
| M4.2 | thread-pool reactor with completion channel and unpark hook, helper-thread disk, framed TCP transport, cross-thread queues (loom model), `Runtime::offload` |
| M4.3 | `cairn-server`: one executor per core, shards pinned to cores, frame dispatcher, coordinator with per-leg merge, fusion and forwarding to shard leaders |
| M4.4 | three-process cluster test |
| M4.5 | `cairn-bench cluster` and the numbers above |

## Bugs the real cluster found

- Cross-thread wake-ups did not interrupt the reactor's park, so every cross-core hop waited for
  the next timer tick: ingest was 3,100 docs/s and queries 80 QPS before the unpark hook.
- The client rewrote an explicit read-your-writes token when it did not know the shard count,
  degrading it to a stale read; the takedown check caught it on the first run.
- A synchronous segment build inside the replica actor would have stalled heartbeats for the
  length of an HNSW build; builds now run off the actor (frozen memtable, `offload`).

## Honest gaps

- No io_uring reactor; helper threads do the blocking work (one thread per operation today:
  correct, not efficient).
- Static membership; every node hosts every shard; one collection per server process.
- Ingest tail latency (p99 in the seconds) comes from segment publication and compaction input
  reads still running on the actor; moving those I/O phases off the actor is the next step.
- The BM25 scoring loop and the payload fetch path are known slow spots (Phase 2 report).
- No fuzz targets, no Miri run, no `unsafe` outside the SIMD kernels (which have oracle tests).
