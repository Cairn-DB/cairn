# Verification strategy (Phase 0 proposal)

Status: proposed, 2026-09-22. Harness before code (CLAUDE.md rule 2).

## 1. Layers

| Layer | Tool | What it covers | From phase |
|---|---|---|---|
| Unit and property tests | `proptest` | codecs, formats, kernels vs oracles, crash-recovery prefixes | 1 |
| Fuzzing | `cargo-fuzz` (`fuzz/`) | every parser: segment footer/sections, log records, manifest, frames, protobuf messages, query AST | 1 |
| Concurrency | `loom` | SPSC queues and any atomic in `cairn-runtime` | 1 |
| Undefined behaviour | Miri (nightly) | `unsafe` in `cairn-storage` (byte views, alignment), scalar kernels and dispatch in `cairn-index` | 1 |
| Deterministic simulation | `cairn-sim` | single node (crash/recovery) in Phase 1, 3-node Raft cluster in Phase 3, checkers below | 1 (single node), 3 (cluster) |
| Real-cluster tests | same workload + checkers over `cairn-client` | Phase 4 exit: the real cluster passes the simulator's suite | 4 |
| Benchmarks | `criterion`, `cairn-bench`, `hdrhistogram` | kernels, storage, recall/latency curves; results in `bench-results/` | 1 |

## 2. Deterministic simulator

- **Single thread, one seed.** A run is `(seed, scenario)`. The seed drives a ChaCha RNG that is
  forked per node, per shard and per fault injector (ADR 0011). The scenario fixes the cluster
  shape, the workload mix and the fault schedule.
- **Discrete-event time.** The simulated `Clock` advances to the next due timer only when every
  task on every simulated core is idle. Wall time never enters.
- **Same executor as production.** The simulator supplies the reactor (ADR 0002); the scheduler
  is the production one, so interleavings are the production interleavings under a chosen order.
  The run queue is polled in an order derived from the seed so different seeds explore different
  schedules.
- **Simulated disk**: per-op latency drawn from the seed; unsynced writes are lost on crash; a
  crash can tear the last page; reads can return bit flips or short reads with configured
  probability; `ENOSPC` and `EIO` are injectable; rename atomicity is honoured (that is what we
  rely on) but the write of the new file before rename can be lost if not fsynced.
- **Simulated network**: per-link delay distribution, drop, duplicate, reorder, bounded
  buffers with backpressure, symmetric and asymmetric partitions, partial partitions (A sees B,
  B does not see A), slow links.
- **Process faults**: crash a node (all in-memory state gone, disk kept), restart it, pause it
  (clock keeps moving, it does not), skew its clock.
- **Trace and replay**: every event is appended to an in-memory trace; the trace hash is the
  run's fingerprint. Each seed is run twice; hashes must match or the run fails with
  "nondeterminism" (this catches leaks such as a stray `HashMap` iteration).
- **Reproducibility**: a failure prints `seed`, `scenario`, git hash, and the last N events.
  `cargo test -p cairn-sim -- --seed X` replays it.

## 3. Workload generator (shared with `cairn-bench-gen`)

Clients issue, with seeded randomness: upserts (new and repeated ids), deletes (takedowns) of
existing ids, point reads by id, filtered hybrid queries, each with a consistency level chosen from
a configured mix, and each carrying the latest token it has seen. Every invocation and response is
recorded in a history with simulated-time stamps.

## 4. Checkers

1. **Per-key linearizability.** The state per document id is a register with `upsert(v)`,
   `delete`, `get`. Because every operation touches exactly one key, linearizability decomposes
   per key (locality), so the checker is a simple per-key search over the history instead of a
   full Jepsen-style search: cheap enough to run on every seed. Only `Linearizable` reads are
   checked for real-time order; `ReadYourWrites(token)` reads are checked for token order;
   `BoundedStale` reads are checked for the lag bound.
2. **Read-your-takedown (signature test).** Expressed as a history predicate:

   ```
   for every delete D of id x acknowledged with token t at time a:
     for every query Q with level Linearizable invoked after a,
       or with level ReadYourWrites(t') where t' >= t on x's shard:
         x is not in Q's results
   ```

   Runs under every fault schedule. This is the property SPEC section 7 calls the signature test.
3. **Index equivalence.** After any segment is published or compacted, for a sample of filtered
   queries, the exact scan and the indexed path must agree on the candidate *set* below the
   threshold, and recall@k of the indexed path above the threshold must stay above a floor.
   Also: the memtable, the log-replayed memtable and the built segment must return identical
   point reads.
4. **Durability.** After a crash and restart, the recovered state of a shard equals the state
   obtained by applying the committed log prefix; no acknowledged write is missing; unacknowledged
   writes are either fully present or fully absent.
5. **Raft safety** (Phase 3): election safety, log matching, leader completeness, state-machine
   safety, checked from the trace; plus the differential comparison against `raft-rs` (ADR 0008).
6. **Replica convergence.** Once partitions heal and faults stop, every replica of a shard
   reaches the same manifest, the same segment hashes and the same deletion bitmaps.

## 5. Property tests and fuzzing per crate

- `cairn-storage`: log record roundtrip; "write a log, crash at any byte offset, recover" yields a
  committed prefix; segment container roundtrip; manifest replace under crash; compaction
  preserves the multiset of live documents. Fuzz: footer, section table, log record, manifest.
- `cairn-index`: kernels vs scalar oracle (ULP tolerance); HNSW build is deterministic (same input
  twice, same bytes); bitmap operations vs a naive set; BM25 scores vs a naive implementation on
  tiny corpora. Fuzz: postings and dictionary readers.
- `cairn-query`: planner produces a plan for every valid query shape; fusion is permutation
  invariant where it should be; k' >= k. Fuzz: query AST decoder.
- `cairn-raft`: state machine step never panics on any message sequence (fuzz on message
  streams); safety properties on short random schedules (proptest) before the full simulator.
- `cairn-runtime`: `loom` on the SPSC queue and the wake path; a smoke test that the io_uring
  reactor and the blocking reactor pass the same `Disk` contract test.

## 6. Miri

Run nightly on `cairn-storage` and `cairn-index` (`cargo +nightly miri test`). Miri does not
execute most vendor SIMD intrinsics (extent unverified), so the intrinsic bodies are excluded via
`cfg(miri)` and covered by the oracle tests on real hardware instead. The `miri` component is not
installed on this machine's nightly yet (checked 2026-09-22); install it before Phase 1's first
`unsafe`.

## 7. Campaign sizing (Phase 3 exit)

"Millions of seeded runs" means: a run is one seed × one scenario × about 10,000 operations. The
nightly CI campaign runs at least 1,000 seeds × 10 scenarios; the Phase 3 exit campaign runs
100 scenarios × 20,000 seeds (2M runs) on one machine with zero violations, results committed with
seeds and git hash.
