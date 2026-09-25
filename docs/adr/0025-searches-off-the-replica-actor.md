# ADR 0025: Shard searches run off the replica actor

- Status: accepted (delegated 2026-09-26; the owner asked for work on the filtered-query
  latency, which missed its target at 50M)
- Date: 2026-09-26

## Context

On the GCP 50M run (`bench-results/phase4-gcp-bigann50m-2026-09-25.md`), filtered
linearizable queries had a p99 of 118 ms against a 100 ms target. The earlier report blamed
the scan threshold, and suggested sending large segments to the filtered graph. The
evidence does not support that:

- **The SIFT1M sweep** (`bench-results/phase2-sift1m.md`): at 1% selectivity, the graph is
  worse than the scan. Capped at 8,192 visits it reaches recall 0.45 to 0.94; uncapped it
  takes 8 to 22 ms. The scan reaches recall 0.99 in 0.23 ms per 10k rows. At 50M, 1% of a
  6.25M-row shard is about 62k rows, a few milliseconds of CPU.
- **A local reproduction** with the same per-shard shape as GCP (one node, 2 shards of
  6.25M rows, 9 segments each, idle), using the filtered linearizable query:

  | client threads | p50 | p99 | throughput |
  |---|---|---|---|
  | 1 | 3.2 ms | 4.3 ms | 302 QPS |
  | 8 | 24 ms | 34-40 ms | 323-326 QPS |

  Eight clients get no more throughput than one, while the machine has 16 hardware
  threads. Latency grew with queueing, not with the work per query.

The cause is in the replica: `execute` ran the search inside the shard's actor. All
searches of a shard ran one after another on a single core, and the shard's Raft work
(heartbeats, appends, commits) waited behind them.

## Options considered

1. **Tune the scan threshold or the graph.** Rejected by the numbers above.
2. **Split each search over its segments** (the `Parallel` trait). This shortens one query,
   but the actor still serializes queries, and it still blocks Raft.
3. **Snapshot on the actor, search elsewhere.** The actor captures what the search needs,
   and the search runs on a helper thread, so searches of a shard overlap. Chosen.

## Decision

- `ShardEngine::prepare_legs` runs on the actor, at the moment the read is released:
  after ReadIndex for linearizable reads, and once the token's index is applied for
  read-your-writes. It captures:
  - the segments' loaded indexes, now shared (`Arc`) and immutable;
  - per segment, the rows allowed at that moment (the filter minus deletions);
  - the memtable's index snapshot.

  Takedowns applied by then are therefore excluded, exactly as before. A result reflects
  the shard at a point between the request and the response, which linearizability allows.
- `LegsJob::run` performs the vector and text searches with no access to the engine. The
  replica runs it through `Runtime::offload`, on a helper thread in production, and inline
  (deterministic) in the simulator.
- `VectorIndex` becomes `Send + Sync`: its search scratch buffers move from a `RefCell` to a
  small pool. Two are kept per index, and extra concurrent searches allocate their own; the
  OS maps zeroed pages lazily, so this is cheap.
- A filter-only query (no legs) and the single-node `Query` path still run inline. They are
  either cheap or not used by the cluster coordinator.

## Evidence

- Unit and simulation tests unchanged and green (118). The campaign (3,000 seeds, zero
  violations) checks read-your-takedown and the per-key model under crashes and
  partitions, with searches going through the new path.
- Alternating A/B, same data, each binary restarted on it and measured idle, 2 runs each,
  1,000 queries per kind (`bench-results/phase4-adr0025-offload.md`):

  | 8 clients | on the actor | off the actor |
  |---|---|---|
  | 1% filter, linearizable, p99 | 40.0 / 34.2 ms | **10.3 / 9.8 ms** |
  | throughput | 326 / 323 QPS | **1,154 / 1,199 QPS** |
  | unfiltered, linearizable, p99 | 33.0 / 30.8 ms | **9.7 / 10.0 ms** |
  | throughput | 412 / 422 QPS | **1,231 / 1,271 QPS** |

  Recall is identical. **One client is slower**: filtered p50 3.2 → 4.1 ms, unfiltered
  2.6 → 3.1 ms. That is the cost of the helper-thread hop: `offload` starts a thread per
  call, then wakes the executor.

## Consequences

- The throughput of a shard's searches is no longer capped at one core. Raft work on the
  shard is not held up by searches.
- **Not measured on GCP.** The 118 ms p99 there came from the same queueing, with 8 shards
  over 3 nodes. The effect is expected to be large, but it has to be measured.
- Concurrency was not bounded at first: each search started a thread. See the search pool
  below.
- Segments replaced by a merge stay in memory until the searches that captured them finish.
- Searches still compete for CPU with index builds. Under the saturated build load of the
  GCP run, latency rose to seconds (ADR 0019).

## Search pool (2026-09-26)

- `Runtime::offload_search`, for short work that must not queue behind builds, which may
  take minutes:
  - production: a process-wide pool with one permanent thread per hardware thread, fed by a
    queue;
  - simulator: inline, without the build delay;
  - default: `offload`.
- The replica runs searches through it. A runtime test runs 500 concurrent jobs; they all
  complete, on at most one thread per hardware thread.
- A/B/C, alternating, 2 runs each (`bench-results/phase4-adr0025-pool.md`), 1% filter,
  linearizable:

  | | 1 client, p50 | 8 clients, throughput |
  |---|---|---|
  | actor | 3.2-3.3 ms | 320-324 QPS |
  | thread per search | 4.1 ms | 1,216-1,224 QPS |
  | pool | 3.7-4.0 ms | 1,139-1,184 QPS |

- **The pool did not recover the single-client overhead**, contrary to what this ADR
  expected. It gains 0.1 to 0.4 ms of about 0.8 ms, so thread start-up was not the main
  cost; the cause is not identified. Suspects: the cross-thread completion hop that wakes the
  executor, and cache locality.
- At 8 clients the pool and a thread per search are equal within noise.
- The pool is kept for what it bounds: search threads never exceed the machine's hardware
  threads, whatever the number of clients.
