# ADR 0019: Parallel, deterministic index builds

- Status: accepted (delegated 2026-09-24; step 3 of ADR 0016, requested by the owner)
- Date: 2026-09-24

## Context

After ADR 0016 the leader builds each flushed segment once, and followers fetch it. The build
still ran on one thread: HNSW inserted rows one at a time (about 5,000 rows/s on SIFT1M), and
so did Vamana (about 3,100 rows/s). A 64 MB flush or a multi-million-row compaction kept one core
busy for minutes while the others idled. Two constraints shape the answer:

- **Determinism.** Engine code starts no threads (ADR 0011), the simulator must replay runs
  exactly, and since ADR 0017 every replica builds the same bytes for a freeze, which lets a
  fallback build and a snapshot reuse agree with the leader's file.
- **Quality.** The parallel graph must search as well as the sequential one.

## Options considered

1. **Lock-based concurrent insertion** (hnswlib style): fast, but the graph depends on thread
   scheduling. Replicas and the simulator would diverge.
2. **Several segments built at once** (more build slots): no help for a single large flush or
   merge, and it multiplies memory.
3. **Batched insertion** (as in ParlayANN): a batch of rows searches the graph as it was at the
   batch start in parallel, then edges are applied in a fixed order. The result is a function
   of the input only.

## Decision

Option 3, for both HNSW and Vamana.

- A `Parallel` trait in `cairn-core` (`run(n, f(worker, i))`) is the only way engine code uses
  several cores. `cairn-runtime::ThreadParallel` implements it with scoped threads; the
  simulator and single-node paths use `Sequential`. Both produce the same bytes.
- **HNSW:** the first 1,024 rows are inserted one at a time. After that, batches hold at most an
  eighth of the rows inserted so far, up to 1,024. For each batch:
  1. every row searches the frozen graph and selects its neighbors in parallel, with the
     earlier rows of its own batch as extra candidates by exact distance;
  2. forward links are written in row order;
  3. each existing node merges its new back links once and prunes with the same heuristic, in
     parallel per node;
  4. the highest new level becomes the entry point.
- **Vamana:** the same passes, alphas, slack and final pruning. Batches are at most a 32nd of the
  linked rows and at most 256, after a 1,024-row sequential seed in the first pass. With
  batches of up to 1,024, Vamana lost 5 points of graph recall in the unit test; with 256 it
  lost none.
- `--build-threads` (default: all hardware threads) sets the threads per build.
  `--compaction-slots` still bounds concurrent builds per node.

## Evidence

- Unit tests: graphs are byte-identical on 1, 3 and 8 threads, for HNSW and Vamana. Recall is
  not lower than the row-at-a-time builders' on hard synthetic data.
- SIFT1M (`bench-results/phase4-adr0019-builds.md`): HNSW builds in 35.6 s on 16 threads
  against 185.5 s row at a time (5.2x), with the same recall and identical graphs on 1, 4 and
  16 threads. Single-pass Vamana builds in 86.1 s against 323.5 s (3.8x), at equal recall.
- Campaign 20,000 seeds with zero violations; 102 workspace tests pass.
- Through a 3-node cluster on one host (2M rows): builds settle in under 50 s instead of up to
  117 s. Ingest shows no clear change, and CPU-seconds rise by about 60%, because the three
  nodes share 16 hardware threads.

## Consequences

- Segment files differ from those of earlier builds (same format, different graph): the segment
  format version does not change, since readers are unaffected.
- A build now uses many cores for a short time instead of one core for a long time. With
  `--compaction-slots` builds at once, a node can run slots × threads build threads; the
  default leaves the executor threads competing with them.
- Builds stay deterministic, so the simulation campaign still covers them (sequentially).
- On one thread the batched HNSW build is 20% slower than the old builder (intra-batch
  candidates). Nodes sharing a machine should set `--build-threads`. Query latency during a
  parallel build has not been measured.

## Default revised (2026-09-25)

- On the GCP 50M run (3 × 8 vCPU), `--compaction-slots 4` with the old default ran up to
  32 build threads per VM. The Raft actors starved and could no longer answer a status
  request. Leadership piled up on one node (8 of 8 shards), and ingest fell from about
  40k to 4.7k docs/s.
- The default is now `hardware threads / (2 × compaction slots)`, at least 1. All
  concurrent builds together use at most half the machine, and the executors keep the
  rest. Results do not change: builds are identical whatever the thread count.
- On this 16-thread development machine with 2 slots, a build now gets 4 threads instead
  of 16, so a single large build takes longer. Pass `--build-threads` explicitly when
  builds do not compete with serving.
