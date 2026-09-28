# ADR 0029: mimalloc as the server's allocator

- Status: accepted (delegated 2026-09-28; the owner asked to reproduce and fix the post-ingest
  slowdown: "vas-y pour la reproduction locale de la fuite mémoire")
- Date: 2026-09-28

## Context

- In GCP run 8 (`bench-results/phase4-gcp-bigann50m-run8.md`), queries measured on the nodes
  that had just ingested 50M rows missed every target: p99 117-151 ms at 80 QPS. After a
  restart on the same data they gave 42-44 ms at 244 QPS. A bisection showed no code
  regression. The ingesting process held 33.8 GB of anonymous memory against 17 GB after the
  restart.
- The problem reproduces locally (`data/run-memrepro.sh`: 3 nodes, 6M rows with merges, then
  merges paused).
  - Memory per node was 4.1-5.1 GB after ingest, against 1.8 GB after a restart.
  - Four-client throughput was 35% lower after ingest.
- Diagnosis:
  - `malloc_trim(0)`, called through gdb, returned about 2.5 GB per node but did not change
    latency.
  - Per-shard timing (`cairn_query::stats`) put the whole slowdown in the search jobs
    themselves.
  - Stack samples under load put 14% of search time in glibc's `_int_malloc` after ingest,
    against 1% after a restart. Most of it came from a bitmap clone per segment and query.
  - Removing that clone (2c12511) halved the gap. The rest came from the other per-query
    allocations.
  - The cause is glibc's heap after hours of index builds: fragmented free lists slow every
    allocation, and freed memory stays in the arenas.

## Options considered

1. **Remove every per-query allocation** (scratch reuse everywhere). Worth doing on hot paths,
   but it never ends: any new allocation reintroduces the problem, and the retained memory
   stays.
2. **Periodic `malloc_trim`.** It returns memory to the system but does not defragment the
   heap. Measured: no latency change.
3. **Another allocator: jemalloc or mimalloc.** Both resist fragmentation and return freed
   memory. mimalloc is a small C library vendored and built by its crate with the system C
   compiler, under the MIT license. jemalloc's crate is larger and slower to build. Chosen:
   mimalloc, measured.

## Decision

- `cairn-server` sets `mimalloc::MiMalloc` as its global allocator (`main.rs`). The library
  crates are unchanged: tests, Miri and the simulator keep the system allocator.
- The new dependencies are `mimalloc` 0.1 and `libmimalloc-sys` (MIT). The Docker build image
  (`rust:1-bookworm`) already has a C compiler.
- `MALLOC_ARENA_MAX=2` in the scripts no longer has any effect on the server.

## Evidence

Local, 3 nodes, 6M rows. Reports: `data/run-memrepro-*.log`, `data/run-alloc-ab.log`
(gitignored); summary in `docs/progress.md`.

| same data (27 segs), freshly started, 2 runs each | glibc | mimalloc |
|---|---|---|
| unfiltered, 4 clients | 1,204 / 1,205 QPS | 1,462 / 1,536 QPS (+21-27%) |
| 1% filter, 4 clients | 2,045 / 2,036 QPS | 2,779 / 2,787 QPS (+36%) |
| unfiltered p50, 1 client | 2.35 / 2.42 ms | 2.02 / 2.04 ms |
| anonymous memory per node | 1.8 GB | 2.0 GB |

| after ingest vs restarted, same run | glibc (with 2c12511) | mimalloc |
|---|---|---|
| memory per node | 4.1-4.5 GB vs 1.8 GB | 2.1-2.2 GB vs 1.9 GB |
| unfiltered, 4 clients | 599 vs 723 QPS (-17%) | 1,442 vs 1,538 QPS (-6%) |

Recall is unchanged: allocation does not affect results.

## Consequences

- A node no longer needs a restart after a large ingest to serve at full speed. This is to
  confirm at 50M on the next GCP run.
- Memory at rest is about 10% higher (mimalloc uses larger pages and segments) but stays flat
  over time.
- A C toolchain is needed to build the server, which the build images already have.

## Confirmed at 50M (GCP run 9, 2026-09-28)

Queries ran on the nodes that had just ingested 50M rows, with no restart
(`bench-results/phase4-gcp-bigann50m-run9.md`):

- p99 of 35/36 ms unfiltered and 31/30 ms filtered, at 435/318 QPS;
- 16.5-17.2 GB of anonymous memory per node, against 33.8 GB in run 8;
- a restart now gains 8-9% in throughput, against a factor of 3 to 4 before.

Ingest went from 8,343 to 20,667 docs/s over the same run, a gain not seen locally. Run 10
reproduced it (19,660 docs/s, `bench-results/phase4-gcp-bigann50m-run10.md`).

