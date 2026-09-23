# Phase 4 follow-up: scale runs through the 3-node cluster

Date: 2026-09-23. The owner asked for 10M, then 50M rows, through the cluster rather than
in-process.

## Setup

- Three `cairn-server` processes on the development machine (ADR 0012). Each node hosts all
  8 shards (static membership), with 4 executor cores per node.
- Flags: 64 MB memtables, `--max-segments 24` (no compaction during the bulk load),
  `--sq8-only` (ADR 0013).
- `cairn-bench cluster-scale` ingests through `cairn-client`. Each batch is acknowledged after
  its Raft commit on a majority. The tool waits until no segment changes for 60 s on any node,
  runs the queries with stale and linearizable reads from 8 client threads, times 200
  takedowns, and reads each node's resident memory from `/proc`.

Commands:

```
cargo build --release -p cairn-server -p cairn-bench
data/run-yfcc10m.sh                  # wraps the two commands below, plus a memory watchdog
CAIRN_SERVER_FLAGS=--sq8-only tools/scripts/cluster.sh start data/scale-yfcc tools/scripts/yfcc-schema.json 8 4 67108864 24
cairn-bench cluster-scale --dataset yfcc --dir data/yfcc10m --node 1=127.0.0.1:7101 --node 2=127.0.0.1:7102 \
  --node 3=127.0.0.1:7103 --n 10000000 --queries 10000 --takedowns 200 --settle-secs 60 \
  --pids data/cluster.pids --out bench-results/phase4-cluster-yfcc10m.md
```

## Memory: why the defaults did not fit

The first YFCC-10M attempt used 128 MB memtables and f32 rows resident. It reached 9 GB per
node at 2M rows (about 4.5 KB per row per node) and was stopped before it exhausted memory.
ADR 0013 made two changes:

- Loaded segments can drop their f32 rows and keep only the SQ8 codes and the graph.
- The replica no longer clones the frozen memtable a second time for the build.

With both changes, 10M rows fit in 9.1 GB per node after settling.

## YFCC-10M (Big-ANN filtered track), official ground truth

Result file: `bench-results/phase4-cluster-yfcc10m.md`.

| metric | result |
|---|---|
| ingest | 6,741 docs/s (10M rows in 24.7 min), batch p99 2.6 s, max 9.3 s |
| memory per node after settling | 9.0-9.2 GB (peak 9.6 GB) |
| recall@10, each query's own tag filter | 0.9893 |
| stale reads p50 / p99 | 29.0 / 79.6 ms, 243 QPS |
| linearizable reads p50 / p99 | 47.7 / 117.2 ms, 148 QPS |
| takedown visible on all nodes p50 / p99 | 29.8 / 38.9 ms |

Against SPEC section 8:

- **Recall above 95% under selective filters: met.** It stays at 0.9926 or higher below 1k
  passing rows. It is 0.989 overall, against 0.9994 in-process with f32 reranking. The one-point
  loss is the price of SQ8-only scoring. YFCC vectors are uint8, so SQ8 is close to lossless
  here. Float embeddings would lose more.
- **p99 under 100 ms: met for stale reads, missed for linearizable reads** (117 ms).
- **Takedown visible cluster-wide in under 1 s: met** (39 ms p99).

Reading:

- Latency is almost flat across selectivity buckets: 26 ms p50 below 1k passing rows, 43 ms
  above 1M. That points to a fixed per-query cost, not to the search itself. Each query touches
  8 shards × 16 segments = 128 segment searches. Each one evaluates the tag filter against a
  string dictionary and builds a bitmap the size of the segment. The in-process run searched
  10 segments and reached p50 0.4 ms on the smallest filters.
- Linearizable reads cost about 19 ms more at p50. Stale legs are served on the node the client
  talks to. Linearizable legs are forwarded to each shard's leader, which confirms leadership
  with a heartbeat round, on cores that are already saturated by 8 query threads.
- The ingest tail (a 9.3 s maximum batch) comes from segment publication on the replica actor,
  which is the known gap from the Phase 4 report.

## BigANN (SIFT1B) 20M prefix, brute-force ground truth

Result file: `bench-results/phase4-cluster-bigann20m.md`. Rows are a 20M prefix of
`base.1B.u8bin` (128-d uint8) with bench-gen attributes. The tool computes the ground truth by
brute force on 5,000 of the public queries, unfiltered and with `flag_1` (about 1%).

```
N=20000000 TAG=20m data/run-bigann.sh   # same flags as YFCC, sift-schema.json
```

| metric | result |
|---|---|
| ingest | 7,366 docs/s (20M rows in 45 min), batch p99 1.9 s, max 8.4 s |
| memory per node after settling | 8.7-8.9 GB (about 0.43 KB per row per node) |
| segments per shard | 23 (8 shards) |
| unfiltered recall@10, p50 / p99 (stale) | 0.9870, 90.3 / 120.2 ms, 88 QPS |
| unfiltered, linearizable | 0.9870, 86.1 / 110.2 ms, 92 QPS |
| flag_1 (≈1%) recall@10, p50 / p99 (stale) | 0.9910, 31.2 / 46.8 ms, 255 QPS |
| flag_1, linearizable | 0.9910, 33.0 / 41.7 ms, 240 QPS |
| takedown visible on all nodes p50 / p99 | 31.6 / 43.1 ms |

Reading:

- **Selective filters meet every target at 20M:** recall 0.991, p99 under 50 ms at both
  consistency levels, takedowns under 50 ms.
- **Unfiltered queries miss the 100 ms p99 target** (110-120 ms). Each query runs a graph
  search in 8 × 23 = 184 segments of about 110k rows each. The cost grows with the number of
  segments, not with the rows. Fewer, larger segments (compaction after the bulk load, or
  bigger memtables once memory allows) is the obvious lever. It was not tried, to keep memory
  within the host.
- In this run linearizable reads are no slower than stale reads. With 5,000 queries and a longer
  per-query search, the extra hop is a small share of the latency.

## 50M rows

**Not run: it does not fit on this machine.** Every node holds every shard, so three replicas
means three copies in one machine's RAM. The settled footprint measured at 20M is 0.43 KB per
row per node, with SQ8-only residency. 50M rows would need about 0.43 KB × 50M × 3 = 65 GB
before ingest transients, against 58 GB installed and about 48 GB free. The first 50M rows of
BigANN are being downloaded to `data/bigann/` (26M done when this was written) for a run on
larger hardware. Reaching 50M needs one of
the following:

- three machines, or one machine with at least about 96 GB,
- shard placement with fewer copies per host (membership work), or
- a disk-resident index (DiskANN-style) that keeps only compressed codes in RAM.

## Summary against SPEC section 8

| target | YFCC-10M (filtered track) | BigANN-20M |
|---|---|---|
| recall@10 > 95% with selective filter | met, 0.989 | met, 0.991 (1%) |
| p99 < 100 ms | met for stale reads (80 ms), missed for linearizable reads (117 ms) | met with the filter (47 ms), missed unfiltered (120 ms) |
| takedown cluster-wide < 1 s | met, 39 ms p99 | met, 43 ms p99 |
| 10-50M rows | 10M done | 20M done; 50M does not fit on one host |

Recall figures use SQ8-only scoring on uint8 datasets, where SQ8 is almost lossless. They
would be lower on float embeddings without the f32 rerank (ADR 0013).

---

# Part 2 (2026-09-23, afternoon): fixing the missed p99, placement, disk-resident index

The owner asked to catch the two missed 100 ms targets (YFCC linearizable, BigANN unfiltered),
accepted the three levers for 50M (a bigger host, placement, a disk-resident index), and
offered a Hetzner Cloud account.

## Why the p99 was missed

Four causes, each measured or read in the code:

1. **One node did all the work.** Every `cairn-client` started on the lowest node id, so every
   stale read and every query coordination landed on node 1's four cores. Nodes 2 and 3 stayed
   idle.
2. **Sequential forwarding.** For linearizable reads, the coordinator forwarded the legs of
   shards led elsewhere one round trip after another.
3. **Idle memtables were never flushed.** After a bulk load, each shard kept up to 64 MB of
   rows in its memtable, and every query scanned them without a graph.
4. **Too many segments.** Every query ran 23 segment searches per shard (184 in total at 20M).

## Changes

- The client rotates its starting node. The coordinator forwards legs concurrently, and on a
  transport failure it tries the shard's other hosts.
- `--idle-flush-ms`, 5 s by default, flushes a memtable that receives no writes.
- Tiered compaction (`--target-segment-rows`) merges the longest run of adjacent segments that
  fits the target, so each row is rewritten about once per size tier.
  `--compaction-slots` caps concurrent merges per node, because a merge holds its rows in
  RAM.
- `MALLOC_ARENA_MAX=2` in `cluster.sh`. Without it, glibc kept freed merge buffers in
  per-thread arenas: 15.9 GB per node for 8.7 GB of live data.

## Results (same data, restarted, post-load compaction to 2-5 segments per shard)

| | before | after |
|---|---|---|
| BigANN-20M unfiltered p99, stale / linearizable | 120 / 110 ms | **19.7 / 48.6 ms** |
| BigANN-20M unfiltered QPS (8 threads), stale | 88 | 573 |
| BigANN-20M 1% filter p99, stale / linearizable | 47 / 42 ms | 22 / 43 ms |
| YFCC-10M tag filter p99, stale / linearizable | 80 / 117 ms | **34 / 59 ms** |
| YFCC-10M QPS, stale / linearizable | 243 / 148 | 605 / 287 |
| recall@10 (BigANN unfiltered, YFCC) | 0.987, 0.989 | 0.986, 0.988 |
| takedown visible on all nodes, p99 | 43, 39 ms | 43, 43 ms |

Files: `bench-results/phase4-cluster-bigann20m-latency.md`,
`bench-results/phase4-cluster-yfcc10m-latency.md`. **Both missed targets are now met.**
Linearizable reads still cost about 2.5 times the stale ones. Follower reads (ReadIndex
served by the follower) would remove the forwarding. They were not needed to reach the target
and are listed as the next lever.

Memory after the compaction is 12.4 GB per node for YFCC, against 9.1 GB before. The
allocator keeps some of the merge buffers even with two arenas.

## Placement (fewer copies per host)

`--replication N` puts shard *s* on N consecutive nodes, starting at *s* mod the node count.
Nodes host only their shards and redirect the rest to a host. The new process test
`placement_with_fewer_replicas_than_nodes` covers 4 nodes, 8 shards and 3 replicas: every
entry node serves reads, queries and takedowns for every shard, and killing a node leaves
writes and linearizable reads working. **Dynamic membership (adding or removing a replica of a
live shard) is not implemented.** Placement is static, fixed at start.

## Disk-resident vector index (ADR 0014)

A Vamana graph with PQ codes in RAM, and node blocks (full vector and neighbors, packed so
that none straddles a page) read through the new `Disk::map` (mmap in the real runtimes). Search
is a beam of 4 nodes whose blocks are prefetched together with `madvise(WILLNEED)`. It starts
from the medoid plus 64 spread seeds. On one SIFT1M segment
(`bench-results/phase4-diskann-sift1m.md`), single thread:

| | recall@10 | warm p50 / p99 | cold p50 / p99 |
|---|---|---|---|
| L = 64 | 0.968 | 0.35 / 0.64 ms | 9.0 / 14.0 ms |
| L = 100 | 0.983 | 0.49 / 0.87 ms | 13.0 / 18.7 ms |
| L = 128 | 0.989 | 0.62 / 1.07 ms | 16.6 / 24.7 ms |
| 1% filter, PQ scan + rerank 100 | 1.000 | 0.37 / 0.48 ms | 3.8 / 5.9 ms |

It uses 33 B per row in RAM, against 430 B per row in SQ8 mode and about 1.1 KB with f32
resident. It uses 819 B per row of node blocks on disk. The build runs at 3.1k rows per second
per thread (HNSW: 5k). "Cold" drops the index file's pages from the page cache before every
query (`madvise` + `fadvise DONTNEED`). Serial page faults cost 20.8 ms per cold search at
100k rows; the prefetched beam brings that to 7.8 ms.

## Hetzner

`docs/hetzner-plan.md` and `tools/scripts/hcloud/`. The recommended setup is 3 × ccx43
(16 dedicated vCPU, 64 GB) plus a ccx33 for the client, on a real private network, at about
€1.87/h, or roughly €8 for a 50M run. **Nothing was created**, because creating servers bills
the account, which also holds production servers. It waits for the owner's go.

---

# Part 3 (2026-09-23 evening to 2026-09-24): 50M rows

## 50M on the development machine, disk-resident index

`bench-results/phase4-cluster-bigann50m-disk.md`. Three processes on one 58 GB host, 4 shards
with 4 cores each, `--disk-index --vamana-passes 1`, no compaction (about 50 segments per
shard). The load needed five restarts (see below). The final part resumed from row 38.8M on the
same data (`cairn-bench cluster-scale --start`).

| | result |
|---|---|
| memory per node | 8.6-9.7 GB (anonymous 7.5-8.4 GB; the rest is mapped index pages) |
| unfiltered recall@10 / p50 / p99 (stale) | 0.978 / 2.8 s / 13.5 s, 2 QPS |
| 1% filter recall@10 / p50 / p99 (stale) | 0.980 / 1.9 s / 5.1 s |
| takedown visible on all nodes p99 | 118 ms |

**50M fits on this machine with three replicas, but it misses the latency target by one to two
orders of magnitude.** Each query searches about 200 disk segments per node, mostly cold. The
three copies share one page cache, and page faults block the executor cores (ADR 0014). This
configuration demonstrates the memory footprint only. It is not a serving configuration.

## What the 50M attempts exposed (all fixed, all with tests or campaign evidence)

| symptom | cause | fix |
|---|---|---|
| memtable grew without bound while an index built | no write backpressure | writes wait once the memtable is at twice its threshold |
| 15 GB per node, stalled | every shard built its index at once | flush builds share the per-node build slots |
| 32 GB on one node | leader re-sent 19 MB appends to a slow follower on each proposal | Raft flow control (window, byte cap) |
| follower too slow to answer | segment loads hashed and validated on the actor | offloaded |
| 10 GB jump during catch-up | snapshot re-shipped every segment, buffered in RAM; leader read whole files per chunk | only missing segments, streamed to disk, range reads; leader keeps its log for laggards |
| GCP: leader OOM-killed at 65 GB, 42 GB queued for peers | the flow-control window freed a slot on heartbeat responses and re-sent batches repeatedly | exact settlement per append, loss by timeout, per-peer queue capped at 256 MB |
| replicas at the same applied index with different documents (chaos seed 2227) | the hard state (commit index) was persisted before the log entries; a crash in between replayed a stale entry as committed (latent since Phase 3) | entries are synced before the hard state |
| a build that began before a snapshot install could publish over a snapshot segment | no job invalidation | store generation |

The watchdog stops of the fourth and fifth attempts were probably false alarms. That watchdog
measured total RSS, which includes file-backed pages of the mapped index. The kernel reclaims
those, and anonymous memory stayed around 8 GB. The watchdog now measures anonymous memory.

Verification after the fixes: `cargo test --workspace` (83 tests), the Raft harness with
in-order delivery (500 seeds), and a new test that bounds re-sends. The simulation campaign
passed 60,000 seeds with zero violations.
