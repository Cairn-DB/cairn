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
BigANN are downloaded to `data/bigann/` for a run on larger hardware. Reaching 50M needs one of
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
