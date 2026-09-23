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

## 50M rows

**Not run: it does not fit on this machine.** Every node holds every shard, so three replicas
means three copies in one machine's RAM. The measured footprint at 10M is about 0.9 KB per row
per node including transients, or about 0.65 KB per row for the settled indexes. At 128
dimensions, 50M rows need more than 0.5 KB × 50M × 3 = 75 GB against 58 GB installed. Reaching
50M needs one of the following:

- three machines,
- shard placement with fewer copies per host (membership work), or
- a disk-resident index (DiskANN-style) that keeps only compressed codes in RAM.

The largest BigANN run that fits is reported below.
