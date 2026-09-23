# Scale benchmark through the cluster: Bigann, 20000000 rows

- Date: 2026-09-23
- Cluster: 3 nodes (processes on one machine, every node holds every shard); 20000000 × 128-d BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 5000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest throughput | 7366 docs/s (2715 s for 20000000 rows) |
| upsert batch latency p50 / p99 / max | 423 / 1891 / 8355 ms |
| background builds settled (60s without segment change) | 96 s after ingest |
| node 1 after settling | 8 shards, segments per shard [23, 23, 23, 23, 23, 23, 23, 23], live docs 20000000 |
| node 1 memory: RSS after ingest / settled, peak | 9.1 GB / 8.7 GB, 9.1 GB |
| node 2 memory: RSS after ingest / settled, peak | 9.0 GB / 8.7 GB, 9.1 GB |
| node 3 memory: RSS after ingest / settled, peak | 9.2 GB / 8.9 GB, 9.3 GB |

## unfiltered, stale reads

**recall@10 0.9870**, p50 90.31 ms, p99 120.19 ms, max 209.2 ms, 88 QPS (8 threads)

## unfiltered, linearizable reads

**recall@10 0.9870**, p50 86.05 ms, p99 110.23 ms, max 447.5 ms, 92 QPS (8 threads)

## flag_1 filter (≈1%), stale reads

**recall@10 0.9910**, p50 31.15 ms, p99 46.79 ms, max 53.6 ms, 255 QPS (8 threads)

## flag_1 filter (≈1%), linearizable reads

**recall@10 0.9910**, p50 33.01 ms, p99 41.71 ms, max 60.6 ms, 240 QPS (8 threads)

## Takedowns

Visible on all nodes under read-your-writes: p50 31.6 ms, p99 43.1 ms, max 46.0 ms (200 takedowns)

