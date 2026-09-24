# Scale benchmark through the cluster: Bigann, 2000000 rows

- Date: 2026-09-24
- Cluster: 3 nodes (processes on one machine, every node holds every shard); 2000000 × 128-d BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 2000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest throughput | 12045 docs/s (166 s for rows 0..2000000) |
| upsert batch latency p50 / p99 / max | 240 / 818 / 45668 ms |
| background builds settled (30s without segment change) | 124 s after ingest |
| node 1 after settling | 4 shards, segments per shard [4, 4, 4, 4], live docs 2000000 |
| node 1 memory: RSS after ingest / settled, peak | 2.9 GB / 2.9 GB, 3.3 GB |
| node 2 memory: RSS after ingest / settled, peak | 2.7 GB / 2.7 GB, 3.0 GB |
| node 3 memory: RSS after ingest / settled, peak | 3.0 GB / 3.0 GB, 3.2 GB |

## unfiltered, stale reads

**recall@10 0.9884**, p50 3.97 ms, p99 7.82 ms, max 237.8 ms, 1451 QPS (8 threads)

## unfiltered, linearizable reads

**recall@10 0.9884**, p50 7.10 ms, p99 9.95 ms, max 12.0 ms, 1099 QPS (8 threads)

## flag_1 filter (≈1%), stale reads

**recall@10 0.9926**, p50 1.44 ms, p99 3.04 ms, max 3.7 ms, 4483 QPS (8 threads)

## flag_1 filter (≈1%), linearizable reads

**recall@10 0.9926**, p50 2.74 ms, p99 3.93 ms, max 4.7 ms, 2851 QPS (8 threads)

## Takedowns

Visible on all nodes under read-your-writes: p50 70.3 ms, p99 103.5 ms, max 113.7 ms (500 takedowns)

