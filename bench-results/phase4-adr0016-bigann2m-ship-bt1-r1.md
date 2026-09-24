# Scale benchmark through the cluster: Bigann, 2000000 rows

- Date: 2026-09-24
- Cluster: 3 nodes (processes on one machine, every node holds every shard); 2000000 × 128-d BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 2000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest throughput | 16829 docs/s (119 s for rows 0..2000000) |
| upsert batch latency p50 / p99 / max | 243 / 957 / 2863 ms |
| background builds settled (30s without segment change) | 117 s after ingest |
| node 1 after settling | 4 shards, segments per shard [4, 5, 4, 5], live docs 2000000 |
| node 1 memory: RSS after ingest / settled, peak | 2.5 GB / 2.6 GB, 2.7 GB |
| node 2 memory: RSS after ingest / settled, peak | 2.0 GB / 2.1 GB, 2.3 GB |
| node 3 memory: RSS after ingest / settled, peak | 2.3 GB / 2.5 GB, 2.6 GB |

## unfiltered, stale reads

**recall@10 0.9884**, p50 5.71 ms, p99 8.77 ms, max 11.2 ms, 1278 QPS (8 threads)

## unfiltered, linearizable reads

**recall@10 0.9884**, p50 9.31 ms, p99 13.34 ms, max 16.2 ms, 840 QPS (8 threads)

## flag_1 filter (≈1%), stale reads

**recall@10 0.9927**, p50 1.55 ms, p99 2.93 ms, max 3.5 ms, 4605 QPS (8 threads)

## flag_1 filter (≈1%), linearizable reads

**recall@10 0.9927**, p50 2.74 ms, p99 3.95 ms, max 4.7 ms, 2861 QPS (8 threads)

## Takedowns

Visible on all nodes under read-your-writes: p50 65.6 ms, p99 102.6 ms, max 103.7 ms (200 takedowns)

