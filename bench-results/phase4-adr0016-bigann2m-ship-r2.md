# Scale benchmark through the cluster: Bigann, 2000000 rows

- Date: 2026-09-24
- Cluster: 3 nodes (processes on one machine, every node holds every shard); 2000000 × 128-d BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 2000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest throughput | 16258 docs/s (123 s for rows 0..2000000) |
| upsert batch latency p50 / p99 / max | 251 / 926 / 2168 ms |
| background builds settled (30s without segment change) | 68 s after ingest |
| node 1 after settling | 4 shards, segments per shard [5, 5, 5, 5], live docs 2000000 |
| node 1 memory: RSS after ingest / settled, peak | 2.3 GB / 2.3 GB, 2.4 GB |
| node 2 memory: RSS after ingest / settled, peak | 1.8 GB / 1.9 GB, 2.0 GB |
| node 3 memory: RSS after ingest / settled, peak | 2.3 GB / 2.3 GB, 2.5 GB |

## unfiltered, stale reads

**recall@10 0.9882**, p50 7.24 ms, p99 12.14 ms, max 14.6 ms, 1008 QPS (8 threads)

## unfiltered, linearizable reads

**recall@10 0.9882**, p50 12.61 ms, p99 17.97 ms, max 22.9 ms, 628 QPS (8 threads)

## flag_1 filter (≈1%), stale reads

**recall@10 0.9921**, p50 1.54 ms, p99 2.76 ms, max 3.7 ms, 4813 QPS (8 threads)

## flag_1 filter (≈1%), linearizable reads

**recall@10 0.9921**, p50 2.64 ms, p99 3.96 ms, max 5.6 ms, 2948 QPS (8 threads)

## Takedowns

Visible on all nodes under read-your-writes: p50 70.3 ms, p99 121.4 ms, max 158.1 ms (500 takedowns)

