# Scale benchmark through the cluster: Bigann, 2000000 rows

- Date: 2026-09-24
- Cluster: 3 nodes (processes on one machine, every node holds every shard); 2000000 × 128-d BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 2000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest throughput | 10703 docs/s (187 s for rows 0..2000000) |
| upsert batch latency p50 / p99 / max | 272 / 2349 / 4970 ms |
| background builds settled (30s without segment change) | 232 s after ingest |
| node 1 after settling | 4 shards, segments per shard [3, 3, 3, 3], live docs 2000000 |
| node 1 memory: RSS after ingest / settled, peak | 1.9 GB / 2.0 GB, 2.4 GB |
| node 2 memory: RSS after ingest / settled, peak | 1.9 GB / 2.1 GB, 2.6 GB |
| node 3 memory: RSS after ingest / settled, peak | 1.7 GB / 2.1 GB, 2.4 GB |

## unfiltered, stale reads

**recall@10 0.9882**, p50 3.17 ms, p99 5.71 ms, max 7.3 ms, 2308 QPS (8 threads)

## unfiltered, linearizable reads

**recall@10 0.9881**, p50 5.32 ms, p99 7.53 ms, max 9.9 ms, 1495 QPS (8 threads)

## flag_1 filter (≈1%), stale reads

**recall@10 0.9938**, p50 1.35 ms, p99 2.71 ms, max 4.1 ms, 5232 QPS (8 threads)

## flag_1 filter (≈1%), linearizable reads

**recall@10 0.9925**, p50 2.35 ms, p99 3.55 ms, max 4.3 ms, 3296 QPS (8 threads)

## Takedowns

Visible on all nodes under read-your-writes: p50 95.3 ms, p99 106.3 ms, max 106.3 ms (50 takedowns)

