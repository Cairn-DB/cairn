# Scale benchmark through the cluster: Bigann, 2000000 rows

- Date: 2026-09-25
- Cluster: 3 nodes (processes on one machine, every node holds every shard); 2000000 × 128-d BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 2000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest throughput | 10152 docs/s (197 s for rows 0..2000000) |
| upsert batch latency p50 / p99 / max | 282 / 2052 / 8359 ms |
| background builds settled (30s without segment change) | 161 s after ingest |
| node 1 after settling | 4 shards, segments per shard [3, 3, 3, 3], live docs 2000000 |
| node 1 memory: RSS after ingest / settled, peak | 2.0 GB / 1.9 GB, 2.3 GB |
| node 2 memory: RSS after ingest / settled, peak | 1.6 GB / 1.9 GB, 2.1 GB |
| node 3 memory: RSS after ingest / settled, peak | 1.9 GB / 2.0 GB, 2.3 GB |

## unfiltered, stale reads

**recall@10 0.9887**, p50 3.25 ms, p99 5.97 ms, max 7.9 ms, 2280 QPS (8 threads)

## unfiltered, linearizable reads

**recall@10 0.9887**, p50 5.27 ms, p99 7.68 ms, max 9.2 ms, 1492 QPS (8 threads)

## flag_1 filter (≈1%), stale reads

**recall@10 0.9926**, p50 1.39 ms, p99 2.53 ms, max 3.0 ms, 5271 QPS (8 threads)

## flag_1 filter (≈1%), linearizable reads

**recall@10 0.9926**, p50 2.42 ms, p99 3.50 ms, max 5.1 ms, 3224 QPS (8 threads)

## Takedowns

Visible on all nodes under read-your-writes: p50 73.7 ms, p99 103.7 ms, max 125.0 ms (200 takedowns)

