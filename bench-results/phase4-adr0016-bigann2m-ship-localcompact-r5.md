# Scale benchmark through the cluster: Bigann, 2000000 rows

- Date: 2026-09-25
- Cluster: 3 nodes (processes on one machine, every node holds every shard); 2000000 × 128-d BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 2000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest throughput | 9687 docs/s (206 s for rows 0..2000000) |
| upsert batch latency p50 / p99 / max | 281 / 3462 / 7827 ms |
| background builds settled (30s without segment change) | 226 s after ingest |
| node 1 after settling | 4 shards, segments per shard [3, 3, 3, 3], live docs 2000000 |
| node 1 memory: RSS after ingest / settled, peak | 2.1 GB / 2.1 GB, 2.4 GB |
| node 2 memory: RSS after ingest / settled, peak | 2.0 GB / 2.1 GB, 2.4 GB |
| node 3 memory: RSS after ingest / settled, peak | 2.0 GB / 2.1 GB, 2.4 GB |

## unfiltered, stale reads

**recall@10 0.9885**, p50 3.30 ms, p99 6.03 ms, max 7.1 ms, 2236 QPS (8 threads)

## unfiltered, linearizable reads

**recall@10 0.9883**, p50 5.31 ms, p99 7.72 ms, max 8.8 ms, 1472 QPS (8 threads)

## flag_1 filter (≈1%), stale reads

**recall@10 0.9920**, p50 1.39 ms, p99 2.94 ms, max 4.2 ms, 5152 QPS (8 threads)

## flag_1 filter (≈1%), linearizable reads

**recall@10 0.9919**, p50 2.46 ms, p99 3.62 ms, max 5.1 ms, 3169 QPS (8 threads)

## Takedowns

Visible on all nodes under read-your-writes: p50 67.8 ms, p99 103.5 ms, max 122.5 ms (200 takedowns)

