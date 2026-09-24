# Scale benchmark through the cluster: Bigann, 2000000 rows

- Date: 2026-09-25
- Cluster: 3 nodes (processes on one machine, every node holds every shard); 2000000 × 128-d BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 2000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest throughput | 12206 docs/s (164 s for rows 0..2000000) |
| upsert batch latency p50 / p99 / max | 257 / 1562 / 3997 ms |
| background builds settled (30s without segment change) | 122 s after ingest |
| node 1 after settling | 4 shards, segments per shard [3, 3, 3, 3], live docs 2000000 |
| node 1 memory: RSS after ingest / settled, peak | 2.0 GB / 2.0 GB, 2.3 GB |
| node 2 memory: RSS after ingest / settled, peak | 1.8 GB / 1.8 GB, 2.1 GB |
| node 3 memory: RSS after ingest / settled, peak | 1.6 GB / 1.8 GB, 2.1 GB |

## unfiltered, stale reads

**recall@10 0.9883**, p50 3.17 ms, p99 5.87 ms, max 7.0 ms, 2263 QPS (8 threads)

## unfiltered, linearizable reads

**recall@10 0.9883**, p50 5.34 ms, p99 7.82 ms, max 9.5 ms, 1454 QPS (8 threads)

## flag_1 filter (≈1%), stale reads

**recall@10 0.9920**, p50 1.41 ms, p99 2.75 ms, max 5.0 ms, 5115 QPS (8 threads)

## flag_1 filter (≈1%), linearizable reads

**recall@10 0.9920**, p50 2.48 ms, p99 3.84 ms, max 5.0 ms, 3167 QPS (8 threads)

## Takedowns

Visible on all nodes under read-your-writes: p50 63.7 ms, p99 121.4 ms, max 122.2 ms (200 takedowns)

