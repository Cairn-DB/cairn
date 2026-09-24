# Scale benchmark through the cluster: Bigann, 2000000 rows

- Date: 2026-09-24
- Cluster: 3 nodes (processes on one machine, every node holds every shard); 2000000 × 128-d BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 2000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest throughput | 9303 docs/s (215 s for rows 0..2000000) |
| upsert batch latency p50 / p99 / max | 279 / 3496 / 6140 ms |
| background builds settled (30s without segment change) | 207 s after ingest |
| node 1 after settling | 4 shards, segments per shard [3, 3, 3, 3], live docs 2000000 |
| node 1 memory: RSS after ingest / settled, peak | 2.1 GB / 2.1 GB, 2.4 GB |
| node 2 memory: RSS after ingest / settled, peak | 2.0 GB / 2.0 GB, 2.4 GB |
| node 3 memory: RSS after ingest / settled, peak | 1.9 GB / 2.0 GB, 2.3 GB |

## unfiltered, stale reads

**recall@10 0.9877**, p50 3.41 ms, p99 6.14 ms, max 7.3 ms, 2217 QPS (8 threads)

## unfiltered, linearizable reads

**recall@10 0.9877**, p50 5.40 ms, p99 7.74 ms, max 9.3 ms, 1456 QPS (8 threads)

## flag_1 filter (≈1%), stale reads

**recall@10 0.9927**, p50 1.49 ms, p99 2.56 ms, max 3.4 ms, 5009 QPS (8 threads)

## flag_1 filter (≈1%), linearizable reads

**recall@10 0.9927**, p50 2.50 ms, p99 3.60 ms, max 4.9 ms, 3130 QPS (8 threads)

## Takedowns

Visible on all nodes under read-your-writes: p50 80.3 ms, p99 184.3 ms, max 217.7 ms (200 takedowns)

