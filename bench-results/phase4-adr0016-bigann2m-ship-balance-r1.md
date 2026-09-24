# Scale benchmark through the cluster: Bigann, 2000000 rows

- Date: 2026-09-24
- Cluster: 3 nodes (processes on one machine, every node holds every shard); 2000000 × 128-d BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 2000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest throughput | 12301 docs/s (163 s for rows 0..2000000) |
| upsert batch latency p50 / p99 / max | 264 / 1706 / 3955 ms |
| background builds settled (30s without segment change) | 46 s after ingest |
| node 1 after settling | 4 shards, segments per shard [5, 5, 5, 5], live docs 2000000 |
| node 1 memory: RSS after ingest / settled, peak | 1.7 GB / 1.7 GB, 1.9 GB |
| node 2 memory: RSS after ingest / settled, peak | 1.6 GB / 1.6 GB, 1.7 GB |
| node 3 memory: RSS after ingest / settled, peak | 1.5 GB / 1.5 GB, 1.6 GB |

## unfiltered, stale reads

**recall@10 0.9883**, p50 4.55 ms, p99 7.77 ms, max 9.9 ms, 1612 QPS (8 threads)

## unfiltered, linearizable reads

**recall@10 0.9883**, p50 7.66 ms, p99 10.80 ms, max 13.5 ms, 1026 QPS (8 threads)

## flag_1 filter (≈1%), stale reads

**recall@10 0.9919**, p50 1.38 ms, p99 2.62 ms, max 3.1 ms, 5182 QPS (8 threads)

## flag_1 filter (≈1%), linearizable reads

**recall@10 0.9919**, p50 2.46 ms, p99 3.82 ms, max 5.0 ms, 3147 QPS (8 threads)

## Takedowns

Visible on all nodes under read-your-writes: p50 67.8 ms, p99 154.5 ms, max 161.0 ms (200 takedowns)

