# Scale benchmark through the cluster: Bigann, 2000000 rows

- Date: 2026-09-24
- Cluster: 3 nodes (processes on one machine, every node holds every shard); 2000000 × 128-d BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 2000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest throughput | 16695 docs/s (120 s for rows 0..2000000) |
| upsert batch latency p50 / p99 / max | 246 / 786 / 1536 ms |
| background builds settled (30s without segment change) | 57 s after ingest |
| node 1 after settling | 4 shards, segments per shard [5, 5, 5, 5], live docs 2000000 |
| node 1 memory: RSS after ingest / settled, peak | 1.9 GB / 2.0 GB, 2.0 GB |
| node 2 memory: RSS after ingest / settled, peak | 1.8 GB / 2.0 GB, 2.1 GB |
| node 3 memory: RSS after ingest / settled, peak | 2.1 GB / 2.2 GB, 2.3 GB |

## unfiltered, stale reads

**recall@10 0.9883**, p50 4.92 ms, p99 8.77 ms, max 11.1 ms, 1481 QPS (8 threads)

## unfiltered, linearizable reads

**recall@10 0.9883**, p50 8.11 ms, p99 11.58 ms, max 14.1 ms, 969 QPS (8 threads)

## flag_1 filter (≈1%), stale reads

**recall@10 0.9921**, p50 1.63 ms, p99 3.57 ms, max 4.5 ms, 4321 QPS (8 threads)

## flag_1 filter (≈1%), linearizable reads

**recall@10 0.9921**, p50 2.63 ms, p99 4.51 ms, max 5.0 ms, 2902 QPS (8 threads)

## Takedowns

Visible on all nodes under read-your-writes: p50 67.8 ms, p99 116.9 ms, max 119.4 ms (500 takedowns)

