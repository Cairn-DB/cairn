# Scale benchmark through the cluster: Bigann, 2000000 rows

- Date: 2026-09-24
- Cluster: 3 nodes (processes on one machine, every node holds every shard); 2000000 × 128-d BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 2000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest throughput | 15157 docs/s (132 s for rows 0..2000000) |
| upsert batch latency p50 / p99 / max | 242 / 667 / 16056 ms |
| background builds settled (30s without segment change) | 115 s after ingest |
| node 1 after settling | 4 shards, segments per shard [5, 3, 5, 5], live docs 2000000 |
| node 1 memory: RSS after ingest / settled, peak | 2.0 GB / 2.2 GB, 2.4 GB |
| node 2 memory: RSS after ingest / settled, peak | 1.7 GB / 2.0 GB, 2.1 GB |
| node 3 memory: RSS after ingest / settled, peak | 2.4 GB / 2.5 GB, 2.7 GB |

## unfiltered, stale reads

**recall@10 0.9885**, p50 4.77 ms, p99 8.73 ms, max 10.6 ms, 1563 QPS (8 threads)

## unfiltered, linearizable reads

**recall@10 0.9885**, p50 7.72 ms, p99 10.95 ms, max 13.0 ms, 1034 QPS (8 threads)

## flag_1 filter (≈1%), stale reads

**recall@10 0.9925**, p50 1.57 ms, p99 2.85 ms, max 3.3 ms, 4774 QPS (8 threads)

## flag_1 filter (≈1%), linearizable reads

**recall@10 0.9925**, p50 2.63 ms, p99 3.87 ms, max 5.2 ms, 2972 QPS (8 threads)

## Takedowns

Visible on all nodes under read-your-writes: p50 98.8 ms, p99 118.0 ms, max 130.9 ms (500 takedowns)

