# Scale benchmark through the cluster: Bigann, 2000000 rows

- Date: 2026-09-25
- Cluster: 3 nodes (processes on one machine, every node holds every shard); 2000000 × 128-d BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 2000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest throughput | 12153 docs/s (165 s for rows 0..2000000) |
| upsert batch latency p50 / p99 / max | 264 / 1397 / 4245 ms |
| background builds settled (30s without segment change) | 101 s after ingest |
| node 1 after settling | 4 shards, segments per shard [3, 3, 3, 3], live docs 2000000 |
| node 1 memory: RSS after ingest / settled, peak | 2.0 GB / 2.0 GB, 2.3 GB |
| node 2 memory: RSS after ingest / settled, peak | 1.6 GB / 1.8 GB, 2.1 GB |
| node 3 memory: RSS after ingest / settled, peak | 1.5 GB / 1.8 GB, 2.1 GB |

## unfiltered, stale reads

**recall@10 0.9886**, p50 3.16 ms, p99 5.41 ms, max 7.6 ms, 2315 QPS (8 threads)

## unfiltered, linearizable reads

**recall@10 0.9886**, p50 5.21 ms, p99 7.73 ms, max 9.3 ms, 1485 QPS (8 threads)

## flag_1 filter (≈1%), stale reads

**recall@10 0.9922**, p50 1.35 ms, p99 2.50 ms, max 4.0 ms, 5516 QPS (8 threads)

## flag_1 filter (≈1%), linearizable reads

**recall@10 0.9922**, p50 2.45 ms, p99 3.59 ms, max 4.6 ms, 3201 QPS (8 threads)

## Takedowns

Visible on all nodes under read-your-writes: p50 56.2 ms, p99 119.4 ms, max 121.8 ms (200 takedowns)

