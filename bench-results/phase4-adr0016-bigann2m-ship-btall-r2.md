# Scale benchmark through the cluster: Bigann, 2000000 rows

- Date: 2026-09-24
- Cluster: 3 nodes (processes on one machine, every node holds every shard); 2000000 × 128-d BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 2000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest throughput | 13756 docs/s (145 s for rows 0..2000000) |
| upsert batch latency p50 / p99 / max | 262 / 1044 / 2978 ms |
| background builds settled (30s without segment change) | 49 s after ingest |
| node 1 after settling | 4 shards, segments per shard [5, 5, 5, 5], live docs 2000000 |
| node 1 memory: RSS after ingest / settled, peak | 1.6 GB / 1.7 GB, 1.8 GB |
| node 2 memory: RSS after ingest / settled, peak | 1.5 GB / 1.6 GB, 1.7 GB |
| node 3 memory: RSS after ingest / settled, peak | 1.7 GB / 1.7 GB, 1.8 GB |

## unfiltered, stale reads

**recall@10 0.9880**, p50 4.60 ms, p99 8.04 ms, max 10.0 ms, 1599 QPS (8 threads)

## unfiltered, linearizable reads

**recall@10 0.9880**, p50 7.43 ms, p99 10.73 ms, max 12.9 ms, 1031 QPS (8 threads)

## flag_1 filter (≈1%), stale reads

**recall@10 0.9923**, p50 1.35 ms, p99 2.66 ms, max 3.0 ms, 5308 QPS (8 threads)

## flag_1 filter (≈1%), linearizable reads

**recall@10 0.9923**, p50 2.35 ms, p99 3.53 ms, max 4.3 ms, 3314 QPS (8 threads)

## Takedowns

Visible on all nodes under read-your-writes: p50 77.8 ms, p99 108.9 ms, max 109.1 ms (200 takedowns)

