# Scale benchmark through the cluster: Bigann, 2000000 rows

- Date: 2026-09-24
- Cluster: 3 nodes (processes on one machine, every node holds every shard); 2000000 × 128-d BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 2000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest throughput | 16580 docs/s (121 s for rows 0..2000000) |
| upsert batch latency p50 / p99 / max | 240 / 743 / 6392 ms |
| background builds settled (30s without segment change) | 83 s after ingest |
| node 1 after settling | 4 shards, segments per shard [5, 5, 4, 4], live docs 2000000 |
| node 1 memory: RSS after ingest / settled, peak | 2.0 GB / 2.2 GB, 2.4 GB |
| node 2 memory: RSS after ingest / settled, peak | 1.9 GB / 2.2 GB, 2.3 GB |
| node 3 memory: RSS after ingest / settled, peak | 2.5 GB / 2.5 GB, 2.7 GB |

## unfiltered, stale reads

**recall@10 0.9887**, p50 4.59 ms, p99 8.86 ms, max 11.9 ms, 1603 QPS (8 threads)

## unfiltered, linearizable reads

**recall@10 0.9887**, p50 7.70 ms, p99 10.73 ms, max 13.9 ms, 1027 QPS (8 threads)

## flag_1 filter (≈1%), stale reads

**recall@10 0.9925**, p50 1.50 ms, p99 2.81 ms, max 3.5 ms, 4905 QPS (8 threads)

## flag_1 filter (≈1%), linearizable reads

**recall@10 0.9925**, p50 2.62 ms, p99 3.89 ms, max 4.7 ms, 2976 QPS (8 threads)

## Takedowns

Visible on all nodes under read-your-writes: p50 93.2 ms, p99 176.6 ms, max 248.8 ms (100 takedowns)

