# Scale benchmark through the cluster: Bigann, 2000000 rows

- Date: 2026-09-24
- Cluster: 3 nodes (processes on one machine, every node holds every shard); 2000000 × 128-d BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 2000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest throughput | 10444 docs/s (191 s for rows 0..2000000) |
| upsert batch latency p50 / p99 / max | 268 / 2957 / 4639 ms |
| background builds settled (30s without segment change) | 323 s after ingest |
| node 1 after settling | 4 shards, segments per shard [3, 3, 3, 3], live docs 2000000 |
| node 1 memory: RSS after ingest / settled, peak | 1.9 GB / 2.3 GB, 2.6 GB |
| node 2 memory: RSS after ingest / settled, peak | 2.0 GB / 2.1 GB, 2.5 GB |
| node 3 memory: RSS after ingest / settled, peak | 1.9 GB / 2.0 GB, 2.4 GB |

## unfiltered, stale reads

**recall@10 0.9883**, p50 3.12 ms, p99 5.56 ms, max 36.6 ms, 2234 QPS (8 threads)

## unfiltered, linearizable reads

**recall@10 0.9883**, p50 5.15 ms, p99 7.34 ms, max 8.7 ms, 1507 QPS (8 threads)

## flag_1 filter (≈1%), stale reads

**recall@10 0.9928**, p50 1.36 ms, p99 2.60 ms, max 4.1 ms, 5224 QPS (8 threads)

## flag_1 filter (≈1%), linearizable reads

**recall@10 0.9928**, p50 2.36 ms, p99 3.38 ms, max 4.6 ms, 3323 QPS (8 threads)

## Takedowns

Visible on all nodes under read-your-writes: p50 79.8 ms, p99 129.8 ms, max 129.8 ms (50 takedowns)

