# Scale benchmark through the cluster: Bigann, 2000000 rows

- Date: 2026-09-24
- Cluster: 3 nodes (processes on one machine, every node holds every shard); 2000000 × 128-d BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 2000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest throughput | 12027 docs/s (166 s for rows 0..2000000) |
| upsert batch latency p50 / p99 / max | 261 / 1410 / 5704 ms |
| background builds settled (30s without segment change) | 47 s after ingest |
| node 1 after settling | 4 shards, segments per shard [5, 5, 5, 5], live docs 2000000 |
| node 1 memory: RSS after ingest / settled, peak | 1.7 GB / 1.7 GB, 1.9 GB |
| node 2 memory: RSS after ingest / settled, peak | 1.5 GB / 1.5 GB, 1.6 GB |
| node 3 memory: RSS after ingest / settled, peak | 1.6 GB / 1.6 GB, 1.8 GB |

## unfiltered, stale reads

**recall@10 0.9884**, p50 4.49 ms, p99 8.01 ms, max 9.6 ms, 1573 QPS (8 threads)

## unfiltered, linearizable reads

**recall@10 0.9884**, p50 7.84 ms, p99 11.83 ms, max 14.2 ms, 995 QPS (8 threads)

## flag_1 filter (≈1%), stale reads

**recall@10 0.9922**, p50 1.50 ms, p99 2.68 ms, max 3.6 ms, 5101 QPS (8 threads)

## flag_1 filter (≈1%), linearizable reads

**recall@10 0.9922**, p50 2.61 ms, p99 3.91 ms, max 4.5 ms, 2975 QPS (8 threads)

## Takedowns

Visible on all nodes under read-your-writes: p50 77.5 ms, p99 133.6 ms, max 143.1 ms (200 takedowns)

