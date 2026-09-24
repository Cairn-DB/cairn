# Scale benchmark through the cluster: Bigann, 2000000 rows

- Date: 2026-09-25
- Cluster: 3 nodes (processes on one machine, every node holds every shard); 2000000 × 128-d BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 2000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest throughput | 10834 docs/s (185 s for rows 0..2000000) |
| upsert batch latency p50 / p99 / max | 274 / 2345 / 5885 ms |
| background builds settled (30s without segment change) | 230 s after ingest |
| node 1 after settling | 4 shards, segments per shard [3, 3, 3, 3], live docs 2000000 |
| node 1 memory: RSS after ingest / settled, peak | 2.0 GB / 2.0 GB, 2.3 GB |
| node 2 memory: RSS after ingest / settled, peak | 1.9 GB / 2.0 GB, 2.3 GB |
| node 3 memory: RSS after ingest / settled, peak | 1.9 GB / 2.2 GB, 2.5 GB |

## unfiltered, stale reads

**recall@10 0.9887**, p50 3.18 ms, p99 5.71 ms, max 6.7 ms, 2313 QPS (8 threads)

## unfiltered, linearizable reads

**recall@10 0.9880**, p50 5.19 ms, p99 7.33 ms, max 10.7 ms, 1500 QPS (8 threads)

## flag_1 filter (≈1%), stale reads

**recall@10 0.9924**, p50 1.39 ms, p99 2.93 ms, max 3.9 ms, 5021 QPS (8 threads)

## flag_1 filter (≈1%), linearizable reads

**recall@10 0.9921**, p50 2.43 ms, p99 3.56 ms, max 4.1 ms, 3230 QPS (8 threads)

## Takedowns

Visible on all nodes under read-your-writes: p50 66.2 ms, p99 109.4 ms, max 122.0 ms (200 takedowns)

