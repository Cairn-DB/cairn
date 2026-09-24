# Scale benchmark through the cluster: Bigann, 2000000 rows

- Date: 2026-09-24
- Cluster: 3 nodes (processes on one machine, every node holds every shard); 2000000 × 128-d BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 2000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest throughput | 16784 docs/s (119 s for rows 0..2000000) |
| upsert batch latency p50 / p99 / max | 243 / 766 / 2275 ms |
| background builds settled (30s without segment change) | 83 s after ingest |
| node 1 after settling | 4 shards, segments per shard [5, 5, 5, 5], live docs 2000000 |
| node 1 memory: RSS after ingest / settled, peak | 2.3 GB / 2.4 GB, 2.6 GB |
| node 2 memory: RSS after ingest / settled, peak | 2.1 GB / 2.1 GB, 2.3 GB |
| node 3 memory: RSS after ingest / settled, peak | 2.3 GB / 2.3 GB, 2.4 GB |

## unfiltered, stale reads

**recall@10 0.9883**, p50 6.48 ms, p99 11.23 ms, max 14.6 ms, 1131 QPS (8 threads)

## unfiltered, linearizable reads

**recall@10 0.9883**, p50 11.03 ms, p99 15.79 ms, max 19.5 ms, 710 QPS (8 threads)

## flag_1 filter (≈1%), stale reads

**recall@10 0.9921**, p50 1.51 ms, p99 2.69 ms, max 3.5 ms, 4838 QPS (8 threads)

## flag_1 filter (≈1%), linearizable reads

**recall@10 0.9921**, p50 2.67 ms, p99 3.93 ms, max 5.0 ms, 2914 QPS (8 threads)

## Takedowns

Visible on all nodes under read-your-writes: p50 76.7 ms, p99 122.2 ms, max 129.0 ms (500 takedowns)

