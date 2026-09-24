# Scale benchmark through the cluster: Bigann, 2000000 rows

- Date: 2026-09-24
- Cluster: 3 nodes (processes on one machine, every node holds every shard); 2000000 × 128-d BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 2000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest throughput | 16718 docs/s (120 s for rows 0..2000000) |
| upsert batch latency p50 / p99 / max | 246 / 686 / 1261 ms |
| background builds settled (30s without segment change) | 103 s after ingest |
| node 1 after settling | 4 shards, segments per shard [5, 5, 4, 5], live docs 2000000 |
| node 1 memory: RSS after ingest / settled, peak | 2.1 GB / 2.2 GB, 2.4 GB |
| node 2 memory: RSS after ingest / settled, peak | 2.1 GB / 2.2 GB, 2.4 GB |
| node 3 memory: RSS after ingest / settled, peak | 2.6 GB / 2.7 GB, 2.8 GB |

## unfiltered, stale reads

**recall@10 0.9879**, p50 4.73 ms, p99 8.20 ms, max 11.3 ms, 1568 QPS (8 threads)

## unfiltered, linearizable reads

**recall@10 0.9879**, p50 7.54 ms, p99 10.55 ms, max 12.0 ms, 1021 QPS (8 threads)

## flag_1 filter (≈1%), stale reads

**recall@10 0.9918**, p50 1.53 ms, p99 2.98 ms, max 3.9 ms, 4596 QPS (8 threads)

## flag_1 filter (≈1%), linearizable reads

**recall@10 0.9918**, p50 2.64 ms, p99 4.07 ms, max 5.9 ms, 2964 QPS (8 threads)

## Takedowns

Visible on all nodes under read-your-writes: p50 62.3 ms, p99 112.9 ms, max 122.2 ms (500 takedowns)

