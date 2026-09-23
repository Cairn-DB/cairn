# Scale benchmark through the cluster: Bigann, 20000000 rows

- Date: 2026-09-23
- Cluster: 3 nodes (processes on one machine, every node holds every shard); 20000000 × 128-d BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 5000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest | skipped (data already loaded) |
| background builds settled (10s without segment change) | 10 s after ingest |
| node 1 after settling | 8 shards, segments per shard [8, 4, 8, 3, 7, 3, 4, 3], live docs 19999800 |
| node 1 memory: RSS after ingest / settled, peak | 7.8 GB / 7.8 GB, 8.6 GB |
| node 2 memory: RSS after ingest / settled, peak | 9.2 GB / 9.2 GB, 10.8 GB |
| node 3 memory: RSS after ingest / settled, peak | 8.1 GB / 8.1 GB, 9.4 GB |

## unfiltered, stale reads

**recall@10 0.9856**, p50 12.89 ms, p99 19.68 ms, max 28.2 ms, 573 QPS (8 threads)

## unfiltered, linearizable reads

**recall@10 0.9855**, p50 24.56 ms, p99 48.63 ms, max 61.0 ms, 271 QPS (8 threads)

## flag_1 filter (≈1%), stale reads

**recall@10 0.9904**, p50 12.68 ms, p99 22.00 ms, max 29.3 ms, 565 QPS (8 threads)

## flag_1 filter (≈1%), linearizable reads

**recall@10 0.9903**, p50 26.36 ms, p99 42.53 ms, max 54.6 ms, 283 QPS (8 threads)

## Takedowns

Visible on all nodes under read-your-writes: p50 28.7 ms, p99 39.0 ms, max 41.2 ms (200 takedowns)

