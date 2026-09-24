# Scale benchmark through the cluster: Bigann, 2000000 rows

- Date: 2026-09-24
- Cluster: 3 nodes (processes on one machine, every node holds every shard); 2000000 × 128-d BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 2000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest throughput | 16371 docs/s (122 s for rows 0..2000000) |
| upsert batch latency p50 / p99 / max | 246 / 849 / 1732 ms |
| background builds settled (30s without segment change) | 173 s after ingest |
| node 1 after settling | 4 shards, segments per shard [4, 3, 3, 4], live docs 2000000 |
| node 1 memory: RSS after ingest / settled, peak | 3.0 GB / 3.3 GB, 3.5 GB |
| node 2 memory: RSS after ingest / settled, peak | 2.9 GB / 3.1 GB, 3.4 GB |
| node 3 memory: RSS after ingest / settled, peak | 3.0 GB / 3.4 GB, 3.6 GB |

## unfiltered, stale reads

**recall@10 0.9882**, p50 3.90 ms, p99 7.52 ms, max 109.6 ms, 1790 QPS (8 threads)

## unfiltered, linearizable reads

**recall@10 0.9882**, p50 5.97 ms, p99 10.04 ms, max 12.0 ms, 1244 QPS (8 threads)

## flag_1 filter (≈1%), stale reads

**recall@10 0.9926**, p50 1.53 ms, p99 2.85 ms, max 3.2 ms, 4860 QPS (8 threads)

## flag_1 filter (≈1%), linearizable reads

**recall@10 0.9926**, p50 2.60 ms, p99 3.86 ms, max 4.8 ms, 2982 QPS (8 threads)

## Takedowns

Visible on all nodes under read-your-writes: p50 71.3 ms, p99 105.4 ms, max 124.1 ms (500 takedowns)

