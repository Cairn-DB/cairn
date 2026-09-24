# Scale benchmark through the cluster: Bigann, 2000000 rows

- Date: 2026-09-24
- Cluster: 3 nodes (processes on one machine, every node holds every shard); 2000000 × 128-d BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 2000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest throughput | 8560 docs/s (234 s for rows 0..2000000) |
| upsert batch latency p50 / p99 / max | 305 / 2944 / 8116 ms |
| background builds settled (30s without segment change) | 521 s after ingest |
| node 1 after settling | 4 shards, segments per shard [3, 3, 3, 3], live docs 2000000 |
| node 1 memory: RSS after ingest / settled, peak | 2.1 GB / 2.2 GB, 2.6 GB |
| node 2 memory: RSS after ingest / settled, peak | 2.1 GB / 2.1 GB, 2.5 GB |
| node 3 memory: RSS after ingest / settled, peak | 2.1 GB / 2.1 GB, 2.4 GB |

## unfiltered, stale reads

**recall@10 0.9882**, p50 3.27 ms, p99 6.05 ms, max 7.9 ms, 2240 QPS (8 threads)

## unfiltered, linearizable reads

**recall@10 0.9882**, p50 5.34 ms, p99 7.85 ms, max 9.2 ms, 1474 QPS (8 threads)

## flag_1 filter (≈1%), stale reads

**recall@10 0.9930**, p50 1.45 ms, p99 2.75 ms, max 4.5 ms, 5063 QPS (8 threads)

## flag_1 filter (≈1%), linearizable reads

**recall@10 0.9930**, p50 2.47 ms, p99 3.55 ms, max 4.9 ms, 3159 QPS (8 threads)

## Takedowns

Visible on all nodes under read-your-writes: p50 61.5 ms, p99 111.0 ms, max 123.8 ms (200 takedowns)

