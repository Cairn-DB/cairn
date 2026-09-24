# Scale benchmark through the cluster: Bigann, 2000000 rows

- Date: 2026-09-24
- Cluster: 3 nodes (processes on one machine, every node holds every shard); 2000000 × 128-d BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 2000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest throughput | 10188 docs/s (196 s for rows 0..2000000) |
| upsert batch latency p50 / p99 / max | 259 / 2873 / 7554 ms |
| background builds settled (30s without segment change) | 213 s after ingest |
| node 1 after settling | 4 shards, segments per shard [3, 3, 3, 3], live docs 2000000 |
| node 1 memory: RSS after ingest / settled, peak | 2.0 GB / 2.0 GB, 2.3 GB |
| node 2 memory: RSS after ingest / settled, peak | 1.8 GB / 2.0 GB, 2.3 GB |
| node 3 memory: RSS after ingest / settled, peak | 1.9 GB / 2.0 GB, 2.4 GB |

## unfiltered, stale reads

**recall@10 0.9882**, p50 3.36 ms, p99 6.32 ms, max 7.3 ms, 2216 QPS (8 threads)

## unfiltered, linearizable reads

**recall@10 0.9882**, p50 5.42 ms, p99 7.80 ms, max 9.7 ms, 1430 QPS (8 threads)

## flag_1 filter (≈1%), stale reads

**recall@10 0.9926**, p50 1.42 ms, p99 2.67 ms, max 3.8 ms, 5287 QPS (8 threads)

## flag_1 filter (≈1%), linearizable reads

**recall@10 0.9926**, p50 2.43 ms, p99 3.55 ms, max 4.5 ms, 3236 QPS (8 threads)

## Takedowns

Visible on all nodes under read-your-writes: p50 61.8 ms, p99 113.2 ms, max 118.0 ms (200 takedowns)

