# Scale benchmark through the cluster: Bigann, 2000000 rows

- Date: 2026-09-24
- Cluster: 3 nodes (processes on one machine, every node holds every shard); 2000000 × 128-d BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 2000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest throughput | 14862 docs/s (135 s for rows 0..2000000) |
| upsert batch latency p50 / p99 / max | 244 / 980 / 6728 ms |
| background builds settled (30s without segment change) | 187 s after ingest |
| node 1 after settling | 4 shards, segments per shard [4, 4, 3, 4], live docs 2000000 |
| node 1 memory: RSS after ingest / settled, peak | 3.0 GB / 3.1 GB, 3.4 GB |
| node 2 memory: RSS after ingest / settled, peak | 2.9 GB / 3.2 GB, 3.4 GB |
| node 3 memory: RSS after ingest / settled, peak | 3.2 GB / 3.3 GB, 3.6 GB |

## unfiltered, stale reads

**recall@10 0.9884**, p50 5.98 ms, p99 10.74 ms, max 40.7 ms, 1212 QPS (8 threads)

## unfiltered, linearizable reads

**recall@10 0.9884**, p50 9.27 ms, p99 14.88 ms, max 17.7 ms, 811 QPS (8 threads)

## flag_1 filter (≈1%), stale reads

**recall@10 0.9915**, p50 1.50 ms, p99 2.81 ms, max 3.6 ms, 4837 QPS (8 threads)

## flag_1 filter (≈1%), linearizable reads

**recall@10 0.9915**, p50 2.69 ms, p99 3.91 ms, max 4.7 ms, 2957 QPS (8 threads)

## Takedowns

Visible on all nodes under read-your-writes: p50 70.1 ms, p99 143.1 ms, max 188.0 ms (500 takedowns)

