# Scale benchmark through the cluster: Bigann, 2000000 rows

- Date: 2026-09-24
- Cluster: 3 nodes (processes on one machine, every node holds every shard); 2000000 × 128-d BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 2000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest throughput | 8881 docs/s (225 s for rows 0..2000000) |
| upsert batch latency p50 / p99 / max | 289 / 3190 / 8288 ms |
| background builds settled (30s without segment change) | 251 s after ingest |
| node 1 after settling | 4 shards, segments per shard [4, 4, 4, 4], live docs 2000000 |
| node 1 memory: RSS after ingest / settled, peak | 1.9 GB / 2.2 GB, 2.4 GB |
| node 2 memory: RSS after ingest / settled, peak | 1.8 GB / 2.1 GB, 2.2 GB |
| node 3 memory: RSS after ingest / settled, peak | 1.8 GB / 2.1 GB, 2.2 GB |

## unfiltered, stale reads

**recall@10 0.9875**, p50 24.43 ms, p99 79.50 ms, max 2518.7 ms, 187 QPS (8 threads)

## unfiltered, linearizable reads

**recall@10 0.9876**, p50 71.76 ms, p99 1109.39 ms, max 2885.7 ms, 79 QPS (8 threads)

## flag_1 filter (≈1%), stale reads

**recall@10 0.9923**, p50 17.43 ms, p99 47.48 ms, max 63.0 ms, 365 QPS (8 threads)

## flag_1 filter (≈1%), linearizable reads

**recall@10 0.9923**, p50 20.99 ms, p99 71.96 ms, max 2105.5 ms, 236 QPS (8 threads)

## Takedowns

Visible on all nodes under read-your-writes: p50 95.2 ms, p99 1185.9 ms, max 1646.3 ms (200 takedowns)

