# Scale benchmark through the cluster: Bigann, 2000000 rows

- Date: 2026-09-24
- Cluster: 3 nodes (processes on one machine, every node holds every shard); 2000000 × 128-d BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 2000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest throughput | 10845 docs/s (184 s for rows 0..2000000) |
| upsert batch latency p50 / p99 / max | 274 / 1707 / 6057 ms |
| background builds settled (30s without segment change) | 46 s after ingest |
| node 1 after settling | 4 shards, segments per shard [5, 5, 5, 5], live docs 2000000 |
| node 1 memory: RSS after ingest / settled, peak | 1.6 GB / 1.7 GB, 1.7 GB |
| node 2 memory: RSS after ingest / settled, peak | 1.6 GB / 1.6 GB, 1.7 GB |
| node 3 memory: RSS after ingest / settled, peak | 1.7 GB / 1.7 GB, 1.9 GB |

## unfiltered, stale reads

**recall@10 0.9884**, p50 4.56 ms, p99 8.04 ms, max 10.2 ms, 1586 QPS (8 threads)

## unfiltered, linearizable reads

**recall@10 0.9884**, p50 7.60 ms, p99 10.63 ms, max 14.3 ms, 1034 QPS (8 threads)

## flag_1 filter (≈1%), stale reads

**recall@10 0.9920**, p50 1.41 ms, p99 2.51 ms, max 3.0 ms, 5315 QPS (8 threads)

## flag_1 filter (≈1%), linearizable reads

**recall@10 0.9920**, p50 2.51 ms, p99 3.65 ms, max 4.5 ms, 3088 QPS (8 threads)

## Takedowns

Visible on all nodes under read-your-writes: p50 61.5 ms, p99 116.3 ms, max 116.5 ms (200 takedowns)

