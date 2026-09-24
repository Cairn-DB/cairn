# Scale benchmark through the cluster: Bigann, 2000000 rows

- Date: 2026-09-24
- Cluster: 3 nodes (processes on one machine, every node holds every shard); 2000000 × 128-d BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 2000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest throughput | 10459 docs/s (191 s for rows 0..2000000) |
| upsert batch latency p50 / p99 / max | 276 / 2156 / 7755 ms |
| background builds settled (30s without segment change) | 585 s after ingest |
| node 1 after settling | 4 shards, segments per shard [3, 3, 3, 3], live docs 2000000 |
| node 1 memory: RSS after ingest / settled, peak | 1.9 GB / 2.0 GB, 2.4 GB |
| node 2 memory: RSS after ingest / settled, peak | 1.9 GB / 1.9 GB, 2.3 GB |
| node 3 memory: RSS after ingest / settled, peak | 1.9 GB / 1.9 GB, 2.4 GB |

## unfiltered, stale reads

**recall@10 0.9888**, p50 3.28 ms, p99 6.09 ms, max 7.9 ms, 2240 QPS (8 threads)

## unfiltered, linearizable reads

**recall@10 0.9888**, p50 5.27 ms, p99 7.77 ms, max 9.6 ms, 1477 QPS (8 threads)

## flag_1 filter (≈1%), stale reads

**recall@10 0.9922**, p50 1.46 ms, p99 2.70 ms, max 3.8 ms, 5083 QPS (8 threads)

## flag_1 filter (≈1%), linearizable reads

**recall@10 0.9922**, p50 2.44 ms, p99 3.44 ms, max 4.6 ms, 3219 QPS (8 threads)

## Takedowns

Visible on all nodes under read-your-writes: p50 84.8 ms, p99 213.9 ms, max 232.0 ms (200 takedowns)

