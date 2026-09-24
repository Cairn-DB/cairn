# Scale benchmark through the cluster: Bigann, 2000000 rows

- Date: 2026-09-25
- Cluster: 3 nodes (processes on one machine, every node holds every shard); 2000000 × 128-d BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 2000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest throughput | 11064 docs/s (181 s for rows 0..2000000) |
| upsert batch latency p50 / p99 / max | 263 / 1557 / 7097 ms |
| background builds settled (30s without segment change) | 129 s after ingest |
| node 1 after settling | 4 shards, segments per shard [3, 3, 3, 3], live docs 2000000 |
| node 1 memory: RSS after ingest / settled, peak | 2.0 GB / 1.9 GB, 2.2 GB |
| node 2 memory: RSS after ingest / settled, peak | 1.8 GB / 1.9 GB, 2.2 GB |
| node 3 memory: RSS after ingest / settled, peak | 1.6 GB / 1.9 GB, 2.1 GB |

## unfiltered, stale reads

**recall@10 0.9883**, p50 3.32 ms, p99 6.61 ms, max 12.7 ms, 2150 QPS (8 threads)

## unfiltered, linearizable reads

**recall@10 0.9883**, p50 5.33 ms, p99 8.32 ms, max 10.2 ms, 1449 QPS (8 threads)

## flag_1 filter (≈1%), stale reads

**recall@10 0.9928**, p50 1.54 ms, p99 3.77 ms, max 5.3 ms, 4628 QPS (8 threads)

## flag_1 filter (≈1%), linearizable reads

**recall@10 0.9928**, p50 2.48 ms, p99 4.71 ms, max 5.8 ms, 3106 QPS (8 threads)

## Takedowns

Visible on all nodes under read-your-writes: p50 100.0 ms, p99 118.0 ms, max 118.0 ms (50 takedowns)

