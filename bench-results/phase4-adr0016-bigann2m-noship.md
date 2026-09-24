# Scale benchmark through the cluster: Bigann, 2000000 rows

- Date: 2026-09-24
- Cluster: 3 nodes (processes on one machine, every node holds every shard); 2000000 × 128-d BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 2000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest throughput | 11349 docs/s (176 s for rows 0..2000000) |
| upsert batch latency p50 / p99 / max | 245 / 796 / 51103 ms |
| background builds settled (30s without segment change) | 133 s after ingest |
| node 1 after settling | 4 shards, segments per shard [4, 4, 4, 4], live docs 2000000 |
| node 1 memory: RSS after ingest / settled, peak | 3.0 GB / 2.9 GB, 3.1 GB |
| node 2 memory: RSS after ingest / settled, peak | 2.9 GB / 2.7 GB, 3.1 GB |
| node 3 memory: RSS after ingest / settled, peak | 3.0 GB / 3.0 GB, 3.2 GB |

## unfiltered, stale reads

**recall@10 0.9881**, p50 4.31 ms, p99 7.78 ms, max 62.3 ms, 1700 QPS (8 threads)

## unfiltered, linearizable reads

**recall@10 0.9881**, p50 6.77 ms, p99 10.08 ms, max 13.0 ms, 1153 QPS (8 threads)

## flag_1 filter (≈1%), stale reads

**recall@10 0.9924**, p50 1.52 ms, p99 3.39 ms, max 5.2 ms, 4761 QPS (8 threads)

## flag_1 filter (≈1%), linearizable reads

**recall@10 0.9924**, p50 2.70 ms, p99 4.67 ms, max 5.6 ms, 2895 QPS (8 threads)

## Takedowns

Visible on all nodes under read-your-writes: p50 52.0 ms, p99 102.0 ms, max 112.5 ms (100 takedowns)

