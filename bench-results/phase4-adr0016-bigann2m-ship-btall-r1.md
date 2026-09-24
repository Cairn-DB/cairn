# Scale benchmark through the cluster: Bigann, 2000000 rows

- Date: 2026-09-24
- Cluster: 3 nodes (processes on one machine, every node holds every shard); 2000000 × 128-d BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 2000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest throughput | 11954 docs/s (167 s for rows 0..2000000) |
| upsert batch latency p50 / p99 / max | 267 / 1982 / 3175 ms |
| background builds settled (30s without segment change) | 47 s after ingest |
| node 1 after settling | 4 shards, segments per shard [5, 5, 5, 5], live docs 2000000 |
| node 1 memory: RSS after ingest / settled, peak | 1.5 GB / 1.5 GB, 1.6 GB |
| node 2 memory: RSS after ingest / settled, peak | 1.5 GB / 1.6 GB, 1.7 GB |
| node 3 memory: RSS after ingest / settled, peak | 1.6 GB / 1.7 GB, 1.8 GB |

## unfiltered, stale reads

**recall@10 0.9881**, p50 4.86 ms, p99 8.42 ms, max 10.8 ms, 1537 QPS (8 threads)

## unfiltered, linearizable reads

**recall@10 0.9881**, p50 7.59 ms, p99 11.33 ms, max 14.0 ms, 1014 QPS (8 threads)

## flag_1 filter (≈1%), stale reads

**recall@10 0.9925**, p50 1.54 ms, p99 2.94 ms, max 4.1 ms, 4639 QPS (8 threads)

## flag_1 filter (≈1%), linearizable reads

**recall@10 0.9925**, p50 2.56 ms, p99 3.88 ms, max 4.8 ms, 3020 QPS (8 threads)

## Takedowns

Visible on all nodes under read-your-writes: p50 68.0 ms, p99 121.5 ms, max 126.2 ms (200 takedowns)

