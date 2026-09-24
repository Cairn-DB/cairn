# Scale benchmark through the cluster: Bigann, 2000000 rows

- Date: 2026-09-24
- Cluster: 3 nodes (processes on one machine, every node holds every shard); 2000000 × 128-d BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 2000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest throughput | 16920 docs/s (118 s for rows 0..2000000) |
| upsert batch latency p50 / p99 / max | 242 / 803 / 1834 ms |
| background builds settled (30s without segment change) | 67 s after ingest |
| node 1 after settling | 4 shards, segments per shard [5, 5, 5, 5], live docs 2000000 |
| node 1 memory: RSS after ingest / settled, peak | 2.0 GB / 2.1 GB, 2.2 GB |
| node 2 memory: RSS after ingest / settled, peak | 2.0 GB / 2.1 GB, 2.1 GB |
| node 3 memory: RSS after ingest / settled, peak | 2.3 GB / 2.2 GB, 2.3 GB |

## unfiltered, stale reads

**recall@10 0.9889**, p50 7.34 ms, p99 9.81 ms, max 13.5 ms, 1038 QPS (8 threads)

## unfiltered, linearizable reads

**recall@10 0.9889**, p50 11.35 ms, p99 15.80 ms, max 19.4 ms, 696 QPS (8 threads)

## flag_1 filter (≈1%), stale reads

**recall@10 0.9926**, p50 1.55 ms, p99 2.82 ms, max 4.3 ms, 4773 QPS (8 threads)

## flag_1 filter (≈1%), linearizable reads

**recall@10 0.9926**, p50 2.66 ms, p99 3.81 ms, max 4.6 ms, 2950 QPS (8 threads)

## Takedowns

Visible on all nodes under read-your-writes: p50 88.3 ms, p99 128.8 ms, max 144.8 ms (500 takedowns)

