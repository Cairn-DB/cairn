# Scale benchmark through the cluster: Bigann, 2000000 rows

- Date: 2026-09-24
- Cluster: 3 nodes (processes on one machine, every node holds every shard); 2000000 × 128-d BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 2000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest throughput | 17115 docs/s (117 s for rows 0..2000000) |
| upsert batch latency p50 / p99 / max | 243 / 785 / 2057 ms |
| background builds settled (30s without segment change) | 65 s after ingest |
| node 1 after settling | 4 shards, segments per shard [5, 5, 5, 5], live docs 2000000 |
| node 1 memory: RSS after ingest / settled, peak | 2.0 GB / 2.1 GB, 2.1 GB |
| node 2 memory: RSS after ingest / settled, peak | 2.0 GB / 2.1 GB, 2.1 GB |
| node 3 memory: RSS after ingest / settled, peak | 2.3 GB / 2.3 GB, 2.4 GB |

## unfiltered, stale reads

**recall@10 0.9887**, p50 7.89 ms, p99 13.89 ms, max 17.2 ms, 907 QPS (8 threads)

## unfiltered, linearizable reads

**recall@10 0.9887**, p50 13.58 ms, p99 18.02 ms, max 22.1 ms, 584 QPS (8 threads)

## flag_1 filter (≈1%), stale reads

**recall@10 0.9917**, p50 1.50 ms, p99 3.06 ms, max 5.7 ms, 4365 QPS (8 threads)

## flag_1 filter (≈1%), linearizable reads

**recall@10 0.9917**, p50 2.83 ms, p99 4.24 ms, max 5.1 ms, 2786 QPS (8 threads)

## Takedowns

Visible on all nodes under read-your-writes: p50 83.2 ms, p99 117.6 ms, max 159.9 ms (500 takedowns)

