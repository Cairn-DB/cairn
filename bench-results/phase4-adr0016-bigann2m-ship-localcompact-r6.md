# Scale benchmark through the cluster: Bigann, 2000000 rows

- Date: 2026-09-25
- Cluster: 3 nodes (processes on one machine, every node holds every shard); 2000000 × 128-d BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 2000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest throughput | 10220 docs/s (196 s for rows 0..2000000) |
| upsert batch latency p50 / p99 / max | 287 / 1990 / 6325 ms |
| background builds settled (30s without segment change) | 216 s after ingest |
| node 1 after settling | 4 shards, segments per shard [3, 3, 3, 3], live docs 2000000 |
| node 1 memory: RSS after ingest / settled, peak | 2.0 GB / 2.1 GB, 2.4 GB |
| node 2 memory: RSS after ingest / settled, peak | 2.1 GB / 2.1 GB, 2.4 GB |
| node 3 memory: RSS after ingest / settled, peak | 1.9 GB / 2.1 GB, 2.3 GB |

## unfiltered, stale reads

**recall@10 0.9886**, p50 3.18 ms, p99 5.80 ms, max 8.2 ms, 2330 QPS (8 threads)

## unfiltered, linearizable reads

**recall@10 0.9883**, p50 5.16 ms, p99 7.53 ms, max 9.3 ms, 1500 QPS (8 threads)

## flag_1 filter (≈1%), stale reads

**recall@10 0.9923**, p50 1.37 ms, p99 2.79 ms, max 4.4 ms, 5322 QPS (8 threads)

## flag_1 filter (≈1%), linearizable reads

**recall@10 0.9923**, p50 2.60 ms, p99 3.82 ms, max 4.5 ms, 2989 QPS (8 threads)

## Takedowns

Visible on all nodes under read-your-writes: p50 76.5 ms, p99 116.2 ms, max 118.2 ms (200 takedowns)

