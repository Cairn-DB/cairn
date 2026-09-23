# Scale benchmark through the cluster: Bigann, 50000000 rows

- Date: 2026-09-23
- Cluster: 3 nodes (processes on one machine, every node holds every shard); 50000000 × 128-d BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 2000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest throughput | 3500 docs/s (429 s for rows 48500000..50000000) |
| upsert batch latency p50 / p99 / max | 240 / 515 / 252598 ms |
| background builds settled (120s without segment change) | 290 s after ingest |
| node 1 after settling | 4 shards, segments per shard [55, 47, 45, 55], live docs 48900020 |
| node 1 memory: RSS after ingest / settled, peak | 9.7 GB / 9.7 GB, 11.9 GB |
| node 2 memory: RSS after ingest / settled, peak | 8.6 GB / 8.7 GB, 12.1 GB |
| node 3 memory: RSS after ingest / settled, peak | 9.4 GB / 9.1 GB, 12.0 GB |

## unfiltered, stale reads

**recall@10 0.9779**, p50 2759.85 ms, p99 13505.66 ms, max 47177.5 ms, 2 QPS (8 threads)

## unfiltered, linearizable reads

**recall@10 0.9778**, p50 4018.14 ms, p99 33873.20 ms, max 62881.0 ms, 1 QPS (8 threads)

## flag_1 filter (≈1%), stale reads

**recall@10 0.9802**, p50 1945.11 ms, p99 5121.25 ms, max 6232.7 ms, 4 QPS (8 threads)

## flag_1 filter (≈1%), linearizable reads

**recall@10 0.9802**, p50 482.08 ms, p99 3080.72 ms, max 6819.9 ms, 13 QPS (8 threads)

## Takedowns

Visible on all nodes under read-your-writes: p50 86.4 ms, p99 117.5 ms, max 129.7 ms (200 takedowns)

