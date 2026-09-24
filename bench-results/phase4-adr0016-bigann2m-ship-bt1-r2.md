# Scale benchmark through the cluster: Bigann, 2000000 rows

- Date: 2026-09-24
- Cluster: 3 nodes (processes on one machine, every node holds every shard); 2000000 × 128-d BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 2000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest throughput | 13642 docs/s (147 s for rows 0..2000000) |
| upsert batch latency p50 / p99 / max | 247 / 1337 / 19545 ms |
| background builds settled (30s without segment change) | 45 s after ingest |
| node 1 after settling | 4 shards, segments per shard [4, 2, 2, 5], live docs 2000000 |
| node 1 memory: RSS after ingest / settled, peak | 2.4 GB / 2.4 GB, 2.5 GB |
| node 2 memory: RSS after ingest / settled, peak | 2.2 GB / 2.5 GB, 2.5 GB |
| node 3 memory: RSS after ingest / settled, peak | 2.5 GB / 2.7 GB, 2.7 GB |

## unfiltered, stale reads

**recall@10 0.9906**, p50 23.22 ms, p99 1207.96 ms, max 2146.4 ms, 158 QPS (8 threads)

## unfiltered, linearizable reads

**recall@10 0.9899**, p50 31.39 ms, p99 167.25 ms, max 1616.9 ms, 162 QPS (8 threads)

## flag_1 filter (≈1%), stale reads

**recall@10 0.9924**, p50 3.50 ms, p99 5.79 ms, max 248.8 ms, 1981 QPS (8 threads)

## flag_1 filter (≈1%), linearizable reads

**recall@10 0.9925**, p50 5.21 ms, p99 7.76 ms, max 9.9 ms, 1452 QPS (8 threads)

## Takedowns

Visible on all nodes under read-your-writes: p50 94.2 ms, p99 126.1 ms, max 127.9 ms (200 takedowns)

