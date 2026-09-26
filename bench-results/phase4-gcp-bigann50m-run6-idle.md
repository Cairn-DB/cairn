# Scale benchmark through the cluster: Bigann, 50000000 rows

- Date: 2026-09-26
- Cluster: 3 GCP VMs n2-highmem-8 (8 vCPU, 64 GB, pd-ssd), europe-west1-b, private network; every node holds every shard; client on an e2-standard-4; 50000000 × 128-d BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool
- Commit 67cc4ff (ADR 0025: shard searches off the replica actor, on a bounded search pool). Ingested with --compaction-slots 2; merges then disabled (--max-segments 1000), and queries ran with nothing pending and all nodes idle (0% CPU), at 95 segments per node (6-20 per shard, about 12 on average). Queries only (--skip-ingest).
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 1000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest | skipped (data already loaded) |
| background builds settled (60s without segment change) | 60 s after ingest |
| node 1 after settling | 8 shards, segments per shard [20, 14, 6, 11, 11, 11, 9, 13], live docs 50000000 |

## unfiltered, stale reads

**recall@10 0.9859**, p50 20.94 ms, p99 33.88 ms, max 125.5 ms, 301 QPS (8 threads)

## unfiltered, linearizable reads

**recall@10 0.9859**, p50 21.74 ms, p99 32.96 ms, max 39.9 ms, 311 QPS (8 threads)

## flag_1 filter (≈1%), stale reads

**recall@10 0.9893**, p50 34.05 ms, p99 40.11 ms, max 45.5 ms, 224 QPS (8 threads)

## flag_1 filter (≈1%), linearizable reads

**recall@10 0.9893**, p50 29.16 ms, p99 40.11 ms, max 42.0 ms, 221 QPS (8 threads)

## Takedowns

Visible on all nodes under read-your-writes: p50 57.1 ms, p99 108.3 ms, max 110.0 ms (200 takedowns)

