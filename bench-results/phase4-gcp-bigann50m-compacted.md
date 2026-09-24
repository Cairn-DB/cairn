# Scale benchmark through the cluster: Bigann, 50000000 rows

- Date: 2026-09-24
- Cluster: 3 GCP VMs n2-highmem-8 (8 vCPU, 64 GB, pd-ssd), europe-west1-b, private network; every node holds every shard; client on an e2-standard-4; 50000000 × 128-d BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool
- Same data as phase4-gcp-bigann50m.md after a post-load tiered compaction (--target-segment-rows 3000000, 6 slots): 3 segments per shard instead of 11-19. Queries only (--skip-ingest).
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 1000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest | skipped (data already loaded) |
| background builds settled (30s without segment change) | 30 s after ingest |
| node 1 after settling | 8 shards, segments per shard [3, 3, 3, 3, 3, 3, 3, 3], live docs 49999800 |

## unfiltered, stale reads

**recall@10 0.9826**, p50 24.69 ms, p99 35.72 ms, max 80.4 ms, 266 QPS (8 threads)

## unfiltered, linearizable reads

**recall@10 0.9829**, p50 35.68 ms, p99 47.55 ms, max 59.5 ms, 221 QPS (8 threads)

## flag_1 filter (≈1%), stale reads

**recall@10 0.9908**, p50 51.28 ms, p99 82.12 ms, max 91.6 ms, 151 QPS (8 threads)

## flag_1 filter (≈1%), linearizable reads

**recall@10 0.9909**, p50 80.61 ms, p99 108.07 ms, max 117.1 ms, 98 QPS (8 threads)

## Takedowns

Visible on all nodes under read-your-writes: p50 64.0 ms, p99 107.3 ms, max 110.5 ms (200 takedowns)

