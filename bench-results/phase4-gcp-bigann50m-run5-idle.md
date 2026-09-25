# Scale benchmark through the cluster: Bigann, 50000000 rows

- Date: 2026-09-25
- Cluster: 3 GCP VMs n2-highmem-8 (8 vCPU, 64 GB, pd-ssd), europe-west1-b, private network; every node holds every shard; client on an e2-standard-4; 50000000 × 128-d BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool
- Same data as phase4-gcp-bigann50m-run5-under-compaction.md. The nodes were restarted with new merges disabled (--max-segments 1000); queries ran once nothing was pending and the nodes were idle (0% CPU), with 70 segments per node (6-19 per shard, about 9 on average). Queries only (--skip-ingest).
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 1000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest | skipped (data already loaded) |
| background builds settled (60s without segment change) | 60 s after ingest |
| node 1 after settling | 8 shards, segments per shard [19, 7, 6, 7, 12, 6, 6, 7], live docs 49999800 |

## unfiltered, stale reads

**recall@10 0.9851**, p50 32.90 ms, p99 64.65 ms, max 123.2 ms, 146 QPS (8 threads)

## unfiltered, linearizable reads

**recall@10 0.9851**, p50 51.48 ms, p99 67.57 ms, max 77.0 ms, 154 QPS (8 threads)

## flag_1 filter (≈1%), stale reads

**recall@10 0.9908**, p50 45.74 ms, p99 92.67 ms, max 106.8 ms, 129 QPS (8 threads)

## flag_1 filter (≈1%), linearizable reads

**recall@10 0.9908**, p50 71.83 ms, p99 117.83 ms, max 132.9 ms, 94 QPS (8 threads)

## Takedowns

Visible on all nodes under read-your-writes: p50 86.8 ms, p99 106.8 ms, max 116.1 ms (200 takedowns)

