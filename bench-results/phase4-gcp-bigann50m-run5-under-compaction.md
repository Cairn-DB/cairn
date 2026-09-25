# Scale benchmark through the cluster: Bigann, 50000000 rows

- Date: 2026-09-25
- Cluster: 3 GCP VMs n2-highmem-8 (8 vCPU, 64 GB, pd-ssd), europe-west1-b, private network; every node holds every shard; client on an e2-standard-4; 50000000 × 128-d BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool
- Commit 3e9bf12 (ADR 0016-0023 plus the slow-leader fix e97f01e and the build-thread default). Server flags: 8 shards, 6 cores, 256 MB memtables, --sq8-only --target-segment-rows 3000000 --compaction-slots 2 (build threads 2, the new default) during ingest; after ingest the nodes were restarted on their data with --compaction-slots 4 --build-threads 2. **Queries ran while merges were still building on every node** (the settle test passed during a long merge): these latencies are under heavy build load, not a steady state. See phase4-gcp-bigann50m-run5-idle.md for the idle measurement.
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 1000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest throughput | 6839 docs/s (7311 s for rows 0..50000000) |
| upsert batch latency p50 / p99 / max | 149 / 350 / 1165010 ms |
| background builds settled (120s without segment change) | 1087 s after ingest |
| node 1 after settling | 8 shards, segments per shard [19, 12, 11, 12, 11, 11, 13, 14], live docs 50000000 |

## unfiltered, stale reads

**recall@10 0.9872**, p50 254.80 ms, p99 12650.02 ms, max 20501.8 ms, 16 QPS (8 threads)

## unfiltered, linearizable reads

**recall@10 0.9872**, p50 268.17 ms, p99 502.79 ms, max 840.4 ms, 23 QPS (8 threads)

## flag_1 filter (≈1%), stale reads

**recall@10 0.9897**, p50 136.71 ms, p99 1363.15 ms, max 2447.4 ms, 40 QPS (8 threads)

## flag_1 filter (≈1%), linearizable reads

**recall@10 0.9896**, p50 152.17 ms, p99 1163.92 ms, max 3663.7 ms, 39 QPS (8 threads)

## Takedowns

Visible on all nodes under read-your-writes: p50 66.4 ms, p99 116.0 ms, max 118.8 ms (200 takedowns)

