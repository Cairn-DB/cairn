# Scale benchmark through the cluster: Bigann, 50000000 rows

- Date: 2026-09-24
- Cluster: 3 GCP VMs n2-highmem-8 (8 vCPU, 64 GB, pd-ssd), europe-west1-b, private network; every node holds every shard; client on an e2-standard-4; 50000000 × 128-d BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool
- Server flags: 8 shards, 6 cores, 256 MB memtables, --sq8-only --compaction-slots 4 --max-segments 64 (no compaction). Rows 0..15M were loaded by a first client run; the nodes were then restarted on their data with a fix (held-back proposals) and rows 15M..50M loaded by this run.
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 1000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest throughput | 8738 docs/s (4006 s for rows 15000000..50000000) |
| upsert batch latency p50 / p99 / max | 132 / 303 / 621697 ms |
| background builds settled (120s without segment change) | 676 s after ingest |
| node 1 after settling | 8 shards, segments per shard [19, 16, 18, 11, 15, 19, 17, 11], live docs 50000000 |

## unfiltered, stale reads

**recall@10 0.9846**, p50 87.95 ms, p99 118.03 ms, max 140.0 ms, 80 QPS (8 threads)

## unfiltered, linearizable reads

**recall@10 0.9850**, p50 114.23 ms, p99 154.40 ms, max 205.3 ms, 67 QPS (8 threads)

## flag_1 filter (≈1%), stale reads

**recall@10 0.9900**, p50 84.02 ms, p99 162.53 ms, max 186.3 ms, 77 QPS (8 threads)

## flag_1 filter (≈1%), linearizable reads

**recall@10 0.9897**, p50 116.06 ms, p99 157.02 ms, max 188.6 ms, 66 QPS (8 threads)

## Takedowns

Visible on all nodes under read-your-writes: p50 61.0 ms, p99 107.2 ms, max 111.7 ms (200 takedowns)

