# Scale benchmark through the cluster: Yfcc, 10000000 rows

- Date: 2026-09-23
- Cluster: 3 nodes (processes on one machine, every node holds every shard); 10000000 × 192-d YFCC-10M (Big-ANN filtered track, CC BY 4.0), tags as a Set field; official GT.public.ibin
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 10000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest throughput | 6741 docs/s (1483 s for 10000000 rows) |
| upsert batch latency p50 / p99 / max | 430 / 2607 / 9320 ms |
| background builds settled (60s without segment change) | 60 s after ingest |
| node 1 after settling | 8 shards, segments per shard [16, 16, 16, 16, 16, 16, 16, 16], live docs 10000000 |
| node 1 memory: RSS after ingest / settled, peak | 9.0 GB / 9.0 GB, 9.4 GB |
| node 2 memory: RSS after ingest / settled, peak | 9.1 GB / 9.1 GB, 9.5 GB |
| node 3 memory: RSS after ingest / settled, peak | 9.2 GB / 9.2 GB, 9.6 GB |

## tag filter (each query's own tags), stale reads

**recall@10 0.9893**, p50 28.97 ms, p99 79.56 ms, max 1813.0 ms, 243 QPS (8 threads)

| rows passing | queries | recall@10 | p50 ms | p99 ms |
|---|---|---|---|---|
| <1k | 2453 | 0.9926 | 25.99 | 76.15 |
| 1k-10k | 2277 | 0.9906 | 26.46 | 77.33 |
| 10k-100k | 2386 | 0.9878 | 27.62 | 77.07 |
| 100k-1M | 2334 | 0.9871 | 31.98 | 81.53 |
| >=1M | 550 | 0.9849 | 43.48 | 85.85 |

## tag filter (each query's own tags), linearizable reads

**recall@10 0.9893**, p50 47.68 ms, p99 117.18 ms, max 2271.2 ms, 148 QPS (8 threads)

| rows passing | queries | recall@10 | p50 ms | p99 ms |
|---|---|---|---|---|
| <1k | 2453 | 0.9926 | 39.81 | 110.43 |
| 1k-10k | 2277 | 0.9906 | 41.03 | 111.21 |
| 10k-100k | 2386 | 0.9878 | 42.73 | 111.80 |
| 100k-1M | 2334 | 0.9871 | 56.92 | 117.05 |
| >=1M | 550 | 0.9849 | 89.85 | 138.28 |

## Takedowns

Visible on all nodes under read-your-writes: p50 29.8 ms, p99 38.9 ms, max 39.4 ms (200 takedowns)

