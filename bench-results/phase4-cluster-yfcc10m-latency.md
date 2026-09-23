# Scale benchmark through the cluster: Yfcc, 10000000 rows

- Date: 2026-09-23
- Cluster: 3 nodes (processes on one machine, every node holds every shard); 10000000 × 192-d YFCC-10M (Big-ANN filtered track, CC BY 4.0), tags as a Set field; official GT.public.ibin
- Ingest: 8 writer threads, batches of 500 (acknowledged after Raft commit on a majority); queries: 10000 from 8 client threads, k = 10, ef = 128

| metric | value |
|---|---|
| ingest | skipped (data already loaded) |
| background builds settled (30s without segment change) | 30 s after ingest |
| node 1 after settling | 8 shards, segments per shard [2, 2, 5, 5, 2, 2, 2, 2], live docs 9999800 |
| node 1 memory: RSS after ingest / settled, peak | 12.4 GB / 12.4 GB, 14.4 GB |
| node 2 memory: RSS after ingest / settled, peak | 12.5 GB / 12.5 GB, 14.4 GB |
| node 3 memory: RSS after ingest / settled, peak | 12.8 GB / 12.8 GB, 14.6 GB |

## tag filter (each query's own tags), stale reads

**recall@10 0.9879**, p50 10.13 ms, p99 34.24 ms, max 53.9 ms, 605 QPS (8 threads)

| rows passing | queries | recall@10 | p50 ms | p99 ms |
|---|---|---|---|---|
| <1k | 2453 | 0.9917 | 6.26 | 30.20 |
| 1k-10k | 2277 | 0.9887 | 7.41 | 28.84 |
| 10k-100k | 2386 | 0.9873 | 8.82 | 32.42 |
| 100k-1M | 2334 | 0.9858 | 14.61 | 38.08 |
| >=1M | 550 | 0.9795 | 17.22 | 39.65 |

## tag filter (each query's own tags), linearizable reads

**recall@10 0.9880**, p50 24.02 ms, p99 59.38 ms, max 77.5 ms, 287 QPS (8 threads)

| rows passing | queries | recall@10 | p50 ms | p99 ms |
|---|---|---|---|---|
| <1k | 2453 | 0.9917 | 21.17 | 54.36 |
| 1k-10k | 2277 | 0.9888 | 21.91 | 54.39 |
| 10k-100k | 2386 | 0.9872 | 22.35 | 54.26 |
| 100k-1M | 2334 | 0.9862 | 28.93 | 65.83 |
| >=1M | 550 | 0.9791 | 31.67 | 64.52 |

## Takedowns

Visible on all nodes under read-your-writes: p50 30.3 ms, p99 42.7 ms, max 49.1 ms (200 takedowns)

