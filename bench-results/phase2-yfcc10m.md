# YFCC-10M filtered track sweep (Big-ANN NeurIPS'23, CC BY 4.0)

- Date: 2026-09-22
- Base rows: 10000000 (prefix), queries: 10000 (prefix of the public 100K), dims: 192 (uint8 as f32), k = 10
- Segments: 10 × up to 1000000 rows; HNSW M = 16, efConstruction = 100, search ef = 128; SQ8 + exact rerank
- Build: 456.5 s wall with one thread per segment (2191 rows/s/thread)
- Ground truth: official GT.public.ibin
- Kernels: avx512

## Strategy: adaptive

Strategy chosen per query (last segment): scan 8278 / hnsw 1722 / hnsw+2hop 0

| rows passing (all segments) | queries | recall@10 | p50 µs | p99 µs |
|---|---|---|---|---|
| <1k | 2453 | 1.0000 | 424 | 3539 |
| 1k-10k | 2277 | 1.0000 | 909 | 4026 |
| 10k-100k | 2386 | 1.0000 | 2654 | 5452 |
| 100k-1M | 2334 | 0.9983 | 9757 | 16515 |
| >=1M | 550 | 0.9969 | 9118 | 12067 |
| **all** | 10000 | **0.9994** | | QPS (1 thread): 258 |

## Strategy: scan

| rows passing (all segments) | queries | recall@10 | p50 µs | p99 µs |
|---|---|---|---|---|
| <1k | 2453 | 1.0000 | 417 | 3506 |
| 1k-10k | 2277 | 1.0000 | 904 | 3992 |
| 10k-100k | 2386 | 1.0000 | 2646 | 5439 |
| 100k-1M | 2334 | 1.0000 | 16433 | 44270 |
| >=1M | 550 | 1.0000 | 59900 | 82248 |
| **all** | 10000 | **1.0000** | | QPS (1 thread): 112 |

## Strategy: hnsw (capped 8192 visits)

| rows passing (all segments) | queries | recall@10 | p50 µs | p99 µs |
|---|---|---|---|---|
| <1k | 2453 | 0.6411 | 9830 | 17285 |
| 1k-10k | 2277 | 0.8910 | 10068 | 17416 |
| 10k-100k | 2386 | 0.9824 | 9880 | 16810 |
| 100k-1M | 2334 | 0.9952 | 9691 | 15450 |
| >=1M | 550 | 0.9969 | 9093 | 12018 |
| **all** | 10000 | **0.8816** | | QPS (1 thread): 97 |

## Strategy: hnsw+2hop (capped)

| rows passing (all segments) | queries | recall@10 | p50 µs | p99 µs |
|---|---|---|---|---|
| <1k | 2453 | 0.5754 | 7647 | 11805 |
| 1k-10k | 2277 | 0.7939 | 7999 | 11944 |
| 10k-100k | 2386 | 0.9291 | 7725 | 11583 |
| 100k-1M | 2334 | 0.9782 | 7836 | 10674 |
| >=1M | 550 | 0.9845 | 8356 | 10666 |
| **all** | 10000 | **0.8261** | | QPS (1 thread): 123 |


## Reading (2026-09-22)

- Ground truth is the track's official `GT.public.ibin`; 10,000 of the public 100K queries.
- **Adaptive: recall@10 = 0.9994**, p99 ≤ 16.5 ms in every selectivity bucket, single thread over
  ten segments. SPEC section 8 asks for recall > 95% below 5% selectivity and p99 < 100 ms for the
  hybrid query: both hold with margin on this track (vector + tag filter, no text leg).
- 83% of queries end in the scan path; the graph path only serves filters passing more than 5% of a
  segment. Forced graph search is poor for small filters (0.64 recall below 1k rows) exactly as on
  SIFT1M, and two-hop is worse again; the policy is confirmed.
- Latency scales with the number of segments (each query touches all ten): 100k–1M-row filters
  cost ~10 ms because every segment scans. Cutting that is a Phase 4 item: per-core parallel
  segments, and a masked contiguous scan instead of gathering rows.
- Throughput: 258 QPS on one thread. The per-core design multiplies this by cores; not measured.
- Build: 456 s wall for 10M rows with ten threads (2,191 rows/s/thread at 192-d; the 128-d SIFT
  build ran 5,040 rows/s). The uint8 vectors were widened to f32 (7.7 GB); a native u8 path would
  save memory, not time.
