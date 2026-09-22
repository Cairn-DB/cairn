# SIFT1M filtered-search sweep

- Date: 2026-09-22
- Base vectors: 1000000 (prefix of sift_base.fvecs), queries: 1000, dims: 128, k = 10
- Index: HNSW M = 16, efConstruction = 100, SQ8 candidates + exact rerank (4k)
- Kernels: avx512
- Hardware: see docs/progress.md (Ryzen 7 8845HS, single thread)

## Build

- HNSW build (single thread, f32 distances): 198.4 s for 1000000 rows = 5040 rows/s

## Unfiltered recall@10 vs ef (HNSW + SQ8 + rerank)

| ef | recall@10 | p50 µs | p99 µs | QPS (1 thread) |
|---|---|---|---|---|
| 32 | 0.9093 | 71 | 101 | 13744 |
| 64 | 0.9502 | 96 | 119 | 10443 |
| 128 | 0.9773 | 163 | 225 | 6197 |
| 256 | 0.9874 | 283 | 348 | 3600 |

## Filtered recall@10 (ef = 128 for graph paths)

Selectivity is the fraction of rows passing the filter. `adaptive` is the strategy the index picks
(scan below 20000 rows or 10% of the segment; two-hop below 30%).

| correlation | selectivity | rows passing | strategy | recall@10 | p50 µs | p99 µs |
|---|---|---|---|---|---|---|
| Random | 50% | 500388 | adaptive→Hnsw | 0.9843 | 289 | 388 |
| Random | 50% | 500388 | scan | 0.9887 | 13697 | 15761 |
| Random | 50% | 500388 | hnsw (capped 8192 visits) | 0.9843 | 293 | 505 |
| Random | 50% | 500388 | hnsw+2hop (capped 8192 visits) | 0.9721 | 533 | 760 |
| Random | 50% | 500388 | hnsw (unlimited) | 0.9843 | 293 | 383 |
| Random | 10% | 99738 | adaptive→Scan | 0.9907 | 3138 | 4202 |
| Random | 10% | 99738 | scan | 0.9907 | 3115 | 3979 |
| Random | 10% | 99738 | hnsw (capped 8192 visits) | 0.9865 | 646 | 1134 |
| Random | 10% | 99738 | hnsw+2hop (capped 8192 visits) | 0.9294 | 555 | 827 |
| Random | 10% | 99738 | hnsw (unlimited) | 0.9900 | 1244 | 2226 |
| Random | 1% | 9915 | adaptive→Scan | 0.9931 | 231 | 319 |
| Random | 1% | 9915 | scan | 0.9931 | 247 | 626 |
| Random | 1% | 9915 | hnsw (capped 8192 visits) | 0.9372 | 701 | 1280 |
| Random | 1% | 9915 | hnsw+2hop (capped 8192 visits) | 0.7700 | 566 | 835 |
| Random | 1% | 9915 | hnsw (unlimited) | 0.9930 | 7991 | 10863 |
| Random | 0.1% | 924 | adaptive→Scan | 0.9957 | 37 | 49 |
| Random | 0.1% | 924 | scan | 0.9957 | 37 | 43 |
| Random | 0.1% | 924 | hnsw (capped 8192 visits) | 0.4912 | 666 | 1303 |
| Random | 0.1% | 924 | hnsw+2hop (capped 8192 visits) | 0.3798 | 542 | 873 |
| Random | 0.1% | 924 | hnsw (unlimited) | 0.9957 | 65962 | 82182 |
| Clustered | 50% | 504640 | adaptive→Hnsw | 0.9846 | 292 | 899 |
| Clustered | 50% | 504640 | scan | 0.9890 | 13165 | 14967 |
| Clustered | 50% | 504640 | hnsw (capped 8192 visits) | 0.9846 | 292 | 805 |
| Clustered | 50% | 504640 | hnsw+2hop (capped 8192 visits) | 0.9669 | 520 | 839 |
| Clustered | 50% | 504640 | hnsw (unlimited) | 0.9846 | 292 | 1021 |
| Clustered | 10% | 105989 | adaptive→HnswTwoHop | 0.8493 | 553 | 930 |
| Clustered | 10% | 105989 | scan | 0.9889 | 3113 | 4997 |
| Clustered | 10% | 105989 | hnsw (capped 8192 visits) | 0.9689 | 629 | 1232 |
| Clustered | 10% | 105989 | hnsw+2hop (capped 8192 visits) | 0.8493 | 544 | 836 |
| Clustered | 10% | 105989 | hnsw (unlimited) | 0.9884 | 1331 | 4854 |
| Clustered | 1% | 10195 | adaptive→Scan | 0.9890 | 215 | 355 |
| Clustered | 1% | 10195 | scan | 0.9890 | 214 | 270 |
| Clustered | 1% | 10195 | hnsw (capped 8192 visits) | 0.4477 | 670 | 1335 |
| Clustered | 1% | 10195 | hnsw+2hop (capped 8192 visits) | 0.3568 | 558 | 943 |
| Clustered | 1% | 10195 | hnsw (unlimited) | 0.9887 | 22249 | 89784 |
| Clustered | 0.1% | 665 | adaptive→Scan | 0.9904 | 32 | 37 |
| Clustered | 0.1% | 665 | scan | 0.9904 | 32 | 94 |
| Clustered | 0.1% | 665 | hnsw (capped 8192 visits) | 0.0630 | 675 | 1343 |
| Clustered | 0.1% | 665 | hnsw+2hop (capped 8192 visits) | 0.0547 | 557 | 831 |
| Clustered | 0.1% | 665 | hnsw (unlimited) | 0.9904 | 188088 | 289145 |

## Reading (2026-09-22)

- **Exact scan below 5%**: recall 0.99 (the 1% gap to 1.0 is the SQ8 candidate pass with a 4k
  rerank; `exact = true` gives 1.0) at 231 µs for 1% and 37 µs for 0.1%, whatever the
  correlation. This is the regime the spec cares about (recall@10 > 95% below 5%), and it is met
  with margin: 5% of a 1M segment is ~50k rows, about 1.5 ms.
- **Uncapped graph search collapses under selective filters**: 66 ms (random 0.1%) and 188 ms
  (clustered 0.1%) per query, because the beam never fills and the traversal touches the whole
  graph. ADR 0003's prediction holds; the visit cap is mandatory.
- **Capped graph search at 10%**: recall 0.987 (random) / 0.969 (clustered) at ~640 µs vs 3.1 ms
  for the scan; above 5% the graph wins on latency while staying above 95%.
- **Two-hop expansion (ACORN-1 style on a plain HNSW graph) lost everywhere**: lower recall and
  higher latency than plain graph search under the same cap. It stays available when forced but
  is out of the adaptive policy. ACORN's gains rely on a denser graph built for it; not pursued.
- **Defaults changed after this run**: scan up to 5% or 50k rows (was 10% / 20k), two-hop off,
  8192-visit cap on graph paths by default. The "adaptive" rows above were produced with the
  previous thresholds; the YFCC-10M sweep uses the new ones.
- **Scan cost**: 31 ns/row at 10% vs 6–8 ns/row in the kernel benchmark: gathering filtered rows
  dominates. A masked contiguous scan for dense filters is the next optimization.
- **Build**: 5,040 rows/s single-threaded (M = 16, efConstruction = 100): 198 s per 1M-row
  segment. Segment build will run chunked on a core (incremental builder) and, in Phase 4, one
  segment per core.
