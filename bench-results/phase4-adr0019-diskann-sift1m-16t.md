# Disk-resident index (Vamana + PQ, ADR 0014): sift, one segment

- Date: 2026-09-24
- Rows: 1000000 × 128-d, queries: 1000, k = 10, single-thread queries; R = 48, L_build = 96, alpha = 1.2, passes = 2
- Build: 116.3 s (8598 rows/s, 16 build threads, includes PQ training)
- RAM: 32.1 MB of PQ codes + codebooks (32 B/row); disk: 819.2 MB of node blocks (819 B/row)
- Cold: the index file's pages are dropped from the page cache before every query (`posix_fadvise(DONTNEED)`); warm: after one full pass

| case | L | recall@10 | warm p50 µs | warm p99 µs | cold p50 µs | cold p99 µs |
|---|---|---|---|---|---|---|
| unfiltered, graph | 32 | 0.9108 | 178 | 271 | 5099 | 7707 |
| unfiltered, graph | 64 | 0.9621 | 298 | 423 | 8383 | 13695 |
| unfiltered, graph | 100 | 0.9802 | 403 | 600 | 12255 | 19119 |
| unfiltered, graph | 128 | 0.9861 | 514 | 750 | 15071 | 20847 |
| unfiltered, graph | 200 | 0.9933 | 713 | 950 | 22559 | 29711 |
| flag_50 (50%), graph | 32 | 0.8159 | 187 | 318 | 5155 | 9503 |
| flag_50 (50%), graph | 64 | 0.9300 | 302 | 440 | 8335 | 12839 |
| flag_50 (50%), graph | 100 | 0.9665 | 422 | 601 | 12175 | 19343 |
| flag_50 (50%), graph | 128 | 0.9783 | 504 | 709 | 14999 | 20767 |
| flag_50 (50%), graph | 200 | 0.9902 | 729 | 1039 | 22591 | 29871 |
| flag_1 (1%), PQ scan + rerank | 40 | 0.9979 | 308 | 371 | 1771 | 3559 |
| flag_1 (1%), PQ scan + rerank | 100 | 1.0000 | 361 | 417 | 3483 | 4835 |
