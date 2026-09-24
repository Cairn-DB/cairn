# Disk-resident index (Vamana + PQ, ADR 0014): sift, one segment

- Date: 2026-09-24
- Rows: 1000000 × 128-d, queries: 1000, k = 10, single-thread queries; R = 48, L_build = 96, alpha = 1.2, passes = 1
- Build: 86.1 s (11611 rows/s, 16 build threads, includes PQ training)
- RAM: 32.1 MB of PQ codes + codebooks (32 B/row); disk: 819.2 MB of node blocks (819 B/row)
- Cold: the index file's pages are dropped from the page cache before every query (`posix_fadvise(DONTNEED)`); warm: after one full pass

| case | L | recall@10 | warm p50 µs | warm p99 µs | cold p50 µs | cold p99 µs |
|---|---|---|---|---|---|---|
| unfiltered, graph | 32 | 0.9146 | 218 | 381 | 5223 | 8335 |
| unfiltered, graph | 64 | 0.9685 | 334 | 624 | 8479 | 11063 |
| unfiltered, graph | 100 | 0.9834 | 457 | 878 | 12455 | 18751 |
| unfiltered, graph | 128 | 0.9891 | 570 | 1208 | 15583 | 21695 |
| unfiltered, graph | 200 | 0.9946 | 786 | 1447 | 23359 | 31279 |
| flag_50 (50%), graph | 32 | 0.8199 | 200 | 402 | 5155 | 7879 |
| flag_50 (50%), graph | 64 | 0.9357 | 329 | 651 | 8551 | 12543 |
| flag_50 (50%), graph | 100 | 0.9706 | 451 | 909 | 12567 | 18575 |
| flag_50 (50%), graph | 128 | 0.9807 | 556 | 1091 | 15567 | 23567 |
| flag_50 (50%), graph | 200 | 0.9909 | 796 | 1601 | 23343 | 29791 |
| flag_1 (1%), PQ scan + rerank | 40 | 0.9979 | 324 | 468 | 1792 | 3747 |
| flag_1 (1%), PQ scan + rerank | 100 | 1.0000 | 382 | 581 | 3923 | 12143 |
