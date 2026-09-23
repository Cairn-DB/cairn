# Disk-resident index (Vamana + PQ, ADR 0014): sift, one segment

- Date: 2026-09-23
- Rows: 1000000 × 128-d, queries: 1000, k = 10, single thread; R = 48, L_build = 96, alpha = 1.2, passes = 1
- Build: 323.5 s (3091 rows/s, one thread, includes PQ training)
- RAM: 32.1 MB of PQ codes + codebooks (32 B/row); disk: 819.2 MB of node blocks (819 B/row)
- Cold: the index file's pages are dropped from the page cache before every query (`posix_fadvise(DONTNEED)`); warm: after one full pass

| case | L | recall@10 | warm p50 µs | warm p99 µs | cold p50 µs | cold p99 µs |
|---|---|---|---|---|---|---|
| unfiltered, graph | 32 | 0.9158 | 215 | 412 | 5459 | 50495 |
| unfiltered, graph | 64 | 0.9682 | 345 | 636 | 8967 | 13999 |
| unfiltered, graph | 100 | 0.9833 | 491 | 868 | 13047 | 18719 |
| unfiltered, graph | 128 | 0.9894 | 616 | 1068 | 16559 | 24687 |
| unfiltered, graph | 200 | 0.9955 | 854 | 1634 | 24991 | 32447 |
| flag_50 (50%), graph | 32 | 0.8202 | 189 | 320 | 5219 | 7799 |
| flag_50 (50%), graph | 64 | 0.9348 | 328 | 620 | 8519 | 11775 |
| flag_50 (50%), graph | 100 | 0.9700 | 437 | 809 | 12399 | 18687 |
| flag_50 (50%), graph | 128 | 0.9806 | 540 | 1033 | 15423 | 24735 |
| flag_50 (50%), graph | 200 | 0.9911 | 771 | 1456 | 23391 | 27535 |
| flag_1 (1%), PQ scan + rerank | 40 | 0.9979 | 328 | 608 | 1852 | 3971 |
| flag_1 (1%), PQ scan + rerank | 100 | 1.0000 | 371 | 483 | 3769 | 5919 |
