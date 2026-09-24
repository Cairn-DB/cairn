# HNSW build: row-at-a-time vs batched (ADR 0019)

- Date: 2026-09-24. SIFT prefix 1000000 × 128, 1000 queries, M = 16, efConstruction = 100; graph search with exact distances, k = 10.
- Hardware: see docs/progress.md.

| builder | threads | build s | rows/s | recall@10 ef 32 | ef 64 | ef 128 |
|---|---|---|---|---|---|---|
| row-at-a-time | 1 | 185.5 | 5392 | 0.8871 | 0.9554 | 0.9849 |
| batched | 1 | 223.4 | 4476 | 0.8886 | 0.9553 | 0.9852 |
| batched | 4 | 65.0 | 15393 | 0.8886 | 0.9553 | 0.9852 |
| batched | 16 | 35.6 | 28073 | 0.8886 | 0.9553 | 0.9852 |

Batched graphs byte-identical across thread counts [1, 4, 16]: **true**
