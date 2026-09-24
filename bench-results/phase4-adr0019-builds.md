# ADR 0019: parallel deterministic builds, measurements

- Date: 2026-09-24. Host: development machine (8 cores / 16 hardware threads, docs/progress.md).

## Single builds, SIFT1M (`cairn-bench build-sweep`, `cairn-bench disk-sweep`)

HNSW, M = 16, efConstruction = 100, graph search with exact distances, 1000 queries, official
ground truth. File: `phase4-adr0019-hnsw-build-sift1m.md`.

| builder | threads | build | recall@10 ef 32 / 64 / 128 |
|---|---|---|---|
| row-at-a-time (before) | 1 | 185.5 s | 0.8871 / 0.9554 / 0.9849 |
| batched | 1 | 223.4 s | 0.8886 / 0.9553 / 0.9852 |
| batched | 4 | 65.0 s | same graph |
| batched | 16 | 35.6 s | same graph |

The batched graphs are byte-identical for 1, 4 and 16 threads (checked by the tool). 16 threads
build 5.2x faster than the row-at-a-time builder; on one thread the batched builder is 20%
slower (intra-batch candidates).

Vamana (disk index), R = 48, L_build = 96, alpha 1.2, PQ training included (sequential).
Files: `phase4-adr0019-diskann-sift1m-1pass-16t.md`, `phase4-adr0019-diskann-sift1m-16t.md`,
and the earlier `phase4-diskann-sift1m.md`.

| build | passes | threads | time | graph recall@10 L 32 / 64 / 100 |
|---|---|---|---|---|
| row-at-a-time (2026-09-23) | 1 | 1 | 323.5 s | 0.9158 / 0.9682 / 0.9833 |
| batched | 1 | 16 | 86.1 s | 0.9146 / 0.9685 / 0.9834 |
| batched | 2 | 16 | 116.3 s | 0.9108 / 0.9621 / 0.9802 |

3.8x faster at equal recall for one pass. Two passes had not been measured at 1M before, so
there is no row-at-a-time figure to compare them with.

## Through the cluster, 2M BigANN rows (`data/run-build-ab.sh`)

3 server processes on one host, 4 shards, 64 MB memtables, `--sq8-only`, shipping on,
2 alternating runs per setting. Files: `phase4-adr0016-bigann2m-ship-{bt1,btall}-r{1,2}.md`.

| run | ingest (docs/s) | builds settled after ingest | CPU s total | batch p99 / max (ms) | unfiltered p99 stale |
|---|---|---|---|---|---|
| `--build-threads 1` r1 | 16,829 | 117 s | 835 | 957 / 2,863 | 8.8 ms |
| `--build-threads 1` r2 | 13,642 | 45 s (*) | 1,005 | 1,337 / 19,545 | 1,208 ms (*) |
| all threads r1 | 11,954 | 47 s | 1,501 | 1,982 / 3,175 | 8.4 ms |
| all threads r2 | 13,756 | 49 s | 1,481 | 1,044 / 2,978 | 8.0 ms |

(*) The settle test (30 s without a segment change) passed while a single-threaded compaction
was still running: queries ran against a busy node, hence the 1.2 s p99, and the segment counts
([4, 2, 2, 5]) show merges in progress. With all threads the same compactions finish inside the
settle window.

## Reading

- A single build is 3.8-5.2x faster on 16 hardware threads, with the same recall and
  identical bytes.
- On this host, where three nodes share 16 hardware threads, all-thread builds use about 60%
  more CPU-seconds (SMT pairs and contention make each thread slower). Ingest shows no clear
  gain or loss: 12.0-13.8k docs/s against 13.6-16.8k, with overlapping ranges over 2 runs.
  Builds settle consistently in under 50 s.
- Query latency *during* builds was not measured. On a machine per node, builds now take all
  hardware threads for a short time: `--build-threads` bounds it for nodes that serve heavy
  query load while ingesting.
