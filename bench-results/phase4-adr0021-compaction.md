# ADR 0021: compaction through the log, measurements

- Dates: 2026-09-24/25. Host: development machine (docs/progress.md).
- 2M BigANN rows, 3 server processes on one host, 4 shards, 64 MB memtables, `--sq8-only`,
  shipping and leader balancing on, all build threads, **at most 3 segments per shard** (merges
  all along), 200 takedowns. "old" = commit 44f0195 (local compactions on every replica);
  "new" = this change. Alternating runs (`data/run-compact-ab.sh`); files
  `phase4-adr0016-bigann2m-ship-{localcompact,logcompact}-r{5,6,7}.md`.

## Final A/B (3 runs each)

| run | CPU s nodes 1/2/3 (total) | ingest docs/s | builds settled | recall@10 | unfiltered p99 stale | takedown p50 / p99 |
|---|---|---|---|---|---|---|
| old r5 | 1838 / 1744 / 1647 (5229) | 9,687 | 226 s | 0.9885 | 6.0 ms | 67.8 / 103.5 ms |
| old r6 | 1704 / 1486 / 1409 (4599) | 10,220 | 216 s | 0.9886 | 5.8 ms | 76.5 / 116.2 ms |
| old r7 | 1714 / 1485 / 1681 (4880) | 10,834 | 230 s | 0.9887 | 5.7 ms | 66.2 / 109.4 ms |
| new r5 | 1234 / 731 / 846 (2811) | 12,206 | 122 s | 0.9883 | 5.9 ms | 63.7 / 121.4 ms |
| new r6 | 1125 / 523 / 799 (2447) | 12,153 | 101 s | 0.9886 | 5.4 ms | 56.2 / 119.4 ms |
| new r7 | 1212 / 1121 / 1177 (3510) | 10,152 | 161 s | 0.9887 | 6.0 ms | 73.7 / 103.7 ms |

Means: CPU 2,923 s vs 4,903 s (**-40%**); builds settle in 128 s vs 224 s; ingest 11.5k vs
10.2k docs/s. Recall, query p99 and takedown latency are unchanged within noise.

## The regression before the fixes

The first version of this change (merges through the log, but segment I/O on the actor)
measured, with the same setup:

| run | CPU s total | builds settled |
|---|---|---|
| old r1 / r2 / r3 | 4,912 / 5,310 / 4,761 | 213 / 217 / 207 s |
| first new r1 / r2 / r3 | 7,223 / 9,865 / 10,097 | 251 / 521 / 585 s |

Diagnosis with info-level logs (`data/run-diag.sh`):

| | merge builds | proposals | rows merged | leadership handovers | total CPU |
|---|---|---|---|---|---|
| old | 24 | – | 4.8M | 26 | 4,830 s |
| first new | 31 | 20 | 6.2M | 37 | 5,906 s |
| after the fixes | 14 | 13 | – | 8 | 2,957 s |

The actor did segment I/O itself and missed heartbeats. Leadership moved, uncommitted merges
were lost, and the next leader rebuilt them (see ADR 0021, "The CPU regression").
