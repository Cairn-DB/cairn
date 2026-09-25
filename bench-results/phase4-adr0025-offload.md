# Shard searches on vs off the replica actor (ADR 0025)

- Date: 2026-09-26. Host: development machine (docs/progress.md). Driver: `data/run-offload-ab.sh`.
- Data: 12.5M BigANN rows, one node, 2 shards of 6.25M rows, 9 segments per shard (no merges): the per-shard shape of the GCP 50M run. `--sq8-only`, 6 cores.
- Each binary restarted on the same data, measured once idle; alternating actor, offload, actor, offload. 1,000 queries per kind, k = 10, ef = 128; recall against brute force.
- "actor": commit ca31a85 (searches in the replica actor). "offload": ADR 0025.

```
actor-r1 threads=1
unfiltered / stale: recall 0.9857, p50 2.59 ms, p99 3.42 ms, 387 QPS
unfiltered / linearizable: recall 0.9857, p50 2.65 ms, p99 4.46 ms, 378 QPS
flag_1 filter (≈1%) / stale: recall 0.9901, p50 3.27 ms, p99 4.24 ms, 299 QPS
flag_1 filter (≈1%) / linearizable: recall 0.9901, p50 3.24 ms, p99 4.25 ms, 302 QPS
actor-r1 threads=8
unfiltered / stale: recall 0.9857, p50 19.05 ms, p99 30.31 ms, 421 QPS
unfiltered / linearizable: recall 0.9857, p50 19.33 ms, p99 33.00 ms, 412 QPS
flag_1 filter (≈1%) / stale: recall 0.9901, p50 24.35 ms, p99 42.40 ms, 325 QPS
flag_1 filter (≈1%) / linearizable: recall 0.9901, p50 24.31 ms, p99 39.98 ms, 326 QPS
offload-r1 threads=1
unfiltered / stale: recall 0.9857, p50 3.11 ms, p99 4.23 ms, 324 QPS
unfiltered / linearizable: recall 0.9857, p50 3.09 ms, p99 4.16 ms, 325 QPS
flag_1 filter (≈1%) / stale: recall 0.9901, p50 4.12 ms, p99 5.26 ms, 240 QPS
flag_1 filter (≈1%) / linearizable: recall 0.9901, p50 4.12 ms, p99 5.17 ms, 241 QPS
offload-r1 threads=8
unfiltered / stale: recall 0.9857, p50 6.46 ms, p99 12.28 ms, 1175 QPS
unfiltered / linearizable: recall 0.9857, p50 6.32 ms, p99 9.74 ms, 1231 QPS
flag_1 filter (≈1%) / stale: recall 0.9901, p50 6.73 ms, p99 10.82 ms, 1121 QPS
flag_1 filter (≈1%) / linearizable: recall 0.9901, p50 6.63 ms, p99 10.26 ms, 1154 QPS
actor-r2 threads=1
unfiltered / stale: recall 0.9857, p50 2.56 ms, p99 4.10 ms, 390 QPS
unfiltered / linearizable: recall 0.9857, p50 2.53 ms, p99 4.23 ms, 398 QPS
flag_1 filter (≈1%) / stale: recall 0.9901, p50 3.21 ms, p99 4.87 ms, 301 QPS
flag_1 filter (≈1%) / linearizable: recall 0.9901, p50 3.23 ms, p99 4.89 ms, 301 QPS
actor-r2 threads=8
unfiltered / stale: recall 0.9857, p50 18.83 ms, p99 31.92 ms, 426 QPS
unfiltered / linearizable: recall 0.9857, p50 18.89 ms, p99 30.83 ms, 422 QPS
flag_1 filter (≈1%) / stale: recall 0.9901, p50 24.97 ms, p99 39.49 ms, 318 QPS
flag_1 filter (≈1%) / linearizable: recall 0.9901, p50 24.61 ms, p99 34.24 ms, 323 QPS
offload-r2 threads=1
unfiltered / stale: recall 0.9857, p50 3.03 ms, p99 3.92 ms, 332 QPS
unfiltered / linearizable: recall 0.9857, p50 3.03 ms, p99 3.88 ms, 334 QPS
flag_1 filter (≈1%) / stale: recall 0.9901, p50 3.97 ms, p99 5.12 ms, 249 QPS
flag_1 filter (≈1%) / linearizable: recall 0.9901, p50 3.94 ms, p99 5.23 ms, 249 QPS
offload-r2 threads=8
unfiltered / stale: recall 0.9857, p50 6.15 ms, p99 14.01 ms, 1216 QPS
unfiltered / linearizable: recall 0.9857, p50 6.10 ms, p99 10.02 ms, 1271 QPS
flag_1 filter (≈1%) / stale: recall 0.9901, p50 6.33 ms, p99 10.48 ms, 1189 QPS
flag_1 filter (≈1%) / linearizable: recall 0.9901, p50 6.37 ms, p99 9.80 ms, 1199 QPS
ALL_DONE
```
