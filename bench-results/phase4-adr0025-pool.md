# Shard searches: actor vs a thread per search vs a search pool (ADR 0025)

- Date: 2026-09-26. Host: development machine. Driver: `data/run-offload-ab.sh`. Same data and procedure as `phase4-adr0025-offload.md` (12.5M BigANN rows, one node, 2 shards of 6.25M, 9 segments per shard, idle).
- actor: ca31a85; thread: af95ec9 (a thread per search); pool: `Runtime::offload_search` on a pool of one permanent thread per hardware thread. Alternating actor, thread, pool, twice.

```
actor-p1 threads=1
unfiltered / stale: recall 0.9857, p50 2.54 ms, p99 3.27 ms, 398 QPS
unfiltered / linearizable: recall 0.9857, p50 2.55 ms, p99 3.34 ms, 397 QPS
flag_1 filter (≈1%) / stale: recall 0.9901, p50 3.23 ms, p99 3.97 ms, 305 QPS
flag_1 filter (≈1%) / linearizable: recall 0.9901, p50 3.23 ms, p99 4.23 ms, 305 QPS
actor-p1 threads=8
unfiltered / stale: recall 0.9857, p50 18.37 ms, p99 31.60 ms, 434 QPS
unfiltered / linearizable: recall 0.9857, p50 18.63 ms, p99 31.67 ms, 430 QPS
flag_1 filter (≈1%) / stale: recall 0.9901, p50 24.26 ms, p99 34.24 ms, 326 QPS
flag_1 filter (≈1%) / linearizable: recall 0.9901, p50 24.36 ms, p99 36.11 ms, 324 QPS
thread-p1 threads=1
unfiltered / stale: recall 0.9857, p50 3.06 ms, p99 4.06 ms, 327 QPS
unfiltered / linearizable: recall 0.9857, p50 3.07 ms, p99 4.05 ms, 329 QPS
flag_1 filter (≈1%) / stale: recall 0.9901, p50 4.09 ms, p99 5.41 ms, 241 QPS
flag_1 filter (≈1%) / linearizable: recall 0.9901, p50 4.07 ms, p99 5.32 ms, 242 QPS
thread-p1 threads=8
unfiltered / stale: recall 0.9857, p50 6.18 ms, p99 12.28 ms, 1244 QPS
unfiltered / linearizable: recall 0.9857, p50 6.04 ms, p99 9.62 ms, 1291 QPS
flag_1 filter (≈1%) / stale: recall 0.9901, p50 6.48 ms, p99 10.68 ms, 1181 QPS
flag_1 filter (≈1%) / linearizable: recall 0.9901, p50 6.29 ms, p99 9.61 ms, 1224 QPS
pool-p1 threads=1
unfiltered / stale: recall 0.9857, p50 2.97 ms, p99 3.98 ms, 337 QPS
unfiltered / linearizable: recall 0.9857, p50 2.95 ms, p99 3.91 ms, 342 QPS
flag_1 filter (≈1%) / stale: recall 0.9901, p50 3.95 ms, p99 5.32 ms, 251 QPS
flag_1 filter (≈1%) / linearizable: recall 0.9901, p50 3.99 ms, p99 5.52 ms, 249 QPS
pool-p1 threads=8
unfiltered / stale: recall 0.9857, p50 6.16 ms, p99 12.47 ms, 1232 QPS
unfiltered / linearizable: recall 0.9857, p50 6.06 ms, p99 9.18 ms, 1289 QPS
flag_1 filter (≈1%) / stale: recall 0.9901, p50 6.39 ms, p99 10.13 ms, 1193 QPS
flag_1 filter (≈1%) / linearizable: recall 0.9901, p50 6.51 ms, p99 9.76 ms, 1184 QPS
actor-p2 threads=1
unfiltered / stale: recall 0.9857, p50 2.55 ms, p99 3.21 ms, 396 QPS
unfiltered / linearizable: recall 0.9857, p50 2.58 ms, p99 3.40 ms, 394 QPS
flag_1 filter (≈1%) / stale: recall 0.9901, p50 3.31 ms, p99 4.31 ms, 296 QPS
flag_1 filter (≈1%) / linearizable: recall 0.9901, p50 3.30 ms, p99 4.14 ms, 298 QPS
actor-p2 threads=8
unfiltered / stale: recall 0.9857, p50 18.68 ms, p99 29.90 ms, 429 QPS
unfiltered / linearizable: recall 0.9857, p50 18.61 ms, p99 28.79 ms, 432 QPS
flag_1 filter (≈1%) / stale: recall 0.9901, p50 24.44 ms, p99 42.14 ms, 325 QPS
flag_1 filter (≈1%) / linearizable: recall 0.9901, p50 24.76 ms, p99 43.16 ms, 320 QPS
thread-p2 threads=1
unfiltered / stale: recall 0.9857, p50 3.07 ms, p99 4.04 ms, 327 QPS
unfiltered / linearizable: recall 0.9857, p50 3.07 ms, p99 4.23 ms, 327 QPS
flag_1 filter (≈1%) / stale: recall 0.9901, p50 4.06 ms, p99 5.42 ms, 244 QPS
flag_1 filter (≈1%) / linearizable: recall 0.9901, p50 4.11 ms, p99 5.64 ms, 240 QPS
thread-p2 threads=8
unfiltered / stale: recall 0.9857, p50 6.09 ms, p99 12.89 ms, 1248 QPS
unfiltered / linearizable: recall 0.9857, p50 6.11 ms, p99 10.11 ms, 1278 QPS
flag_1 filter (≈1%) / stale: recall 0.9901, p50 6.36 ms, p99 10.15 ms, 1193 QPS
flag_1 filter (≈1%) / linearizable: recall 0.9901, p50 6.31 ms, p99 9.26 ms, 1216 QPS
pool-p2 threads=1
unfiltered / stale: recall 0.9857, p50 2.94 ms, p99 3.94 ms, 342 QPS
unfiltered / linearizable: recall 0.9857, p50 2.91 ms, p99 3.84 ms, 346 QPS
flag_1 filter (≈1%) / stale: recall 0.9901, p50 3.77 ms, p99 5.18 ms, 261 QPS
flag_1 filter (≈1%) / linearizable: recall 0.9901, p50 3.71 ms, p99 4.62 ms, 266 QPS
pool-p2 threads=8
unfiltered / stale: recall 0.9857, p50 6.09 ms, p99 12.71 ms, 1256 QPS
unfiltered / linearizable: recall 0.9857, p50 6.18 ms, p99 9.08 ms, 1275 QPS
flag_1 filter (≈1%) / stale: recall 0.9901, p50 6.71 ms, p99 13.84 ms, 1110 QPS
flag_1 filter (≈1%) / linearizable: recall 0.9901, p50 6.81 ms, p99 10.49 ms, 1139 QPS
ALL_DONE
```
