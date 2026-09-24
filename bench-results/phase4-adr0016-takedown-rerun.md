# ADR 0016 A/B re-measurement: takedown latency (and ingest, CPU)

- Date: 2026-09-24. Commit: cc68ba4. Host: development machine (see docs/progress.md).
- Same setup as `phase4-adr0016-bigann2m-{ship,noship}.md`: 2M BigANN rows, 3 server
  processes on one host, 4 shards, 64 MB memtables, `--sq8-only`, 30 s settle.
- 3 runs per mode, alternating ship / noship, 500 takedowns per run (the first measurement
  had 1 run per mode and 100 takedowns). Script: `data/rerun-takedown.sh`; per-run files
  `phase4-adr0016-bigann2m-{ship,noship}-r{1,2,3}.md`.
- Takedown = delete, then read-your-writes reads on every node until the document is gone;
  the sample is the slowest node. Followers learn the commit from the next heartbeat (100 ms).

| run | takedown p50 / p99 / max (ms) | ingest (docs/s) | CPU s, nodes 1/2/3 (total) | settled after ingest |
|---|---|---|---|---|
| ship r1 | 83.2 / 117.6 / 159.9 | 17,115 | 186 / 190 / 311 (687) | 65 s |
| ship r2 | 70.3 / 121.4 / 158.1 | 16,258 | 290 / 87 / 319 (696) | 68 s |
| ship r3 | 88.3 / 128.8 / 144.8 | 16,920 | 210 / 185 / 278 (673) | 67 s |
| noship r1 | 71.3 / 105.4 / 124.1 | 16,371 | 554 / 527 / 548 (1629) | 173 s |
| noship r2 | 70.3 / 103.5 / 113.7 | 12,045 | 551 / 545 / 546 (1642) | 124 s |
| noship r3 | 70.1 / 143.1 / 188.0 | 14,862 | 551 / 545 / 550 (1646) | 187 s |
| first run, ship (100 takedowns) | 93.2 / 176.6 / 248.8 | 16,580 | 238 / 107 / 361 (706) | 83 s |
| first run, noship (100 takedowns) | 52.0 / 102.0 / 112.5 | 11,349 | 560 / 546 / 560 (1666) | 133 s |

## Reading

- The 177 ms vs 102 ms p99 gap does not reproduce. Across the repeats, p99 is 118-129 ms with
  shipping and 104-143 ms without: the ranges overlap, and the worst run is a no-shipping one.
- A small residual may exist: the no-shipping p50 is 70-71 ms in all three runs, the shipping
  p50 is 83 and 88 ms in two of three. That is at most about one sixth of a heartbeat and is
  not established by 3 runs. Pinning it down would need per-node latencies (leader vs
  followers), which the benchmark does not record.
- CPU is stable run to run: shipping uses 41-42% of the no-shipping total, every run.
- Ingest: 16.3-17.1k docs/s with shipping, 11.3-16.4k without. Mean over the 4 runs of each
  mode: 16.7k vs 13.7k docs/s, +22%. The +46% reported from the first pair was the extreme of
  a noisy no-shipping range.
- Builds settle 2-3x sooner with shipping (65-83 s vs 124-187 s).
