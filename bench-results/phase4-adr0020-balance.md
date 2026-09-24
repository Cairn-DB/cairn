# ADR 0020: leader balancing, measurements

- Date: 2026-09-24. Host: development machine (docs/progress.md). Commit: working tree of the
  ADR 0020 commit.
- 2M BigANN rows, 3 server processes on one host, 4 shards, 64 MB memtables, `--sq8-only`,
  shipping on, all build threads, 200 takedowns. 2 alternating runs per setting
  (`data/run-balance-ab.sh`). Files: `phase4-adr0016-bigann2m-ship-{balance,nobalance}-r{1,2}.md`.
- With 4 shards on 3 nodes the best split is 2/1/1 (shard `s` prefers node `s mod 3 + 1`).

| run | shards led by nodes 1/2/3 at the end | CPU s nodes 1/2/3 (max/min) | ingest docs/s | builds settled | unfiltered p99 stale | takedown p50 / p99 |
|---|---|---|---|---|---|---|
| balancing r1 | 2 / 1 / 1 | 609 / 469 / 404 (1.51) | 12,301 | 46 s | 7.8 ms | 67.8 / 154.5 ms |
| balancing r2 | 2 / 1 / 1 | 596 / 455 / 567 (1.31) | 12,027 | 47 s | 8.0 ms | 77.5 / 133.6 ms |
| no balancing r1 | 0 / 2 / 2 | 381 / 689 / 717 (1.88) | 11,090 | 53 s | 8.0 ms | 60.3 / 117.0 ms |
| no balancing r2 | 1 / 2 / 1 | 558 / 489 / 667 (1.36) | 10,845 | 46 s | 8.0 ms | 61.5 / 116.3 ms |

## Reading

- Balancing reaches the optimal leadership split in both runs. Without it, the split depends on
  election timing (one run left node 1 leading nothing).
- CPU follows leadership: the spread between the busiest and idlest node narrows when the
  unbalanced run was skewed (1.88 to 1.51), and is similar when chance already balanced it.
- Ingest: 12.0-12.3k with balancing, 10.8-11.1k without (about +10%, 2 runs each, small).
- **Takedown p99 is higher with balancing** in both pairs (134-155 ms vs 116-117 ms). Earlier
  repeated runs of the same benchmark spread over 104-143 ms, so this is not established. It
  may come from handovers: during a transfer, proposals are refused for up to one election
  timeout (500 ms here) and the client retries. The servers log at warn level, so this run
  does not show how many handovers happened, or when. To check before relying on balancing
  in latency-sensitive setups.
