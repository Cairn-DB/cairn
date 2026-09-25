# Pipelined segment fetches: catch-up of a lagging replica

- Date: 2026-09-25. Host: development machine (docs/progress.md), loopback network.
- Scenario (`data/run-catchup.sh`): 3 nodes, 4 shards, 64 MB memtables, at most 3 segments per
  shard, `--sq8-only`. 2M BigANN rows are ingested and settle; node 3 is stopped; 300k more
  rows are ingested (about 180 MB of log, kept by the leader for lagging followers) and settle;
  node 3 restarts and catches up through the log. The figure is the time from the restart until
  every shard has the same applied index and segment list on all nodes, with nothing pending
  (`cairn-bench converge`).
- "sequential": commit fdf77f4 (one fetch at a time, one chunk per round trip). "pipelined":
  this change (up to 4 fetches, 8 chunks in flight each). Same benchmark client.

| run | catch-up | node 3: segments built / fetched |
|---|---|---|
| sequential r1 | 9.5 s | 3 / 5 |
| pipelined r1 | 8.6 s | 3 / 5 |
| sequential r2 | 7.2 s | 2 / 6 |
| pipelined r2 | 7.0 s | 2 / 6 |

## Reading

- **No measurable difference on loopback.** A chunk round trip takes well under a
  millisecond, and the catch-up is dominated by other work: log replay (about 180 MB of
  entries) and 2 or 3 local rebuilds.
- **The rebuilds are the largest cost.** They happen because node 3 was down for longer than
  the 30 s grace for retired files, so the flushes it missed had already been merged and
  purged on the source. Building such a flush, only to merge it right after, is avoidable.
  That is the next lever (not done).
- **Where pipelining matters.** With network latency, a single chunk per round trip caps
  throughput at 256 KiB per RTT: about 25 MB/s at 10 ms. In the simulator, which models
  latency, four campaign seeds had needed up to 15 s to converge. Now all 60,000 seeds
  converge within the original 5 s. This host cannot add latency (`tc` needs root), so it was
  not measured on a real network.
