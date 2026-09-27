# Build threads at low priority, and flushes during merges (local, 2026-09-27)

Question after GCP run 7: ingest waits on index builds while nodes leave half their CPU idle.
Two changes, measured alone and together:

- **nice** (662a8eb): build workers run at nice 10, and by default all concurrent builds
  share the whole machine (hardware threads / slots, instead of half of that).
- **slots** (7b781a0): a shard's flush no longer waits for its own merge; flush and merge each
  hold a node slot.

## Setup

3 nodes on this machine (Ryzen 7 8845HS), each pinned with `taskset` to 5 hardware threads
(`available_parallelism` = 5, like a small VM), the client on the 16th. 4 shards, 3 cores,
64 MB memtables, `--sq8-only --target-segment-rows 3000000 --compaction-slots 2`, 6M BigANN
rows. Baseline a7520b9: 1 build thread per build (5 / (2 × 2)); nice: 2 per build.
Scripts: `data/run-build2-ab.sh`, `data/run-slots-ab.sh`, `data/run-slotsonly.sh`
(gitignored). The machine was otherwise idle.

## Results: seconds to ingest 6M rows

| variant | runs | median | |
|---|---|---|---|
| baseline (a7520b9) | 386, 368, 386, 389, 1659, 776, 370, 368 | 386 s (15.5k docs/s) | long stalls in 2 of 8 |
| nice only | 296, 564, 489, 1074, 455, 452, 535, 537 | 510 s (11.8k docs/s) | worse |
| slots only | 551, 375, 431 | 431 s (13.9k docs/s) | no better |
| **nice + slots** | **287, 345, 289** | **289 s (20.8k docs/s)** | **no stall over 110 s** |

What the timelines show (rows ingested at 5M):

- nice reaches 5M at 215 to 235 s, against about 300 s for the baseline: builds are faster.
  Then it stalls for 230 to 630 s. Faster flushes make more, smaller segments, merges start
  earlier, and a merge blocked its own shard's flushes. That shard's memtable reached its
  write limit, and since every client batch spans all shards, the whole ingest stopped
  (nice-r4: shard 1 took no write for 5 minutes while its leader merged).
- slots alone removes that block but builds stay slow (1 thread each).
- Together: fast builds and no merge stall. 5M at 224 to 275 s, 6M at 287 to 345 s.

Slow actor steps (500 ms or more) stay small in every variant: 2 to 36 per run, 1.5 to 31 s
in total over three nodes.

## Not concluded

- Three runs of the winning variant only, on one machine with 6M rows. The GCP 50M run has
  larger segments (256 MB memtables, 8 shards), different disks and a real network.
- Query latency during builds: the 8 queries at the end of each run land while merges still
  run, and vary from 20 ms to 6 s p50 in every variant, including the baseline. That is
  the known open problem of queries during builds. These runs do not measure it and it is
  not concluded here.
- `live docs` above 6,000,000 in two runs (baseline r1 of the first A/B, 6,000,502; nice r2,
  6,001,499): a count that includes rows not yet masked, or duplicates. Not investigated yet.
