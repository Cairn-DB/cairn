# 50M on GCP after asynchronous Raft persistence (ADR 0027), run 7, 2026-09-26

- Owner: "go gcp". Same fleet, flags and data as run 6 (`phase4-gcp-bigann50m-2026-09-26.md`):
  3 × n2-highmem-8 and one e2-standard-4 client in europe-west1-b, 50M BigANN rows, 8 shards,
  6 cores, 256 MB memtables, `--sq8-only --target-segment-rows 3000000 --compaction-slots 2`
  (2 build threads). Commit 8831dac (ADR 0027 and its follow-ups).
- Only ingest was measured. The query phase was dropped (see below).

## Ingest: target missed, no gain

| | run 6 (67cc4ff) | run 7 (8831dac) |
|---|---|---|
| 50M rows | 7,701 s, **6,493 docs/s** | 9,021 s, **5,542 docs/s** |
| rate between stalls | 25k to 33k docs/s | 25k to 40k docs/s |
| stalls over 5 min | 7 (6.5 to 26 min) | 6 (8.8 to 27 min) |
| upsert batch p99 | | 374 ms (max 26.5 min, a stall) |

Timeline, run 7 (s): 10M 223; 11M 1,341; 17M 1,631; 24M 2,501; 25M 3,120; 31M 4,478;
35M 5,109; 38M 5,227; 39M 5,754; 41M 5,852; 42M 7,137; 48M 7,342; 49M 8,981; 50M 9,021.
Full log: `data/gcp-run7/ingest.log` (gitignored).

## Why: the actor is no longer the limit, builds are

- Slow actor steps (500 ms or more) over the whole run: 152, 155 and 138 per node. Actor time
  in them: 308, 265 and 271 s per node, spread over 8 shards and 9,021 s, about 0.4% of each
  shard's time. No Raft sync ran on the actor.
- During stalls: pending flushes 5 to 7 per node, actor inboxes empty, node CPU 145 to 380%
  of 800%. Writes wait for the write limit (memtable at twice its size) while a flush builds.
  With 2 slots of 2 threads, builds use at most half the machine.
- Leaders were unbalanced (4/3/1 at 17M, 5/1/2 at the end): the leader builds (ADR 0016),
  so one node did most of the building.
- The second flush of a shard was 750 MB instead of 375 MB (the memtable doubles while the
  first builds) and took 5 to 13 minutes to build.
- 8 to 11 fetches per node fell back to a local build ("no data"), duplicating builds.
- The remaining slow steps: `flush_written` (adopting a built file: 45 to 56 per node,
  handle time 1 to 3 s, not diagnosed) and `net` with persist time 0.6 to 3.6 s (likely the log
  append under dirty-page pressure).

Asynchronous persistence did its job (the actor stopped blocking on syncs) but ingest was not
limited by it end to end. The next levers are build parallelism and leader balancing.

## Bug found: too many open files

- Nodes 1 and 3 hit "Too many open files" (soft limit 1024). Four shard replicas reopened from
  disk and recovered. The cause was 3f4e21d: fetched chunks were written by tasks while the
  fetch window kept advancing, so writes piled up behind a stalled disk, each holding a
  descriptor.
- Fixed in a7520b9 (the window counts writes in progress; the server raises its soft limit).
  That fix did not run in this run.

## Query phase dropped

After ingest the nodes were restarted with merges disabled, as in run 6, to query an idle
cluster. After 55 minutes they were still at 200% CPU: segments went from 111 to 90 per node,
so merges decided before the restart kept running. Queries under merges would not compare
with run 6, and the query path had not changed since run 6, so the fleet was deleted instead.
The queries at the end of ingest (8 per kind, under builds) are not meaningful: unfiltered
stale p99 20 s, 1% filter p99 105 ms.

## Cost and teardown

Created 19:35 UTC, deleted 23:31 UTC: 3.9 h at 1.967 USD/h, **about 7.7 USD** (estimate from
the list price, not from the billing console). A watchdog would have deleted the fleet at
00:00 UTC. Checked after teardown: no instances, disks, network or firewall rules.
