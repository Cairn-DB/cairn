# 50M on GCP, run 8 (2026-09-27): build priority, flushes during merges, merge pause

- Owner: "go pour le run gcp". Fleet as in runs 6 and 7: 3 × n2-highmem-8 and one
  e2-standard-4 client in europe-west1-b, 50M BigANN rows, 8 shards, 6 cores, 256 MB memtables,
  `--sq8-only --target-segment-rows 3000000 --compaction-slots 2`. Code 57b71af, with:
  - build threads at nice 10 over the whole machine (662a8eb);
  - a shard's flush no longer waits for its own merge (7b781a0);
  - merge pause (ADR 0028).
- **Disk changed.** Node data sits on a 375 GB local NVMe SSD. fio measured 392 MB/s sequential
  writes and 4,171 synced 4 KB writes/s. Runs 1 to 7 used an 80 GB pd-ssd, about 38 MB/s by
  GCP's grid (0.48 MB/s per GB).
  - Larger persistent disks were refused: the regional SSD quota is 500 GB and counts
    pd-balanced too.
  - The disk change and the code changes are not separated in this run.
- A teardown watchdog ran from the start (deadline 18:16 UTC).

## Ingest: better, still with stalls

| | run 6 | run 7 | **run 8** |
|---|---|---|---|
| 50M rows | 7,701 s | 9,021 s | **5,993 s** |
| docs/s | 6,493 | 5,542 | **8,343** (+28% / +51%) |
| longest stall | 26 min | 27 min | **12 min** |
| upsert batch p99 | | 374 ms | 1,487 ms |

Timeline (s): 10M 370; 11M 1,110; 20M 1,728; 28M 2,532; 29M 3,270; 39M 4,023; 40M 4,702;
46M 5,166; 47M 5,693; 50M 5,993. Four stalls of 9 to 12 minutes remain. Between them the rate
is 20k to 25k docs/s.

- **CPU.** During the first stall, nodes ran at about 690% of 800%, 80% of it at nice 10: builds
  use the machine now.
- **Duplicate builds.** Each node had built 11 to 13 segments and fetched only 4 or 5.
  - Leaders moved during ingest (node 3 went from 1 to 2 shards). A new leader builds the
    freezes it inherits.
  - 7 fetches per node fell back to a local build ("no data" after 2 s). Each fallback is a
    full build, which is costly when builds are the bottleneck.
- **Slow actor steps.** They kept growing, up to 8.2 s in the persist phase of `net` events, on a
  fast local disk. So it is not disk throughput.
  - One suspect is the synchronous path (snapshot, truncation, realignment), which waits for
    the running persistence task.
  - The other is an ext4 fsync that must first write out other files' dirty data.
  - Not measured. The node logs of the ingest phase were overwritten by the restarts below.

## Queries: a regression after ingest, gone after a restart

| | run 6 (95 segs/node) | run 8, after ingest | **run 8, restarted** (118 segs/node) | target |
|---|---|---|---|---|
| unfiltered p99, stale / linearizable | 34 / 33 ms | 117 / 151 ms | **42 / 44 ms** | < 100 ms |
| 1% filter p99, stale / linearizable | 40 / 40 ms | 134 / 135 ms | **42 / 42 ms** | < 100 ms |
| unfiltered throughput | 301 / 311 QPS | 81 / 74 QPS | **244 / 226 QPS** | |
| recall@10, unfiltered / filtered | 0.986 / 0.989 | 0.985 / 0.989 | 0.985 / 0.989 | |
| takedown visible everywhere, p99 | 108 ms | 102 ms | 102 ms | < 1 s |

Both measurements ran with merges paused, nothing pending, and 0.5 to 1.5% CPU per node.

- **After ingest, targets were missed.** Every query cost about 100 ms of CPU per node, and
  8 clients saturated the 8 vCPU at about 80 QPS. Single-client p50 was 40 to 46 ms.
- **Bisection on the same data**, 100 single-client queries per commit, after a restart:

  | commit | unfiltered p50 |
  |---|---|
  | 67cc4ff (run 6) | 19 ms |
  | a363696 | 19 ms |
  | a7520b9 | 20 ms |
  | 662a8eb | 21 ms |
  | 7b781a0 | 21 ms |
  | 57b71af (paused, idle) | 14 ms |

  The code does not regress. The slowness belonged to the process that had ingested.
- **Memory is the lead.** The process after ingest held 33.8 GB of anonymous memory, against
  17 GB after a restart on the same segments. About 17 GB more, with twice the CPU per query:
  something built during ingest stays in memory and is likely still searched. Candidates:
  - indexes of segments replaced by merges;
  - prepared indexes of segments that were never published;
  - memtable indexes.

  **Open, to reproduce locally.** Until fixed, a node that has ingested and merged for a long
  time serves queries 3 to 4 times slower than after a restart.
- After a restart, every latency target is met with more segments than run 6 (118 against 95
  per node). Throughput is lower (244 against 301 QPS) in line with the extra segments.

## Cost and teardown

- Fleet created 13:18 UTC and deleted 16:40 UTC, about 3.4 h at about 2 USD/h: **about 7 USD**,
  estimated from list prices.
- Two provisioning attempts were refused by the SSD quota. Each left one node for a few
  minutes, deleted at once.
- Checked after teardown: no instances, disks, network or firewall rules.
