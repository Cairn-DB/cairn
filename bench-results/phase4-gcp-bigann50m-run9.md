# 50M on GCP, run 9 (2026-09-28): mimalloc confirmed, no restart needed

- Owner: "go pour le run gcp". Same fleet and flags as run 8: 3 × n2-highmem-8 with node data
  on a local NVMe SSD, and an e2-standard-4 client in europe-west1-b. 50M BigANN rows, 8 shards,
  6 cores, 256 MB memtables, `--sq8-only --target-segment-rows 3000000 --compaction-slots 2`.
  Queries: 1,000 per kind from 8 threads, k = 10, ef = 128; 200 takedowns.
- The code is 3e41aaf. Since run 8 (57b71af) it changes two things:
  - the filter bitmap is no longer cloned per segment and query (2c12511);
  - the server uses mimalloc (ADR 0029).
- A teardown watchdog ran from the start. Merges were paused on every node after the last
  row was acknowledged (ADR 0028). Node logs were copied before the restart.

## Ingest: 20,667 docs/s

| | run 6 | run 8 | **run 9** |
|---|---|---|---|
| 50M rows | 7,701 s | 5,993 s | **2,419 s** |
| docs/s | 6,493 | 8,343 | **20,667** |
| anonymous memory per node after ingest | not recorded | 33.8 GB | **16.5-17.2 GB** |

Ingest ran at 27k docs/s on average up to 26M rows, with no long stall. This run does not
separate the causes. Index builds allocate heavily and ran on glibc's fragmented heap until
now. Locally, at 6M rows, mimalloc showed no ingest gain: 9,543 docs/s against 10,000-13,000
with glibc, in noisy runs. The 2.5× is therefore observed at 50M only, and is to be confirmed
by another run before it is claimed.

## Queries: targets met on the nodes that ingested

| | run 8, after ingest | run 8, restarted | **run 9, after ingest** | run 9, restarted |
|---|---|---|---|---|
| segments per node | 118 | 118 | 99 | 99 |
| unfiltered p99, stale / linearizable | 117 / 151 ms | 42 / 44 ms | **35 / 36 ms** | 25 / 30 ms |
| 1% filter p99, stale / linearizable | 134 / 135 ms | 42 / 42 ms | **31 / 30 ms** | 30 / 29 ms |
| unfiltered throughput | 81 / 74 QPS | 244 / 226 QPS | **435 / 318 QPS** | 472 / 350 QPS |
| 1% filter throughput | | | 282 / 338 QPS | 300 / 350 QPS |
| recall@10, unfiltered / filtered | 0.985 / 0.989 | 0.985 / 0.989 | 0.985 / 0.990 | 0.985 / 0.990 |
| takedown visible on all 3 nodes, p99 | 102 ms | 102 ms | **102 ms** | 101 ms |
| anonymous memory per node | 33.8 GB | 17 GB | 16.6-16.9 GB | 15.7 GB |

- **Without a restart** every latency target is met, with the best numbers of any run so far.
  Run 6 had 34/33 ms unfiltered, 40/40 ms filtered and 301 QPS.
- **Restart gap.** A restart still helps by 8-9% in throughput and 10 ms at the unfiltered
  p99, against a factor of 3 to 4 with glibc.
- Run 9 has fewer segments per node than run 8 (99 against 118), which favours it. The
  comparison inside run 9, between the nodes that ingested and the restarted ones, does not
  depend on that.

## Cost and teardown

Fleet created 09:37 UTC and deleted 11:09 UTC, about 1.5 h: **about 3 USD** (list prices).
Checked: no instances, disks, network or firewall rules.
