# 50M on GCP with shard searches off the actor (ADR 0025), 2026-09-26

- Owner: "go gcp". Same fleet and data as `phase4-gcp-bigann50m-2026-09-25.md`: 3 × n2-highmem-8,
  e2-standard-4 client, europe-west1-b, 50M BigANN rows, 1,000 queries per kind from 8
  threads, k = 10, ef = 128, 200 takedowns. Commit 67cc4ff.
- Queries measured on an idle cluster (merges disabled, nothing pending, 0% CPU), at 95
  segments per node: about 12 per shard, 6 to 20 (`phase4-gcp-bigann50m-run6-idle.md`).

## Results

| metric | 2026-09-26 (≈12 seg/shard) | 2026-09-25 (≈9 seg/shard) | 2026-09-24, compacted (3 seg/shard) | target |
|---|---|---|---|---|
| unfiltered recall@10 | 0.986 | 0.985 | 0.983 | |
| unfiltered p99, stale / linearizable | **34 / 33 ms** | 65 / 68 ms | 36 / 48 ms | < 100 ms |
| unfiltered throughput, stale / linearizable | 301 / 311 QPS | 146 / 154 QPS | 266 / 221 QPS | |
| 1% filter recall@10 | 0.989 | 0.991 | 0.991 | |
| 1% filter p99, stale / linearizable | **40 / 40 ms** | 93 / 118 ms | 82 / 108 ms | < 100 ms |
| 1% filter throughput | 224 / 221 QPS | 129 / 94 QPS | 151 / 98 QPS | |
| takedown visible on all 3 machines, p99 | 108 ms | 107 ms | 107 ms | < 1 s |

- **The filtered linearizable target is met: 40 ms against 100 ms**, where the two earlier
  runs missed it (118 and 108 ms). This holds with more segments per shard than either
  earlier run, which works against this run.
- Every p99 target at 50M on real machines is now met, and throughput doubles.
- The cause, found locally and fixed by ADR 0025: each shard ran its searches one at a
  time, inside its replica actor.

## Ingest (not improved)

- 50M rows in 7,701 s, 6,493 docs/s (the previous run: 6,839), with stalls of 5 to 25 minutes.
- The merge-slot reservation (e5a466f) did not remove them: the first stall came before any
  merge.
- Off-actor index loading (ADR 0026, measured locally) did not change them either.
- Per-step instrumentation (ff173ff, run locally) found the cause: synchronous disk syncs on
  the replica actor, taking 0.3 to 1.7 s under build writes. The Raft log and hard state
  sync on every write, and manifests sync at publication. Asynchronous Raft persistence is
  the next lever.
- Queries during ingest, with builds saturating the CPU, had p50 of 4 to 30 s. Unusable
  while builds run.

## Incidents and cost

- **10.7 idle hours.** GCP reused the public IPs of earlier fleets, SSH refused the changed
  host keys, and the deploy script retried forever with errors hidden. There was no timeout
  or watch on it (my mistake). Fixed in 4833326: stale keys are forgotten at provisioning,
  and deploy fails loudly after 10 minutes.
- After a restart, flush files built but not yet published are removed (only merge files
  are kept since 9349678), so followers rebuild them. Noted, not fixed.
- Cost from the audit logs: instances created 22:44:41 UTC on 2026-09-25, deleted 12:42:38
  UTC on 2026-09-26, 13.96 h at 1.967 USD/h, **27.46 USD**, of which about **21 USD** is the
  idle time.
- Fleet deleted. Checked: no instances, disks, network or firewall rules.
