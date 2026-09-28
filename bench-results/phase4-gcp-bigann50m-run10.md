# 50M on GCP, run 10 (2026-09-28): run 9 reproduced

- Owner: "go dans l'ordre", step 1: confirm the ingest rate of run 9 with a second run.
- Same fleet, flags, disks and procedure as run 9 (`phase4-gcp-bigann50m-run9.md`).
- Code 3e41aaf + ac45c58 (HTTP authentication, which does not touch this path: the bench uses
  the binary protocol).
- Queries on the processes that ingested, with no restart. Teardown watchdog from the start.

| | run 9 | run 10 |
|---|---|---|
| 50M rows ingested | 2,419 s, **20,667 docs/s** | 2,543 s, **19,660 docs/s** |
| anonymous memory per node after ingest | 16.5-17.2 GB | 16.5-16.6 GB |
| segments per node | 99 | 99 |
| unfiltered p99, stale / linearizable | 35 / 36 ms | **31 / 31 ms** |
| 1% filter p99, stale / linearizable | 31 / 30 ms | **31 / 33 ms** |
| unfiltered throughput | 435 / 318 QPS | 374 / 352 QPS |
| 1% filter throughput | 282 / 338 QPS | 294 / 287 QPS |
| recall@10, unfiltered / filtered | 0.985 / 0.990 | 0.984 / 0.989 |
| takedown visible on all 3 nodes, p99 | 102 ms | 102 ms |

- **Ingest at about 20k docs/s is reproduced**, 5% apart. It was 8,343 in run 8, before the
  allocator change (ADR 0029) and the bitmap fix.
- Query latency on the ingesting processes is confirmed. Throughput varies by 10-15% between
  runs on the same shape: shared cloud machines, and different leader placements.
- Fleet created 15:08 UTC and deleted 16:30 UTC, about 1.4 h: **about 3 USD**. Checked: no
  instances, disks, network or firewall rules.
