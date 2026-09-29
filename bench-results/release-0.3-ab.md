# 0.1.0 against 0.3.0, before the public release

- Date: 2026-09-29. Machine: the development workstation (local, shared; see
  `docs/progress.md`, "Environment").
- Why: the 50M figures in the README were measured with 0.1 (GCP runs 9 and 10). Since then
  the apply path became asynchronous, search results carry text ids, a catalog replica runs on
  every node, and HTTP reads check tenants and retention. This A/B checks that 0.3 did not
  regress before quoting those figures again. It is not a substitute for a new 50M run.
- How: `data/run-ab-03.sh` (not committed; `data/` is local). v0.1.0 was built from its tag with
  its own `cairn-bench`, and HEAD with its own. Each run used a fresh 3-node local cluster
  (`tools/scripts/cluster.sh start <dir> tools/scripts/sift-schema.json 6 2`) and
  `cairn-bench cluster --n 1000000` (SIFT1M, 8 writers, 8 query threads, 2,000 queries per
  configuration, 200 takedowns). The order was old, new, old, new. Raw reports:
  `ab03-old-r1.md`, `ab03-new-r1.md`, `ab03-old-r2.md`, `ab03-new-r2.md`.

| metric | 0.1.0 r1 | 0.1.0 r2 | 0.3.0 r1 | 0.3.0 r2 |
|---|---|---|---|---|
| ingest (docs/s) | 7,818 | 8,720 | 8,244 | 9,070 |
| unfiltered stale p50 / p99 (ms) | 22.8 / 60.8 | 22.3 / 95.8 | 21.9 / 62.9 | 22.2 / 196.6 |
| unfiltered stale QPS | 258 | 280 | 284 | 275 |
| filtered 1% stale p50 / p99 (ms) | 8.0 / 18.4 | 7.5 / 18.1 | 7.7 / 19.1 | 7.5 / 19.9 |
| filtered 1% stale QPS | 887 | 938 | 896 | 944 |
| filtered linearizable p99 (ms) | 235 | 223 | 366 | 273 |
| takedown visible on all nodes, p99 (ms) | 108 | 102 | 112 | 103 |

**Reading.** No regression shows beyond run-to-run noise: ingest, medians, throughput and
takedown visibility match. The p99 tails of unfiltered and linearizable queries vary a lot
between runs of the same version (unfiltered p99 from 61 to 197 ms). On this shared machine
they are noise, not a finding, and they do not support a comparison either way. The 50M
figures stay those of runs 9 and 10 (0.1 code). A new 50M run on 0.3 has not been done.

Also checked: data written by a 0.2.0 node opens in 0.3.0 without migration. That covers a
text id, a takedown still in force, 40 documents across flushed segments, and a patch on old
data. The collection is listed as `default`.
