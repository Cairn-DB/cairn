# Acceptance suite on 0.1 (2026-09-29)

`examples/acceptance/cairn_acceptance.py`, run on this machine against code at v0.1.0 plus
this suite.

| setup | corpus | result |
|---|---|---|
| one node, Docker image built from the tree, default settings | 3,000 documents | **56/56 checks** |
| three nodes (local processes), 1 MB memtables to force flushes, segment shipping and merges, role keys | 6,000 documents | **72/72 checks** |

Notable figures, cluster run:
- vector recall@10 0.985 unfiltered and 0.970 filtered, with no filter violation;
- every filter operator exact against the local truth, on 314 to 4,500 matches;
- after 51 takedowns, every node lists exactly the 5,949 live documents with the token;
- each node ended with 8 segments, 4 built locally and 4 fetched from shard leaders.

The one-node run kept everything in the memtable (3,000 documents fit in the default 64 MB):
it checks the API end to end, not the segment path. The cluster run covers segments.

Logs: `data/acceptance-single.log`, `data/acceptance-cluster.log` (gitignored).
