# Roadmap

Cairn is pre-release. This page lists what comes next, in rough order, and what is known to be
missing. Nothing here has a date. An item moves only when its evidence is in
(`bench-results/`, an ADR, tests). Ideas and objections are welcome in issues.

## Next

- **Confirm the allocator fix at 50M.** mimalloc (ADR 0029) removed the post-ingest query
  slowdown locally. A 50M cluster run must show that a node that has just ingested serves at
  full speed without a restart.
- **Ingest without long stalls.** Ingest reaches 20k-25k docs/s between stalls, and 8k docs/s
  overall at 50M on 3 nodes. What remains:
  - builds done twice when leadership moves during ingest;
  - fetches that give up too early and rebuild locally;
  - replica steps of several seconds whose cause is not measured yet.
- **Leader balancing under continuous ingest.** Leadership should spread evenly even while
  every shard is busy.
- **Fewer copies in the search hot path.** The largest share of search time is a memory copy
  before distance computation.

## Before a first release

- **Dynamic membership.** Add and remove nodes and replicas without a reload, through Raft
  membership changes, with shards rebalanced.
- **TLS and authentication on the HTTP API**, with roles (read, write, takedown, admin).
- **Backups and restore**, consistent with the deletion guarantee: a restored backup must not
  bring back a document deleted after the backup was taken, unless the operator explicitly
  asks for it.
- **Rolling upgrades** across protocol versions, instead of a full-cluster restart.
- **Operational metrics** (Prometheus) and a small admin console.
- **Client libraries**: Python and TypeScript over the HTTP API.

## Later

- **Deletion propagation beyond Cairn** ([ADR 0024](docs/adr/0024-kafka-ingestion-and-deletion-propagation.md),
  proposed): Kafka ingestion with end-to-end read-your-takedown, and a monitor that checks that
  deletions reached every downstream copy.
- **Multi-region** deployments.
- **Disk-resident indexes by default** for collections larger than memory (DiskANN-style,
  prototyped in ADR 0013).

## Known open problems

These are tracked in [`docs/progress.md`](docs/progress.md).

- In simulation, about 0.1% of runs with very slow disks (150 ms syncs) acknowledge no write
  under the injected faults. Safety holds in every one of them. This is a liveness problem.
- Flush files that are built but not yet published are rebuilt after a restart.
- A merge of merges can be purged before a lagging replica fetches it.
