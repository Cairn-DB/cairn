# Roadmap

Cairn is a developer preview (0.3). This page lists what comes next, in rough order, and what is known to be
missing. Nothing here has a date. An item moves only when its evidence is in
(`bench-results/`, an ADR, tests). Ideas and objections are welcome in issues.

## Done in 0.2 and 0.3

Text ids, deletion by filter and by parent, tenants enforced by the API key, collections
created through the API, retention, partial updates, one hit per document, signed proofs of
deletion, TypeScript and Python clients, LangChain and LlamaIndex vector stores
([ADR 0031](docs/adr/0031-natural-wiring.md), [CHANGELOG](CHANGELOG.md)).

## Next

- **Ingest efficiency.** Two 50M runs ingested at about 20k docs/s with no long stall (ADR
  0029). What remains to look at:
  - builds done twice when leadership moves during ingest;
  - fetches that give up too early and rebuild locally.
- **Leader balancing under continuous ingest.** Leadership should spread evenly even while
  every shard is busy.
- **Fewer copies in the search hot path.** The largest share of search time is a memory copy
  before distance computation.

## Before a first release

- **Dynamic membership.** Add and remove nodes and replicas without a reload, through Raft
  membership changes, with shards rebalanced.
- **Key management**: rotation without restart, keys shared through the cluster instead of
  per-node files, and an external identity provider (OIDC). API keys with roles and HTTPS
  exist (ADR 0030).
- **Backups and restore**, consistent with the deletion guarantee: a restored backup must not
  bring back a document deleted after the backup was taken, unless the operator explicitly
  asks for it.
- **Rolling upgrades** across protocol versions, instead of a full-cluster restart.
- **Operational metrics** (Prometheus) and a small admin console.
- **Deletion propagation beyond Cairn** ([ADR 0024](docs/adr/0024-kafka-ingestion-and-deletion-propagation.md),
  proposed, open for discussion): Kafka ingestion with end-to-end read-your-takedown, and a
  monitor that checks that deletions reached every downstream copy.
- **Collections**: replication per collection, document counts, adding fields to a schema.
- **Proofs of deletion**: key rotation, and chaining successive proofs.

## Later

- **Multi-region** deployments.
- **Disk-resident indexes by default** for collections larger than memory (DiskANN-style,
  prototyped in ADR 0013).

## Known open problems

These are tracked in [`docs/progress.md`](docs/progress.md).

- In simulation, about 0.1% of runs with very slow disks (150 ms syncs) acknowledge no write
  under the injected faults. Safety holds in every one of them. This is a liveness problem.
- Flush files that are built but not yet published are rebuilt after a restart.
- A merge of merges can be purged before a lagging replica fetches it.
