# Changelog

All notable changes, per release. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Versions follow
[Semantic Versioning](https://semver.org/). Before 1.0, a minor version may break the API, the
wire protocol or the on-disk format; the notes say so.

## [Unreleased]

First developer preview, planned as 0.1.0. Everything below is new.

### Engine
- Hybrid queries in one plan: vector legs (HNSW, or a DiskANN-style disk-resident index), a
  BM25 text leg and structured filters, fused with RRF or weights.
- Filtered ANN that adapts to selectivity: graph search, two-hop, or an exact scan.
- SQ8 vector residency for large collections, and segments merged in tiers
  (`--target-segment-rows`).

### Cluster
- Shards replicated with Raft. Placement is static (`--replication`) and leaders are balanced.
- Segments are built once by the shard leader and shipped to followers. Lagging replicas catch
  up through snapshots.
- Consistency levels: linearizable, read-your-writes (tokens, on any node) and stale.
  Followers serve reads.
- Takedowns are replicated writes. Once acknowledged, no read carrying the takedown's token
  returns the document again, on any node.
- Asynchronous Raft persistence with the durability rules kept (ADR 0027). Merges can be paused
  per node (ADR 0028).
- Mutual TLS between nodes, with protocol and segment-format versions checked on every
  connection (ADR 0018).

### API and packaging
- HTTP/JSON API with an OpenAPI description (ADR 0023). Rust client for the binary protocol.
- API keys with roles (read, write, takedown, admin), HTTPS, and a takedown audit trail
  (ADR 0030). The API refuses to start without keys, unless `--http-insecure-dev`.
- Docker image, non-root. The first start generates an admin key. Compose files for 1 and 3
  nodes. The server uses mimalloc (ADR 0029).

### Measured
- 50M 128-d vectors on 3 × 8 vCPU nodes, queried without a restart after ingest:
  - p99 of 35 ms unfiltered and 31 ms with a 1% filter;
  - 435 QPS;
  - takedowns visible everywhere within 102 ms at p99;
  - ingest at 19.7k-20.7k docs/s (`bench-results/phase4-gcp-bigann50m-run9.md` and
    `run10.md`).
- 60,000 seeds of deterministic fault-injection simulation with zero safety violations.

### Known limits
Static membership: a node that loses its disk cannot rejoin yet. One region. Per-node API key
files. No online backup. Upgrades across protocol versions need a full restart. See the README
and `docs/deployment.md`.
