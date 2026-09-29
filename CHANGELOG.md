# Changelog

All notable changes, per release. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Versions follow
[Semantic Versioning](https://semver.org/). Before 1.0, a minor version may break the API, the
wire protocol or the on-disk format; the notes say so.

## [Unreleased]

## [0.3.0] - 2026-09-29

First public release, as a developer preview: collections, retention, partial updates, proofs
of deletion, and LangChain and LlamaIndex integrations (ADR 0031, steps B and C).

**Upgrading from 0.2:** protocol version 7. Stop every node and start them all on 0.3. Data
written by 0.2 is kept: it becomes the `default` collection, with no migration.

### Added
- Collections (ADR 0031): `POST /v1/collections` creates a collection with its own schema and
  shards, and `DELETE /v1/collections/{c}` drops it and deletes its data on every node. Every
  document route exists under `/v1/collections/{c}/`. The collection defined at startup is
  `default`, and the routes of 0.2 act on it, unchanged and without migration. A replicated
  catalog on every node holds the definitions. Clients: `collection(name)` in TypeScript and
  Python (a view with the same calls), `createCollection`/`create_collection`,
  `listCollections`, `dropCollection`. Rust client: `set_collection`, `create_collection`,
  `drop_collection`, `list_collections`.

- `group_by` in searches (ADR 0031): one hit per value of a field, for instance each document
  once at its best chunk. Clients: `groupBy` / `group_by`, and the hit's `group`.

- LangChain (Python `langchain-cairn`, JavaScript `@cairn-db/langchain`) and LlamaIndex
  (`llama-index-vector-stores-cairn`) vector stores, in `integrations/`.
  - Any metadata round-trips, and declared fields can be filtered.
  - Deletion by source document: `delete(parent=...)`, and LlamaIndex's `delete_ref_doc`.
  - Search one hit per document, and hybrid search.
  - Tested against a live node. Python 3.10+ and Node 20+, as their frameworks require.

- Retention (ADR 0031): a collection's `expires_field` (or `--expires-field` for `default`)
  holds each document's expiry in Unix milliseconds. Expired documents are hidden from HTTP
  reads and searches at once, then deleted by the shard leaders every
  `--retention-interval-ms`, audited. Clients: `expiresField` / `expires_field` at creation.

- Partial updates (ADR 0031): `PATCH /v1/documents/{id}` and `POST /v1/documents/patch` change
  some fields (`null` clears one) and keep the others, without creating missing or expired
  documents. The shard leader resolves each patch in log order into a whole document, so a
  patch is atomic with respect to other writes. Clients: `patch`, `patchMany` / `patch_many`.

- Proof of deletion (ADR 0031): `POST /v1/deletions/proof` asks every replica whether it has
  applied a takedown and still holds the documents. It returns a report signed with the
  node's Ed25519 key (`GET /v1/deletions/key`), which `cairn-server verify-proof` checks. A
  document is proven deleted only if every replica answered and none holds it.

### Fixed
- A node forwarding a request to a peer that had restarted waited 10 s on the dead connection
  before retrying. Calls on a connection closed by the peer now fail at once, and the caller
  reconnects.

## [0.2.0] - 2026-09-29

Natural wiring (ADR 0031, step A): your own ids, deletion by filter and by parent, tenants,
and TypeScript and Python clients.

**Upgrading from 0.1:** the on-disk schema and the protocol changed. Stop every node, start
0.2 on empty data directories, and ingest again (the server says so if it finds 0.1 data).

### Added
- Text ids (ADR 0031): a document's `id` can be your own string. It is returned as written by
  reads and searches, and takedowns accept it, alone or mixed with integer ids. Each shard maps
  text ids to internal ids with a dictionary kept in step by the log.
- Deletion by filter (ADR 0031): `POST /v1/documents/delete` with a `filter` removes every
  document that matches it, for example a document and all its chunks, or everything from
  one source before a date. With `ids`, it removes only the listed documents that match. The
  answer gives the number removed and a takedown token. Each shard resolves the filter when it
  applies the deletion, so every replica removes the same documents. Rust client:
  `Client::delete_where`.
- Tenants (ADR 0031): an API key can be scoped to one tenant (`keygen ... --tenant acme`), and
  an unscoped key can act for one with the `Cairn-Tenant` header. Within a tenant, ids belong
  to the tenant, and every read, search and deletion stays inside it, enforced by the server.
  `DELETE /v1/tenants/{tenant}` erases a tenant.
- TypeScript client `@cairn-db/client` (`clients/typescript`: `fetch`, no runtime dependency,
  ESM and CommonJS) and Python client `cairn-db` (`clients/python`: sync and async, `httpx`).
  They track consistency tokens so that a client reads its own writes and never reads back
  its takedowns. Both offer deletion by parent, tenants, typed errors and retries.
  `clients/test-live.sh` tests both against a local node, and CI runs it. Neither is published
  to npm or PyPI yet.
- Acceptance suite (`examples/acceptance/`): a realistic corpus and 91-95 checks against a
  running node or cluster, in Python with no dependency.

### Fixed
- A document the log could not apply (wrong field count or type, sent through the binary
  protocol) stopped its replica, which replayed the same entry on every reopen: one bad write
  blocked a shard. Nodes now validate documents before proposing them, and replicas skip an
  invalid document instead of failing.

### Changed
- Every schema gets two reserved fields, `_key` and `_tenant`, and names starting with `_` are
  refused in user schemas. Data written by 0.1 must be ingested again. Protocol version 6.
  Binary-protocol clients read the reserved fields at the end of each document, and may send
  documents with only their own fields.
- The OpenAPI description now shows text ids, deletion by filter and tenants.
- Text ids cannot contain U+001F.

## [0.1.0] - 2026-09-28

First developer preview. Everything below is new.

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
