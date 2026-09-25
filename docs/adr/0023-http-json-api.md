# ADR 0023: HTTP/JSON API

- Status: accepted (delegated 2026-09-25; requested by the owner: "vas-y pour l'API HTTP")
- Date: 2026-09-25

## Context

The only way into Cairn was its binary protocol, spoken by the Rust client. The Docker
image (ADR 0022) is meant for developers who work in Python and TypeScript. The web
console and the admin UI need an API a browser can call. Competitors (Qdrant, Meilisearch,
Weaviate, Elasticsearch) all offer JSON over HTTP, so that is what users expect.

## Options considered

1. **Where it runs.** In every node, on a second port; or in a separate gateway process.
   In the node, one container is enough and any node answers. A gateway adds a process to
   deploy and monitor. Chosen: in the node, opt-in with `--http-listen`.
2. **How it reaches the engine.** Through the coordinator directly, or through the existing
   blocking client over loopback. The client already follows leader hints, retries, and
   carries consistency tokens. Reusing it means the HTTP path has no replication logic of
   its own, which reliability favors. The extra cost is one loopback round trip.
   Chosen: the client, pooled.
3. **HTTP stack.** axum 0.8 on hyper and tokio; tiny_http (synchronous, small, less used in
   production); or a hand-written HTTP/1.1 parser (a security risk). Reliability comes
   first: hyper is the most widely deployed Rust HTTP implementation. tokio and axum add
   about 40 crates to the build, confined to `cairn-server`. Engine crates stay free of
   them, and determinism (ADR 0011) is untouched. Chosen: axum, HTTP/1.1 only for now.

## Decision

- Routes: `/health`, `/v1/schema`, `/v1/status`, `POST /v1/documents`,
  `GET|DELETE /v1/documents/{id}`, `POST /v1/documents/delete`, `POST /v1/search`
  (`docs/api/http.md`, `docs/api/openapi.yaml`).
- **Documents are flat JSON** keyed by field name, plus `id`. Types are checked against the
  schema before anything is sent, so input errors are 400 with a message.
- **Consistency token.** Writes return a `consistency_token` (per-shard log positions). A
  read or search that passes it as `after` reflects those writes on any node. With no
  explicit level, a read is `read_your_writes` when `after` is given and `linearizable`
  otherwise: correct by default, with `stale` opt-in. The token passed to a write is merged
  into the one returned, so a stateless client needs to keep a single string.
- **Filters** are JSON: `and`, `or`, `not`, `{"field", eq|in|gt|gte|lt|lte|is_null}`.
- **Security.** The HTTP port has no TLS and no authentication. A node with mutual TLS
  refuses to open it unless `--http-allow-plaintext` is given. When it is, the internal
  client uses the node's own certificate. In the Docker image the API is on port 7200 by
  default, and off when `CAIRN_TLS_DIR` is set, unless explicitly enabled.
- Bodies are limited to 64 MiB (`--http-max-body`). Requests run on tokio's blocking pool
  (at most 64 at once), and each gets the client's 10 s timeout and retries.

## Evidence

- Unit tests: documents round-trip through JSON (every type, including base64 blobs);
  wrong dimensions, unknown fields and missing ids are rejected; filters parse; tokens merge
  per shard.
- Process test with 3 nodes over HTTP, 3 runs, all green:
  - writes through one node, reads back through another with the token;
  - hybrid search with a filter, checked against every hit;
  - a takedown through node 2 returns 404 through all three nodes, and is absent from their
    search results;
  - bulk takedown works, input errors return 400, a missing document 404;
  - node 1 killed, writes and reads through the others continue.
- **Bug found by the first curl session, fixed here.** A search with a filter and no legs
  returned nothing through the cluster. The single-node engine had a filter-only path, but
  shards answered the coordinator with empty leg lists. Now each shard sends its first `k`
  matches in id order, and the coordinator merges them. The process test checks it: the 30
  matching documents across 3 shards, in order. It fails without the fix.
- Campaign: 3,000 seeds, zero violations after the engine change. Workspace: 114 tests pass.
- Process test with mTLS: HTTP next to mutual TLS is refused without the flag; with it,
  documents are written and read through the internal mTLS client.

## Consequences

- TypeScript and Python clients can be thin, and even generated from `openapi.yaml`. The
  web console can call the API directly.
- Every HTTP request costs one more loopback hop and a JSON encoding. This was not
  measured; the bench still uses the binary client.
- `/v1/status` cannot name shards: the binary status carries no shard id. That needs
  protocol 5.
- Rootless podman: `localhost` may resolve to `::1`, which its port forwarding resets. Use
  `127.0.0.1` (docs/api/http.md).
- Missing before production: TLS on the HTTP port, authentication and roles (the same gap
  as ADR 0018), request metrics, and hard per-client limits.
