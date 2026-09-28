# ADR 0030: API keys, roles and HTTPS on the HTTP API

- Status: accepted (the owner asked for authentication before a developer preview: "go")
- Date: 2026-09-28

## Context

- The HTTP API (ADR 0023) had no authentication and no TLS. Anyone who could reach port 7200
  could read, write and delete. Node-to-node traffic already used mutual TLS (ADR 0018).
- A public Docker image, even a preview, must not open that port to everyone by default.
- The project's audience is RAG over regulated data and governance: deletions must be
  attributable to whoever asked for them.

## Options considered

1. **A proxy only** (nginx or Envoy with auth in front). No work in Cairn, but insecure by
   default, and nothing links a takedown to a caller.
2. **External identity** (OIDC/JWT). Right for enterprises later, but heavy for a first
   version: validation, key discovery, clock skew.
3. **Static API keys with roles, HTTPS in the server.** Chosen. It is simple to operate and
   test, secure by default, and leaves room for OIDC later behind the same roles.

## Decision

- **Keys**:
  - a key is a random secret: `cairn_` then 32 bytes from the system CSPRNG (`ring`), in
    URL-safe base64;
  - nodes store only SHA-256 digests, in a JSON keys file (`--http-keys`);
  - `cairn-server keygen <id> <roles>` prints a new key once, with its file entry;
  - `CAIRN_HTTP_ADMIN_KEY` adds an admin key given in clear (16 characters or more), for
    secrets shared by every node;
  - lookup hashes the presented secret and compares it with every digest in constant time.
- **Roles**:
  - `read`: reads, search, schema;
  - `write`: upserts;
  - `takedown`: deletions, separate from `write` so that deletions can be granted to a
    compliance service alone;
  - `admin`: status and merge pause, and every other role.
- **Routes**:
  - one role per route;
  - `/health` is open;
  - an unknown route needs `admin`, so a new route stays closed until it is mapped;
  - errors: 401 (with `WWW-Authenticate: Bearer`) for a missing or invalid key, 403 for a
    missing role.
- **Secure by default**:
  - with `--http-listen` and no key, the server refuses to start unless
    `--http-insecure-dev` is given;
  - `--http-keys <file> --http-generate-admin-key` creates the file with one admin key on a
    first start and prints the key once. The Docker image does this when no key is
    configured;
  - the compose cluster file requires `CAIRN_HTTP_ADMIN_KEY`.
- **HTTPS**:
  - `--http-tls-cert/--http-tls-key` serve the API with rustls (ring provider, as between
    nodes, `tokio-rustls` for the async side);
  - each connection does its handshake in its own task, with a 10 s limit, so a slow client
    cannot hold up the accept loop;
  - without TLS the server warns that keys travel in clear;
  - a node that runs mTLS between nodes still refuses plain HTTP without
    `--http-allow-plaintext`.
- **Audit**:
  - every takedown logs the key id, the document ids (the first 100) and the consistency
    token, under the target `cairn_server::audit`;
  - it is on by default even when `RUST_LOG` is unset, unless `RUST_LOG` mentions it;
  - secrets never reach the logs.
- **Dependencies**: `tokio-rustls` 0.26 is new (MIT/Apache-2.0). `ring`, `hyper` and
  `hyper-util` were already in the dependency tree.

## Evidence

- Unit tests (`crates/cairn-server/src/auth.rs`):
  - keys authenticate and roles apply, and a keys file round-trips with digests only;
  - bad files are refused;
  - every route maps to its role.
- A process test (`tests/http_auth.rs`), against a real node over HTTPS with a self-signed
  certificate:
  - 401 without a key or with a wrong one;
  - 403 for the wrong role on writes, reads, takedowns and status;
  - 200 with the right role;
  - a takedown read back as 404 with its token;
  - `/health` open, and plain HTTP on the HTTPS port gets nothing;
  - the audit line names the key, the document and the token, and holds no secret.
- A second process test: the node refuses to start without keys, and a first start generates
  a working admin key whose clear text is not stored.
- The existing HTTP tests now authenticate, over plain HTTP and next to mTLS.

## Consequences

- Clients send `Authorization: Bearer <key>`. The docs, the OpenAPI description (security
  scheme `apiKey`) and the examples are updated.
- Keys are per-node files. Rotation means editing the file on every node and restarting.
  Shared keys through the cluster, rotation without restart, and OIDC are on the roadmap.
- Clients of the binary protocol are unchanged: node mTLS, and certificates for clients.
  Roles for binary clients are future work.
