# ADR 0031: Natural wiring: text ids, deletion by filter, tenants, collections, clients

- Status: accepted (the owner validated the design on 2026-09-29)
- Date: 2026-09-29

## Context

- The engine is measured and the 0.1 preview works, but wiring Cairn into an application is
  not natural yet. The owner, as a developer: "j'ai encore du mal à voir comment câbler la
  database".
- `docs/use-cases.md` lists the uses by sector. Six needs come back in most of them:
  1. text identifiers;
  2. deletion by criterion;
  3. parent documents and their chunks;
  4. multi-tenancy;
  5. collections created through the API;
  6. clients and integrations: Python, TypeScript, LangChain, LlamaIndex.
- Cairn today:
  - 64-bit integer ids chosen by the client, routed with `xxh3(id) % shards`;
  - deletion by id only (`Command::Delete(Vec<DocId>)`);
  - no tenant;
  - one schema per cluster, fixed by a file at startup;
  - HTTP and a Rust client only.
- This ADR designs needs 1 to 6. Retention, partial updates, Kafka (ADR 0024) and an exportable
  proof of deletion come after (see Phasing).

## Decisions

### 1. Text identifiers: a dictionary per shard

Options:
- **Hash the text id to 64 bits.** Simple, but two ids can collide and silently become the same
  document. The probability is about 7e-5 at 50M documents and 3% at 1 billion. That is not
  acceptable for a database whose promise is about which document is gone.
- **Store text ids everywhere instead of integers.** Exact, but it touches every layer (indexes,
  deletion bitmaps, the wire format) for little gain.
- **Chosen: a dictionary per shard.**
  - A text id routes by `xxh3(text) % shards`.
  - The shard maps each text id to an internal `DocId`, assigned when the command is applied,
    from a counter kept in the shard's manifest. Replicas apply the same log, so they assign
    the same ids.
  - Internal ids for text ids set the high bit, so they never meet client-chosen integers.
    Integer ids keep working as before.
  - The text id is stored with the document (a reserved column) and returned by every read and
    search. The dictionary is rebuilt from that column when segments load, plus the memtable.
- Format: a new segment section (the id column) and a manifest field (the counter). Segment
  format and protocol versions are bumped (ADR 0018).

### 2. Deletion by filter, resolved in the log

- **A new command, `DeleteWhere { filter }`.** Each replica resolves the filter when it applies
  the command, against the rows of that log position: segments, frozen memtables and the
  memtable. It then deletes those rows exactly like `Delete`. Every replica applies the same
  log position with the same logical rows, so all of them delete the same documents.
- **Scope.** It deletes what matches at that point of the log; it is not a standing rule. A
  document written afterwards is not affected. The API says so.
- **Evaluation** uses the segments' filter indexes (bitmaps) and scans the memtables. It runs
  when the command is applied, in log order, and it is bounded by the matching rows.
- **Response and audit.** The call returns the number of deleted documents and a consistency
  token. The audit log records the key, the filter, the count and the token (ADR 0030).
- **Parent documents and chunks** need nothing more. A chunk carries its parent's id in an
  ordinary field, and "delete this document" is `DeleteWhere { parent == "doc-42" }`. The
  clients name that operation.

### 3. Tenants enforced by the API key

- **A reserved `_tenant` field**, a keyword with an exact-match index, available in every
  collection.
- **An API key can be scoped to one tenant** (`"tenant": "acme"` in its keys-file entry, or at
  `keygen`). For such a key, the server:
  - sets `_tenant` on every write and refuses a document that names another tenant;
  - adds `_tenant == acme` to every read, search and deletion, including deletion by id. A
    deletion by id is sent as `DeleteWhere { id ∈ ids ∧ _tenant == acme }`, so it cannot
    remove a neighbour's document even with a guessed id.
- An unscoped key (admin) sees every tenant and can erase one:
  `DELETE /v1/collections/{c}/tenants/{t}`, which is a `DeleteWhere { _tenant == t }`.
- This is how per-user agent memory ("forget me") and per-customer SaaS data are wired, with
  isolation that does not depend on the application remembering a filter.

### 4. Collections created through the API

Options:
- **Several schemas declared in the startup file.** Cheap, but it still needs a restart for
  every new collection.
- **Collections as namespaces inside the same shards.** Segments are built per schema, so
  mixing schemas in one segment is not possible without a rewrite.
- **Chosen: each collection has its own shard groups, created from a replicated catalog.**
  - A catalog Raft group, on every node, holds the collection definitions: name, schema,
    shards, replication.
  - When a node applies `CreateCollection`, it starts its replicas of the new collection's
    shards, under `data/<collection>/`. Placement follows ADR 0015 over the existing, static
    nodes. This is not a membership change.
  - `DropCollection` stops those replicas and deletes their files everywhere. It is audited as
    a takedown of the whole collection.
  - The schema of an existing collection is fixed. Adding optional fields comes later, and
    changing the vector dimension means a new collection.
- **Compatibility.** The collection created from `--schema` at startup is named `default`, and
  the current `/v1/documents` and `/v1/search` routes act on it.

### 5. HTTP API

```
POST   /v1/collections                        {name, schema, shards?, replication?}
GET    /v1/collections                        list
GET    /v1/collections/{c}                    definition and counts
DELETE /v1/collections/{c}                    drop (admin)
POST   /v1/collections/{c}/documents          upsert; "id" may be a string or an integer
GET    /v1/collections/{c}/documents/{id}
DELETE /v1/collections/{c}/documents/{id}
POST   /v1/collections/{c}/delete             {"ids": [...]} | {"filter": {...}} -> {count, consistency_token}
POST   /v1/collections/{c}/search             unchanged body; hits carry the id as written
DELETE /v1/collections/{c}/tenants/{t}        erase a tenant (unscoped keys)
```

Errors, roles (ADR 0030) and consistency tokens work as today. The OpenAPI description is
updated with every step.

### 6. Clients and integrations

Both clients share one design and live in this repository, tested in CI against a live node
(the acceptance suite, `examples/acceptance/`):

- **TypeScript** `@cairn-db/client` (npm):
  - built on `fetch`, with no runtime dependency, for Node 18+, Deno and Bun;
  - types generated from the OpenAPI description, under a hand-written ergonomic layer;
  - ESM and CommonJS.
- **Python** `cairn-db` (PyPI):
  - built on `httpx`, sync and async, typed (dataclasses and `TypedDict`);
  - `httpx` is the only dependency.
- **Shared behaviour**:
  - `client.collection("docs")` exposes `upsert`, `get`, `search`, `delete({ids} | {filter} |
    {parent})` and `forgetTenant(t)`;
  - filters are plain objects, or built with small helpers (`eq`, `in_`, `range`, `and_`,
    `or_`, `not_`);
  - consistency tokens are tracked per client, so a client reads its own writes and
    takedowns by default, and `client.token` can hand them to another service;
  - retries on 503 with a leader hint;
  - typed errors: authentication, forbidden, not found, invalid input.
- **Integrations**:
  - LangChain (`langchain-cairn` in Python, `@cairn-db/langchain` in JavaScript) and LlamaIndex
    (Python), as vector-store adapters;
  - they need text ids, metadata filters and deletion by id and filter, which is why they come
    after sections 1 and 2;
  - embeddings stay on the application side: the adapters take the framework's embedding
    object. Server-side embedding is not planned. It would tie Cairn to a model and its
    hardware.

## Phasing

| step | content | release |
|---|---|---|
| A | text ids; `DeleteWhere` (by filter and by parent); tenant-scoped keys and tenant erasure; TypeScript and Python clients on the `default` collection; acceptance suite extended | 0.2 |
| B | collections (catalog, create and drop), `/v1/collections/...` routes, a search option to return one hit per parent; LangChain and LlamaIndex adapters | 0.3 |
| C | retention (an expiry field, deleted through `DeleteWhere` proposed by the leader with the time in the command), partial updates (`Patch`, applied against the current document), Kafka ingestion (ADR 0024), exportable proof of deletion | later |

Step A changes the segment format and the protocol: upgrading from 0.1 needs the
full-cluster restart that ADR 0018 already requires across versions. Step B adds the catalog
and its files. Existing data becomes the `default` collection without migration.

## Validation

- **Property tests**: the dictionary assigns the same ids on every replica whatever the order
  of segment loads; `DeleteWhere` deletes exactly the rows a local evaluation finds, over
  segments, frozen memtables and the memtable.
- **The simulation campaign** gets text ids, `DeleteWhere`, tenants and (step B) collection
  create and drop, with the same checks. The deletion checks gain one invariant: no read with
  a later token returns a row matched by an applied `DeleteWhere`.
- **Isolation tests**: a tenant-scoped key never reads, searches, lists or deletes another
  tenant's document, including with guessed ids and hostile filters.
- **The acceptance suite and both clients' tests** run against a live node in CI.

## Consequences

- An application wires Cairn in with its own ids, its own tenants and its own document
  structure. "Forget this user", "delete this document and its chunks" and "erase this
  customer" are one call each.
- The deletion guarantee extends from ids to criteria. That is the property the project's
  community is meant to gather around.
- The work is significant. Step A alone touches storage (ids, `DeleteWhere`), the server
  (tenants) and adds two clients. Step B adds dynamic creation of shard groups.
- `SPEC.md` gains collections, tenants and deletion by filter once this ADR is accepted.
