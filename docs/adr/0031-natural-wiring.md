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
  - **Every write of a text id takes a new internal id**, and the previous one is deleted. A
    first version reused the id found in the dictionary. The chaos campaign showed replicas
    diverging after restarts (seed 100): deletion files are persisted at publication and can
    mask rows that a later entry replaced, so a restarted replica rebuilt its dictionary
    without a text id that was still live, and gave it a new id where its peers reused the
    old one. With the counter as the only input, a restarted replica replays exactly the same
    ids.
  - Internal ids for text ids set the high bit, so they never meet client-chosen integers.
    Integer ids keep working as before.
  - The text id is stored with the document, in the reserved `_key` field (stored, not
    indexed), and returned by every read and search. The dictionary is rebuilt from that column
    when segments load and after a snapshot install; log replay does the rest.
  - The internal id carries its shard (bit 63 set, 23 bits of shard, a 40-bit counter). Every
    path that routes by id (reads, deletions, fetching hit documents) therefore finds the shard
    without a lookup.
  - Search results carry text ids: each shard adds the text ids of its keyed hits to its leg
    lists, from the dictionary, so no document read is needed.
  - In the HTTP API, an id is an integer below 2^63 or a string of 1 to 1024 bytes. In a URL
    path, digits mean an integer id (as in 0.1), and `?id_type=text` reaches a text id made of
    digits. Names starting with `_` are reserved.
- Format:
  - two reserved fields added to every schema, `_key` and `_tenant`, so no new segment section;
  - a trailing manifest field for the counter, which older manifests read as 0;
  - protocol version 6.

  Data written by 0.1 has a schema without the reserved fields and must be ingested again. The
  server says so at startup.

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
- **As implemented (step 2, 2026-09-29):**
  - `Command::DeleteWhere { scope, filter }`, where `scope` is every document, a list of ids
    or a list of text ids. The id lists are for step 3: a deletion by id under a tenant-scoped
    key is `DeleteWhere { Ids(..), _tenant == t }`.
  - `Store::apply` became asynchronous: before a `DeleteWhere` changes anything, the store
    resolves it against the rows of that log position.
    - Segments: through their filter indexes, decoding only the fields the filter reads from
      the mapped file. The index crate provides that function to the store; without it, the
      store reads the filtered columns and evaluates each row, with the same result.
    - Frozen memtables: their rows not replaced since the freeze.
    - The memtable.
    - Rows found are then deleted exactly like `Delete`, text ids included.
  - Replay after a restart may find fewer rows than the first application: deletion files
    persisted past the manifest point already mask rows that a later entry replaced or
    deleted. That later entry removes them again, so the final state is the same. Only the
    count differs, and nobody reads it on replay.
  - The leader returns the count with the token. The node sends one command per shard the
    scope reaches (every shard for a filter alone) and sums the counts. The shards are not
    atomic together: if one fails, the call fails, and repeating it is safe.
  - HTTP: `POST /v1/documents/delete` takes `filter`, alone or with `ids`, and answers
    `{"deleted", "consistency_token"}`. Filters that match everything and filters on reserved
    fields are refused. The audit event is `takedown by filter`.
  - Cost, not measured at scale yet: every `DeleteWhere` reads the filtered fields' index
    sections of all the shard's segments, on the replica's actor.

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
- **As implemented (step 3, 2026-09-29).** Two additions to the design above, decided while
  implementing it:
  - **Ids belong to their tenant.** With client-chosen ids shared by all tenants, `globex`
    writing `doc-1` would have replaced `acme`'s `doc-1`: a scoped key could overwrite a
    neighbour's document. Under a tenant, every id is therefore stored as a text id
    `<tenant>U+001F#<digits>` (integer) or `<tenant>U+001F$<text>`. Client text ids cannot
    contain U+001F, so a tenant's ids never meet another tenant's, nor ids written without a
    tenant. Answers show the id as written.
    - Other options were rejected. Checking the owner of an id before each write costs a read
      per write and races with concurrent writes. Refusing integer ids under a tenant would
      break the clients' uniform id handling.
  - **An unscoped key can act for a tenant** through a `Cairn-Tenant` header. One backend
    serving many customers needs that without holding one key per customer. A scoped key
    with a header naming another tenant gets 403.
  - Everything else is as designed:
    - `_tenant` is set on every write;
    - `_tenant == t` is added to every read, search and deletion (by id: `DeleteWhere` over
      the tenant's stored ids with `_tenant == t`; by filter: the filter and `_tenant == t`);
    - a document read by id is checked against the tenant again.
  - The erase route is `DELETE /v1/tenants/{t}` until collections exist (step B moves it
    under `/v1/collections/{c}`). It is a `DeleteWhere { All, _tenant == t }` and needs the
    `takedown` role on an unscoped key.
  - Keys: `"tenant"` in the keys-file entry, `keygen ... --tenant t`. A scoped key cannot hold
    `admin`, whose routes (status, merges) are not tenant-aware.
  - Enforcement is in the HTTP API only. The binary protocol serves nodes and trusted clients
    under mutual TLS, and is not tenant-aware. No storage or protocol change: tenants use text
    ids, `DeleteWhere` and filters from steps 1 and 2, which the simulation campaign already
    covers.
  - Validation: `crates/cairn-server/tests/http_tenants.rs` runs a node with two scoped keys
    and one unscoped key. It checks shared ids, guessed ids, hostile filters, switching
    tenants through the header, deletions by id and by filter, and erasure. Positive control:
    without the added tenant filter, the test fails.

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
- **As implemented (step B, 2026-09-29):**
  - **The catalog is an ordinary shard group**, with shard id 2^23 - 1 (shard ids fit in the
    23 bits that internal ids of text ids carry), and a replica on every node. Its documents
    are the definitions, as JSON, keyed by the collection name. A drop writes a tombstone
    `dropped/<id>` first, then deletes the name, so a node that was down still learns that
    it must delete the files.
  - **Creations and drops run on the catalog leader, one at a time.** A node that is not the
    leader forwards the request. The leader reads the catalog linearizably, picks the next
    collection id and shard range past everything ever allocated (never reused), then writes.
    - Why two leaders cannot both allocate: a deposed leader's write either reached a
      majority, and then the new leader commits it before its own first read, or it never
      commits.
  - **Each node runs a reconciler** that reads its local catalog replica every 300 ms (a
    stale read, no leader needed). It starts the replicas it hosts of live collections, and
    stops those of dropped collections before deleting their files (`ReplicaHandle::stop`).
    A node restarting alone gets its collections back this way.
  - **Each node's view of the catalog only grows.** A collection seen live is added unless it
    is known dropped, and a dropped one stays dropped. A complete linearizable listing also
    marks as dropped the collections it no longer contains. The first version replaced the
    view on each read: a node answered for a dropped collection until its reconciler caught
    up, because the listing that showed the drop had been answered by another node (the
    acceptance suite caught it).
  - **Data layout:**
    - a collection's shards live under `c<id>/shard<global shard id>`;
    - the catalog lives under `catalog/`;
    - `default` keeps shards `0..--shards` and the top-level `shard<n>` directories of 0.2, so
      nothing is migrated. `default` is not in the catalog, and cannot be dropped.
  - **Requests:**
    - document requests for a named collection travel as `Request::In { collection, req }`;
    - a node resolves the name from its view of the catalog, or else from a linearizable read
      (a collection just created through another node);
    - forwarded requests keep the wrapper.
  - **Creation answers once every shard answers a linearizable read**, within 20 s, so the
    collection works through every node as soon as the call returns.
  - **Not done:**
    - replication per collection: every collection uses `--replication`;
    - document counts in `GET /v1/collections/{c}`;
    - adding fields to an existing schema.
  - **A bug found on the way,** in existing code: a node forwarding to a peer that had
    restarted reused its dead connection and waited 10 s for a call nobody would answer.
    Fixed in the runtime: a connection closed by the peer now fails calls at once.
  - **Tests:**
    - unit: routing, catalog entries, id allocation;
    - simulator: a stopped replica reopens intact;
    - three processes (`tests/http_collections.rs`): create through one node and use through
      the others, isolation from `default` with the same ids, per-collection schema,
      deletion by parent, a node restarted alone, drop with files deleted everywhere,
      recreation of the name.
    - The reconciler and the catalog requests are node code on real threads: they are not in
      the deterministic simulation. The catalog's replication is, since it is an ordinary
      shard group.

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
- **As implemented (step 4, 2026-09-29):** `clients/typescript` and `clients/python`, with
  the shared behaviour above.
  - Not yet: `client.collection(name)`, since collections come in step B. The methods act on
    today's single collection and will stay as the `default` collection's.
  - Deviation: the TypeScript types are written by hand, not generated from the OpenAPI
    description. The API is small, and a generator would have added a tool to the build. The
    live contract tests catch drift.
  - `delete({ parent })` is a deletion by filter on a configurable parent field (`parent` by
    default). `withTenant(t)` / `with_tenant(t)` gives a view acting for a tenant through the
    `Cairn-Tenant` header, sharing the client's token. `forgetTenant` erases a tenant.
  - Tests:
    - unit tests on a fake `fetch` and on `httpx.MockTransport`;
    - live tests, one scenario for both clients, run against a fresh node by
      `clients/test-live.sh`;
    - in CI on Node 18 with Python 3.9, and Node 22 with Python 3.13.
  - Not published to npm or PyPI: that is a public release, which needs the owner's go.
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

**One hit per parent (step B.2, 2026-09-29).**
- Implemented as `group_by` in the HTTP search, without a protocol change. The search ranks
  4 × `k` candidates with their documents, keeps each group's best hit in rank order, and
  doubles the candidates (up to 10,000) while it has fewer than `k` groups and more
  candidates exist.
- Exact while one group's chunks do not fill 10,000 candidates. It reads candidate
  documents, so it costs more than a plain search.
- A grouping inside the shards (legs carrying the group value) would be cheaper. It is
  deferred until a measurement shows the need.
- Tested:
  - three processes: order, `k` groups, no documents when asked, the 400 case, and widening
    past a parent with 40 chunks (positive control: without widening, the test fails);
  - both clients' live tests.
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
