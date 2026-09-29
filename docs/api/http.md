# HTTP/JSON API

Every node can serve the API (`--http-listen`, port 7200 in the Docker image). Any node
accepts any request and forwards it to the shard leaders. Machine-readable description:
[`openapi.yaml`](openapi.yaml).

```bash
docker run -p 7200:7200 cairn:dev        # one node, default schema (docker/schema.json)
docker logs <container> | grep cairn_    # the admin API key generated on the first start
export KEY=cairn_...                     # used as "Authorization: Bearer $KEY" below
```

With rootless podman, use `127.0.0.1` rather than `localhost`: `localhost` may resolve to
`::1`, and podman's port forwarding resets that connection.

## Collections

A collection has its own schema, shards and documents (ADR 0031). The collection defined at
startup (`--schema`) is `default`: the routes without a collection name act on it, as in 0.1
and 0.2.

```bash
curl -s -X POST localhost:7200/v1/collections -H "Authorization: Bearer $KEY" \
  -H content-type:application/json -d '{
    "name": "notes",
    "shards": 4,
    "schema": { "fields": [
      { "name": "embedding", "kind": { "Vector": { "dims": 384, "metric": "Cosine" } } },
      { "name": "body", "kind": "Text" },
      { "name": "parent", "kind": "Enum" } ] } }'
# 201 {"name":"notes","shards":4,"schema":{...}}
curl -s localhost:7200/v1/collections/notes/search -H "Authorization: Bearer $KEY" \
  -H content-type:application/json -d '{"text":{"field":"body","query":"otter"}}'
```

| method and path | role | answer |
|---|---|---|
| `POST /v1/collections` | admin | `{"name", "schema", "shards"?, "expires_field"?}`; 201 with the definition, 409 if the name is taken |
| `GET /v1/collections` | read | `{"collections":[...]}`, `default` first |
| `GET /v1/collections/{c}` | read | the definition |
| `DELETE /v1/collections/{c}` | admin | drops it and deletes its data on every node |
| `/v1/collections/{c}/documents`, `/documents/{id}`, `/documents/delete`, `/search`, `/schema`, `/tenants/{t}` | as for `default` | as for `default` |

- A name has 1 to 64 characters among `a-z`, `0-9`, `_` and `-`.
- `shards` defaults to the shard count of `default`.
- The schema of a collection is fixed. To change it, create a new collection.
- Creating a collection answers once each of its shards has a leader, usually within a
  second. It then works through every node.
- Dropping a collection is final: its documents are gone and each node deletes its files. The
  name can then be used again, for a new, empty collection. `default` cannot be dropped.
- Ids, tenants and deletions by filter work in each collection as in `default`. A document id
  identifies a document within its collection only.
- Every collection uses the node's `--replication`. Per-collection replication and document
  counts come later.

## Retention

A collection can expire its documents (ADR 0031): name a field that holds each document's
expiry, in Unix milliseconds, as `expires_field` when creating the collection (an `I64` or
`Date` field), or with `--expires-field` for `default`.

```bash
curl -s -X POST localhost:7200/v1/collections -H "Authorization: Bearer $KEY" \
  -H content-type:application/json -d '{"name": "sessions", "expires_field": "until",
  "schema": {"fields": [{"name": "text", "kind": "Text"}, {"name": "until", "kind": "Date"}]}}'
```

- **Reads.** From its expiry on, a document is never returned by the HTTP API, neither by
  reads nor by searches.
- **Deletion.** The shard leaders delete expired documents every `--retention-interval-ms`
  (10 s by default). Each deletion is a deletion by filter carrying the time, so every
  replica deletes the same documents. The audit log records it (`expired documents deleted`,
  with the collection, shard, count, time and token).
- A document without a value in the field never expires. Writing a new expiry postpones it.
- The binary protocol does not hide expired documents between sweeps.

## Documents

A document is a flat JSON object: an `id` plus fields named as in the schema.

**Ids** (ADR 0031):
- An `id` is either your own string of 1 to 1024 bytes (`"article/2024-17"`, a UUID, anything),
  or an unsigned integer below 2^63.
- Reads, searches and takedowns return the id as you wrote it.
- Writing a document again with the same id replaces it.
- In a URL path, digits mean an integer id. A string id made only of digits is reached with
  `?id_type=text`, for example `GET /v1/documents/12345?id_type=text`. Encode other characters
  as usual (`%2F` for `/`).
- Field names starting with `_` are reserved.
- A text id cannot contain the character U+001F, which separates a tenant from its ids.

JSON types by field kind:

| field kind | JSON |
|---|---|
| Vector | array of numbers (exact dimension) |
| Text, Enum | string |
| I64, Date | integer (for dates, days or milliseconds since an epoch you choose) |
| F64 | number |
| Bool | boolean |
| Set | array of strings |
| Blob | base64 string |

A missing field or `null` means no value. An upsert replaces the whole document.

```bash
curl -s localhost:7200/v1/documents -H "Authorization: Bearer $KEY" -H 'content-type: application/json' -d '{
  "documents": [
    { "id": 1, "text": "nuclear energy debate", "source": "tv", "tags": ["politics"],
      "created": 19000, "embedding": [0.1, 0.2, ...] }
  ]}'
# {"count":1,"consistency_token":"2.17"}
```

## The consistency token, and the deletion guarantee

Every write returns a `consistency_token`. Pass it as `after` on later reads, and they
reflect that write, on any node, even one that lags. The token grows: pass the previous
token as `after` on the next write, and the new token covers both.

This is what makes a takedown final for the client that asked for it:

```bash
T=$(curl -s -H "Authorization: Bearer $KEY" -X DELETE localhost:7200/v1/documents/1 | jq -r .consistency_token)
curl -s -H "Authorization: Bearer $KEY" "localhost:7200/v1/documents/1?after=$T"   # 404, through any node
curl -s localhost:7200/v1/search -H "Authorization: Bearer $KEY" -H content-type:application/json -d "{\"text\":{\"field\":\"text\",\"query\":\"nuclear\"},\"after\":\"$T\"}"
                                                         # document 1 is not in the hits
```

Consistency levels (`consistency`, on reads and searches):

| level | meaning | default |
|---|---|---|
| `linearizable` | reflects every acknowledged write in the cluster | when no `after` is given |
| `read_your_writes` | reflects the writes covered by `after` | when `after` is given |
| `stale` | any replica, whatever it has applied; fastest | never |

## Endpoints

| method and path | role | body or parameters | answer |
|---|---|---|---|
| `GET /health` | none | | `{"status":"ok"}` |
| `GET /v1/schema` | read | | the schema |
| `GET /v1/status` | admin | | the replicas hosted by the node answering: role, term, commit and applied indexes, segments |
| `POST /v1/documents` | write | `{"documents":[...], "after"?}` | `{"count", "consistency_token"}` |
| `GET /v1/documents/{id}` | read | `?consistency=&after=` | the document, or 404 |
| `DELETE /v1/documents/{id}` | takedown | `?after=` | `{"count":1, "consistency_token"}` |
| `PATCH /v1/documents/{id}` | write | `{"set": {...}, "after"?}` | `{"patched", "consistency_token"}` |
| `POST /v1/documents/patch` | write | `{"patches": [{"id", "set"}], "after"?}` | `{"patched", "consistency_token"}` |
| `POST /v1/deletions/proof` | takedown | `{"ids":[...], "after"}` | a signed report (see Proof of deletion) |
| `GET /v1/deletions/key` | read | | `{"public_key", "algorithm"}` |
| `POST /v1/documents/delete` | takedown | `{"ids":[...], "after"?}`, or `{"filter":{...}, "ids"?, "after"?}` | `{"count", "consistency_token"}`; with a filter, `{"deleted", "consistency_token"}` |
| `POST /v1/search` | read | see below | `{"hits":[...]}` |
| `…/v1/collections/…` | | see Collections | |
| `DELETE /v1/tenants/{tenant}` | takedown, unscoped key | `?after=` | `{"deleted", "consistency_token"}`: erases the tenant |
| `GET /v1/admin/merges` | admin | | `{"paused"}` for the node answering |
| `POST /v1/admin/merges` | admin | `{"paused": true}` | `{"paused"}`: pauses or resumes merges on the node answering |

Merge pause (ADR 0028): a paused node starts no new merge. Merges already running, and merges
committed in a shard's log, still complete; `merges_running` and `merges_pending` in
`/v1/status` show when none is left. The call applies to one node: call it on every node to
pause the cluster. It is an administrative endpoint with no authentication (see Security).

## Proof of deletion

After a takedown, `POST /v1/deletions/proof` asks **every replica** of the documents' shards
whether it has applied the takedown and still holds them. It answers with a report signed by
the node (ADR 0031):

```bash
T=$(curl -s -X DELETE -H "Authorization: Bearer $KEY" localhost:7200/v1/documents/user-42 | jq -r .consistency_token)
curl -s -H "Authorization: Bearer $KEY" localhost:7200/v1/deletions/proof \
  -H content-type:application/json -d "{\"ids\": [\"user-42\"], \"after\": \"$T\"}" > proof.json
cairn-server verify-proof proof.json --public-key "$(curl -s -H "Authorization: Bearer $KEY" localhost:7200/v1/deletions/key | jq -r .public_key)"
# signature OK (with the given public key); verdict: deleted everywhere
```

- The report lists, for each document, its shard and its `verdict`. It is `deleted` only if
  every replica answered, had applied the token, and does not hold the document. Otherwise it
  is `not proven`, with the reasons: a node that did not answer, a replica not caught up, a
  replica that still holds the document. For each shard, it lists every replica with its
  applied index.
- The report's `verdict` is `deleted everywhere` only if every document is.
- The signature is Ed25519, over the report serialized as compact JSON with sorted keys.
  Each node has its own key, created on first start (`--proof-key`, by default
  `<data>/proof-key.pk8`), and `GET /v1/deletions/key` gives it. Record a node's public key
  beforehand, and check proofs against it. The key embedded in a proof only shows that the
  report was not changed after signing.
- It needs the `takedown` role. Under a tenant, the ids are the tenant's. It is audited
  (`deletion proof`).
- What it proves: at the time of the check, no replica serves the document. It does not
  prove anything about copies outside Cairn (backups, exports, logs). Segment files may keep
  the deleted bytes until a merge rewrites them: a masked row is never returned, but it is
  not wiped from disk at once.

## Partial updates

```bash
curl -s -X PATCH localhost:7200/v1/documents/sku-42 -H "Authorization: Bearer $KEY" \
  -H content-type:application/json -d '{"set": {"price": 1990, "promo": null}}'
# {"patched": 1, "consistency_token": "2.88"}
```

- `PATCH /v1/documents/{id}` with `{"set": {...}}` changes the named fields of one document
  and keeps the others. A value sets the field, and `null` clears it.
  `POST /v1/documents/patch` with `{"patches": [{"id": ..., "set": {...}}]}` changes several
  documents. Both need the `write` role.
- A missing document is not created (`"patched": 0`), and neither is an expired one.
- Each patch is atomic with respect to other writes. The shard leader reads the document at
  the patch's place in the log order, then writes the whole document. A concurrent write to
  the same document lands entirely before or entirely after it.
- Reserved fields and the id cannot be patched.

## Deletion by filter

`POST /v1/documents/delete` with a `filter` removes every document that matches it (ADR 0031),
with the filter syntax of searches:

```bash
# Delete a document and all its chunks, stored with the parent's id in a "parent" field.
curl -s -H "Authorization: Bearer $KEY" localhost:7200/v1/documents/delete \
  -H content-type:application/json -d '{"filter": {"field": "parent", "eq": "report-9"}}'
# {"deleted": 14, "consistency_token": "0.212,1.198"}
```

- It removes the documents that match **when each shard applies the deletion**, in its log
  order. It is not a standing rule: a document written afterwards is not affected.
- With `ids`, only those of the listed documents that match are removed. Step 3 of ADR 0031
  (tenants) uses this for deletions by id under a tenant-scoped key.
- The answer counts the removed documents, and its token is a takedown token like any other:
  a read or a search that passes it never returns a removed document, on any node.
- A filter that matches every document (`{}`, `{"and": []}`) is refused. Reserved fields
  (names starting with `_`) cannot be filtered on.
- It needs the `takedown` role, and the audit log records the key, the filter, the count and
  the token (`takedown by filter`).
- Each shard runs its part as one log entry. If a shard fails (no leader after retries), the
  call answers 503 and the other shards may already have deleted their part. Deletion is
  idempotent, so the call can simply be repeated.

## Tenants

A tenant is a customer, a user, or any unit whose data must stay apart (ADR 0031). A request
acts for one tenant in two ways:
- its key is **scoped** to that tenant (`cairn-server keygen app read,write,takedown --tenant
  acme`). Hand such a key to code that must only ever see `acme`.
- an **unscoped** key names the tenant in a `Cairn-Tenant: acme` header. This is how one
  backend serves all its customers. A scoped key with a header naming another tenant gets 403.

Tenant names have 1 to 128 letters, digits, `_`, `.` or `-`.

Within a tenant:
- **Ids belong to the tenant.** `doc-1` in `acme` and `doc-1` in `globex` are two documents,
  and neither is the `doc-1` written without a tenant. Integer ids work the same way.
- **Writes** are stored for the tenant.
- **Reads, searches and deletions**, by id or by filter, only ever reach the tenant's
  documents. The server adds the restriction itself, so an application that forgets a filter,
  guesses an id or sends a hostile filter still cannot reach another tenant. Filters on
  reserved fields (`_tenant`) are refused.
- **Answers** show ids as written, without the tenant.

Without a tenant, an unscoped key sees everything. Hits and documents that belong to a tenant
carry a `_tenant` field. To read one by id, name its tenant in the header.

**Erasing a tenant.** `DELETE /v1/tenants/acme` removes every document of `acme`, on every
shard, and answers `{"deleted", "consistency_token"}`, as a deletion by filter does. It needs
the `takedown` role on an unscoped key. It is audited (`tenant erased`, with the key, the
tenant, the count and the token). Documents written for the tenant afterwards are not
affected.

```bash
curl -s -X DELETE -H "Authorization: Bearer $KEY" localhost:7200/v1/tenants/acme
# {"deleted": 1250, "consistency_token": "0.88,1.91,2.87,3.90"}
```

Scope: tenants are enforced by the HTTP API. The binary protocol between nodes and trusted
clients (mutual TLS) is not tenant-aware.

## Search

```json
{
  "k": 10,
  "vector": { "field": "embedding", "values": [0.1, 0.2], "ef": 0 },
  "text": { "field": "text", "query": "nuclear energy", "all_terms": false },
  "filter": { "and": [ { "field": "source", "eq": "tv" },
                       { "field": "created", "gte": 19000, "lt": 19500 } ] },
  "fusion": { "rrf": { "k": 60 } },
  "with_documents": true,
  "consistency": "stale",
  "after": "2.17"
}
```

- Legs: zero or more vector legs (`vector`, or `vectors` for several) and at most one text
  leg. With no legs, a query is a pure filter.
- `fusion`: `{"rrf":{"k":60}}` (the default) or `{"weighted":[0.7,0.3]}`, with one weight per
  leg: vectors first, then text.
- `filter`: `and`, `or`, `not`, or `{"field": name, op: value}` with the operators `eq`,
  `in`, `gt`, `gte`, `lt`, `lte` and `is_null`. On a Set field, `eq` and `in` mean
  "contains".
- Each hit has `id`, the fused `score`, `legs` (rank and raw score in each leg, `null`
  where the document is absent from that leg), and the `document` unless
  `with_documents` is false.
- `group_by` (a scalar field, for instance `"parent"`) returns at most one hit per value of
  that field: each document once, at the rank of its best chunk. Hits carry the value as
  `group`.
  - The search ranks more candidates than `k` (4 times, doubling up to 10,000) until it has
    `k` groups or runs out of candidates. If a few parents own more than 10,000 of the best
    chunks, fewer than `k` groups come back.
  - It reads each candidate's document, so it costs more than a plain search.

## Errors

Errors come back as `{"error": "message"}`:
- 400: invalid input (unknown field, wrong type or dimension, bad filter or token);
- 401: missing or invalid API key;
- 403: the key lacks the role the endpoint needs;
- 404: missing document;
- 503: the cluster is unreachable, or has no leader for a shard after retries;
- 500: anything else.

## Authentication and security

Every request except `/health` carries an API key (ADR 0030):
`Authorization: Bearer <key>`.

**Roles.** Each key holds one or more roles:
- `read`: reads, searches and the schema;
- `write`: inserts and replacements;
- `takedown`: deletions, kept apart from `write` so that deletions can be granted to a
  compliance service alone;
- `admin`: status and merge pause, and every other role.

**Keys.**
- `cairn-server keygen <id> <roles> [--tenant <name>]` prints a new key once, with the entry
  to add to the keys file (`--http-keys`). The file holds only SHA-256 digests. A key with a
  tenant reaches that tenant only (see Tenants); it cannot hold the `admin` role.
- `CAIRN_HTTP_ADMIN_KEY` adds an admin key given in clear, for instance from a secret shared by
  every node.
- On a first start, `--http-keys <file> --http-generate-admin-key` creates the file with one
  admin key and prints that key once. This is what the Docker image does when no key is
  configured.
- Without any key the node refuses to serve the API, unless `--http-insecure-dev` says
  otherwise (development only).

**TLS.** `--http-tls-cert/--http-tls-key` serve the API over HTTPS. Without them, keys travel
in clear and the node warns at startup: use TLS, or an HTTPS proxy in front. A node that runs
mutual TLS between nodes refuses plain HTTP unless `--http-allow-plaintext` confirms it.

**Audit.** Every takedown is logged with the key id, the tenant, the document ids (or the
filter and the count) and the consistency token (log target `cairn_server::audit`, on by default). Secrets never reach the logs.
