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
| `POST /v1/collections` | admin | `{"name", "schema", "shards"?}`; 201 with the definition, 409 if the name is taken |
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
