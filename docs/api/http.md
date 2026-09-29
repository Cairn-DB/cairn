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
| `POST /v1/documents/delete` | takedown | `{"ids":[...], "after"?}` | `{"count", "consistency_token"}` |
| `POST /v1/search` | read | see below | `{"hits":[...]}` |
| `GET /v1/admin/merges` | admin | | `{"paused"}` for the node answering |
| `POST /v1/admin/merges` | admin | `{"paused": true}` | `{"paused"}`: pauses or resumes merges on the node answering |

Merge pause (ADR 0028): a paused node starts no new merge. Merges already running, and merges
committed in a shard's log, still complete; `merges_running` and `merges_pending` in
`/v1/status` show when none is left. The call applies to one node: call it on every node to
pause the cluster. It is an administrative endpoint with no authentication (see Security).

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
- `cairn-server keygen <id> <roles>` prints a new key once, with the entry to add to the
  keys file (`--http-keys`). The file holds only SHA-256 digests.
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

**Audit.** Every takedown is logged with the key id, the document ids and the consistency
token (log target `cairn_server::audit`, on by default). Secrets never reach the logs.
