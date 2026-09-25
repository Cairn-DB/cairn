# HTTP/JSON API

Every node can serve the API (`--http-listen`, port 7200 in the Docker image). Any node
accepts any request and forwards it to the shard leaders. Machine-readable description:
[`openapi.yaml`](openapi.yaml).

```bash
docker run -p 7200:7200 cairn:dev        # one node, default schema (docker/schema.json)
```

With rootless podman, use `127.0.0.1` rather than `localhost`: `localhost` may resolve to
`::1`, and podman's port forwarding resets that connection.

## Documents

A document is a flat JSON object: an unsigned integer `id` plus fields named as in the
schema. JSON types by field kind:

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
curl -s localhost:7200/v1/documents -H 'content-type: application/json' -d '{
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
T=$(curl -s -X DELETE localhost:7200/v1/documents/1 | jq -r .consistency_token)
curl -s "localhost:7200/v1/documents/1?after=$T"        # 404, through any node
curl -s localhost:7200/v1/search -H content-type:application/json -d "{\"text\":{\"field\":\"text\",\"query\":\"nuclear\"},\"after\":\"$T\"}"
                                                         # document 1 is not in the hits
```

Consistency levels (`consistency`, on reads and searches):

| level | meaning | default |
|---|---|---|
| `linearizable` | reflects every acknowledged write in the cluster | when no `after` is given |
| `read_your_writes` | reflects the writes covered by `after` | when `after` is given |
| `stale` | any replica, whatever it has applied; fastest | never |

## Endpoints

| method and path | body or parameters | answer |
|---|---|---|
| `GET /health` | | `{"status":"ok"}` |
| `GET /v1/schema` | | the schema |
| `GET /v1/status` | | the replicas hosted by the node answering: role, term, commit and applied indexes, segments |
| `POST /v1/documents` | `{"documents":[...], "after"?}` | `{"count", "consistency_token"}` |
| `GET /v1/documents/{id}` | `?consistency=&after=` | the document, or 404 |
| `DELETE /v1/documents/{id}` | `?after=` | `{"count":1, "consistency_token"}` |
| `POST /v1/documents/delete` | `{"ids":[...], "after"?}` | `{"count", "consistency_token"}` |
| `POST /v1/search` | see below | `{"hits":[...]}` |

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
- 404: missing document;
- 503: the cluster is unreachable, or has no leader for a shard after retries;
- 500: anything else.

## Security

The HTTP port has **no TLS and no authentication** yet. A node running with mutual TLS
refuses to open it without `--http-allow-plaintext`. Bind it to a trusted interface, or put
an authenticating TLS proxy in front.
