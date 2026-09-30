# 3. Inside a backend

## One client per process

```python
cairn = Client(URL, api_key=os.environ["CAIRN_KEY"])       # one per process
articles = cairn.collection("articles")                    # views: cheap, shared pool
```

Create the client once, at startup, and share it. `collection()` and `with_tenant()` return
views that share its connection pool and its consistency token: making one per request costs
nothing. The Python client is safe to share between threads. `AsyncClient` has the same
methods for asyncio (FastAPI's `async def` handlers). In TypeScript, `new Cairn({...})` works
the same way.

Give the client every node's address if you run a cluster:
`Client(["http://n1:7200", "http://n2:7200", "http://n3:7200"], ...)`. It retries a call on
another node on network errors and on 502, 503 and 504.

## Search

```python
def search(db: Client, question: str, *, product: str | None = None, k: int = 5):
    hits = db.search(
        k=k,
        vector={"field": "embedding", "values": embed(question)},
        text=question,
        text_field="text",                      # hybrid: vector + BM25, fused
        filter=eq("product", product) if product else None,
        group_by="parent",                      # one hit per article, at its best chunk
    )
    return [
        {"article": h.group, "title": h.document["title"], "snippet": h.document["text"][:200]}
        for h in hits
    ]
```

- **Hybrid by default for RAG.** The vector leg finds paraphrases; the BM25 leg finds exact
  terms (product names, error codes). They are fused with reciprocal rank fusion. For a
  weighted fusion, pass `fusion={"weighted": [0.7, 0.3]}` (vectors first, then text).
- **Filters** are built with `eq`, `in_`, `range_`, `is_null`, `and_`, `or_` and `not_`, and
  run before ranking, even when they select 1% of the collection or less.
- **`group_by="parent"`** returns each article once, at the rank of its best chunk, with that
  chunk as `h.document`, and the article in `h.group`.
- `with_documents=False` returns ids and scores only: faster when you only need ids.
- `k` is at most 10,000.

## Consistency tokens

Every write and deletion returns a **consistency token**. A read that passes it reflects that
write or deletion, on any node. Within one client, this is automatic.

Across processes and services, pass the token along:

```python
token = cairn.token                                      # after a write or a takedown
other_service = Client(URL, api_key=ADMIN_KEY, token=token)
# other_service's reads reflect those writes and takedowns, on any node.
```

In a web backend behind a load balancer, the next request may hit another instance of your
service. Return the token to the caller (for instance in an `X-Cairn-Token` response header),
and have the caller send it back. The recipes (chapter 7) do exactly this:

```python
def kb(customer: str, token: str | None) -> Client:
    if token:
        cairn.observe(token)          # this request now sees that write, on any node
    return cairn.collection("articles").with_tenant(customer)
```

Tokens only ever move forward, so observing an old token is harmless. Reads without any token
are linearizable (they reflect every acknowledged write); `consistency="stale"` is faster and
may lag.

## Errors

```python
try:
    acme.upsert([{"id": "x", "no_such_field": 1}])
except InvalidInputError as e:
    print("refused:", e.message)
```

| exception | HTTP | typical cause | what to do |
|---|---|---|---|
| `InvalidInputError` | 400 | unknown field, wrong type or dimension, bad filter | fix the request |
| `AuthenticationError` | 401 | missing or wrong key | configuration |
| `ForbiddenError` | 403 | the key lacks the role, or cannot act for that tenant | configuration |
| `UnavailableError` | 503 or network | no leader during a failover, node down | already retried; retry later |
| `CairnError` | other | anything else | log it |

`get()` returns `None` for a missing document; it does not raise.

Next: [customers and tenants](04-tenants.md).
