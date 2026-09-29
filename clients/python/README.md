# cairn-db

Python client for [Cairn](https://github.com/cairn-db/cairn), the hybrid search database
where a deletion is final. It comes in two flavours, sync (`Client`) and async
(`AsyncClient`), and is typed. Its only dependency is `httpx`.

Not published to PyPI yet. Install it from this directory with `pip install .`.

```python
from cairn_db import Client, eq, range_, and_

db = Client("http://localhost:7200", api_key=KEY)

# Your own ids; chunks carry their parent in an ordinary field ("parent" by default).
db.upsert([
    {"id": "report-9#0", "parent": "report-9", "text": "...", "embedding": [...], "lang": "en"},
    {"id": "report-9#1", "parent": "report-9", "text": "...", "embedding": [...], "lang": "en"},
])

hits = db.search(
    k=10,
    vector={"field": "embedding", "values": query_vector},
    text="nuclear energy", text_field="text",
    filter=and_(eq("lang", "en"), range_("year", gte=2020)),
)
for h in hits:
    print(h.id, h.score, h.document["text"])

db.delete(parent="report-9")               # the document and all its chunks
db.delete(filter=eq("source", "crawler"))  # everything that matches, now
db.delete(ids=["report-7#0"])
```

`AsyncClient` has the same methods, as coroutines (`async with AsyncClient(...) as db:`).

## Read-your-writes and takedowns

The client keeps the consistency token of its writes and takedowns, and sends it with every
read. It therefore reads what it wrote, and never reads back what it deleted, through any
node. To give the same guarantee to another service, pass it `db.token`:
`Client(..., token=token)`, or `db.observe(token)` on an existing client.

## Tenants

```python
acme = db.with_tenant("acme")   # an unscoped key acting for tenant "acme"
acme.upsert([{"id": "note-1", "text": "...", "embedding": [...]}])
db.forget_tenant("acme")        # erase everything of "acme"
```

A key scoped to a tenant (`cairn-server keygen app read,write --tenant acme`) needs nothing
else: every call stays inside its tenant. Ids belong to their tenant.

## Errors and retries

- `InvalidInputError` (400), `AuthenticationError` (401), `ForbiddenError` (403),
  `UnavailableError` (503 or network), all subclasses of `CairnError` with a `status`.
- `get` returns `None` for a missing document.
- 502, 503, 504 and network errors are retried (`retries=3` by default) with backoff, on the
  next address when `url` is a list.

## Tests

`PYTHONPATH=src python -m pytest tests` runs the unit tests. `../test-live.sh` also runs the
live tests against a fresh local node.
