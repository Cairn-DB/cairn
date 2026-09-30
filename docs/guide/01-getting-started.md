# 1. Getting started

## Run Cairn

```bash
docker run -d --name cairn -p 7200:7200 -v cairn-data:/data ghcr.io/cairn-db/cairn:0.3
docker logs cairn 2>&1 | grep -o 'cairn_[A-Za-z0-9_-]*' | head -1     # the admin key, printed once
```

On its first start, the container creates an admin key and prints it once: keep it. Podman
works the same way; use `127.0.0.1` rather than `localhost` if the connection is refused.

The image serves one collection, `default`, with a small built-in schema, plus any collection
you create through the API. For three replicated nodes, see
[`docs/deployment.md`](../deployment.md).

## Keys for your application

The admin key can do everything. Give your application a key with only the roles it needs:

```bash
docker exec cairn cairn-server keygen my-backend read,write,takedown
```

The command prints the key once, and an entry for the keys file (the file keeps only a
digest). [`docs/deployment.md`](../deployment.md) explains where the file lives. A node reads
it at startup: restart the node after adding a key. The roles are `read`, `write`, `takedown` (deletions) and `admin` (collections,
status). A key can also be limited to one tenant (chapter 4).

## First calls

```bash
pip install cairn-db-client          # Python 3.9+; npm install @cairn-db/client for TypeScript
```

```python
from cairn_db import Client

db = Client("http://localhost:7200", api_key=KEY)
db.upsert([{"id": "hello-1", "text": "Cairn keeps deletions final", "source": "tv"}])
print(db.search(text="deletions", text_field="text"))    # finds it
db.delete(ids=["hello-1"])
print(db.get("hello-1"))                                  # None, on every node
```

Three things happened that you will rely on everywhere:
- **Your own ids.** A string such as `"hello-1"`, a UUID or `"article/2024-17"`, returned as
  written. Integers below 2^63 work too.
- **Read-your-writes by default.** The client keeps the consistency token of its writes and
  sends it with each read, so the search saw the document it had just written.
- **Read-your-takedowns.** After the deletion, no read through this client returns the
  document again, through any node, even a replica that lags.

Next: [modelling your data](02-data-modeling.md).
