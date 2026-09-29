# Acceptance suite

`cairn_acceptance.py` checks a running Cairn node or cluster through the HTTP API, with a
realistic corpus. It needs Python 3.9+ and nothing else.

The corpus is a deterministic media archive: articles on six topics, from four sources, with
tags, dates and some attachments. It uses the Docker image's default schema. Some documents
have no tags, no date or no attachment, to exercise missing values.

Embeddings are a stand-in: words hashed into 384 dimensions, so texts that share words are
close. No model is downloaded; real embeddings go through the API the same way.

## Run it

```bash
# One node (Docker): the first start prints an admin key
docker run -d --name cairn -p 7200:7200 ghcr.io/cairn-db/cairn:0.1
python3 cairn_acceptance.py --url http://127.0.0.1:7200 --key "$(docker logs cairn 2>&1 | grep -o 'cairn_[A-Za-z0-9_-]*' | head -1)"

# Three nodes: reads go round-robin through every node
CAIRN_HTTP_ADMIN_KEY=<secret> docker compose -f docker/compose.cluster.yaml up -d
python3 cairn_acceptance.py --url http://127.0.0.1:7201 --url http://127.0.0.1:7202 \
  --url http://127.0.0.1:7203 --key <secret>
```

Options:
- `--docs N`: corpus size, 3,000 by default, at most 9,000.
- `--insecure`: accept a self-signed HTTPS certificate.
- `CAIRN_READ_KEY`, `CAIRN_WRITE_KEY`, `CAIRN_TAKEDOWN_KEY`: keys with a single role each, which
  add the role checks (403).

The exit status is 0 when every check passes.

## What it checks

- Authentication: `/health` open, 401 without a key or with a wrong one, 403 for a missing role.
- Every field type (vector, text, enum, set, date, blob, missing values) reads back exactly,
  with the write's token, through every node.
- Text search (BM25, `all_terms`) and vector search:
  - a document is its own nearest neighbour;
  - recall@10 against a brute-force ground truth, unfiltered and filtered.
- Hybrid search: RRF and weighted fusion.
- Every filter operator (`eq`, `in`, `gt`, `gte`, `lt`, `lte`, `is_null`, `and`, `or`, `not`, set
  membership), compared exactly with a locally computed result.
- Updates replace the whole document.
- Takedowns, single and bulk: with the takedown's token, no read, vector search, text search or
  listing returns the document, on any node.
- Text ids (0.2): string ids of any shape (encoded in paths, made of digits), read back through
  every node, carried by search hits, replaced by a second write, taken down alone or mixed
  with integer ids; reserved field names refused.
- Deletion by filter (0.2): a document and its chunks (by a parent tag), only the listed ids
  that match, and a criterion over the corpus. The count must equal what a search found, and
  afterwards every node holds exactly the documents that did not match. Filters that match
  everything are refused.
- Tenants (0.2), through the `Cairn-Tenant` header: the same ids in two tenants read back
  separately on every node, searches return only the tenant's documents, a takedown in one
  tenant leaves the other's document, and erasing a tenant removes all of it everywhere.
- Collections (0.3): one is created with its own schema (then 409 for the same name), its
  documents are found through every node and not in `default`, deletion by parent works
  inside it, and once dropped it is gone from every node.
- Consistency levels, ten invalid inputs rejected with 400 and a message, 404 on a missing
  document.
- Administration: status and merge pause.
