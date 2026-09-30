# 5. Document lifecycle

## Ingest

```python
def chunks(text: str, size: int = 800, overlap: int = 100) -> list[str]:
    """Paragraph-aware chunks of about `size` characters."""
    out, cur = [], ""
    for para in text.split("\n\n"):
        if cur and len(cur) + len(para) > size:
            out.append(cur)
            cur = cur[-overlap:]
        cur = (cur + "\n\n" + para).strip()
    if cur:
        out.append(cur)
    return out
```

Chunking is yours to choose: Cairn stores whatever you give it. Upsert in batches of a few
hundred documents; each call is one replicated write per shard.

## Update, without stale chunks

Writing a document again with the same id replaces it. But when an article gets shorter, its
old trailing chunks would stay searchable. Give every chunk the article's revision, write the
new chunks, then delete the chunks of any other revision:

```python
def ingest_article(db: Client, article_id: str, title: str, body: str, *, product: str, lang: str):
    """Writes an article's chunks, then deletes the chunks of its previous revisions."""
    rev = now_ms()
    parts = chunks(body)
    db.upsert([
        {
            "id": f"{article_id}#{i}",
            "text": part,
            "title": title,
            "parent": article_id,
            "product": product,
            "lang": lang,
            "rev": rev,
            "deprecated": False,
            "embedding": embed(title + "\n" + part),
        }
        for i, part in enumerate(parts)
    ])
    # Chunks of older revisions: gone, including trailing ones when the article got shorter.
    db.delete(filter=and_(eq("parent", article_id), not_(eq("rev", rev))))
    return len(parts)
```

Writing first and deleting second means the article is never missing from search. A reader
may briefly see old and new chunks together; `group_by="parent"` still shows the article once.

## Delete

```python
acme.delete(parent="kb-invoices")                       # the article and all its chunks
```

`delete` also takes `ids=[...]`, or `filter=...` for anything that matches ("every ticket of
this product before 2024"). A deletion by filter removes the documents that match when it is
applied: documents written afterwards are not affected. Filters that match everything are
refused. Every deletion is final and audited.

## Metadata edits

```python
acme.patch("kb-invoices#0", {"deprecated": True})
```

`patch` changes the named fields and keeps the others (`None` clears a field). It does not
re-embed anything. A missing or expired document is not created. `patch_many` changes several
documents in one call: to mark a whole article, patch its chunks (the FastAPI recipe lists
them with a filter-only search). Each patch is atomic with respect to other writes.

## Retention

Name a field holding each document's expiry, in Unix milliseconds, when you create the
collection:

```python
admin.create_collection(
    "tickets",
    {"fields": [
        {"name": "embedding", "kind": {"Vector": {"dims": 384, "metric": "Cosine"}}},
        {"name": "text", "kind": "Text"},
        {"name": "expires_at", "kind": "I64"},   # Unix milliseconds
    ]},
    expires_field="expires_at",
)
```

```python
tickets = cairn.collection("tickets").with_tenant("acme")
DAY = 24 * 3600 * 1000
tickets.upsert([
    {"id": "T-1001", "text": "Cannot log in since Monday", "embedding": embed("cannot log in"),
     "expires_at": now_ms() + 90 * DAY},
])
```

From its expiry on, a document is never returned by reads or searches. The shard leaders then
delete it (every 10 s by default), and each such deletion is audited. A document without the
field never expires; writing a new expiry postpones it.

Next: [compliance](06-compliance.md).
