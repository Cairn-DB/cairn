# 2. Modelling your data

## A collection per kind of record

A collection has its own schema and shards. Create one per kind of record that you search the
same way: articles, tickets, products. Creating and dropping collections needs the admin key,
so it belongs in a setup step, not in request handlers.

```python
ARTICLES = {
    "fields": [
        {"name": "embedding", "kind": {"Vector": {"dims": 384, "metric": "Cosine"}}},
        {"name": "text", "kind": "Text"},        # the chunk's text: BM25 and the snippet
        {"name": "title", "kind": "Text"},
        {"name": "parent", "kind": "Enum"},      # the article this chunk belongs to
        {"name": "product", "kind": "Enum"},     # filterable metadata
        {"name": "lang", "kind": "Enum"},
        {"name": "rev", "kind": "I64"},          # the article's revision, for updates
        {"name": "deprecated", "kind": "Bool"},
    ]
}

admin.create_collection("articles", ARTICLES, shards=4)
```

The schema is fixed once created. To change it, create a new collection, write into it, and
drop the old one.

## Field kinds, and what they are for

| kind | JSON | search | filters |
|---|---|---|---|
| `Vector {dims, metric}` | array of numbers | vector leg | |
| `Text` | string | BM25 text leg | `is_null` only |
| `Enum` | string | | `eq`, `in` |
| `Set` | array of strings | | `eq` / `in` mean "contains" |
| `I64`, `F64`, `Date` | number | | `eq`, ranges (`gt`, `gte`, `lt`, `lte`) |
| `Bool` | boolean | | `eq` |
| `Blob` | base64 string | | |

Rules of thumb:
- **Filter on `Enum`, not on `Text`.** Anything you will select by (customer, source, language,
  status, parent) is an `Enum` or a `Set`.
- **One vector field per embedding model.** Several vector fields are possible (image and
  text, for instance), fused in one query.
- **`metric`**: `Cosine` for most text embedding models; `Dot` if your model says so; `L2` for
  raw features.
- Every field is optional in a document. Names starting with `_` are reserved.

## Chunks and their parent

RAG splits a document into chunks. Store **one Cairn document per chunk**, and give every
chunk:
- an id derived from its parent: `f"{article_id}#{i}"`;
- the parent's id in an `Enum` field, `parent`.

With that, "this article and all its chunks" is one call in every operation that matters:
search one hit per article (`group_by="parent"`), delete an article (`delete(parent=...)`), and
update it without stale chunks (chapter 5).

## Ids

Use the ids of your own system. An id identifies a document within its collection. Under a
tenant (chapter 4), an id identifies a document within that tenant: two customers can both
have an article `"faq"`.

Next: [inside a backend](03-backend.md).
