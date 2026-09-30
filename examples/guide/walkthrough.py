"""Every code sample of the developer guide (docs/guide/), run in order against a live node.

    CAIRN_URL=http://localhost:7200 CAIRN_ADMIN_KEY=... python walkthrough.py

It creates the collections `articles` and `tickets` (dropped again at the end) and checks what
the guide says each step does. `embed()` is a stand-in: words hashed into 384 dimensions, so
the script needs no model download. Use a real embedding model in your application.
"""

from __future__ import annotations

import hashlib
import math
import os
import time

from cairn_db import Client, InvalidInputError, and_, eq, in_, not_

URL = os.environ.get("CAIRN_URL", "http://localhost:7200")
ADMIN_KEY = os.environ["CAIRN_ADMIN_KEY"]
DIMS = 384


def embed(text: str) -> list[float]:
    """Stand-in embedding (see the module docstring)."""
    v = [0.0] * DIMS
    for w in text.lower().split():
        h = hashlib.sha1(w.encode()).digest()
        v[int.from_bytes(h[:2], "little") % DIMS] += 1.0 if h[2] & 1 else -1.0
    n = math.sqrt(sum(x * x for x in v)) or 1.0
    return [x / n for x in v]


def now_ms() -> int:
    return int(time.time() * 1000)


# --- Chapter 2: data modelling ---------------------------------------------------------------
# [schema]
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
# [/schema]

admin = Client(URL, api_key=ADMIN_KEY)
for name in ("articles", "tickets"):
    if name in [c["name"] for c in admin.list_collections()]:
        admin.drop_collection(name)

# [create]
admin.create_collection("articles", ARTICLES, shards=4)
admin.create_collection(
    "tickets",
    {"fields": [
        {"name": "embedding", "kind": {"Vector": {"dims": 384, "metric": "Cosine"}}},
        {"name": "text", "kind": "Text"},
        {"name": "expires_at", "kind": "I64"},   # Unix milliseconds
    ]},
    expires_field="expires_at",
)
# [/create]


# --- Chapter 3: a backend ---------------------------------------------------------------------
# [client]
cairn = Client(URL, api_key=os.environ.get("CAIRN_KEY", ADMIN_KEY))  # one per process
articles = cairn.collection("articles")                              # views: cheap, shared pool
# [/client]


# --- Chapter 5: document lifecycle (ingest and update) ---------------------------------------
# [chunk]
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
# [/chunk]


# [ingest]
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
# [/ingest]


# --- Chapter 4: tenants -----------------------------------------------------------------------
# [tenant]
def for_customer(customer_id: str) -> Client:
    return articles.with_tenant(customer_id)   # every call stays inside this customer
# [/tenant]

acme, globex = for_customer("acme"), for_customer("globex")
long_body = "\n\n".join(f"Paragraph {i}: to reset a password, open Settings then Security. " * 6 for i in range(6))
n1 = ingest_article(acme, "kb-reset-password", "Reset your password", long_body, product="auth", lang="en")
ingest_article(acme, "kb-invoices", "Download invoices", "Invoices are in Billing, then History.", product="billing", lang="en")
ingest_article(globex, "kb-reset-password", "Globex password policy", "Globex passwords rotate every 90 days.", product="auth", lang="en")
assert n1 > 1

# --- Chapter 3: search ------------------------------------------------------------------------
# [search]
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
# [/search]

res = search(acme, "how do I reset my password", k=5)
assert res[0]["article"] == "kb-reset-password" and len({r["article"] for r in res}) == len(res), res
assert all(r["title"] != "Globex password policy" for r in res), "tenant isolation"
assert [r["article"] for r in search(acme, "password", product="billing")] == ["kb-invoices"]

# Update: a shorter article leaves no stale chunk.
ingest_article(acme, "kb-reset-password", "Reset your password", "Open Settings, then Security.", product="auth", lang="en")
left = acme.search(k=50, filter=eq("parent", "kb-reset-password"))
assert [h.id for h in left] == ["kb-reset-password#0"], [h.id for h in left]

# [patch]
acme.patch("kb-invoices#0", {"deprecated": True})
# [/patch]
assert acme.get("kb-invoices#0")["deprecated"] is True

# [errors]
try:
    acme.upsert([{"id": "x", "no_such_field": 1}])
except InvalidInputError as e:
    print("refused:", e.message)
# [/errors]

# [delete-article]
acme.delete(parent="kb-invoices")                       # the article and all its chunks
# [/delete-article]
assert acme.search(k=10, filter=eq("parent", "kb-invoices")) == []
assert globex.get("kb-reset-password#0") is not None, "another customer's article is untouched"

# --- Chapter 3: tokens across services --------------------------------------------------------
# [token]
token = cairn.token                                      # after a write or a takedown
other_service = Client(URL, api_key=ADMIN_KEY, token=token)
# other_service's reads reflect those writes and takedowns, on any node.
# [/token]
assert other_service.collection("articles").with_tenant("acme").get("kb-invoices#0") is None

# --- Chapter 5: retention ---------------------------------------------------------------------
# [retention]
tickets = cairn.collection("tickets").with_tenant("acme")
DAY = 24 * 3600 * 1000
tickets.upsert([
    {"id": "T-1001", "text": "Cannot log in since Monday", "embedding": embed("cannot log in"),
     "expires_at": now_ms() + 90 * DAY},
])
# [/retention]
tickets.upsert([{"id": "T-0001", "text": "old ticket", "embedding": embed("old"), "expires_at": now_ms() - 1}])
assert tickets.get("T-0001") is None, "expired: hidden at once"
assert tickets.get("T-1001") is not None

# --- Chapter 6: compliance --------------------------------------------------------------------
# [forget]
result = articles.forget_tenant("globex")               # every article chunk of globex
cairn.collection("tickets").forget_tenant("globex")
# [/forget]
assert result.deleted >= 1
# [proof]
proof = acme.prove_deletion(["kb-invoices#0"])
assert proof["report"]["verdict"] == "deleted everywhere"
# Keep `proof` as evidence; check it with the node's public key (docs/guide/06-compliance.md).
# [/proof]

for name in ("articles", "tickets"):
    admin.drop_collection(name)
print("walkthrough OK")
