"""A multi-customer knowledge-base search service on Cairn, with FastAPI (docs/guide/).

    CAIRN_URL=http://localhost:7200 CAIRN_KEY=... uvicorn app:app

`CAIRN_KEY` needs the read, write and takedown roles, and no tenant: the service acts for each
customer through `with_tenant`. Create the collection once with `python setup.py` (admin key).
"""

from __future__ import annotations

import os
import time
from functools import lru_cache

from cairn_db import CairnError, Client, InvalidInputError, and_, eq, not_
from fastapi import FastAPI, Header, HTTPException, Response
from pydantic import BaseModel

COLLECTION = "articles"
cairn = Client(os.environ.get("CAIRN_URL", "http://localhost:7200"), api_key=os.environ["CAIRN_KEY"])
app = FastAPI(title="Helpdesk KB")


# --- embeddings: a real model by default, a stand-in for tests (EMBEDDER=hash) ----------------
@lru_cache(maxsize=1)
def _model():
    from fastembed import TextEmbedding  # pip install fastembed

    return TextEmbedding("BAAI/bge-small-en-v1.5")  # 384 dimensions, runs locally


def embed(texts: list[str]) -> list[list[float]]:
    if os.environ.get("EMBEDDER") == "hash":
        from embed_stub import embed_stub

        return [embed_stub(t) for t in texts]
    return [v.tolist() for v in _model().embed(texts)]


def chunks(text: str, size: int = 800, overlap: int = 100) -> list[str]:
    out, cur = [], ""
    for para in text.split("\n\n"):
        if cur and len(cur) + len(para) > size:
            out.append(cur)
            cur = cur[-overlap:]
        cur = (cur + "\n\n" + para).strip()
    if cur:
        out.append(cur)
    return out


def kb(customer: str, token: str | None) -> Client:
    """The customer's view of the collection. A token from the caller (a previous write's
    `X-Cairn-Token`) makes this request see that write, on any node, behind any load balancer."""
    if token:
        cairn.observe(token)
    return cairn.collection(COLLECTION).with_tenant(customer)


def with_token(response: Response, db: Client) -> None:
    response.headers["X-Cairn-Token"] = db.token


# --- routes ------------------------------------------------------------------------------------
class Article(BaseModel):
    title: str
    body: str
    product: str
    lang: str = "en"


@app.put("/customers/{customer}/articles/{article_id}")
def put_article(customer: str, article_id: str, a: Article, response: Response,
                x_cairn_token: str | None = Header(default=None)):
    """Creates or replaces an article: new chunks first, then the old revision's chunks go."""
    db = kb(customer, x_cairn_token)
    rev = int(time.time() * 1000)
    parts = chunks(a.body)
    vectors = embed([a.title + "\n" + p for p in parts])
    db.upsert([
        {"id": f"{article_id}#{i}", "text": p, "title": a.title, "parent": article_id,
         "product": a.product, "lang": a.lang, "rev": rev, "deprecated": False, "embedding": v}
        for i, (p, v) in enumerate(zip(parts, vectors))
    ])
    db.delete(filter=and_(eq("parent", article_id), not_(eq("rev", rev))))
    with_token(response, db)
    return {"article": article_id, "chunks": len(parts)}


@app.get("/customers/{customer}/search")
def search(customer: str, q: str, product: str | None = None, lang: str | None = None, k: int = 5,
           x_cairn_token: str | None = Header(default=None)):
    db = kb(customer, x_cairn_token)
    conditions = [eq("product", product)] if product else []
    conditions += [eq("lang", lang)] if lang else []
    conditions.append(eq("deprecated", False))
    hits = db.search(
        k=k,
        vector={"field": "embedding", "values": embed([q])[0]},
        text=q,
        text_field="text",
        filter=and_(*conditions) if len(conditions) > 1 else conditions[0],
        group_by="parent",
    )
    return [{"article": h.group, "title": h.document["title"], "snippet": h.document["text"][:300],
             "score": h.score} for h in hits]


def chunk_ids(db: Client, article_id: str | None = None) -> list[str]:
    f = eq("parent", article_id) if article_id else not_(eq("parent", ""))
    return [h.id for h in db.search(k=10_000, filter=f, with_documents=False)]


@app.post("/customers/{customer}/articles/{article_id}/deprecate")
def deprecate(customer: str, article_id: str, response: Response,
              x_cairn_token: str | None = Header(default=None)):
    """A metadata change: no re-ingestion, no new embeddings."""
    db = kb(customer, x_cairn_token)
    ids = chunk_ids(db, article_id)
    if not ids:
        raise HTTPException(404, "no such article")
    db.patch_many([{"id": i, "set": {"deprecated": True}} for i in ids])
    with_token(response, db)
    return {"article": article_id, "chunks": len(ids)}


@app.delete("/customers/{customer}/articles/{article_id}")
def delete_article(customer: str, article_id: str, response: Response,
                   x_cairn_token: str | None = Header(default=None)):
    db = kb(customer, x_cairn_token)
    deleted = db.delete(parent=article_id).deleted
    with_token(response, db)
    return {"article": article_id, "chunks_deleted": deleted}


@app.delete("/customers/{customer}")
def forget_customer(customer: str, response: Response, x_cairn_token: str | None = Header(default=None)):
    """GDPR erasure: everything of the customer goes, and the answer carries signed evidence."""
    db = kb(customer, x_cairn_token)
    ids = chunk_ids(db)                                   # what we are about to erase
    erased = cairn.collection(COLLECTION).forget_tenant(customer).deleted
    proof = db.prove_deletion(ids) if ids else None      # every replica is asked
    with_token(response, db)
    return {"customer": customer, "chunks_erased": erased, "proof": proof}


@app.exception_handler(InvalidInputError)
def invalid(_, e: InvalidInputError):
    from fastapi.responses import JSONResponse

    return JSONResponse({"error": e.message}, status_code=400)


@app.exception_handler(CairnError)
def cairn_error(_, e: CairnError):
    from fastapi.responses import JSONResponse

    return JSONResponse({"error": "search backend unavailable"}, status_code=503)
