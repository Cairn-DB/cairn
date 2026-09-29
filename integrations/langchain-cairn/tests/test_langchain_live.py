"""Against a running node (clients/test-live.sh): CAIRN_URL, CAIRN_ADMIN_KEY, CAIRN_KEY."""

import hashlib
import math
import os

import pytest
from cairn_db import Client, NotFoundError
from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings

from langchain_cairn import CairnVectorStore, collection_schema

URL = os.environ.get("CAIRN_URL")
pytestmark = pytest.mark.skipif(not URL, reason="CAIRN_URL not set")
DIMS = 64


class WordHash(Embeddings):
    """Words hashed into 64 dimensions: texts sharing words are close."""

    def _one(self, text: str) -> list[float]:
        v = [0.0] * DIMS
        for w in text.lower().split():
            h = hashlib.sha1(w.encode()).digest()
            v[h[0] % DIMS] += 1.0 if h[1] & 1 else -1.0
        n = math.sqrt(sum(x * x for x in v)) or 1.0
        return [x / n for x in v]

    def embed_documents(self, texts):
        return [self._one(t) for t in texts]

    def embed_query(self, text):
        return self._one(text)


def test_langchain_store_against_a_live_node():
    admin = Client(URL, os.environ["CAIRN_ADMIN_KEY"])
    name = f"lc-{os.getpid()}"
    admin.create_collection(name, collection_schema(DIMS, filterable={"source": "Enum", "page": "I64"}), shards=2)
    try:
        store = CairnVectorStore(Client(URL, os.environ["CAIRN_KEY"]).collection(name), WordHash())
        chunks = [
            Document(page_content="the otter swims in the river", metadata={"source": "a", "parent": "doc-a", "page": 1, "extra": {"k": [1, 2]}}),
            Document(page_content="an otter eats fish near the river bank", metadata={"source": "a", "parent": "doc-a", "page": 2}),
            Document(page_content="the stock market fell sharply today", metadata={"source": "b", "parent": "doc-b", "page": 1}),
            Document(page_content="markets and stocks after the election", metadata={"source": "b", "parent": "doc-b", "page": 2}),
        ]
        ids = store.add_documents(chunks, ids=["a1", "a2", "b1", "b2"])
        assert ids == ["a1", "a2", "b1", "b2"]

        top = store.similarity_search("otter river", k=2)
        assert {d.id for d in top} == {"a1", "a2"}
        assert top[0].metadata["parent"] == "doc-a"
        # Metadata round-trips whole, including keys that are not fields.
        got = store.get_by_ids(["a1"])[0]
        assert got.metadata == chunks[0].metadata and got.page_content == chunks[0].page_content
        # Filters on declared fields: a plain dict, or a Cairn filter.
        only_b = store.similarity_search("otter river", k=4, filter={"source": "b"})
        assert {d.id for d in only_b} == {"b1", "b2"}
        pages = store.similarity_search("otter", k=4, filter={"field": "page", "gte": 2})
        assert {d.id for d in pages} == {"a2", "b2"}
        scored = store.similarity_search_with_relevance_scores("otter river", k=4)
        assert all(0.0 <= s <= 1.0 for _, s in scored) and scored[0][1] >= scored[-1][1]
        # One hit per parent, a retriever, and hybrid search.
        grouped = store.similarity_search("otter river market", k=4, group_by="parent")
        assert sorted(d.metadata["parent"] for d in grouped) == ["doc-a", "doc-b"]
        assert store.as_retriever(search_kwargs={"k": 1}).invoke("stock market")[0].id == "b1"
        assert store.hybrid_search("election", k=1)[0].id == "b2"

        # Deletions are final, through any read.
        store.delete(parent="doc-a")
        assert {d.id for d in store.similarity_search("otter river", k=4)} == {"b1", "b2"}
        assert store.get_by_ids(["a1", "a2"]) == []
        store.delete(ids=["b1"])
        assert [d.id for d in store.similarity_search("market", k=4)] == ["b2"]

        again = CairnVectorStore.from_texts(["hello otter"], WordHash(), client=store.client, ids=["h1"])
        assert again.similarity_search("otter", k=1)[0].id == "h1"
    finally:
        admin.drop_collection(name)
    with pytest.raises(NotFoundError):
        store.similarity_search("otter", k=1)
