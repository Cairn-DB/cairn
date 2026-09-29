"""Against a running node (clients/test-live.sh): CAIRN_URL, CAIRN_ADMIN_KEY, CAIRN_KEY."""

import hashlib
import math
import os
from typing import List

import pytest
from cairn_db import Client, NotFoundError
from llama_index.core import Document, StorageContext, VectorStoreIndex
from llama_index.core.base.embeddings.base import BaseEmbedding
from llama_index.core.schema import NodeRelationship, RelatedNodeInfo, TextNode
from llama_index.core.vector_stores.types import (
    FilterCondition,
    FilterOperator,
    MetadataFilter,
    MetadataFilters,
    VectorStoreQuery,
    VectorStoreQueryMode,
)

from llama_index.vector_stores.cairn import CairnVectorStore, collection_schema

URL = os.environ.get("CAIRN_URL")
pytestmark = pytest.mark.skipif(not URL, reason="CAIRN_URL not set")
DIMS = 64


def word_hash(text: str) -> List[float]:
    v = [0.0] * DIMS
    for w in text.lower().split():
        h = hashlib.sha1(w.encode()).digest()
        v[h[0] % DIMS] += 1.0 if h[1] & 1 else -1.0
    n = math.sqrt(sum(x * x for x in v)) or 1.0
    return [x / n for x in v]


class WordHash(BaseEmbedding):
    def _get_query_embedding(self, query: str) -> List[float]:
        return word_hash(query)

    def _get_text_embedding(self, text: str) -> List[float]:
        return word_hash(text)

    async def _aget_query_embedding(self, query: str) -> List[float]:
        return word_hash(query)


def node(id: str, text: str, doc: str, **md) -> TextNode:
    n = TextNode(id_=id, text=text, metadata=md, embedding=word_hash(text))
    n.relationships[NodeRelationship.SOURCE] = RelatedNodeInfo(node_id=doc)
    return n


def test_llamaindex_store_against_a_live_node():
    admin = Client(URL, os.environ["CAIRN_ADMIN_KEY"])
    name = f"li-{os.getpid()}"
    admin.create_collection(name, collection_schema(DIMS, filterable={"source": "Enum", "page": "I64"}), shards=2)
    try:
        store = CairnVectorStore(Client(URL, os.environ["CAIRN_KEY"]).collection(name))
        nodes = [
            node("a1", "the otter swims in the river", "doc-a", source="a", page=1),
            node("a2", "an otter eats fish near the river bank", "doc-a", source="a", page=2),
            node("b1", "the stock market fell sharply today", "doc-b", source="b", page=1),
            node("b2", "markets and stocks after the election", "doc-b", source="b", page=2),
        ]
        assert store.add(nodes) == ["a1", "a2", "b1", "b2"]

        r = store.query(VectorStoreQuery(query_embedding=word_hash("otter river"), similarity_top_k=2))
        assert set(r.ids) == {"a1", "a2"}
        assert r.nodes[0].ref_doc_id == "doc-a" and r.nodes[0].metadata["source"] == "a"
        assert r.similarities[0] >= r.similarities[1]
        # Filters: operators and conditions, and doc_ids.
        f = MetadataFilters(filters=[MetadataFilter(key="source", value="b")])
        r = store.query(VectorStoreQuery(query_embedding=word_hash("otter"), similarity_top_k=4, filters=f))
        assert set(r.ids) == {"b1", "b2"}
        f = MetadataFilters(
            filters=[
                MetadataFilter(key="page", value=2, operator=FilterOperator.GTE),
                MetadataFilter(key="source", value=["a"], operator=FilterOperator.NIN),
            ],
            condition=FilterCondition.AND,
        )
        r = store.query(VectorStoreQuery(query_embedding=word_hash("otter"), similarity_top_k=4, filters=f))
        assert r.ids == ["b2"]
        r = store.query(VectorStoreQuery(query_embedding=word_hash("otter"), similarity_top_k=4, doc_ids=["doc-b"]))
        assert set(r.ids) == {"b1", "b2"}
        r = store.query(VectorStoreQuery(query_str="election", query_embedding=word_hash("election"), similarity_top_k=1, mode=VectorStoreQueryMode.HYBRID))
        assert r.ids == ["b2"]
        r = store.query(VectorStoreQuery(query_str="election", similarity_top_k=1, mode=VectorStoreQueryMode.TEXT_SEARCH))
        assert r.ids == ["b2"]
        assert [n.node_id for n in store.get_nodes(["a2"])] == ["a2"]

        # A source document and all its nodes, then single nodes.
        store.delete("doc-a")
        r = store.query(VectorStoreQuery(query_embedding=word_hash("otter river"), similarity_top_k=4))
        assert set(r.ids) == {"b1", "b2"}
        store.delete_nodes(["b1"])
        assert store.get_nodes(["b1", "b2"])[0].node_id == "b2"

        # End to end: an index over source documents, then a retriever.
        index = VectorStoreIndex.from_documents(
            [Document(text="beavers build dams on rivers", id_="doc-c"), Document(text="volcanoes erupt lava", id_="doc-d")],
            storage_context=StorageContext.from_defaults(vector_store=store),
            embed_model=WordHash(),
        )
        got = index.as_retriever(similarity_top_k=1).retrieve("beavers dams")
        assert got[0].node.ref_doc_id == "doc-c"
        index.delete_ref_doc("doc-c")
        got = index.as_retriever(similarity_top_k=5).retrieve("beavers dams")
        assert all(g.node.ref_doc_id != "doc-c" for g in got)
        store.clear()
        assert store.query(VectorStoreQuery(query_embedding=word_hash("x"), similarity_top_k=5)).ids == []
    finally:
        admin.drop_collection(name)
    with pytest.raises(NotFoundError):
        store.query(VectorStoreQuery(query_embedding=word_hash("x"), similarity_top_k=1))
