"""``CairnVectorStore``: LangChain's vector store interface over a Cairn collection.

Documents are stored with the text in ``text_field``, the embedding in ``vector_field``, the
whole metadata as JSON in the ``metadata`` field (so any metadata round-trips), and the
metadata keys that are also fields of the schema in those fields (so they can be filtered).
A document's ``parent`` metadata, if the schema has a ``parent`` field, lets
``delete(parent=...)`` remove a document and all its chunks.

Deletions are final: once ``delete`` returns, no search or read through this store returns
the documents again, on any node (the client passes its consistency token).
"""

from __future__ import annotations

import base64
import json
import math
import uuid
from typing import Any, Callable, Iterable, Optional, Sequence

from cairn_db import Client, and_, eq
from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings
from langchain_core.vectorstores import VectorStore

METADATA_FIELD = "metadata"


def collection_schema(
    dims: int,
    *,
    metric: str = "Cosine",
    text_field: str = "text",
    vector_field: str = "embedding",
    filterable: Optional[dict[str, str]] = None,
) -> dict[str, Any]:
    """A schema for a store: text, embedding, ``parent``, the full metadata as JSON, and
    ``filterable`` metadata fields (name -> kind: ``Enum``, ``I64``, ``F64``, ``Date``,
    ``Bool``, ``Set`` or ``Text``)."""
    fields = [
        {"name": vector_field, "kind": {"Vector": {"dims": dims, "metric": metric}}},
        {"name": text_field, "kind": "Text"},
        {"name": "parent", "kind": "Enum"},
        {"name": METADATA_FIELD, "kind": "Blob"},
    ]
    for name, kind in (filterable or {}).items():
        fields.append({"name": name, "kind": kind})
    return {"fields": fields}


def _filter(f: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    """A Cairn filter, or ``{"field": value, ...}`` meaning every field equals its value."""
    if not f:
        return None
    if any(k in f for k in ("and", "or", "not", "field")):
        return f
    parts = [eq(k, v) for k, v in f.items()]
    return parts[0] if len(parts) == 1 else and_(*parts)


class CairnVectorStore(VectorStore):
    """A Cairn collection as a LangChain vector store.

    >>> from cairn_db import Client
    >>> client = Client("http://localhost:7200", api_key=KEY)
    >>> client.create_collection("docs", collection_schema(1536, filterable={"source": "Enum"}))
    >>> store = CairnVectorStore(client.collection("docs"), embeddings)
    >>> store.add_documents(chunks)
    >>> store.similarity_search("what changed in 2024?", k=4, filter={"source": "handbook"})
    >>> store.delete(parent="handbook.pdf")   # the document and all its chunks
    """

    def __init__(
        self,
        client: Client,
        embedding: Embeddings,
        *,
        text_field: str = "text",
        vector_field: str = "embedding",
    ) -> None:
        self.client = client
        self._embedding = embedding
        self.text_field = text_field
        self.vector_field = vector_field
        fields = {f["name"]: f["kind"] for f in client.schema()["fields"]}
        if vector_field not in fields or text_field not in fields:
            raise ValueError(f"the collection needs a {vector_field!r} and a {text_field!r} field")
        self.metric = fields[vector_field]["Vector"]["metric"]
        self._fields = fields

    @property
    def embeddings(self) -> Embeddings:
        return self._embedding

    # ------------------------------------------------------------------ writes

    def _doc(self, id: str, text: str, vector: list[float], metadata: dict[str, Any]) -> dict[str, Any]:
        d: dict[str, Any] = {"id": id, self.text_field: text, self.vector_field: vector}
        if METADATA_FIELD in self._fields:
            d[METADATA_FIELD] = base64.b64encode(json.dumps(metadata, default=str).encode()).decode()
        for k, v in metadata.items():
            if k in self._fields and k not in (self.text_field, self.vector_field, METADATA_FIELD):
                d[k] = v
        return d

    def add_texts(
        self,
        texts: Iterable[str],
        metadatas: Optional[list[dict[str, Any]]] = None,
        *,
        ids: Optional[list[str]] = None,
        batch_size: int = 256,
        **kwargs: Any,
    ) -> list[str]:
        texts = list(texts)
        metadatas = metadatas or [{} for _ in texts]
        ids = ids or [str(uuid.uuid4()) for _ in texts]
        vectors = self._embedding.embed_documents(texts)
        for i in range(0, len(texts), batch_size):
            self.client.upsert(
                [self._doc(ids[j], texts[j], vectors[j], metadatas[j]) for j in range(i, min(i + batch_size, len(texts)))]
            )
        return ids

    def add_documents(self, documents: list[Document], **kwargs: Any) -> list[str]:
        ids = kwargs.pop("ids", None) or [d.id or str(uuid.uuid4()) for d in documents]
        return self.add_texts([d.page_content for d in documents], [d.metadata for d in documents], ids=ids, **kwargs)

    def delete(self, ids: Optional[list[str]] = None, **kwargs: Any) -> Optional[bool]:
        """Deletes ``ids``, or with ``parent=`` a document and all its chunks, or with
        ``filter=`` every document that matches."""
        if ids:
            self.client.delete(ids=list(ids))
        elif "parent" in kwargs:
            self.client.delete(parent=kwargs["parent"])
        elif kwargs.get("filter"):
            self.client.delete(filter=_filter(kwargs["filter"]))
        else:
            raise ValueError("give ids, parent= or filter=")
        return True

    # ------------------------------------------------------------------ reads

    def _to_document(self, id: Any, doc: dict[str, Any]) -> Document:
        metadata: dict[str, Any] = {}
        raw = doc.get(METADATA_FIELD)
        if raw:
            try:
                metadata = json.loads(base64.b64decode(raw))
            except ValueError:
                metadata = {}
        else:
            metadata = {k: v for k, v in doc.items() if k not in ("id", self.text_field, self.vector_field)}
        return Document(id=str(id), page_content=doc.get(self.text_field, ""), metadata=metadata)

    def get_by_ids(self, ids: Sequence[str], /) -> list[Document]:
        out = []
        for i in ids:
            d = self.client.get(i)
            if d is not None:
                out.append(self._to_document(i, d))
        return out

    def _score(self, hit: Any) -> float:
        """Similarity for Cosine and Dot (higher is better), squared distance for L2."""
        leg = hit.legs[0] if hit.legs else None
        if leg is None:
            return float("nan")
        return leg.score if self.metric == "L2" else -leg.score

    def similarity_search_by_vector_with_score(
        self,
        embedding: list[float],
        k: int = 4,
        filter: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ) -> list[tuple[Document, float]]:
        hits = self.client.search(
            k=k,
            vector={"field": self.vector_field, "values": list(embedding)},
            filter=_filter(filter),
            group_by=kwargs.get("group_by"),
        )
        return [(self._to_document(h.id, h.document or {}), self._score(h)) for h in hits]

    def similarity_search_with_score(
        self, query: str, k: int = 4, filter: Optional[dict[str, Any]] = None, **kwargs: Any
    ) -> list[tuple[Document, float]]:
        """Nearest documents to ``query``. ``group_by="parent"`` returns each document once."""
        return self.similarity_search_by_vector_with_score(
            self._embedding.embed_query(query), k, filter, **kwargs
        )

    def similarity_search(
        self, query: str, k: int = 4, filter: Optional[dict[str, Any]] = None, **kwargs: Any
    ) -> list[Document]:
        return [d for d, _ in self.similarity_search_with_score(query, k, filter, **kwargs)]

    def similarity_search_by_vector(
        self, embedding: list[float], k: int = 4, filter: Optional[dict[str, Any]] = None, **kwargs: Any
    ) -> list[Document]:
        return [d for d, _ in self.similarity_search_by_vector_with_score(embedding, k, filter, **kwargs)]

    def hybrid_search(
        self, query: str, k: int = 4, filter: Optional[dict[str, Any]] = None, **kwargs: Any
    ) -> list[Document]:
        """Vector and BM25 legs fused (RRF): exact words and meaning together."""
        hits = self.client.search(
            k=k,
            vector={"field": self.vector_field, "values": self._embedding.embed_query(query)},
            text=query,
            text_field=self.text_field,
            filter=_filter(filter),
            group_by=kwargs.get("group_by"),
        )
        return [self._to_document(h.id, h.document or {}) for h in hits]

    def _select_relevance_score_fn(self) -> Callable[[float], float]:
        if self.metric == "Cosine":
            return lambda s: (1.0 + s) / 2.0
        if self.metric == "L2":
            return lambda d: 1.0 - math.sqrt(max(d, 0.0)) / math.sqrt(2)
        return self._max_inner_product_relevance_score_fn

    @classmethod
    def from_texts(
        cls,
        texts: list[str],
        embedding: Embeddings,
        metadatas: Optional[list[dict[str, Any]]] = None,
        *,
        ids: Optional[list[str]] = None,
        client: Optional[Client] = None,
        **kwargs: Any,
    ) -> "CairnVectorStore":
        """A store over ``client`` (a :class:`cairn_db.Client`, or a collection view of one)
        holding ``texts``."""
        if client is None:
            raise ValueError("from_texts needs client=cairn_db.Client(...)")
        store = cls(client, embedding, **kwargs)
        store.add_texts(texts, metadatas, ids=ids)
        return store
