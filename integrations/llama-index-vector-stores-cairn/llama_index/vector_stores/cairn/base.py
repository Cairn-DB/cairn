"""``CairnVectorStore``: LlamaIndex's vector store interface over a Cairn collection.

Each node is stored under its node id, with its text, its embedding, its source document's id
(``ref_doc_id``) in ``parent``, the serialized node in ``metadata`` (so nodes come back whole)
and the metadata keys that are also fields of the schema in those fields (so they can be
filtered). ``delete(ref_doc_id)`` removes a source document and all its nodes, everywhere.
"""

from __future__ import annotations

import base64
import json
from typing import Any, List, Optional, Union

from cairn_db import Client, and_, eq, in_, is_null, not_, or_, range_
from llama_index.core.bridge.pydantic import PrivateAttr
from llama_index.core.schema import BaseNode, MetadataMode
from llama_index.core.vector_stores.types import (
    BasePydanticVectorStore,
    FilterCondition,
    FilterOperator,
    MetadataFilter,
    MetadataFilters,
    VectorStoreQuery,
    VectorStoreQueryMode,
    VectorStoreQueryResult,
)
from llama_index.core.vector_stores.utils import metadata_dict_to_node, node_to_metadata_dict

METADATA_FIELD = "metadata"


def collection_schema(
    dims: int,
    *,
    metric: str = "Cosine",
    text_field: str = "text",
    vector_field: str = "embedding",
    filterable: Optional[dict[str, str]] = None,
) -> dict[str, Any]:
    """A schema for a store: text, embedding, ``parent`` (the source document), the serialized
    node, and ``filterable`` metadata fields (name -> kind: ``Enum``, ``I64``, ``F64``,
    ``Date``, ``Bool``, ``Set`` or ``Text``)."""
    fields = [
        {"name": vector_field, "kind": {"Vector": {"dims": dims, "metric": metric}}},
        {"name": text_field, "kind": "Text"},
        {"name": "parent", "kind": "Enum"},
        {"name": METADATA_FIELD, "kind": "Blob"},
    ]
    for name, kind in (filterable or {}).items():
        fields.append({"name": name, "kind": kind})
    return {"fields": fields}


def _condition(f: MetadataFilter) -> dict[str, Any]:
    k, v, op = f.key, f.value, f.operator
    if op == FilterOperator.EQ:
        return eq(k, v)
    if op == FilterOperator.NE:
        return not_(eq(k, v))
    if op == FilterOperator.GT:
        return range_(k, gt=v)
    if op == FilterOperator.GTE:
        return range_(k, gte=v)
    if op == FilterOperator.LT:
        return range_(k, lt=v)
    if op == FilterOperator.LTE:
        return range_(k, lte=v)
    if op in (FilterOperator.IN, FilterOperator.ANY):
        return in_(k, list(v))
    if op == FilterOperator.NIN:
        return not_(in_(k, list(v)))
    if op == FilterOperator.ALL:
        return and_(*[eq(k, x) for x in v])
    if op == FilterOperator.CONTAINS:
        return eq(k, v)
    if op == FilterOperator.IS_EMPTY:
        return is_null(k)
    raise ValueError(f"filter operator {op} is not supported by Cairn")


def to_cairn_filter(filters: Optional[MetadataFilters]) -> Optional[dict[str, Any]]:
    """LlamaIndex metadata filters as a Cairn filter."""
    if filters is None or not filters.filters:
        return None
    parts = [
        to_cairn_filter(f) if isinstance(f, MetadataFilters) else _condition(f) for f in filters.filters
    ]
    parts = [p for p in parts if p is not None]
    cond = filters.condition or FilterCondition.AND
    if cond == FilterCondition.OR:
        return parts[0] if len(parts) == 1 else or_(*parts)
    if cond == FilterCondition.NOT:
        return not_(parts[0] if len(parts) == 1 else and_(*parts))
    return parts[0] if len(parts) == 1 else and_(*parts)


class CairnVectorStore(BasePydanticVectorStore):
    """A Cairn collection as a LlamaIndex vector store.

    >>> from cairn_db import Client
    >>> client = Client("http://localhost:7200", api_key=KEY)
    >>> client.create_collection("docs", collection_schema(1536))
    >>> store = CairnVectorStore(client.collection("docs"))
    >>> index = VectorStoreIndex.from_documents(docs, storage_context=StorageContext.from_defaults(vector_store=store))
    >>> store.delete("handbook.pdf")   # a source document and all its nodes
    """

    stores_text: bool = True
    flat_metadata: bool = False
    text_field: str = "text"
    vector_field: str = "embedding"

    _client: Any = PrivateAttr()
    _fields: dict = PrivateAttr()
    _metric: str = PrivateAttr()

    def __init__(self, client: Client, text_field: str = "text", vector_field: str = "embedding", **kwargs: Any) -> None:
        super().__init__(text_field=text_field, vector_field=vector_field, **kwargs)
        self._client = client
        self._fields = {f["name"]: f["kind"] for f in client.schema()["fields"]}
        if vector_field not in self._fields or text_field not in self._fields:
            raise ValueError(f"the collection needs a {vector_field!r} and a {text_field!r} field")
        self._metric = self._fields[vector_field]["Vector"]["metric"]

    @classmethod
    def class_name(cls) -> str:
        return "CairnVectorStore"

    @property
    def client(self) -> Any:
        return self._client

    def _doc(self, node: BaseNode) -> dict[str, Any]:
        d: dict[str, Any] = {
            "id": node.node_id,
            self.text_field: node.get_content(metadata_mode=MetadataMode.NONE),
            self.vector_field: node.get_embedding(),
        }
        if node.ref_doc_id is not None and "parent" in self._fields:
            d["parent"] = node.ref_doc_id
        if METADATA_FIELD in self._fields:
            md = node_to_metadata_dict(node, remove_text=True, flat_metadata=False)
            d[METADATA_FIELD] = base64.b64encode(json.dumps(md, default=str).encode()).decode()
        for k, v in node.metadata.items():
            if k in self._fields and k not in (self.text_field, self.vector_field, METADATA_FIELD, "parent"):
                d[k] = v
        return d

    def add(self, nodes: List[BaseNode], **add_kwargs: Any) -> List[str]:
        batch = int(add_kwargs.get("batch_size", 256))
        for i in range(0, len(nodes), batch):
            self._client.upsert([self._doc(n) for n in nodes[i : i + batch]])
        return [n.node_id for n in nodes]

    def delete(self, ref_doc_id: str, **delete_kwargs: Any) -> None:
        """Deletes a source document and all its nodes."""
        self._client.delete(parent=ref_doc_id)

    def delete_nodes(
        self, node_ids: Optional[List[str]] = None, filters: Optional[MetadataFilters] = None, **delete_kwargs: Any
    ) -> None:
        f = to_cairn_filter(filters)
        if node_ids:
            self._client.delete(ids=list(node_ids), filter=f)
        elif f is not None:
            self._client.delete(filter=f)

    def clear(self) -> None:
        """Deletes every node of the collection."""
        self._client.delete(filter=or_(is_null(self.text_field), not_(is_null(self.text_field))))

    def _node(self, id: Any, doc: dict[str, Any]) -> BaseNode:
        raw = doc.get(METADATA_FIELD)
        text = doc.get(self.text_field, "")
        if raw:
            node = metadata_dict_to_node(json.loads(base64.b64decode(raw)), text=text)
        else:
            from llama_index.core.schema import TextNode

            node = TextNode(id_=str(id), text=text)
        return node

    def get_nodes(
        self, node_ids: Optional[List[str]] = None, filters: Optional[MetadataFilters] = None
    ) -> List[BaseNode]:
        if not node_ids:
            raise ValueError("get_nodes needs node_ids")
        out = []
        for i in node_ids:
            d = self._client.get(i)
            if d is not None:
                out.append(self._node(i, d))
        return out

    def query(self, query: VectorStoreQuery, **kwargs: Any) -> VectorStoreQueryResult:
        if query.node_ids:
            raise ValueError("querying by node_ids is not supported: use get_nodes")
        f = to_cairn_filter(query.filters)
        if query.doc_ids:
            docs = in_("parent", list(query.doc_ids))
            f = docs if f is None else and_(f, docs)
        mode = query.mode
        vector = None
        text = None
        if mode in (VectorStoreQueryMode.DEFAULT, VectorStoreQueryMode.HYBRID):
            if query.query_embedding is None:
                raise ValueError("a vector query needs query_embedding")
            vector = {"field": self.vector_field, "values": list(query.query_embedding)}
        if mode in (VectorStoreQueryMode.HYBRID, VectorStoreQueryMode.TEXT_SEARCH):
            text = query.query_str
        elif mode != VectorStoreQueryMode.DEFAULT:
            raise ValueError(f"query mode {mode} is not supported by Cairn")
        hits = self._client.search(
            k=query.similarity_top_k,
            vector=vector,
            text=text,
            text_field=self.text_field if text is not None else None,
            filter=f,
            group_by=kwargs.get("group_by"),
        )
        nodes, sims, ids = [], [], []
        for h in hits:
            nodes.append(self._node(h.id, h.document or {}))
            ids.append(str(h.id))
            if vector is not None and h.legs and h.legs[0] is not None:
                s = h.legs[0].score
                sims.append(-s if self._metric != "L2" else 1.0 / (1.0 + s))
            else:
                sims.append(h.score)
        return VectorStoreQueryResult(nodes=nodes, similarities=sims, ids=ids)
