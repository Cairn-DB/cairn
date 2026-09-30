# llama-index-vector-stores-cairn

LlamaIndex vector store for [Cairn](https://github.com/Cairn-DB/cairn), the hybrid search
database where a deletion is final.

```bash
pip install llama-index-vector-stores-cairn
```

```python
from cairn_db import Client
from llama_index.core import StorageContext, VectorStoreIndex
from llama_index.vector_stores.cairn import CairnVectorStore, collection_schema

admin = Client("http://localhost:7200", api_key=ADMIN_KEY)
admin.create_collection("docs", collection_schema(1536, filterable={"source": "Enum"}))

store = CairnVectorStore(Client("http://localhost:7200", api_key=KEY).collection("docs"))
index = VectorStoreIndex.from_documents(documents, storage_context=StorageContext.from_defaults(vector_store=store))
index.as_retriever(similarity_top_k=4).retrieve("what changed in 2024?")
index.delete_ref_doc("handbook.pdf")   # the document and all its nodes, everywhere
```

- **Storage.** Each node is stored under its node id, with its source document in `parent`,
  so `delete(ref_doc_id)` is a single deletion by filter. The node's own serialization is
  kept, so nodes come back whole.
- **Filters.** Metadata filters on `filterable` fields: `EQ`, `NE`, `GT`, `GTE`, `LT`, `LTE`,
  `IN`, `NIN`, `ANY`, `ALL`, `CONTAINS`, `IS_EMPTY`, combined with `AND`, `OR` or `NOT`, and
  nested. `doc_ids` restricts the query to those source documents.
- **Query modes.** `DEFAULT` (vector), `HYBRID` (vector + BM25, fused) and `TEXT_SEARCH`
  (BM25).
- **Not supported.** `TEXT_MATCH` filters, and queries by `node_ids` (use `get_nodes`).
- **Deletions are final.** The client carries its consistency token, so later queries never
  return deleted nodes, on any node.
