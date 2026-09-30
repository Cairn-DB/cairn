# langchain-cairn

LangChain vector store for [Cairn](https://github.com/Cairn-DB/cairn), the hybrid search
database where a deletion is final.

```bash
pip install langchain-cairn
```

```python
from cairn_db import Client
from langchain_cairn import CairnVectorStore, collection_schema

admin = Client("http://localhost:7200", api_key=ADMIN_KEY)
admin.create_collection("docs", collection_schema(1536, filterable={"source": "Enum"}))

store = CairnVectorStore(Client("http://localhost:7200", api_key=KEY).collection("docs"), embeddings)
store.add_documents(chunks)                      # chunk.metadata["parent"] = the source document
store.similarity_search("what changed in 2024?", k=4, filter={"source": "handbook"})
store.similarity_search("...", k=4, group_by="parent")   # each document once
store.hybrid_search("error E1234", k=4)          # vector + BM25, fused
store.as_retriever()
store.delete(parent="handbook.pdf")              # the document and all its chunks, everywhere
```

- **Schema.** `collection_schema` creates the fields the store needs:
  - the text and the embedding;
  - `parent`;
  - `metadata`, which holds each document's whole metadata as JSON, so any metadata
    round-trips;
  - the `filterable` fields, which store the same metadata keys so they can be filtered.
- **Filters.** Either a plain dict (`{"source": "a"}`: every key equals its value), or a
  Cairn filter (`{"field": "page", "gte": 2}`, `and`/`or`/`not`).
- **Scores.** `similarity_search_with_score` returns the similarity for Cosine and Dot, and
  the squared distance for L2. `similarity_search_with_relevance_scores` maps them to [0, 1].
- **Deletions.** A deletion by ids, by parent or by filter is final. The client carries its
  consistency token, so later searches never return the deleted documents, on any node.
