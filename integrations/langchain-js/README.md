# @cairn-db/langchain

LangChain.js vector store for [Cairn](https://github.com/cairn-db/cairn), the hybrid search
database where a deletion is final. ESM, Node 20+. Not published to npm yet.

```ts
import { Cairn } from "@cairn-db/client";
import { CairnVectorStore, collectionSchema } from "@cairn-db/langchain";

const admin = new Cairn({ url, apiKey: ADMIN_KEY });
await admin.createCollection("docs", collectionSchema(1536, { filterable: { source: "Enum" } }));

const store = new CairnVectorStore(embeddings, { client: new Cairn({ url, apiKey: KEY }).collection("docs") });
await store.addDocuments(chunks);                      // chunk.metadata.parent = the source document
await store.similaritySearch("what changed in 2024?", 4, { source: "handbook" });
await store.similaritySearchGrouped("…", 4);           // each document once
await store.hybridSearch("error E1234", 4);            // vector + BM25, fused
await store.delete({ parent: "handbook.pdf" });        // the document and all its chunks, everywhere
```

It stores documents in the same layout as `langchain-cairn` (Python). See
that package's README for the schema, filters and scores.
