# 7. Recipes

Complete services, tested against a live node:

- **FastAPI (Python)**: [`examples/backend-fastapi`](../../examples/backend-fastapi). A
  multi-customer knowledge base: ingest with revisions, hybrid search one hit per article,
  deprecate an article, delete an article, erase a customer with a signed proof, and
  `X-Cairn-Token` passed to and from the caller. Real embeddings with `fastembed`
  (`BAAI/bge-small-en-v1.5`, local, no API key). `python setup.py` creates the collection.
- **Express (TypeScript/JavaScript)**: [`examples/backend-express`](../../examples/backend-express).
  The same routes, with `@cairn-db/client`. It ships a stand-in embedding function: plug your
  model in `embed()`.

Frameworks:

- **LangChain (Python)**: [`integrations/langchain-cairn`](../../integrations/langchain-cairn),
  `pip install langchain-cairn`. `CairnVectorStore` with metadata that round-trips, filters,
  `delete(parent=...)`, one hit per document, hybrid search.
- **LangChain.js**: [`integrations/langchain-js`](../../integrations/langchain-js),
  `npm install @cairn-db/langchain`.
- **LlamaIndex**: [`integrations/llama-index-vector-stores-cairn`](../../integrations/llama-index-vector-stores-cairn),
  `pip install llama-index-vector-stores-cairn`. `delete_ref_doc` removes a source document and
  all its nodes.

To run a recipe's tests against a local node:

```bash
docker run -d --name cairn -p 7200:7200 -e CAIRN_HTTP_ADMIN_KEY=dev-admin-key-000000 ghcr.io/cairn-db/cairn:0.3
cd examples/backend-fastapi && pip install -r requirements.txt pytest
CAIRN_URL=http://127.0.0.1:7200 CAIRN_ADMIN_KEY=dev-admin-key-000000 pytest test_app.py
```
