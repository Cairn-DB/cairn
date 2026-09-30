# Developer guide: building on Cairn

This guide takes you from an empty machine to a multi-customer RAG backend that can prove its
deletions. It assumes you know your web framework and what embeddings are. It does not assume
you know Cairn.

1. [Getting started](01-getting-started.md): run Cairn, get a key, write, search and delete.
2. [Modelling your data](02-data-modeling.md): ids, chunks and their parent, which fields to
   filter on, collections.
3. [Inside a backend](03-backend.md): one client per process, search, consistency tokens
   between requests and services, errors.
4. [Customers and tenants](04-tenants.md): isolation enforced by the server, one backend for
   every customer, erasing a customer.
5. [Document lifecycle](05-lifecycle.md): ingest, update without stale chunks, delete,
   metadata edits, retention.
6. [Compliance](06-compliance.md): the audit trail, signed proofs of deletion, and what they do
   and do not cover.
7. [Recipes](07-recipes.md): complete services in FastAPI and Express, and the LangChain and
   LlamaIndex integrations.

Every code sample in chapters 2 to 6 comes from
[`examples/guide/walkthrough.py`](../../examples/guide/walkthrough.py), which runs them in
order against a live node and checks what the text says they do. The recipes are tested the
same way. The API reference is [`docs/api/http.md`](../api/http.md).

Python is used in the text; the TypeScript client (`@cairn-db/client`) has the same calls in
camelCase (`withTenant`, `forgetTenant`, `proveDeletion`, `groupBy`).
