## Cairn: hybrid search where a deletion is final

Cairn is an open-source distributed database for vector search, full-text search and filters,
replicated with Raft and written in Rust.

It is built around one promise. Once a takedown is acknowledged, the document is gone from
every replica, and a client holding the takedown's token will never read it again, through any
node. That makes Cairn a fit for RAG over regulated or sensitive data, rights management, and
anything where "we deleted it" has to be provable.

**Where it stands: 0.3, developer preview.**

- **Scale:** 50 million 128-dimension vectors on three 8-vCPU machines: filtered and
  unfiltered search at p99 of 31-36 ms, 435 queries per second, and a takedown visible on
  all three machines within 102 ms at p99.
- **Wiring:** your own ids, collections, tenants enforced by the API key, deletion by filter
  and by parent document, retention, partial updates, and signed proofs of deletion.
- **Clients and integrations:** TypeScript and Python clients, and vector stores for
  LangChain and LlamaIndex.
- **Testing:** thousands of simulated fault-injection runs with zero safety violations.

Every number comes with its report, including the misses.

**Get involved.** We are looking for:

- RAG and AI engineers who need retrieval they can trust;
- data-governance and compliance specialists who can say what proof of deletion should cover;
- systems engineers who like consensus, storage and search.

Start with the [README](https://github.com/cairn-db/cairn), the
[roadmap](https://github.com/cairn-db/cairn/blob/main/ROADMAP.md), and issues labelled
`good first issue`.

Contact: contact@cairn-db.com
