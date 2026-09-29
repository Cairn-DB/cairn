# Announcement draft (0.3.0)

For the owner to post, where and when they choose (Hacker News "Show HN", Reddit r/rust and
r/LocalLLaMA, LinkedIn, the Rust users forum). Nothing has been posted.

---

**Cairn 0.3: a hybrid search database where a deletion is final**

Cairn is an open-source (Apache-2.0) distributed database for RAG and search. It combines
vector search, BM25 and filters in one query, replicated with Raft, and is written in Rust.

It is built around one promise. Once a takedown is acknowledged, the document is gone from
every replica, and a client holding the takedown's token never reads it again, through any
node. Version 0.3 adds what makes that usable in an application:
- deletion by filter: "everything of this customer", or "this document and all its chunks";
- tenants enforced by the API key;
- retention: documents hidden at their expiry, then deleted;
- signed proofs of deletion, gathered from every replica.

It also comes with collections, your own ids, partial updates, TypeScript and Python
clients, and LangChain and LlamaIndex vector stores.

Measured: 50M vectors on three 8-vCPU machines, p99 of 31-36 ms at 435 QPS. A takedown is
visible on all three machines within 102 ms at p99. Thousands of simulated fault-injection
runs found zero safety violations. Every figure has its report in the repository, misses
included. It is a developer preview, not production-ready: the known limits are listed in
the README.

We are looking for:
- RAG engineers who need deletions they can trust;
- compliance and governance people, to say what a proof of deletion should prove;
- systems engineers who like consensus and storage.

https://github.com/Cairn-DB/cairn
