## Cairn: hybrid search where a deletion is final

Cairn is an open-source distributed database for vector search, full-text search and filters,
replicated with Raft and written in Rust.

It is built around one promise. Once a takedown is acknowledged, the document is gone from
every replica, and a client holding the takedown's token will never read it again, through any
node. That makes Cairn a fit for RAG over regulated or sensitive data, rights management, and
anything where "we deleted it" has to be provable.

**Where it stands (pre-release):** 50 million 128-dimension vectors on three 8-vCPU machines.

- Filtered and unfiltered search at p99 of about 42-44 ms, above 240 queries per second.
- A takedown visible on all three machines within 102 ms at p99.
- 60,000 simulated fault-injection runs with zero safety violations.

Every number comes with its report, including the misses.

**Get involved.** We are looking for:

- RAG and AI engineers who need retrieval they can trust;
- data-governance and compliance specialists who can say what proof of deletion should cover;
- systems engineers who like consensus, storage and search.

Start with the [README](https://github.com/cairn-db/cairn), the
[roadmap](https://github.com/cairn-db/cairn/blob/main/ROADMAP.md), and issues labelled
`good first issue`.

Contact: contact@cairn-db.com
