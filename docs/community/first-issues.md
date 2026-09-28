# First issues to open

Drafts, each taken from something observed while building Cairn. They are meant to give newcomers
a way in, from RAG developers to governance specialists to systems engineers. Titles and bodies
are ready to paste.

## 1. `cairn-bench` panics when its output is piped and closed early
Labels: `bug`, `good first issue`, `ops`

`cairn-bench status ... | grep -q ...` makes the bench panic with "failed printing to stdout:
Broken pipe", because `println!` panics on EPIPE. Exit quietly instead: write through a locked
`stdout` and stop on `ErrorKind::BrokenPipe`. Code: `tools/cairn-bench/src/main.rs`, the
`Status` and `Merges` commands.

## 2. `run-cluster.sh` overwrites node logs on every restart
Labels: `ops`, `good first issue`

`tools/scripts/gcp/run-cluster.sh start` redirects each node's output with `>`. A restart
therefore erases the log of the previous run: the ingest logs of GCP run 8 were lost this way.
Append with a timestamped separator, or rotate to `node$i.log.1`.

## 3. Python client for the HTTP API
Labels: `enhancement`, `api`, `help wanted`

A small, typed client (`httpx`) covering the endpoints in `docs/api/http.md`:
- documents, search and takedowns;
- consistency tokens, kept per session so that read-your-writes and read-your-takedowns are
  the default;
- merge pause;
- API keys (`Authorization: Bearer`, ADR 0030) and HTTPS.

It should come with an example that runs a RAG retrieval step and proves a takedown is never
returned again.

## 4. TypeScript client for the HTTP API
Labels: `enhancement`, `api`, `help wanted`

Same scope as the Python client, generated from or checked against `docs/api/openapi.yaml`.

## 5. Deployment guide: three nodes over three zones
Labels: `docs`, `good first issue`

It should cover:
- placement and replication (ADR 0015);
- what latency between sites is acceptable (a round trip under 5 ms is ideal; above 20-30 ms,
  the Raft timers need tuning);
- sizing measured at 50M vectors (64 GB of RAM per node, local NVMe);
- an HTTPS proxy in front of the HTTP port.

## 6. Prometheus metrics endpoint
Labels: `enhancement`, `ops`, `help wanted`

Expose on `/metrics` what replica status already reports (Raft role and term, commit and
applied indexes, memtable and Raft log bytes, segments, flushes and merges running or pending,
merge pause), plus the per-shard search timing (`cairn_query::stats`). No new dependency
without an ADR discussion.

## 7. Avoid the copy before distance computation
Labels: `performance`, `help wanted`

Stack samples under query load put the largest share of search time in a `memmove` inside
`Vectors::distances_to` (`crates/cairn-index/src/vectors.rs`), which gathers SQ8 rows before
computing distances. Computing distances in place, or with a prefetch, could cut search CPU
noticeably. Measure with `cargo bench -p cairn-index` and the local cluster bench.

## 8. Staging files left next to installed segments
Labels: `bug`, `ops`

On the GCP run 8 nodes, some shards held both `<id>.seg` and `<id>.seg.ship.fetch` while the
cluster was idle. Confirm whether the staging file of a completed fetch can survive, for
example when a fetch is abandoned and the segment is then built locally. Then either remove it
at install, or have startup clean it up. Code: `crates/cairn-storage/src/store.rs`
(`staged_fetch_path`, `install_verified`, `reset_fetch_staging`).

## 9. Deletion guarantee: an end-to-end conformance test anyone can run
Labels: `deletion-guarantee`, `help wanted`

A black-box test suite against the HTTP API that a user can point at their own cluster:
- concurrent writes and takedowns;
- node restarts and network cuts (through Docker);
- a check that no read made with a takedown's token ever returns the document.

This is where governance and compliance people can help define what "proof of deletion" should
cover.
