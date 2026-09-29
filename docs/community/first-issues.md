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

## 3. Prometheus metrics endpoint
Labels: `enhancement`, `ops`, `help wanted`

Expose on `/metrics` what replica status already reports (Raft role and term, commit and
applied indexes, memtable and Raft log bytes, segments, flushes and merges running or pending,
merge pause), plus the per-shard search timing (`cairn_query::stats`). No new dependency
without an ADR discussion.

## 4. Avoid the copy before distance computation
Labels: `performance`, `help wanted`

Stack samples under query load put the largest share of search time in a `memmove` inside
`Vectors::distances_to` (`crates/cairn-index/src/vectors.rs`), which gathers SQ8 rows before
computing distances. Computing distances in place, or with a prefetch, could cut search CPU
noticeably. Measure with `cargo bench -p cairn-index` and the local cluster bench.

## 5. Staging files left next to installed segments
Labels: `bug`, `ops`

On the GCP run 8 nodes, some shards held both `<id>.seg` and `<id>.seg.ship.fetch` while the
cluster was idle. Confirm whether the staging file of a completed fetch can survive, for
example when a fetch is abandoned and the segment is then built locally. Then either remove it
at install, or have startup clean it up. Code: `crates/cairn-storage/src/store.rs`
(`staged_fetch_path`, `install_verified`, `reset_fetch_staging`).

## 6. Deletion guarantee: a conformance suite anyone can run under faults
Labels: `deletion-guarantee`, `help wanted`

`examples/acceptance/` checks a healthy cluster, and the simulator checks the engine under
faults. What is missing is a black-box suite that a user points at their own cluster, which
injects faults through Docker (node restarts, network cuts) while writes, takedowns and
deletions by filter run concurrently. It checks that no read made with a takedown's token
ever returns the document, and that a proof of deletion (`POST /v1/deletions/proof`) agrees.

## 7. Raft errors name shard 0
Labels: `bug`, `good first issue`

Errors raised inside `cairn-raft` (`propose`, `read_index`, `transfer_leadership`) carry
`ShardId(0)`, because the Raft crate does not know its shard. A client therefore reads "not
the leader for shard 0" whatever the shard. The replica
(`crates/cairn-query/src/replica.rs`) should put its own shard in these errors before passing
them on.

## 8. Acceptance suite: retention, partial updates and proofs of deletion
Labels: `good first issue`, `api`

`examples/acceptance/cairn_acceptance.py` covers 0.1 and 0.2, and collections. Add sections for
the rest of 0.3, as in `docs/api/http.md`:
- a collection with `expires_field`: documents hidden from their expiry;
- `PATCH` and `POST /v1/documents/patch`: fields changed and kept, `null` clears, missing
  documents not created;
- `POST /v1/deletions/proof`, checked with `cairn-server verify-proof`.

## 9. Kafka ingestion: discuss ADR 0024
Labels: `enhancement`, `needs-adr`, `help wanted`

[ADR 0024](../adr/0024-kafka-ingestion-and-deletion-propagation.md) proposes a Kafka sink with
end-to-end read-your-takedown (source offsets in the log, per-shard watermarks, Kafka-offset
tokens), and `forget-watch`, a monitor of deletion propagation to every downstream copy. Its
open questions need people who run Kafka and change-data-capture in production: Debezium
envelopes, several topics into one collection, watermark overhead, and the "applied up to"
endpoint that other sinks could implement.

## 10. What should a proof of deletion prove?
Labels: `deletion-guarantee`, `help wanted`

Cairn signs a report from every replica (ADR 0031, "step C.4"). Compliance and governance
people: what is missing for it to count as evidence?
- key rotation and publication;
- chaining successive proofs;
- timestamps from a trusted source;
- covering segment files that still hold masked bytes until a merge;
- what an auditor expects to read.

## 11. Document counts per collection
Labels: `enhancement`, `good first issue`

`GET /v1/collections/{c}` returns the definition only. Add the number of live documents, as
the sum of each shard leader's count (`ReplicaStatus.live_docs`) over the collection's shards
(`crates/cairn-server/src/http.rs`, `crates/cairn-server/src/node.rs`).
