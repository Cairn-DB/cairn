# Cairn — Project Specification

> Distributed multimodal database in Rust: filtered vector search + full-text + structured
> filters in a single query plan. Raft-replicated shards, thread-per-core.

This document is the source of truth for the project. Read it fully at the start of every session.
Where it says DECIDED, follow it unless you find a concrete flaw, in which case say so *before*
coding around it. Where it says OPEN, propose and justify.

---

## 1. Context and motivation

Illustrative scenario (hypothetical, NOT a real client): a national audiovisual archive
(think INA) or a large media group.

- Millions of hours of video, tens of millions of images, transcripts, and metadata
  (date, channel, speakers, usage rights).
- Users: journalists, archivists, researchers, external clients buying clips.

Typical stack today: Elasticsearch (text + metadata) + a vector database (pgvector/Qdrant) +
object storage + an application layer that merges results. Three systems, three consistency models.

Typical query:

> "Find sequences where a minister talks about nuclear energy, with a set visually similar to
> this screenshot, between 1995 and 2005, only where online-broadcast rights are cleared."

This combines speech/text search, image-embedding search, structured filters, and fused ranking.

Pain points this project targets:

1. **Fan-out**: 3 round-trips to 3 systems, merged app-side; multi-second p99.
2. **Post-filtering**: ANN returns 1000 candidates, 950 are dropped by the rights filter. Recall and
   latency collapse when filters are selective.
3. **Takedown window**: a rights-holder requests removal. Metadata is updated, but the vector index
   still serves the item for minutes/hours. Content gets sold that should not be. This is a
   legal and financial risk, not just a technical nuisance.

Cairn is the answer: one engine, one plan, atomic writes across vector + metadata + text.

## 2. Goal

A Rust database engine that is:

- **Shard-per-core**: one thread per core, no shared locks on the hot path, async I/O (io_uring on Linux).
- **Replicated with Raft**: one Raft group per *logical shard* (not per core).
- **Multimodal**: first-class embeddings for image / audio / video-segment / text, plus payload and metadata.
- **Hybrid-query native**: filtered ANN + BM25 full-text + structured predicates, one planner.
- **Verifiable**: deterministic simulation testing from day 1.

## 3. Non-goals (v1)

- No leaderless / Dynamo-style replication.
- No multi-region / geo-replication (design so it is *possible* later; do not build it).
- No full SQL. A small, typed query API is enough.
- No model inference inside the database. Embeddings are computed outside and inserted.
- No security-offensive tooling of any kind.

## 4. Decisions made (with rationale)

| # | Decision | Why |
|---|----------|-----|
| D1 | Raft, not Dynamo/leaderless | ANN graphs (HNSW/Vamana) are order-dependent and not mergeable: replicas fed the same vectors in different orders build different graphs and different recall. Raft gives one log, one order, deterministic index construction. |
| D2 | Atomic writes across vector + metadata + text | Hybrid queries require these to be mutually consistent. Takedowns must be visible everywhere at once. |
| D3 | One Raft group per logical shard, several shards per node, spread over cores | One group per core explodes consensus traffic. |
| D4 | Raft implemented as a pure state machine with injectable I/O (candidate: `raft-rs`) | Makes deterministic simulation possible. Validate the crate choice in Phase 0. |
| D5 | Log replication for recent writes, immutable segment shipping for bulk data (Lucene/Quickwit style) | Followers should not rebuild the whole index from the log. Flushes: ADR 0016 (leader builds, followers fetch); compaction is still local. |
| D6 | Followers may serve reads with bounded staleness; leader serves linearizable reads | Read scaling for a read-heavy workload. |
| D7 | Every source of nondeterminism (time, network, disk, randomness, scheduling where feasible) sits behind an injectable trait | Deterministic simulation (FoundationDB / TigerBeetle style) cannot be retrofitted cheaply. |

## 5. Open questions (propose, justify, and challenge my assumptions)

Phase 0 proposals: O1 → ADR 0002, O2 → 0003, O3 → 0004, O4 → 0005, O5 → 0006, O6 → 0007,
O7 → 0008, O8 → 0009 (all `proposed`). ADR 0010 (consistency levels, takedown tokens) and
ADR 0011 (determinism rules) refine D6 and D7. Review of D1–D7: `docs/spec-review-phase0.md`.

- **O1** I/O runtime: `glommio` vs `monoio` vs custom io_uring layer. Consider maintenance status,
  compatibility with deterministic simulation, and how the Raft state machine is driven.
- **O2** Filtered ANN algorithm: filtered HNSW, filtered-DiskANN / Vamana, ACORN-style, or
  segment-level pre-filtering + brute force below a selectivity threshold. Selectivity down to 0.1% must work.
- **O3** Segment format on disk (columnar? row? separate files per index type?), compression, quantization (PQ / SQ / binary).
- **O4** Full-text: own BM25 over the same segment format vs embedding `tantivy`.
- **O5** SIMD strategy for distance kernels (portable vs per-arch intrinsics, runtime dispatch).
- **O6** Hybrid ranking fusion (RRF, weighted score, learned) and where it runs in the plan.
- **O7** Is `raft-rs` (v0.7, old) still the right base, or should we use `openraft`, or write a minimal Raft?
- **O8** Client API and wire protocol (gRPC? custom binary? both?).

## 6. Architecture sketch (refined in Phase 0; see `docs/architecture.md`)

```
crates/
  cairn-core      ids, schema, errors, time newtypes, deterministic HashMap aliases,
                  traits: Clock / Rng / Disk / Network / Spawn      (no I/O, no runtime)
  cairn-storage   log (doubles as the Raft log), manifest, memtable, segment container,
                  deletion bitmaps, compaction, checksums
  cairn-index     distance kernels, HNSW, BM25 postings, structured indexes, filter bitmaps
  cairn-query     hybrid planner + per-shard executor + fusion
  cairn-raft      Raft state machine (pure), multi-raft driver, snapshot = segment shipping
  cairn-runtime   (Phase 1) per-core executor, reactors (blocking, io_uring), real trait impls
  cairn-proto     (Phase 4) protobuf schemas + length-prefixed framing, shared by client and nodes
  cairn-client    (Phase 4) Rust client library
  cairn-sim       simulated reactor, fault injection, workload generator, checkers
  cairn-server    node binary: config, listener, shard placement, coordinator
tools/
  cairn-bench-gen synthetic metadata + workload generator (Phase 1)
  cairn-bench     (Phase 2) dataset loaders, recall/latency runner, writes bench-results/
fuzz/             (Phase 1) cargo-fuzz targets
bench-results/    committed, reproducible results
```

Dependency rule: engine crates (`core`, `storage`, `index`, `query`, `raft`) never depend on
`cairn-runtime`, an async runtime, or `std` I/O; `clippy.toml` enforces it (ADR 0011). New crates
are created when their phase starts. Trait boundaries, the write path (client → Raft log →
memtable → segment → shipped to followers) and the hybrid read path are described in prose in
`docs/architecture.md`.

## 7. Datasets and benchmarks

No real archive data is available. We build a public benchmark with synthetic metadata.

- **Dev**: SIFT1M (and/or GIST1M) + synthetic attributes. Fast iteration on recall and SIMD kernels.
- **Filtered search**: Big-ANN-Benchmarks filtered track (CLIP embeddings of YFCC-10M with tags).
  Verify current details and licence at the official repo before relying on it.
- **Scale (10–50M vectors)**: a LAION subset with precomputed CLIP embeddings (no GPU needed), or DEEP1B subset.
- **Hybrid text**: MS MARCO and/or Wikipedia dumps (BM25 + embeddings).
- **Audio/video**: precomputed embeddings only (Common Voice, MSR-VTT). Multimodality is a schema
  and query-model concern, not a modelling concern.

`cairn-bench-gen` produces per-vector: date, channel, rights status, speaker id, with:

- **Configurable filter selectivity**: 50%, 10%, 1%, 0.1%.
- **Configurable filter/vector correlation**: filter correlated with vector clusters (realistic, hard)
  vs random (easy).
- **Takedown workload**: X% deletions per minute while queries run.

**Signature test**: after a takedown is acknowledged, no query at any consistency level that
promises "read-your-takedown" may ever return the removed item. Verified automatically under
fault injection (crashes, partitions, disk corruption) in the simulator.

Baseline to compare against (later): Elasticsearch + Qdrant + app-side fusion, and single-system
references (FAISS, hnswlib, LanceDB). All benchmarks must be reproducible: seeds, dataset versions,
hardware documented, results committed under `bench-results/`.

## 8. Success criteria

On ~10–50M vectors, on documented hardware:

- p99 hybrid query latency **< 100 ms**.
- recall@10 **> 95%** with a very selective filter (< 5% of documents pass).
- Takedown visible cluster-wide (leader-consistent reads) in **< 1 s**.
- Zero linearizability violations across N million simulated operations with fault injection.
- Hardware cost lower than the fragmented reference stack (hypothesis to validate, not assume).

These are targets to measure against, not claims. Report honestly when they are missed.

## 9. Roadmap and exit criteria

| Phase | Scope | Exit criterion |
|-------|-------|----------------|
| 0 | Architecture plan, crate layout, ADRs, risk list, `cargo check --workspace` green. **No feature code.** | I review and approve the plan. |
| 1 | Single-node engine behind the Clock/Disk/Network traits: WAL, memtable, segments, compaction, checksums, crash recovery. `cairn-bench-gen` v1. | Crash-recovery property tests green; storage benchmarks vs RocksDB documented. |
| 2 | Index layer: SIMD distance kernels, filtered ANN, BM25, structured filters, hybrid planner (single node). | recall/latency curves on SIFT1M and filtered track, published in `bench-results/`. |
| 3 | Deterministic simulator + Raft groups on 3 simulated nodes; fault injection; linearizability checker. | Millions of seeded runs with zero violations; takedown signature test green. |
| 4 | Real networking, shard placement, segment shipping, follower reads, snapshots. | 3-node real cluster passes the same test suite; 10–50M benchmark run. |
| 5 | (Later, out of scope for now) async cross-cluster replication. | — |

Do not start a phase before the previous exit criterion is met and I have approved it.

Milestones per phase, with measurable exit criteria: `docs/roadmap.md`. Risks: `docs/risks.md`.
Verification strategy: `docs/verification.md`.

## 10. Quality bar

- `unsafe` is allowed only for SIMD and I/O boundaries; every block has a `// SAFETY:` comment and is
  covered by Miri where possible.
- Concurrency-sensitive code has `loom` tests. Parsers/formats have `cargo-fuzz` targets.
- Storage formats are versioned and checksummed.
- Public items are documented. `cargo fmt`, `cargo clippy -D warnings` and all tests must pass.
- Performance claims come with a reproducible benchmark or they are not made.
