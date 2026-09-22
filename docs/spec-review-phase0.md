# Phase 0 review of SPEC.md decisions D1–D7

Status: 2026-09-22. Written before any ADR, as CLAUDE.md rule 6 asks: concerns first, then
proposals. Verdict per decision, with the evidence.

## D1 Raft, not leaderless — sound, but the stated rationale is half right

The decision is right for the reason D2 gives (a takedown is one atomic, ordered write). The
rationale written under D1 ("replicas fed the same vectors in different orders build different
graphs") is only true if replicas build their own graphs. Under D5 the leader ships built
segments, so followers never build graphs and their construction order is irrelevant. Worse, even
with a single order, graphs are not guaranteed identical across replicas: SIMD distance kernels
differ in low bits across CPU generations, and HNSW is sensitive to ties. So: keep D1, but state
that the reason is atomicity and a single order for *deletions and visibility*, and make "leader
builds, followers receive bytes" explicit (docs/architecture.md section 5, ADR 0003, ADR 0006).

## D2 Atomic writes across vector + metadata + text — sound within a shard only

Atomicity holds for one document, because all its modalities live in one shard. The spec does
not say that; it should, and it should say that a multi-document takedown is *not* atomic across
shards (no cross-shard transactions is consistent with the non-goals). ADR 0010 defines per-write
tokens and their composition so clients can still get a precise "done" for a set of documents.

## D3 One Raft group per logical shard — sound; two things left open

The number of shards per collection is fixed at creation in v1 (no resharding), and heartbeat
traffic scales with groups × nodes, so the driver must coalesce heartbeats per node pair (risk
R7). Both are recorded as constraints in docs/architecture.md; the fixed shard count needs the
user's confirmation.

## D4 Raft as a pure state machine — sound; `raft-rs` 0.7 is not the base I recommend

The crate is unreleased since 2023-03, needs `protoc` with the codec the scaffold chose, and fixes
old generations of `prost`, `protobuf`, `rand` and `thiserror` in our tree. Its API shape is
right and worth copying. ADR 0008 proposes an own minimal Raft with `raft-rs` kept as a
differential-testing oracle in the simulator, and a dated fallback to `raft-rs` if ours is late.

## D5 Log replication + segment shipping — sound, with an interplay to specify

Followers must apply the log to their own memtable anyway (for takedowns and fresh reads), so
"shipping" means: the leader publishes a segment, followers fetch it and drop the memtable rows it
covers. That protocol (a `SegmentPublished` log entry with a hash and a log range) is in
docs/architecture.md section 5; log truncation is bounded by the slowest follower or by snapshot.

## D6 Follower reads with bounded staleness — sound, but it must not weaken the signature test

The signature test says "no query at any consistency level that promises read-your-takedown".
Bounded-stale follower reads do not make that promise and must say so in the API. ADR 0010 names
the three levels and states exactly which two carry the promise, so the checker can be mechanical.

## D7 Everything nondeterministic behind traits — sound; needs enforcement and two additions

Additions: hash-map iteration order and floating-point dispatch are nondeterminism sources the
spec does not list. ADR 0011 lists all sources, `clippy.toml` bans the `std` paths in engine
crates, and the simulator's double-run trace comparison detects leaks.

## Other spec-level concerns

- Section 8 targets have no hardware or memory model. 50M × 512-d f32 is about 100 GB before any
  index; "RAM-resident with SQ8" and "disk-resident" are different Phase 2 designs (risk R3). A
  hardware target is the most important decision missing from the spec.
- Phase 1's "storage benchmarks vs RocksDB" compares a KV store with a segment store and drags a
  C++ build into CI (risk R10). Proposal: compare WAL append and point reads only, or replace with
  absolute targets.
- Section 8's "N million simulated operations" is sized in docs/verification.md section 7.
- CLAUDE.md rule 4 requires small commits, but the directory is not a git repository. Nothing was
  committed in Phase 0.
- The scaffold's `rust-toolchain.toml` requested the `miri` component on stable; Miri is
  nightly-only. Fixed (removed from the stable component list; CLAUDE.md already runs Miri via
  `cargo +nightly miri`). The nightly toolchain on this machine does not have Miri installed yet.
