# ADR 0008: Raft — own minimal pure-state-machine Raft, `raft-rs` kept for differential testing

- Status: proposed
- Date: 2026-09-22
- Resolves: SPEC.md O7; refines D4

## Context

D4 requires Raft as a pure state machine with injectable I/O so it can run in the deterministic
simulator. The scaffold pinned `raft` (raft-rs) 0.7 with the `prost-codec` feature.

Facts checked on 2026-09-22:

- `raft` 0.7.0 was released 2023-03-07 and is the latest release; the repository (tikv/raft-rs)
  was last pushed 2026-05-13, so master moves but nothing is published. With `prost-codec` the
  build script needs a system `protoc` (verified: `cargo check` failed on this machine). With
  `protobuf-codec` it builds without `protoc` in about 4 s (verified in a probe crate). Either way
  it pulls `protobuf` 2.28, `prost` 0.11 (latest is 0.14), `slog`, `rand` 0.8 (coexists with our
  `rand` 0.10; verified) and `thiserror` 1 next to our `thiserror` 2. Its API (`RawNode::step`,
  `tick`, `ready`, `advance`) is exactly the pure state machine D4 asks for; storage is a
  caller-provided trait.
- `openraft` 0.9.25 stable, 0.10 alpha, repo pushed 2026-09-22, actively maintained, MIT/Apache.
  It is an async framework: it spawns its core loop, uses channels and timers through a runtime
  abstraction (`openraft-rt`, `tokio-rt` default). Whether a fully deterministic runtime can be
  plugged in without touching internals is unverified. Its storage and network traits are broad and
  opinionated.

## Options considered

1. **Keep `raft-rs` 0.7 (`protobuf-codec`)**. Pros: battle-tested (TiKV), exactly the right API
   shape, no consensus code to write. Cons: effectively unreleased for 3.5 years; old dependency
   generations we will carry forever; protobuf-encoded messages force a codec we did not choose;
   snapshot/compaction semantics are TiKV-shaped.
2. **`openraft` 0.9**. Pros: maintained, rich features (joint consensus, learners). Cons: it is a
   runtime-driving framework, not a state machine; fitting it under our executor and the simulator
   is unverified and likely to mean living with its scheduling; large API surface.
3. **Own minimal Raft** in `cairn-raft`, written as a pure state machine with an interface shaped
   like raft-rs (`step(msg)`, `tick()`, `ready()` returning messages to send, entries to persist,
   entries to apply, snapshot to install; `advance()` after the caller persisted). Scope v1:
   election with pre-vote, log replication with batching and pipelining, ReadIndex for
   linearizable reads, single-server membership changes (joint consensus deferred), snapshots
   expressed as segment shipping (ADR 0004, docs/architecture.md), log compaction. Estimated
   2,000–4,000 lines plus tests.

## Decision

Option 3, with option 1 as the safety net:

- `raft-rs` 0.7 stays in the workspace only as a **dev-dependency of `cairn-sim`** (move it out of
  `cairn-raft`'s normal dependencies at the start of Phase 3; approval requested in the Phase 0
  summary). The simulator feeds identical message sequences to both implementations and compares
  elected leaders, terms and commit indexes: a differential test that retires the "our Raft has a
  subtle bug" risk cheaply, from the first week of Phase 3.
- If our implementation is not passing the simulator's linearizability and takedown checks by
  milestone M3.3, we ship `raft-rs` behind the same interface and record a superseding ADR.

Why own: Raft is well specified; the hard parts (membership change, snapshot/log interplay,
ReadIndex) are exactly what the simulator is built to hammer; owning the code means the log format,
message codec (ADR 0009) and snapshot mechanism are Cairn's, not TiKV's; and no dependency
generation gets frozen for the project's lifetime.

## Consequences

- This is the largest correctness risk in the project (docs/risks.md, R2). The mitigation is the
  simulator plus differential testing, not confidence.
- The `Message`, `Entry` and `Snapshot` types are Cairn's and encoded with the wire codec of ADR
  0009, so the same framing carries client traffic, Raft traffic and segment shipping.
- Multi-raft concerns (heartbeat coalescing across groups sharing a node pair, per-core group
  placement) live in a driver layer above the state machine.

## Experiment that confirms or refutes

Phase 3, M3.2: differential simulation against `raft-rs` over 10,000 seeded runs with partitions
and crashes; zero divergences in safety properties (election safety, log matching, leader
completeness, state machine safety). Phase 3, M3.5: the million-run campaign with the checkers in
docs/verification.md.
