# ADR 0011: Determinism rules for engine crates

- Status: accepted (delegated 2026-09-22, see ADR 0012)
- Date: 2026-09-22
- Refines: SPEC.md D7

## Context

D7 says every source of nondeterminism sits behind an injectable trait. Rules that are not
enforced by tooling erode; several sources are easy to miss (hash map iteration order, RNG seeds,
thread spawning, floating-point dispatch).

## Decision

For the engine crates (`cairn-core`, `cairn-storage`, `cairn-index`, `cairn-query`, `cairn-raft`)
and the simulator core:

1. **Time**: no `std::time::{Instant, SystemTime}`. `cairn-core` defines its own monotonic
   `Instant`/`Duration` newtypes and a `Clock` trait. Wall-clock values that end up in data (dates
   of archive items) come from the client, never from the engine.
2. **Randomness**: no `rand::rng()`/`thread_rng`, no `getrandom`. Components receive an `Rng`
   from the runtime; seeds are derived as `hash(run_seed, node_id, shard_id, purpose)` so streams
   are independent and reproducible. HNSW level assignment uses a hash of the doc id, not an RNG.
3. **I/O**: no `std::fs`, `std::net`, `std::thread`, `std::process`. Only the `Disk`, `Network`,
   `Spawn` traits.
4. **Hash maps**: no `std::collections::{HashMap, HashSet}` with `RandomState`. `cairn-core`
   exports `HashMap`/`HashSet` aliases with a fixed-seed hasher (candidate: `rustc-hash` 2.x, light;
   approval at Phase 1 kickoff). Any iteration whose order is observable must be over a sorted or
   insertion-ordered structure.
5. **Threads**: none. Parallelism is cores driven by the runtime; long CPU work yields.
6. **Floating point**: the simulator forces the scalar kernels (ADR 0006). Production replicas do
   not rebuild graphs from the log; they receive bytes (ADR 0003, ADR 0008).
7. **Enforcement**: `clippy.toml` at the workspace root lists the disallowed methods and types
   (added in Phase 0; `clippy -D warnings` is already mandatory). Crates that legitimately do real
   I/O (`cairn-runtime`, `cairn-server`, `cairn-bench-gen`, tests) opt out with a crate-level
   `#![allow(clippy::disallowed_methods, clippy::disallowed_types)]` and a comment, so the exception
   is visible in review.
8. **Detection**: every simulator seed is run twice and the event-trace hashes must match; a
   mismatch fails the run and points at the leak (docs/verification.md).

## Consequences

- Slightly more ceremony (aliases, trait objects) in engine code; in exchange, any bug seen in
  simulation is reproducible by seed.
- Third-party crates used inside engine crates must obey the same rules; that is a review item
  for each new dependency.
