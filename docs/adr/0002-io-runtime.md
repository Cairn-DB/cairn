# ADR 0002: I/O runtime — own per-core executor over `io-uring`, reactor swappable for simulation

- Status: accepted (delegated 2026-09-22, see ADR 0012)
- Date: 2026-09-22
- Resolves: SPEC.md O1

## Context

SPEC.md asks for thread-per-core execution with io_uring, and D7 requires every source of
nondeterminism (time, disk, network, scheduling) to sit behind injectable traits so the whole engine
can run inside a deterministic simulator. The two requirements interact: whichever runtime we pick
also owns the *scheduler*, and scheduling order is itself a source of nondeterminism. If production
and simulation use different executors, the simulator exercises a different interleaving space than
production and its guarantees weaken.

The engine's I/O surface is small: append and read a log, write and read immutable segment files,
fsync, atomic rename, and message-oriented node-to-node traffic plus client connections.

Facts checked on 2026-09-22 (crates.io API and GitHub API):

| Crate | Latest stable | Released | Repo last push | Notes |
|---|---|---|---|---|
| `glommio` | 0.9.0 | 2024-03-25 | 2026-08-31 | 86 open issues, MSRV 1.65, io_uring only, owns executor and timers |
| `monoio` | 0.2.4 | 2024-08-20 | 2026-07-20 (repo now monoio-rs/monoio) | 88 open issues, owns executor |
| `compio` | 0.19.2 | 2026-08-18 | 2026-09-21 | 23 open issues, completion-based, cross-platform, owns executor |
| `tokio-uring` | 0.5.0 | 2024-05-27 | 2025-07-07 | dormant |
| `io-uring` (tokio-rs) | 0.7.15 | 2026-09-07 | 2026-09-07 | low-level bindings only, no executor |

Unverified: exact kernel version each runtime needs; whether `compio`'s runtime is strictly
thread-local (it appears so from its docs, not confirmed by reading the code).

## Options considered

1. **`glommio`**: the most complete thread-per-core design (task queues with shares, latency
   classes, io_uring for everything). Cons: no release in 2.5 years even though the repo moves;
   its executor cannot be replaced, so the simulator would need a second executor with different
   scheduling semantics; io_uring only, so tests on non-Linux CI are impossible.
2. **`compio`** (or `monoio`): actively maintained (`compio`), completion-based, portable. Same
   structural con: the executor is theirs, the simulator's is ours. `monoio` is stagnant.
3. **Own thin runtime**: a single-threaded executor per core (task slab, wakers, run queue, timer
   wheel, roughly 400–800 lines) plus a *reactor* trait. Production reactor: `io-uring` crate.
   Simulation reactor: the simulator's event queue. Engine code sees only the `Clock`, `Disk`,
   `Network`, `Spawn` and `Rng` traits from `cairn-core`.

## Decision

Option 3. The scheduler is part of the state machine we want to verify, so it must be the same
code in simulation and production; only the reactor differs. The I/O surface is small enough that
owning it is cheaper than adapting around someone else's executor. `io-uring` is light (it depends
on `libc` and `bitflags`) and actively maintained by the tokio team.

Phase 1 does not need io_uring at all: the first production `Disk` implementation is synchronous
`pread`/`pwrite`/`fsync` behind the same trait, run inline on the core (the storage benchmarks in
Phase 1 measure the format and the log, not the reactor). The io_uring reactor lands at the end of
Phase 1 as a spike and becomes the default in Phase 4.

Fallback: if the Phase 1 spike shows `glommio` beating our reactor by more than 2x on WAL append
p99 and we cannot close the gap in a week, adopt `glommio` for production and accept the executor
mismatch, documenting it in a superseding ADR.

## Consequences

- No `tokio` and no other async runtime in engine crates (`cairn-core`, `-storage`, `-index`,
  `-query`, `-raft`). Traits use native `async fn` (stable, verified on 1.98.1) with no `Send`
  bound: everything a core owns stays on that core.
- Engine tasks must be cooperative: no blocking calls, and CPU-heavy work (segment build, graph
  construction, compaction) yields every N items through `Spawn::yield_now`.
- Cross-core communication is limited to SPSC queues owned by the runtime crate; these get `loom`
  tests (see docs/verification.md).
- We own io_uring edge cases: buffer ownership (completion I/O needs owned buffers, so the `Disk`
  trait passes buffers by value and returns them), cancellation, registered files. This is listed
  in docs/risks.md.
- New crate `cairn-runtime` (Phase 1) holds the executor, both reactors' scaffolding, and the real
  `Clock`/`Disk`/`Network` implementations. `cairn-sim` provides the simulated reactor.

## Experiment that confirms or refutes

Phase 1, milestone M1.5: WAL append (4 KiB records, fsync per batch) and segment read (random
64 KiB pages) on the same NVMe device, three backends: sync `pwrite`/`fsync`, own io_uring reactor,
`glommio` on a throwaway branch. Report throughput and p50/p99/p999 with `hdrhistogram`. Budget:
two days.

## Outcome (2026-09-22)

The executor is shared by simulation and production as planned. The production reactor is a
**thread-pool reactor** (`cairn-runtime::pool`): blocking disk and socket work on helper threads,
completions through a channel the executor parks on. The io_uring reactor was not built; the
`Disk` trait is completion-shaped so it remains a drop-in. The "engine never touches the OS"
rule held (clippy bans), and `Runtime::offload` was added for CPU-heavy segment builds.
