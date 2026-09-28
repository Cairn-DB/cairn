# CLAUDE.md — working rules for Cairn

Read `SPEC.md` first, every session. It is the source of truth.

## Rules

1. **No phase is "done"** without green tests and reproducible benchmarks. Say what you ran and show the result.
2. **Harness before code**: property tests (`proptest`), fuzz targets, `loom`, Miri for `unsafe`,
   and deterministic simulation for anything touching durability or consensus.
3. **Determinism**: time, network, disk and randomness go through the traits in `cairn-core`. Never call
   `std::time::SystemTime::now`, `rand::thread_rng`-style globals, or raw `std::fs`/sockets from engine code.
4. **Small, atomic commits.** One logical change per commit, message explains *why*.
5. **Record decisions** as ADRs in `docs/adr/` (copy `0000-template.md`). Update `SPEC.md` when a decision changes.
6. **Challenge the spec** when something looks wrong. State the concern and evidence *before* working around it.
7. **Be honest about results.** If a target is missed, or a benchmark is noisy, say so plainly. Never
   fabricate numbers, and never claim a test passed without running it.
8. **Ask before** adding a heavy dependency, changing an on-disk format, or leaving the current phase.
9. Stay within the current phase. Do not build ahead.

## Commands

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo +nightly miri test -p cairn-storage      # unsafe / UB checks
cargo bench -p cairn-index                     # criterion benchmarks
```

## Definition of done (per task)

- Compiles, formatted, clippy-clean.
- Tests added or updated, all green.
- Docs updated (public items, ADR if a decision was made).
- Benchmarks re-run if the change touches a hot path.

## How this project is built

Cairn is developed with an AI coding agent working under these rules. For the context behind
the documents:

- `docs/progress.md` is the journal: state, next step, open problems. It is the hand-off between
  sessions and is updated at every milestone. State is never kept in conversation memory.
- A decision the agent took on the owner's behalf is recorded as an ADR with the status
  `accepted (delegated YYYY-MM-DD)`, with its justification.
- A phase or milestone is closed only with evidence: the exact commands, their results, and
  the benchmark files under `bench-results/`. A missed target is reported as missed.
- Commits are small, on `main`, and say why. Commits written with the agent carry a
  `Co-Authored-By` trailer.
