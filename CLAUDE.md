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
