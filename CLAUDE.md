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

## Autonomous delivery mode (in force since 2026-09-22)

The owner delegated all decisions and will not intervene until a fully functional prototype is
delivered. These rules amend the ones above for that period:

- Read, in order: `SPEC.md`, this file, `docs/progress.md` (the journal: state, next step, open
  problems). The journal is the hand-off between context windows; update it at every milestone and
  whenever a non-obvious problem is solved. Never rely on conversation memory for state.
- Decisions the owner would have taken are taken by the agent and recorded: ADR status
  `accepted (delegated YYYY-MM-DD)`. Rule 8 (ask before heavy deps / format changes / phase exit)
  becomes: decide, justify in an ADR or in `docs/progress.md`, proceed.
- Phase gates are self-approved only with evidence: a report in `docs/reports/phase-N.md` with the
  exact commands run, their results, and benchmark files under `bench-results/`.
- Commit per milestone or smaller, on `main`, message says why, with the Co-Authored-By trailer.
- Long jobs (> 2 min) run in the background with output to a file; poll, do not block.
- Absolute paths in every shell command (parallel calls share one cwd).
- No `sudo`, no system packages. Tooling is `rustup`/`cargo install` only.
- Datasets live in `data/` (gitignored), fetched by `tools/fetch-datasets.sh` with checksums.
- Hardware target for all benchmarks: this machine (see `docs/progress.md` "Environment").
- Honesty rule unchanged: a missed target is reported as missed, in the report and the journal.
