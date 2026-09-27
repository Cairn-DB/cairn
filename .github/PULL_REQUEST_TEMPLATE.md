## What and why

<!-- One logical change. Link the issue or ADR. -->

## Evidence

<!-- What you ran and what it showed. Tests added or updated; for durability or consensus, the
chaos campaign (CAIRN_SEEDS=0..3000 ...); for hot paths, the benchmark before and after. -->

## Checklist

- [ ] `cargo fmt --all` and `cargo clippy --workspace --all-targets -- -D warnings` are clean
- [ ] `cargo test --workspace` passes
- [ ] Commits are signed off (`git commit -s`, see CONTRIBUTING.md)
- [ ] A format, protocol or Raft change has an ADR and bumps its version (ADR 0018)
- [ ] Docs updated (API docs, README, ADR) where behaviour changed
