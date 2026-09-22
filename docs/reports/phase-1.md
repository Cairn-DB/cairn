# Phase 1 report: single-node storage engine and bench-gen v1

Date: 2026-09-22. Self-approved under the delegated mandate (ADR 0012), with the evidence below.

## Exit criterion (SPEC.md section 9)

> Crash-recovery property tests green; storage benchmarks vs RocksDB documented.

- Crash-recovery property tests: green (details below).
- Storage benchmarks: documented in `bench-results/phase1-storage.md`. The RocksDB comparison was
  replaced by absolute numbers plus a `std::fs` baseline (ADR 0012, item 5).

## What was built

| Milestone | Crates | Summary |
|---|---|---|
| M1.1 | cairn-core, cairn-runtime, cairn-sim | Runtime/Disk/Network traits; per-core executor with pluggable reactor, task tags, seeded scheduler; blocking OS disk; simulator with virtual time, disk model (unsynced-write loss, torn writes, bit flips, injected errors), network (delay, drop, partition), trace fingerprint |
| M1.2 | cairn-storage | Segmented log (crc32 records, recovery truncates at first bad record), codec, atomic manifest store |
| M1.3 | cairn-core, cairn-storage | Schema, Document, Command; segment container (page-aligned sections, xxh3 per section, file hash); columns; memtable; deletion sets; shard Store with recovery |
| M1.4 | cairn-storage | Compaction (stale-segment rewrite, smallest-adjacent-pair merge) |
| M1.5 | cairn-storage | Criterion storage benchmarks on the real disk |
| M1.6 | cairn-bench-gen | Library + CLI: controlled selectivity flags (random or cluster-correlated), realistic attributes, takedown schedule |

Deferred from the roadmap: the SPSC cross-core queue and its `loom` test (to M4.2, first use), the
io_uring reactor and `glommio` comparison (to M4.2).

## Commands and results

```
cargo fmt --all --check                      OK
cargo clippy --workspace --all-targets -- -D warnings   OK (clippy.toml determinism bans active)
cargo test --workspace                       40 tests, all green
cargo bench -p cairn-storage --bench storage bench-results/phase1-storage.md
cargo run -p cairn-bench-gen -- attributes --n 2000 --correlation clustered --clusters 50 --out data/bench-tmp/attrs.csv
```

Test inventory that carries the exit criterion:

- `cairn_storage::log::tests::crash_recovery_never_loses_acknowledged_entries`: 150 seeds × 6
  crash/restart rounds in the simulator with `persist_unsynced_prob = 0.5` and torn writes; after
  every recovery the log is a contiguous prefix containing every synced entry.
- `cairn_storage::log::tests::damaged_image_recovers_to_a_prefix`: 400 proptest cases; a valid
  multi-file log image is cut or bit-flipped at an arbitrary byte; recovery yields a prefix or
  reports corruption (never a wrong entry).
- `cairn_storage::store::tests::crash_recovery_matches_model`: 120 seeds × 5 rounds of upserts,
  deletes, flushes and compactions with crashes at random times; the recovered store equals the
  command model replayed to its applied index, which is at least the acknowledged index.
- `cairn_storage::manifest::tests::store_survives_crash_at_any_point_with_old_or_new_manifest`.
- `cairn_storage::segment::tests::corruption_anywhere_is_detected`.
- `cairn_sim` determinism tests: identical trace digests for identical seeds across crash,
  restart, disk and network activity.

## Honest gaps

- Miri has not been run yet: there is no `unsafe` in the crates it would cover (`cairn-storage`
  has none). It becomes relevant with the SIMD kernels in Phase 2 (scalar paths only).
- No fuzz targets yet, although every parser is bounds-checked and proptested. `cargo-fuzz` is
  installed; targets are added in Phase 2 together with the index formats.
- Point reads perform several small `pread`s per field; acceptable now, measured, and on the list.
- The WAL benchmark shows fsync jitter of a consumer NVMe; no p99 figures yet.
