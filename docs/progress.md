# Progress journal (read after SPEC.md and CLAUDE.md; update at every milestone)

## Mandate
2026-09-22: owner delegated all decisions; no review until a fully functional prototype is
delivered. Decisions: `docs/adr/0012-delegated-decisions.md`. Rules: CLAUDE.md "Autonomous
delivery mode". Milestones: `docs/roadmap.md`. Evidence per phase: `docs/reports/phase-N.md`.

## Environment (verified 2026-09-22)
- AMD Ryzen 7 8845HS: 8 cores / 16 threads, AVX2, AVX-512 (F/BW/VL/VNNI/VPOPCNTDQ), FMA.
- 58 GB RAM, ~800 GB free NVMe under /home, /tmp is a 30 GB tmpfs (do not put datasets there).
- Linux 7.2.5 (Fedora 44), io_uring enabled (`/proc/sys/kernel/io_uring_disabled` = 0).
- Rust stable 1.98.1 (pinned by rust-toolchain.toml), nightly 1.100 with Miri (install started
  2026-09-22), cargo-fuzz (install started 2026-09-22). No `protoc`, no `sudo`.
- Python 3.14 + numpy 2.5 available for dataset conversion and ground-truth checks.
- git identity configured; repo initialised 2026-09-22 on `main`.

## Datasets
- SIFT1M: ftp://ftp.irisa.fr/local/texmex/corpus/sift.tar.gz (168 MB, fvecs/ivecs).
- YFCC-10M filtered (Big-ANN NeurIPS'23): files under
  https://dl.fbaipublicfiles.com/billion-scale-ann-benchmarks/yfcc100M/ — sizes recorded in
  `tools/fetch-datasets.sh` when written (Phase 2).
- MS MARCO passage: decide the exact subset at M2.4.

## State
- Phase 0: DONE 2026-09-22 (commit "Phase 0: ...").
- Phase 1: NOT STARTED. Next: M1.1 (traits, executor, blocking reactor, sim clock/disk/rng with
  trace hashing, SPSC queue with loom).

## Next step
M1.1. Create `crates/cairn-runtime`; define in `cairn-core`: `Instant`/`Duration` newtypes,
`Clock`, `Rng`, `Disk`, `Network`, `Spawn` traits, `HashMap`/`HashSet` aliases (rustc-hash),
error types, ids. Executor: single-threaded, slab of tasks, `Rc<RefCell>`-free waker design,
timer wheel or BTreeMap of deadlines. Sim reactor in `cairn-sim`: discrete-event clock, disk
model with unsynced-write loss and torn pages, trace with hash; run each seed twice.

## Open problems
(none yet)

## Log
- 2026-09-22: Phase 0 delivered; mandate changed to autonomous delivery; ADR 0012 written;
  raft-rs removed from cairn-raft; tool installs started.
