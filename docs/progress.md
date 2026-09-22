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
- Phase 1: IN PROGRESS.
  - M1.1 DONE 2026-09-22: cairn-core (ids, time, error, hash aliases, SeededRng, Runtime/Disk/
    Network traits), cairn-runtime (executor with pluggable Reactor, tags, seeded scheduler;
    blocking OS disk; RealRuntime), cairn-sim (Simulation, SimReactor, SimDisk with unsynced-loss
    and torn writes, SimNetwork with delay/drop/partition, Trace digest). 20 tests. The SPSC
    queue + loom item is deferred to M4.2 (no cross-core traffic before real networking).
  - M1.2 DONE 2026-09-22: cairn-storage log (segmented files, crc32 records, recovery truncates
    at first bad record, suffix/prefix truncation), codec, manifest store (tmp+sync+rename).
    Tests: 150-seed sim crash/recovery with torn writes, 400-case damaged-image proptest,
    manifest crash-at-any-point, real-fs roundtrip. 29 tests total.
  - M1.3 DONE 2026-09-22: schema/document/command types (cairn-core), segment container
    (page-aligned sections, xxh3 per section, file hash, crc'd TOC), document columns, memtable,
    deletion sets (.del files via the manifest container), shard Store (log + memtable +
    segments + manifest; replay from manifest.applied_index). 120-seed crash test vs model.
  - M1.4 DONE 2026-09-22: compaction (rewrite stale segments, merge smallest adjacent pair
    when over max_segments), bulk column reads, orphan cleanup. 36 workspace tests.
  - M1.5 DONE 2026-09-22: criterion storage benchmarks on the NVMe, results in
    `bench-results/phase1-storage.md` (fsync 2-4 ms dominates; 14.5k rec/s at batch 64;
    100k-row segment build 120 ms). io_uring/glommio spike deferred to M4.2.
  - M1.6 DONE 2026-09-22: cairn-bench-gen lib + CLI (flags at 50/10/1/0.1%, random or
    cluster-correlated; realistic attrs; takedown schedule).
- Phase 1: DONE 2026-09-22. Report: `docs/reports/phase-1.md`.
- Phase 2: IN PROGRESS.
  - M2.1 DONE 2026-09-22: kernels (scalar/AVX2/AVX-512, f32 + SQ8), proptests vs scalar on all
    levels, `bench-results/phase2-kernels.md`. MSRV 1.89.
  - M2.2 DONE 2026-09-22 (code): bitmap, deterministic incremental HNSW, exact scan, VectorIndex
    (adaptive scan/graph/two-hop, SQ8 + rerank, exact mode, visit cap), segment sections, tests
    vs brute force. 100k smoke sweep: scan wins below ~10%; uncapped filtered graph search is
    catastrophic at 0.1% (22 ms); 1M sweep pending -> `bench-results/phase2-sift1m.md`.
  - M2.3 DONE 2026-09-22: Predicate AST (cairn-core::filter), StructuredIndex (term lists,
    ordered keys, IsNull via nulls), proptest vs document semantics, segment round-trip.
  - M2.2 sweep DONE: `bench-results/phase2-sift1m.md` (scan <5% exact & fast; capped graph
    above; two-hop dropped; defaults updated).
  - M2.4 DONE 2026-09-22: TextIndex (tokenizer, postings, BM25 vs naive reference, section).
  - M2.5 DONE 2026-09-22: SegmentIndexer hook in Store, DefaultIndexer, cairn-query (Query,
    fusion RRF/weighted, ShardEngine with per-segment indexes + lazily rebuilt memtable
    indexes), end-to-end tests vs reference incl. takedown visibility across memtable/segments.
  - M2.6 IN PROGRESS: YFCC-10M sweep (download running), phase report.

## Next step
M2.1 kernels (compile, test, bench, commit), then M2.2 vector search: HNSW (deterministic build,
level from hash of doc id), exact scan, filter bitmaps, selectivity-adaptive dispatch, SQ8 +
rerank; SIFT1M loader in a new `tools/cairn-bench` crate; selectivity x correlation sweep.

## Previous step (kept for context)
M1.2: `cairn-storage::log` (segmented files named by first index, header with magic/version,
records `[len][crc32][term][index][payload]`, recovery truncates at the first bad record,
suffix truncation for Raft, prefix truncation by whole files), `codec` (bounds-checked
reader/writer, fuzzable), `manifest` (tmp + sync + rename). Tests: sim-driven crash/recovery
property test, byte-level truncation/corruption proptest, real-fs smoke test.

## Design notes that are not in the code
- Engine code is generic over `R: Runtime`; the runtime is cloned into every component.
- Sim: one executor for all nodes; tasks tagged by node id; `Simulation::crash` cancels tasks,
  drops the node's events and inbox, and applies disk crash semantics.
- Disk trait: data ops take effect at completion (after latency); directory ops at issue.

## Open problems
(none yet)

## Log
- 2026-09-22: Phase 1 closed (40 tests). Bench lesson: consumer NVMe fsync jitter makes
  batch-1 vs batch-16 comparisons meaningless; only report group-commit throughput.
- 2026-09-22: M1.2 done. Lesson: edition 2024 reserves `gen`; `truncate_suffix` below the
  first file's start must empty the file.
- 2026-09-22: M1.1 done (executor, blocking runtime, simulator core). Lesson: with a seeded
  scheduler, two tasks' issue order is not their spawn order; per-file ordering is per task.
- 2026-09-22: Phase 0 delivered; mandate changed to autonomous delivery; ADR 0012 written;
  raft-rs removed from cairn-raft; tool installs started.
