# Phase 2 report: index layer and single-node hybrid queries

Date: 2026-09-22. Self-approved under ADR 0012 with the evidence below.

## Exit criterion (SPEC.md section 9)

> recall/latency curves on SIFT1M and filtered track, published in `bench-results/`.

Published:

- `bench-results/phase2-kernels.md`: distance kernels, scalar vs AVX2 vs AVX-512, f32 and SQ8.
- `bench-results/phase2-sift1m.md`: SIFT1M unfiltered recall/latency vs ef, and the selectivity ×
  correlation × strategy sweep that fixed the adaptive policy (ADR 0003 outcome).
- `bench-results/phase2-yfcc10m.md`: Big-ANN filtered track at 10M rows, ten 1M-row segments,
  official ground truth: **recall@10 0.9994, p99 ≤ 16.5 ms, 258 QPS single thread**.
- `bench-results/phase2-msmarco.md`: BM25 MRR@10 0.204 on MS MARCO dev small (with titles).

Against SPEC section 8 (measured on the development machine, single thread, 10M rows):

| target | result |
|---|---|
| recall@10 > 95% with < 5% selectivity | 1.000 (scan path), 0.9994 overall on YFCC-10M |
| p99 hybrid query < 100 ms | 16.5 ms worst bucket on YFCC-10M (vector + filter); text+vector hybrid end-to-end latency is measured in Phase 4 |

## What was built

| Milestone | Summary |
|---|---|
| M2.1 | kernels with runtime dispatch, scalar oracle proptests on every level, SQ8 dequant-on-the-fly |
| M2.2 | dense bitmap, deterministic incremental HNSW, exact scan with SQ8 + rerank, `VectorIndex` with adaptive dispatch, exact mode, visit cap |
| M2.3 | `Predicate` AST, per-segment `StructuredIndex` (term row lists, ordered keys), proptest vs document semantics |
| M2.4 | tokenizer, `TextIndex` with interned build, BM25 vs naive reference |
| M2.5 | `SegmentIndexer` hook in the store, `DefaultIndexer`, `cairn-query`: `Query`, RRF/weighted fusion, `ShardEngine` (per-segment indexes, lazily indexed memtable), end-to-end tests vs a reference including takedown visibility |
| M2.6 | `cairn-bench` (SIFT, YFCC, MS MARCO loaders and sweeps), results above |

## Commands

```
cargo fmt --all --check; cargo clippy --workspace --all-targets -- -D warnings   OK
cargo test --workspace                                                            all green
cargo bench -p cairn-index --bench kernels
cargo run --release -p cairn-bench -- sift-sweep --n 1000000 --queries 1000 --ef 32,64,128,256 --clusters 1000 --out bench-results/phase2-sift1m.md
cargo run --release -p cairn-bench -- yfcc-sweep --n 10000000 --queries 10000 --out bench-results/phase2-yfcc10m.md
cargo run --release -p cairn-bench -- msmarco --out bench-results/phase2-msmarco.md
```

## Decisions taken from the numbers

- Adaptive policy: scan when the filter passes ≤ 5% of the segment or ≤ 50k rows; graph search
  otherwise with an 8,192-visit cap; two-hop expansion disabled (ADR 0003 outcome).
- SQ8 kept as the candidate representation with exact rerank (recall cost ≈ 1 point before rerank,
  none after); its memory benefit is what matters at 10M rows.

## Honest gaps

- The text leg's scoring loop is slow (24 q/s single thread on 8.8M passages); correct but to be
  rewritten before end-to-end latency claims that include a text leg.
- Filtered latency grows linearly with segment count; the per-core design and a masked scan are
  Phase 4 work.
- No fuzz targets yet (all parsers are proptested and bounds-checked); Miri not yet run on the
  scalar kernel paths. Both are queued with Phase 4's hardening.
- The MS MARCO baseline figure is from memory and the corpus differs (titles); the run is a sanity
  check, not a leaderboard entry.
