# ADR 0003: Filtered ANN — selectivity-adaptive search per immutable segment

- Status: accepted (delegated 2026-09-22, see ADR 0012)
- Date: 2026-09-22
- Resolves: SPEC.md O2

## Context

Filters down to 0.1% selectivity must keep recall@10 above 95%, including when the filter is
correlated with vector clusters (the realistic case: "rights cleared" correlates with channel and
era, which correlate with visual style). Takedowns must be visible immediately. Segments are
immutable (SPEC D5) and bounded in size, and every segment carries a structured index over its
metadata columns, so a filter can be evaluated to a bitmap *before* any vector work.

Reference benchmark: the NeurIPS'23 Big-ANN filtered track (verified 2026-09-22 in the official
repo README): YFCC 10M CLIP embeddings, `uint8`, 192 dimensions, L2, 100K queries with one or two
required tags out of a 200,386-tag vocabulary, dataset licence CC BY 4.0, baseline `faiss` entry at
about 3,200 QPS on the evaluation machine. Note that this track's filters are tag-membership only;
Cairn additionally needs ranges (dates) and equality on enums, which is why `cairn-bench-gen`
exists.

## Options considered

1. **Filtered HNSW** (traverse the graph, discard non-matching candidates). Simple, one graph.
   At low selectivity the matching points are sparse in the graph, traversal wanders through
   non-matching nodes and either explores far too much or gets stuck; recall collapses exactly when
   the filter is correlated. Qdrant mitigates with extra payload-aware edges (unverified detail).
2. **Filtered-DiskANN / Vamana (FilteredVamana, StitchedVamana)**: graph built with label
   awareness. Strong for label-set filters, but the build must know the filter attributes in
   advance, and it does not cover range or arbitrary predicates.
3. **ACORN** (Patel et al., SIGMOD 2024, "ACORN: Performant and Predicate-Agnostic Search Over
   Vector Embeddings and Structured Data"): HNSW-like graph with denser neighbour lists and
   filter-aware two-hop neighbour expansion at query time. Predicate-agnostic. Costs more memory
   per node and the paper's numbers are not verified by us.
4. **Selectivity-adaptive per segment**: evaluate the predicate to a bitmap first (structured
   indexes AND NOT deletion bitmap). If the bitmap's cardinality is below a threshold `T`, run an
   exact scan over the matching vectors (SIMD kernels over the quantized column, rerank on f32).
   Otherwise run HNSW with bitmap-restricted acceptance, switching to two-hop expansion when the
   acceptance rate drops below a bound.

## Decision

Option 4, with option 3's two-hop expansion as the graph-side fallback. Reasons:

- Segments are bounded (default cap 1M vectors, configurable), so at 0.1% selectivity the exact
  scan touches at most about 1,000 vectors per segment: trivially cheap, and recall is 100% by
  construction. This is the regime where graph methods are weakest and where correlation hurts
  them most; the scan does not care about correlation at all.
- Above the threshold the graph does the work, and there a bitmap check per candidate is cheap
  because the filter is no longer sparse.
- Deletions never touch the graph. A takedown is a bit cleared in the deletion bitmap at log-apply
  time, so takedown visibility equals log-apply latency on every replica.
- The memtable (recent writes not yet in a segment) has no graph at all; it is bounded and always
  scanned exactly.

`T` is a tuning parameter measured, not guessed; the initial guess is 5–20% of the segment.

## Consequences

- Structured indexes must produce bitmaps fast: sorted dictionaries with a bitmap per value,
  sorted columns for ranges. Bitmap representation is decided in ADR 0004.
- The vector column needs fast random access by segment-local id (fixed-width rows) and a
  quantized copy (SQ8) for the scan and graph traversal, with f32 kept for reranking.
- HNSW construction must be deterministic (level assignment from a hash of the doc id, no RNG;
  single-threaded insertion order = log order) so a leader-built segment equals what any replica
  would build. Even so, replicas do not build graphs: the leader ships bytes (ADR 0008,
  docs/architecture.md), because SIMD kernels can differ in low bits across CPU generations and the
  graph is sensitive to that.
- Recall at high selectivity now depends on HNSW parameters (M, efConstruction, efSearch) as usual;
  the sweep is part of Phase 2.

## Experiment that confirms or refutes

Phase 2, milestone M2.2, first week: SIFT1M plus `cairn-bench-gen` attributes. Sweep selectivity
{50%, 10%, 1%, 0.1%} x {random, correlated} x {exact scan, HNSW+bitmap, HNSW+two-hop}. Report
recall@10 and p50/p99 per cell, plot the crossover, fix `T`. Then run the YFCC-10M filtered track
subset (tag filters) and compare with the published baselines. If HNSW+two-hop cannot hold 95%
recall at the 1–5% band on correlated filters, the fallback is to raise `T` (more scanning, more
CPU) and to reduce segment size; the ultimate fallback is a Vamana-style label-aware build for
enum attributes only.
