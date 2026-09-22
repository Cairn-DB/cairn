# ADR 0005: Full-text — own minimal BM25 over the Cairn segment format

- Status: accepted (delegated 2026-09-22, see ADR 0012)
- Date: 2026-09-22
- Resolves: SPEC.md O4

## Context

Hybrid queries need BM25 over transcripts and titles, filtered by the same structured predicate as
the vector leg and consistent with it (SPEC D2: one atomic write updates vector, metadata and text).
The full-text feature set required by the spec is small: term and boolean queries with BM25
scoring, field-scoped. Cairn is not a general search engine (SPEC non-goals: no full SQL, small typed
API).

`tantivy` facts (checked 2026-09-22): 0.26.2, released 2026-09-08, MIT, MSRV 1.85, very active.

## Options considered

1. **Embed `tantivy`.** Pros: mature, fast, tokenizers, BM25, block-WAND, faceting. Cons: it owns
   its own segment and commit lifecycle, merge policy and threads, and a synchronous `Directory`
   abstraction built around mmap. Making a tantivy commit atomic with a Cairn segment publication,
   routing its I/O through our `Disk` trait and running it inside the deterministic simulator would
   each need adapters whose feasibility we could not verify in Phase 0. It is also a large
   dependency tree.
2. **Own BM25** inside `cairn-index`, using the segment sections from ADR 0004: term dictionary,
   block-packed postings with skip entries, per-document lengths, BM25 with per-segment statistics.
   Tokenizer: Unicode word segmentation, lowercase, optional ASCII folding; stemming later if
   needed. Pros: one segment lifecycle, one shipping path, deterministic by construction, atomic
   with everything else. Cons: we write and maintain it; quality features (phrase queries,
   stemming, block-max WAND) are extra milestones.
3. **Hybrid**: tantivy for analysis (tokenizers) only, own postings. Little gain: the tokenizer is
   the easy part.

## Decision

Option 2. The atomicity and determinism requirements (D2, D7) are the whole point of the project;
an embedded engine with its own lifecycle undermines both. The v1 scope is deliberately narrow:

- Query forms: term, AND/OR/NOT of terms, field-scoped. Phrase queries deferred (positions off in
  v1, ADR 0004).
- Scoring: BM25 with k1 = 1.2, b = 0.75, per-shard collection statistics in v1 (documented bias
  across shards; global statistics exchange is a Phase 4 item if the benchmark shows it matters).
- Tokenizer: Unicode segmentation (the `unicode-segmentation` crate is light; approve at Phase 2
  kickoff), lowercase, no stemming in v1.
- Execution: document-at-a-time with skips over the filter bitmap; block-max WAND is a Phase 2
  stretch milestone, not v1.

## Consequences

- No `tantivy` dependency. Text quality is ours to measure.
- Every posting list is written once at segment build and merged at compaction like other sections.
- French and multilingual content (the archive scenario) get exact-token matching only in v1;
  stemming/decompounding is a later decision.

## Experiment that confirms or refutes

Phase 2, M2.4: MS MARCO passage dev set, BM25 MRR@10 compared with the published Anserini/Pyserini
BM25 baseline (look up the exact figure when running; do not quote from memory). Target: within
0.01 of the baseline with the same k1/b. Throughput: queries/s on the same set, single core,
reported with p50/p99.
