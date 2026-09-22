# ADR 0004: Segment format — single container file, columnar sections, SQ8 first

- Status: accepted (delegated 2026-09-22, see ADR 0012)
- Date: 2026-09-22
- Resolves: SPEC.md O3

## Context

A segment is the immutable unit of storage, replication (shipping, SPEC D5) and indexing. It holds
vectors for one or more vector fields, metadata columns, text postings, structured indexes and the
ANN graph for a bounded set of documents. It must be checksummed and versioned (SPEC section 10),
byte-identical across replicas so shipping can be verified by hash, and cheap to open (no parse of
the whole file).

## Options considered

1. **Row-oriented single file**: simplest writer; terrible for scans (a filter scan would read
   vectors it does not need) and for shipping partial data.
2. **Lucene-style directory of files**, one file per column/index: easy to add sections, but many
   small files per segment make shipping, atomic publication and io_uring file registration
   clumsier (Quickwit bundles them for this reason).
3. **Single container file per segment**: a header with magic + format version, then page-aligned
   (4 KiB) sections, then a footer with a table of contents (section name, offset, length, hash)
   and its own checksum. Readers `pread` the footer, then only the sections they need.

## Decision

Option 3. Sections in v1:

| Section | Layout | Notes |
|---|---|---|
| `docids` | sorted u64 doc ids, segment-local id = position | binary search for point lookups; bloom filter section beside it |
| `vec.<field>.f32` | fixed-width rows, dims from schema | rerank and exact scan fallback |
| `vec.<field>.sq8` | per-dimension min/max + u8 rows | traversal and scan; f32 kept for rerank |
| `col.<field>` | fixed-width for i64/f64/bool/date; dictionary-coded u32 for enums; offsets + bytes for strings/blobs | metadata and payload |
| `sidx.<field>` | sorted dictionary + one bitmap per value (enum), or sorted (value, local id) pairs (ranges) | structured indexes → filter bitmaps |
| `graph.<field>` | CSR adjacency per level, fixed max degree M | HNSW |
| `text.<field>.terms` / `.postings` | sorted term dictionary; block-packed doc ids + term frequencies with skip entries | BM25 (ADR 0005); positions optional, off in v1 |
| `stats` | doc count, per-field norms, avg doc length, min/max per column | planner |

Mutable state is kept **outside** the segment so the file never changes after publication:

- the deletion bitmap (tombstones live in the log; a per-segment `.del` checkpoint file is written
  at compaction and on snapshot),
- nothing else. Compaction merges segments and applies deletions physically.

Checksums: xxh3-64 per section and for the footer; CRC32C for log records (short, hardware
accelerated). Both crates are already in the workspace. Compression in v1: bit-packing for
postings only; no general-purpose compressor (adding `lz4_flex` or `zstd` for payload blobs is a
later, separate decision). Quantization in v1: SQ8 mandatory copy for vector fields; PQ deferred to
Phase 4 if the 50M target does not fit in RAM on the chosen hardware; binary quantization is an
experiment, not a feature.

Bitmaps: roaring-style (run/array/bitmap containers). Whether to write our own or use the
`roaring` crate (1.x, actively maintained; light) is a dependency question for the Phase 1 kickoff.

The manifest (list of live segments, their `.del` checkpoints, the log truncation point) is a
separate small file replaced by atomic rename; it is versioned and checksummed like everything
else. Format changes bump the version and require an ADR (CLAUDE.md rule 8).

## Consequences

- Readers touch only what a query needs: a filter-only query never reads vectors.
- Shipping a segment is one file plus its checksum; a follower verifies before installing.
- Page alignment costs a little space per section and makes O_DIRECT/io_uring reads simple.
- The SQ8 copy costs 1 byte/dimension on top of f32 (4 bytes/dimension). Memory at scale is
  tracked in docs/risks.md.
- Positions being off in v1 means no phrase queries in v1 (see ADR 0005).

## Experiment that confirms or refutes

Phase 1, M1.3: write/read microbenchmark of the container (segment of 1M rows, 128-d) with
page-aligned vs unaligned sections. Phase 2, M2.2: recall@10 loss f32 → SQ8 with rerank on SIFT1M
and YFCC-10M (expectation, unverified: under one point). Fuzz targets on the footer and section
parsers from day one (docs/verification.md).
