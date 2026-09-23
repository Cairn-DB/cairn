# ADR 0014: Disk-resident vector index (Vamana + PQ, DiskANN-style)

- Status: accepted (owner approved the direction 2026-09-23; design delegated)
- Date: 2026-09-23

## Context

With three replicas, 50M rows do not fit in one host's RAM. The settled footprint is 0.43 KB per
row per node even in SQ8-only mode, and every node hosts every shard (ADR 0013,
`docs/reports/phase-4-scale.md`). In RAM, each row costs its SQ8 codes (d bytes), its HNSW
level-0 lists (128 B) and attributes. The owner approved three levers: a bigger host, fewer
copies per host (placement, done), and a disk-resident index. This ADR covers the third.

## Options considered

1. **mmap the existing HNSW sections and f32 column.** No new index, but each hop touches a
   neighbor list and a vector in two unrelated pages, over hundreds of visited nodes per query.
   Cold queries would take tens of milliseconds per segment.
2. **DiskANN-style: Vamana graph + PQ codes in RAM + co-located node blocks on disk.** Navigation
   uses PQ distances computed from RAM. Only the nodes the search *expands* are read from disk.
   Each read returns the full vector, for an exact distance, together with the neighbor list, in
   one page. About L = 100 reads per segment search, one page each. This is the published
   design (Subramanya et al., NeurIPS 2019) and its filtered variant.
3. **IVF-PQ on disk.** It is simpler, but recall at the selective filters that matter here is
   worse, and it adds a second index family next to the graph.

## Decision

Option 2, as a third *residency* of `VectorIndex`: `memory` (f32 + SQ8 + HNSW, the default),
`sq8` (ADR 0013), and `disk`.

- **PQ.** M = dims / 4 subspaces with 256 centroids each (one byte per subspace), trained per
  segment by seeded k-means on at most 64k rows. Queries build an M × 256 distance table
  (asymmetric distance). About 32 B per row stay in RAM at 128-d, 48 B at 192-d.
- **Vamana.** Max degree R = 48, build list L = 96, alpha 1.2. The start node is the medoid. Two
  passes (alpha 1, then alpha), with rows inserted in row order and a seeded tie-break, so the
  build is deterministic (ADR 0001/D1: replicas build identical graphs).
- **Disk layout.** Section `vamana.<field>`: a header, then fixed-size node blocks
  `[f32 × dims][u32 degree][u32 × R]`, packed so that no block straddles a 4 KiB page.
  Section `pq.<field>`: the codebooks and the codes.
- **Search.** A beam search over PQ distances with a candidate list L. Expanding a node reads its
  block and records its exact distance, and the results are the best exact distances among
  expanded nodes. Filters follow the ADR 0003 policy: a scan of the allowed rows' PQ codes, then
  an exact rerank from disk, when a filter passes at most 5% of the segment or at most 50k rows,
  and otherwise a graph search that traverses all nodes but only admits allowed ones.
- **I/O.** A new `Disk::map` returns a read-only view of an immutable file. The real runtimes
  implement it with `memmap2`, an approved dependency already in the workspace; the `unsafe` sits
  at the I/O boundary with a SAFETY note, since segments are write-once and renamed into
  place. The simulator returns the file's bytes, so behaviour stays deterministic. Page faults
  block the core that takes them. An io_uring beam search with W in-flight reads is the
  follow-up that removes that stall.

## Consequences

- Memory per row per node drops from about 0.43 KB to about 0.1 KB (PQ codes plus attributes).
  50M × 3 replicas then needs about 15 GB of RAM plus page cache. It fits the development
  machine, where the data takes about 115 GB of NVMe.
- Latency depends on the page cache. Warm queries cost CPU only; cold queries cost about
  L page faults per segment. Benchmarks must say whether they were warm or cold.
- A corrupt section must fail cleanly. The loader validates the header, the block count and
  the neighbor ids before any search, and proptests feed it damaged bytes.
- The format is new and versioned (header magic and version). The existing segments are
  unchanged: the residency is chosen at build time per segment and recorded by which sections
  exist.
