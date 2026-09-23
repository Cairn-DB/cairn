# ADR 0013: Memory residency of vector indexes at 10M+ rows

- Status: accepted (delegated 2026-09-23)
- Date: 2026-09-23

## Context

The owner asked for scale runs through the 3-node cluster at 10M and 50M rows. The cluster runs
as three processes on one machine (ADR 0012): 58 GB RAM, about 48 GB available. Static
membership puts every shard on every node, so the three replicas need three copies of every
index in RAM.

The first YFCC-10M attempt used the defaults: 8 shards, 128 MB memtables, f32 rows kept
next to SQ8. It used 9 GB per node after 2M rows, about 4.5 KB per row per node, and was
stopped before it could exhaust memory. Measured and estimated contributors per shard:

- The loaded segment indexes keep the f32 rows (768 B/row at 192-d), the SQ8 codes (192 B)
  and the HNSW graph (128 B at level 0).
- The ingest transients hold several copies of the recent writes: the active memtable, the
  frozen memtable, the build job's document copy, a second clone made by the replica actor
  for the offloaded build, and the Raft log entries kept in memory until the flush compacts
  them.

## Options considered

1. **Keep the defaults and use fewer rows.** Honest, but 10M would not fit with the three
   replicas.
2. **Read the f32 rows from the segment file for reranking** (pread or mmap). This keeps exact
   reranking, but it puts disk I/O in the synchronous search path. With mmap it also bypasses
   the `Disk` trait (ADR 0011). That is a design change, not a benchmark setting.
3. **SQ8-only residency as an option.** Loaded segments drop their f32 rows and score with SQ8
   everywhere. Builds still use f32 from the memtable, and the segment file keeps the f32 column
   on disk, so the choice is reversible at load time. The default is unchanged.
4. **Remove the redundant clone** of the frozen memtable in the replica's flush job. The
   `finish_flush` path never read the job's copy.

## Decision

Options 3 and 4. `VectorIndexParams::keep_f32`, which defaults to true, and the server's
`--sq8-only` flag select option 3. Option 4 applies always.

## Consequences

- Scoring is SQ8-only when the flag is set: "exact" requests and the rerank step fall back to
  SQ8 distances. The YFCC and BigANN vectors are uint8 at the source, so SQ8 is almost lossless
  on them. Recall measured on these datasets therefore overstates what SQ8-only would give on
  float embeddings (for example CLIP f32). Reports must say so.
- A unit test covers every search path with the f32 rows dropped
  (`sq8_only_residency_answers_every_path`).
- 50M rows with three full replicas on this machine is out of reach even with this option,
  because every node needs about 0.5 KB/row or more for the SQ8 codes, the graph and the
  attributes. 50M needs three machines, fewer copies per host, or a disk-resident index
  (DiskANN-style). That is a design discussion, not a flag.
- Option 2 stays the path if exact reranking at scale is required.
