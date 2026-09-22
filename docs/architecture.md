# Cairn architecture (Phase 0 proposal)

Status: proposed, 2026-09-22. Companion to SPEC.md; decisions are in `docs/adr/`. Nothing here is
implemented yet.

## 1. One-page picture

```
                 clients (cairn-client, CLI)            other nodes
                          |                                  |
              length-prefixed protobuf frames (ADR 0009)     |
                          |                                  |
 +-------- node ----------|----------------------------------|--------------------------+
 |  core 0             core 1             core 2   ...     core N-1                    |
 |  +-------------+    +-------------+    +-------------+                              |
 |  | executor    |    | executor    |    | executor    |   one thread per core        |
 |  | + reactor   |    | + reactor   |    | + reactor   |   (cairn-runtime, ADR 0002)  |
 |  | (io_uring)  |    | (io_uring)  |    | (io_uring)  |                              |
 |  +------+------+    +------+------+    +------+------+                              |
 |         | owns             | owns             | owns                                |
 |  shards {3, 9, 12}   shards {1, 5, 14}   shards {0, 7, 11}   (shard = Raft group)  |
 |     each shard: raft state machine + log + memtable + segments + deletion bitmaps   |
 |                                                                                      |
 |  cross-core traffic only through SPSC queues (routing a request to a shard's core)  |
 +--------------------------------------------------------------------------------------+
```

A **collection** has a schema and a fixed number of **logical shards** (chosen at creation; no
resharding in v1). A document is routed to a shard by hash of its id; all its modalities, text,
metadata and payload live together in that shard. Each shard is one Raft group (SPEC D3) with
replicas on different nodes; on a node, a shard is owned by exactly one core and its state is never
touched by another core.

## 2. Crate layout (refines SPEC.md section 6)

```
crates/
  cairn-core      ids, schema, errors, time/duration newtypes, deterministic HashMap aliases,
                  traits: Clock, Rng, Disk, Network, Spawn            (no I/O, no runtime)
  cairn-storage   log (the Raft log doubles as the WAL), manifest, memtable, segment container
                  reader/writer, deletion bitmaps, compaction        (depends on core)
  cairn-index     distance kernels, HNSW, BM25 postings, structured indexes, filter bitmaps,
                  segment build                                       (depends on core, storage)
  cairn-query     planner, per-shard executor, fusion                 (depends on core, index)
  cairn-raft      Raft state machine (pure), multi-raft driver, snapshot = segment shipping
                                                                      (depends on core, storage)
  cairn-proto     NEW (Phase 4): .proto schemas, generated messages, framing codec
  cairn-runtime   NEW (Phase 1): per-core executor, reactors (blocking, io_uring), real
                  Clock/Disk/Network/Rng                              (depends on core only)
  cairn-sim       simulated reactor, faults, workload generator, checkers
                                                                      (depends on all engine crates)
  cairn-server    node binary: config, listener, shard placement, coordinator
                                                                      (depends on everything + runtime)
  cairn-client    NEW (Phase 4): Rust client library
tools/
  cairn-bench-gen synthetic attributes and workloads (Phase 1)
  cairn-bench     NEW (Phase 2): dataset loaders (SIFT1M, YFCC-10M, MS MARCO), recall/latency
                  runner, writes bench-results/
fuzz/             NEW (Phase 1): cargo-fuzz targets, excluded from the default workspace build
bench-results/    committed, reproducible results (seed, dataset version, hardware)
```

Dependency rule: engine crates (`core`, `storage`, `index`, `query`, `raft`) never depend on
`cairn-runtime`, on an async runtime, or on `std` I/O (enforced by `clippy.toml`, ADR 0011).
`cairn-sim` and `cairn-server` are the only crates that know how tasks are actually driven.

The new crates are created when their phase starts, not before.

## 3. Trait boundaries (cairn-core), in prose

All traits use native `async fn` without `Send` bounds: a future never leaves the core that
created it.

- **`Clock`**: `now() -> Instant` (monotonic, Cairn's own newtype), `sleep_until(Instant)`, and
  `wall_now() -> WallTime` used only for logging and metrics, never for data. The simulator's
  clock advances only when every task is idle (discrete-event time).
- **`Rng`**: a seeded, splittable generator (`fork(purpose) -> Rng`). Production seeds it from
  the OS once at startup in `cairn-runtime`; the simulator seeds it from the run seed. Engine code
  never asks for entropy.
- **`Disk`**: files are opened by a path relative to the node's data directory. Operations are
  completion-style and take owned buffers: `write_at(file, offset, buf) -> (Result, buf)`,
  `read_at(file, offset, len) -> Result<Buf>`, `append`, `fsync`/`fdatasync`, `allocate`,
  `rename` (atomic replace), `remove`, `list`. Durability semantics are explicit: a write is
  durable only after the matching `fsync` returns; the simulator's disk loses unsynced writes on
  crash and can tear a page.
- **`Network`**: message-oriented between nodes, `send(NodeId, Frame)` best-effort and
  `recv() -> (NodeId, Frame)`, plus a stream primitive for bulk transfer (segment shipping) with
  backpressure. Client connections are accepted by the runtime and surface to the engine as
  frames too. The simulator's network delays, drops, duplicates, reorders and partitions.
- **`Spawn`**: `spawn_local(future)`, `yield_now()`. Long CPU loops (graph build, compaction,
  scans over large bitmaps) call `yield_now` every N items so latency-sensitive tasks on the same
  core keep running.

The runtime crate provides one implementation of each; the simulator provides another. There is
no third path.

## 4. Data model

- Document id: `u64` chosen by the client (stable across upserts).
- Field kinds: `Vector { dims, metric: L2 | Cosine | Dot, quantization: Sq8 }` (several per
  schema: image, audio, text embedding), `Text` (BM25), `I64`, `F64`, `Bool`, `Date` (i64 days or
  ms, client-provided), `Enum` (dictionary-coded), `Set<Enum>` (tags), `Blob` (payload, opaque).
- An upsert carries the whole document; partial updates are a later feature. A delete is a
  takedown of one document id.

## 5. Write path: client to segment

1. A client frame arrives on some core's connection. The server decodes it, computes the shard
   from the document id, and, if the shard is owned by another core, hands the request over an
   SPSC queue to that core (one hop, no locks).
2. The shard's owner core validates the document against the schema and proposes one log entry
   (an upsert batch or a delete batch) to the shard's Raft state machine. If this node is not the
   leader, the request is redirected with a leader hint.
3. Raft replicates the entry (ADR 0008). The Raft log **is** the WAL: `cairn-storage` persists
   entries through `Disk` with CRC32C per record and an fsync before the leader counts its own
   vote for commitment.
4. On commit, each replica applies the entry to its **memtable**: vectors and columns appended to
   in-memory rows, terms added to an in-memory postings map, structured values indexed, and for
   deletes the document id recorded in the shard's deletion set (which covers both memtable rows
   and, via per-segment bitmaps, every existing segment).
5. The leader acknowledges with a token `(shard, applied index)` (ADR 0010).
6. When the memtable exceeds its size cap, the leader **builds a segment** (ADR 0004): columns,
   SQ8 copy, HNSW graph (deterministic build, yields regularly), postings, structured indexes,
   stats; writes it through `Disk`; fsyncs; then publishes it by rewriting the manifest
   atomically. It then proposes a small `SegmentPublished { id, hash, log range }` entry.
7. Followers, on applying `SegmentPublished`, fetch the segment file (streamed with per-chunk
   checksums), verify the hash, install it, and drop the memtable rows covered by its log range.
   Followers never build graphs: SIMD results can differ across CPUs and the graph would diverge.
   A follower that is too far behind receives a snapshot, which is just the manifest plus the
   segments plus the deletion bitmaps.
8. The log is truncated up to the oldest index still needed by the memtable or by a follower not
   yet caught up.
9. **Compaction** merges small segments into larger ones, drops deleted rows physically, and
   publishes the result the same way; deletions applied since the merge started are carried over
   by id.

Takedown latency is therefore the Raft commit + apply latency: the deletion bit flips on every
replica when it applies the entry, before any query on that replica can see the item again,
regardless of segments, graphs or compaction.

## 6. Read path: hybrid query through the planner

1. The coordinator (the core that received the query) parses and validates the query: a
   structured predicate, zero or one text leg, zero or more vector legs, `k`, a fusion method, a
   consistency level.
2. **Planning**: the predicate is normalized and pushed down as-is to every shard; each leg
   becomes a per-shard sub-plan; the consistency level decides which replica of each shard is
   asked (leader for `Linearizable`, any replica with a sufficient applied index for
   `ReadYourWrites`, any for `BoundedStale`; ADR 0010). `Linearizable` adds one ReadIndex round
   per shard, batched.
3. **Per shard, per segment** (and the memtable): build the filter bitmap = structured indexes
   AND NOT deletion bitmap. Estimate selectivity from its cardinality. Vector legs run either an
   exact scan (below the threshold) or HNSW with bitmap-restricted acceptance and two-hop
   expansion (ADR 0003). The text leg runs BM25 document-at-a-time skipping over the same bitmap
   (ADR 0005). Each leg produces a ranked list of k' candidates.
4. **Per shard**: merge each leg across segments by score, keep k' per leg, return per-leg lists.
5. **Coordinator**: merge each leg across shards, fuse once (RRF or weighted, ADR 0007), take the
   top k, fetch payloads for those k only (one point read per shard involved), and reply.

Every stage is core-local except the shard fan-out (SPSC queues on the same node, frames across
nodes) and the final payload fetch.

## 7. Threading and ownership

- One OS thread per core, pinned. One executor per thread. No shared mutable state between
  cores; the only cross-core structures are SPSC queues in `cairn-runtime`, covered by `loom`.
- A shard's Raft state, log writer, memtable, segment list and deletion bitmaps are plain
  single-threaded structures owned by one task on one core.
- Segment files are immutable, so reading them from another core (for a query on a segment
  owned by a shard on this core) is not needed: queries are routed to the owning core, like writes.
- Client connections are distributed across cores by the listener; a request for shard S on core
  A is forwarded to core B and the response travels back the same way.

## 8. Crash recovery (single node)

On start, a shard reads the manifest, opens the listed segments (verifying footers), loads the
deletion checkpoints, replays the log from the manifest's truncation point into a fresh memtable
and deletion set, and rejoins its Raft group. Everything not fsynced before the crash is
reconstructed from the log or absent; the property tests in docs/verification.md check that the
recovered state equals the committed prefix.

## 9. Explicitly not in v1

Resharding, cross-shard transactions, partial updates, phrase queries, learned fusion, PQ, geo
replication, model inference, any HTTP or gRPC endpoint (a gateway comes later, ADR 0009).
