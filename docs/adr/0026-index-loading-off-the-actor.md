# ADR 0026: Segment indexes are prepared off the replica actor

- Status: accepted (delegated 2026-09-26; the owner asked for it after the GCP 50M runs)
- Date: 2026-09-26

## Context

After a flush is published or a merge installed, the engine loaded the new segment's
indexes inside the replica actor. That meant the structured index, the text index, the vector
store (the whole f32 column, even under `--sq8-only`, where it was dropped right away) and
the HNSW graph, with every section hashed and decoded on the actor's thread.

For a merged segment of 3M 128-d rows, that is gigabytes of work. The shard's actor served
nothing meanwhile: no writes, no reads, and no Raft heartbeats, with a 500 ms election
timeout. On the GCP 50M runs (`bench-results/phase4-gcp-bigann50m-2026-09-25.md`) this
matched what was seen:
- a node stopped answering status requests during ingest;
- leadership moved around (0 to 6 shards per node);
- ingest stalled for 5 to 25 minutes at a time.

Commit 27d5ffc removed the f32 read under SQ8 residency: a node restart on 18 segments went
from 6.0 s to 2.6 s. That still leaves about 0.6 s per 3M-row segment on the actor, over the
election timeout.

## Options considered

1. **Await a helper thread from the actor.** It frees the core's thread, but the actor still
   processes nothing for that shard while it waits, heartbeats included.
2. **Prepare before publication, asynchronously.** When a segment's file is in place, its
   indexes are decoded on a helper thread, and the segment is published only once they are
   ready. This is the pattern builds and fetches already follow (ADR 0021). Chosen.

## Decision

- `SegmentReader::mapped` returns a `MappedSegment`: the file mapped (`Disk::map`) with its
  section table. It needs no runtime, and every section read is checked against its hash.
  `StructuredIndex`, `TextIndex` and `VectorIndex` gain a synchronous `decode` from it. Their
  async `load` becomes "map, then decode".
- `ShardEngine::index_job(id)` does the cheap part on the actor (open, map) and returns an
  `IndexJob`. `IndexJob::run` decodes everything on any thread, into `PreparedIndexes`. The
  engine keeps these until the segment is published, and `refresh` then adopts them instead
  of loading.
- The replica gates publication:
  - flushes: `publish_ready_where` stops at the first segment whose indexes are not ready;
  - merges: `install_compactions_where`, same rule;
  - merges installed over flushes (subsumption): same rule.

  Missing preparations are started and come back as `IndexesReady` events. Meanwhile the
  actor keeps serving Raft, writes and reads.
- A preparation that fails is logged, and the segment loads its indexes at publication as
  before. Segments published by other paths (startup, snapshot install, single-node use) also
  load at publication, with the decode offloaded to a helper thread.
- Prepared indexes of segments that will not be published (merged away first) are dropped.

## Evidence

- Tests: 120 pass. A new test checks that SQ8-only decoding equals loading everything then
  dropping the f32 rows. The campaign (3,000 seeds, zero violations) covers publication,
  merges, merges over flushes, snapshots and crashes through the gated path.
- **It did not fix the stalls.** Local A/B, alternating, 2 runs each: one node, 2 shards,
  12.5M rows, merges to 3M rows (`data/run-idxload-ab.sh`). A probe timed a status request
  every 0.5 s:

  | | ingest | probes > 500 ms | probes > 2 s | max |
  |---|---|---|---|---|
  | load on the actor (27d5ffc) | 17,044 / 17,061 docs/s | 18 / 18 | 13 / 11 | 5.1 s |
  | prepared off the actor | 17,283 / 17,019 docs/s | 19 / 25 | 11 / 11 | 5.1 s |

  No difference: decoding was not what held the actor.
- **What does hold it**, measured with the new per-step instrumentation (a replica step over
  500 ms is logged with its parts; a 6M-row ingest):
  - a Raft log append and sync of a single entry: 0.3 to 1.7 s;
  - a hard-state store (sync): 0.3 to 0.9 s;
  - publication (`indexes_ready`): 0.6 to 2.4 s, which opens the doc store and syncs the
    manifest and deletions;
  - adopting a built file (`flush_written`, rename and sync): 0.5 to 1.5 s.

  The common factor is disk syncs waiting behind the builds' large writes. The host runs
  btrfs; the GCP VMs run ext4, where this was not measured separately.
- Writing segment files in 8 MB pieces with a sync every 32 MB was tried, and gave 23 slow
  events against 22. It was reverted.

## Consequences

- Kept, because decoding (about 0.15 s per 700k-row segment, 0.6 s per 3M rows, measured
  at restart) no longer runs on the actor. But it was not the bottleneck.
- **The stalls come from synchronous persistence on the actor.** Raft log and hard-state
  syncs, and the manifest syncs at publication, take 0.3 to 1.7 s under build write load.
  The fix is to persist asynchronously, as etcd does: the actor hands entries to a writer and
  keeps working, and acknowledgements follow durability. It touches Raft's core
  (persist -> send -> apply), so it needs its own ADR and full campaign coverage.
- The per-step instrumentation stays: "slow replica step", "slow Raft log append and sync"
  and "slow hard-state store" are logged at warn level.
- Publication waits for decoding. A segment becomes visible slightly later, which was
  already the case in effect, since the actor was busy decoding. Memtables and the Raft log
  keep the rows until then.
- Still on the actor: opening the segment and its doc store (doc ids, deletions) at
  publication, and reading merge inputs' metadata. These are much smaller than the indexes,
  and not measured separately.
