# ADR 0016: The leader builds segments, followers fetch them

- Status: accepted (owner approved the direction 2026-09-24; design delegated). Steps 1 and 2
  of the plan below are implemented with this ADR, step 6 with ADR 0018; steps 3 to 5 are
  not. Validating it exposed bugs in the older snapshot path, fixed under ADR 0017.
- Date: 2026-09-24

## Context

Every replica built every segment and every merge itself: HNSW, SQ8, term lists and postings
from the same log (ADR 0001, D1: a deterministic build gives identical indexes). Building is the
dominant cost of ingest and compaction. On the GCP 50M run, compacting to 3 segments per shard
took about 1 h 50 min on 8 vCPU per node, three times over (`docs/reports/phase-4-scale.md`).
SPEC D5 already called for shipping immutable segments to followers. Only snapshot catch-up
did so.

The owner intends to run Cairn in production. Operational consequences are part of this
decision.

## Decision

**A flush goes through the Raft log in two steps. The leader builds, and followers fetch the
file, with a local build as fallback.**

1. `Command::FlushBegin` (no payload): every replica that applies it at index *i* freezes its
   memtable. The segment id is *i*, so the cut and the id are the same on every replica. Frozen
   memtables queue up (a follower may still be installing an earlier one). They stay readable,
   and later deletions mask rows in them, as before.
2. The leader builds the frozen rows off the actor, writes the segment, publishes it locally and
   proposes `Command::FlushCommit { id, len, hash }` (the file length and body hash).
3. A follower that applies `FlushCommit` fetches `segs/<id>.seg` from the leader. It streams the
   file to disk, checks the length and body hash against the commit, and publishes it in place of
   the frozen memtable. Rows deleted or replaced after the freeze are masked in the new segment,
   from the follower's own record: the file holds exactly the frozen rows, whenever the leader
   built it.
4. **Fallback:** a follower builds the frozen rows itself when the fetch fails (the file is
   missing on the leader, or no data arrives for a while), when it becomes leader with an
   uncommitted freeze, or when shipping is disabled (`ReplicaConfig::ship_segments = false`,
   step 1 of the plan). A frozen memtable never changes after its freeze (later changes only
   go to its masked set, ADR 0017), and builds are deterministic, so a local build yields the
   same bytes as the leader's.
5. **Order:** frozen memtables are published strictly in `FlushBegin` order. Publishing one
   truncates the log up to its index, and a later one must never outlive an earlier one's
   entries.
6. Only the leader proposes `FlushBegin`: when its memtable is full, when it has been idle, and
   only when it has no unpublished freeze of its own.
7. Compaction stays local to each replica for now (step 4 of the plan). Local compaction ids
   carry bit 62, so they never collide with log-index ids.

## Invariants (checked by the simulation campaign and new cluster tests)

- Every replica freezes the same rows under the same id: the freeze is a log entry.
- A takedown applied after a freeze is masked in the segment that replaces the freeze,
  whichever replica built it.
- A follower never truncates its log past a freeze that it has not published.
- Losing the leader between `FlushBegin` and `FlushCommit`, or losing the only copy of a file,
  makes followers fall back to a local build.

## Consequences

- Build CPU drops by the replication factor. The leader may build in parallel (step 3), since
  replicas no longer need byte-identical builds.
- The network carries each segment once per follower: about 0.9 to 1.3 KB per row in SQ8 mode.
  Cross-zone traffic is billed in the clouds. Fetching from any replica that has the file is a
  follow-up.
- Build load concentrates on leaders. Leader balancing becomes necessary (step 5).
- The segment format becomes a cluster-wide contract: a follower must read what the leader
  writes. Rolling upgrades need a format version negotiated in the cluster (step 6, required
  before production).
- Nodes accept segment files from peers: production needs mTLS between nodes. The hash in the
  log detects corruption, not malice from a compromised peer.
- Log format: two new command tags (3 and 4). Older logs still decode. Older binaries cannot read
  new logs.
- Segment ids: flush ids are log indexes, and local compaction ids carry bit 62. A shard
  directory written before this ADR used a sequential counter for both, so an old flush id can
  in theory equal a new log index. No deployed data exists, so no migration is written: shard
  directories from earlier builds must be reloaded.

## Implementation notes (steps 1 and 2)

- Store (`cairn-storage`): a queue of frozen memtables replaces the single frozen slot. Each
  entry keeps its rows as cut, the ids changed after the freeze (masked at publication), the commit
  `(len, hash)` and the local file, if any. `publish_ready` publishes the head of the queue
  only, so the log is never truncated past an unpublished freeze. A freeze whose rows were all
  deleted publishes without a file. Pending freezes are not persisted: after a restart, log
  replay recreates them, and leftover `.fetch` staging files are removed.
- Replica (`cairn-query`): one flush build or compaction at a time per replica, and one segment
  fetch at a time. Fetches reuse the snapshot chunk protocol (`FetchFile`/`FileChunk`, 256 KiB
  per request). A fetch falls back to a local build when the file is missing on the leader,
  its length or hash differs from the commit, or no chunk arrives for 4 election timeouts. A
  follower that waits 200 election timeouts for a commit builds too. A replica that built a
  segment keeps announcing it (`FlushCommit`) whenever it is leader, until a commit for that
  id is applied: a new leader thus unblocks followers of a freeze the old leader never
  committed.
- Upkeep entries (`FlushBegin`, `FlushCommit`, `Noop`) do not reset the idle-flush timer.
- Diagnostics: `ReplicaStatus::flushes` = built here, fetched, pending; printed by
  `cairn-bench status`. Server flag `--no-ship-segments` turns shipping off.

## Plan

1. Two-step flush through the log, local builds everywhere (`ship_segments = false`).
2. Followers fetch, with fallback. **(done with this ADR)**
3. Parallel build on the leader.
4. Compaction through the log (`CompactBegin`/`CompactCommit`) and shipped.
5. Leader balancing.
6. Versioned segment-format contract and mTLS between nodes. **(done: ADR 0018)**
