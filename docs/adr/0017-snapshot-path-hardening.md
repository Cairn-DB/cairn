# ADR 0017: Snapshot path hardening (found once the campaign flushed)

- Status: accepted (delegated 2026-09-24)
- Date: 2026-09-24

## Context

While validating ADR 0016, the simulation campaign reported zero segments built. The chaos
scenario writes 12 keys of about 176 bytes, and the memtable limit was 3,000 bytes, so no
memtable ever filled. Every earlier campaign, including the 60,000 seeds cited in ADR 0015,
therefore exercised Raft, the memtable and reads, but never flushes, compaction, segment
masking or snapshot installation with files.

With the limit lowered to 800 bytes (about four documents), about one seed in ten failed. Most
failures were in the snapshot path that predates ADR 0016:

| Seed | Symptom | Cause |
|---|---|---|
| 14 | a leader never applies past its snapshot | snapshot fetch had no retry: one dropped request or chunk stalled it forever |
| 1431 | a follower never hears its leader | a leader that had installed a snapshot had none registered with Raft, so it sent a follower that needed one nothing at all, not even heartbeats |
| 101, 11699 | a read returns a deleted value as absent | snapshot files reflect entries after the snapshot index (compactions, deletion checkpoints); the replica served reads before replaying them |
| 14440 | committed entries lost | a replica that restarted during a snapshot install ignored the log after the snapshot, although it had acknowledged those entries |
| 700 | a replica cannot restart | local publication and compaction continued during the fetch; the install then wrote a manifest referencing a file compaction had deleted |
| 18607, 19 | wrong rows masked, deleted values resurrected | a local deletion checkpoint was kept for a replaced file, or for a file the source had compacted away |
| 607 | no leader can be elected | a node whose term fell behind was rejected by pre-vote as stale and never learned the current term |

The ADR 0016 flush path had one bug of the same class: the leader built from the frozen rows
minus rows changed after the freeze, so a lagging follower installing that file lost rows early.

## Decision

1. **Immutable freezes** (ADR 0016). A frozen memtable keeps exactly the rows cut by its
   `FlushBegin`. Later changes are recorded in its masked set only. Every replica builds
   identical bytes for a freeze, so a fetched file never carries future effects.
2. **Read floor.** Each file chunk carries the server's applied index. A replica that installs
   a snapshot persists the highest one as its read floor, before the install. Reads of every
   consistency level wait until the applied index reaches it, including reads already queued.
3. **Durable snapshot acceptance.** An accepted snapshot is persisted (`SNAPSHOT`) before the
   log restarts after it and before anything after it is acknowledged. On restart, Raft resumes
   on it (`InitialState::installing`) with the logged entries, and the fetch starts again.
4. **Stale snapshots and the vote barrier.** When a file of the snapshot is gone at the source,
   the replica drops the snapshot and the log after it and reopens, so the leader sends a fresh
   one. It had acknowledged those entries, so it persists the highest acknowledged index as a
   vote barrier: until its log reaches that index again it neither votes nor campaigns
   (`InitialState::vote_barrier`). This is the usual remedy for a Raft member that lost part of
   its log.
5. **Fetch robustness.** A stalled snapshot fetch re-requests missing chunks from the same source
   (another replica cannot hold the source's compaction outputs). While a snapshot is being
   installed, the replica does not publish, compact, or finish a compaction. The install checks
   every file against the manifest before changing anything. A deletion checkpoint is kept only
   for a file that is kept, and a missing checkpoint is trusted only when the server still has
   the segment (`NO_CHECKPOINT`); otherwise the snapshot is stale.
6. **Per-replica compaction ids.** Compaction ids carry the node id in bits 40 to 61, so two
   replicas never have different files under the same name.
7. **Pre-vote carries the responder's current term**, and a pre-candidate behind it adopts it
   (as etcd does). This is a real term, not a prospective one, so it does not reintroduce the
   disruption that pre-vote prevents.
8. The chaos scenario flushes about every four documents, and the campaign summary reports the
   segments built and fetched, so a harness that stops exercising a path shows it.

## Consequences

- The campaign (20,000 seeds, then 40,000 more) passes with flushes, shipping, compaction and
  snapshots exercised: about 380,000 segments built and 320,000 fetched per 20,000 seeds.
- After a snapshot install, a replica serves nothing until it has caught up to its source's
  applied index at fetch time. Reads there wait rather than fail.
- A replica that had to drop a stale snapshot cannot vote until it catches up. With one other
  replica down at the same time, the shard has no leader until the down replica returns.
  Safety is kept at the cost of liveness in that double-fault case.
- On-disk formats changed: `RAFT` gains two fields (older files still decode), `SNAPSHOT` is
  new, compaction ids changed, and the `FileChunk` and `PreVoteResp` wire messages gained a
  field (all nodes of a cluster must run the same build; see ADR 0016, step 6).
- Evidence of earlier campaigns covered the log and read paths only. The report and journal say
  so.
