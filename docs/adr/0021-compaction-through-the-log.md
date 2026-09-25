# ADR 0021: Compaction through the log

- Status: accepted (delegated 2026-09-24; step 4 of ADR 0016, requested by the owner)
- Date: 2026-09-24

## Context

After ADR 0016 flushes went through the log, but compaction stayed local: every replica chose
its own merges, at its own time, and built each one itself. Merges are the largest builds (the
GCP 50M compaction took 1 h 50 min per node, three times over), and replicas ended up with
different segment sets, so a snapshot shipped from one replica rarely matched another's files.

## Decision

1. **The leader decides and builds.**
   - Only the leader runs the compaction policy.
   - It reserves an output id in its own namespace (ADR 0017), persisted before use, and never
     reuses one.
   - It reads the inputs' live rows, builds the merged segment (ADR 0019), writes the file
     and proposes `CompactCommit { id, inputs, len, hash, from }`.
   - One compaction at a time: none while another is proposed and not committed, or
     committed and not installed.
2. **Everyone installs on commit**, the leader included, so a leader that loses its leadership
   before the commit cannot diverge. Committed compactions are installed in log order, once
   their inputs are published here (an input may be a flush still being fetched).
3. **Every other replica fetches** the file from the node that built it: `CompactCommit`,
   and now `FlushCommit` too, carry that node (`from`). This includes a new leader, and
   the replica checks length and hash against the commit. It builds locally only when the
   fetch fails, the file is its own (a restart lost it), or shipping is off. At first the
   leader rebuilt pending items itself after a leadership change, and followers then asked it
   for a file it did not have yet (see the measurements).
4. **No cut index.** A merged row is masked at install time when its row in the inputs is
   deleted by then. Input deletion bits record every change this replica has applied since
   the rows were read, and nothing it has not applied. So the merge needs no deterministic
   read point, and installing early never shows a future effect.
5. **Durability.** Committed, uninstalled compactions live in the shard manifest
   (`compactions`), persisted with every manifest write, so neither log truncation nor a
   restart can lose one. `CompactCommit` is idempotent on replay.
6. **Retired files.** Segments replaced by a merge keep their files for 60 election timeouts
   (30 s in production), so followers and snapshot installs still fetching them can finish.
   A restart removes leftovers.
7. **Deterministic validity.** Every replica keeps the segment list as the log defines it:
   published segments, freezes with rows in `FlushBegin` order, and accepted merges applied.
   A `CompactCommit` is accepted only if its inputs are adjacent in that list, so every
   replica accepts or rejects it alike, whatever each has installed yet. The manifest records
   the index of the last commit it reflects (`compacted_through`), and replay skips commits up
   to it: they were decided against the list as it stood then.
8. **Segment I/O off the actor.** Reading merge inputs and writing segment files run in a
   task of their own, and the parallel build runs on a helper thread. The actor only collects
   inputs (paths and deletion bits) and adopts the result. A build writes to a side name
   (`<id>.seg.built`), and the actor moves it into place only if the result is still wanted.
9. **Lost merges are re-proposed, not rebuilt.** A leader that picks the same inputs as its
   last merge proposes that file again. A late duplicate commit is harmless: the id is
   known, or the inputs are already merged.

## Found by the simulation campaign (fixed here)

- **Output ids reused after a snapshot install.** Installing a snapshot replaced this
  replica's compaction counter with the sender's. A later leadership here could reuse an id,
  and two different files shared one id: "deletion set has the wrong size" on restart, and
  diverging documents. Now the counter keeps the maximum of the local and incoming values, and
  reserving an id skips every id of the namespace still known.
- **Snapshot files fetched from the wrong node.** A snapshot's files were fetched from the
  current leader, which may not be the node that sent the manifest. The same id can hold other
  bytes on another replica after a local fallback build, and a verification loop followed. The
  sender is now recorded with the pending snapshot and its files come from it. A remaining
  mismatch makes the snapshot stale, handled like a vanished file (ADR 0017).
- **Fallback loop** (found by the first A/B measurement, not by the campaign): the flush
  driver pruned the fallback set down to pending flushes, dropping compaction ids, so a
  follower whose fetch failed fetched again on every tick (38,885 warnings in one run) instead
  of building.

## The CPU regression, and what it exposed

The first A/B on a local 3-node cluster (2M rows, at most 3 segments per shard) showed the
log-driven version using 45% to 105% more CPU than local compaction. The causes, in order:

1. **Stalled heartbeats, then wasted merges.** The replica actor read merge inputs and wrote
   segment files (hundreds of MB) itself. While it waited on that I/O it sent no heartbeats,
   so elections moved leadership about every 30 to 60 s (37 handovers in one run).
   - A merge proposed just before a leadership change was lost with it, and the next leader
     rebuilt the same merge.
   - With local compaction the same stalls cost nothing extra, because every replica merged
     anyway.
   - Info-level logs showed it: 31 merge builds for 20 proposals, the same inputs merged up
     to four times, and 6.2M rows merged against 4.8M for three local replicas.

   Fixed by decisions 8 and 9 above. The same diagnostic run then used 2,957 CPU seconds
   against about 4,830 for local compaction, settled in 129 s against about 213 s, and saw
   8 handovers.
2. **Fallback loop** (38,885 warnings in one run). The flush driver pruned the fallback set
   to pending flushes and dropped compaction ids, so a failed fetch was retried on every tick
   instead of falling back to a local build.
3. **Rebuild after a leadership change.** A new leader rebuilt committed items instead of
   fetching them. Fixed by fetching from the builder (decision 3).

A check added to the chaos test (replicas with nothing pending hold the same segment list)
then found three more bugs:

- **A Raft safety bug, older than this ADR.** A follower that received a snapshot it
  already covered answered with its last log index. That index included uncommitted entries
  from an older term, which conflicted with the leader's log. The leader counted them as
  matched and committed entries without a majority. A later leader lacked those entries and
  overwrote them (seed 11892). The follower now reports its commit index; a Raft unit test
  reproduces the case and fails on the old code.
- **Replay judged commits against a later state.** After a restart, a replica rebuilt its
  list from a manifest that already reflected later merges, and rejected an earlier commit
  that the others had accepted. Fixed by `compacted_through` (decision 7).
- **A late local build overwrote an installed file.** Seed 26048: a build finished after a
  snapshot had installed the same id. Fixed by the side name (decision 8).

## Evidence

- Store test: a commit waits for its inputs; rows changed after the read are masked; pending
  compactions survive a restart; a corrupt fetch is rejected.
- Cluster test (simulated): with merges running, all replicas end with the same segment list,
  merged segments included, and followers build nothing.
- Campaign: seeds 0 to 60,000, zero violations, with compaction through the log, shipping,
  leader balancing and the segment-list check: 4.70 million segments built and 5.04 million
  fetched. Workspace: 108 tests pass.
- Local cluster, 2M rows, at most 3 segments per shard, 3 alternating runs each
  (`bench-results/phase4-adr0021-compaction.md`): total CPU 2,923 s against 4,903 s for local
  compaction (-40%); builds settle in 128 s against 224 s; ingest 11.5k against 10.2k docs/s;
  recall, query p99 and takedown latency unchanged.

## Consequences

- Segment sets are the same on every replica (ids, and bytes except after local fallback
  builds), so snapshots mostly reuse files.
- Compaction CPU is spent once, on the leader; followers spend network instead.
- A follower that is far behind installs merges only after the flushes they consume.
- Disk: replaced segments linger for the grace period.
- Log format: new command tag 5 (`CompactCommit`), and `FlushCommit` gains the builder; the
  manifest gains a field. Older binaries cannot decode these, so the protocol version is now 4
  and version 3 is refused: upgrading to this build needs a full-cluster restart once.
- A replica that comes back far behind fetches the segments it missed. At first it did so one
  file at a time, with one 256 KiB chunk per round trip. Four campaign seeds then needed
  between 5 and 15 simulated seconds to converge, where local rebuilds had taken no simulated
  time. See "Pipelined fetches" below.

## Pipelined fetches (2026-09-25)

- Up to 4 segment files, flushed or merged, are fetched at once. Merges are fetched ahead of
  their turn, even while the oldest waits for its inputs. Installation stays in log order.
- Each file keeps up to 8 chunk requests of 256 KiB in flight once its length is known.
  Chunks are written at their offset, so arrival order does not matter. Chunks in flight are
  asked again after an election timeout without progress, and the fetch falls back to a local
  build after 4.
- The chaos test is back to a 5 s settle window. Seeds 0 to 60,000 show zero violations, with
  3.0 million segments built and 5.6 million fetched, against 4.7 and 5.0 million before:
  fewer stalled fetches fall back to local builds.
- Local catch-up of a replica that missed 300k rows (`bench-results/phase4-adr0021-catchup.md`):
  7.0 to 8.6 s pipelined against 7.2 to 9.5 s sequential, no measurable difference on
  loopback. Log replay and local rebuilds of flushes that were already merged away dominate.
  Skipping those rebuilds was the next lever (next section). The gain from pipelining is
  expected with network latency, and was not measured on a real network.

## Merges installed over flushes never built here (2026-09-25)

A replica that falls behind can hold committed flushes it never built, whose files the
source has already merged away and purged after the grace period. It used to fetch or rebuild
each such flush, only to replace it with the merge right after.

- When the oldest pending flush has no file, the replica composes the committed merges over
  its logical segment list (decision 7). A merge **subsumes** the flushes when, after it,
  merge outputs no longer appear in the list except that merge's own, and the flushes it
  consumed are the oldest pending ones.
- Once that merge's file is here, the replica installs it in one manifest write:
  - the consumed flushes are published without files, and their log prefix is truncated;
  - the intermediate merges are dropped;
  - the merge replaces the consumed segments.
  Masking follows decision 4: a merged row stays visible only if its row is still live in a
  consumed input. For a flush without a file, that row is a memtable row not changed since
  the freeze.
- While the merge is being fetched, the consumed flushes are neither built nor fetched, and
  fetches already running for them are dropped. The normal path resumes if the merge's fetch
  fails (local fallback) or if the merge was built here (its file lost in a restart).
- Nothing changes on the wire or in the log. The manifest reaches the same state as with the
  flushes built first, so the segment-list check holds. Status counters stay as they were,
  and a subsumption logs at info level.

Evidence:

- Store test: a replica that never built a flush installs a merge of a merge over it, with
  rows deleted and upserted between the freeze and each merge. Documents match the source's,
  before and after a restart.
- Campaign: seeds 0 to 60,000, zero violations (3.02 million segments built, 5.56 million
  fetched). A trial run of 3,000 seeds with a temporary counter saw 4,060 such installs,
  covering 4,215 flushes. Workspace: 109 tests pass.
- Local catch-up (`bench-results/phase4-adr0021-catchup.md`, 3 alternating runs): 3.8 to
  3.9 s against 4.7 to 8.9 s. The lagging node built 0 segments, against 1 to 3.
