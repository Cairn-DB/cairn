# ADR 0027: Asynchronous Raft persistence

- Status: accepted (delegated 2026-09-26; the owner approved the plan: "go")
- Date: 2026-09-26

## Context

Every replica step runs, in order: append the new entries to the Raft log, sync the log,
store the hard state (which syncs too), send the messages, apply what is committed. The actor
awaits each sync. ADR 0026 instrumented the steps: under the write load of index builds, a
single-entry log sync takes 0.3 to 1.7 s, and a hard-state store 0.3 to 0.9 s. During that
time the shard's actor serves nothing: no writes, no reads, no heartbeats (election timeout
500 ms). On GCP, 50M rows ingested at 6.5k docs/s with stalls of 5 to 25 minutes, against
25k to 33k docs/s between stalls.

## What must stay true

1. A follower acknowledges entries (`AppendResp`) only once they are durable.
2. A term or vote change is durable before any message that relies on it leaves the node:
   votes, pre-votes, requests for votes, and responses carrying the new term.
3. The leader may send entries to followers before its own copy is durable (Raft thesis
   §10.2.1). It counts itself towards a majority only up to its durable index. `cairn-raft`
   already does this: its `persisted` index feeds `maybe_commit`.
4. A node applies an entry only when it is committed and durable locally. Already true
   (`apply_to = min(commit, persisted)`). The flush pipeline relies on it: the log must hold
   everything the manifest will say was applied.
5. Truncations (a conflicting suffix, a reset for a snapshot) are ordered with appends and
   syncs.

## Options considered

1. **A writer task owning the log.** Every log operation goes through it, and the store
   reads through it. Clean, but the store also reads the log (replay, `applied_term`) and
   truncates its prefix at each publication: shared ownership and a large rewrite.
2. **Only the sync leaves the actor.** Appending writes to the page cache and returns fast;
   the sync is the slow part. The actor keeps appending to the log itself, and a background
   task syncs copies of the file handles written since the last sync. Chosen.
3. **Faster syncs** (group commit only, a separate device for the log). They help, but the
   actor would still wait.

## Decision

- **`cairn-raft`** tracks what it has handed to the driver separately from what is durable.
  `Ready.entries` lists only entries not handed out yet; `advance(persisted, …)` still
  reports durability. A truncation moves the handed mark back.
- **Log**: `Log::sync_plan()` returns the file handles written since the last sync and the
  index they cover. `Log::mark_synced(index)` records the result. Appends, rollovers and
  truncations stay on the actor.
- **Replica**, a single background persistence task at a time:
  - Each ready's entries are appended on the actor (fast). Its hard state is kept as the
    latest to store, and its messages are either sent now or held under a sequence number.
  - When no persistence task is running and there is unsynced work, one starts. It syncs the
    planned files, then stores the latest hard state, then posts `Persisted { seq, index }`.
    Work that arrives meanwhile is covered by the next task: one sync for many batches, a
    group commit.
  - On `Persisted`, the actor calls `raft.advance(index, …)`, then sends the messages held up
    to that sequence.
  - **Sent at once**, because they do not depend on local durability: messages of a leader
    whose term and vote are already durable, that is appends, heartbeats, snapshots,
    `TimeoutNow` and read-index responses. **Held** until their ready is durable: everything
    else, including every follower response, every vote-related message, and any message
    sent after a term or vote change.
  - **Truncations** (conflicting suffix, reset, snapshot install) wait for the running
    persistence task to finish, then run synchronously as today. They are rare.
- The single-node path (`Store::write`) is unchanged.

## Validation plan

- `cairn-raft` unit tests: entries are handed out once; advance-before-persist never counts
  the leader; truncation after entries were handed out.
- The Raft chaos harness gets random persistence delays and crashes between append and sync,
  where anything not synced is lost. Election safety, log matching, leader completeness and
  state-machine safety are checked at every step.
- Simulator: a configurable sync latency (`SimConfig`), and the chaos campaign over 60,000
  seeds with slow syncs and crashes.
- Local A/B: slow-step count and ingest rate. A GCP run only with the owner's go.

## Risks

- This is the core of consensus: a mistake loses acknowledged writes. Each step lands
  separately with its tests, and the campaign runs before any measurement.
- The actor still syncs at publication (manifest, deletion files). It is measured after this
  change before anything else is moved.

## Implementation and evidence (2026-09-26)

- **Raft** (c8cf067): a `handed` mark; `Ready.messages` and `Ready.persisted_messages`.
  - The Raft chaos harness gained an asynchronous mode: syncs at random steps, held
    messages, and crashes that lose anything unsynced. It found a bug: the commit index at
    restart came from the stored hard state, which may lag what was applied, and that moved
    `applied` back. The commit index now covers applied entries.
  - 660 asynchronous seeds pass, with 3 and 5 nodes, in-order and reordered delivery.
  - Positive control: sending follower acknowledgements before the sync makes the harness
    report a lost committed entry.
- **Replica and log** (d215697), as decided above. A log rollover no longer syncs on the
  spot: the next sync covers every file written, in order, and a gap after a crash still
  means unacknowledged files.
- **Simulator**: `DiskConfig::sync_extra_max`. The chaos test runs a third of its seeds with
  syncs up to 30 ms and a third up to 150 ms, beyond the 100 ms election timeout.
- **A bug older than this ADR** (5a3a787) came out of the slow-sync seeds. Snapshot
  installation renamed files before storing the manifest, so a crash in between broke the
  next open. It now stores an intent, then the manifest, then renames, and the open rolls the
  intent forward. The synchronous code hits the same bug (seed 266).
- **Liveness under 150 ms syncs**, on the same 1,000 seeds of that class:

  | | acknowledged no write | other failures |
  |---|---|---|
  | synchronous (before) | 10 | 1 (the snapshot bug above) |
  | asynchronous | 0 | 0 |

  Over the 60,000-seed campaign, about 0.1% of the 150 ms seeds (15 found) still
  acknowledge no write under the injected faults. Proposals keep meeting a node that is not
  leader or is handing leadership over. This is **open**. Those seeds still check every
  safety property.
- Campaign: seeds 0 to 60,000, **zero violations**, 1.72 million segments built and 2.99
  million fetched. Two thirds of the seeds ran with slow syncs. The 150 ms seeds require no
  minimum activity (see above); every other seed requires at least 5 acknowledged writes
  and 5 checked reads.
- **Local A/B** (`bench-results/phase4-adr0027-async-persistence.md`): 3 nodes on one host,
  6M rows, alternating, 2 runs each:

  | | synchronous | asynchronous |
  |---|---|---|
  | ingest | 4,951 / 4,535 docs/s | 5,560 / 7,020 docs/s |
  | Raft syncs on the actor over 250 ms | 94 / 157 | 0 / 0 |
  | actor steps over 500 ms | 165 / 219 | 121 / 98 |

  Raft syncs no longer block the actor. The remaining slow steps are mostly publication
  (`indexes_ready`, about 80 per run: doc store opening, manifest and deletion syncs) and
  adopting a built file (`flush_written`). As foreseen under Risks, these are the next
  lever. The two async runs differ by 26%: the effect on ingest is real but noisy on a
  shared host.

## Publication syncs (2026-09-26, follow-up)

- Timing the publication path, one node, 4M rows: `publish_ready` itself took 22 ms on
  average. What blocked was around it:
  - the Raft-state store that followed each publication, 332 ms on average and up to
    581 ms, because it waits for the log sync;
  - the rename that adopts a built file, 175 ms on average, because it syncs the directory.
- **State after publication**: now stored by the next persistence task. A restart takes the
  snapshot term from the manifest's `applied_term`, which every publication writes with its
  index (ADR 0020); the state's copy only serves older manifests.
- **Renames**: `Disk::rename_nosync` and `Disk::sync_dir`. Adopting a built file and
  installing a fetched flush or merge rename without syncing the directory. The index
  preparation (ADR 0026) syncs it off the actor, before publication. Publication, merge
  installation and subsumption sync it themselves only if a file they reference is not
  covered yet.
- A/B, 3 nodes, 6M rows, 2 runs each (`bench-results/phase4-adr0027-publication.md`):

  | | before | after |
  |---|---|---|
  | actor time in steps over 500 ms | 63.4 / 46.5 s | 36.9 / 34.0 s |
  | longest step | 5.13 / 2.06 s | 2.41 / 1.76 s |
  | slow publication steps | 44 / 45 | 14 / 15 |
  | ingest | 12.3k / 10.8k docs/s | 12.0k / 12.6k docs/s |

  Ingest shows no measurable change on this host. What remains is mostly the end of a
  segment fetch (`net` events): the actor syncs the fetched file and verifies its hash.
  That is the next lever.
- 122 tests; campaign 3,000 seeds zero violations.

## Segment fetches (2026-09-26, follow-up)

- **End of a fetch**: syncing the fetched file and checking its hash (which re-read the whole
  file and hashed it on the core's thread) run in a task. The hash is computed on a helper
  thread from a mapping of the file. The actor only renames the file into place on
  `FetchVerified`, after checking that it is still wanted. A segment being checked is not
  fetched again. Measured alone, this did not remove the slow `net` steps.
- **What did**: instrumenting by frame type showed that the slow `net` steps were disk reads
  of chunks served to peers (mean 1.4 ms, up to 4.2 s under build writes) and writes of
  received chunks (up to 5.1 s). Sends never took more than 1 ms. Serving a chunk and
  writing a received chunk now run in tasks, and the check at the end waits for the writes.
- A/B, comparable pair (`bench-results/phase4-adr0027-fetch.md`): steps over 500 ms 66 -> 26,
  blocked time 53.0 -> 20.0 s, longest 3.49 -> 2.03 s, ingest 9.4k -> 11.2k docs/s. The
  other pair ran under an unrelated CPU-heavy process and is not comparable.
- Frame handling no longer blocks (`handle_ms` 0 in every remaining slow `net` step). What
  remains is in the step that follows. The likely cause is the Raft log append, a page-cache
  write that the kernel can throttle under dirty-page pressure. This was not measured
  separately. Moving appends off the actor is option 1 above (a writer owning the log).
- 122 tests; campaign 3,000 seeds zero violations.

