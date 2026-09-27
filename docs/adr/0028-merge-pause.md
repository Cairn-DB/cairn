# ADR 0028: Pausing merges per node

- Status: accepted (delegated 2026-09-27; the owner said "vas-y pour la pause des fusions")
- Date: 2026-09-27

## Context

- GCP run 7 could not measure queries on an idle cluster. After ingest, the nodes were
  restarted with `--max-segments 1000`, as in run 6. They stayed at 200% CPU for 55 minutes,
  and segments per node went from 111 to 90.
- `--max-segments` only forces merges above its cap. The tiered policy
  (`--target-segment-rows`) keeps merging whatever the cap, so nothing could stop merges.
- The bench's settle test was also fooled by long merges. It waited for the segment list to
  stay unchanged for a few seconds, and a merge lasts minutes (GCP run 5).
- Operators need the same control: holding merges off during peak hours, or before a
  measurement or a backup.

## Options considered

1. **A startup flag only.** Simple, but it needs a restart. Today a restart drops flush files
   that are built but not yet published, so followers rebuild them.
2. **A cluster-wide setting through the Raft log.** Consistent on every node, but each shard
   has its own log, so it would need one command per shard. It is also persistent, while a
   pause is an operational action.
3. **A per-node switch at runtime, plus a startup flag.** Chosen. Merges are started by a
   shard's leader, so a leader on a paused node starts none. Pausing every node pauses the
   cluster.

## Decision

- `JobSlots` (the node's build slots) carries a pause flag. `maybe_compact_job`, the only place
  where a leader selects a new merge, returns when it is set.
  - Merges already building, and merges committed in a shard's log, still complete. A
    committed merge is a decision every replica must install, and leaving it pending would
    hold back later work on that shard.
- Controls:
  - `--merges-paused` at startup;
  - the client request `SetMergesPaused(bool)`, answered by the node that receives it and
    never forwarded;
  - `cairn-bench merges pause|resume --node ...`;
  - HTTP `GET` and `POST /v1/admin/merges`.
- Replica status gains `merges: [running here, committed and not installed]` and
  `merges_paused`. They appear in `cairn-bench status` and `/v1/status`.
- Protocol version 5: the status encoding and a new request tag changed. Version 4 is
  refused, so the upgrade needs a full restart, as for versions 3 and 4 (ADR 0018).
- The bench's settle test now also waits until no replica has a flush or merge pending or
  building.

## Consequences

- Idle measurement becomes: `merges pause` on every node, then wait until `status` shows
  merges running and pending at 0 on every node. No restart is needed.
- The flag lives in memory. A restarted node takes `--merges-paused` again.
- The build slots, and so the flag, are shared by every node of one process (tests and
  single-process demos).
- The admin endpoint has no authentication, like the rest of the HTTP port for now.
- Validation:
  - a simulation test (`merges_pause_and_resume`) checks that no merge happens while paused,
    that status reports it, and that merges happen after resuming with every document
    readable;
  - positive control: with the check disabled, the test fails;
  - proto round trips cover the new request and fields.
