# ADR 0010: Consistency levels and takedown tokens

- Status: accepted (delegated 2026-09-22, see ADR 0012)
- Date: 2026-09-22
- Refines: SPEC.md D6 and the "signature test" in section 7

## Context

D6 allows followers to serve reads with bounded staleness while the leader serves linearizable
reads. The signature test says: after a takedown is acknowledged, no query at any consistency
level that promises "read-your-takedown" may return the item. "Cluster-wide in under 1 s" (section
8) needs a precise meaning: which reads, on which replicas. Also, a document lives in exactly one
shard (docs/architecture.md), and a takedown request may cover many documents in many shards;
Cairn has no cross-shard transactions (non-goal), so "the takedown is complete" must be defined
per document and composed by the client.

## Decision

Every acknowledged write returns a **token**: the (shard id, applied log index) pair of the write.
Tokens compose by taking the per-shard maximum; a client library does this for a multi-document
takedown and stores the result.

Read consistency levels:

| Level | Served by | Promise |
|---|---|---|
| `Linearizable` | leader, after a ReadIndex round | sees every write acknowledged before the read was issued (real time), across the whole shard |
| `ReadYourWrites(token)` | any replica whose applied index ≥ token on each shard involved | sees every write covered by the token; may wait or redirect if behind |
| `BoundedStale { max_lag }` | any replica | may miss writes newer than `max_lag` (expressed in log entries in v1; wall-clock bound when clocks are trusted, later) |

Read-your-takedown is promised by `Linearizable` and by `ReadYourWrites(token)` with a token that
covers the takedown; it is explicitly **not** promised by `BoundedStale`. The signature test is
expressed exactly on those two levels (docs/verification.md). Section 8's "visible cluster-wide in
under 1 s" is measured as: time from takedown acknowledgement to the moment every replica of the
shard has applied the takedown index, i.e. the moment `ReadYourWrites(token)` succeeds without
waiting on every replica.

## Consequences

- Clients that must never sell a taken-down clip use `ReadYourWrites` with the stored takedown
  token or `Linearizable`; the API documentation says so in the takedown call's docstring.
- Follower reads need the applied index exposed; a `ReadYourWrites` read on a lagging follower
  either waits (with timeout) or returns a redirect hint.
- Multi-shard queries at `Linearizable` cost one ReadIndex round per shard involved; the planner
  batches them.
- The simulator can check all three promises mechanically, since tokens are ordinary values in the
  recorded history.
