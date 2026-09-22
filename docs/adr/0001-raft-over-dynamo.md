# ADR 0001: Raft replication instead of leaderless (Dynamo-style)

- Status: accepted
- Date: 2026-09-21

## Context
Cairn serves hybrid queries (filtered vector search + full-text + structured filters). The workload
is read-heavy with upserts by id and few write conflicts. Consistency between vector, metadata and
text is business-critical (takedowns must be visible everywhere at once).

## Options considered
1. **Leaderless (Scylla/Cassandra style)**: high availability, low write latency, but requires
   mergeable state (read repair, anti-entropy, hinted handoff, clocks/tombstones).
2. **Multi-Raft (TiKV/CockroachDB style)**: strong consistency, deterministic replicated log,
   more tractable to verify (TLA+, simulation, linearizability checking); consensus latency on writes.

## Decision
Multi-Raft, one group per logical shard.

## Consequences
- ANN graph construction is deterministic across replicas (same log, same order).
- Atomic write across vector + metadata + text is a single log entry.
- We accept consensus latency on writes and reduced availability under minority partitions.
- If the target workload shifts to massive multi-region active-active ingestion, revisit.
