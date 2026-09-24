# ADR 0020: Leader balancing through leadership transfer

- Status: accepted (delegated 2026-09-24; step 5 of ADR 0016, requested by the owner)
- Date: 2026-09-24

## Context

Since ADR 0016 the leader of a shard builds its segments and serves them to followers, and
ADR 0019 lets each build use every core. Leaders are wherever elections left them: in the
earlier 2M runs one node led 3 of 4 shards and used about twice the CPU of another. Raft had no
way to move leadership except by failure.

## Decision

1. **Leadership transfer** (Raft thesis 3.10).
   - `Raft::transfer_leadership(to)` stops accepting proposals, which get "not the leader" with
     `to` as hint. It brings `to` up to date.
   - Once `to` has every entry, the leader sends it the new `TimeoutNow` message, and `to`
     starts an election at once, skipping pre-vote.
   - The transfer is abandoned after one election timeout.
   - A follower ignores `TimeoutNow` while it installs a snapshot, and while its vote barrier
     holds.
2. **Balancing by preferred leader.** Each shard's preferred leader is its first host in the
   placement (ADR 0015), which spreads leadership evenly: shard `s` prefers the node at
   position `s mod N` in id order.
   A leader that is not the preferred replica hands leadership to it when all of these hold:
   - the preferred replica has matched everything committed, so a node that is down or
     lagging is never chosen;
   - this replica has led for at least 3 election timeouts;
   - it is not building or holding an unpublished freeze, since the new leader would redo the
     build;
   - it is not installing a snapshot;
   - no handover was attempted in the last 10 election timeouts.

   Server flag: `--no-leader-balancing`.
3. **Protocol version 3** (ADR 0018) adds `TimeoutNow`. Version 2 stays accepted: an older node
   drops the unknown message and the transfer times out.

The simulation campaign runs with balancing on (preferred leader node 1), so transfers happen
under partitions, drops, crashes and snapshots. It found four problems, fixed here:

- **Vote barrier deadlock** (seed 19255). A leader handed leadership to a replica still
  installing a snapshot, whose files then vanished. Two replicas ended up behind vote barriers
  (ADR 0017), and no majority could form again. The barrier now holds the remembered position
  (index, term): a barred replica votes for any candidate at least that up to date, which is
  the decision it would have made with its full log, and still does not campaign.
- **Stale snapshot term after a crash** (seed 19929). The term of an installed snapshot was
  persisted after the install. A crash in between restarted Raft with an older term at the new
  index, so the follower refused every append there. The term is now persisted before the
  install.
- **Manifest index and term persisted separately** (seed 34367, a safety violation). A crash
  between publishing a segment, which stores the manifest's applied index, and storing the
  Raft state, which stored the term of that index, paired a new index with an old term. The
  replica then voted as if its log were older than it was, and helped elect a leader that
  lacked committed entries. The manifest now records `applied_term`, read from the log entry
  and written atomically with the index. Older manifests decode with term 0 and fall back to
  the previous source.
- A compaction refreshed the offered snapshot with Raft's snapshot term as fallback, which may
  belong to another index. It now uses the manifest's term.

## Evidence

- Raft unit tests: a transfer hands over and keeps the log; a transfer to an unreachable peer
  is abandoned and the leader resumes. The Raft chaos harness now issues random transfers,
  with every step checked for election safety, log matching and leader completeness.
- Cluster test (simulated): leadership moves to the preferred replica, leaves it while it is
  down, and returns once it has caught up. No acknowledged write is lost.
- Campaign: seeds 0 to 60,000 with balancing on, zero violations. Workspace: 105 tests pass.
- Local cluster, 2M rows, 2 runs each (`bench-results/phase4-adr0020-balance.md`): balancing
  ends at the optimal 2/1/1 split both times; without it, 0/2/2 and 1/2/1. Ingest is about
  10% higher. Takedown p99 is higher with balancing (134-155 ms against 116-117 ms). That is
  within the earlier run-to-run spread, but seen in both pairs, so it is unexplained and
  flagged.

## Consequences

- A handover makes proposals fail with a hint for up to one election timeout; clients retry.
- The preferred leader is static. It ignores the actual load of each node and the number of
  shards a node leads for other collections. A load-aware balancer would need cross-shard
  coordination on the node.
- Shard directories written before this ADR decode, but their manifests carry no term until
  their next publication.
