# ADR 0015: Shard placement, follower reads, Raft flow control and write ordering

- Status: accepted (delegated 2026-09-24)
- Date: 2026-09-24

## Context

The 10M-50M runs (`docs/reports/phase-4-scale.md`) used a real 3-machine cluster for the first
time. They exposed limits of the Phase 3/4 protocol:

- every node hosted every shard;
- linearizable reads had to be served by each shard's leader;
- the leader re-sent unacknowledged appends without a window;
- the replica persisted the Raft hard state before the log entries.

## Decisions

1. **Static placement with a replication factor** (`--replication N`). Shard *s* lives on N
   consecutive nodes, by id, starting at *s* mod the node count. A node that does not host a
   shard answers "not the leader" with a hosting node as hint, and the coordinator forwards,
   following up to three hints and trying other hosts on transport failure. Membership stays
   static. Adding or removing a replica of a live shard (joint consensus or single-server
   changes) is the next step and is not implemented.
2. **Follower reads** (Raft thesis 6.4). A follower asks its leader for a read index
   (`ReadIndexReq`/`ReadIndexResp`). The leader confirms its leadership with a heartbeat round
   and answers with its commit index. The follower serves the read once it has applied up to
   that index. Reads that can no longer complete (leadership changed, or no answer within two
   election timeouts) are reported in `Ready::failed_reads`, and the replica fails them with a
   leader hint.
3. **Raft flow control.** At most 4 entry-bearing appends are in flight per follower, each
   limited to 4 MB of entries. Each such append gets a fresh sequence number, and a response
   settles exactly the append it answers. Heartbeat responses settle nothing. Appends
   unanswered for six election timeouts count as lost and are re-sent from `next`. A rejection
   resets the window at once. Each TCP peer queue is capped at 256 MB, and messages beyond it
   are dropped, since Raft and file shipping retry.
4. **Write ordering.** Log truncation and append, and their sync, come before the hard state
   write. A crash between the two then leaves a commit index that is too low (harmless), never
   one that covers a stale entry.
5. **Write backpressure and build slots.** Proposals are held back while the memtable is at
   twice its threshold. They are retried on every tick, and fail over with a hint if leadership
   is lost. Flush and compaction builds share a per-node slot pool (`--compaction-slots`).

## Evidence

- The simulation campaign passed 60,000 seeds with zero violations after these changes. Seed
  2227 found the write-ordering bug.
- The Raft harness passes with in-order delivery (500 seeds). New tests cover resend
  amplification, follower reads, failed reads, and held-back proposals under leadership loss.
- 50M rows ingested on three GCP VMs, with every node at about 34 GB and no queue growth.

## Consequences

- Linearizable reads no longer need forwarding when the entry node hosts the shard.
- A slow follower gets at most 16 MB of entries per shard in flight, instead of an unbounded
  stream.
- The static placement cannot be rebalanced without a restart. Dynamic membership is the
  follow-up.
