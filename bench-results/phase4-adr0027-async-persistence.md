# Raft persistence: synchronous vs asynchronous (ADR 0027)

- Date: 2026-09-26. Host: development machine. Driver: `data/run-async-ab.sh`: local 3-node cluster, 4 shards, 3 cores per node, 64 MB memtables, `--sq8-only`, merges to 3M rows, 6M BigANN rows. Alternating, 2 runs each.
- sync: ff173ff (with the per-step instrumentation); async: 52b9b7d.
- "slow steps": replica steps of at least 500 ms; "slow syncs on actor": Raft log or hard-state syncs of at least 250 ms on the actor.

```
sync-r1: ingested rows 0..6000000 in 1212s (4951 docs/s) | slow steps 165 | slow syncs on actor 94
async-r1: ingested rows 0..6000000 in 1079s (5560 docs/s) | slow steps 121 | slow syncs on actor 0
sync-r2: ingested rows 0..6000000 in 1323s (4535 docs/s) | slow steps 219 | slow syncs on actor 157
async-r2: ingested rows 0..6000000 in 855s (7020 docs/s) | slow steps 98 | slow syncs on actor 0
ALL_DONE
```

Slow steps by event, run 1:

```
sync-r1
     84 event="indexes_ready"
     57 event="net"
     12 event="flush_written"
      7 event="propose"
      5 event="compact_written"
async-r1
     80 event="indexes_ready"
     19 event="net"
     14 event="flush_written"
      5 event="propose"
      1 event="tick"
      1 event="persisted"
      1 event="compact_written"
```
