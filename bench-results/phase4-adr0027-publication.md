# Publication syncs off the replica actor (ADR 0027, follow-up)

- Date: 2026-09-26. Driver: `data/run-pub-ab.sh` (same setup as `phase4-adr0027-async-persistence.md`: local 3-node cluster, 4 shards, 6M BigANN rows, merges to 3M rows). Alternating, 2 runs each.
- before: fdc329c; after: this change (no Raft-state store on the actor after a publication; segment renames without a directory sync, which runs in the index preparation off the actor).

```
before-r1: ingested rows 0..6000000 in 487s (12314 docs/s) | slow steps 64 | slow syncs on actor 0
after-r1: ingested rows 0..6000000 in 500s (12007 docs/s) | slow steps 42 | slow syncs on actor 0
before-r2: ingested rows 0..6000000 in 557s (10782 docs/s) | slow steps 56 | slow syncs on actor 0
after-r2: ingested rows 0..6000000 in 477s (12575 docs/s) | slow steps 41 | slow syncs on actor 0
ALL_DONE

before-r1  steps>=500ms  64  total   63.4 s  max  5.13 s  >=1s 19
after-r1   steps>=500ms  42  total   36.9 s  max  2.41 s  >=1s 13
before-r2  steps>=500ms  56  total   46.5 s  max  2.06 s  >=1s 14
after-r2   steps>=500ms  41  total   34.0 s  max  1.76 s  >=1s 8
```
