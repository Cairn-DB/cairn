# Segment fetches off the replica actor (ADR 0027, follow-up)

- Date: 2026-09-26. Same setup as `phase4-adr0027-publication.md` (local 3-node cluster, 6M BigANN rows).
- Two changes: fetch completion (sync, hash check) outside the actor; serving and writing chunks outside the actor. A/B runs: fetch completion alone (`run-fetch-ab.sh`, before 9164580), then both (`run-fetch2-ab.sh`, before 9164580).
- The host was shared with an unrelated `llama-server` process at about 330% CPU (load average 14-26). The second pair of the second A/B ran at half speed on both sides; only the first pair is comparable.

## Fetch completion alone

```
fbefore-r1: ingested rows 0..6000000 in 501s (11974 docs/s) | slow steps 41 | slow syncs on actor 0
fafter-r1: ingested rows 0..6000000 in 476s (12604 docs/s) | slow steps 31 | slow syncs on actor 0
fbefore-r2: ingested rows 0..6000000 in 509s (11778 docs/s) | slow steps 38 | slow syncs on actor 0
fafter-r2: ingested rows 0..6000000 in 631s (9505 docs/s) | slow steps 38 | slow syncs on actor 1
ALL_DONE
```

## Completion, serving and chunk writes

```
gbefore-r1: ingested rows 0..6000000 in 637s (9425 docs/s) | slow steps 66 | slow syncs on actor 0
gafter-r1: ingested rows 0..6000000 in 536s (11194 docs/s) | slow steps 26 | slow syncs on actor 0
gbefore-r2: ingested rows 0..6000000 in 1477s (4062 docs/s) | slow steps 124 | slow syncs on actor 0
gafter-r2: ingested rows 0..6000000 in 1662s (3610 docs/s) | slow steps 108 | slow syncs on actor 1
ALL_DONE
gbefore-r1  steps>=500ms  66  total   53.0 s  max  3.49 s  >=1s 7
        38 event="net"      17 event="indexes_ready"       7 event="compact_written"       4 event="flush_written" 
gafter-r1   steps>=500ms  26  total   20.0 s  max  2.03 s  >=1s 4
        17 event="net"       4 event="indexes_ready"       3 event="compact_written"       1 event="propose" 
gbefore-r2  steps>=500ms 124  total  156.6 s  max  5.89 s  >=1s 58
        53 event="net"      43 event="indexes_ready"      18 event="flush_written"       6 event="compact_written" 
gafter-r2   steps>=500ms 108  total  143.0 s  max  4.27 s  >=1s 57
        38 event="net"      28 event="indexes_ready"      24 event="flush_written"      10 event="propose" 

```

Instrumented run before the second change (3M rows): slow `fetch_file` frames came from the chunk read (13,563 reads, mean 1.4 ms, max 4.2 s); sends never took more than 1 ms.
