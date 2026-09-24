# Simulation campaign

- Date: 2026-09-24. Commit: 3d928e2 (run on the uncommitted tree that became this commit; no code changed in between).
- Scenario: crates/cairn-query/tests/chaos.rs (3 nodes, 3 clients, 12 keys, 40 fault rounds: partitions, 3% drops, crashes and restarts; 800-byte memtables, so flushes (ADR 0016 shipping), compactions and snapshot installs happen in every run; segments built and fetched are counted per process).
- Seeds: 0..20000 in 14 processes; wall time 134 s.
- Checks per run: every read against the per-key model with real-time bounds, read-your-takedown on Linearizable and ReadYourWrites reads, replica convergence (applied index and documents), determinism digest.

**Result: zero violations.**

```
CAMPAIGN OK seeds=0..1428 runs=1428 reads_checked=532145 segments_built=27579 segments_fetched=23424
CAMPAIGN OK seeds=14280..15708 runs=1428 reads_checked=508646 segments_built=25831 segments_fetched=21851
CAMPAIGN OK seeds=15708..17136 runs=1428 reads_checked=521899 segments_built=27202 segments_fetched=23433
CAMPAIGN OK seeds=17136..18564 runs=1428 reads_checked=507493 segments_built=26917 segments_fetched=21775
CAMPAIGN OK seeds=18564..19992 runs=1428 reads_checked=525285 segments_built=28347 segments_fetched=24165
CAMPAIGN OK seeds=1428..2856 runs=1428 reads_checked=516268 segments_built=26509 segments_fetched=22743
CAMPAIGN OK seeds=2856..4284 runs=1428 reads_checked=522815 segments_built=27022 segments_fetched=21682
CAMPAIGN OK seeds=4284..5712 runs=1428 reads_checked=524236 segments_built=27134 segments_fetched=22145
CAMPAIGN OK seeds=5712..7140 runs=1428 reads_checked=519740 segments_built=27310 segments_fetched=23151
CAMPAIGN OK seeds=7140..8568 runs=1428 reads_checked=523024 segments_built=26962 segments_fetched=23566
CAMPAIGN OK seeds=8568..9996 runs=1428 reads_checked=521503 segments_built=27015 segments_fetched=23132
CAMPAIGN OK seeds=9996..11424 runs=1428 reads_checked=525939 segments_built=28488 segments_fetched=24093
CAMPAIGN OK seeds=11424..12852 runs=1428 reads_checked=519668 segments_built=28530 segments_fetched=23637
CAMPAIGN OK seeds=12852..14280 runs=1428 reads_checked=525591 segments_built=27159 segments_fetched=23752
```

## Extra seeds 20000..60000 (12 processes, same binary)

**Result: zero violations.**

```
CAMPAIGN OK seeds=20000..23334 runs=3334 reads_checked=1211865 segments_built=63541 segments_fetched=52394
CAMPAIGN OK seeds=23334..26668 runs=3334 reads_checked=1209800 segments_built=63852 segments_fetched=53546
CAMPAIGN OK seeds=26668..30002 runs=3334 reads_checked=1202801 segments_built=64309 segments_fetched=53539
CAMPAIGN OK seeds=30002..33336 runs=3334 reads_checked=1214559 segments_built=64806 segments_fetched=53181
CAMPAIGN OK seeds=33336..36670 runs=3334 reads_checked=1208545 segments_built=64646 segments_fetched=53480
CAMPAIGN OK seeds=36670..40004 runs=3334 reads_checked=1206626 segments_built=61907 segments_fetched=52989
CAMPAIGN OK seeds=40004..43338 runs=3334 reads_checked=1217840 segments_built=64446 segments_fetched=53068
CAMPAIGN OK seeds=43338..46672 runs=3334 reads_checked=1202697 segments_built=63313 segments_fetched=52165
CAMPAIGN OK seeds=46672..50006 runs=3334 reads_checked=1221616 segments_built=65250 segments_fetched=55149
CAMPAIGN OK seeds=50006..53340 runs=3334 reads_checked=1212801 segments_built=63582 segments_fetched=53478
CAMPAIGN OK seeds=53340..56674 runs=3334 reads_checked=1212759 segments_built=63724 segments_fetched=53633
CAMPAIGN OK seeds=56674..60000 runs=3326 reads_checked=1201883 segments_built=63698 segments_fetched=52960
```
