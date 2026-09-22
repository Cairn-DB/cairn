# Phase 3 simulation campaign

- Date: 2026-09-22. Commit: 5c1cb77.
- Scenario: crates/cairn-query/tests/chaos.rs (3 nodes, 3 clients, 12 keys, 40 fault rounds: partitions, 3% drops, crashes and restarts, flushes every ~3 KB, snapshots).
- Seeds: 0..20000 in 8 processes; wall time 52 s.
- Checks per run: every read against the per-key model with real-time bounds, read-your-takedown on Linearizable and ReadYourWrites reads, replica convergence (applied index and documents), determinism digest.

**Result: zero violations.**

```
CAMPAIGN OK seeds=0..2500 runs=2500 reads_checked=1041635
CAMPAIGN OK seeds=2500..5000 runs=2500 reads_checked=1027569
CAMPAIGN OK seeds=5000..7500 runs=2500 reads_checked=1044438
CAMPAIGN OK seeds=7500..10000 runs=2500 reads_checked=1044196
CAMPAIGN OK seeds=10000..12500 runs=2500 reads_checked=1022052
CAMPAIGN OK seeds=12500..15000 runs=2500 reads_checked=1036175
CAMPAIGN OK seeds=15000..17500 runs=2500 reads_checked=1014244
CAMPAIGN OK seeds=17500..20000 runs=2500 reads_checked=1036506
```
