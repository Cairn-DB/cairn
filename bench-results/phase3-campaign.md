# Phase 3 simulation campaign

- Date: 2026-09-23. Code: commit dfafc80 (run on its working tree before committing).
- Scenario: crates/cairn-query/tests/chaos.rs (3 nodes, 3 clients, 12 keys, 40 fault rounds: partitions, 3% drops, crashes and restarts, flushes every ~3 KB, snapshots).
- Seeds: 0..20000 in 8 processes; wall time 53 s.
- Checks per run: every read against the per-key model with real-time bounds, read-your-takedown on Linearizable and ReadYourWrites reads, replica convergence (applied index and documents), determinism digest.

**Result: zero violations.**

```
CAMPAIGN OK seeds=0..2500 runs=2500 reads_checked=1140976
CAMPAIGN OK seeds=2500..5000 runs=2500 reads_checked=1132369
CAMPAIGN OK seeds=5000..7500 runs=2500 reads_checked=1136631
CAMPAIGN OK seeds=7500..10000 runs=2500 reads_checked=1137931
CAMPAIGN OK seeds=10000..12500 runs=2500 reads_checked=1127068
CAMPAIGN OK seeds=12500..15000 runs=2500 reads_checked=1134448
CAMPAIGN OK seeds=15000..17500 runs=2500 reads_checked=1122506
CAMPAIGN OK seeds=17500..20000 runs=2500 reads_checked=1134759
```
