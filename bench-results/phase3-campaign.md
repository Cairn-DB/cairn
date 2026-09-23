# Phase 3 simulation campaign

- Date: 2026-09-24. Code: the durability-order commit (run on its working tree before committing); 40,000 further seeds (20000..60000) also passed.
- Scenario: crates/cairn-query/tests/chaos.rs (3 nodes, 3 clients, 12 keys, 40 fault rounds: partitions, 3% drops, crashes and restarts, flushes every ~3 KB, snapshots).
- Seeds: 0..20000 in 8 processes; wall time 70 s.
- Checks per run: every read against the per-key model with real-time bounds, read-your-takedown on Linearizable and ReadYourWrites reads, replica convergence (applied index and documents), determinism digest.

**Result: zero violations.**

```
CAMPAIGN OK seeds=0..2500 runs=2500 reads_checked=1073152
CAMPAIGN OK seeds=2500..5000 runs=2500 reads_checked=1064748
CAMPAIGN OK seeds=5000..7500 runs=2500 reads_checked=1062282
CAMPAIGN OK seeds=7500..10000 runs=2500 reads_checked=1059753
CAMPAIGN OK seeds=10000..12500 runs=2500 reads_checked=1064914
CAMPAIGN OK seeds=12500..15000 runs=2500 reads_checked=1061530
CAMPAIGN OK seeds=15000..17500 runs=2500 reads_checked=1054581
CAMPAIGN OK seeds=17500..20000 runs=2500 reads_checked=1068214
```
