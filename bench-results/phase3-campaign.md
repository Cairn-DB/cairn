# Phase 3 simulation campaign

- Date: 2026-09-22. Commit: d5bda7a.
- Scenario: crates/cairn-query/tests/chaos.rs (3 nodes, 3 clients, 12 keys, 40 fault rounds: partitions, 3% drops, crashes and restarts, flushes every ~3 KB, snapshots).
- Seeds: 0..20000 in 8 processes; wall time 51 s.
- Checks per run: every read against the per-key model with real-time bounds, read-your-takedown on Linearizable and ReadYourWrites reads, replica convergence (applied index and documents), determinism digest.

**Result: zero violations.**

```
CAMPAIGN OK seeds=0..2500 runs=2500 reads_checked=1064863
CAMPAIGN OK seeds=2500..5000 runs=2500 reads_checked=1059772
CAMPAIGN OK seeds=5000..7500 runs=2500 reads_checked=1059736
CAMPAIGN OK seeds=7500..10000 runs=2500 reads_checked=1071625
CAMPAIGN OK seeds=10000..12500 runs=2500 reads_checked=1049618
CAMPAIGN OK seeds=12500..15000 runs=2500 reads_checked=1064091
CAMPAIGN OK seeds=15000..17500 runs=2500 reads_checked=1041772
CAMPAIGN OK seeds=17500..20000 runs=2500 reads_checked=1061241
```
