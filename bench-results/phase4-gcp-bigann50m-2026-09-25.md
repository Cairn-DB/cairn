# 50M on a real 3-machine cluster, after ADR 0016-0023 (GCP, 2026-09-25)

- Owner's go: "feu vert pour gcp". Fleet: 3 × n2-highmem-8 (8 vCPU, 64 GB, pd-ssd) and an
  e2-standard-4 client, europe-west1-b, private network; scripts in `tools/scripts/gcp/`.
  Up from 17:10 to 23:07 local time (about 6 h), then deleted. Checked afterwards: no
  instances, disks, firewall rules or network left.
- Cost: estimated from list prices at about 11 USD. Not checked on the billing console.
- Data: the first 50M rows of BigANN (128-d, u8) with bench-gen attributes; brute-force
  ground truth; 1,000 queries per kind from 8 threads, k = 10, ef = 128; 200 takedowns.
- Compared with the previous GCP run (2026-09-24, commit before ADR 0016, no compaction during
  ingest): `phase4-gcp-bigann50m.md` and `phase4-gcp-bigann50m-compacted.md`.

## Results

Idle cluster, about 9 segments per shard (6 to 19), no builds running
(`phase4-gcp-bigann50m-run5-idle.md`):

| metric | this run (≈9 seg/shard) | previous, 11-19 seg/shard | previous, 3 seg/shard | target |
|---|---|---|---|---|
| unfiltered recall@10 | 0.985 | 0.985 | 0.983 | |
| unfiltered p99, stale / linearizable | **65 / 68 ms** | 118 / 154 ms | 36 / 48 ms | < 100 ms |
| 1% filter recall@10 | 0.991 | 0.990 | 0.991 | |
| 1% filter p99, stale / linearizable | **93 / 118 ms** | 163 / 157 ms | 82 / 108 ms | < 100 ms |
| takedown visible on all 3 machines, p99 | 107 ms | 107 ms | 107 ms | < 1 s |

- Met: recall, the unfiltered p99 at both consistency levels, the filtered stale p99, and
  takedowns.
- **Missed: filtered linearizable p99, 118 ms against 100 ms** (18% over). It was already
  missed at 3 segments per shard (108 ms). At a comparable segment count, every latency is
  lower than in the previous run. Full compaction was not reached (see below), so the
  3-segment state was not measured again.
- Ingest: **50M rows in 7,311 s, 6,839 docs/s on average** (previous: 8,738 docs/s, with no
  merges during ingest). Outside the stalls described below it ran at 25k to 33k docs/s.
  The 9 stalls add up to about 5,500 s, 75% of the ingest time.
- Under heavy build load (merges running on every node, 8 build threads per 8-vCPU node)
  the same queries had p99 values of 0.5 to 12.6 s (`phase4-gcp-bigann50m-run5-under-compaction.md`).
  Builds that saturate the machine make queries unusable. ADR 0019 had left this
  unmeasured.

## What the run found (five problems, four fixed and committed)

Five attempts were needed. Each problem below comes from the node logs and status output
of these runs.

1. **Build threads oversubscribed by default** (run 1). With every hardware thread per build,
   4 slots meant up to 32 build threads per 8-vCPU VM. Raft actors starved and one node
   stopped answering status requests. Leadership piled up on one node (8 of 8 shards), and
   ingest fell from about 40k to 4.7k docs/s by 5M rows.
   **Fixed** (3e9bf12): builds default to hardware threads / (2 × slots).
2. **Followers treated a slow leader as a failed one** (runs 3 and 4).
   - A follower waited 100 s for a `FlushCommit`, then built the segment itself. One
     256 MB build on 2 threads takes about 2 minutes, so every follower rebuilt every
     segment. Fetched was 0 on all nodes, with 40 to 58 local rebuilds per run.
   - A first fix (fa5693c, restart the wait on leader progress) was not enough, because a
     single build already outlasted the wait.
   - **Fixed** (e97f01e): the wait is a 20-minute safety net, since a failed leader is
     already replaced by election.
   - The simulator gained `offload_delay`, and a test with 30 s builds reproduces the
     problem.
   - Run 5 (with the fix): no follower rebuilt a flush for lack of a commit, and followers
     fetched.
3. **Ingest stalls while merges hold the build slots** (every run, 5 to 20 minutes each).
   Flushes and merges share a node's slots, and a merge of about 3M rows holds one for
   minutes. With 2 slots, two merges held off every flush, memtables filled, and
   backpressure stopped writes.
   **Fixed in code, not measured** (e5a466f): merges never take the last free slot.
4. **A restart deleted committed merge files** (after the post-ingest restarts). Startup
   cleanup removed segment files the manifest did not list as installed, including the
   files of committed merges that were not installed yet. After a restart, all three
   replicas rebuilt each such merge (10+ minutes each). Evidence: "segment fetch failed (no
   data)" right after each restart, for merges whose builder was the node that had just
   restarted.
   **Fixed** (9349678): committed merge files are kept and reused.
5. **Leader balancing cannot act during continuous ingest** (not fixed). ADR 0020 forbids a
   handover while the leader builds or holds an unpublished freeze, which is always true
   under sustained writes. Leadership stayed wherever elections left it (0 to 6 shards per
   node). That node then did most of the builds.

Also seen:
- Fetch failures ("no data") for segments that the source had probably merged away and
  purged after the 30 s grace. Replicas rebuilt them. This is the same pattern as the
  flushes fixed by subsumption, one level up (a merge of a merge). Not fixed.
- The benchmark's settle test (120 s without a segment change) passed while merges were
  still running, as ADR 0019 had noted. The idle measurement therefore disabled new merges
  and waited for nothing to be pending.
- A tool error, not a Cairn bug: a remote `pkill -f` matched its own shell. The run-1
  benchmark survived and wrote into the next cluster, so that run was discarded.

## Not measured

- Ingest with fixes 3 and 4 deployed.
- Latency at 3 segments per shard with this code. Merges did not reach it within the time
  budget: every restart deleted merge files (fixed since), and merges ran at most 4 at a time.
- The filtered-latency lever (a scan threshold for large segments) remains untried.
