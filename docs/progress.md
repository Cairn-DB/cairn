# Progress journal (read after SPEC.md and CLAUDE.md; update at every milestone)

## Mandate
2026-09-22: owner delegated all decisions; no review until a fully functional prototype is
delivered. Decisions: `docs/adr/0012-delegated-decisions.md`. Rules: CLAUDE.md "Autonomous
delivery mode". Milestones: `docs/roadmap.md`. Evidence per phase: `docs/reports/phase-N.md`.

## Environment (verified 2026-09-22)
- AMD Ryzen 7 8845HS: 8 cores / 16 threads, AVX2, AVX-512 (F/BW/VL/VNNI/VPOPCNTDQ), FMA.
- 58 GB RAM, ~800 GB free NVMe under /home, /tmp is a 30 GB tmpfs (do not put datasets there).
- Linux 7.2.5 (Fedora 44), io_uring enabled (`/proc/sys/kernel/io_uring_disabled` = 0).
- Rust stable 1.98.1 (pinned by rust-toolchain.toml), nightly 1.100 with Miri (install started
  2026-09-22), cargo-fuzz (install started 2026-09-22). No `protoc`, no `sudo`.
- Python 3.14 + numpy 2.5 available for dataset conversion and ground-truth checks.
- git identity configured; repo initialised 2026-09-22 on `main`.

## Datasets
- SIFT1M: ftp://ftp.irisa.fr/local/texmex/corpus/sift.tar.gz (168 MB, fvecs/ivecs).
- YFCC-10M filtered (Big-ANN NeurIPS'23): files under
  https://dl.fbaipublicfiles.com/billion-scale-ann-benchmarks/yfcc100M/ — sizes recorded in
  `tools/fetch-datasets.sh` when written (Phase 2).
- MS MARCO passage: decide the exact subset at M2.4.

## State
- Phase 0: DONE 2026-09-22 (commit "Phase 0: ...").
- Phase 1: IN PROGRESS.
  - M1.1 DONE 2026-09-22: cairn-core (ids, time, error, hash aliases, SeededRng, Runtime/Disk/
    Network traits), cairn-runtime (executor with pluggable Reactor, tags, seeded scheduler;
    blocking OS disk; RealRuntime), cairn-sim (Simulation, SimReactor, SimDisk with unsynced-loss
    and torn writes, SimNetwork with delay/drop/partition, Trace digest). 20 tests. The SPSC
    queue + loom item is deferred to M4.2 (no cross-core traffic before real networking).
  - M1.2 DONE 2026-09-22: cairn-storage log (segmented files, crc32 records, recovery truncates
    at first bad record, suffix/prefix truncation), codec, manifest store (tmp+sync+rename).
    Tests: 150-seed sim crash/recovery with torn writes, 400-case damaged-image proptest,
    manifest crash-at-any-point, real-fs roundtrip. 29 tests total.
  - M1.3 DONE 2026-09-22: schema/document/command types (cairn-core), segment container
    (page-aligned sections, xxh3 per section, file hash, crc'd TOC), document columns, memtable,
    deletion sets (.del files via the manifest container), shard Store (log + memtable +
    segments + manifest; replay from manifest.applied_index). 120-seed crash test vs model.
  - M1.4 DONE 2026-09-22: compaction (rewrite stale segments, merge smallest adjacent pair
    when over max_segments), bulk column reads, orphan cleanup. 36 workspace tests.
  - M1.5 DONE 2026-09-22: criterion storage benchmarks on the NVMe, results in
    `bench-results/phase1-storage.md` (fsync 2-4 ms dominates; 14.5k rec/s at batch 64;
    100k-row segment build 120 ms). io_uring/glommio spike deferred to M4.2.
  - M1.6 DONE 2026-09-22: cairn-bench-gen lib + CLI (flags at 50/10/1/0.1%, random or
    cluster-correlated; realistic attrs; takedown schedule).
- Phase 1: DONE 2026-09-22. Report: `docs/reports/phase-1.md`.
- Phase 2: IN PROGRESS.
  - M2.1 DONE 2026-09-22: kernels (scalar/AVX2/AVX-512, f32 + SQ8), proptests vs scalar on all
    levels, `bench-results/phase2-kernels.md`. MSRV 1.89.
  - M2.2 DONE 2026-09-22 (code): bitmap, deterministic incremental HNSW, exact scan, VectorIndex
    (adaptive scan/graph/two-hop, SQ8 + rerank, exact mode, visit cap), segment sections, tests
    vs brute force. 100k smoke sweep: scan wins below ~10%; uncapped filtered graph search is
    catastrophic at 0.1% (22 ms); 1M sweep pending -> `bench-results/phase2-sift1m.md`.
  - M2.3 DONE 2026-09-22: Predicate AST (cairn-core::filter), StructuredIndex (term lists,
    ordered keys, IsNull via nulls), proptest vs document semantics, segment round-trip.
  - M2.2 sweep DONE: `bench-results/phase2-sift1m.md` (scan <5% exact & fast; capped graph
    above; two-hop dropped; defaults updated).
  - M2.4 DONE 2026-09-22: TextIndex (tokenizer, postings, BM25 vs naive reference, section).
  - M2.5 DONE 2026-09-22: SegmentIndexer hook in Store, DefaultIndexer, cairn-query (Query,
    fusion RRF/weighted, ShardEngine with per-segment indexes + lazily rebuilt memtable
    indexes), end-to-end tests vs reference incl. takedown visibility across memtable/segments.
  - M2.6 DONE 2026-09-22: YFCC-10M (recall 0.9994, p99 16.5 ms, 258 QPS/thread) and MS MARCO
    (MRR@10 0.204, corpus with titles) in bench-results/. Report: docs/reports/phase-2.md.
- Phase 2: DONE 2026-09-22.
- Phase 3: IN PROGRESS (built while Phase 2 benchmarks downloaded/ran).
  - M3.2 DONE: cairn-raft pure state machine (pre-vote, replication, ReadIndex, snapshots as
    opaque bytes, in-memory log suffix) + seeded chaos harness (3 and 5 nodes, crashes,
    partitions, drops; every step checks election safety, log matching, leader completeness,
    state-machine safety; convergence after healing). Bugs it caught: followers acked stale
    entries beyond the leader's batch; harness completeness check must skip stale leaders.
  - M3.3 DONE: cairn-query::replica actor (persist -> send -> apply -> advance; flush compacts to
    the manifest snapshot; FetchFile/FileChunk segment shipping; consistency levels), wire
    frames, cluster tests (replication, RYW/linearizable/stale, takedown, leader crash,
    re-election, snapshot catch-up). Bugs: snapshot accepted before files fetched -> log reset
    ordering; store replay must stop at the persisted commit index (uncommitted tail).
  - M3.4 DONE (first version): tests/chaos.rs signature test: 3 clients, partitions, drops,
    crashes/restarts, per-key model with real-time bounds, read-your-takedown rule, convergence,
    determinism (same digest). 12 seeds in CI-speed test.
  - M3.5 DONE: campaign 20k seeds zero violations (bench-results/phase3-campaign.md; rerun
    after the background-build change is in progress). Report: docs/reports/phase-3.md.
- Phase 4: IN PROGRESS.
  - M4.1 DONE: cairn-proto (client Request/Response with Cairn's codec; ADR 0009 revised:
    no protobuf), cairn-client (blocking, follows leader hints, keeps per-shard tokens).
  - M4.2 DONE (deviation): cairn-runtime ThreadReactor + PoolDisk (helper threads, completion
    channel) and TcpNetwork (reader thread per connection, writer thread per peer, framed;
    client connections multiplexed by request id); CrossQueue/cross_oneshot for cross-core
    traffic with a loom model (tests/loom_cross.rs). io_uring reactor NOT done.
  - M4.3 DONE: cairn-server Node: one executor per core, shard s on core s % cores, dispatcher
    routes frames by shard, coordinator fans out per shard (per-leg merge then fusion) and
    forwards sub-requests to shard leaders (Request::Forwarded/ShardLegs).
  - M4.4 DONE: crates/cairn-server/tests/cluster.rs: 3 processes, 4 shards x 2 cores, writes,
    RYW reads, hybrid query, takedown, kill + restart + convergence.
  - Background segment builds (frozen memtable + Runtime::offload) DONE after the first
    cluster runs showed a synchronous HNSW build would stall Raft heartbeats.
  - M4.5 DONE 2026-09-22: bench-results/phase4-cluster-sift1m.md (4.6k docs/s ingest,
    filtered p99 6.5 ms, takedown visible on all nodes p99 41 ms). Report:
    docs/reports/phase-4.md.
- Phase 4: DONE 2026-09-22. PROTOTYPE DELIVERED.

- Scale follow-up (2026-09-23, owner request: 10M then 50M through the cluster):
  - `cairn-bench cluster-scale` (YFCC-10M with official GT; BigANN prefix with brute-force GT),
    `--max-segments` and `--sq8-only` server flags (ADR 0013).
  - First YFCC-10M attempt with defaults: 9 GB/node at 2M rows, stopped (would not fit).
    After ADR 0013 (SQ8-only residency, one less memtable copy): 9.1 GB/node settled at 10M.
  - YFCC-10M through the cluster DONE: bench-results/phase4-cluster-yfcc10m.md. 6.7k docs/s,
    recall@10 0.989, p99 80 ms stale / 117 ms linearizable (target < 100 ms MISSED for
    linearizable), takedown p99 39 ms. Latency is flat across selectivity buckets: fixed cost
    of 8 shards x 16 segments per query.
  - BigANN-20M through the cluster DONE: bench-results/phase4-cluster-bigann20m.md. 7.4k docs/s,
    8.7 GB/node settled (0.43 KB/row/node), 1% filter: recall 0.991, p99 47 ms; unfiltered:
    recall 0.987, p99 120 ms (MISSED, 184 segment searches per query). Takedown p99 43 ms.
  - 50M: does not fit (3 full replicas on one 58 GB host: ~65 GB before transients). First
    50M BigANN rows downloading to data/bigann/ (fetch.sh; the host stalls over IPv6, use -4).
  - Report: docs/reports/phase-4-scale.md.

- Scale follow-up part 2 (2026-09-23, owner: fix the missed 100 ms p99, accept 96 GB host,
  membership/placement and a disk-resident index; Hetzner account available):
  - Latency causes found: all clients on node 1; sequential leader forwards; idle memtables
    scanned by every query; 23 segments/shard. Fixed (commit a967eca): client rotation,
    concurrent forwards, --idle-flush-ms, tiered compaction (--target-segment-rows,
    --compaction-slots per node). Measurement on the 20M data: IN PROGRESS (cluster restarted
    on data/scale-bigann, post-load compaction to ~3 segments/shard, watcher
    data/watch-compaction.sh).
  - Placement DONE: --replication N (shard s on N consecutive nodes), 4-node RF3 process test
    incl. a killed node. Dynamic membership changes (add/remove replica) NOT done yet.
  - Hetzner: scripts + plan (docs/hetzner-plan.md), NOTHING created; needs the owner's go
    (the hcloud project also holds production servers; everything is labelled project=cairn).
  - Disk-resident index DONE (ADR 0014, commit 335182c): Vamana + PQ, Disk::map/prefetcher,
    beam search. 100k SIFT: recall 0.994 @L64, warm 0.32 ms, cold 7.8 ms (beam 4). Build
    3.4k rows/s/thread. 33 B/row RAM, 819 B/row disk.
  - Latency fix MEASURED: BigANN-20M unfiltered p99 120 -> 20 ms stale / 49 ms linearizable;
    YFCC-10M linearizable p99 117 -> 59 ms. Both targets met (commits ad661d7, ea153ca).
  - SIFT1M disk-sweep DONE (bench-results/phase4-diskann-sift1m.md).
  - Follower reads + failed-read reporting + write backpressure DONE (e6250b8); campaign 20k
    seeds zero violations. Linearizable numbers above predate follower reads: re-measure on
    data/scale-bigann and data/scale-yfcc (restart cluster, --skip-ingest).
  - 50M local run with --disk-index, attempts and what each exposed:
    1. stopped at 6M (watchdog): memtable grew without bound while a flush built -> write
       backpressure (e6250b8).
    2. stalled at 5M, 15 GB/node: 4 concurrent Vamana builds per node -> flushes share the
       per-node build slots (dfafc80).
    3. slow (2.6k docs/s) -> --vamana-passes 1, 3 slots (e95423a); then node 2 grew to 32 GB
       at 9M rows: leader re-sent 19 MB appends to a slow follower on every proposal -> Raft
       flow control (4130c92); follower slow because segment loads hashed/validated on the
       actor -> offloaded (85f4145). Campaign zero violations after each Raft change.
    4. RUNNING since 19:13 (data/run-disk50m.sh: 4 shards x 4 cores, 128 MB memtables,
       --compaction-slots 3 --vamana-passes 1, no compaction; watchdog stops on avail < 4 GB
       or any node > 20 GB). Expected ~5k docs/s.
  - Lessons: glibc arenas hold freed merge buffers (MALLOC_ARENA_MAX=2 in cluster.sh);
    `pkill -f <pattern>` kills the calling shell when the pattern is in its own command line
    (use pids); the BigANN CDN stalls over IPv6 (curl -4).

- 50M through a real cluster (GCP, owner approved 2026-09-23 night): Hetzner blocked by the
  account's dedicated-core quota (nothing created there except a free key/network/firewall
  labelled project=cairn); Azure 10 vCPU, AWS 5 vCPU; the GCP project allows 30.
  Fleet: cairn-node1..3 n2-highmem-8 + cairn-bench e2-standard-4, europe-west1-b, labelled
  project=cairn, scripts tools/scripts/gcp/ (teardown.sh deletes only cairn-*). TORN DOWN
  2026-09-24 ~04:50 (verified: no cairn instances, disks, firewall rules or network).
  - GCP exposed: flow-control window drained by heartbeat responses (42 GB queued, OOM kill),
    held-back proposals stuck after leadership loss (client timeouts). Fixed (6320891, 4fb4977).
    Chasing a campaign failure exposed the hard-state-before-entries durability bug (fixed).
  - Results: bench-results/phase4-gcp-bigann50m.md (before compaction: p99 118/154 ms) and
    phase4-gcp-bigann50m-compacted.md (3 segs/shard: unfiltered p99 36/48 ms, 1% filter
    82/108 ms -> filtered linearizable MISSES 100 ms by 8%; recall 0.983/0.991; takedown
    107 ms). Report: docs/reports/phase-4-scale.md part 3.
  - Next levers: scan threshold for large segments (filtered path), segment publication off
    the actor (ingest stalls, elections under load), dynamic membership, Hetzner once the
    owner's dedicated-core quota is raised (free key/network/firewall labelled project=cairn
    still exist there).
  - Decisions: ADR 0015 (placement, follower reads, flow control, write ordering).

## Next step
Scale runs delivered 2026-09-23; owner wants to discuss concepts next. Scale-specific levers:
fewer, larger segments (post-load compaction), per-query fixed cost (filter evaluation per
segment), linearizable leg forwarding, and a disk-resident index for 50M on one host.
Earlier candidate follow-ups, by value: (1) move segment publication and compaction input
reads off the actor (ingest p99); (2) group commit across proposals; (3) io_uring reactor;
(4) BM25 scoring loop (term-at-a-time accumulators or block-max WAND); (5) membership changes;
(6) fuzz targets + Miri on the scalar kernel paths; (7) 10M-row end-to-end ingest run.

## Old next step
M2.1 kernels (compile, test, bench, commit), then M2.2 vector search: HNSW (deterministic build,
level from hash of doc id), exact scan, filter bitmaps, selectivity-adaptive dispatch, SQ8 +
rerank; SIFT1M loader in a new `tools/cairn-bench` crate; selectivity x correlation sweep.

## Previous step (kept for context)
M1.2: `cairn-storage::log` (segmented files named by first index, header with magic/version,
records `[len][crc32][term][index][payload]`, recovery truncates at the first bad record,
suffix truncation for Raft, prefix truncation by whole files), `codec` (bounds-checked
reader/writer, fuzzable), `manifest` (tmp + sync + rename). Tests: sim-driven crash/recovery
property test, byte-level truncation/corruption proptest, real-fs smoke test.

## Design notes that are not in the code
- Engine code is generic over `R: Runtime`; the runtime is cloned into every component.
- Sim: one executor for all nodes; tasks tagged by node id; `Simulation::crash` cancels tasks,
  drops the node's events and inbox, and applies disk crash semantics.
- Disk trait: data ops take effect at completion (after latency); directory ops at issue.

## Open problems
(none yet)

## Log
- 2026-09-22 (end): Phases 3 and 4 closed. Real-cluster bugs: cross-thread wake did not
  unpark the reactor (50 ms hops); client rewrote explicit RYW tokens; synchronous builds in
  the actor. Campaign re-validated after every Raft/sim change.
- 2026-09-22: Phase 1 closed (40 tests). Bench lesson: consumer NVMe fsync jitter makes
  batch-1 vs batch-16 comparisons meaningless; only report group-commit throughput.
- 2026-09-22: M1.2 done. Lesson: edition 2024 reserves `gen`; `truncate_suffix` below the
  first file's start must empty the file.
- 2026-09-22: M1.1 done (executor, blocking runtime, simulator core). Lesson: with a seeded
  scheduler, two tasks' issue order is not their spawn order; per-file ordering is per task.
- 2026-09-22: Phase 0 delivered; mandate changed to autonomous delivery; ADR 0012 written;
  raft-rs removed from cairn-raft; tool installs started.
