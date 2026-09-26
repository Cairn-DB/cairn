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

- Leader-built segments (2026-09-24, owner: "write the ADR and run steps 1 and 2"; the owner
  also intends to take Cairn to production):
  - ADR 0016 steps 1-2 DONE (commit 3d928e2): FlushBegin/FlushCommit through the log, immutable
    freezes published in order, followers fetch and verify the leader's file, local-build
    fallback (fetch stall, missing file, hash mismatch, no commit), `--no-ship-segments`,
    `ReplicaStatus::flushes`. Steps 3-6 (parallel leader build, compaction through the log,
    leader balancing, format versioning + mTLS) NOT done.
  - FINDING: the chaos campaign had never flushed (12 keys x 176 B never filled a 3 KB
    memtable). Every campaign before 2026-09-24 (incl. the 60k seeds of ADR 0015) covered Raft,
    memtable and reads only, not flushes, compaction or snapshot installs. Chaos memtable is now
    800 B; the campaign line prints segments built/fetched so this cannot recur silently.
  - With flushing on, ~10% of seeds failed. Seven snapshot-path bugs (pre-existing) and one
    ADR 0016 bug fixed; ADR 0017 lists them with the seeds. Raft changed twice: vote barrier
    (a replica that dropped acknowledged entries does not vote until it has them again) and
    pre-vote responses carrying the responder's term.
  - Evidence: bench-results/phase4-adr0016-campaign.md (seeds 0..60000, zero violations,
    ~19 segments built and ~16 fetched per run); 89 workspace tests.
  - A/B, 2M BigANN, local 3-node cluster (bench-results/phase4-adr0016-bigann2m-{ship,noship}.md):
    total CPU ~690 s vs ~1645 s (stable over 4 runs each), ingest 16.7k vs 13.7k docs/s mean
    (+22%; the first pair suggested +46%), recall and query p99 unchanged.
  - Takedown re-measured (3 alternating runs x 500 takedowns, bench-results/
    phase4-adr0016-takedown-rerun.md): the first 177 vs 102 ms p99 gap does not reproduce
    (118-129 vs 104-143 ms). Possible small median shift (83/88 ms in 2 of 3 shipping runs vs
    70 ms) not established; would need per-node timings.
  - On-disk/wire changes: RAFT file +2 fields (old files decode), new SNAPSHOT file, compaction
    ids carry the node id, FileChunk and PreVoteResp gained a field. Old shard dirs must be
    reloaded (flush ids are log indexes now).

- Step 6 of ADR 0016 (2026-09-24, owner: "vas-y pour l'etape 6"): ADR 0018.
  - Hello v2 with protocol version (2; the old 4-byte hello is refused, so upgrading needs one
    full restart) and the newest segment format a node reads; nodes write the negotiated format.
  - mTLS (rustls + ring; certificates for node-<id>.cairn; `--tls-*` server flags,
    `--tls-anonymous-clients`, bench `--tls-*`, `CAIRN_TLS_DIR` in cluster.sh,
    tools/scripts/gen-certs.sh with openssl).
  - Evidence: 9 transport tests (identity forgery with control, foreign CA, plaintext and
    certificate-less clients, versions, negotiation, 3 MB frames), format contract test,
    three-process cluster test over mTLS, manual check with openssl certificates, campaign
    20k seeds zero violations, 99 workspace tests.
  - TLS cost (bench-results/phase4-adr0018-tls.md, 2 alternating runs each, 2M rows): +22%
    total CPU; ingest, recall, takedown latency unchanged within noise.
  - Limits (ADR 0018): no revocation (CRL) or hot rotation, no authorization, no connection
    limits, no feature gating by cluster protocol version yet.

- Step 3 of ADR 0016 (2026-09-24, owner: "vas-y pour l'etape 3"): ADR 0019, parallel
  deterministic builds (batched HNSW and Vamana on a `Parallel` trait; ThreadParallel in the
  runtime; `--build-threads`, default all hardware threads).
  - SIFT1M: HNSW 185.5 s -> 35.6 s on 16 threads (5.2x), same recall, graphs byte-identical on
    1/4/16 threads; Vamana 1 pass 323.5 s -> 86.1 s (3.8x), same recall.
  - Cluster (2M, one host): builds settle < 50 s (was up to 117 s); ingest no clear change;
    +60% CPU-seconds (SMT, 3 nodes on 16 threads). Query latency during builds NOT measured.
  - FINDING: the bench's settle test (30 s without segment change) can pass during a long
    compaction; one single-thread run measured queries against a compacting node (1.2 s p99).
  - Evidence: bench-results/phase4-adr0019-builds.md; campaign 20k zero violations; 102 tests.

- Step 5 of ADR 0016 (2026-09-24, owner: "vas-y pour l'etape 5"): ADR 0020, leader
  balancing (Raft leadership transfer with TimeoutNow; preferred leader = first host in the
  placement; `--no-leader-balancing`; protocol version 3, version 2 still accepted).
  - The campaign with balancing on found 4 problems, all fixed (ADR 0020): vote-barrier
    deadlock (barrier now votes by remembered position), snapshot term persisted after
    install, manifest index and term persisted separately (SAFETY: a crash could let a
    replica vote with an older term and elect a leader missing committed entries; the
    manifest now records applied_term), compaction snapshot-term fallback.
  - Evidence: campaign seeds 0..60000 zero violations with balancing on; Raft chaos harness
    with random transfers; 105 workspace tests; bench-results/phase4-adr0020-balance.md
    (2/1/1 split reached in both runs; takedown p99 higher with balancing, 134-155 vs
    116-117 ms, not established, OPEN).

- Step 4 of ADR 0016 (2026-09-24/25, owner: "vas-y pour l'etape 4", then "trouve la cause
  de la regression CPU"): ADR 0021, compaction through the log (leader decides and builds,
  `CompactCommit`, everyone installs on commit, fetch from the builder, pending compactions in
  the manifest, log-defined segment list, retired files kept 30 s). Protocol 4 (3 refused).
  - First A/B: +45..105% CPU vs local compaction. ROOT CAUSE: segment I/O on the replica actor
    stalled heartbeats -> leadership churn (37 handovers/run) -> uncommitted merges lost and
    rebuilt by the next leader (31 builds for 20 proposals, 6.2M vs 4.8M rows merged). Fixed:
    reads/writes in their own task, lost merges re-proposed, fetch from the builder.
    Final A/B (3 runs each): CPU -40% (2,923 vs 4,903 s), settle 128 vs 224 s, ingest +13%.
  - A segment-list check added to chaos found: a RAFT SAFETY BUG (pre-existing, commit
    34601fe): a snapshot response reported uncommitted entries of another term as matched, so
    a leader committed without a majority. Also fixed: replay against a later list
    (`compacted_through`), late local build overwriting an installed file (`.built` side name),
    duplicate merges after a leadership change, compaction id reuse after snapshot install,
    snapshot files fetched from the wrong node, fallback retry loop.
  - Evidence: campaign 0..60000 zero violations (4.70M segments built, 5.04M fetched); 108
    workspace tests; bench-results/phase4-adr0021-compaction.md.
  - Chaos test settle window 5 s -> 15 s: a replica that comes back late fetches the merges it
    missed one at a time (was instant in simulated time). Pipelined fetches: follow-up.

- Pipelined segment fetches (2026-09-25, owner: "vas-y pour les fetchs pipelines"): up to 4
  files at once, 8 chunks of 256 KiB in flight per file, written at their offset, lost chunks
  re-asked after an election timeout. Chaos settle window back to 5 s; campaign 0..60000 zero
  violations (3.04M built, 5.58M fetched: fewer fallback builds); 108 tests.
  - Local catch-up (bench-results/phase4-adr0021-catchup.md): no measurable gain on loopback
    (7.0-8.6 s vs 7.2-9.5 s); dominated by log replay and by rebuilding flushes that were
    already merged and purged on the source. Gain expected with network latency; NOT measured
    (no tc/netem without root).
- Merges installed over unbuilt flushes (2026-09-25, owner: "vas-y pour eviter les rebuilds
  des flushs deja merges"): a lagging replica whose oldest pending flushes are consumed by a
  committed merge (possibly a chain of merges) installs that merge directly once its file is
  fetched. The flushes are published without files, and nothing is built or fetched for them
  (Store::subsumption_candidates / install_subsumed, replica subsume_flushes). No wire or log
  change. The status counter was not added: status is a wire type, so it would need a
  protocol bump.
  - Evidence: store test; 109 tests; campaign 0..60000 zero violations (3.02M built, 5.56M
    fetched); catch-up 3.8-3.9 s vs 4.7-8.9 s, node 3 builds 0 vs 1-3
    (bench-results/phase4-adr0021-catchup.md).
  - Campaign tool note: `campaign.sh 20000 14` skips seeds 19992..19999 (integer split); ran
    them separately, OK.
- Packaging (2026-09-25, owner: open source, community around deletion guarantees, Docker
  image first): ADR 0022. Image
  (docker/Dockerfile, 122 MB, non-root, env configuration), compose for 1 and 3 nodes.
  Verified with podman: single node, 3-node cluster bench (100k SIFT, takedowns), kill and
  restart converges in 1.1 s. Podman lesson: compose needs `podman system service` on a short
  socket path (DOCKER_HOST); /run/user/1000/podman does not exist here, and the scratchpad
  path is too long for a unix socket.
  - Owner priorities: (1) 50M latency on real machines (GCP ready; needs the owner's explicit
    go for billing), (2) product: familiar UI, HTTP/JSON API, Python client, publication on
    GitHub/GHCR (checklist in ADR 0022).
- 2026-09-25 (owner: go for the HTTP API; history rewrite, organization and contributor
  docs approved; Hetzner approved):
  - HTTP/JSON API, ADR 0023: axum in each node (`--http-listen`, port 7200 in Docker), with a
    pooled binary client inside, a consistency token for read-your-writes, JSON filters,
    `docs/api/http.md` and `openapi.yaml`. Found and fixed: filter-only search returned nothing
    through the cluster. 114 tests; campaign 3,000 seeds OK.
  - LICENSE, CONTRIBUTING (DCO), SECURITY, CODE_OF_CONDUCT (contact address TO SET).
    Sensitive names removed from the files.
  - History rewrite (e-mail to 193751724+FCHEHIDI@users.noreply.github.com, names removed
    from old commits) and `git config user.email`: BLOCKED by the tool's permission
    classifier (destructive git / identity change). The owner must run them; backup bundle
    in data/backup/.
  - Hetzner provisioning refused again: "dedicated core limit exceeded". Nothing created.
    The owner is checking the quota, or an alternative provider.
  - Podman lesson: `localhost` -> ::1 is reset by rootless port forwarding; use 127.0.0.1.
- 50M on GCP again (2026-09-25, owner: "feu vert pour gcp"; Hetzner still refused, dedicated
  core limit). bench-results/phase4-gcp-bigann50m-2026-09-25.md. Fleet deleted and checked
  (5.96 h; 11.71 USD computed from audit-log durations and catalog prices; invoice not
  readable from the CLI, no billing export).
  - Idle, about 9 seg/shard: unfiltered p99 65/68 ms, 1% filter p99 93 ms stale, **118 ms
    linearizable (target 100 ms MISSED)**, recall 0.985/0.991, takedown p99 107 ms.
    Ingest 6,839 docs/s on average, 25-33k outside stalls.
  - Fixed and committed from what the run exposed:
    - 3e9bf12: build threads default to hardware threads / (2 x slots);
    - e97f01e: a slow leader is not a failed one (20 min commit wait, new
      `SimConfig::offload_delay` test);
    - e5a466f: merges never take the last build slot (stalls; not measured yet);
    - 9349678: committed merge files survive a restart.
  - Open: leader balancing blocked under continuous ingest; a merge of a merge purged before
    a lagging replica fetches it; the settle test passes during long merges; the filtered
    linearizable p99; query latency collapses when builds saturate the CPU.
  - Lesson: remote `pkill -f` / `pgrep -f` match their own `bash -c`; kill by pid from `ps`
    and check that exactly one process remains.
- Filtered latency (2026-09-26, owner: "vas-y pour la latence filtree"): ADR 0025.
  - The scan threshold was ruled out by the SIFT1M sweep: at 1% the graph is worse.
  - A local reproduction (2 shards of 6.25M, 9 seg/shard) showed the cause: searches ran
    in the replica actor, one at a time per shard.
  - Now: snapshot on the actor (`prepare_legs`), search on a helper thread (`LegsJob::run`).
  - A/B, 8 clients: filtered linearizable p99 34-40 -> 10 ms, 323-326 -> 1,154-1,199 QPS.
    One client is 0.5-0.9 ms slower.
  - Search pool (`Runtime::offload_search`, one permanent thread per hardware thread):
    bounds search threads. It did NOT recover the single-client overhead (3.7-4.0 vs
    3.2 ms). The cause is not identified (suspects: the completion hop, cache locality).
  - NOT measured on GCP yet.
- Index loading off the actor (2026-09-26, owner: "vas-y pour le chargement des index hors de
  l'acteur"): ADR 0026. Done (prepared before publication, gated publish and install), but
  it did NOT fix the ingest stalls (A/B identical).
  - New per-step instrumentation found the real blocker: synchronous disk syncs on the actor
    (Raft log 0.3-1.7 s per single-entry sync, hard state, manifest at publication) behind
    build writes.
  - Chunked segment writes with periodic syncs did not help (reverted).
  - Next lever: asynchronous Raft persistence (needs an ADR).
  - Mistake: `git checkout -- segment.rs` to revert the chunked writes also reverted the
    uncommitted MappedSegment code; it was reapplied from the session. Revert edits
    precisely, not whole files, while work is uncommitted.
- GCP 50M run 6 (2026-09-26, owner: "go gcp"): bench-results/phase4-gcp-bigann50m-2026-09-26.md.
  - **Filtered linearizable p99 40 ms: target MET** (was 118 ms), with about 12 seg/shard.
    Unfiltered p99 34/33 ms, throughput doubled, takedown p99 108 ms.
  - Ingest not improved: 6,493 docs/s, stalls caused by synchronous syncs on the actor
    (instrumented locally).
  - Cost 27.46 USD, of which about 21 USD was a deploy blocked for 10.7 h on stale SSH host
    keys (scripts fixed in 4833326). Fleet deleted and checked.
- Asynchronous Raft persistence (2026-09-26, owner: plan approved, "go"): ADR 0027.
  - Raft: `handed` mark, split messages; bug found and fixed (commit index at restart).
  - Replica: one background persistence task, messages held until durable.
  - Simulator: slow syncs.
  - Pre-existing snapshot-install bug fixed (intent log).
  - Campaign 0..60000 zero violations with slow syncs.
  - Local A/B: Raft syncs blocking the actor 94-157 -> 0, ingest +12..55%.
  - Open: publication syncs on the actor (next lever); about 0.1% of the 150 ms-sync seeds
    make no progress under faults (1% before). GCP measurement pending (owner's go).

## Next step
Proposed (not scheduled before the public release): ADR 0024, Kafka ingestion with
end-to-end read-your-takedown (source offsets in the log, per-shard watermarks, Kafka-offset
tokens) and `forget-watch`, a deletion propagation monitor (verified vs observed sinks).
ADR 0016 is complete (steps 1-6, 2026-09-25), with pipelined fetches. Remaining toward
production: measure ingest with e5a466f + 9349678 on a real cluster; reach and measure 3
segments per shard with this code; filtered linearizable p99: the cause (searches in the
replica actor) is fixed by ADR 0025 and measured locally, not yet on GCP (the scan threshold
was ruled out) is MET on GCP (40 ms); asynchronous Raft persistence for the ingest stalls;
the single-client overhead of off-actor searches (about 0.5 ms, cause
unknown); leader balancing under continuous ingest; flush publication and compaction
install still read section headers on the actor; takedown p99 with balancing (open);
from ADR 0018: CRL / certificate rotation, authorization, connection limits; from ADR 0019:
query latency during parallel builds, the bench settle test. Other levers: scan threshold for
large segments (filtered linearizable p99 missed by 8% at 50M), dynamic membership.

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
- Vote barrier liveness (ADR 0017): a replica that dropped a stale snapshot cannot vote until
  it catches up; if another replica of the shard is down at the same time, the shard has no
  leader until it returns. Safe, but a double fault stalls the shard.
- Security limits of ADR 0018: no certificate revocation or hot rotation, no authorization
  (any client certificate may write and take down), no connection limits.

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
