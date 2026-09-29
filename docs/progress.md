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
  - Follow-ups (9164580, 3f4e21d): publication state and directory syncs, then fetch
    verification, chunk serving and chunk writes, all moved off the actor. Local A/B: blocked
    time 53 -> 20 s, slow steps 66 -> 26 (one comparable pair; host shared with an unrelated
    llama-server at ~330% CPU). Remaining slow `net` steps have handle_ms 0 and persist_ms
    0.6-1 s: most likely the Raft log append (page-cache write throttled under dirty
    pressure), not measured separately. Next: time the append, or the GCP run (owner's go).

- GCP 50M run 7 (2026-09-26, owner: "go gcp"): bench-results/phase4-gcp-bigann50m-run7-ingest.md.
  - Ingest 5,542 docs/s (run 6: 6,493): **no gain**. The actor now blocks about 0.4% of the
    time. Stalls are build throughput: 2 slots x 2 threads use at most half of 8 vCPU,
    leaders are unbalanced (the leader builds), and some fetches fall back to local builds.
  - Bug: too many open files (fetch writes unbounded since 3f4e21d), fixed in a7520b9.
  - Query phase dropped: merges kept running after the restart without merges. Fleet
    deleted at 23:31 UTC, about 7.7 USD. Owner's rule: always put a hard deadline on a
    cloud run (a watchdog that tears the fleet down).
  - Next levers: build parallelism (threads per slot now that the actor does not block),
    leader balancing, the `flush_written` handle time (1 to 3 s), and merges that ignore
    `--max-segments` after a restart.

- Build throughput (2026-09-27, owner: "vas-y pour les threads de construction"):
  - 662a8eb: build workers at nice 10, default threads = hardware / slots (was / (2 x slots)).
  - 7b781a0: a shard's flush no longer waits for its own merge (separate node slots). Found
    while measuring: a merge blocked its shard's flushes, the memtable hit the write limit,
    and the whole ingest stopped (every batch spans all shards).
  - Local A/B, 6M rows, nodes pinned to 5 threads (bench-results/phase4-build-priority-and-slots.md):
    median 386 s baseline, 510 s nice only, 431 s slots only, **289 s both (20.8k docs/s)**,
    no stall over 110 s. Only 3 runs of the winner; not yet on GCP.
  - 123 tests; campaign 3,000 seeds zero violations.
  - Open: query latency while builds run (8 end-of-run queries vary 20 ms to 6 s p50 in every
    variant); `live docs` above the row count in two runs; merges that ignore `--max-segments`
    after a restart (needed to query an idle cluster on GCP).
  - Next: the merge pause flag, then a GCP 50M run with a larger pd-ssd (throughput scales
    with size) and a teardown watchdog from the start (owner's go needed).

- Merge pause (2026-09-27, owner: "vas-y pour la pause des fusions"): ADR 0028.
  - `--max-segments` never disabled merges (the tiered policy runs regardless): that is why
    run 7 could not reach an idle cluster.
  - Per-node pause at runtime: `--merges-paused`, `SetMergesPaused` request, `cairn-bench merges
    pause|resume`, HTTP `/v1/admin/merges`. Running and committed merges complete. Status shows
    merges running/pending and the flag. Protocol 5 (full restart to upgrade).
  - The bench settle test now waits until no flush or merge is building or pending.
  - Simulation test with a positive control; 124 tests; campaign 3,000 seeds zero violations.
    Local end-to-end (data/run-pause-e2e.sh): paused during ingest, idle (0/0, CPU < 1%)
    right after ingest, segments unchanged, merges back within 30 s after resume.
  - `live_docs` is approximate by design (memtable + segments minus deletions), which
    explains counts slightly above the row count seen in the A/B.

- GCP 50M run 8 (2026-09-27, owner: "go pour le run gcp"): bench-results/phase4-gcp-bigann50m-run8.md.
  - Node data on local NVMe SSD (392 MB/s; the regional SSD quota of 500 GB also counts
    pd-balanced). Teardown watchdog from the start.
  - Ingest **8,343 docs/s** (run 6: 6,493; run 7: 5,542). Longest stall 12 min (was 27).
    Remaining: duplicate builds (leader moves, 2 s fetch stall fallbacks), actor persist
    steps up to 8 s on a fast disk (not diagnosed; ingest node logs lost to restarts).
  - Queries after ingest MISSED targets (p99 117-151 ms, 80 QPS). Bisection on the same data
    after restarts: no code regression; restarted 57b71af gives **p99 42/44 ms unfiltered,
    42/42 ms filtered, 244/226 QPS** (targets met, 118 segs/node). The ingesting process held
    33.8 GB anon vs 17 GB after restart: something from ingest stays in memory and is likely
    searched. OPEN, reproduce locally next.
  - Lesson: run-cluster.sh overwrites node logs on restart; keep a copy before restarting.
  - Cost about 7 USD. Fleet deleted 16:40 UTC and checked.

- Post-ingest slowdown, reproduced and fixed locally (2026-09-28, owner: "vas-y pour la
  reproduction locale de la fuite mémoire"): ADR 0029.
  - Repro data/run-memrepro.sh (3 nodes, 6M rows, merges, then paused): after ingest 4.1-5.1 GB
    per node vs 1.8 GB restarted, 4-client throughput -35%. `malloc_trim` via gdb frees ~2.5 GB
    but does not change latency. Per-shard timing (cairn_query::stats) puts the slowdown in
    the search jobs. gdb stack samples under load: glibc `_int_malloc` 14% vs 1%, mostly a
    bitmap clone per segment and query.
  - 2c12511 removes that clone (gap halves). mimalloc as the server allocator (ADR 0029)
    closes it: after ingest vs restarted 2.1 vs 1.9 GB, -6% throughput. On identical data,
    fresh: +21-27% unfiltered and +36% filtered throughput over glibc, +10% memory.
  - Method mistakes: gdb pauses held the bench's settle step, so the first two sampling runs
    captured no query; a wait loop without timeout idled 2.5 h (10K-query file, 20K asked).
  - To confirm at 50M on the next GCP run. Docker image builds and serves (podman smoke test).

- Publication prep, local only (2026-09-28, owner: "vas-y pour la partie 1 en local"):
  README rewritten (deletion guarantee first, 50M results, known limits), ROADMAP.md, issue
  templates (bug, proposal, deletion guarantee routed to private reporting for real data), PR
  template, docs/community/ (labels, 9 first issues, org profile, publication checklist with
  audit: no secrets, names or public IPs in tree or history; licenses permissive). Nothing
  pushed; the org cairn-db is still empty. Owner decides on internal files (CLAUDE.md,
  prompts/, progress.md) and visibility.

- GCP 50M run 9 (2026-09-28, owner: "go pour le run gcp"): bench-results/phase4-gcp-bigann50m-run9.md.
  - mimalloc confirmed at 50M: queries on the ingesting processes, no restart: p99 35/36 ms
    unfiltered, 31/30 ms filtered, 435/318 QPS, takedown p99 102 ms. Anon 16.5-17.2 GB per node
    (run 8: 33.8 GB). A restart now gains 8-9% (was 3-4x).
  - Ingest 20,667 docs/s (run 8: 8,343), no long stall; not seen locally, one run: to confirm.
  - About 3 USD, fleet deleted 11:09 UTC and checked. Repository published privately at
    github.com/Cairn-DB/cairn (owner's go, 2026-09-28); CI and image workflows green.

- HTTP authentication (2026-09-28, owner: auth before the developer preview, "go"): ADR 0030.
  API keys (SHA-256 digests in a keys file, `cairn-server keygen`, CAIRN_HTTP_ADMIN_KEY), roles
  read/write/takedown/admin (takedown apart from write), 401/403, unknown routes need admin.
  Secure by default: no keys, no API, unless --http-insecure-dev; the Docker image generates
  an admin key on first start and prints it once. HTTPS with rustls (tokio-rustls, the only
  new dependency). Takedown audit log (key id, ids, token), on by default. 129 tests (process
  tests over HTTPS); image smoke-tested (401/200, audit line, digests only on disk).

- Toward a developer preview (2026-09-28, owner: "go dans l'ordre"):
  1. GCP run 10 reproduced run 9: ingest 19,660 docs/s; no-restart p99 31/31 ms unfiltered,
     31/33 ms filtered, 374/352 QPS, takedown p99 102 ms. About 3 USD, fleet deleted and checked.
  2. docs/deployment.md (3 zones, sizing, security, operations). Review caught and fixed two
     unsafe claims: rejoining with an empty disk, restoring one node from an old copy
     (forgotten Raft votes).
  3. CHANGELOG.md, docs/releasing.md, release.yml: tag-gated signed multi-arch images (cross-
     compiled arm64, SBOM, provenance, cosign keyless) and a GitHub release. The rehearsal
     run is green on GitHub, and the arm64 image builds. It was not run on arm64 hardware.
  4. Pending: the owner's decisions on internal files (CLAUDE.md, prompts/, progress.md) and
     visibility.

- Release 0.1.0, developer preview (2026-09-28, owner: "go avec la v0.1"): history rewritten
  by the owner (data/rewrite-consultant.sh, filter-repo) to remove personal details; old -> new
  commit ids in docs/history-rewrite-map.txt. Tag v0.1.0 at f0da9f9: release pipeline green
  (verify, signed amd64+arm64 images ghcr.io/cairn-db/cairn:0.1.0 and :0.1, GitHub
  pre-release). Everything is still private. Pending: contact@cairn-db.com reception (an OVH
  alias to create), DKIM/DMARC, then going public with the owner's go.

- 0.2 step 1, text ids (2026-09-29, owner: "go pour la 0.2, dans l'ordre"): ADR 0031.
  - Reserved `_key`/`_tenant` fields in every schema; per-shard dictionary rebuilt from `_key`
    at open and snapshot install; internal ids carry the shard (bit 63, 23-bit shard, 40-bit
    counter); counter recorded at freeze and written to the manifest at publication.
  - Chaos (default seeds) caught a divergence: reusing the dictionary's id on rewrite broke
    after restarts, because deletion files are persisted past the manifest point. Fixed: every
    keyed write takes a new id from the counter. Chaos workload now half keyed.
  - Found and fixed a pre-existing poison pill: an invalid document from a binary client
    stopped its replica in a replay loop. Nodes validate and pad before proposing; replicas skip
    invalid documents.
  - 131 tests; campaign 3,000 seeds zero violations; acceptance 75/75 on 3 nodes; positive
    control on the snapshot rebuild path.

- 0.2 step 2, deletion by filter (2026-09-29): ADR 0031 section 2, implementation notes there.
  - `Command::DeleteWhere { scope: All | Ids | Keys, filter }`, resolved by the store when it
    applies the entry (`Store::apply` is now async): segments through their filter indexes
    (only the filtered fields decoded; a column-read fallback without the index crate),
    frozen memtables minus rows replaced since, the memtable. Proto: request 12, response
    `Deleted` (7); protocol stays 6 (not released yet).
  - Leader returns the count with the token (`ReplicaHandle::propose_applied`); the node fans
    out one command per shard and sums. HTTP `POST /v1/documents/delete` with `filter`
    (optionally `ids`); match-everything filters and reserved fields refused; audited.
  - Chaos: a "delete versions <= X" operation in the workload, folded into the model in log
    order; read-your-takedown checked for it. A retried deletion by filter can commit twice
    (ambiguous failure), and the earlier copy can remove versions the acknowledged one no
    longer sees: the checker explains an absent result by any such deletion called before the
    read returned. Positive control: a store that resolves nothing fails the signature test.
  - Tests: store crash workload with filter deletions (fallback path), engine reference test
    over segments/updates/memtable/reopen (index path), HTTP end to end (forwarding, scoped,
    400s), acceptance 87/87 on 3 nodes (data/acceptance-02.log). Campaign seeds 0..3000:
    zero violations, 494,400 reads checked. 132 tests, clippy clean.
  - Open: cost at scale not measured (reads the filtered index sections of every segment on
    the replica actor).

- 0.2 step 3, tenants (2026-09-29, owner: "go pour l'étape 3"): ADR 0031 section 3, notes there.
  - HTTP layer only; no storage or protocol change. Scope per request: key's tenant, or an
    unscoped key's `Cairn-Tenant` header (scoped key + other tenant: 403).
  - Found while implementing: shared client ids would let one tenant overwrite another's
    document. Ids under a tenant are stored as text ids `<tenant>U+001F#<int>` /
    `<tenant>U+001F$<text>`; U+001F refused in client text ids.
  - `_tenant == t` added to every search and deletion; reads rechecked; `DELETE
    /v1/tenants/{t}` (takedown, unscoped); keys file `"tenant"`, `keygen --tenant`; scoped keys
    cannot be admin. Audit lines carry the tenant.
  - Tests: unit (namespacing, filters, key files), `tests/http_tenants.rs` (isolation against a
    running node; positive control: without the tenant filter it fails), acceptance section
    through the header. 135 tests, clippy clean, acceptance 95/95 on 3 nodes
    (data/acceptance-03.log).

- 0.2 step 4, clients (2026-09-29, owner: "go pour l'étape 4"): ADR 0031 section 6, notes there.
  - `clients/typescript` (`@cairn-db/client`, TypeScript is the only dev dependency, built to
    ESM + CJS with tsc) and `clients/python` (`cairn-db`, `Client` + `AsyncClient`, httpx).
  - `clients/test-live.sh`: builds cairn-server, starts a node with an unscoped key, a key
    scoped to "acme" and a read key, runs both suites. Local run: TypeScript 6/6 (5 unit + 1
    live), Python 6/6 (4 unit + 2 live, sync and async). CI job `clients` on Node 18 +
    Python 3.9 and Node 22 + Python 3.13.
  - Not run locally: Python 3.9 and Node 18 (uv Python downloads are set to manual on this
    machine; not changed). The CI matrix is the check: run 36582347306 (commit 2a85ba9),
    both entries green, live tests run (TypeScript 6/6, Python 6/6, 0 skipped).
  - Not published to npm/PyPI (public release, owner's go).

- Release 0.2.0, private (2026-09-29, owner: "parfait go" on: private tag now, public launch
  with 0.3 after step B). Tag v0.2.0 at ff3df22; release workflow green (verify with the
  3,000-seed campaign, signed multi-arch image ghcr.io/cairn-db/cairn:0.2.0 and :0.2, GitHub
  pre-release). Everything private. npm/PyPI not published (names `cairn-db`,
  `langchain-cairn`, `@cairn-db/client` were free on 2026-09-29; the owner may reserve the
  npm org).

- 0.3 step B.1, collections (2026-09-29): ADR 0031 section 4, notes there.
  - Catalog = shard group 2^23-1 on every node; create/drop serialized on its leader after a
    linearizable read; per-node reconciler (300 ms, local stale read) starts/stops replicas and
    deletes dropped collections' files; `Request::In` wrapper; HTTP routes under
    `/v1/collections/{c}`; default collection unchanged.
  - Found and fixed (runtime): calls on a connection closed by a restarted peer waited out the
    10 s timeout. The collections test failed 4 runs out of 6 before the fix, 0 out of 8 after.
    Regression test with positive control.
  - `ReplicaHandle::stop` (simulator test: stop, reopen, catch up).
  - The acceptance suite caught a node answering for a dropped collection (its catalog view
    was replaced by each read, and the listing that showed the drop came from another node).
    Fixed: the view only grows (ids are never reused). Acceptance 100/100 on 3 nodes
    (data/acceptance-04.log).
  - Not done: per-collection replication, counts, schema evolution.
  - Clients: `collection(name)` views, create/list/drop (live 7/7 each).
- 0.3 step B.2, `group_by` search (2026-09-29): HTTP-level, widening candidates 4k -> 10,000;
  3-process test incl. widening past 40 chunks of one parent (positive control fails without
  widening); clients `groupBy`/`group_by` (live 7/7 each).
- 0.3 step B.3, integrations (2026-09-29): `integrations/langchain-cairn`, `langchain-js`,
  `llama-index-vector-stores-cairn`; one storage layout (metadata blob + declared fields,
  `parent`). Live: LangChain.js 1/1, LangChain + LlamaIndex 2/2 (venv in the scratchpad:
  langchain-core 1.6.6, llama-index-core 0.14.25, @langchain/core 1.2.13). Requirements found:
  Python 3.10+, Node 20+; CI runs adapters on Node 22 / Python 3.13 only.
  - Step B complete. Owner chose step C before the public launch ("étape c").
- 0.3 step C.1, retention (2026-09-29): ADR 0031 "step C.1" notes. `Runtime::unix_millis`;
  per-collection `expires_field` (+ `--expires-field`, `--retention-interval-ms`); leader sweep
  = probe then `DeleteWhere{field <= now}`; HTTP hides expired docs at once; audited.
  `tests/http_retention.rs` (3/3 runs); idle sweeps add no log entries (checked).
- 0.3 step C.2, partial updates (2026-09-29): ADR 0031 "step C.2". First design (Patch in the
  log, resolved at apply) rejected before testing: replay after a restart could lose the
  document (seed-100 masks). Chosen: leader resolves in log order (barrier), proposes whole
  documents. Chaos with patches: 3,000 seeds OK (285,094 reads); positive control (no
  barrier) fails at seed 24. Process tests, clients live.
- 0.3 step C.4, proof of deletion (2026-09-29): signed report from every replica
  (`/v1/deletions/proof`, `verify-proof`); ADR 0031 "step C.4". Tested (unit + 3 processes incl.
  a node down).
- C.3 Kafka: NOT started. ADR 0024 is proposed and says "after the public release, with the
  first contributors"; librdkafka is a heavy C dependency. Asked the owner (2026-09-29).

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
