# Roadmap with milestones (refines SPEC.md section 9)

Status: proposed, 2026-09-22. Each milestone ends with green `cargo fmt --check`, `clippy -D
warnings`, `cargo test --workspace`, and, where it says so, a committed benchmark. Phases do not
start before the previous exit criterion is met and approved (SPEC.md).

## Phase 0: architecture (this phase)

Exit: the user reviews and accepts the proposed ADRs (0002–0011), the architecture, the
verification strategy and this roadmap; `cargo check --workspace` and `cargo fmt --check` green
(done, 2026-09-22).

## Phase 1: single-node storage engine + bench-gen v1

| Milestone | Scope | Exit criterion |
|---|---|---|
| M1.1 Traits and simulated reactor | `cairn-core` traits and time types; `cairn-runtime` executor with the blocking reactor; `cairn-sim` clock/disk/rng with trace hashing; `clippy.toml` bans active | A trivial task graph runs identically (same trace hash) under two runs of the same seed; `loom` test on the SPSC queue green |
| M1.2 Log and manifest | Log records with CRC32C, append/fsync/truncate; manifest with atomic replace; recovery | Proptest "crash at any byte" recovers the committed prefix on the simulated disk with torn pages; fuzz targets for log records and manifest run 10 min clean |
| M1.3 Memtable and segment container | Memtable for all field kinds; segment writer/reader for `docids`, `col.*`, `vec.*.f32/sq8`, `stats`; deletion bitmaps and `.del` checkpoints | Roundtrip and fuzz on the container; point reads identical across memtable, replayed memtable and segment |
| M1.4 Compaction | Merge segments, apply deletions physically, publish via manifest | Proptest: multiset of live documents preserved under random compaction schedules with crashes |
| M1.5 Runtime spike and storage benchmarks | io_uring reactor; WAL append and segment read benchmarks: sync vs own reactor vs `glommio` branch | Results in `bench-results/phase1-storage.md` with hardware, kernel, p50/p99/p999; ADR 0002 confirmed or superseded. RocksDB comparison only if kept (risks R10) |
| M1.6 bench-gen v1 | Synthetic attributes (date, channel, rights, speaker) with selectivity and correlation knobs; takedown workload; deterministic by seed | Generated datasets are byte-identical across runs for the same seed; documented CLI |

Phase 1 exit (SPEC): crash-recovery property tests green; storage benchmarks documented.

## Phase 2: index layer and single-node hybrid queries

| Milestone | Scope | Exit criterion |
|---|---|---|
| M2.1 Kernels | Scalar oracle; AVX2/AVX-512/NEON kernels; dispatch; batched variants | Proptest vs oracle green; criterion results committed; `wide` adopted where intrinsics gain < 10% (ADR 0006) |
| M2.2 Vector search | HNSW build (deterministic) and search; exact scan; filter bitmaps; selectivity-adaptive dispatch; SQ8 with rerank | SIFT1M recall@10 ≥ 95% unfiltered at documented QPS; selectivity × correlation sweep committed; threshold T fixed (ADR 0003, risk R1) |
| M2.3 Structured indexes | Enum/set bitmaps, range indexes, bloom on ids; predicate evaluation to bitmaps | Bitmap results equal naive evaluation (proptest); filter-only queries never read vectors |
| M2.4 Full text | Tokenizer, postings, BM25, DAAT over bitmaps | MS MARCO MRR@10 within 0.01 of the published BM25 baseline (ADR 0005) |
| M2.5 Planner and executor | Query AST, planning, per-segment/per-shard execution, RRF and weighted fusion, k' oversampling | Hybrid experiment committed (ADR 0007); planner fuzz clean |
| M2.6 Published curves | `cairn-bench`: SIFT1M and YFCC-10M filtered track, recall/latency curves at 50/10/1/0.1% | `bench-results/phase2-*.md` with seeds, dataset versions, hardware; memory budget vs risk R3 stated |

Phase 2 exit (SPEC): recall/latency curves on SIFT1M and the filtered track published.

## Phase 3: deterministic simulator and Raft on 3 simulated nodes

| Milestone | Scope | Exit criterion |
|---|---|---|
| M3.1 Cluster simulator | Simulated network with all faults; process crash/restart/pause; clock skew; scenario files | Fault schedules reproducible by seed; trace hashes stable |
| M3.2 Raft state machine | Election with pre-vote, replication, ReadIndex; differential test vs `raft-rs` 0.7 (dev-dependency of `cairn-sim`) | 10,000 seeds, zero safety divergences (ADR 0008) |
| M3.3 Multi-raft and shipping | Driver, heartbeat coalescing, log-as-WAL integration, `SegmentPublished`, snapshot = manifest + segments + bitmaps; single-server membership change | Replica convergence checker green; hard decision point for the `raft-rs` fallback |
| M3.4 Checkers | Per-key linearizability, read-your-takedown, durability, index equivalence, consistency levels (ADR 0010) | All checkers green on 1,000 seeds × 10 scenarios nightly |
| M3.5 Campaign | 100 scenarios × 20,000 seeds | Zero violations; results committed with seeds and git hash |

Phase 3 exit (SPEC): millions of seeded runs with zero violations; takedown signature test green.

## Phase 4: real cluster

| Milestone | Scope | Exit criterion |
|---|---|---|
| M4.1 Protocol and client | `cairn-proto` (protobuf + framing), `cairn-client`, CLI | Frame codec fuzz clean; codec throughput benchmark |
| M4.2 Real runtime | io_uring reactor as default, real `Network`, listener, per-core connection handling | Contract tests pass on both reactors |
| M4.3 Placement and follower reads | Shard placement, leader hints, `ReadYourWrites` redirect/wait, snapshots over the network | 3-node cluster serves all three consistency levels |
| M4.4 Cluster test suite | The simulator's workload and checkers driven over `cairn-client` against the real cluster with real fault injection (kill, `tc netem` partitions) | Same suite green as Phase 3 |
| M4.5 Scale benchmark | 10–50M vectors on documented hardware; takedown-under-load workload | Success criteria of SPEC section 8 measured and reported honestly, met or not |

Phase 5 (later): async cross-cluster replication, gRPC gateway, PQ or disk-resident graphs if R3
requires them.
