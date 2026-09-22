# Top risks (Phase 0), ranked by likelihood × impact

Status: proposed, 2026-09-22. L and I on 1–5; score = L × I. Each risk names the cheap experiment
that retires or confirms it and the milestone (docs/roadmap.md) where it runs.

| # | Risk | L | I | Score | Early experiment | When |
|---|---|---|---|---|---|---|
| R1 | Filtered ANN cannot hold recall@10 > 95% in the 1–5% selectivity band with **correlated** filters; the scan/graph crossover is worse than assumed | 4 | 5 | 20 | Selectivity × correlation sweep on SIFT1M + bench-gen attributes, then YFCC-10M; fix threshold T or fall back (ADR 0003) | M2.2, week 1 |
| R2 | Own Raft has a safety bug that the simulator finds late, or takes far longer than planned | 3 | 5 | 15 | Differential test against `raft-rs` 0.7 in the simulator from the first Raft milestone; hard fallback date at M3.3 (ADR 0008) | M3.2 |
| R3 | Memory at 10–50M vectors: 50M × 512-d f32 = about 100 GB raw, SQ8 = 25 GB, plus graph (M = 32 → about 6.4 GB) and text. The success criteria never state the hardware or whether the working set is RAM-resident | 4 | 4 | 16 | Decide target hardware now (user decision); compute the budget per node; if RAM-resident is out, schedule PQ or disk-resident graph (DiskANN-style) as a Phase 4 milestone | Phase 0 decision, checked at M2.6 |
| R4 | Nondeterminism leaks into engine code (hash maps, time, threads, FP dispatch) and the simulator silently loses reproducibility | 4 | 4 | 16 | `clippy.toml` bans (done); run every seed twice and compare trace hashes from the first sim milestone (ADR 0011) | M1.1 |
| R5 | Owning the runtime (executor + io_uring reactor) costs more than expected or underperforms `glommio` | 3 | 3 | 9 | Two-day WAL append / segment read spike comparing sync, own reactor, `glommio`; documented fallback (ADR 0002) | M1.5 |
| R6 | Segment build cost (HNSW on up to 1M vectors) stalls ingest or lags the memtable; memtable exact-scan cost grows with the cap | 3 | 3 | 9 | Measure build time per 100k vectors at M = 16/32 and set the memtable cap so the memtable scan stays under a latency budget; smaller segments + compaction | M2.2 |
| R7 | Multi-raft overhead: heartbeats and elections scale with groups × nodes | 2 | 4 | 8 | Coalesce heartbeats per node pair in the driver; measure idle CPU with 256 groups on 3 simulated nodes | M3.3 |
| R8 | Own BM25 quality lags `tantivy`/Anserini and hybrid results look bad for reasons unrelated to the vector side | 2 | 3 | 6 | MS MARCO MRR@10 vs the published BM25 baseline; the hybrid experiment (ADR 0005, 0007) | M2.4, M2.5 |
| R9 | Datasets: YFCC-10M filtered track is verified (CC BY 4.0, 192-d uint8), but LAION subsets with precomputed CLIP embeddings and their licence status are **unverified**, and DEEP1B hosting has moved in the past | 3 | 2 | 6 | Verify download and licence of the 10–50M dataset at the start of Phase 2; prefer YFCC-10M plus a DEEP subset from the Big-ANN repo | M2.6 |
| R10 | The Phase 1 exit criterion "storage benchmarks vs RocksDB" requires building `librocksdb` (heavy C++ dependency, long CI builds) and compares a KV store with a segment store | 3 | 2 | 6 | Decide (user): keep, and do only a WAL-append and point-read comparison; or replace with absolute targets on documented hardware | Phase 0 decision |
| R11 | Cross-shard takedowns are not atomic; a client that stops after acknowledging half of a multi-document takedown believes it is done | 2 | 4 | 8 | Tokens per document, composed client-side; the takedown call in `cairn-client` is all-or-report (ADR 0010); the signature test is per document | M3.4 |
| R12 | Scope vs. bandwidth: one person plus an agent, five subsystems each the size of a project | 4 | 3 | 12 | The roadmap's milestones are small and each phase has a hard exit; features listed under "not in v1" stay out | continuous |
| R13 | io_uring availability and behaviour differences across kernels (unverified minimum versions for the features we use) | 2 | 2 | 4 | The blocking reactor is a first-class fallback, not a stub; the runtime spike documents the kernel version | M1.5 |

What worries me most: R1 and R3 together. If the target is 50M CLIP vectors RAM-resident on one
or three ordinary machines, SQ8 alone may not fit, and the disk-resident alternatives push the
filtered-ANN design toward DiskANN-style graphs, which is a different Phase 2. That decision needs
a hardware target before Phase 2 starts.
