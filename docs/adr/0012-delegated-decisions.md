# ADR 0012: Decisions taken under delegation, and reconsiderations for autonomous delivery

- Status: accepted (delegated 2026-09-22)
- Date: 2026-09-22

## Context

On 2026-09-22 the owner delegated every pending Phase 0 decision to the agent and asked for the
best deliverable against SPEC.md, explicitly allowing the Phase 0 choices to be reconsidered, with
no review until a fully functional prototype exists. Without a reviewer, choices that add months of
work for marginal benefit must be cut, and every choice must be verifiable by the test suite alone.

## Decisions

1. **ADRs 0002–0011 are accepted** with the amendments below. Their status lines say so.
2. **Hardware target** (risk R3): the development machine. AMD Ryzen 7 8845HS, 8 cores / 16
   threads, AVX-512 with VNNI and VPOPCNTDQ, 58 GB RAM, NVMe, Linux 7.2.5 with io_uring enabled.
   Working set is RAM-resident. Scale target: the Big-ANN filtered track at 10M vectors (YFCC,
   192-d uint8, tag filters, CC BY 4.0), which satisfies the "10–50M" range at its low end.
   50M is not attempted on this hardware; that is stated in the reports.
3. **Raft** (ADR 0008): own implementation stands. `raft-rs` is removed from `cairn-raft`'s
   dependencies now (it was never going to be used there); the differential test against it is
   an optional Phase 3 item, the mandatory oracle being the simulator's safety checkers.
4. **Wire protocol** (ADR 0009): protobuf messages defined as Rust structs with `prost` derives
   (no `.proto` files, no `protoc`, no `protox`). Wire-compatible with protobuf; a `.proto` can be
   generated later for other languages.
5. **RocksDB comparison** (risk R10) is dropped. Phase 1 storage benchmarks report absolute
   numbers on the documented hardware plus a baseline of plain `std::fs` append+fsync.
6. **Shard count** is fixed per collection at creation (default 8 for a single node).
7. **Approved light dependencies** (added when their milestone starts): `io-uring`,
   `rustc-hash`, `roaring`, `unicode-segmentation`, `prost`, `zerocopy` (byte views without
   hand-written unsafe), `smallvec` if profiling shows a need.
8. **Real-cluster fault injection** (Phase 4) uses process kills and an in-process network
   proxy in `cairn-server` test mode, not `tc netem` (no `sudo`).
9. **Git**: repository initialised 2026-09-22; commits per milestone on `main`.

## Consequences

The prototype is single-machine (three nodes are three processes on one host). Numbers are
comparable across runs, not across machines. Everything else in the ADRs stands.
