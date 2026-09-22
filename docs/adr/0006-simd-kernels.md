# ADR 0006: SIMD strategy — hand-written kernels, runtime dispatch, scalar oracle

- Status: proposed
- Date: 2026-09-22
- Resolves: SPEC.md O5

## Context

Distance kernels (L2, dot/cosine on f32; dot on SQ8 u8/i8; Hamming on binary) dominate query CPU.
The toolchain is pinned to stable (rust-toolchain.toml), which rules out `std::simd`. `unsafe` is
allowed only for SIMD and I/O boundaries and must carry `// SAFETY:` comments and Miri coverage
where possible (SPEC section 10).

## Options considered

1. **`std::simd`**: portable and pleasant, nightly only. Rejected by the stable pin.
2. **`wide` crate** (1.7.1, 2026-09-14, active): portable SIMD on stable, safe API, no runtime
   dispatch (compiles for the baseline target unless we build with `target-cpu`). Good fallback.
3. **Hand-written `core::arch` intrinsics** with runtime dispatch (`is_x86_feature_detected!` +
   `#[target_feature]` functions): AVX2+FMA and AVX-512 on x86_64, NEON on aarch64, scalar
   elsewhere. Maximum control (multiple accumulators, batched query-vs-N layouts, prefetch), most
   `unsafe`.
4. **`simsimd`** (C, FFI): fast and broad, but an FFI boundary in the hottest loop, a C toolchain
   dependency, and no Miri story.

## Decision

Option 3 for the four kernels that matter, with a scalar reference implementation as the oracle
and `wide` kept as the portable baseline if the benchmark shows intrinsics buy less than 10%.
Rules:

- Kernels take slices of known length; batch variants compute one query against N rows to amortize
  loads. Four accumulators minimum for f32 reductions (the compiler will not reorder float adds).
- Dispatch is decided once per process and cached; the sim always uses the scalar kernel so
  simulated replicas are bit-identical.
- Every kernel has a `proptest` that compares it with the scalar reference within a tolerance
  expressed in ULPs, over random lengths including non-multiples of the lane width.
- Every `unsafe` block is a `#[target_feature]` function body with a `// SAFETY:` comment stating the
  detected feature; call sites stay in one dispatch module.
- Miri: covers the scalar kernels, the layout/byte-cast code and the dispatch logic. Miri does not
  execute most vendor intrinsics (extent unverified); this gap is documented and closed by the
  proptest oracle plus running the test suite on real AVX2/AVX-512/NEON hardware in CI.

## Consequences

- Portable correctness comes from the oracle tests, performance from the intrinsics.
- Floating-point results may differ in low bits across CPU generations: this is why graphs are
  built once on the leader and shipped as bytes (ADR 0003, ADR 0008) rather than rebuilt per
  replica.
- aarch64 (NEON) is a second-class target until someone runs the benchmarks on it.

## Experiment that confirms or refutes

Phase 2, M2.1: criterion benchmarks on 128-d (SIFT), 192-d (YFCC) and 512/768-d (CLIP/text)
vectors: scalar vs AVX2 vs AVX-512 vs `wide`, single and batched. Report GB/s and ns per distance.
Adopt `wide` for any kernel where intrinsics gain under 10%.
