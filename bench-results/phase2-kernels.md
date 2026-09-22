# Phase 2 kernel benchmarks (M2.1)

- Date: 2026-09-22. Command: `cargo bench -p cairn-index --bench kernels` (criterion, release,
  single thread). Hardware: AMD Ryzen 7 8845HS (Zen 4, AVX-512 double-pumped over 256-bit
  units), data resident in L2 (1,024 rows per batch).
- Query vs 1,024 rows, mean time per batch; "ns/row" = time / 1024.

| dims | kind | scalar | avx2 | avx512 | best ns/row |
|---|---|---|---|---|---|
| 128 | L2 f32 | 29.7 µs | 6.25 µs | 5.99 µs | 5.9 |
| 128 | L2 SQ8 (dequant on the fly) | 92.8 µs | 10.5 µs | 7.88 µs | 7.7 |
| 128 | dot f32 | 27.0 µs | 5.60 µs | 5.76 µs | 5.5 |
| 192 | L2 f32 | 43.5 µs | 10.1 µs | 9.81 µs | 9.6 |
| 192 | L2 SQ8 | 139.6 µs | 17.7 µs | 14.1 µs | 13.7 |
| 192 | dot f32 | 40.2 µs | 9.53 µs | 9.98 µs | 9.3 |
| 512 | L2 f32 | 114.5 µs | 25.7 µs | 25.8 µs | 25.1 |
| 512 | L2 SQ8 | 368.1 µs | 47.4 µs | 35.3 µs | 34.5 |
| 512 | dot f32 | 104.2 µs | 25.1 µs | 24.9 µs | 24.3 |
| 768 | L2 f32 | 166.8 µs | 38.1 µs | 37.5 µs | 36.6 |
| 768 | L2 SQ8 | 535.5 µs | 72.9 µs | 52.4 µs | 51.1 |
| 768 | dot f32 | 153.5 µs | 37.3 µs | 37.0 µs | 36.1 |

Reading:

- SIMD is about 5x faster than the scalar reference for f32 and 12x for SQ8 (the scalar SQ8
  path pays for the `u8 -> f32` conversion per element).
- AVX-512 and AVX2 are equal for f32 on this CPU, as expected from Zen 4's 256-bit execution;
  AVX-512 wins 25–30% on SQ8 thanks to the 16-lane `vpmovzxbd` conversion.
- With rows in cache, SQ8 is *slower* than f32 (conversion cost, no bandwidth to save). SQ8 pays
  off when the working set exceeds cache: 1M × 128-d is 512 MB f32 vs 128 MB SQ8. The SIFT1M
  sweep measures that end to end; if SQ8 does not win there, ADR 0004's "SQ8 mandatory" is revisited.
- ADR 0006's rule "adopt `wide` where intrinsics gain under 10%" does not apply: the intrinsics
  gain 5x over scalar, and `wide` was not benchmarked (it would need its own dispatch story).
