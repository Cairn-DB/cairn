//! AVX2+FMA and AVX-512F kernels for x86_64.
//!
//! Each public entry point checks nothing itself: the dispatch in the parent module only hands
//! out these tables after `is_x86_feature_detected!` confirmed the features. Inside, the
//! `#[target_feature]` functions are the only `unsafe` code in the index crate.

use super::Kernels;
use std::arch::x86_64::*;

/// AVX2 + FMA table.
pub const AVX2: Kernels = Kernels {
    name: "avx2",
    l2_sq: avx2_l2_sq,
    dot: avx2_dot,
    l2_sq_u8_batch: avx2_l2_sq_u8_batch,
    dot_u8_batch: avx2_dot_u8_batch,
    l2_sq_batch: avx2_l2_sq_batch,
    dot_batch: avx2_dot_batch,
};

/// AVX-512F + BW table.
pub const AVX512: Kernels = Kernels {
    name: "avx512",
    l2_sq: avx512_l2_sq,
    dot: avx512_dot,
    l2_sq_u8_batch: avx512_l2_sq_u8_batch,
    dot_u8_batch: avx512_dot_u8_batch,
    l2_sq_batch: avx512_l2_sq_batch,
    dot_batch: avx512_dot_batch,
};

// ---------------------------------------------------------------- AVX2

/// Horizontal sum of 8 lanes. Pointer-free intrinsics are safe inside a target-feature function.
#[target_feature(enable = "avx2,fma")]
fn hsum256(v: __m256) -> f32 {
    let hi = _mm256_extractf128_ps(v, 1);
    let lo = _mm256_castps256_ps128(v);
    let s = _mm_add_ps(hi, lo);
    let s = _mm_add_ps(s, _mm_movehl_ps(s, s));
    let s = _mm_add_ss(s, _mm_shuffle_ps(s, s, 1));
    _mm_cvtss_f32(s)
}

#[target_feature(enable = "avx2,fma")]
unsafe fn avx2_l2_sq_inner(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len();
    let (pa, pb) = (a.as_ptr(), b.as_ptr());
    let mut i = 0;
    // SAFETY: every load reads 8 floats at i..i+8 with i + 8 <= n, from slices of equal length.
    unsafe {
        let mut acc0 = _mm256_setzero_ps();
        let mut acc1 = _mm256_setzero_ps();
        while i + 16 <= n {
            let d0 = _mm256_sub_ps(_mm256_loadu_ps(pa.add(i)), _mm256_loadu_ps(pb.add(i)));
            let d1 = _mm256_sub_ps(
                _mm256_loadu_ps(pa.add(i + 8)),
                _mm256_loadu_ps(pb.add(i + 8)),
            );
            acc0 = _mm256_fmadd_ps(d0, d0, acc0);
            acc1 = _mm256_fmadd_ps(d1, d1, acc1);
            i += 16;
        }
        while i + 8 <= n {
            let d0 = _mm256_sub_ps(_mm256_loadu_ps(pa.add(i)), _mm256_loadu_ps(pb.add(i)));
            acc0 = _mm256_fmadd_ps(d0, d0, acc0);
            i += 8;
        }
        let mut s = hsum256(_mm256_add_ps(acc0, acc1));
        while i < n {
            let d = a[i] - b[i];
            s += d * d;
            i += 1;
        }
        s
    }
}

#[target_feature(enable = "avx2,fma")]
unsafe fn avx2_dot_inner(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len();
    let (pa, pb) = (a.as_ptr(), b.as_ptr());
    let mut i = 0;
    // SAFETY: as in `avx2_l2_sq_inner`.
    unsafe {
        let mut acc0 = _mm256_setzero_ps();
        let mut acc1 = _mm256_setzero_ps();
        while i + 16 <= n {
            acc0 = _mm256_fmadd_ps(_mm256_loadu_ps(pa.add(i)), _mm256_loadu_ps(pb.add(i)), acc0);
            acc1 = _mm256_fmadd_ps(
                _mm256_loadu_ps(pa.add(i + 8)),
                _mm256_loadu_ps(pb.add(i + 8)),
                acc1,
            );
            i += 16;
        }
        while i + 8 <= n {
            acc0 = _mm256_fmadd_ps(_mm256_loadu_ps(pa.add(i)), _mm256_loadu_ps(pb.add(i)), acc0);
            i += 8;
        }
        let mut s = hsum256(_mm256_add_ps(acc0, acc1));
        while i < n {
            s += a[i] * b[i];
            i += 1;
        }
        s
    }
}

/// Dequantized SQ8 row distance: `sum((q - (min + r*scale))^2)` or `sum(q * (min + r*scale))`.
#[target_feature(enable = "avx2,fma")]
unsafe fn avx2_u8_row(q: &[f32], row: &[u8], min: &[f32], scale: &[f32], l2: bool) -> f32 {
    let n = q.len();
    let mut i = 0;
    // SAFETY: loads of 8 u8 / 8 f32 at i..i+8 with i + 8 <= n; all slices have length n.
    unsafe {
        let mut acc = _mm256_setzero_ps();
        while i + 8 <= n {
            let r8 = _mm_loadl_epi64(row.as_ptr().add(i) as *const __m128i);
            let r = _mm256_cvtepi32_ps(_mm256_cvtepu8_epi32(r8));
            let x = _mm256_fmadd_ps(
                r,
                _mm256_loadu_ps(scale.as_ptr().add(i)),
                _mm256_loadu_ps(min.as_ptr().add(i)),
            );
            let qv = _mm256_loadu_ps(q.as_ptr().add(i));
            if l2 {
                let d = _mm256_sub_ps(qv, x);
                acc = _mm256_fmadd_ps(d, d, acc);
            } else {
                acc = _mm256_fmadd_ps(qv, x, acc);
            }
            i += 8;
        }
        let mut s = hsum256(acc);
        while i < n {
            let x = min[i] + f32::from(row[i]) * scale[i];
            s += if l2 {
                (q[i] - x) * (q[i] - x)
            } else {
                q[i] * x
            };
            i += 1;
        }
        s
    }
}

fn avx2_l2_sq(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len());
    // SAFETY: the dispatcher selected this table only after detecting AVX2 and FMA.
    unsafe { avx2_l2_sq_inner(a, b) }
}

fn avx2_dot(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len());
    // SAFETY: as in `avx2_l2_sq`.
    unsafe { avx2_dot_inner(a, b) }
}

fn avx2_l2_sq_batch(query: &[f32], rows: &[f32], out: &mut [f32]) {
    let d = query.len();
    for (o, row) in out.iter_mut().zip(rows.chunks_exact(d)) {
        // SAFETY: as in `avx2_l2_sq`.
        *o = unsafe { avx2_l2_sq_inner(query, row) };
    }
}

fn avx2_dot_batch(query: &[f32], rows: &[f32], out: &mut [f32]) {
    let d = query.len();
    for (o, row) in out.iter_mut().zip(rows.chunks_exact(d)) {
        // SAFETY: as in `avx2_l2_sq`.
        *o = unsafe { avx2_dot_inner(query, row) };
    }
}

fn avx2_l2_sq_u8_batch(query: &[f32], rows: &[u8], min: &[f32], scale: &[f32], out: &mut [f32]) {
    let d = query.len();
    assert!(min.len() == d && scale.len() == d);
    for (o, row) in out.iter_mut().zip(rows.chunks_exact(d)) {
        // SAFETY: as in `avx2_l2_sq`; lengths checked above.
        *o = unsafe { avx2_u8_row(query, row, min, scale, true) };
    }
}

fn avx2_dot_u8_batch(query: &[f32], rows: &[u8], min: &[f32], scale: &[f32], out: &mut [f32]) {
    let d = query.len();
    assert!(min.len() == d && scale.len() == d);
    for (o, row) in out.iter_mut().zip(rows.chunks_exact(d)) {
        // SAFETY: as in `avx2_l2_sq`; lengths checked above.
        *o = unsafe { avx2_u8_row(query, row, min, scale, false) };
    }
}

// ---------------------------------------------------------------- AVX-512

#[target_feature(enable = "avx512f,avx512bw")]
unsafe fn avx512_l2_sq_inner(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len();
    let (pa, pb) = (a.as_ptr(), b.as_ptr());
    let mut i = 0;
    // SAFETY: loads of 16 floats at i..i+16 with i + 16 <= n; the masked tail load reads only
    // the `n - i` lanes selected by the mask.
    unsafe {
        let mut acc0 = _mm512_setzero_ps();
        let mut acc1 = _mm512_setzero_ps();
        while i + 32 <= n {
            let d0 = _mm512_sub_ps(_mm512_loadu_ps(pa.add(i)), _mm512_loadu_ps(pb.add(i)));
            let d1 = _mm512_sub_ps(
                _mm512_loadu_ps(pa.add(i + 16)),
                _mm512_loadu_ps(pb.add(i + 16)),
            );
            acc0 = _mm512_fmadd_ps(d0, d0, acc0);
            acc1 = _mm512_fmadd_ps(d1, d1, acc1);
            i += 32;
        }
        while i + 16 <= n {
            let d0 = _mm512_sub_ps(_mm512_loadu_ps(pa.add(i)), _mm512_loadu_ps(pb.add(i)));
            acc0 = _mm512_fmadd_ps(d0, d0, acc0);
            i += 16;
        }
        if i < n {
            let mask: __mmask16 = (1u16 << (n - i)) - 1;
            let d0 = _mm512_sub_ps(
                _mm512_maskz_loadu_ps(mask, pa.add(i)),
                _mm512_maskz_loadu_ps(mask, pb.add(i)),
            );
            acc0 = _mm512_fmadd_ps(d0, d0, acc0);
        }
        _mm512_reduce_add_ps(_mm512_add_ps(acc0, acc1))
    }
}

#[target_feature(enable = "avx512f,avx512bw")]
unsafe fn avx512_dot_inner(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len();
    let (pa, pb) = (a.as_ptr(), b.as_ptr());
    let mut i = 0;
    // SAFETY: as in `avx512_l2_sq_inner`.
    unsafe {
        let mut acc0 = _mm512_setzero_ps();
        let mut acc1 = _mm512_setzero_ps();
        while i + 32 <= n {
            acc0 = _mm512_fmadd_ps(_mm512_loadu_ps(pa.add(i)), _mm512_loadu_ps(pb.add(i)), acc0);
            acc1 = _mm512_fmadd_ps(
                _mm512_loadu_ps(pa.add(i + 16)),
                _mm512_loadu_ps(pb.add(i + 16)),
                acc1,
            );
            i += 32;
        }
        while i + 16 <= n {
            acc0 = _mm512_fmadd_ps(_mm512_loadu_ps(pa.add(i)), _mm512_loadu_ps(pb.add(i)), acc0);
            i += 16;
        }
        if i < n {
            let mask: __mmask16 = (1u16 << (n - i)) - 1;
            acc0 = _mm512_fmadd_ps(
                _mm512_maskz_loadu_ps(mask, pa.add(i)),
                _mm512_maskz_loadu_ps(mask, pb.add(i)),
                acc0,
            );
        }
        _mm512_reduce_add_ps(_mm512_add_ps(acc0, acc1))
    }
}

#[target_feature(enable = "avx512f,avx512bw")]
unsafe fn avx512_u8_row(q: &[f32], row: &[u8], min: &[f32], scale: &[f32], l2: bool) -> f32 {
    let n = q.len();
    let mut i = 0;
    // SAFETY: 16-lane loads at i..i+16 with i + 16 <= n; masked tail reads `n - i` lanes.
    unsafe {
        let mut acc = _mm512_setzero_ps();
        while i + 16 <= n {
            let r8 = _mm_loadu_si128(row.as_ptr().add(i) as *const __m128i);
            let r = _mm512_cvtepi32_ps(_mm512_cvtepu8_epi32(r8));
            let x = _mm512_fmadd_ps(
                r,
                _mm512_loadu_ps(scale.as_ptr().add(i)),
                _mm512_loadu_ps(min.as_ptr().add(i)),
            );
            let qv = _mm512_loadu_ps(q.as_ptr().add(i));
            if l2 {
                let d = _mm512_sub_ps(qv, x);
                acc = _mm512_fmadd_ps(d, d, acc);
            } else {
                acc = _mm512_fmadd_ps(qv, x, acc);
            }
            i += 16;
        }
        if i < n {
            let mask: __mmask16 = (1u16 << (n - i)) - 1;
            let r8 = _mm_maskz_loadu_epi8(mask, row.as_ptr().add(i) as *const i8);
            let r = _mm512_cvtepi32_ps(_mm512_cvtepu8_epi32(r8));
            let x = _mm512_fmadd_ps(
                r,
                _mm512_maskz_loadu_ps(mask, scale.as_ptr().add(i)),
                _mm512_maskz_loadu_ps(mask, min.as_ptr().add(i)),
            );
            let qv = _mm512_maskz_loadu_ps(mask, q.as_ptr().add(i));
            if l2 {
                let d = _mm512_maskz_sub_ps(mask, qv, x);
                acc = _mm512_fmadd_ps(d, d, acc);
            } else {
                // mask3 form: unselected lanes keep the accumulator's value.
                acc = _mm512_mask3_fmadd_ps(qv, x, acc, mask);
            }
        }
        _mm512_reduce_add_ps(acc)
    }
}

fn avx512_l2_sq(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len());
    // SAFETY: the dispatcher selected this table only after detecting AVX-512F and BW.
    unsafe { avx512_l2_sq_inner(a, b) }
}

fn avx512_dot(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len());
    // SAFETY: as in `avx512_l2_sq`.
    unsafe { avx512_dot_inner(a, b) }
}

fn avx512_l2_sq_batch(query: &[f32], rows: &[f32], out: &mut [f32]) {
    let d = query.len();
    for (o, row) in out.iter_mut().zip(rows.chunks_exact(d)) {
        // SAFETY: as in `avx512_l2_sq`.
        *o = unsafe { avx512_l2_sq_inner(query, row) };
    }
}

fn avx512_dot_batch(query: &[f32], rows: &[f32], out: &mut [f32]) {
    let d = query.len();
    for (o, row) in out.iter_mut().zip(rows.chunks_exact(d)) {
        // SAFETY: as in `avx512_l2_sq`.
        *o = unsafe { avx512_dot_inner(query, row) };
    }
}

fn avx512_l2_sq_u8_batch(query: &[f32], rows: &[u8], min: &[f32], scale: &[f32], out: &mut [f32]) {
    let d = query.len();
    assert!(min.len() == d && scale.len() == d);
    for (o, row) in out.iter_mut().zip(rows.chunks_exact(d)) {
        // SAFETY: as in `avx512_l2_sq`; lengths checked above.
        *o = unsafe { avx512_u8_row(query, row, min, scale, true) };
    }
}

fn avx512_dot_u8_batch(query: &[f32], rows: &[u8], min: &[f32], scale: &[f32], out: &mut [f32]) {
    let d = query.len();
    assert!(min.len() == d && scale.len() == d);
    for (o, row) in out.iter_mut().zip(rows.chunks_exact(d)) {
        // SAFETY: as in `avx512_l2_sq`; lengths checked above.
        *o = unsafe { avx512_u8_row(query, row, min, scale, false) };
    }
}
