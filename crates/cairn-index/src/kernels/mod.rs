//! Distance kernels with runtime dispatch (ADR 0006).
//!
//! Every kernel exists as a scalar reference implementation ([`scalar`]) and, on x86_64, as
//! AVX2+FMA and AVX-512F variants. The variant is chosen once per process from CPU features,
//! unless [`force_scalar`] was called first (the simulator does this so every simulated replica
//! computes bit-identical results). Property tests check each variant against the scalar
//! reference within a tolerance expressed relative to the operand norms.
//!
//! Layouts: vectors are `&[f32]` of equal length; batched variants take a row-major matrix
//! `rows` with `n * dims` elements. SQ8 rows are `u8` per dimension with per-dimension `min` and
//! `scale`, dequantized on the fly as `min[d] + row[d] * scale[d]`.

pub mod scalar;
#[cfg(target_arch = "x86_64")]
pub mod x86;

use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

/// Signature of a batched SQ8 kernel: `(query, rows, min, scale, out)`.
pub type U8BatchFn = fn(&[f32], &[u8], &[f32], &[f32], &mut [f32]);

/// Function table for one dispatch level.
#[derive(Clone, Copy)]
pub struct Kernels {
    /// Name of the level (`scalar`, `avx2`, `avx512`).
    pub name: &'static str,
    /// Squared L2 distance.
    pub l2_sq: fn(&[f32], &[f32]) -> f32,
    /// Dot product.
    pub dot: fn(&[f32], &[f32]) -> f32,
    /// Squared L2 between a query and every row of an SQ8 matrix.
    pub l2_sq_u8_batch: U8BatchFn,
    /// Dot between a query and every row of an SQ8 matrix.
    pub dot_u8_batch: U8BatchFn,
    /// Squared L2 between a query and every row of an f32 matrix.
    pub l2_sq_batch: fn(&[f32], &[f32], &mut [f32]),
    /// Dot between a query and every row of an f32 matrix.
    pub dot_batch: fn(&[f32], &[f32], &mut [f32]),
}

static FORCE_SCALAR: AtomicBool = AtomicBool::new(false);
static SELECTED: OnceLock<Kernels> = OnceLock::new();

/// Forces the scalar kernels for the rest of the process. Must be called before the first
/// distance computation to take effect; returns whether it did.
pub fn force_scalar() -> bool {
    FORCE_SCALAR.store(true, Ordering::SeqCst);
    SELECTED.get().is_none()
}

/// The scalar function table.
pub const SCALAR: Kernels = Kernels {
    name: "scalar",
    l2_sq: scalar::l2_sq,
    dot: scalar::dot,
    l2_sq_u8_batch: scalar::l2_sq_u8_batch,
    dot_u8_batch: scalar::dot_u8_batch,
    l2_sq_batch: scalar::l2_sq_batch,
    dot_batch: scalar::dot_batch,
};

fn detect() -> Kernels {
    if FORCE_SCALAR.load(Ordering::SeqCst) {
        return SCALAR;
    }
    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("avx512f")
            && std::arch::is_x86_feature_detected!("avx512bw")
        {
            return x86::AVX512;
        }
        if std::arch::is_x86_feature_detected!("avx2") && std::arch::is_x86_feature_detected!("fma")
        {
            return x86::AVX2;
        }
    }
    SCALAR
}

/// The kernels selected for this process.
pub fn kernels() -> &'static Kernels {
    SELECTED.get_or_init(detect)
}

/// All levels available on this machine (for tests and benchmarks), scalar first.
pub fn available_levels() -> Vec<Kernels> {
    let mut v = vec![SCALAR];
    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("avx2") && std::arch::is_x86_feature_detected!("fma")
        {
            v.push(x86::AVX2);
        }
        if std::arch::is_x86_feature_detected!("avx512f")
            && std::arch::is_x86_feature_detected!("avx512bw")
        {
            v.push(x86::AVX512);
        }
    }
    v
}

/// Squared L2 distance using the selected kernels.
pub fn l2_sq(a: &[f32], b: &[f32]) -> f32 {
    (kernels().l2_sq)(a, b)
}

/// Dot product using the selected kernels.
pub fn dot(a: &[f32], b: &[f32]) -> f32 {
    (kernels().dot)(a, b)
}

/// Squared L2 from `query` to each row of `rows` (row-major, `rows.len() / query.len()` rows).
pub fn l2_sq_batch(query: &[f32], rows: &[f32], out: &mut [f32]) {
    (kernels().l2_sq_batch)(query, rows, out)
}

/// Dot from `query` to each row of `rows`.
pub fn dot_batch(query: &[f32], rows: &[f32], out: &mut [f32]) {
    (kernels().dot_batch)(query, rows, out)
}

/// Squared L2 from `query` to each SQ8 row.
pub fn l2_sq_u8_batch(query: &[f32], rows: &[u8], min: &[f32], scale: &[f32], out: &mut [f32]) {
    (kernels().l2_sq_u8_batch)(query, rows, min, scale, out)
}

/// Dot from `query` to each SQ8 row.
pub fn dot_u8_batch(query: &[f32], rows: &[u8], min: &[f32], scale: &[f32], out: &mut [f32]) {
    (kernels().dot_u8_batch)(query, rows, min, scale, out)
}

/// Per-dimension scalar quantization parameters for a matrix.
#[derive(Debug, Clone, PartialEq)]
pub struct Sq8Params {
    /// Per-dimension minimum.
    pub min: Vec<f32>,
    /// Per-dimension `(max - min) / 255`.
    pub scale: Vec<f32>,
}

impl Sq8Params {
    /// Fits parameters to `rows` (row-major with `dims` columns).
    pub fn fit(rows: &[f32], dims: usize) -> Self {
        let mut min = vec![f32::INFINITY; dims];
        let mut max = vec![f32::NEG_INFINITY; dims];
        for row in rows.chunks_exact(dims) {
            for d in 0..dims {
                min[d] = min[d].min(row[d]);
                max[d] = max[d].max(row[d]);
            }
        }
        let scale = (0..dims)
            .map(|d| {
                if !min[d].is_finite() {
                    min[d] = 0.0;
                    max[d] = 0.0;
                }
                let s = (max[d] - min[d]) / 255.0;
                if s > 0.0 { s } else { 1.0 }
            })
            .collect();
        Sq8Params { min, scale }
    }

    /// Quantizes one vector.
    pub fn quantize(&self, v: &[f32], out: &mut [u8]) {
        for d in 0..v.len() {
            let q = ((v[d] - self.min[d]) / self.scale[d]).round();
            out[d] = q.clamp(0.0, 255.0) as u8;
        }
    }

    /// Dequantizes one vector.
    pub fn dequantize(&self, q: &[u8], out: &mut [f32]) {
        for d in 0..q.len() {
            out[d] = self.min[d] + f32::from(q[d]) * self.scale[d];
        }
    }

    /// Encodes the parameters (min then scale, f32 little-endian).
    pub fn encode(&self, w: &mut cairn_core::codec::Writer) {
        w.u32(self.min.len() as u32);
        for x in &self.min {
            w.f32(*x);
        }
        for x in &self.scale {
            w.f32(*x);
        }
    }

    /// Decodes parameters.
    pub fn decode(r: &mut cairn_core::codec::Reader<'_>) -> cairn_core::Result<Self> {
        let n = r.u32()? as usize;
        if n > 1 << 16 {
            return Err(cairn_core::Error::corruption("sq8 dims"));
        }
        let mut min = Vec::with_capacity(n);
        for _ in 0..n {
            min.push(r.f32()?);
        }
        let mut scale = Vec::with_capacity(n);
        for _ in 0..n {
            let s = r.f32()?;
            if s <= 0.0 || !s.is_finite() {
                return Err(cairn_core::Error::corruption("sq8 scale"));
            }
            scale.push(s);
        }
        Ok(Sq8Params { min, scale })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn tol(a: &[f32], b: &[f32]) -> f32 {
        let na: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        let nb: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
        1e-4 * (1.0 + na * nb + na * na + nb * nb)
    }

    fn vec_strategy(dims: usize) -> impl Strategy<Value = Vec<f32>> {
        proptest::collection::vec(-100.0f32..100.0, dims)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(300))]
        #[test]
        fn every_level_matches_scalar(dims in 1usize..300, seed in any::<u64>()) {
            let mut r = cairn_core::SeededRng::from_seed(seed);
            let n = 1 + (r.next_u64() % 9) as usize;
            let mut rnd = || (r.next_u64() % 20001) as f32 / 100.0 - 100.0;
            let q: Vec<f32> = (0..dims).map(|_| rnd()).collect();
            let rows: Vec<f32> = (0..n * dims).map(|_| rnd()).collect();
            let params = Sq8Params::fit(&rows, dims);
            let mut q8 = vec![0u8; n * dims];
            for (i, row) in rows.chunks_exact(dims).enumerate() {
                params.quantize(row, &mut q8[i * dims..(i + 1) * dims]);
            }
            for k in available_levels() {
                for (i, row) in rows.chunks_exact(dims).enumerate() {
                    let t = tol(&q, row);
                    prop_assert!(((k.l2_sq)(&q, row) - scalar::l2_sq(&q, row)).abs() <= t, "{} l2 row {i}", k.name);
                    prop_assert!(((k.dot)(&q, row) - scalar::dot(&q, row)).abs() <= t, "{} dot row {i}", k.name);
                }
                let mut a = vec![0f32; n];
                let mut b = vec![0f32; n];
                (k.l2_sq_batch)(&q, &rows, &mut a);
                scalar::l2_sq_batch(&q, &rows, &mut b);
                for i in 0..n {
                    prop_assert!((a[i] - b[i]).abs() <= tol(&q, &rows[i * dims..(i + 1) * dims]), "{} l2 batch", k.name);
                }
                (k.dot_batch)(&q, &rows, &mut a);
                scalar::dot_batch(&q, &rows, &mut b);
                for i in 0..n {
                    prop_assert!((a[i] - b[i]).abs() <= tol(&q, &rows[i * dims..(i + 1) * dims]), "{} dot batch", k.name);
                }
                (k.l2_sq_u8_batch)(&q, &q8, &params.min, &params.scale, &mut a);
                scalar::l2_sq_u8_batch(&q, &q8, &params.min, &params.scale, &mut b);
                for i in 0..n {
                    prop_assert!((a[i] - b[i]).abs() <= tol(&q, &rows[i * dims..(i + 1) * dims]), "{} l2 u8", k.name);
                }
                (k.dot_u8_batch)(&q, &q8, &params.min, &params.scale, &mut a);
                scalar::dot_u8_batch(&q, &q8, &params.min, &params.scale, &mut b);
                for i in 0..n {
                    prop_assert!((a[i] - b[i]).abs() <= tol(&q, &rows[i * dims..(i + 1) * dims]), "{} dot u8", k.name);
                }
            }
        }

        #[test]
        fn sq8_roundtrip_error_is_bounded(dims in 1usize..64, v in vec_strategy(64)) {
            let v = &v[..dims];
            let rows: Vec<f32> = v.iter().chain(v.iter().map(|x| x * 0.5).collect::<Vec<_>>().iter()).copied().collect();
            let p = Sq8Params::fit(&rows, dims);
            let mut q = vec![0u8; dims];
            p.quantize(v, &mut q);
            let mut back = vec![0f32; dims];
            p.dequantize(&q, &mut back);
            for d in 0..dims {
                prop_assert!((back[d] - v[d]).abs() <= p.scale[d] * 0.51 + 1e-5);
            }
        }
    }

    #[test]
    fn selected_level_is_the_best_available() {
        let levels = available_levels();
        assert_eq!(kernels().name, levels.last().unwrap().name);
    }
}
