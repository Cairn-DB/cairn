//! In-memory vectors of one segment field: f32 rows plus an optional SQ8 copy.
//!
//! Distances are "lower is better" for every metric: squared L2 for `L2`, negated dot product
//! for `Dot` and `Cosine` (rows and queries are normalized for `Cosine`).

use crate::kernels::{self, Sq8Params};
use cairn_core::Metric;
use cairn_core::codec::{Reader, Writer};
use cairn_core::{Error, Result};

/// One field's vectors.
#[derive(Debug, Clone, PartialEq)]
pub struct Vectors {
    dims: usize,
    n: u32,
    metric: Metric,
    f32: Vec<f32>,
    sq8: Option<(Vec<u8>, Sq8Params)>,
}

fn normalize(v: &mut [f32]) {
    let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in v {
            *x /= norm;
        }
    }
}

impl Vectors {
    /// Wraps row-major `rows` (`n * dims` values), normalizing for `Cosine` and fitting SQ8 when
    /// `with_sq8`.
    pub fn from_rows(metric: Metric, dims: usize, mut rows: Vec<f32>, with_sq8: bool) -> Self {
        assert!(dims > 0 && rows.len() % dims == 0);
        let n = (rows.len() / dims) as u32;
        if metric == Metric::Cosine {
            for r in rows.chunks_exact_mut(dims) {
                normalize(r);
            }
        }
        let sq8 = with_sq8.then(|| {
            let params = Sq8Params::fit(&rows, dims);
            let mut q = vec![0u8; rows.len()];
            for (i, r) in rows.chunks_exact(dims).enumerate() {
                params.quantize(r, &mut q[i * dims..(i + 1) * dims]);
            }
            (q, params)
        });
        Vectors {
            dims,
            n,
            metric,
            f32: rows,
            sq8,
        }
    }

    /// Number of rows.
    pub fn len(&self) -> u32 {
        self.n
    }

    /// Whether there are no rows.
    pub fn is_empty(&self) -> bool {
        self.n == 0
    }

    /// Dimensionality.
    pub fn dims(&self) -> usize {
        self.dims
    }

    /// Metric.
    pub fn metric(&self) -> Metric {
        self.metric
    }

    /// Whether an SQ8 copy exists.
    pub fn has_sq8(&self) -> bool {
        self.sq8.is_some()
    }

    /// One row.
    pub fn row_f32(&self, row: u32) -> &[f32] {
        &self.f32[row as usize * self.dims..(row as usize + 1) * self.dims]
    }

    /// All rows, row-major.
    pub fn rows_f32(&self) -> &[f32] {
        &self.f32
    }

    /// Prepares a query (normalizes for `Cosine`).
    pub fn prepare_query(&self, q: &[f32]) -> Result<Vec<f32>> {
        if q.len() != self.dims {
            return Err(Error::InvalidRequest(format!(
                "query has {} dims, field has {}",
                q.len(),
                self.dims
            )));
        }
        let mut v = q.to_vec();
        if self.metric == Metric::Cosine {
            normalize(&mut v);
        }
        Ok(v)
    }

    fn finish_metric(&self, out: &mut [f32]) {
        if self.metric != Metric::L2 {
            for x in out {
                *x = -*x;
            }
        }
    }

    /// Distances from `q` to the given rows, using SQ8 when available unless `exact`.
    /// `gather_u8` / `gather_f32` are scratch buffers.
    pub fn distances_to(
        &self,
        q: &[f32],
        rows: &[u32],
        exact: bool,
        gather_u8: &mut Vec<u8>,
        gather_f32: &mut Vec<f32>,
        out: &mut [f32],
    ) {
        debug_assert_eq!(rows.len(), out.len());
        let d = self.dims;
        match (&self.sq8, exact) {
            (Some((q8, params)), false) => {
                gather_u8.clear();
                gather_u8.reserve(rows.len() * d);
                for &r in rows {
                    gather_u8.extend_from_slice(&q8[r as usize * d..(r as usize + 1) * d]);
                }
                match self.metric {
                    Metric::L2 => {
                        kernels::l2_sq_u8_batch(q, gather_u8, &params.min, &params.scale, out)
                    }
                    _ => kernels::dot_u8_batch(q, gather_u8, &params.min, &params.scale, out),
                }
            }
            _ => {
                gather_f32.clear();
                gather_f32.reserve(rows.len() * d);
                for &r in rows {
                    gather_f32.extend_from_slice(self.row_f32(r));
                }
                match self.metric {
                    Metric::L2 => kernels::l2_sq_batch(q, gather_f32, out),
                    _ => kernels::dot_batch(q, gather_f32, out),
                }
            }
        }
        self.finish_metric(out);
    }

    /// Distances from `q` to the contiguous rows `start..start + out.len()`, without gathering.
    pub fn distances_range(&self, q: &[f32], start: u32, exact: bool, out: &mut [f32]) {
        let d = self.dims;
        let (s, e) = (start as usize * d, (start as usize + out.len()) * d);
        match (&self.sq8, exact) {
            (Some((q8, params)), false) => match self.metric {
                Metric::L2 => {
                    kernels::l2_sq_u8_batch(q, &q8[s..e], &params.min, &params.scale, out)
                }
                _ => kernels::dot_u8_batch(q, &q8[s..e], &params.min, &params.scale, out),
            },
            _ => match self.metric {
                Metric::L2 => kernels::l2_sq_batch(q, &self.f32[s..e], out),
                _ => kernels::dot_batch(q, &self.f32[s..e], out),
            },
        }
        self.finish_metric(out);
    }

    /// Encodes the SQ8 copy (`params` then `n * dims` bytes), if any.
    pub fn encode_sq8(&self) -> Option<Vec<u8>> {
        let (q8, params) = self.sq8.as_ref()?;
        let mut w = Writer::with_capacity(q8.len() + 8 * self.dims + 16);
        params.encode(&mut w);
        w.u32(self.n).raw(q8);
        Some(w.into_vec())
    }

    /// Rebuilds from f32 rows and an encoded SQ8 section.
    pub fn from_rows_and_sq8(
        metric: Metric,
        dims: usize,
        rows: Vec<f32>,
        sq8: Option<&[u8]>,
    ) -> Result<Self> {
        let mut v = Vectors::from_rows(metric, dims, rows, false);
        if let Some(bytes) = sq8 {
            let mut r = Reader::new(bytes);
            let params = Sq8Params::decode(&mut r)?;
            if params.min.len() != dims {
                return Err(Error::corruption("sq8 dims mismatch"));
            }
            let n = r.u32()?;
            if n != v.n {
                return Err(Error::corruption("sq8 row count mismatch"));
            }
            let q8 = r.raw(n as usize * dims)?.to_vec();
            r.finish()?;
            v.sq8 = Some((q8, params));
        }
        Ok(v)
    }
}
