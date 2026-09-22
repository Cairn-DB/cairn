//! Readers for the texmex (`.fvecs`/`.ivecs`) and Big-ANN (`.fbin`/`.u8bin`/`.ibin`) formats.
#![allow(dead_code)] // u8bin/fbin/ibin readers are used by the YFCC sweep (M2.6)
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use anyhow::{Context, ensure};
use std::path::Path;

/// Row-major matrix of f32.
pub struct Matrix {
    /// Rows.
    pub n: usize,
    /// Columns.
    pub dims: usize,
    /// Values.
    pub data: Vec<f32>,
}

impl Matrix {
    /// Row `i`.
    pub fn row(&self, i: usize) -> &[f32] {
        &self.data[i * self.dims..(i + 1) * self.dims]
    }
}

/// Reads an `.fvecs` file (each row: `u32 d` then `d` f32), at most `limit` rows.
pub fn read_fvecs(path: &Path, limit: Option<usize>) -> anyhow::Result<Matrix> {
    let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    ensure!(bytes.len() >= 4, "empty fvecs");
    let dims = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    let row_bytes = 4 + 4 * dims;
    ensure!(
        bytes.len() % row_bytes == 0,
        "fvecs size not a multiple of the row size"
    );
    let n_all = bytes.len() / row_bytes;
    let n = limit.map_or(n_all, |l| l.min(n_all));
    let mut data = Vec::with_capacity(n * dims);
    for i in 0..n {
        let row = &bytes[i * row_bytes..(i + 1) * row_bytes];
        let d = u32::from_le_bytes([row[0], row[1], row[2], row[3]]) as usize;
        ensure!(d == dims, "inconsistent dims at row {i}");
        data.extend(
            row[4..]
                .chunks_exact(4)
                .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]])),
        );
    }
    Ok(Matrix { n, dims, data })
}

/// Reads an `.ivecs` file (each row: `u32 k` then `k` i32).
pub fn read_ivecs(path: &Path, limit: Option<usize>) -> anyhow::Result<Vec<Vec<u32>>> {
    let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    let mut out = Vec::new();
    let mut pos = 0;
    while pos + 4 <= bytes.len() && limit.is_none_or(|l| out.len() < l) {
        let k = u32::from_le_bytes([bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3]])
            as usize;
        pos += 4;
        ensure!(pos + 4 * k <= bytes.len(), "truncated ivecs");
        out.push(
            bytes[pos..pos + 4 * k]
                .chunks_exact(4)
                .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect(),
        );
        pos += 4 * k;
    }
    Ok(out)
}

/// Reads a Big-ANN `.u8bin` (`u32 n, u32 d, n*d u8`) into f32, at most `limit` rows.
pub fn read_u8bin(path: &Path, limit: Option<usize>) -> anyhow::Result<Matrix> {
    let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    ensure!(bytes.len() >= 8, "short u8bin");
    let n_all = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    let dims = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]) as usize;
    let n = limit.map_or(n_all, |l| l.min(n_all));
    ensure!(bytes.len() >= 8 + n * dims, "truncated u8bin");
    let data = bytes[8..8 + n * dims]
        .iter()
        .map(|&b| f32::from(b))
        .collect();
    Ok(Matrix { n, dims, data })
}

/// Reads a Big-ANN `.fbin` (`u32 n, u32 d, n*d f32`), at most `limit` rows.
pub fn read_fbin(path: &Path, limit: Option<usize>) -> anyhow::Result<Matrix> {
    let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    ensure!(bytes.len() >= 8, "short fbin");
    let n_all = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    let dims = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]) as usize;
    let n = limit.map_or(n_all, |l| l.min(n_all));
    ensure!(bytes.len() >= 8 + 4 * n * dims, "truncated fbin");
    let data = bytes[8..8 + 4 * n * dims]
        .chunks_exact(4)
        .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect();
    Ok(Matrix { n, dims, data })
}

/// Reads a Big-ANN ground-truth `.ibin` (`u32 n, u32 k, n*k i32 ids, n*k f32 dists`).
pub fn read_ibin_gt(path: &Path, limit: Option<usize>) -> anyhow::Result<Vec<Vec<u32>>> {
    let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    ensure!(bytes.len() >= 8, "short ibin");
    let n_all = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    let k = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]) as usize;
    let n = limit.map_or(n_all, |l| l.min(n_all));
    ensure!(bytes.len() >= 8 + 4 * n * k, "truncated ibin");
    Ok((0..n)
        .map(|i| {
            bytes[8 + 4 * i * k..8 + 4 * (i + 1) * k]
                .chunks_exact(4)
                .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect()
        })
        .collect())
}
