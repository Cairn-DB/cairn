//! Small k-means (Lloyd) to derive cluster labels for the correlated-filter benchmarks.

use cairn_core::SeededRng;
use cairn_index::kernels;

/// Assigns every row of `rows` (row-major, `dims` columns) to the nearest of `centroids`.
pub fn assign(rows: &[f32], dims: usize, centroids: &[f32]) -> Vec<u32> {
    let k = centroids.len() / dims;
    let mut out = Vec::with_capacity(rows.len() / dims);
    let mut d = vec![0f32; k];
    for row in rows.chunks_exact(dims) {
        kernels::l2_sq_batch(row, centroids, &mut d);
        let mut best = 0;
        for (i, &x) in d.iter().enumerate() {
            if x < d[best] {
                best = i;
            }
        }
        out.push(best as u32);
    }
    out
}

/// Fits `k` centroids on a seeded sample of `sample` rows with `iters` Lloyd iterations, then
/// labels every row. Deterministic.
pub fn kmeans_labels(
    rows: &[f32],
    dims: usize,
    k: usize,
    sample: usize,
    iters: usize,
    seed: u64,
) -> Vec<u32> {
    let n = rows.len() / dims;
    let mut rng = SeededRng::from_seed(seed);
    let sample_n = sample.min(n);
    let mut sample_rows = Vec::with_capacity(sample_n * dims);
    for _ in 0..sample_n {
        let i = rng.below(n as u64) as usize;
        sample_rows.extend_from_slice(&rows[i * dims..(i + 1) * dims]);
    }
    let mut centroids = Vec::with_capacity(k * dims);
    for _ in 0..k {
        let i = rng.below(sample_n as u64) as usize;
        centroids.extend_from_slice(&sample_rows[i * dims..(i + 1) * dims]);
    }
    for _ in 0..iters {
        let labels = assign(&sample_rows, dims, &centroids);
        let mut sums = vec![0f64; k * dims];
        let mut counts = vec![0u32; k];
        for (i, &l) in labels.iter().enumerate() {
            counts[l as usize] += 1;
            for d in 0..dims {
                sums[l as usize * dims + d] += f64::from(sample_rows[i * dims + d]);
            }
        }
        for c in 0..k {
            if counts[c] > 0 {
                for d in 0..dims {
                    centroids[c * dims + d] = (sums[c * dims + d] / f64::from(counts[c])) as f32;
                }
            } else {
                // Re-seed an empty cluster from a random sample row.
                let i = rng.below(sample_n as u64) as usize;
                centroids[c * dims..(c + 1) * dims]
                    .copy_from_slice(&sample_rows[i * dims..(i + 1) * dims]);
            }
        }
    }
    assign(rows, dims, &centroids)
}
