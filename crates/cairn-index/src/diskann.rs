//! Disk-resident vector index (ADR 0014): a Vamana graph whose node blocks (full vector plus
//! neighbor list, one page each) stay on disk, and product-quantized codes that stay in RAM.
//!
//! Search navigates with PQ distances computed from the codes; it only reads the blocks of the
//! nodes it expands, and ranks results by the exact distances those reads provide.
//!
//! Formats (both versioned, both validated before use):
//! - `pq.<field>`: magic, version, dims, m, dsub, ksub, centroids (f32), n, codes (`n * m` bytes).
//! - `vamana.<field>`: one header page (magic, version, dims, n, r, medoid, block, blocks per
//!   page, pages per block), then fixed-size blocks `[f32 × dims][u32 degree][u32 × r]`, packed
//!   so that no block straddles a page (or aligned to whole pages when larger than one).

use crate::bitmap::Bitmap;
use crate::kernels;
use crate::scan::TopK;
use bytes::Bytes;
use cairn_core::codec::{Reader, Writer};
use cairn_core::{Error, HashSet, Metric, Result, SeededRng};

const PAGE: usize = 4096;
const VAMANA_MAGIC: &[u8; 8] = b"CAIRNVAM";
const PQ_MAGIC: &[u8; 8] = b"CAIRNPQ1";
const VERSION: u32 = 1;
const NONE: u32 = u32::MAX;
const HEADER_FIELDS: usize = 8 + 4 * 8;

/// Build parameters.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VamanaParams {
    /// Maximum out-degree.
    pub r: u32,
    /// Candidate list size during build.
    pub l_build: u32,
    /// Pruning factor (> 1 keeps longer edges, which shortens search paths).
    pub alpha: f32,
    /// Rows sampled to train the PQ codebooks.
    pub pq_sample: u32,
    /// k-means iterations per PQ subspace.
    pub pq_iters: u32,
    /// Seed for PQ initialisation.
    pub seed: u64,
    /// Build passes: the first inserts rows with alpha = 1, later ones re-search every row on
    /// the full graph and re-prune with `alpha` (DiskANN's two-pass build). One pass is
    /// faster but leaves a few rows unreachable.
    pub passes: u32,
}

impl Default for VamanaParams {
    fn default() -> Self {
        VamanaParams {
            r: 48,
            l_build: 96,
            alpha: 1.2,
            pq_sample: 16_384,
            pq_iters: 6,
            seed: 0x5eed_da7a,
            passes: 2,
        }
    }
}

/// Per-query options.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DiskSearch {
    /// Results.
    pub k: usize,
    /// Candidate list size (the `ef` of graph search).
    pub l: usize,
    /// Scan path: candidates re-scored exactly (block reads).
    pub rerank: usize,
    /// Stop after this many expansions (0: no cap).
    pub max_expansions: usize,
    /// Nodes expanded per step; their blocks are prefetched together so the reads overlap.
    pub beam: usize,
}

fn l2(a: &[f32], b: &[f32]) -> f32 {
    kernels::l2_sq(a, b)
}

/// Exact distance under `metric`, lower is better (matches `Vectors`).
fn exact(metric: Metric, q: &[f32], v: &[f32]) -> f32 {
    match metric {
        Metric::L2 => kernels::l2_sq(q, v),
        _ => -kernels::dot(q, v),
    }
}

// ---------------------------------------------------------------------------------------------
// Product quantization
// ---------------------------------------------------------------------------------------------

/// Product quantizer: `m` subspaces of `dsub` dimensions, `ksub` centroids each.
#[derive(Debug, Clone, PartialEq)]
pub struct Pq {
    dims: usize,
    m: usize,
    dsub: usize,
    ksub: usize,
    /// `m * ksub * dsub` values.
    centroids: Vec<f32>,
}

impl Pq {
    /// Trains codebooks on (a deterministic sample of) `rows`.
    pub fn train(rows: &[f32], dims: usize, params: &VamanaParams) -> Pq {
        let n = rows.len() / dims;
        let dsub = if dims % 4 == 0 {
            4
        } else if dims % 2 == 0 {
            2
        } else {
            1
        };
        let m = dims / dsub;
        let sample: Vec<usize> = if n <= params.pq_sample as usize {
            (0..n).collect()
        } else {
            let step = n as f64 / f64::from(params.pq_sample);
            (0..params.pq_sample as usize)
                .map(|i| (i as f64 * step) as usize)
                .collect()
        };
        let ns = sample.len();
        let ksub = ns.clamp(1, 256);
        let mut centroids = vec![0f32; m * ksub * dsub];
        if ns == 0 {
            return Pq {
                dims,
                m,
                dsub,
                ksub,
                centroids,
            };
        }
        let rng = SeededRng::from_seed(params.seed);
        let mut data = vec![0f32; ns * dsub];
        let mut assign = vec![0usize; ns];
        for j in 0..m {
            for (si, &r) in sample.iter().enumerate() {
                data[si * dsub..(si + 1) * dsub]
                    .copy_from_slice(&rows[r * dims + j * dsub..r * dims + (j + 1) * dsub]);
            }
            // Initial centroids: a seeded permutation of the sample.
            let mut order: Vec<usize> = (0..ns).collect();
            let mut r = rng.fork(&format!("pq{j}"));
            for i in (1..ns).rev() {
                let k = r.below(i as u64 + 1) as usize;
                order.swap(i, k);
            }
            let cj = &mut centroids[j * ksub * dsub..(j + 1) * ksub * dsub];
            for c in 0..ksub {
                cj[c * dsub..(c + 1) * dsub]
                    .copy_from_slice(&data[order[c] * dsub..(order[c] + 1) * dsub]);
            }
            for _ in 0..params.pq_iters {
                for si in 0..ns {
                    assign[si] = nearest(cj, dsub, ksub, &data[si * dsub..(si + 1) * dsub]);
                }
                let mut sums = vec![0f64; ksub * dsub];
                let mut counts = vec![0u32; ksub];
                for si in 0..ns {
                    let c = assign[si];
                    counts[c] += 1;
                    for d in 0..dsub {
                        sums[c * dsub + d] += f64::from(data[si * dsub + d]);
                    }
                }
                for c in 0..ksub {
                    if counts[c] > 0 {
                        for d in 0..dsub {
                            cj[c * dsub + d] = (sums[c * dsub + d] / f64::from(counts[c])) as f32;
                        }
                    }
                }
            }
        }
        Pq {
            dims,
            m,
            dsub,
            ksub,
            centroids,
        }
    }

    /// Bytes per code.
    pub fn code_len(&self) -> usize {
        self.m
    }

    /// Encodes one row.
    pub fn encode_into(&self, row: &[f32], out: &mut [u8]) {
        for j in 0..self.m {
            let cj = &self.centroids[j * self.ksub * self.dsub..(j + 1) * self.ksub * self.dsub];
            out[j] = nearest(
                cj,
                self.dsub,
                self.ksub,
                &row[j * self.dsub..(j + 1) * self.dsub],
            ) as u8;
        }
    }

    /// Distance table for `q`: `m * ksub` partial distances, lower is better.
    pub fn table(&self, metric: Metric, q: &[f32]) -> Vec<f32> {
        let mut t = vec![0f32; self.m * self.ksub];
        for j in 0..self.m {
            let qs = &q[j * self.dsub..(j + 1) * self.dsub];
            for c in 0..self.ksub {
                let off = (j * self.ksub + c) * self.dsub;
                let cv = &self.centroids[off..off + self.dsub];
                t[j * self.ksub + c] = match metric {
                    Metric::L2 => qs.iter().zip(cv).map(|(a, b)| (a - b) * (a - b)).sum(),
                    _ => -qs.iter().zip(cv).map(|(a, b)| a * b).sum::<f32>(),
                };
            }
        }
        t
    }

    /// Asymmetric distance from a table to one code.
    #[inline]
    pub fn adc(&self, table: &[f32], code: &[u8]) -> f32 {
        let mut s = 0f32;
        for (j, &c) in code.iter().enumerate() {
            s += table[j * self.ksub + c as usize];
        }
        s
    }

    fn encode_section(&self, codes: &[u8], n: u32) -> Vec<u8> {
        let mut w = Writer::with_capacity(64 + self.centroids.len() * 4 + codes.len());
        w.raw(PQ_MAGIC)
            .u32(VERSION)
            .u32(self.dims as u32)
            .u32(self.m as u32)
            .u32(self.dsub as u32)
            .u32(self.ksub as u32);
        for &c in &self.centroids {
            w.f32(c);
        }
        w.u32(n).raw(codes);
        w.into_vec()
    }

    fn decode_section(bytes: &[u8], dims: usize, n: u32) -> Result<(Pq, Vec<u8>)> {
        let mut r = Reader::new(bytes);
        if r.raw(8)? != PQ_MAGIC {
            return Err(Error::corruption("pq: bad magic"));
        }
        let version = r.u32()?;
        if version != VERSION {
            return Err(Error::UnsupportedVersion {
                found: version,
                supported: VERSION,
            });
        }
        let (d, m, dsub, ksub) = (
            r.u32()? as usize,
            r.u32()? as usize,
            r.u32()? as usize,
            r.u32()? as usize,
        );
        if d != dims || dsub == 0 || m * dsub != d || ksub == 0 || ksub > 256 {
            return Err(Error::corruption("pq: inconsistent shape"));
        }
        let nc = m * ksub * dsub;
        if r.remaining() < nc * 4 {
            return Err(Error::corruption("pq: truncated centroids"));
        }
        let mut centroids = Vec::with_capacity(nc);
        for _ in 0..nc {
            centroids.push(r.f32()?);
        }
        if r.u32()? != n {
            return Err(Error::corruption("pq: row count mismatch"));
        }
        let codes = r.raw(n as usize * m)?.to_vec();
        r.finish()?;
        if codes.iter().any(|&c| c as usize >= ksub) {
            return Err(Error::corruption("pq: code out of range"));
        }
        Ok((
            Pq {
                dims,
                m,
                dsub,
                ksub,
                centroids,
            },
            codes,
        ))
    }
}

fn nearest(centroids: &[f32], dsub: usize, ksub: usize, x: &[f32]) -> usize {
    let mut best = (f32::INFINITY, 0usize);
    for c in 0..ksub {
        let cv = &centroids[c * dsub..(c + 1) * dsub];
        let d: f32 = x.iter().zip(cv).map(|(a, b)| (a - b) * (a - b)).sum();
        if d < best.0 {
            best = (d, c);
        }
    }
    best.1
}

// ---------------------------------------------------------------------------------------------
// Vamana graph build
// ---------------------------------------------------------------------------------------------

/// Sorted candidate list of bounded size with an "expanded" flag per entry.
struct Candidates {
    cap: usize,
    items: Vec<(f32, u32, bool)>,
}

impl Candidates {
    fn new(cap: usize) -> Self {
        Candidates {
            cap: cap.max(1),
            items: Vec::with_capacity(cap + 1),
        }
    }

    fn insert(&mut self, d: f32, row: u32) {
        if self.items.len() >= self.cap && self.items.last().is_some_and(|w| (d, row) >= (w.0, w.1))
        {
            return;
        }
        let pos = self
            .items
            .partition_point(|x| x.0 < d || (x.0 == d && x.1 < row));
        self.items.insert(pos, (d, row, false));
        self.items.truncate(self.cap);
    }

    /// Best unexpanded entry, marked expanded.
    fn next(&mut self) -> Option<(f32, u32)> {
        let e = self.items.iter_mut().find(|e| !e.2)?;
        e.2 = true;
        Some((e.0, e.1))
    }

    /// Up to `w` best unexpanded rows, marked expanded.
    fn next_beam(&mut self, w: usize, out: &mut Vec<u32>) {
        out.clear();
        for e in self.items.iter_mut() {
            if out.len() >= w {
                break;
            }
            if !e.2 {
                e.2 = true;
                out.push(e.1);
            }
        }
    }
}

/// Visited marks for build-time searches (an epoch per search avoids clearing).
struct Visited {
    marks: Vec<u32>,
    epoch: u32,
}

impl Visited {
    fn next(&mut self) {
        self.epoch += 1;
    }

    /// Marks `row`; returns whether it was unmarked in this epoch.
    fn mark(&mut self, row: u32) -> bool {
        let m = &mut self.marks[row as usize];
        let fresh = *m != self.epoch;
        *m = self.epoch;
        fresh
    }
}

/// Greedy search over the in-RAM graph (build time); returns the expanded nodes with their
/// distances to `q`.
fn greedy_ram(
    (vectors, dims): (&[f32], usize),
    adj: &[Vec<u32>],
    start: u32,
    q: &[f32],
    l: usize,
    visited: &mut Visited,
) -> Vec<(f32, u32)> {
    let row = |i: u32| &vectors[i as usize * dims..(i as usize + 1) * dims];
    let mut cand = Candidates::new(l);
    visited.next();
    visited.mark(start);
    cand.insert(l2(q, row(start)), start);
    let mut expanded = Vec::new();
    while let Some((d, p)) = cand.next() {
        expanded.push((d, p));
        for &nb in &adj[p as usize] {
            if visited.mark(nb) {
                cand.insert(l2(q, row(nb)), nb);
            }
        }
    }
    expanded
}

/// Robust prune (DiskANN): keeps at most `r` candidates, dropping any candidate that a kept
/// neighbor already covers (`alpha * d(kept, c) <= d(p, c)`, on distances, not squares).
fn robust_prune(
    vectors: &[f32],
    dims: usize,
    p: u32,
    mut cand: Vec<(f32, u32)>,
    alpha: f32,
    r: usize,
) -> Vec<u32> {
    let row = |i: u32| &vectors[i as usize * dims..(i as usize + 1) * dims];
    cand.retain(|c| c.1 != p);
    cand.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
    cand.dedup_by_key(|c| c.1);
    let a2 = alpha * alpha;
    let mut kept: Vec<u32> = Vec::with_capacity(r);
    for (d, c) in cand {
        if kept.len() >= r {
            break;
        }
        let covered = kept.iter().any(|&k| a2 * l2(row(k), row(c)) <= d);
        if !covered {
            kept.push(c);
        }
    }
    kept
}

/// Rows inserted one at a time in the first pass before batching starts (ADR 0019).
const SEED_ROWS: usize = 1024;
/// Largest batch, and at most a 32nd of the rows already linked in the first pass. Vamana
/// relies more than HNSW on each row seeing the edges added just before it: batches of up to
/// 1024 (an eighth) lost 5 points of graph recall in the unit test, 256 (a 32nd) lost none.
const MAX_BATCH: usize = 256;

/// Batched Vamana build (ADR 0019): same passes, alphas, slack and final pruning as the
/// row-at-a-time build, but each batch searches the graph as it was at the batch start (plus
/// the batch's earlier rows by exact distance) on `par`, and back edges merge once per target.
/// The result depends only on the input, not on the thread count.
fn build_graph_parallel(
    vectors: &[f32],
    dims: usize,
    params: &VamanaParams,
    par: &dyn cairn_core::Parallel,
) -> (u32, Vec<Vec<u32>>) {
    use std::sync::Mutex;
    let n = vectors.len() / dims;
    if n == 0 {
        return (0, Vec::new());
    }
    let row = |i: usize| &vectors[i * dims..(i + 1) * dims];
    let mut mean = vec![0f64; dims];
    for i in 0..n {
        for (m, &x) in mean.iter_mut().zip(row(i)) {
            *m += f64::from(x);
        }
    }
    let mean: Vec<f32> = mean.iter().map(|m| (m / n as f64) as f32).collect();
    let medoid = (0..n)
        .map(|i| (l2(&mean, row(i)), i))
        .min_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)))
        .map_or(0, |x| x.1) as u32;
    let r = params.r as usize;
    let slack = r + r / 3;
    let l_build = params.l_build as usize;
    let mut adj: Vec<Vec<u32>> = vec![Vec::new(); n];
    let visited: Vec<Mutex<Visited>> = (0..par.threads().max(1))
        .map(|_| {
            Mutex::new(Visited {
                marks: vec![0u32; n],
                epoch: 0,
            })
        })
        .collect();
    let order: Vec<u32> = (0..n as u32).collect();
    for pass in 0..params.passes.max(1) {
        let alpha = if pass == 0 && params.passes > 1 {
            1.0
        } else {
            params.alpha
        };
        let rows: Vec<u32> = order
            .iter()
            .copied()
            .filter(|&p| !(p == medoid && pass == 0))
            .collect();
        let mut done = 0usize;
        while done < rows.len() {
            let size = if pass == 0 {
                if done < SEED_ROWS {
                    1
                } else {
                    (done / 32).clamp(1, MAX_BATCH)
                }
            } else {
                MAX_BATCH
            }
            .min(rows.len() - done);
            let batch = &rows[done..done + size];
            // 1. Candidates and pruned out-lists against the frozen graph.
            let outs: Vec<Mutex<Vec<u32>>> = (0..size).map(|_| Mutex::new(Vec::new())).collect();
            let frozen = &adj;
            par.run(size, &|w, i| {
                let p = batch[i];
                let q = row(p as usize);
                let mut vis = visited[w].lock().expect("visited");
                let mut cand = greedy_ram((vectors, dims), frozen, medoid, q, l_build, &mut vis);
                cand.extend(
                    frozen[p as usize]
                        .iter()
                        .map(|&c| (l2(q, row(c as usize)), c)),
                );
                cand.extend(batch[..i].iter().map(|&e| (l2(q, row(e as usize)), e)));
                *outs[i].lock().expect("out") = robust_prune(vectors, dims, p, cand, alpha, r);
            });
            // 2. Out-lists in row order; back edges grouped by target.
            let mut back: Vec<(u32, u32)> = Vec::new();
            for (i, out) in outs.into_iter().enumerate() {
                let p = batch[i];
                let out = out.into_inner().expect("out");
                back.extend(out.iter().map(|&j| (j, p)));
                adj[p as usize] = out;
            }
            back.sort_unstable();
            back.dedup();
            let mut groups: Vec<(usize, usize)> = Vec::new();
            let mut g0 = 0;
            for k in 1..=back.len() {
                if k == back.len() || back[k].0 != back[g0].0 {
                    groups.push((g0, k));
                    g0 = k;
                }
            }
            // 3. Each target takes its new back edges once, pruned past the slack.
            let merged: Vec<Mutex<Option<Vec<u32>>>> =
                (0..groups.len()).map(|_| Mutex::new(None)).collect();
            let current = &adj;
            par.run(groups.len(), &|_, g| {
                let (a, b) = groups[g];
                let j = back[a].0;
                let mut list = current[j as usize].clone();
                for &(_, p) in &back[a..b] {
                    if !list.contains(&p) {
                        list.push(p);
                    }
                }
                if list.len() > slack {
                    let cand: Vec<(f32, u32)> = list
                        .iter()
                        .map(|&c| (l2(row(j as usize), row(c as usize)), c))
                        .collect();
                    list = robust_prune(vectors, dims, j, cand, alpha, r);
                }
                *merged[g].lock().expect("merged") = Some(list);
            });
            for (g, list) in merged.into_iter().enumerate() {
                let j = back[groups[g].0].0;
                adj[j as usize] = list.into_inner().expect("merged").expect("set");
            }
            done += size;
        }
    }
    for (j, list) in adj.iter_mut().enumerate() {
        if list.len() > r {
            let cand: Vec<(f32, u32)> = list
                .iter()
                .map(|&c| (l2(row(j), row(c as usize)), c))
                .collect();
            *list = robust_prune(vectors, dims, j as u32, cand, params.alpha, r);
        }
    }
    (medoid, adj)
}

/// Builds the graph over row-major `vectors` (already normalized for cosine). Rows are
/// inserted in order; ties break on row ids, so the result depends only on the input.
/// Returns the medoid and the adjacency lists. The row-at-a-time reference for
/// [`build_graph_parallel`] (tests compare their recall).
#[cfg(test)]
fn build_graph(vectors: &[f32], dims: usize, params: &VamanaParams) -> (u32, Vec<Vec<u32>>) {
    let n = vectors.len() / dims;
    if n == 0 {
        return (0, Vec::new());
    }
    let row = |i: usize| &vectors[i * dims..(i + 1) * dims];
    let mut mean = vec![0f64; dims];
    for i in 0..n {
        for (m, &x) in mean.iter_mut().zip(row(i)) {
            *m += f64::from(x);
        }
    }
    let mean: Vec<f32> = mean.iter().map(|m| (m / n as f64) as f32).collect();
    let medoid = (0..n)
        .map(|i| (l2(&mean, row(i)), i))
        .min_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)))
        .map_or(0, |x| x.1) as u32;
    let r = params.r as usize;
    // Back edges may overfill a list by this slack before it is pruned back to `r`, which
    // amortises the pruning cost (FreshDiskANN's slack factor).
    let slack = r + r / 3;
    let mut adj: Vec<Vec<u32>> = vec![Vec::new(); n];
    let mut visited = Visited {
        marks: vec![0u32; n],
        epoch: 0,
    };
    for pass in 0..params.passes.max(1) {
        let alpha = if pass == 0 && params.passes > 1 {
            1.0
        } else {
            params.alpha
        };
        for p in 0..n as u32 {
            if p == medoid && pass == 0 {
                continue;
            }
            let q = row(p as usize).to_vec();
            let mut cand = greedy_ram(
                (vectors, dims),
                &adj,
                medoid,
                &q,
                params.l_build as usize,
                &mut visited,
            );
            cand.extend(
                adj[p as usize]
                    .iter()
                    .map(|&c| (l2(&q, row(c as usize)), c)),
            );
            let out = robust_prune(vectors, dims, p, cand, alpha, r);
            for &j in &out {
                let list = &mut adj[j as usize];
                if list.contains(&p) {
                    continue;
                }
                list.push(p);
                if list.len() > slack {
                    let cand: Vec<(f32, u32)> = list
                        .iter()
                        .map(|&c| (l2(row(j as usize), row(c as usize)), c))
                        .collect();
                    adj[j as usize] = robust_prune(vectors, dims, j, cand, alpha, r);
                }
            }
            adj[p as usize] = out;
        }
    }
    for (j, list) in adj.iter_mut().enumerate() {
        if list.len() > r {
            let cand: Vec<(f32, u32)> = list
                .iter()
                .map(|&c| (l2(row(j), row(c as usize)), c))
                .collect();
            *list = robust_prune(vectors, dims, j as u32, cand, params.alpha, r);
        }
    }
    (medoid, adj)
}

// ---------------------------------------------------------------------------------------------
// Block layout
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq)]
struct Layout {
    dims: usize,
    n: usize,
    r: usize,
    block: usize,
    /// Blocks per page (0 when a block spans several pages).
    bpp: usize,
    /// Pages per block (when `bpp == 0`).
    ppb: usize,
}

impl Layout {
    fn new(dims: usize, n: usize, r: usize) -> Layout {
        let block = dims * 4 + 4 + r * 4;
        if block <= PAGE {
            Layout {
                dims,
                n,
                r,
                block,
                bpp: PAGE / block,
                ppb: 0,
            }
        } else {
            Layout {
                dims,
                n,
                r,
                block,
                bpp: 0,
                ppb: block.div_ceil(PAGE),
            }
        }
    }

    fn offset(&self, row: usize) -> usize {
        match row.checked_div(self.bpp) {
            Some(page) => PAGE + page * PAGE + (row % self.bpp) * self.block,
            None => PAGE + row * self.ppb * PAGE,
        }
    }

    fn total(&self) -> usize {
        if self.bpp > 0 {
            PAGE + self.n.div_ceil(self.bpp) * PAGE
        } else {
            PAGE + self.n * self.ppb * PAGE
        }
    }
}

// ---------------------------------------------------------------------------------------------
// The index
// ---------------------------------------------------------------------------------------------

enum Graph {
    /// Just built: vectors and lists in RAM (the build side and tests).
    Ram {
        vectors: Vec<f32>,
        adj: Vec<Vec<u32>>,
    },
    /// Loaded: blocks in a mapped section.
    Mapped { bytes: Bytes, layout: Layout },
}

/// Disk-resident index of one segment's vector field.
pub struct DiskAnn {
    metric: Metric,
    dims: usize,
    n: u32,
    r: usize,
    medoid: u32,
    pq: Pq,
    codes: Vec<u8>,
    graph: Graph,
    prefetch: Option<fn(&[u8])>,
}

/// Reusable per-query buffers.
#[derive(Default)]
pub struct DiskScratch {
    vec: Vec<f32>,
    nbrs: Vec<u32>,
}

impl DiskAnn {
    /// Builds from row-major `vectors` (normalized for cosine). The build keeps everything in
    /// RAM; [`DiskAnn::sections`] produces the on-disk form.
    pub fn build(metric: Metric, dims: usize, vectors: Vec<f32>, params: &VamanaParams) -> Self {
        Self::build_with(metric, dims, vectors, params, &cairn_core::Sequential)
    }

    /// Like [`DiskAnn::build`], with the graph built on `par` (ADR 0019); the result does not
    /// depend on the thread count.
    pub fn build_with(
        metric: Metric,
        dims: usize,
        vectors: Vec<f32>,
        params: &VamanaParams,
        par: &dyn cairn_core::Parallel,
    ) -> Self {
        let n = vectors.len() / dims;
        let pq = Pq::train(&vectors, dims, params);
        let mut codes = vec![0u8; n * pq.code_len()];
        for i in 0..n {
            pq.encode_into(
                &vectors[i * dims..(i + 1) * dims],
                &mut codes[i * pq.m..(i + 1) * pq.m],
            );
        }
        let (medoid, adj) = build_graph_parallel(&vectors, dims, params, par);
        DiskAnn {
            metric,
            dims,
            n: n as u32,
            r: params.r as usize,
            medoid,
            pq,
            codes,
            graph: Graph::Ram { vectors, adj },
            prefetch: None,
        }
    }

    /// Rows.
    pub fn len(&self) -> u32 {
        self.n
    }

    /// Whether empty.
    pub fn is_empty(&self) -> bool {
        self.n == 0
    }

    /// Bytes held in RAM (codes and codebooks), for accounting.
    pub fn resident_bytes(&self) -> usize {
        self.codes.len() + self.pq.centroids.len() * 4
    }

    /// `(pq section, vamana section)` payloads.
    pub fn sections(&self) -> (Vec<u8>, Vec<u8>) {
        let pq = self.pq.encode_section(&self.codes, self.n);
        let layout = Layout::new(self.dims, self.n as usize, self.r);
        let mut out = vec![0u8; layout.total()];
        let mut w = Writer::with_capacity(HEADER_FIELDS);
        w.raw(VAMANA_MAGIC)
            .u32(VERSION)
            .u32(self.dims as u32)
            .u32(self.n)
            .u32(self.r as u32)
            .u32(self.medoid)
            .u32(layout.block as u32)
            .u32(layout.bpp as u32)
            .u32(layout.ppb as u32);
        let h = w.into_vec();
        out[..h.len()].copy_from_slice(&h);
        let mut scratch = DiskScratch::default();
        for row in 0..self.n {
            self.node(row, &mut scratch);
            let off = layout.offset(row as usize);
            let b = &mut out[off..off + layout.block];
            for (i, x) in scratch.vec.iter().enumerate() {
                b[i * 4..i * 4 + 4].copy_from_slice(&x.to_le_bytes());
            }
            let base = self.dims * 4;
            b[base..base + 4].copy_from_slice(&(scratch.nbrs.len() as u32).to_le_bytes());
            for i in 0..self.r {
                let v = scratch.nbrs.get(i).copied().unwrap_or(NONE);
                b[base + 4 + i * 4..base + 8 + i * 4].copy_from_slice(&v.to_le_bytes());
            }
        }
        (pq, out)
    }

    /// Loads from a PQ section (read into RAM) and a mapped Vamana section. Validates the
    /// header, the layout and every neighbor id, so a damaged section fails here and never
    /// during a search.
    pub fn load(metric: Metric, dims: usize, n: u32, pq: &[u8], vamana: Bytes) -> Result<Self> {
        Self::load_with(metric, dims, n, pq, vamana, None)
    }

    /// [`DiskAnn::load`] with the runtime's readahead hint (see `Disk::prefetcher`), used to
    /// overlap the block reads of one beam step.
    pub fn load_with(
        metric: Metric,
        dims: usize,
        n: u32,
        pq: &[u8],
        vamana: Bytes,
        prefetch: Option<fn(&[u8])>,
    ) -> Result<Self> {
        let (pq, codes) = Pq::decode_section(pq, dims, n)?;
        if vamana.len() < PAGE {
            return Err(Error::corruption("vamana: short header"));
        }
        let mut r = Reader::new(&vamana[..HEADER_FIELDS]);
        if r.raw(8)? != VAMANA_MAGIC {
            return Err(Error::corruption("vamana: bad magic"));
        }
        let version = r.u32()?;
        if version != VERSION {
            return Err(Error::UnsupportedVersion {
                found: version,
                supported: VERSION,
            });
        }
        let (d, nn, rr, medoid, block, bpp, ppb) = (
            r.u32()? as usize,
            r.u32()?,
            r.u32()? as usize,
            r.u32()?,
            r.u32()? as usize,
            r.u32()? as usize,
            r.u32()? as usize,
        );
        let layout = Layout::new(d, nn as usize, rr);
        if d != dims
            || nn != n
            || rr == 0
            || rr > 1024
            || (n > 0 && medoid >= n)
            || layout.block != block
            || layout.bpp != bpp
            || layout.ppb != ppb
            || vamana.len() < layout.total()
        {
            return Err(Error::corruption("vamana: inconsistent header"));
        }
        let idx = DiskAnn {
            metric,
            dims,
            n,
            r: rr,
            medoid,
            pq,
            codes,
            graph: Graph::Mapped {
                bytes: vamana,
                layout,
            },
            prefetch,
        };
        let mut s = DiskScratch::default();
        for row in 0..n {
            if !idx.node_checked(row, &mut s) {
                return Err(Error::corruption(format!("vamana: bad block {row}")));
            }
        }
        Ok(idx)
    }

    /// Reads a node's vector and neighbors into the scratch (one block read when mapped).
    fn node(&self, row: u32, s: &mut DiskScratch) {
        let ok = self.node_checked(row, s);
        debug_assert!(ok, "validated at load");
    }

    fn node_checked(&self, row: u32, s: &mut DiskScratch) -> bool {
        s.vec.clear();
        s.nbrs.clear();
        match &self.graph {
            Graph::Ram { vectors, adj } => {
                let r = row as usize;
                s.vec
                    .extend_from_slice(&vectors[r * self.dims..(r + 1) * self.dims]);
                s.nbrs.extend_from_slice(&adj[r]);
                true
            }
            Graph::Mapped { bytes, layout } => {
                let off = layout.offset(row as usize);
                let b = &bytes[off..off + layout.block];
                s.vec.extend(
                    b[..self.dims * 4]
                        .chunks_exact(4)
                        .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]])),
                );
                let base = self.dims * 4;
                let deg = u32::from_le_bytes(b[base..base + 4].try_into().expect("4 bytes"));
                if deg as usize > self.r {
                    return false;
                }
                for i in 0..deg as usize {
                    let o = base + 4 + i * 4;
                    let v = u32::from_le_bytes(b[o..o + 4].try_into().expect("4 bytes"));
                    if v >= self.n {
                        return false;
                    }
                    s.nbrs.push(v);
                }
                true
            }
        }
    }

    /// Starts reading the blocks of `rows` (no-op for RAM graphs or without a prefetcher).
    fn prefetch_rows(&self, rows: &[u32]) {
        if let (Some(f), Graph::Mapped { bytes, layout }) = (self.prefetch, &self.graph) {
            for &r in rows {
                let off = layout.offset(r as usize);
                f(&bytes[off..off + layout.block]);
            }
        }
    }

    fn seeds(&self) -> impl Iterator<Item = u32> + '_ {
        const SEEDS: u32 = 64;
        let step = (self.n / SEEDS).max(1);
        (0..self.n.min(SEEDS)).map(move |i| i * step)
    }

    fn code(&self, row: u32) -> &[u8] {
        let m = self.pq.m;
        &self.codes[row as usize * m..(row as usize + 1) * m]
    }

    /// Graph search: beam search on PQ distances, results ranked by exact distances of the
    /// expanded nodes. Rows outside `filter` are traversed but never returned.
    pub fn search_graph(
        &self,
        q: &[f32],
        filter: Option<&Bitmap>,
        opts: DiskSearch,
        s: &mut DiskScratch,
    ) -> Vec<(f32, u32)> {
        if self.n == 0 || opts.k == 0 {
            return Vec::new();
        }
        let table = self.pq.table(self.metric, q);
        let mut cand = Candidates::new(opts.l.max(opts.k));
        let mut visited: HashSet<u32> = HashSet::default();
        // Seeds: the medoid plus evenly spaced rows scored by PQ (RAM only). A single entry
        // point cannot reach a component that pruning cut off (very clustered data); a spread
        // of seeds starts the search near the query wherever it lies.
        for seed in std::iter::once(self.medoid).chain(self.seeds()) {
            if visited.insert(seed) {
                cand.insert(self.pq.adc(&table, self.code(seed)), seed);
            }
        }
        let mut top = TopK::new(opts.k);
        let mut expansions = 0usize;
        let mut beam = Vec::with_capacity(opts.beam.max(1));
        let mut found: Vec<u32> = Vec::new();
        loop {
            cand.next_beam(opts.beam.max(1), &mut beam);
            if beam.is_empty() {
                break;
            }
            self.prefetch_rows(&beam);
            found.clear();
            for &p in &beam {
                self.node(p, s);
                if filter.is_none_or(|f| f.contains(p)) {
                    top.push(exact(self.metric, q, &s.vec), p);
                }
                found.extend(s.nbrs.iter().copied().filter(|nb| visited.insert(*nb)));
            }
            for &nb in &found {
                cand.insert(self.pq.adc(&table, self.code(nb)), nb);
            }
            expansions += beam.len();
            if opts.max_expansions > 0 && expansions >= opts.max_expansions {
                break;
            }
        }
        top.into_sorted()
    }

    /// Scan: PQ distances over the rows of `filter` (all rows when `None`), then the best
    /// `rerank` re-scored exactly from their blocks.
    pub fn search_scan(
        &self,
        q: &[f32],
        filter: Option<&Bitmap>,
        opts: DiskSearch,
        s: &mut DiskScratch,
    ) -> Vec<(f32, u32)> {
        if self.n == 0 || opts.k == 0 {
            return Vec::new();
        }
        let table = self.pq.table(self.metric, q);
        let mut first = TopK::new(opts.rerank.max(opts.k));
        match filter {
            Some(f) => {
                for row in f.iter() {
                    if row < self.n {
                        first.push(self.pq.adc(&table, self.code(row)), row);
                    }
                }
            }
            None => {
                for row in 0..self.n {
                    first.push(self.pq.adc(&table, self.code(row)), row);
                }
            }
        }
        let rows: Vec<u32> = first.into_sorted().into_iter().map(|x| x.1).collect();
        self.prefetch_rows(&rows);
        let mut top = TopK::new(opts.k);
        for row in rows {
            self.node(row, s);
            top.push(exact(self.metric, q, &s.vec), row);
        }
        top.into_sorted()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn blobs(seed: u64, n: usize, dims: usize, clusters: usize) -> Vec<f32> {
        let mut r = SeededRng::from_seed(seed);
        let mut unit = || r.unit_f64() as f32 * 2.0 - 1.0;
        let centers: Vec<f32> = (0..clusters * dims).map(|_| unit() * 10.0).collect();
        let mut rows = Vec::with_capacity(n * dims);
        for i in 0..n {
            let c = i % clusters;
            for d in 0..dims {
                rows.push(centers[c * dims + d] + unit() * 2.0);
            }
        }
        rows
    }

    fn brute(v: &[f32], dims: usize, q: &[f32], k: usize, f: Option<&Bitmap>) -> Vec<u32> {
        let mut top = TopK::new(k);
        for i in 0..v.len() / dims {
            if f.is_none_or(|f| f.contains(i as u32)) {
                top.push(l2(q, &v[i * dims..(i + 1) * dims]), i as u32);
            }
        }
        top.into_sorted().into_iter().map(|x| x.1).collect()
    }

    fn recall(got: &[(f32, u32)], truth: &[u32]) -> f64 {
        got.iter().filter(|g| truth.contains(&g.1)).count() as f64 / truth.len() as f64
    }

    fn opts(k: usize) -> DiskSearch {
        DiskSearch {
            k,
            l: 100,
            rerank: 50,
            max_expansions: 0,
            beam: 4,
        }
    }

    fn small() -> VamanaParams {
        VamanaParams {
            r: 24,
            l_build: 48,
            ..VamanaParams::default()
        }
    }

    /// Queries near stored rows (in distribution), perturbed.
    fn near(v: &[f32], dims: usize, seed: u64, count: usize) -> Vec<f32> {
        let mut r = SeededRng::from_seed(seed);
        let n = v.len() / dims;
        let mut out = Vec::with_capacity(count * dims);
        for _ in 0..count {
            let i = r.below(n as u64) as usize;
            out.extend(
                v[i * dims..(i + 1) * dims]
                    .iter()
                    .map(|x| x + (r.unit_f64() as f32 - 0.5)),
            );
        }
        out
    }

    fn mapped(idx: &DiskAnn) -> DiskAnn {
        let (pq, vam) = idx.sections();
        DiskAnn::load(idx.metric, idx.dims, idx.n, &pq, Bytes::from(vam)).unwrap()
    }

    #[test]
    fn graph_search_recall_and_mapped_equals_ram() {
        let (n, dims) = (6_000, 32);
        let v = blobs(1, n, dims, 40);
        let idx = DiskAnn::build(Metric::L2, dims, v.clone(), &VamanaParams::default());
        let disk = mapped(&idx);
        let qs = near(&v, dims, 2, 100);
        let (mut total, mut s) = (0.0, DiskScratch::default());
        for q in qs.chunks_exact(dims) {
            let truth = brute(&v, dims, q, 10, None);
            let a = idx.search_graph(q, None, opts(10), &mut s);
            let b = disk.search_graph(q, None, opts(10), &mut s);
            assert_eq!(a, b, "mapped blocks give the same answers as RAM");
            total += recall(&a, &truth);
        }
        let r = total / 100.0;
        assert!(r > 0.95, "graph recall {r}");
    }

    #[test]
    fn filtered_scan_and_graph() {
        let (n, dims) = (5_000, 16);
        let v = blobs(3, n, dims, 25);
        let idx = mapped(&DiskAnn::build(Metric::L2, dims, v.clone(), &small()));
        let mut sparse = Bitmap::empty(n as u32);
        let mut half = Bitmap::empty(n as u32);
        for i in 0..n as u32 {
            if i % 97 == 0 {
                sparse.set(i);
            }
            if i % 2 == 0 {
                half.set(i);
            }
        }
        let qs = near(&v, dims, 4, 50);
        let (mut rs, mut rg, mut s) = (0.0, 0.0, DiskScratch::default());
        for q in qs.chunks_exact(dims) {
            let got = idx.search_scan(q, Some(&sparse), opts(10), &mut s);
            assert!(got.iter().all(|g| sparse.contains(g.1)));
            rs += recall(&got, &brute(&v, dims, q, 10, Some(&sparse)));
            let got = idx.search_graph(q, Some(&half), opts(10), &mut s);
            assert!(got.iter().all(|g| half.contains(g.1)));
            rg += recall(&got, &brute(&v, dims, q, 10, Some(&half)));
        }
        assert!(rs / 50.0 > 0.95, "scan recall {}", rs / 50.0);
        assert!(rg / 50.0 > 0.9, "graph recall at 50% {}", rg / 50.0);
    }

    #[test]
    fn build_is_deterministic_and_cosine_works() {
        let dims = 12;
        let mut v = blobs(5, 1_500, dims, 10);
        for r in v.chunks_exact_mut(dims) {
            let n = r.iter().map(|x| x * x).sum::<f32>().sqrt();
            r.iter_mut().for_each(|x| *x /= n);
        }
        let a = DiskAnn::build(Metric::Cosine, dims, v.clone(), &small()).sections();
        let b = DiskAnn::build(Metric::Cosine, dims, v.clone(), &small()).sections();
        assert_eq!(a, b);
        let idx = DiskAnn::load(Metric::Cosine, dims, 1_500, &a.0, Bytes::from(a.1)).unwrap();
        let q = &v[7 * dims..8 * dims];
        let got = idx.search_graph(q, None, opts(1), &mut DiskScratch::default());
        assert_eq!(got[0].1, 7, "a stored vector finds itself first");
    }

    /// ADR 0019: the batched build is identical for any thread count and matches the
    /// row-at-a-time build's recall (graph-only search with a small beam).
    #[test]
    fn batched_build_is_thread_independent_and_keeps_recall() {
        let dims = 24;
        let n = 12_000;
        let v = blobs(11, n, dims, 300);
        let params = VamanaParams {
            r: 16,
            l_build: 32,
            passes: 2,
            ..VamanaParams::default()
        };
        let seq = build_graph_parallel(&v, dims, &params, &cairn_core::Sequential);
        for t in [3, 8] {
            let par =
                build_graph_parallel(&v, dims, &params, &cairn_runtime::ThreadParallel::new(t));
            assert!(par == seq, "{t} threads differ from 1");
        }
        let reference = build_graph(&v, dims, &params);
        let graph_recall = |(medoid, adj): &(u32, Vec<Vec<u32>>)| {
            let mut vis = Visited {
                marks: vec![0; n],
                epoch: 0,
            };
            let mut total = 0.0;
            for qi in 0..200usize {
                let (a, b) = ((qi * 7919) % n, (qi * 104_729 + 3) % n);
                let q: Vec<f32> = v[a * dims..(a + 1) * dims]
                    .iter()
                    .zip(&v[b * dims..(b + 1) * dims])
                    .map(|(x, y)| (x + y) / 2.0)
                    .collect();
                let mut got = greedy_ram((&v, dims), adj, *medoid, &q, 12, &mut vis);
                got.sort_by(|x, y| x.0.total_cmp(&y.0).then(x.1.cmp(&y.1)));
                got.truncate(10);
                total += recall(&got, &brute(&v, dims, &q, 10, None));
            }
            total / 200.0
        };
        let (r_new, r_old) = (graph_recall(&seq), graph_recall(&reference));
        eprintln!("vamana graph recall@10 batched {r_new:.4} row-at-a-time {r_old:.4}");
        assert!(
            r_new >= r_old - 0.01,
            "batched {r_new} vs row-at-a-time {r_old}"
        );
    }

    #[test]
    fn tiny_and_empty_segments() {
        for n in [0usize, 1, 2, 5] {
            let v = blobs(6, n, 8, 2);
            let idx = mapped(&DiskAnn::build(Metric::L2, 8, v, &small()));
            let q = vec![0.0; 8];
            let got = idx.search_graph(&q, None, opts(3), &mut DiskScratch::default());
            assert_eq!(got.len(), n.min(3));
        }
    }

    #[test]
    fn large_blocks_span_pages() {
        let dims = 1100; // block > 4 KiB
        let v = blobs(7, 300, dims, 5);
        let idx = DiskAnn::build(Metric::L2, dims, v.clone(), &small());
        let disk = mapped(&idx);
        let q = &v[42 * dims..43 * dims];
        let a = idx.search_graph(q, None, opts(3), &mut DiskScratch::default());
        let got = disk.search_graph(q, None, opts(3), &mut DiskScratch::default());
        assert_eq!(a, got);
        assert_eq!(got[0].1, 42);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        /// Damaged sections load as an error or as an index whose searches do not panic.
        #[test]
        fn damaged_sections_never_panic(flips in prop::collection::vec((0usize..200_000, any::<u8>()), 1..8),
                                        cut in prop::option::of(0usize..200_000)) {
            let dims = 8;
            let v = blobs(8, 400, dims, 4);
            let (pq, vam) = DiskAnn::build(Metric::L2, dims, v, &small()).sections();
            let mut pq = pq;
            let mut vam = vam;
            for (i, (pos, x)) in flips.iter().enumerate() {
                if i % 2 == 0 { let p = pos % vam.len(); vam[p] ^= x | 1; }
                else { let p = pos % pq.len(); pq[p] ^= x | 1; }
            }
            if let Some(c) = cut { vam.truncate(c % (vam.len() + 1)); }
            if let Ok(idx) = DiskAnn::load(Metric::L2, dims, 400, &pq, Bytes::from(vam)) {
                let q = vec![0.5; dims];
                let mut s = DiskScratch::default();
                let _ = idx.search_graph(&q, None, opts(5), &mut s);
                let _ = idx.search_scan(&q, None, opts(5), &mut s);
            }
        }
    }
}
