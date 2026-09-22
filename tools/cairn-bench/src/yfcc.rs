//! Big-ANN NeurIPS'23 filtered track (YFCC-10M, 192-d uint8, tag filters), CC BY 4.0.
//!
//! The base is split into segments of `segment_rows` rows, as the engine would (ADR 0004 caps
//! segments), each with its own vector index and tag row lists; a query evaluates its tag filter
//! per segment, searches each segment, and merges the per-segment results. Build runs one thread
//! per segment (tool-side parallelism only; the engine builds one segment per core).

use crate::datasets;
use anyhow::{Context, ensure};
use cairn_core::Metric;
use cairn_index::{Bitmap, HnswParams, Strategy, VectorIndex, VectorIndexParams, VectorQuery};
use hdrhistogram::Histogram;
use std::fmt::Write as _;
use std::path::Path;
use std::time::Instant;

/// CSR sparse matrix in the track's `.spmat` layout.
pub struct SpMat {
    /// Rows.
    pub nrow: usize,
    /// Columns (tag vocabulary).
    pub ncol: usize,
    /// Row pointers (`nrow + 1`).
    pub indptr: Vec<u64>,
    /// Column indices (`nnz`).
    pub indices: Vec<u32>,
}

impl SpMat {
    /// Tags of row `r`.
    pub fn row(&self, r: usize) -> &[u32] {
        &self.indices[self.indptr[r] as usize..self.indptr[r + 1] as usize]
    }
}

/// Reads `.spmat`: `i64 nrow, i64 ncol, i64 nnz, i64 indptr[nrow+1], i32 indices[nnz], f32 data[nnz]`.
pub fn read_spmat(path: &Path, limit_rows: Option<usize>) -> anyhow::Result<SpMat> {
    let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    ensure!(bytes.len() >= 24, "short spmat");
    let i64_at = |o: usize| i64::from_le_bytes(bytes[o..o + 8].try_into().unwrap()) as u64;
    let (nrow_all, ncol, nnz) = (i64_at(0) as usize, i64_at(8) as usize, i64_at(16) as usize);
    let nrow = limit_rows.map_or(nrow_all, |l| l.min(nrow_all));
    let indptr_off = 24;
    let indices_off = indptr_off + 8 * (nrow_all + 1);
    ensure!(bytes.len() >= indices_off + 4 * nnz, "truncated spmat");
    let indptr: Vec<u64> = (0..=nrow).map(|r| i64_at(indptr_off + 8 * r)).collect();
    let last = indptr[nrow] as usize;
    let indices: Vec<u32> = (0..last)
        .map(|i| {
            let o = indices_off + 4 * i;
            i32::from_le_bytes(bytes[o..o + 4].try_into().unwrap()) as u32
        })
        .collect();
    Ok(SpMat {
        nrow,
        ncol,
        indptr,
        indices,
    })
}

/// Per-segment tag → sorted rows.
struct TagLists {
    rows: u32,
    /// Sorted `(tag, row)` pairs; `starts[tag]..starts[tag+1]` indexes into `rows_by_tag`.
    starts: Vec<u64>,
    rows_by_tag: Vec<u32>,
}

impl TagLists {
    fn build(meta: &SpMat, first_row: usize, rows: usize) -> Self {
        let mut pairs: Vec<(u32, u32)> = Vec::new();
        for r in 0..rows {
            for &t in meta.row(first_row + r) {
                pairs.push((t, r as u32));
            }
        }
        pairs.sort_unstable();
        let mut starts = vec![0u64; meta.ncol + 1];
        for (t, _) in &pairs {
            starts[*t as usize + 1] += 1;
        }
        for t in 0..meta.ncol {
            starts[t + 1] += starts[t];
        }
        TagLists {
            rows: rows as u32,
            starts,
            rows_by_tag: pairs.into_iter().map(|p| p.1).collect(),
        }
    }

    /// Rows having every tag in `tags`.
    fn filter(&self, tags: &[u32]) -> Bitmap {
        let mut acc: Option<Bitmap> = None;
        for &t in tags {
            let mut b = Bitmap::empty(self.rows);
            for &r in &self.rows_by_tag
                [self.starts[t as usize] as usize..self.starts[t as usize + 1] as usize]
            {
                b.set(r);
            }
            match &mut acc {
                None => acc = Some(b),
                Some(a) => a.and_with(&b),
            }
        }
        acc.unwrap_or_else(|| Bitmap::full(self.rows))
    }
}

struct Segment {
    first_row: usize,
    index: VectorIndex,
    tags: TagLists,
}

fn recall(got: &[u32], truth: &[u32]) -> f64 {
    got.iter().filter(|r| truth.contains(r)).count() as f64 / truth.len().max(1) as f64
}

fn bucket(passing: u64) -> &'static str {
    match passing {
        0..=999 => "<1k",
        1_000..=9_999 => "1k-10k",
        10_000..=99_999 => "10k-100k",
        100_000..=999_999 => "100k-1M",
        _ => ">=1M",
    }
}

#[allow(clippy::too_many_arguments)]
pub fn yfcc_sweep(
    dir: &Path,
    n: usize,
    nq: usize,
    k: usize,
    segment_rows: usize,
    m: u32,
    efc: u32,
    ef: u32,
    out: &Path,
) -> anyhow::Result<()> {
    let t0 = Instant::now();
    let base = datasets::read_u8bin(&dir.join("base.10M.u8bin"), Some(n))?;
    let queries = datasets::read_u8bin(&dir.join("query.public.100K.u8bin"), Some(nq))?;
    let base_meta = read_spmat(&dir.join("base.metadata.10M.spmat"), Some(n))?;
    let query_meta = read_spmat(&dir.join("query.metadata.public.100K.spmat"), Some(nq))?;
    let dims = base.dims;
    ensure!(
        base_meta.nrow >= base.n && query_meta.nrow >= queries.n,
        "metadata shorter than vectors"
    );
    eprintln!(
        "loaded {} base, {} queries, {dims}-d, {} tags in {:.1?}",
        base.n,
        queries.n,
        base_meta.ncol,
        t0.elapsed()
    );
    let official_gt = if n >= 10_000_000 {
        Some(datasets::read_ibin_gt(
            &dir.join("GT.public.ibin"),
            Some(nq),
        )?)
    } else {
        None
    };

    // Split the base into segments without copying twice.
    let mut data = base.data;
    let mut chunks: Vec<(usize, Vec<f32>)> = Vec::new();
    let mut first = 0;
    while first < base.n {
        let rows = segment_rows.min(base.n - first);
        let rest = data.split_off(rows * dims);
        chunks.push((first, std::mem::replace(&mut data, rest)));
        first += rows;
    }
    let params = VectorIndexParams {
        hnsw: HnswParams {
            m,
            ef_construction: efc,
        },
        ..VectorIndexParams::default()
    };
    let t = Instant::now();
    let segments: Vec<Segment> = std::thread::scope(|s| {
        let handles: Vec<_> = chunks
            .into_iter()
            .map(|(first_row, rows)| {
                let meta = &base_meta;
                s.spawn(move || {
                    let n_rows = rows.len() / dims;
                    let ids: Vec<u64> = (first_row as u64..(first_row + n_rows) as u64).collect();
                    let index = VectorIndex::build(
                        0,
                        Metric::L2,
                        dims,
                        rows,
                        Bitmap::full(n_rows as u32),
                        &ids,
                        params,
                    );
                    let tags = TagLists::build(meta, first_row, n_rows);
                    Segment {
                        first_row,
                        index,
                        tags,
                    }
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().expect("segment build panicked"))
            .collect()
    });
    let build_s = t.elapsed().as_secs_f64();
    eprintln!(
        "built {} segments in {build_s:.1}s ({} threads)",
        segments.len(),
        segments.len()
    );

    let mut md = String::new();
    writeln!(
        md,
        "# YFCC-10M filtered track sweep (Big-ANN NeurIPS'23, CC BY 4.0)\n"
    )?;
    writeln!(
        md,
        "- Date: {}",
        std::env::var("CAIRN_BENCH_DATE").unwrap_or_default()
    )?;
    writeln!(
        md,
        "- Base rows: {} (prefix), queries: {} (prefix of the public 100K), dims: {dims} (uint8 as f32), k = {k}",
        base.n, queries.n
    )?;
    writeln!(
        md,
        "- Segments: {} × up to {segment_rows} rows; HNSW M = {m}, efConstruction = {efc}, search ef = {ef}; SQ8 + exact rerank",
        segments.len()
    )?;
    writeln!(
        md,
        "- Build: {build_s:.1} s wall with one thread per segment ({:.0} rows/s/thread)",
        base.n as f64 / build_s / segments.len() as f64
    )?;
    writeln!(
        md,
        "- Ground truth: {}",
        if official_gt.is_some() {
            "official GT.public.ibin"
        } else {
            "exact filtered scan over the prefix (computed here)"
        }
    )?;
    writeln!(md, "- Kernels: {}\n", cairn_index::kernels::kernels().name)?;

    let search = |q: &[f32],
                  tags: &[u32],
                  force: Option<Strategy>,
                  exact: bool|
     -> (Vec<u32>, u64, Strategy) {
        let mut all: Vec<(f32, u32)> = Vec::new();
        let mut passing = 0u64;
        let mut used = Strategy::Scan;
        for seg in &segments {
            let f = seg.tags.filter(tags);
            passing += u64::from(f.count());
            if f.count() == 0 {
                continue;
            }
            let (res, s) = seg
                .index
                .search(
                    q,
                    Some(&f),
                    VectorQuery {
                        ef,
                        force,
                        exact,
                        max_visits: 8192,
                        ..VectorQuery::new(k)
                    },
                )
                .unwrap();
            used = s;
            all.extend(res.into_iter().map(|(d, r)| (d, r + seg.first_row as u32)));
        }
        all.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        all.truncate(k);
        (all.into_iter().map(|x| x.1).collect(), passing, used)
    };

    // Ground truth.
    let t = Instant::now();
    let truth: Vec<Vec<u32>> = match &official_gt {
        Some(gt) => gt.iter().map(|g| g[..k.min(g.len())].to_vec()).collect(),
        None => (0..queries.n)
            .map(|i| {
                search(
                    queries.row(i),
                    query_meta.row(i),
                    Some(Strategy::Scan),
                    true,
                )
                .0
            })
            .collect(),
    };
    eprintln!("ground truth ready in {:.1?}", t.elapsed());

    // Sweep: adaptive vs forced strategies, bucketed by selectivity.
    let buckets = ["<1k", "1k-10k", "10k-100k", "100k-1M", ">=1M"];
    for (label, force) in [
        ("adaptive", None),
        ("scan", Some(Strategy::Scan)),
        ("hnsw (capped 8192 visits)", Some(Strategy::Hnsw)),
        ("hnsw+2hop (capped)", Some(Strategy::HnswTwoHop)),
    ] {
        let mut per: Vec<(u64, f64, Histogram<u64>, usize)> = buckets
            .iter()
            .map(|_| {
                (
                    0,
                    0.0,
                    Histogram::new_with_bounds(1, 60_000_000_000, 3).unwrap(),
                    0,
                )
            })
            .collect();
        let mut used_count = [0usize; 3];
        let tq = Instant::now();
        for i in 0..queries.n {
            let s = Instant::now();
            let (got, passing, used) = search(queries.row(i), query_meta.row(i), force, false);
            let el = s.elapsed();
            let b = buckets.iter().position(|x| *x == bucket(passing)).unwrap();
            per[b].0 += 1;
            per[b].1 += recall(&got, &truth[i]);
            per[b].2.record(el.as_nanos() as u64).unwrap();
            per[b].3 += 1;
            used_count[match used {
                Strategy::Scan => 0,
                Strategy::Hnsw => 1,
                Strategy::HnswTwoHop => 2,
            }] += 1;
        }
        let total_s = tq.elapsed().as_secs_f64();
        writeln!(md, "## Strategy: {label}\n")?;
        if label == "adaptive" {
            writeln!(
                md,
                "Strategy chosen per query (last segment): scan {} / hnsw {} / hnsw+2hop {}\n",
                used_count[0], used_count[1], used_count[2]
            )?;
        }
        writeln!(
            md,
            "| rows passing (all segments) | queries | recall@{k} | p50 µs | p99 µs |\n|---|---|---|---|---|"
        )?;
        let mut all_rec = 0.0;
        let mut all_n = 0u64;
        for (bi, (cnt, rec, h, _)) in per.iter().enumerate() {
            if *cnt == 0 {
                continue;
            }
            all_rec += rec;
            all_n += cnt;
            writeln!(
                md,
                "| {} | {cnt} | {:.4} | {:.0} | {:.0} |",
                buckets[bi],
                rec / *cnt as f64,
                h.value_at_quantile(0.5) as f64 / 1000.0,
                h.value_at_quantile(0.99) as f64 / 1000.0
            )?;
        }
        writeln!(
            md,
            "| **all** | {all_n} | **{:.4}** | | QPS (1 thread): {:.0} |\n",
            all_rec / all_n as f64,
            queries.n as f64 / total_s
        )?;
        eprintln!(
            "{label}: recall {:.4}, {:.0} QPS",
            all_rec / all_n as f64,
            queries.n as f64 / total_s
        );
    }
    std::fs::write(out, md)?;
    eprintln!("wrote {} in {:.1?}", out.display(), t0.elapsed());
    Ok(())
}
