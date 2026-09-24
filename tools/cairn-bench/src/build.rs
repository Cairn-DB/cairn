//! ADR 0019: HNSW build time and recall, row-at-a-time builder vs batched builder on several
//! thread counts (SIFT prefix, official ground truth at 1M, brute force below).

use crate::datasets;
use cairn_core::Metric;
use cairn_index::hnsw::{Hnsw, HnswBuilder, SearchScratch, build_parallel};
use cairn_index::{HnswParams, SearchOptions, Vectors};
use cairn_runtime::ThreadParallel;
use std::fmt::Write as _;
use std::path::Path;
use std::time::Instant;

fn brute(base: &[f32], dims: usize, q: &[f32], k: usize) -> Vec<u32> {
    let mut d: Vec<(f32, u32)> = base
        .chunks_exact(dims)
        .enumerate()
        .map(|(i, r)| {
            (
                r.iter().zip(q).map(|(a, b)| (a - b) * (a - b)).sum(),
                i as u32,
            )
        })
        .collect();
    d.select_nth_unstable_by(k, |a, b| a.0.total_cmp(&b.0));
    let mut top: Vec<(f32, u32)> = d[..k].to_vec();
    top.sort_by(|a, b| a.0.total_cmp(&b.0));
    top.into_iter().map(|x| x.1).collect()
}

fn recall(g: &Hnsw, v: &Vectors, queries: &datasets::Matrix, truth: &[Vec<u32>], ef: u32) -> f64 {
    let mut scratch = SearchScratch::new(v.len());
    let mut hits = 0usize;
    for (i, t) in truth.iter().enumerate() {
        let got = g.search(
            v,
            queries.row(i),
            10,
            None,
            SearchOptions {
                ef,
                exact: true,
                ..SearchOptions::default()
            },
            &mut scratch,
        );
        hits += got.iter().filter(|(_, r)| t[..10].contains(r)).count();
    }
    hits as f64 / (truth.len() * 10) as f64
}

#[allow(clippy::too_many_arguments)]
pub fn build_sweep(
    dir: &Path,
    n: usize,
    nq: usize,
    m: u32,
    efc: u32,
    threads: &[usize],
    reference: bool,
    out: &Path,
) -> anyhow::Result<()> {
    let base = datasets::read_fvecs(&dir.join("sift_base.fvecs"), Some(n))?;
    let queries = datasets::read_fvecs(&dir.join("sift_query.fvecs"), Some(nq))?;
    let (n, dims) = (base.n, base.dims);
    let truth: Vec<Vec<u32>> = if n == 1_000_000 {
        datasets::read_ivecs(&dir.join("sift_groundtruth.ivecs"), Some(queries.n))?
    } else {
        (0..queries.n)
            .map(|i| brute(&base.data, dims, queries.row(i), 10))
            .collect()
    };
    let v = Vectors::from_rows(Metric::L2, dims, base.data.clone(), false);
    let ids: Vec<u64> = (0..n as u64).collect();
    let params = HnswParams {
        m,
        ef_construction: efc,
    };
    let mut md = String::new();
    writeln!(md, "# HNSW build: row-at-a-time vs batched (ADR 0019)\n")?;
    writeln!(
        md,
        "- Date: {}. SIFT prefix {n} × {dims}, {} queries, M = {m}, efConstruction = {efc}; \
         graph search with exact distances, k = 10.",
        crate::chrono_free_date(),
        queries.n
    )?;
    writeln!(md, "- Hardware: see docs/progress.md.\n")?;
    writeln!(
        md,
        "| builder | threads | build s | rows/s | recall@10 ef 32 | ef 64 | ef 128 |\n|---|---|---|---|---|---|---|"
    )?;
    let mut row = |name: &str, t: usize, s: f64, g: &Hnsw| -> anyhow::Result<()> {
        let r: Vec<f64> = [32, 64, 128]
            .iter()
            .map(|&ef| recall(g, &v, &queries, &truth, ef))
            .collect();
        eprintln!("{name} {t} threads: {s:.1}s recall {r:?}");
        writeln!(
            md,
            "| {name} | {t} | {s:.1} | {:.0} | {:.4} | {:.4} | {:.4} |",
            n as f64 / s,
            r[0],
            r[1],
            r[2]
        )?;
        Ok(())
    };
    if reference {
        let t = Instant::now();
        let g = HnswBuilder::new(&v, &ids, params).finish();
        row("row-at-a-time", 1, t.elapsed().as_secs_f64(), &g)?;
    }
    let mut first: Option<Vec<u8>> = None;
    let mut identical = true;
    for &th in threads {
        let t = Instant::now();
        let g = build_parallel(&v, &ids, params, &ThreadParallel::new(th));
        let s = t.elapsed().as_secs_f64();
        let bytes = g.encode();
        match &first {
            None => first = Some(bytes),
            Some(f) => identical &= *f == bytes,
        }
        row("batched", th, s, &g)?;
    }
    writeln!(
        md,
        "\nBatched graphs byte-identical across thread counts {threads:?}: **{identical}**"
    )?;
    std::fs::write(out, md)?;
    eprintln!("wrote {}", out.display());
    anyhow::ensure!(identical, "batched builds differ across thread counts");
    Ok(())
}
