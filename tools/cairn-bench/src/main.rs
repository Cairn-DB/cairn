//! `cairn-bench`: recall / latency sweeps over public datasets, writing markdown reports.
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    clippy::too_many_arguments,
    clippy::needless_range_loop
)]

mod datasets;
mod kmeans;
mod msmarco;
mod yfcc;

use anyhow::Context;
use cairn_bench_gen::{Correlation, GenConfig, Generator};
use cairn_core::Metric;
use cairn_index::{Bitmap, HnswParams, Strategy, VectorIndex, VectorIndexParams, VectorQuery};
use clap::{Parser, Subcommand};
use hdrhistogram::Histogram;
use std::fmt::Write as _;
use std::path::PathBuf;
use std::time::Instant;

#[derive(Parser)]
#[command(name = "cairn-bench")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Filtered-search sweep on SIFT1M (texmex format) with synthetic attributes.
    SiftSweep {
        /// Directory holding sift_base.fvecs, sift_query.fvecs, sift_groundtruth.ivecs.
        #[arg(long, default_value = "data/sift")]
        dir: PathBuf,
        /// Base vectors to use (prefix).
        #[arg(long, default_value_t = 1_000_000)]
        n: usize,
        /// Queries to use (prefix).
        #[arg(long, default_value_t = 1000)]
        queries: usize,
        /// k for recall@k.
        #[arg(long, default_value_t = 10)]
        k: usize,
        /// HNSW M.
        #[arg(long, default_value_t = 16)]
        m: u32,
        /// HNSW efConstruction.
        #[arg(long, default_value_t = 100)]
        ef_construction: u32,
        /// Search ef values to sweep (comma separated).
        #[arg(long, default_value = "32,64,128,256")]
        ef: String,
        /// Number of k-means clusters for the correlated attributes.
        #[arg(long, default_value_t = 1000)]
        clusters: usize,
        /// Output markdown path.
        #[arg(long)]
        out: PathBuf,
    },
    /// BM25 MRR@10 on MS MARCO passage dev (small).
    Msmarco {
        /// Directory holding collection.tsv, queries.dev.small.tsv, qrels.dev.small.tsv.
        #[arg(long, default_value = "data/msmarco")]
        dir: PathBuf,
        /// Passages to index (prefix); default all.
        #[arg(long)]
        limit: Option<usize>,
        /// Rows per segment.
        #[arg(long, default_value_t = 1_000_000)]
        segment_rows: usize,
        /// BM25 k1.
        #[arg(long, default_value_t = 0.9)]
        k1: f32,
        /// BM25 b.
        #[arg(long, default_value_t = 0.4)]
        b: f32,
        /// Output markdown path.
        #[arg(long)]
        out: PathBuf,
    },
    /// Filtered-search sweep on the Big-ANN YFCC-10M filtered track.
    YfccSweep {
        /// Directory holding base.10M.u8bin, query.public.100K.u8bin, *.spmat, GT.public.ibin.
        #[arg(long, default_value = "data/yfcc10m")]
        dir: PathBuf,
        /// Base rows to use (prefix; 10000000 enables the official ground truth).
        #[arg(long, default_value_t = 10_000_000)]
        n: usize,
        /// Queries to use (prefix).
        #[arg(long, default_value_t = 10_000)]
        queries: usize,
        /// k for recall@k.
        #[arg(long, default_value_t = 10)]
        k: usize,
        /// Rows per segment.
        #[arg(long, default_value_t = 1_000_000)]
        segment_rows: usize,
        /// HNSW M.
        #[arg(long, default_value_t = 16)]
        m: u32,
        /// HNSW efConstruction.
        #[arg(long, default_value_t = 100)]
        ef_construction: u32,
        /// Search ef.
        #[arg(long, default_value_t = 128)]
        ef: u32,
        /// Output markdown path.
        #[arg(long)]
        out: PathBuf,
    },
}

struct Lat {
    h: Histogram<u64>,
}

impl Lat {
    fn new() -> Self {
        Lat {
            h: Histogram::new_with_bounds(1, 60_000_000_000, 3).unwrap(),
        }
    }
    fn record(&mut self, d: std::time::Duration) {
        self.h.record(d.as_nanos() as u64).unwrap();
    }
    fn p(&self, q: f64) -> f64 {
        self.h.value_at_quantile(q) as f64 / 1000.0
    }
}

fn recall(got: &[(f32, u32)], truth: &[u32]) -> f64 {
    got.iter().filter(|(_, r)| truth.contains(r)).count() as f64 / truth.len() as f64
}

fn sift_sweep(
    dir: PathBuf,
    n: usize,
    nq: usize,
    k: usize,
    m: u32,
    efc: u32,
    efs: Vec<u32>,
    clusters: usize,
    out: PathBuf,
) -> anyhow::Result<()> {
    let t0 = Instant::now();
    let base = datasets::read_fvecs(&dir.join("sift_base.fvecs"), Some(n)).context("base")?;
    let queries =
        datasets::read_fvecs(&dir.join("sift_query.fvecs"), Some(nq)).context("queries")?;
    let dims = base.dims;
    eprintln!(
        "loaded {} base, {} queries, {dims}-d in {:.1?}",
        base.n,
        queries.n,
        t0.elapsed()
    );
    let mut md = String::new();
    writeln!(md, "# SIFT1M filtered-search sweep\n")?;
    writeln!(md, "- Date: {}", chrono_free_date())?;
    writeln!(
        md,
        "- Base vectors: {} (prefix of sift_base.fvecs), queries: {}, dims: {dims}, k = {k}",
        base.n, queries.n
    )?;
    writeln!(
        md,
        "- Index: HNSW M = {m}, efConstruction = {efc}, SQ8 candidates + exact rerank (4k)"
    )?;
    writeln!(md, "- Kernels: {}", cairn_index::kernels::kernels().name)?;
    writeln!(
        md,
        "- Hardware: see docs/progress.md (Ryzen 7 8845HS, single thread)\n"
    )?;

    // Cluster labels for the correlated attributes.
    let t = Instant::now();
    let labels = kmeans::kmeans_labels(&base.data, dims, clusters, 100_000.min(base.n), 8, 7);
    eprintln!("k-means ({clusters} clusters) in {:.1?}", t.elapsed());

    // Build.
    let ids: Vec<u64> = (0..base.n as u64).collect();
    let params = VectorIndexParams {
        hnsw: HnswParams {
            m,
            ef_construction: efc,
        },
        ..VectorIndexParams::default()
    };
    let t = Instant::now();
    let index = VectorIndex::build(
        0,
        Metric::L2,
        dims,
        base.data.clone(),
        Bitmap::full(base.n as u32),
        &ids,
        params,
    );
    let build_s = t.elapsed().as_secs_f64();
    eprintln!("built index in {build_s:.1}s");
    writeln!(
        md,
        "## Build\n\n- HNSW build (single thread, f32 distances): {build_s:.1} s for {} rows = {:.0} rows/s\n",
        base.n,
        base.n as f64 / build_s
    )?;

    // Unfiltered: ground truth from brute force on the prefix (the official GT covers 1M only).
    let t = Instant::now();
    let truth: Vec<Vec<u32>> = (0..queries.n)
        .map(|i| {
            index
                .search(
                    queries.row(i),
                    None,
                    VectorQuery {
                        force: Some(Strategy::Scan),
                        rerank: k,
                        ..VectorQuery::new(k)
                    },
                )
                .unwrap()
                .0
                .into_iter()
                .map(|(_, r)| r)
                .collect()
        })
        .collect();
    eprintln!("unfiltered ground truth in {:.1?}", t.elapsed());
    writeln!(
        md,
        "## Unfiltered recall@{k} vs ef (HNSW + SQ8 + rerank)\n\n| ef | recall@{k} | p50 µs | p99 µs | QPS (1 thread) |\n|---|---|---|---|---|"
    )?;
    for &ef in &efs {
        let mut lat = Lat::new();
        let mut rec = 0.0;
        let t = Instant::now();
        for i in 0..queries.n {
            let s = Instant::now();
            let (got, _) = index
                .search(
                    queries.row(i),
                    None,
                    VectorQuery {
                        ef,
                        force: Some(Strategy::Hnsw),
                        ..VectorQuery::new(k)
                    },
                )
                .unwrap();
            lat.record(s.elapsed());
            rec += recall(&got, &truth[i]);
        }
        let qps = queries.n as f64 / t.elapsed().as_secs_f64();
        writeln!(
            md,
            "| {ef} | {:.4} | {:.0} | {:.0} | {:.0} |",
            rec / queries.n as f64,
            lat.p(0.5),
            lat.p(0.99),
            qps
        )?;
    }

    // Filtered sweep.
    writeln!(md, "\n## Filtered recall@{k} (ef = 128 for graph paths)\n")?;
    writeln!(
        md,
        "Selectivity is the fraction of rows passing the filter. `adaptive` is the strategy the index picks\n(scan below {} rows or {:.0}% of the segment; two-hop below {:.0}%).\n",
        params.scan_max_rows,
        params.scan_max_fraction * 100.0,
        params.two_hop_below_fraction * 100.0
    )?;
    writeln!(
        md,
        "| correlation | selectivity | rows passing | strategy | recall@{k} | p50 µs | p99 µs |\n|---|---|---|---|---|---|---|"
    )?;
    for correlation in [Correlation::Random, Correlation::Clustered] {
        let generator = Generator::new(GenConfig {
            seed: 1,
            n: base.n as u64,
            correlation,
            clusters: clusters as u32,
            ..GenConfig::default()
        });
        let attrs = generator.all(Some(&labels));
        for (name, pick) in [("50%", 0usize), ("10%", 1), ("1%", 2), ("0.1%", 3)] {
            let mut filter = Bitmap::empty(base.n as u32);
            for a in &attrs {
                let f = match pick {
                    0 => a.flag_50,
                    1 => a.flag_10,
                    2 => a.flag_1,
                    _ => a.flag_01,
                };
                if f {
                    filter.set(a.id as u32);
                }
            }
            let passing = filter.count();
            if passing < k as u32 {
                writeln!(
                    md,
                    "| {correlation:?} | {name} | {passing} | - | (fewer than k rows pass) | | |"
                )?;
                continue;
            }
            let t = Instant::now();
            let ftruth: Vec<Vec<u32>> = (0..queries.n)
                .map(|i| {
                    index
                        .search(
                            queries.row(i),
                            Some(&filter),
                            VectorQuery {
                                force: Some(Strategy::Scan),
                                rerank: k,
                                ..VectorQuery::new(k)
                            },
                        )
                        .unwrap()
                        .0
                        .into_iter()
                        .map(|(_, r)| r)
                        .collect()
                })
                .collect();
            eprintln!(
                "{correlation:?} {name}: {passing} rows, ground truth in {:.1?}",
                t.elapsed()
            );
            for (label, force, max_visits) in [
                ("adaptive", None, 8192u32),
                ("scan", Some(Strategy::Scan), u32::MAX),
                ("hnsw (capped 8192 visits)", Some(Strategy::Hnsw), 8192),
                (
                    "hnsw+2hop (capped 8192 visits)",
                    Some(Strategy::HnswTwoHop),
                    8192,
                ),
                ("hnsw (unlimited)", Some(Strategy::Hnsw), u32::MAX),
            ] {
                let mut lat = Lat::new();
                let mut rec = 0.0;
                let mut used = None;
                for i in 0..queries.n {
                    let s = Instant::now();
                    let (got, strat) = index
                        .search(
                            queries.row(i),
                            Some(&filter),
                            VectorQuery {
                                ef: 128,
                                force,
                                max_visits,
                                ..VectorQuery::new(k)
                            },
                        )
                        .unwrap();
                    lat.record(s.elapsed());
                    used = Some(strat);
                    rec += recall(&got, &ftruth[i]);
                }
                let strat = match (label, used) {
                    ("adaptive", Some(s)) => format!("adaptive→{s:?}"),
                    _ => label.to_string(),
                };
                writeln!(
                    md,
                    "| {correlation:?} | {name} | {passing} | {strat} | {:.4} | {:.0} | {:.0} |",
                    rec / queries.n as f64,
                    lat.p(0.5),
                    lat.p(0.99)
                )?;
            }
        }
    }
    std::fs::write(&out, md).with_context(|| format!("writing {}", out.display()))?;
    eprintln!("wrote {} in {:.1?}", out.display(), t0.elapsed());
    Ok(())
}

fn chrono_free_date() -> String {
    // No chrono dependency: read the date from the environment or the shell.
    std::env::var("CAIRN_BENCH_DATE").unwrap_or_else(|_| "(set CAIRN_BENCH_DATE)".to_string())
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    match Cli::parse().cmd {
        Cmd::SiftSweep {
            dir,
            n,
            queries,
            k,
            m,
            ef_construction,
            ef,
            clusters,
            out,
        } => {
            let efs = ef
                .split(',')
                .map(|s| s.trim().parse::<u32>())
                .collect::<Result<Vec<_>, _>>()?;
            sift_sweep(dir, n, queries, k, m, ef_construction, efs, clusters, out)
        }
        Cmd::Msmarco {
            dir,
            limit,
            segment_rows,
            k1,
            b,
            out,
        } => msmarco::msmarco(&dir, limit, segment_rows, k1, b, &out),
        Cmd::YfccSweep {
            dir,
            n,
            queries,
            k,
            segment_rows,
            m,
            ef_construction,
            ef,
            out,
        } => yfcc::yfcc_sweep(
            &dir,
            n,
            queries,
            k,
            segment_rows,
            m,
            ef_construction,
            ef,
            &out,
        ),
    }
}
