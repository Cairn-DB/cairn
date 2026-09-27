//! Scale benchmark through a running cluster (10M–50M rows).
//!
//! Two datasets:
//! - `yfcc`: Big-ANN NeurIPS'23 filtered track (YFCC-10M, 192-d uint8, tag sets), queried with
//!   each query's own tag filter and scored against the official ground truth.
//! - `bigann`: a prefix of BigANN / SIFT1B (128-d uint8) with bench-gen attributes, scored
//!   against a brute-force ground truth computed here (unfiltered and `flag_1`, about 1%).
//!
//! Vectors stay `u8` in this process (converted per row) so the three local replicas get the RAM.
//! Reported: ingest throughput and batch latency, time for background builds to settle, query
//! recall and latency per consistency level (YFCC: per selectivity bucket), takedown visibility,
//! and each node's resident memory.

use crate::yfcc::{SpMat, read_spmat};
use anyhow::{Context, ensure};
use bytes::Bytes;
use cairn_bench_gen::{Correlation, GenConfig, Generator};
use cairn_client::Client;
use cairn_core::{DocId, Document, HashMap, NodeId, Predicate, Value};
use cairn_query::{Consistency, Query, VectorLeg};
use hdrhistogram::Histogram;
use std::fmt::Write as _;
use std::io::Read as _;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Row-major `u8` matrix.
pub struct U8Matrix {
    /// Rows.
    pub n: usize,
    /// Columns.
    pub dims: usize,
    /// Values.
    pub data: Vec<u8>,
}

impl U8Matrix {
    /// Row `i` widened to f32.
    pub fn row_f32(&self, i: usize) -> Vec<f32> {
        self.data[i * self.dims..(i + 1) * self.dims]
            .iter()
            .map(|&b| f32::from(b))
            .collect()
    }
}

/// Reads the first `limit` rows of a Big-ANN `.u8bin` without reading the rest of the file.
pub fn read_u8bin_prefix(path: &Path, limit: usize) -> anyhow::Result<U8Matrix> {
    let mut f = std::fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut hdr = [0u8; 8];
    f.read_exact(&mut hdr)?;
    let n_all = u32::from_le_bytes(hdr[0..4].try_into().unwrap()) as usize;
    let dims = u32::from_le_bytes(hdr[4..8].try_into().unwrap()) as usize;
    let file_rows = (f.metadata()?.len() as usize - 8) / dims;
    let n = limit.min(n_all).min(file_rows);
    ensure!(
        n == limit,
        "{} holds {n} rows, {limit} requested",
        path.display()
    );
    let mut data = vec![0u8; n * dims];
    f.read_exact(&mut data)?;
    Ok(U8Matrix { n, dims, data })
}

/// Dataset choice.
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum Dataset {
    /// Big-ANN filtered track, YFCC-10M.
    Yfcc,
    /// BigANN (SIFT1B) prefix with synthetic attributes.
    Bigann,
}

/// Arguments.
pub struct ScaleArgs {
    /// Dataset.
    pub dataset: Dataset,
    /// Dataset directory.
    pub dir: PathBuf,
    /// Nodes.
    pub nodes: Vec<(u32, SocketAddr)>,
    /// Rows to ingest.
    pub n: usize,
    /// Batch size.
    pub batch: usize,
    /// Writer threads.
    pub writers: usize,
    /// Queries.
    pub queries: usize,
    /// Query threads.
    pub query_threads: usize,
    /// Search ef.
    pub ef: u32,
    /// Takedowns to time.
    pub takedowns: usize,
    /// Seconds without segment changes that count as settled.
    pub settle_secs: u64,
    /// Skip ingest (the cluster already holds the data).
    pub skip_ingest: bool,
    /// First row to ingest (resume a partial load; earlier rows must already be there).
    pub start: usize,
    /// File with the server pids (one per line), for memory reporting.
    pub pids: Option<PathBuf>,
    /// Output markdown.
    pub out: PathBuf,
}

fn hist() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 3_600_000_000_000, 3).unwrap()
}

fn ms(h: &Histogram<u64>, q: f64) -> f64 {
    h.value_at_quantile(q) as f64 / 1e6
}

fn recall(got: &[u64], truth: &[u32]) -> f64 {
    got.iter().filter(|g| truth.contains(&(**g as u32))).count() as f64 / truth.len().max(1) as f64
}

fn bucket(passing: u64) -> usize {
    match passing {
        0..=999 => 0,
        1_000..=9_999 => 1,
        10_000..=99_999 => 2,
        100_000..=999_999 => 3,
        _ => 4,
    }
}
/// Per-bucket (queries, recall sum, latency), overall latency, mean recall, QPS.
type RunResult = (Vec<(u64, f64, Histogram<u64>)>, Histogram<u64>, f64, f64);

const BUCKETS: [&str; 5] = ["<1k", "1k-10k", "10k-100k", "100k-1M", ">=1M"];

/// Tag postings over the base (sorted rows per tag), for per-query selectivity.
struct Postings {
    starts: Vec<u64>,
    rows: Vec<u32>,
}

impl Postings {
    fn build(meta: &SpMat, n: usize) -> Self {
        let mut starts = vec![0u64; meta.ncol + 1];
        for r in 0..n {
            for &t in meta.row(r) {
                starts[t as usize + 1] += 1;
            }
        }
        for t in 0..meta.ncol {
            starts[t + 1] += starts[t];
        }
        let mut fill = starts.clone();
        let mut rows = vec![0u32; starts[meta.ncol] as usize];
        for r in 0..n {
            for &t in meta.row(r) {
                rows[fill[t as usize] as usize] = r as u32;
                fill[t as usize] += 1;
            }
        }
        Postings { starts, rows }
    }

    fn list(&self, t: u32) -> &[u32] {
        &self.rows[self.starts[t as usize] as usize..self.starts[t as usize + 1] as usize]
    }

    /// Rows carrying every tag.
    fn count_all(&self, tags: &[u32]) -> u64 {
        let mut lists: Vec<&[u32]> = tags.iter().map(|&t| self.list(t)).collect();
        lists.sort_by_key(|l| l.len());
        let Some((first, rest)) = lists.split_first() else {
            return 0;
        };
        first
            .iter()
            .filter(|r| rest.iter().all(|l| l.binary_search(r).is_ok()))
            .count() as u64
    }
}

fn tag_name(t: u32) -> String {
    format!("t{t}")
}

fn tag_filter(tags: &[u32]) -> Predicate {
    let mut eqs: Vec<Predicate> = tags
        .iter()
        .map(|&t| Predicate::Eq {
            field: 1,
            value: Value::Enum(tag_name(t)),
        })
        .collect();
    if eqs.len() == 1 {
        eqs.pop().unwrap()
    } else {
        Predicate::And(eqs)
    }
}

/// Brute-force top-k (squared L2) of `queries` over the base rows passing `keep`, multi-threaded
/// over base chunks.
fn brute_force(
    base: &U8Matrix,
    queries: &[Vec<f32>],
    k: usize,
    keep: &(dyn Fn(usize) -> bool + Sync),
) -> Vec<Vec<u32>> {
    let threads = std::thread::available_parallelism().map_or(8, |n| n.get());
    let chunk = base.n.div_ceil(threads);
    let parts: Vec<Vec<Vec<(f32, u32)>>> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..threads)
            .map(|t| {
                s.spawn(move || {
                    let (lo, hi) = (t * chunk, ((t + 1) * chunk).min(base.n));
                    let mut tops: Vec<Vec<(f32, u32)>> = vec![Vec::new(); queries.len()];
                    let mut rows_f = Vec::new();
                    let mut ids = Vec::new();
                    let mut out = Vec::new();
                    let mut r = lo;
                    while r < hi {
                        let end = (r + 4096).min(hi);
                        rows_f.clear();
                        ids.clear();
                        for i in r..end {
                            if keep(i) {
                                rows_f.extend(
                                    base.data[i * base.dims..(i + 1) * base.dims]
                                        .iter()
                                        .map(|&b| f32::from(b)),
                                );
                                ids.push(i as u32);
                            }
                        }
                        out.resize(ids.len(), 0.0);
                        for (qi, q) in queries.iter().enumerate() {
                            cairn_index::kernels::l2_sq_batch(q, &rows_f, &mut out);
                            let top = &mut tops[qi];
                            for (j, &d) in out.iter().enumerate() {
                                if top.len() < k || d < top[k - 1].0 {
                                    let pos = top.partition_point(|x| x.0 <= d);
                                    top.insert(pos, (d, ids[j]));
                                    top.truncate(k);
                                }
                            }
                        }
                        r = end;
                    }
                    tops
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    (0..queries.len())
        .map(|qi| {
            let mut all: Vec<(f32, u32)> = parts.iter().flat_map(|p| p[qi].clone()).collect();
            all.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
            all.into_iter().take(k).map(|x| x.1).collect()
        })
        .collect()
}

fn node_memory(pids: &Path) -> Vec<(String, String)> {
    let Ok(s) = std::fs::read_to_string(pids) else {
        return Vec::new();
    };
    s.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|pid| {
            let status =
                std::fs::read_to_string(format!("/proc/{}/status", pid.trim())).unwrap_or_default();
            let field = |name: &str| {
                status
                    .lines()
                    .find(|l| l.starts_with(name))
                    .and_then(|l| l.split_whitespace().nth(1))
                    .and_then(|kb| kb.parse::<f64>().ok())
                    .map_or("?".to_string(), |kb| {
                        format!("{:.1} GB", kb / 1024.0 / 1024.0)
                    })
            };
            (field("VmRSS:"), field("VmHWM:"))
        })
        .collect()
}

/// Waits until every node reports, for every shard, the same applied index and a segment list
/// that has not changed for `stable`, with no flush or merge building or pending. Without the
/// last condition a merge longer than `stable` passed for settled (GCP run 5).
fn settle(addrs: &HashMap<NodeId, SocketAddr>, nodes: &[(u32, SocketAddr)], stable: Duration) {
    let mut clients: Vec<Client> = nodes
        .iter()
        .map(|(i, a)| Client::new([(NodeId(*i), *a)].into_iter().collect()))
        .collect();
    let _ = addrs;
    let mut last: Option<Vec<(u64, u64, Vec<u64>)>> = None;
    let mut since = Instant::now();
    let t0 = Instant::now();
    loop {
        let mut snap = Vec::new();
        let mut ok = true;
        for c in clients.iter_mut() {
            match c.status() {
                Ok(st) => {
                    for s in st {
                        if s.flushes[2] > 0 || s.merges != [0, 0] {
                            ok = false;
                        }
                        snap.push((u64::from(s.id.get()), s.applied.0, s.segments.clone()));
                    }
                }
                Err(_) => ok = false,
            }
        }
        snap.sort();
        if !ok || last.as_ref() != Some(&snap) {
            last = Some(snap);
            since = Instant::now();
        } else if since.elapsed() >= stable {
            eprintln!("settled after {:.0?}", t0.elapsed());
            return;
        }
        if t0.elapsed() > Duration::from_secs(3 * 3600) {
            eprintln!("settle: giving up after 3 h");
            return;
        }
        std::thread::sleep(Duration::from_secs(2));
    }
}

/// Runs the benchmark.
pub fn scale_bench(a: ScaleArgs) -> anyhow::Result<()> {
    let t0 = Instant::now();
    let addrs: HashMap<NodeId, SocketAddr> =
        a.nodes.iter().map(|(i, s)| (NodeId(*i), *s)).collect();
    let k = 10;

    // Load data and the ground truth.
    let (base_file, query_file) = match a.dataset {
        Dataset::Yfcc => ("base.10M.u8bin", "query.public.100K.u8bin"),
        Dataset::Bigann => ("base.50M.u8bin", "query.public.10K.u8bin"),
    };
    let base = Arc::new(read_u8bin_prefix(&a.dir.join(base_file), a.n)?);
    let qm = read_u8bin_prefix(&a.dir.join(query_file), a.queries)?;
    let queries: Vec<Vec<f32>> = (0..qm.n).map(|i| qm.row_f32(i)).collect();
    let dims = base.dims;
    eprintln!("loaded {} × {dims} in {:.1?}", base.n, t0.elapsed());

    let gen_attrs = Generator::new(GenConfig {
        seed: 1,
        n: a.n as u64,
        correlation: Correlation::Random,
        ..GenConfig::default()
    });
    // Per query: filter (None = unfiltered), passing rows, truth.
    struct Q {
        filter: Option<Predicate>,
        passing: u64,
        truth: Vec<u32>,
    }
    let mut workloads: Vec<(&str, Vec<Q>)> = Vec::new();
    let mut base_meta: Option<SpMat> = None;
    let t = Instant::now();
    match a.dataset {
        Dataset::Yfcc => {
            ensure!(
                a.n == 10_000_000,
                "the official YFCC ground truth needs n = 10M"
            );
            let meta = read_spmat(&a.dir.join("base.metadata.10M.spmat"), Some(a.n))?;
            let qmeta = read_spmat(
                &a.dir.join("query.metadata.public.100K.spmat"),
                Some(a.queries),
            )?;
            let gt = crate::datasets::read_ibin_gt(&a.dir.join("GT.public.ibin"), Some(a.queries))?;
            let post = Postings::build(&meta, a.n);
            let qs = (0..qm.n)
                .map(|i| Q {
                    filter: Some(tag_filter(qmeta.row(i))),
                    passing: post.count_all(qmeta.row(i)),
                    truth: gt[i][..k].to_vec(),
                })
                .collect();
            workloads.push(("tag filter (each query's own tags)", qs));
            base_meta = Some(meta);
        }
        Dataset::Bigann => {
            let unf = brute_force(&base, &queries, k, &|_| true);
            let flag: Vec<bool> = (0..a.n)
                .map(|i| gen_attrs.attributes(i as u64, None).flag_1)
                .collect();
            let passing = flag.iter().filter(|f| **f).count() as u64;
            let flt = brute_force(&base, &queries, k, &|i| flag[i]);
            workloads.push((
                "unfiltered",
                unf.into_iter()
                    .map(|truth| Q {
                        filter: None,
                        passing: a.n as u64,
                        truth,
                    })
                    .collect(),
            ));
            workloads.push((
                "flag_1 filter (≈1%)",
                flt.into_iter()
                    .map(|truth| Q {
                        filter: Some(Predicate::Eq {
                            field: 3,
                            value: Value::Bool(true),
                        }),
                        passing,
                        truth,
                    })
                    .collect(),
            ));
        }
    }
    let gt_s = t.elapsed().as_secs_f64();
    eprintln!("ground truth / selectivity ready in {gt_s:.1}s");

    // Ingest.
    let mut ingest_line = String::from("| ingest | skipped (data already loaded) |");
    let mut batch_line = String::new();
    if !a.skip_ingest {
        let t = Instant::now();
        let lat = Arc::new(Mutex::new(hist()));
        let done = Arc::new(AtomicUsize::new(0));
        let meta = base_meta.as_ref();
        std::thread::scope(|s| {
            for w in 0..a.writers {
                let (base, addrs, lat, done, gen_attrs) = (
                    base.clone(),
                    addrs.clone(),
                    lat.clone(),
                    done.clone(),
                    &gen_attrs,
                );
                let dataset = a.dataset;
                let batch = a.batch;
                let start = a.start;
                let writers = a.writers;
                s.spawn(move || {
                    let mut client = Client::new(addrs);
                    // Writes may wait for an index build under backpressure.
                    client.timeout = Duration::from_secs(300);
                    let mut docs = Vec::with_capacity(batch);
                    let mut i = start + w * batch;
                    // Writers take whole batches round-robin so each batch is contiguous.
                    while i < base.n {
                        for r in i..(i + batch).min(base.n) {
                            let d = match dataset {
                                Dataset::Yfcc => {
                                    let tags = meta.unwrap().row(r);
                                    let d = Document::new(DocId(r as u64), 2)
                                        .set(0, Value::Vector(base.row_f32(r)));
                                    if tags.is_empty() {
                                        d
                                    } else {
                                        d.set(
                                            1,
                                            Value::Set(tags.iter().map(|&t| tag_name(t)).collect()),
                                        )
                                    }
                                }
                                Dataset::Bigann => {
                                    let at = gen_attrs.attributes(r as u64, None);
                                    Document::new(DocId(r as u64), 5)
                                        .set(0, Value::Vector(base.row_f32(r)))
                                        .set(1, Value::Enum(at.rights.as_str().into()))
                                        .set(2, Value::I64(i64::from(at.date)))
                                        .set(3, Value::Bool(at.flag_1))
                                        .set(4, Value::Blob(Bytes::from(vec![0u8; 32])))
                                }
                            };
                            docs.push(d);
                        }
                        let n_docs = docs.len();
                        let s = Instant::now();
                        let mut tries = 0;
                        loop {
                            match client.upsert(docs.clone()) {
                                Ok(_) => break,
                                Err(e) => {
                                    tries += 1;
                                    assert!(tries < 50, "upsert keeps failing: {e}");
                                    std::thread::sleep(Duration::from_millis(200));
                                }
                            }
                        }
                        docs.clear();
                        lat.lock()
                            .unwrap()
                            .record(s.elapsed().as_nanos() as u64)
                            .unwrap();
                        let before = done.fetch_add(n_docs, Ordering::Relaxed);
                        if (before + n_docs) / 1_000_000 != before / 1_000_000 {
                            eprintln!(
                                "ingested {}M after {:.0?}",
                                (before + n_docs) / 1_000_000,
                                t.elapsed()
                            );
                        }
                        i += writers * batch;
                    }
                });
            }
        });
        let secs = t.elapsed().as_secs_f64();
        let h = Arc::try_unwrap(lat).unwrap().into_inner().unwrap();
        eprintln!(
            "ingested rows {}..{} in {secs:.0}s ({:.0} docs/s)",
            a.start,
            base.n,
            (base.n - a.start) as f64 / secs
        );
        ingest_line = format!(
            "| ingest throughput | {:.0} docs/s ({:.0} s for rows {}..{}) |",
            (base.n - a.start) as f64 / secs,
            secs,
            a.start,
            base.n
        );
        batch_line = format!(
            "| upsert batch latency p50 / p99 / max | {:.0} / {:.0} / {:.0} ms |\n",
            ms(&h, 0.5),
            ms(&h, 0.99),
            h.max() as f64 / 1e6
        );
    }
    let mem_after_ingest = a.pids.as_deref().map(node_memory).unwrap_or_default();
    let t = Instant::now();
    settle(&addrs, &a.nodes, Duration::from_secs(a.settle_secs));
    let settle_s = t.elapsed().as_secs_f64();
    let mem_settled = a.pids.as_deref().map(node_memory).unwrap_or_default();

    // Segment counts after settling.
    let mut seg_line = String::new();
    {
        let (i, s) = a.nodes[0];
        let mut c = Client::new([(NodeId(i), s)].into_iter().collect());
        if let Ok(st) = c.status() {
            let segs: Vec<usize> = st.iter().map(|s| s.segments.len()).collect();
            let live: u64 = st.iter().map(|s| s.live_docs).sum();
            seg_line = format!(
                "| node 1 after settling | {} shards, segments per shard {:?}, live docs {live} |\n",
                st.len(),
                segs
            );
        }
    }

    // Queries.
    let run = |qs: &[Q], consistency: Consistency| -> RunResult {
        let per = Arc::new(Mutex::new(
            (0..5).map(|_| (0u64, 0.0f64, hist())).collect::<Vec<_>>(),
        ));
        let all = Arc::new(Mutex::new(hist()));
        let rec_sum = Arc::new(Mutex::new(0.0f64));
        let t = Instant::now();
        std::thread::scope(|s| {
            for th in 0..a.query_threads {
                let (addrs, per, all, rec_sum, queries) = (
                    addrs.clone(),
                    per.clone(),
                    all.clone(),
                    rec_sum.clone(),
                    &queries,
                );
                s.spawn(move || {
                    let mut client = Client::new(addrs);
                    let mut i = th;
                    while i < qs.len() {
                        let mut q = Query::new(k);
                        q.vectors.push(VectorLeg {
                            field: 0,
                            vector: queries[i].clone(),
                            ef: a.ef,
                        });
                        if let Some(f) = &qs[i].filter {
                            q.filter = f.clone();
                        }
                        let st = Instant::now();
                        let hits = client.query(q, consistency).expect("query");
                        let el = st.elapsed().as_nanos() as u64;
                        let got: Vec<u64> = hits.iter().map(|h| h.doc_id.0).collect();
                        let r = recall(&got, &qs[i].truth);
                        let b = bucket(qs[i].passing);
                        {
                            let mut p = per.lock().unwrap();
                            p[b].0 += 1;
                            p[b].1 += r;
                            p[b].2.record(el).unwrap();
                        }
                        all.lock().unwrap().record(el).unwrap();
                        *rec_sum.lock().unwrap() += r;
                        i += a.query_threads;
                    }
                });
            }
        });
        let secs = t.elapsed().as_secs_f64();
        let per = Arc::try_unwrap(per).unwrap().into_inner().unwrap();
        let all = Arc::try_unwrap(all).unwrap().into_inner().unwrap();
        let rec = *rec_sum.lock().unwrap() / qs.len().max(1) as f64;
        (per, all, rec, qs.len() as f64 / secs)
    };

    let mut md = String::new();
    writeln!(
        md,
        "# Scale benchmark through the cluster: {:?}, {} rows\n",
        a.dataset, base.n
    )?;
    writeln!(
        md,
        "- Date: {}",
        std::env::var("CAIRN_BENCH_DATE").unwrap_or_default()
    )?;
    writeln!(
        md,
        "- Cluster: {} nodes (processes on one machine, every node holds every shard); {} × {dims}-d {}",
        a.nodes.len(),
        base.n,
        match a.dataset {
            Dataset::Yfcc =>
                "YFCC-10M (Big-ANN filtered track, CC BY 4.0), tags as a Set field; official GT.public.ibin",
            Dataset::Bigann =>
                "BigANN/SIFT1B prefix with bench-gen attributes; brute-force ground truth computed by the tool",
        }
    )?;
    writeln!(
        md,
        "- Ingest: {} writer threads, batches of {} (acknowledged after Raft commit on a majority); queries: {} from {} client threads, k = {k}, ef = {}\n",
        a.writers, a.batch, qm.n, a.query_threads, a.ef
    )?;
    writeln!(md, "| metric | value |\n|---|---|")?;
    writeln!(md, "{ingest_line}")?;
    write!(md, "{batch_line}")?;
    writeln!(
        md,
        "| background builds settled ({}s without segment change) | {settle_s:.0} s after ingest |",
        a.settle_secs
    )?;
    write!(md, "{seg_line}")?;
    for (i, (rss, hwm)) in mem_after_ingest.iter().enumerate() {
        let (rss2, hwm2) = mem_settled.get(i).cloned().unwrap_or_default();
        writeln!(
            md,
            "| node {} memory: RSS after ingest / settled, peak | {rss} / {rss2}, {} |",
            i + 1,
            if hwm2.is_empty() { hwm.clone() } else { hwm2 }
        )?;
    }
    writeln!(md)?;

    for (name, qs) in &workloads {
        for consistency in [Consistency::Stale, Consistency::Linearizable] {
            let (per, all, rec, qps) = run(qs, consistency);
            let cname = match consistency {
                Consistency::Stale => "stale",
                _ => "linearizable",
            };
            eprintln!(
                "{name} / {cname}: recall {rec:.4}, p50 {:.2} ms, p99 {:.2} ms, {qps:.0} QPS",
                ms(&all, 0.5),
                ms(&all, 0.99)
            );
            writeln!(md, "## {name}, {cname} reads\n")?;
            writeln!(
                md,
                "**recall@{k} {rec:.4}**, p50 {:.2} ms, p99 {:.2} ms, max {:.1} ms, {qps:.0} QPS ({} threads)\n",
                ms(&all, 0.5),
                ms(&all, 0.99),
                all.max() as f64 / 1e6,
                a.query_threads
            )?;
            if a.dataset == Dataset::Yfcc {
                writeln!(
                    md,
                    "| rows passing | queries | recall@{k} | p50 ms | p99 ms |\n|---|---|---|---|---|"
                )?;
                for (b, (cnt, r, h)) in per.iter().enumerate() {
                    if *cnt > 0 {
                        writeln!(
                            md,
                            "| {} | {cnt} | {:.4} | {:.2} | {:.2} |",
                            BUCKETS[b],
                            r / *cnt as f64,
                            ms(h, 0.5),
                            ms(h, 0.99)
                        )?;
                    }
                }
                writeln!(md)?;
            }
        }
    }

    // Takedowns: delete, then poll every node under read-your-writes until the doc is gone.
    if a.takedowns > 0 {
        let mut vis = hist();
        let mut client = Client::new(addrs.clone());
        let mut per_node: Vec<Client> = a
            .nodes
            .iter()
            .map(|(i, s)| Client::new([(NodeId(*i), *s)].into_iter().collect()))
            .collect();
        for t in 0..a.takedowns {
            let id = DocId((t * 7919 % a.n) as u64);
            let s = Instant::now();
            let token = client.delete(vec![id]).context("delete")?[0];
            let mut worst = s.elapsed();
            for c in per_node.iter_mut() {
                loop {
                    match c.get(id, Consistency::ReadYourWrites(token)) {
                        Ok(None) => break,
                        Ok(Some(_)) => anyhow::bail!("takedown not visible under read-your-writes"),
                        Err(_) => std::thread::sleep(Duration::from_millis(5)),
                    }
                }
                worst = worst.max(s.elapsed());
            }
            vis.record(worst.as_nanos() as u64).unwrap();
        }
        writeln!(
            md,
            "## Takedowns\n\nVisible on all nodes under read-your-writes: p50 {:.1} ms, p99 {:.1} ms, max {:.1} ms ({} takedowns)\n",
            ms(&vis, 0.5),
            ms(&vis, 0.99),
            vis.max() as f64 / 1e6,
            a.takedowns
        )?;
    }
    std::fs::write(&a.out, md)?;
    eprintln!("wrote {} in {:.1?}", a.out.display(), t0.elapsed());
    Ok(())
}
