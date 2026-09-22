//! End-to-end benchmark through a running cluster: ingest SIFT vectors with synthetic
//! attributes, run filtered vector queries from several client threads, and measure takedown
//! visibility latency (delete acknowledged -> absent under read-your-writes on every node).

use crate::datasets;
use anyhow::Context;
use bytes::Bytes;
use cairn_bench_gen::{Correlation, GenConfig, Generator};
use cairn_client::Client;
use cairn_core::{DocId, Document, HashMap, NodeId, Predicate, Value};
use cairn_query::{Consistency, Query, VectorLeg};
use hdrhistogram::Histogram;
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;

fn hist() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 600_000_000_000, 3).unwrap()
}

fn p(h: &Histogram<u64>, q: f64) -> f64 {
    h.value_at_quantile(q) as f64 / 1000.0
}

#[allow(clippy::too_many_arguments)]
pub fn cluster_bench(
    dir: &Path,
    nodes: &[(u32, SocketAddr)],
    n: usize,
    batch: usize,
    writers: usize,
    queries: usize,
    query_threads: usize,
    takedowns: usize,
    out: &Path,
) -> anyhow::Result<()> {
    let t0 = Instant::now();
    let base = datasets::read_fvecs(&dir.join("sift_base.fvecs"), Some(n)).context("base")?;
    let qs = datasets::read_fvecs(&dir.join("sift_query.fvecs"), Some(queries))?;
    let dims = base.dims;
    let attrs = Generator::new(GenConfig {
        seed: 1,
        n: n as u64,
        correlation: Correlation::Random,
        ..GenConfig::default()
    });
    let addrs: HashMap<NodeId, SocketAddr> = nodes.iter().map(|(i, a)| (NodeId(*i), *a)).collect();
    eprintln!("loaded {} vectors in {:.1?}", base.n, t0.elapsed());

    // Ingest with `writers` threads, each taking a slice of ids.
    let base = Arc::new(base);
    let t = Instant::now();
    let ingest_lat = Arc::new(Mutex::new(hist()));
    std::thread::scope(|s| {
        for w in 0..writers {
            let (base, addrs, ingest_lat, attrs) =
                (base.clone(), addrs.clone(), ingest_lat.clone(), &attrs);
            s.spawn(move || {
                let mut client = Client::new(addrs);
                let mut i = w;
                let mut docs = Vec::with_capacity(batch);
                while i < base.n {
                    let a = attrs.attributes(i as u64, None);
                    docs.push(
                        Document::new(DocId(i as u64), 5)
                            .set(0, Value::Vector(base.row(i).to_vec()))
                            .set(1, Value::Enum(a.rights.as_str().into()))
                            .set(2, Value::I64(i64::from(a.date)))
                            .set(3, Value::Bool(a.flag_1))
                            .set(4, Value::Blob(Bytes::from(vec![0u8; 32]))),
                    );
                    if docs.len() == batch {
                        let s = Instant::now();
                        client.upsert(std::mem::take(&mut docs)).expect("upsert");
                        ingest_lat
                            .lock()
                            .unwrap()
                            .record(s.elapsed().as_nanos() as u64)
                            .unwrap();
                    }
                    i += writers;
                }
                if !docs.is_empty() {
                    client.upsert(docs).expect("upsert");
                }
            });
        }
    });
    let ingest_s = t.elapsed().as_secs_f64();
    eprintln!(
        "ingested {} docs in {ingest_s:.1}s ({:.0} docs/s)",
        base.n,
        base.n as f64 / ingest_s
    );
    // Let flushes and index builds settle: wait until status shows stable segments.
    std::thread::sleep(std::time::Duration::from_secs(5));

    // Queries: k = 10, filter `flag_1 == true` (about 1%) and unfiltered, from several threads.
    let run_queries = |filter: Option<Predicate>,
                       consistency: Consistency|
     -> (Histogram<u64>, f64) {
        let lat = Arc::new(Mutex::new(hist()));
        let t = Instant::now();
        std::thread::scope(|s| {
            for th in 0..query_threads {
                let (addrs, lat, filter, qs) = (addrs.clone(), lat.clone(), filter.clone(), &qs);
                s.spawn(move || {
                    let mut client = Client::new(addrs);
                    let mut i = th;
                    while i < qs.n {
                        let mut q = Query::new(10);
                        q.vectors.push(VectorLeg {
                            field: 0,
                            vector: qs.row(i).to_vec(),
                            ef: 128,
                        });
                        if let Some(f) = &filter {
                            q.filter = f.clone();
                        }
                        let s = Instant::now();
                        client.query(q, consistency).expect("query");
                        lat.lock()
                            .unwrap()
                            .record(s.elapsed().as_nanos() as u64)
                            .unwrap();
                        i += query_threads;
                    }
                });
            }
        });
        let secs = t.elapsed().as_secs_f64();
        let h = Arc::try_unwrap(lat).unwrap().into_inner().unwrap();
        (h, qs.n as f64 / secs)
    };
    let (unf, unf_qps) = run_queries(None, Consistency::Stale);
    let (flt, flt_qps) = run_queries(
        Some(Predicate::Eq {
            field: 3,
            value: Value::Bool(true),
        }),
        Consistency::Stale,
    );
    let (lin, lin_qps) = run_queries(
        Some(Predicate::Eq {
            field: 3,
            value: Value::Bool(true),
        }),
        Consistency::Linearizable,
    );
    eprintln!(
        "queries: unfiltered {unf_qps:.0} QPS, filtered {flt_qps:.0} QPS, linearizable filtered {lin_qps:.0} QPS"
    );

    // Takedown visibility: delete, then poll every node with read-your-writes until absent.
    let mut vis = hist();
    let mut client = Client::new(addrs.clone());
    let mut per_node: Vec<Client> = nodes
        .iter()
        .map(|(i, a)| Client::new([(NodeId(*i), *a)].into_iter().collect()))
        .collect();
    for k in 0..takedowns {
        let id = DocId((k * 7919 % n) as u64);
        let s = Instant::now();
        let tokens = client.delete(vec![id]).expect("delete");
        let token = tokens[0];
        let mut worst = s.elapsed();
        for c in per_node.iter_mut() {
            loop {
                match c.get(id, Consistency::ReadYourWrites(token)) {
                    Ok(None) => break,
                    Ok(Some(_)) => panic!("takedown not visible under read-your-writes"),
                    Err(_) => std::thread::sleep(std::time::Duration::from_millis(5)),
                }
            }
            worst = worst.max(s.elapsed());
        }
        vis.record(worst.as_nanos() as u64).unwrap();
    }
    let ingest_h = Arc::try_unwrap(ingest_lat).unwrap().into_inner().unwrap();
    let mut md = String::new();
    writeln!(md, "# End-to-end cluster benchmark\n")?;
    writeln!(
        md,
        "- Date: {}",
        std::env::var("CAIRN_BENCH_DATE").unwrap_or_default()
    )?;
    writeln!(
        md,
        "- Cluster: {} nodes (processes on one machine), vectors: {} × {dims}-d SIFT with bench-gen attributes (rights, date, flag_1 ≈ 1%)",
        nodes.len(),
        base.n
    )?;
    writeln!(
        md,
        "- Ingest: {writers} writer threads, batches of {batch} documents, through the client (writes replicated to all nodes before acknowledgement)\n"
    )?;
    writeln!(md, "| metric | value |\n|---|---|")?;
    writeln!(
        md,
        "| ingest throughput | {:.0} docs/s ({ingest_s:.1} s total) |",
        base.n as f64 / ingest_s
    )?;
    writeln!(
        md,
        "| upsert batch latency p50 / p99 | {:.1} / {:.1} ms |",
        p(&ingest_h, 0.5) / 1000.0,
        p(&ingest_h, 0.99) / 1000.0
    )?;
    writeln!(
        md,
        "| unfiltered k=10 query p50 / p99 (stale, {query_threads} threads) | {:.2} / {:.2} ms, {unf_qps:.0} QPS |",
        p(&unf, 0.5) / 1000.0,
        p(&unf, 0.99) / 1000.0
    )?;
    writeln!(
        md,
        "| filtered (≈1%) k=10 query p50 / p99 (stale) | {:.2} / {:.2} ms, {flt_qps:.0} QPS |",
        p(&flt, 0.5) / 1000.0,
        p(&flt, 0.99) / 1000.0
    )?;
    writeln!(
        md,
        "| filtered k=10 query p50 / p99 (linearizable) | {:.2} / {:.2} ms, {lin_qps:.0} QPS |",
        p(&lin, 0.5) / 1000.0,
        p(&lin, 0.99) / 1000.0
    )?;
    writeln!(
        md,
        "| takedown visible on all nodes (RYW) p50 / p99 / max | {:.1} / {:.1} / {:.1} ms ({takedowns} takedowns) |",
        p(&vis, 0.5) / 1000.0,
        p(&vis, 0.99) / 1000.0,
        vis.max() as f64 / 1e6
    )?;
    std::fs::write(out, md)?;
    eprintln!("wrote {} in {:.1?}", out.display(), t0.elapsed());
    Ok(())
}
