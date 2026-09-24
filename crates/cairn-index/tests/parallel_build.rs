//! ADR 0019: the batched graph builds give byte-identical results for any thread count, and
//! their recall matches the sequential builders'.

use cairn_core::{Metric, Parallel, SeededRng, Sequential};
use cairn_index::hnsw::{HnswBuilder, build_parallel};
use cairn_index::{HnswParams, SearchOptions, Vectors};
use cairn_runtime::ThreadParallel;

fn l2(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
}

fn recall(graph: &cairn_index::Hnsw, v: &Vectors, rows: &[f32], dims: usize, k: usize) -> f64 {
    let n = rows.len() / dims;
    let mut scratch = cairn_index::hnsw::SearchScratch::new(n as u32);
    let mut hit = 0usize;
    let queries: Vec<usize> = (0..200).map(|i| (i * 7919) % n).collect();
    for &qi in &queries {
        // Midpoint of two rows: not a data point, so the search has to work for it.
        let other = (qi * 31 + 17) % n;
        let q: Vec<f32> = rows[qi * dims..(qi + 1) * dims]
            .iter()
            .zip(&rows[other * dims..(other + 1) * dims])
            .map(|(a, b)| (a + b) / 2.0)
            .collect();
        let mut truth: Vec<(f32, usize)> = (0..n)
            .map(|j| (l2(&q, &rows[j * dims..(j + 1) * dims]), j))
            .collect();
        truth.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        let got = graph.search(
            v,
            &q,
            k,
            None,
            SearchOptions {
                ef: 16,
                exact: true,
                ..SearchOptions::default()
            },
            &mut scratch,
        );
        hit += got
            .iter()
            .filter(|(_, r)| truth[..k].iter().any(|t| t.1 == *r as usize))
            .count();
    }
    hit as f64 / (queries.len() * k) as f64
}

#[test]
fn hnsw_batched_build_is_thread_count_independent_and_keeps_recall() {
    let (n, dims) = (20_000usize, 32usize);
    let mut rng = SeededRng::from_seed(7);
    let rows: Vec<f32> = (0..n * dims)
        .map(|_| rng.below(1_000_000) as f32 / 1_000_000.0)
        .collect();
    let ids: Vec<u64> = (0..n as u64).map(|i| i * 3 + 1).collect();
    let v = Vectors::from_rows(Metric::L2, dims, rows.clone(), false);
    let params = HnswParams {
        m: 12,
        ef_construction: 64,
    };
    let seq = build_parallel(&v, &ids, params, &Sequential);
    for t in [3, 8] {
        let par = build_parallel(&v, &ids, params, &ThreadParallel::new(t));
        assert_eq!(par.encode(), seq.encode(), "{t} threads differ from 1");
    }
    let old = HnswBuilder::new(&v, &ids, params).finish();
    let (r_new, r_old) = (
        recall(&seq, &v, &rows, dims, 10),
        recall(&old, &v, &rows, dims, 10),
    );
    eprintln!("recall@10 batched {r_new:.4} sequential {r_old:.4}");
    assert!(
        r_new >= r_old - 0.01,
        "batched {r_new} vs sequential {r_old}"
    );
    // Uniform 32-d data with ef 16 is hard on purpose: the sequential builder gets about 0.70.
    assert!(r_new > 0.6);
}

#[test]
fn thread_parallel_runs_every_task_once() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    for t in [1, 2, 7] {
        let p = ThreadParallel::new(t);
        let seen: Vec<AtomicUsize> = (0..1000).map(|_| AtomicUsize::new(0)).collect();
        let max_worker = AtomicUsize::new(0);
        p.run(1000, &|w, i| {
            seen[i].fetch_add(1, Ordering::Relaxed);
            max_worker.fetch_max(w, Ordering::Relaxed);
        });
        assert!(seen.iter().all(|s| s.load(Ordering::Relaxed) == 1));
        assert!(max_worker.load(Ordering::Relaxed) < t);
    }
}
