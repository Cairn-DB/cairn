//! In-process benchmark of the disk-resident index (ADR 0014) on one segment: build rate,
//! recall@10 against brute force, latency warm (page cache hot) and cold (the index file's pages
//! dropped from the cache before every query with `posix_fadvise(DONTNEED)`, no root needed),
//! and resident memory. Filtered cases use bench-gen flags (1% scan path, 50% graph path).

use crate::datasets;
use crate::scale::read_u8bin_prefix;
use anyhow::Context;
use bytes::Bytes;
use cairn_bench_gen::{Correlation, GenConfig, Generator};
use cairn_core::Metric;
use cairn_index::Bitmap;
use cairn_index::diskann::{DiskAnn, DiskScratch, DiskSearch, VamanaParams};
use hdrhistogram::Histogram;
use std::fmt::Write as _;
use std::io::Write as _;
use std::os::fd::AsRawFd;
use std::path::Path;
use std::time::Instant;

fn will_need(data: &[u8]) {
    let start = data.as_ptr() as usize & !4095;
    let end = data.as_ptr() as usize + data.len();
    // SAFETY: advice on a range inside a live mapping (rounded down to its page); never changes
    // the mapping's contents.
    unsafe {
        libc::madvise(start as *mut libc::c_void, end - start, libc::MADV_WILLNEED);
    }
}

/// Drops the index pages: first unmap them from this process (`MADV_DONTNEED`: the mapping
/// stays valid and faults pages back in on access), then evict them from the page cache
/// (`POSIX_FADV_DONTNEED` only evicts pages no process maps).
fn drop_cache(file: &std::fs::File, map: (usize, usize)) {
    // SAFETY: `map` is the address and length of a live read-only file mapping owned by the
    // index; MADV_DONTNEED on a private read-only file mapping only discards page-table
    // entries, and later reads re-fault the same file contents. fadvise is advice only.
    unsafe {
        libc::madvise(map.0 as *mut libc::c_void, map.1, libc::MADV_DONTNEED);
        libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED);
    }
}

fn brute(base: &[f32], dims: usize, q: &[f32], k: usize, keep: Option<&Bitmap>) -> Vec<u32> {
    let n = base.len() / dims;
    let mut top: Vec<(f32, u32)> = Vec::with_capacity(k + 1);
    let mut out = vec![0f32; 4096];
    let mut start = 0;
    while start < n {
        let end = (start + 4096).min(n);
        cairn_index::kernels::l2_sq_batch(
            q,
            &base[start * dims..end * dims],
            &mut out[..end - start],
        );
        for (i, &d) in out[..end - start].iter().enumerate() {
            let row = (start + i) as u32;
            if keep.is_some_and(|b| !b.contains(row)) {
                continue;
            }
            if top.len() < k || d < top[k - 1].0 {
                let pos = top.partition_point(|x| x.0 <= d);
                top.insert(pos, (d, row));
                top.truncate(k);
            }
        }
        start = end;
    }
    top.into_iter().map(|x| x.1).collect()
}

/// Runs the sweep and writes `out`.
#[allow(clippy::too_many_arguments)]
pub fn disk_sweep(
    dataset: &str,
    dir: &Path,
    n: usize,
    nq: usize,
    ls: &[usize],
    r: u32,
    l_build: u32,
    passes: u32,
    tmp: &Path,
    build_threads: usize,
    out: &Path,
) -> anyhow::Result<()> {
    let t0 = Instant::now();
    let (base, queries, dims) = match dataset {
        "sift" => {
            let b = datasets::read_fvecs(&dir.join("sift_base.fvecs"), Some(n))?;
            let q = datasets::read_fvecs(&dir.join("sift_query.fvecs"), Some(nq))?;
            (b.data, q.data, b.dims)
        }
        _ => {
            let b = read_u8bin_prefix(&dir.join("base.50M.u8bin"), n)?;
            let q = read_u8bin_prefix(&dir.join("query.public.10K.u8bin"), nq)?;
            let dims = b.dims;
            (
                b.data.iter().map(|&x| f32::from(x)).collect::<Vec<f32>>(),
                q.data.iter().map(|&x| f32::from(x)).collect::<Vec<f32>>(),
                dims,
            )
        }
    };
    let n = base.len() / dims;
    let nq = queries.len() / dims;
    eprintln!("loaded {n} × {dims} in {:.1?}", t0.elapsed());
    let gen_ = Generator::new(GenConfig {
        seed: 1,
        n: n as u64,
        correlation: Correlation::Random,
        ..GenConfig::default()
    });
    let mut f1 = Bitmap::empty(n as u32);
    let mut f50 = Bitmap::empty(n as u32);
    for i in 0..n {
        let a = gen_.attributes(i as u64, None);
        if a.flag_1 {
            f1.set(i as u32);
        }
        if a.flag_50 {
            f50.set(i as u32);
        }
    }

    let params = VamanaParams {
        r,
        l_build,
        passes,
        ..VamanaParams::default()
    };
    let t = Instant::now();
    let idx = DiskAnn::build_with(
        Metric::L2,
        dims,
        base.clone(),
        &params,
        &cairn_runtime::ThreadParallel::new(build_threads),
    );
    let build_s = t.elapsed().as_secs_f64();
    eprintln!(
        "built in {build_s:.1}s ({:.0} rows/s, {build_threads} threads)",
        n as f64 / build_s
    );
    let (pq, vam) = idx.sections();
    drop(idx);
    std::fs::create_dir_all(tmp)?;
    let path = tmp.join("vamana.bin");
    {
        let mut f = std::fs::File::create(&path)?;
        f.write_all(&vam)?;
        f.sync_all()?;
    }
    let vam_len = vam.len();
    drop(vam);
    let file = std::fs::File::open(&path).context("index file")?;
    // SAFETY: the file was written and synced above and is not modified while mapped.
    let map = unsafe { memmap2::Mmap::map(&file)? };
    let _ = map.advise(memmap2::Advice::Random);
    let region = (map.as_ptr() as usize, map.len());
    let idx = DiskAnn::load_with(
        Metric::L2,
        dims,
        n as u32,
        &pq,
        Bytes::from_owner(map),
        Some(will_need),
    )?;

    let t = Instant::now();
    let truth: Vec<Vec<u32>> = (0..nq)
        .map(|i| brute(&base, dims, &queries[i * dims..(i + 1) * dims], 10, None))
        .collect();
    let truth1: Vec<Vec<u32>> = (0..nq)
        .map(|i| {
            brute(
                &base,
                dims,
                &queries[i * dims..(i + 1) * dims],
                10,
                Some(&f1),
            )
        })
        .collect();
    let truth50: Vec<Vec<u32>> = (0..nq)
        .map(|i| {
            brute(
                &base,
                dims,
                &queries[i * dims..(i + 1) * dims],
                10,
                Some(&f50),
            )
        })
        .collect();
    eprintln!("ground truth in {:.1?}", t.elapsed());

    let mut md = String::new();
    writeln!(
        md,
        "# Disk-resident index (Vamana + PQ, ADR 0014): {dataset}, one segment\n"
    )?;
    writeln!(
        md,
        "- Date: {}",
        std::env::var("CAIRN_BENCH_DATE").unwrap_or_default()
    )?;
    writeln!(
        md,
        "- Rows: {n} × {dims}-d, queries: {nq}, k = 10, single-thread queries; R = {r}, L_build = {l_build}, alpha = {}, passes = {passes}",
        params.alpha
    )?;
    writeln!(
        md,
        "- Build: {build_s:.1} s ({:.0} rows/s, {build_threads} build threads, includes PQ training)",
        n as f64 / build_s
    )?;
    writeln!(
        md,
        "- RAM: {:.1} MB of PQ codes + codebooks ({:.0} B/row); disk: {:.1} MB of node blocks ({:.0} B/row)",
        idx.resident_bytes() as f64 / 1e6,
        idx.resident_bytes() as f64 / n as f64,
        vam_len as f64 / 1e6,
        vam_len as f64 / n as f64
    )?;
    writeln!(
        md,
        "- Cold: the index file's pages are dropped from the page cache before every query (`posix_fadvise(DONTNEED)`); warm: after one full pass\n"
    )?;
    writeln!(
        md,
        "| case | L | recall@10 | warm p50 µs | warm p99 µs | cold p50 µs | cold p99 µs |\n|---|---|---|---|---|---|---|"
    )?;
    let mut s = DiskScratch::default();
    let mut run = |label: &str,
                   l: usize,
                   filter: Option<&Bitmap>,
                   scan: bool,
                   truth: &[Vec<u32>],
                   md: &mut String|
     -> anyhow::Result<()> {
        let opts = DiskSearch {
            k: 10,
            l,
            rerank: l,
            max_expansions: 0,
            beam: std::env::var("BEAM")
                .ok()
                .and_then(|b| b.parse().ok())
                .unwrap_or(4),
        };
        let search = |i: usize, s: &mut DiskScratch| {
            let q = &queries[i * dims..(i + 1) * dims];
            if scan {
                idx.search_scan(q, filter, opts, s)
            } else {
                idx.search_graph(q, filter, opts, s)
            }
        };
        let (mut warm, mut cold) = (
            Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)?,
            Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)?,
        );
        let mut rec = 0.0;
        for i in 0..nq {
            let _ = search(i, &mut s);
        }
        for i in 0..nq {
            let t = Instant::now();
            let got = search(i, &mut s);
            warm.record(t.elapsed().as_nanos() as u64 / 1000 + 1)?;
            rec += got.iter().filter(|g| truth[i].contains(&g.1)).count() as f64 / 10.0;
        }
        let cold_n = nq.min(200);
        for i in 0..cold_n {
            drop_cache(&file, region);
            let t = Instant::now();
            let _ = search(i, &mut s);
            cold.record(t.elapsed().as_nanos() as u64 / 1000 + 1)?;
        }
        let line = format!(
            "| {label} | {l} | {:.4} | {} | {} | {} | {} |",
            rec / nq as f64,
            warm.value_at_quantile(0.5),
            warm.value_at_quantile(0.99),
            cold.value_at_quantile(0.5),
            cold.value_at_quantile(0.99)
        );
        eprintln!("{line}");
        writeln!(md, "{line}")?;
        Ok(())
    };
    for &l in ls {
        run("unfiltered, graph", l, None, false, &truth, &mut md)?;
    }
    for &l in ls {
        run(
            "flag_50 (50%), graph",
            l,
            Some(&f50),
            false,
            &truth50,
            &mut md,
        )?;
    }
    run(
        "flag_1 (1%), PQ scan + rerank",
        40,
        Some(&f1),
        true,
        &truth1,
        &mut md,
    )?;
    run(
        "flag_1 (1%), PQ scan + rerank",
        100,
        Some(&f1),
        true,
        &truth1,
        &mut md,
    )?;
    std::fs::write(out, md)?;
    let _ = std::fs::remove_file(&path);
    eprintln!("wrote {} in {:.1?}", out.display(), t0.elapsed());
    Ok(())
}
