//! MS MARCO passage ranking (dev small): BM25 MRR@10 of the Cairn text index, built as
//! 1M-passage segments with per-segment statistics (ADR 0005), merged like the engine does.

use anyhow::{Context, ensure};
use cairn_core::HashMap;
use cairn_index::text::{Bm25Params, TextIndex, TextQuery};
use std::fmt::Write as _;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::time::Instant;

fn read_tsv2(path: &Path) -> anyhow::Result<Vec<(String, String)>> {
    let f = std::fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut out = Vec::new();
    for line in BufReader::new(f).lines() {
        let line = line?;
        let mut it = line.splitn(2, '\t');
        let (a, b) = (it.next().unwrap_or(""), it.next().unwrap_or(""));
        out.push((a.to_owned(), b.to_owned()));
    }
    Ok(out)
}

pub fn msmarco(
    dir: &Path,
    limit: Option<usize>,
    segment_rows: usize,
    k1: f32,
    b: f32,
    out: &Path,
) -> anyhow::Result<()> {
    let t0 = Instant::now();
    // collection.tsv: pid \t passage (pids are 0..N-1 in order).
    let f = std::fs::File::open(dir.join("collection.tsv")).context("collection.tsv")?;
    let mut texts: Vec<String> = Vec::new();
    let mut pids: Vec<u32> = Vec::new();
    for line in BufReader::new(f).lines() {
        let line = line?;
        let mut it = line.splitn(2, '\t');
        let pid: u32 = it.next().unwrap_or("0").parse().context("pid")?;
        pids.push(pid);
        texts.push(it.next().unwrap_or("").to_owned());
        if limit.is_some_and(|l| texts.len() >= l) {
            break;
        }
    }
    let n = texts.len();
    eprintln!("read {n} passages in {:.1?}", t0.elapsed());
    let queries = read_tsv2(&dir.join("queries.dev.small.tsv"))?;
    let mut qrels: HashMap<String, Vec<u32>> = HashMap::default();
    for line in BufReader::new(std::fs::File::open(dir.join("qrels.dev.small.tsv"))?).lines() {
        let line = line?;
        let cols: Vec<&str> = line.split_whitespace().collect();
        ensure!(cols.len() >= 4, "qrels line");
        qrels
            .entry(cols[0].to_owned())
            .or_default()
            .push(cols[2].parse()?);
    }
    // Build segments in parallel.
    let t = Instant::now();
    let chunks: Vec<&[String]> = texts.chunks(segment_rows).collect();
    let indexes: Vec<TextIndex> = std::thread::scope(|s| {
        let hs: Vec<_> = chunks
            .iter()
            .map(|c| {
                s.spawn(move || TextIndex::build_from_texts(0, c.iter().map(|t| Some(t.as_str()))))
            })
            .collect();
        hs.into_iter()
            .map(|h| h.join().expect("build panicked"))
            .collect()
    });
    let build_s = t.elapsed().as_secs_f64();
    eprintln!("built {} segments in {build_s:.1}s", indexes.len());
    let params = Bm25Params { k1, b };
    let pid_of = |seg: usize, row: u32| pids[seg * segment_rows + row as usize];
    let t = Instant::now();
    let mut mrr = 0.0f64;
    let mut r100 = 0.0f64;
    let mut evaluated = 0usize;
    for (qid, qtext) in &queries {
        let Some(rel) = qrels.get(qid) else { continue };
        let mut all: Vec<(f32, u32)> = Vec::new();
        for (si, idx) in indexes.iter().enumerate() {
            let res = idx.search(
                &TextQuery {
                    field: 0,
                    text: qtext.clone(),
                    all_terms: false,
                },
                100,
                None,
                params,
            );
            all.extend(res.into_iter().map(|(s, row)| (s, pid_of(si, row))));
        }
        all.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
        all.truncate(100);
        if let Some(pos) = all.iter().position(|(_, pid)| rel.contains(pid)) {
            if pos < 10 {
                mrr += 1.0 / (pos as f64 + 1.0);
            }
            r100 += 1.0;
        }
        evaluated += 1;
    }
    let qps = evaluated as f64 / t.elapsed().as_secs_f64();
    let mut md = String::new();
    writeln!(
        md,
        "# MS MARCO passage dev (small): BM25 with the Cairn text index\n"
    )?;
    writeln!(
        md,
        "- Date: {}",
        std::env::var("CAIRN_BENCH_DATE").unwrap_or_default()
    )?;
    writeln!(
        md,
        "- Passages: {n}{}; segments of {segment_rows} rows ({}), per-segment BM25 statistics merged by score",
        if limit.is_some() { " (prefix)" } else { "" },
        indexes.len()
    )?;
    writeln!(
        md,
        "- Tokenizer: Unicode alphanumeric runs, lowercased, no stemming, no stopwords; k1 = {k1}, b = {b}"
    )?;
    writeln!(
        md,
        "- Queries evaluated: {evaluated} (dev small with qrels)"
    )?;
    writeln!(
        md,
        "- Build: {build_s:.1} s wall ({} threads)\n",
        indexes.len()
    )?;
    writeln!(
        md,
        "| metric | value |\n|---|---|\n| MRR@10 | {:.4} |\n| Recall@100 | {:.4} |\n| queries/s (1 thread, {} segments) | {:.0} |",
        mrr / evaluated as f64,
        r100 / evaluated as f64,
        indexes.len(),
        qps
    )?;
    std::fs::write(out, md)?;
    eprintln!(
        "MRR@10 = {:.4}, R@100 = {:.4}, {qps:.0} QPS; wrote {}",
        mrr / evaluated as f64,
        r100 / evaluated as f64,
        out.display()
    );
    Ok(())
}
