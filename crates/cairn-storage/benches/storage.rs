//! Phase 1 storage benchmarks on the real disk (blocking reactor).
//!
//! Run: `cargo bench -p cairn-storage --bench storage`. Data goes under `data/bench-tmp`
//! (the repository's gitignored data directory, on the NVMe device, never tmpfs).

#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    clippy::await_holding_refcell_ref,
    missing_docs
)]

use bytes::Bytes;
use cairn_core::{
    Disk, DocId, Document, FieldDef, FieldKind, LogIndex, Metric, NodeId, OpenMode, Runtime,
    Schema, Term, Value,
};
use cairn_runtime::Executor;
use cairn_runtime::blocking::{BlockingReactor, RealRuntime};
use cairn_storage::columns::{DocStore, write_columns};
use cairn_storage::{Log, LogConfig, LogEntry, SegmentReader, SegmentWriter};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::cell::RefCell;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::rc::Rc;

fn bench_dir() -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../data/bench-tmp");
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn runtime(name: &str) -> (Executor<BlockingReactor>, RealRuntime) {
    let dir = bench_dir().join(name);
    let _ = std::fs::remove_dir_all(&dir);
    let ex = Executor::new(BlockingReactor::new());
    let rt = RealRuntime::new(ex.handle(), &dir, NodeId(1)).unwrap();
    (ex, rt)
}

/// WAL append + fsync per batch, through `Log`, versus a raw `std::fs` append + fsync baseline.
fn wal_append(c: &mut Criterion) {
    let record = Bytes::from(vec![0xABu8; 4096 - 24]);
    let mut group = c.benchmark_group("wal_append_fsync");
    for &batch in &[1usize, 16, 64] {
        group.throughput(Throughput::Elements(batch as u64));
        group.bench_with_input(BenchmarkId::new("cairn_log", batch), &batch, |b, &batch| {
            let (mut ex, rt) = runtime("wal");
            let log = Rc::new(RefCell::new(ex.block_on({
                let rt = rt.clone();
                async move { Log::open(rt, "log", LogConfig::default()).await.unwrap() }
            })));
            let mut next = 1u64;
            b.iter(|| {
                let entries: Vec<LogEntry> = (0..batch as u64)
                    .map(|k| LogEntry {
                        index: LogIndex(next + k),
                        term: Term(1),
                        payload: record.clone(),
                    })
                    .collect();
                next += batch as u64;
                let log = log.clone();
                ex.block_on(async move {
                    let mut log = log.borrow_mut();
                    log.append(&entries).await.unwrap();
                    log.sync().await.unwrap();
                });
            });
        });
        group.bench_with_input(
            BenchmarkId::new("std_fs_baseline", batch),
            &batch,
            |b, &batch| {
                let dir = bench_dir().join("wal-std");
                let _ = std::fs::remove_dir_all(&dir);
                std::fs::create_dir_all(&dir).unwrap();
                let mut f = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(dir.join("log"))
                    .unwrap();
                let buf = vec![0xABu8; 4096 * batch];
                b.iter(|| {
                    f.write_all(&buf).unwrap();
                    f.sync_data().unwrap();
                });
            },
        );
    }
    group.finish();
}

fn schema() -> Schema {
    Schema::new(vec![
        FieldDef {
            name: "img".into(),
            kind: FieldKind::Vector {
                dims: 128,
                metric: Metric::L2,
            },
        },
        FieldDef {
            name: "year".into(),
            kind: FieldKind::I64,
        },
        FieldDef {
            name: "title".into(),
            kind: FieldKind::Text,
        },
    ])
    .unwrap()
}

fn docs(n: u64) -> Vec<Document> {
    (0..n)
        .map(|i| {
            Document::new(DocId(i), 3)
                .set(
                    0,
                    Value::Vector(
                        (0..128)
                            .map(|d| (i as f32 * 0.001 + d as f32).sin())
                            .collect(),
                    ),
                )
                .set(1, Value::I64(1990 + (i % 36) as i64))
                .set(2, Value::Text(format!("document number {i}")))
        })
        .collect()
}

/// Building a 100k-row segment (128-d vectors) and reading it back.
fn segment_build_and_read(c: &mut Criterion) {
    let n = 100_000u64;
    let all = Rc::new(docs(n));
    let mut group = c.benchmark_group("segment_100k_128d");
    group.sample_size(10);
    group.throughput(Throughput::Elements(n));
    group.bench_function("write_columns_and_finish", |b| {
        let (mut ex, rt) = runtime("seg");
        let mut k = 0;
        b.iter(|| {
            k += 1;
            let path = format!("s{k}.seg");
            let s = schema();
            let (rt, all) = (rt.clone(), all.clone());
            ex.block_on(async move {
                let refs: Vec<&Document> = all.iter().collect();
                let mut w = SegmentWriter::create(rt, &path).await.unwrap();
                write_columns(&mut w, &s, &refs).await.unwrap();
                w.finish().await.unwrap();
            });
        });
    });
    // Prepare one segment for the read benchmarks.
    let (mut ex, rt) = runtime("segread");
    ex.block_on({
        let (rt, all) = (rt.clone(), all.clone());
        let s = schema();
        async move {
            let refs: Vec<&Document> = all.iter().collect();
            let mut w = SegmentWriter::create(rt, "r.seg").await.unwrap();
            write_columns(&mut w, &s, &refs).await.unwrap();
            w.finish().await.unwrap();
        }
    });
    let reader = Rc::new(ex.block_on({
        let rt = rt.clone();
        async move { SegmentReader::open(rt, "r.seg").await.unwrap() }
    }));
    let store = Rc::new(ex.block_on({
        let reader = reader.clone();
        async move { DocStore::open(&reader).await.unwrap() }
    }));
    group.bench_function("read_all_rows", |b| {
        b.iter(|| {
            let (store, reader) = (store.clone(), reader.clone());
            ex.block_on(async move { store.read_all(&reader, |_| false).await.unwrap().len() })
        });
    });
    group.finish();
    let mut group = c.benchmark_group("segment_point_read");
    group.throughput(Throughput::Elements(1));
    let mut i = 0u64;
    group.bench_function("get_doc_by_id", |b| {
        b.iter(|| {
            i = (i.wrapping_mul(6364136223846793005).wrapping_add(1)) % n;
            let row = store.row_of(DocId(i)).unwrap();
            let (store, reader) = (store.clone(), reader.clone());
            ex.block_on(async move { store.read_doc(&reader, row).await.unwrap() })
        });
    });
    group.finish();
    // Raw pread of 64 KiB pages: what the disk itself does.
    let mut group = c.benchmark_group("raw_pread_64k");
    group.throughput(Throughput::Bytes(65536));
    let f = ex.block_on({
        let rt = rt.clone();
        async move { rt.disk().open("r.seg", OpenMode::Read).await.unwrap() }
    });
    let len = std::fs::metadata(bench_dir().join("segread/r.seg"))
        .unwrap()
        .len();
    let mut buf = vec![0u8; 65536];
    let mut off = 0u64;
    group.bench_function("std_pread", |b| {
        b.iter(|| {
            off = (off + 65536 * 7) % (len - 65536);
            f.read_exact_at(&mut buf, off).unwrap();
        });
    });
    group.finish();
}

criterion_group!(benches, wal_append, segment_build_and_read);
criterion_main!(benches);
