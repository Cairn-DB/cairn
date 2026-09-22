//! Distance kernel benchmarks: scalar vs AVX2 vs AVX-512, f32 and SQ8, several dimensions.
#![allow(missing_docs)]

use cairn_index::kernels::{Sq8Params, available_levels};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

fn bench(c: &mut Criterion) {
    let n = 1024usize;
    for &dims in &[128usize, 192, 512, 768] {
        let mut r = cairn_core::SeededRng::from_seed(dims as u64);
        let mut rnd = || (r.next_u64() % 2001) as f32 / 1000.0 - 1.0;
        let q: Vec<f32> = (0..dims).map(|_| rnd()).collect();
        let rows: Vec<f32> = (0..n * dims).map(|_| rnd()).collect();
        let p = Sq8Params::fit(&rows, dims);
        let mut q8 = vec![0u8; n * dims];
        for (i, row) in rows.chunks_exact(dims).enumerate() {
            p.quantize(row, &mut q8[i * dims..(i + 1) * dims]);
        }
        let mut out = vec![0f32; n];
        let mut g = c.benchmark_group(format!("l2_batch_{dims}d_x{n}"));
        g.throughput(Throughput::Elements(n as u64));
        for k in available_levels() {
            g.bench_with_input(BenchmarkId::new("f32", k.name), &k, |b, k| {
                b.iter(|| (k.l2_sq_batch)(&q, &rows, &mut out));
            });
            g.bench_with_input(BenchmarkId::new("sq8", k.name), &k, |b, k| {
                b.iter(|| (k.l2_sq_u8_batch)(&q, &q8, &p.min, &p.scale, &mut out));
            });
            g.bench_with_input(BenchmarkId::new("dot_f32", k.name), &k, |b, k| {
                b.iter(|| (k.dot_batch)(&q, &rows, &mut out));
            });
        }
        g.finish();
    }
}

criterion_group!(benches, bench);
criterion_main!(benches);
