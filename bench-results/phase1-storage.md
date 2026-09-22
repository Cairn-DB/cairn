# Phase 1 storage benchmarks

- Date: 2026-09-22. Commit: see `git log` entry "M1.5+M1.6".
- Command: `cargo bench -p cairn-storage --bench storage` (criterion 0.8, release profile,
  `lto = "fat"`, `codegen-units = 1`).
- Hardware: AMD Ryzen 7 8845HS (8 cores / 16 threads), 58 GB RAM, NVMe (`/dev/nvme1n1`, ext4,
  consumer drive: volatile write cache, `fdatasync` costs 2–4 ms), Linux 7.2.5 (Fedora 44).
- Reactor: blocking (`std::fs` `pwrite`/`fdatasync` inline on the core). The io_uring reactor and
  the `glommio` comparison planned in ADR 0002 are **not done** in Phase 1; see the note below.
- Data directory: `data/bench-tmp` on the NVMe device (not tmpfs). Page cache warm for reads.

## WAL append + fdatasync (4 KiB records)

| batch (records per fsync) | Cairn `Log` (mean) | raw `std::fs` append+fdatasync (mean) | Cairn records/s |
|---|---|---|---|
| 1 | 3.65 ms | 2.09 ms | 274 |
| 16 | 2.23 ms | 4.21 ms | 7,164 |
| 64 | 4.41 ms | 4.34 ms | 14,510 |

Reading: the per-record cost of the log (checksum, framing, offset index) is not measurable
against the fsync cost; the batch-1 and batch-16 rows disagree in opposite directions, which is
fsync jitter on this drive (criterion reported 4–12% outliers). Group commit is mandatory for
throughput: 64 records per fsync gives about 57 MB/s of 4 KiB records. Tail latencies are not
reported here because criterion reports means and confidence intervals only; `hdrhistogram`
percentiles will come from the end-to-end write benchmark in Phase 4.

## Segment container, 100,000 rows × (128-d f32 vector + i64 + text)

| Operation | Time (mean) | Rate |
|---|---|---|
| build columns + write + fsync + rename (~55 MB) | 120 ms | 832k rows/s (~460 MB/s) |
| read all rows into `Document`s (whole-section reads) | 118 ms | 848k rows/s |
| point read by id (binary search + per-field `pread`) | 5.2 µs | 192k reads/s |
| raw 64 KiB `pread` (page cache) | 3.6 µs | 16.8 GiB/s |

Reading: a 100k-row segment is built in about 120 ms without any index work; index build will
dominate segment build in Phase 2. Point reads do 2–3 syscalls per field (nulls byte, offsets,
bytes); that is the cost to cut first if payload fetch shows up in query latency.

## What this does not show

- No io_uring numbers: the runtime spike in ADR 0002 moves to M4.2, where the real network
  arrives too. The `Disk` trait is completion-shaped, so the swap does not touch engine code.
- No RocksDB comparison (dropped in ADR 0012).
- No p99: criterion only. Phase 2's query benchmarks use `hdrhistogram`.
