# End-to-end cluster benchmark

- Date: 2026-09-22
- Cluster: 3 nodes (processes on one machine), vectors: 1000000 × 128-d SIFT with bench-gen attributes (rights, date, flag_1 ≈ 1%)
- Ingest: 8 writer threads, batches of 200 documents, through the client (writes replicated to all nodes before acknowledgement)

| metric | value |
|---|---|
| ingest throughput | 4100 docs/s (243.9 s total) |
| upsert batch latency p50 / p99 | 378.5 / 1792.0 ms |
| unfiltered k=10 query p50 / p99 (stale, 8 threads) | 16.56 / 27.79 ms, 475 QPS |
| filtered (≈1%) k=10 query p50 / p99 (stale) | 3.68 / 6.16 ms, 2121 QPS |
| filtered k=10 query p50 / p99 (linearizable) | 5.26 / 7.34 ms, 1466 QPS |
| takedown visible on all nodes (RYW) p50 / p99 / max | 98.6 / 106.0 / 108.1 ms (200 takedowns) |
