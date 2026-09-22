# End-to-end cluster benchmark

- Date: 2026-09-22
- Cluster: 3 nodes (processes on one machine), vectors: 1000000 × 128-d SIFT with bench-gen attributes (rights, date, flag_1 ≈ 1%)
- Ingest: 8 writer threads, batches of 200 documents, through the client (writes replicated to all nodes before acknowledgement)

| metric | value |
|---|---|
| ingest throughput | 4576 docs/s (218.5 s total) |
| upsert batch latency p50 / p99 | 348.4 / 995.6 ms |
| unfiltered k=10 query p50 / p99 (stale, 8 threads) | 17.45 / 544.21 ms, 229 QPS |
| filtered (≈1%) k=10 query p50 / p99 (stale) | 4.05 / 6.53 ms, 1934 QPS |
| filtered k=10 query p50 / p99 (linearizable) | 5.69 / 7.66 ms, 1354 QPS |
| takedown visible on all nodes (RYW) p50 / p99 / max | 30.9 / 40.7 / 124.5 ms (200 takedowns) |
