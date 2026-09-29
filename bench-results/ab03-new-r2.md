# End-to-end cluster benchmark

- Date: 
- Cluster: 3 nodes (processes on one machine), vectors: 1000000 × 128-d SIFT with bench-gen attributes (rights, date, flag_1 ≈ 1%)
- Ingest: 8 writer threads, batches of 200 documents, through the client (writes replicated to all nodes before acknowledgement)

| metric | value |
|---|---|
| ingest throughput | 9070 docs/s (110.3 s total) |
| upsert batch latency p50 / p99 | 163.2 / 482.3 ms |
| unfiltered k=10 query p50 / p99 (stale, 8 threads) | 22.15 / 196.61 ms, 275 QPS |
| filtered (≈1%) k=10 query p50 / p99 (stale) | 7.46 / 19.89 ms, 944 QPS |
| filtered k=10 query p50 / p99 (linearizable) | 1.59 / 273.15 ms, 647 QPS |
| takedown visible on all nodes (RYW) p50 / p99 / max | 68.0 / 102.8 / 105.8 ms (200 takedowns) |
