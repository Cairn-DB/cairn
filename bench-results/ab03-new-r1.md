# End-to-end cluster benchmark

- Date: 
- Cluster: 3 nodes (processes on one machine), vectors: 1000000 × 128-d SIFT with bench-gen attributes (rights, date, flag_1 ≈ 1%)
- Ingest: 8 writer threads, batches of 200 documents, through the client (writes replicated to all nodes before acknowledgement)

| metric | value |
|---|---|
| ingest throughput | 8244 docs/s (121.3 s total) |
| upsert batch latency p50 / p99 | 169.7 / 476.6 ms |
| unfiltered k=10 query p50 / p99 (stale, 8 threads) | 21.89 / 62.88 ms, 284 QPS |
| filtered (≈1%) k=10 query p50 / p99 (stale) | 7.67 / 19.05 ms, 896 QPS |
| filtered k=10 query p50 / p99 (linearizable) | 2.33 / 366.22 ms, 521 QPS |
| takedown visible on all nodes (RYW) p50 / p99 / max | 69.1 / 112.3 / 113.8 ms (200 takedowns) |
