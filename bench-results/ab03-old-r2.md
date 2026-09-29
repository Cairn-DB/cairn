# End-to-end cluster benchmark

- Date: 
- Cluster: 3 nodes (processes on one machine), vectors: 1000000 × 128-d SIFT with bench-gen attributes (rights, date, flag_1 ≈ 1%)
- Ingest: 8 writer threads, batches of 200 documents, through the client (writes replicated to all nodes before acknowledgement)

| metric | value |
|---|---|
| ingest throughput | 8720 docs/s (114.7 s total) |
| upsert batch latency p50 / p99 | 165.0 / 474.0 ms |
| unfiltered k=10 query p50 / p99 (stale, 8 threads) | 22.25 / 95.75 ms, 280 QPS |
| filtered (≈1%) k=10 query p50 / p99 (stale) | 7.51 / 18.10 ms, 938 QPS |
| filtered k=10 query p50 / p99 (linearizable) | 9.75 / 223.35 ms, 576 QPS |
| takedown visible on all nodes (RYW) p50 / p99 / max | 87.8 / 102.0 / 102.2 ms (200 takedowns) |
