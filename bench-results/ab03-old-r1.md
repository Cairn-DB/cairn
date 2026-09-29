# End-to-end cluster benchmark

- Date: 
- Cluster: 3 nodes (processes on one machine), vectors: 1000000 × 128-d SIFT with bench-gen attributes (rights, date, flag_1 ≈ 1%)
- Ingest: 8 writer threads, batches of 200 documents, through the client (writes replicated to all nodes before acknowledgement)

| metric | value |
|---|---|
| ingest throughput | 7818 docs/s (127.9 s total) |
| upsert batch latency p50 / p99 | 172.1 / 554.2 ms |
| unfiltered k=10 query p50 / p99 (stale, 8 threads) | 22.76 / 60.75 ms, 258 QPS |
| filtered (≈1%) k=10 query p50 / p99 (stale) | 7.99 / 18.40 ms, 887 QPS |
| filtered k=10 query p50 / p99 (linearizable) | 1.21 / 235.14 ms, 647 QPS |
| takedown visible on all nodes (RYW) p50 / p99 / max | 61.3 / 107.9 / 107.9 ms (200 takedowns) |
