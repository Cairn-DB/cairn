# ADR 0018: cost of mutual TLS

- Date: 2026-09-24. Commit: f5d9c60. Host: development machine (docs/progress.md).
- Setup: 2M BigANN rows, 3 server processes on one host, 4 shards, 64 MB memtables,
  `--sq8-only`, segment shipping on (ADR 0016), 30 s settle, 2000 queries, 500 takedowns.
- 2 runs per mode, alternating plaintext and mTLS (certificates from
  `tools/scripts/gen-certs.sh`, EC P-256; the bench client presents a certificate).
  Command: `data/run-tls-ab.sh`. Per-run files:
  `phase4-adr0016-bigann2m-ship-{plain,tls}-r{1,2}.md`.

| run | ingest (docs/s) | unfiltered p99, stale / lin. (ms) | 1% filter p99 stale (ms) | recall@10 | takedown p50 / p99 (ms) | CPU s, nodes 1/2/3 (total) |
|---|---|---|---|---|---|---|
| plain r1 | 15,157 | 8.7 / 11.0 | 2.9 | 0.9885 | 98.8 / 118.0 | 270 / 83 / 346 (699) |
| tls r1 | 16,718 | 8.2 / 10.6 | 3.0 | 0.9879 | 62.3 / 112.9 | 241 / 264 / 338 (843) |
| plain r2 | 16,695 | 8.8 / 11.6 | 3.6 | 0.9883 | 67.8 / 116.9 | 187 / 184 / 291 (662) |
| tls r2 | 16,784 | 11.2 / 15.8 | 2.7 | 0.9883 | 76.7 / 122.2 | 311 / 243 / 305 (859) |

## Reading

- **CPU: +22% total with TLS** (851 vs 681 s on average), consistently in both pairs. Most of
  it is encrypting segment shipping and Raft traffic between nodes. Each node's share
  depends on leadership, so per-node figures move between runs.
- **Ingest, recall and takedown latency:** no difference beyond run-to-run noise. On this
  host ingest is not CPU-bound after ADR 0016.
- **Query p99:** one TLS run is higher (11.2 / 15.8 ms), the other lower than both plaintext
  runs. With 2 runs per mode this is not a measurable effect.
- All nodes share one host, so no network is involved; on a real network TLS adds a handshake
  per new connection (connections are long-lived) and the same CPU per byte.
