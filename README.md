# Cairn

Distributed multimodal database in Rust: filtered vector search + full-text + Raft, shard-per-core.

> **Status: pre-alpha, Phase 0 (architecture).** Nothing here is usable yet.

Cairn targets workloads where one query mixes embeddings (image, audio, text), full-text and
structured filters, and where consistency between them matters (rights management, takedowns,
permissions).

- **One engine, one query plan** instead of Elasticsearch + a vector DB + glue code.
- **Filtered ANN** that stays fast and accurate when filters are very selective.
- **Raft-replicated shards** so every replica builds the same index from the same log.
- **Thread-per-core** execution for stable tail latency.
- **Deterministic simulation testing** from day one.

See [`SPEC.md`](SPEC.md) for goals, decisions, roadmap and success criteria, and
[`docs/adr/`](docs/adr) for architecture decision records.

## Build

```bash
cargo check --workspace
cargo test --workspace
```

## Licence

Apache-2.0
