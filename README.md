# Cairn

Distributed multimodal database in Rust: filtered vector search + full-text + Raft, shard-per-core.

> **Status: prototype (Phases 0–4 delivered 2026-09-22).** A 3-node cluster replicates hybrid
> collections with Raft, serves filtered vector + text queries, and hides takedowns cluster-wide
> as soon as they are acknowledged. See `docs/reports/` for the evidence per phase and
> `docs/progress.md` for the journal.

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

## Run with Docker

```bash
docker build -f docker/Dockerfile -t cairn:dev .       # or podman
docker run -p 7200:7200 -v cairn-data:/data cairn:dev  # one development node, HTTP on 7200
docker compose -f docker/compose.cluster.yaml up -d    # three replicated nodes (HTTP 7201-7203)
```

Then, over the HTTP/JSON API ([docs/api/http.md](docs/api/http.md)):

```bash
curl -s 127.0.0.1:7200/v1/documents -H content-type:application/json \
  -d '{"documents":[{"id":1,"text":"nuclear energy debate","source":"tv"}]}'
# {"consistency_token":"2.2","count":1}
curl -s 127.0.0.1:7200/v1/search -H content-type:application/json \
  -d '{"text":{"field":"text","query":"nuclear"},"after":"2.2"}'
```

Configuration is by environment variables (`CAIRN_NODE_ID`, `CAIRN_PEERS`, `CAIRN_SCHEMA`,
`CAIRN_TLS_DIR`, ...): see `docker/entrypoint.sh` and ADR 0022.

## Build and test

```bash
cargo test --workspace                                   # unit, property, simulator and 3-process cluster tests
tools/scripts/campaign.sh 20000 8 bench-results/phase3-campaign.md   # simulation campaign
```

## Run a local cluster

```bash
tools/scripts/cluster.sh start data/demo tools/scripts/sift-schema.json 6 2   # 3 nodes on 127.0.0.1:7101-7103
cargo run --release -p cairn-bench -- cluster --node 1=127.0.0.1:7101 --node 2=127.0.0.1:7102 \
  --node 3=127.0.0.1:7103 --n 100000 --out /tmp/cluster.md                      # needs data/sift (tools/fetch-datasets.sh sift)
tools/scripts/cluster.sh stop
```

The Rust client is `cairn-client` (`Client::new(addrs)`, `upsert`, `delete`, `get`, `query` with
`Consistency::{Linearizable, ReadYourWrites, Stale}`).

## Results

- `bench-results/phase2-yfcc10m.md`: Big-ANN filtered track, 10M rows: recall@10 0.9994, p99 ≤ 16.5 ms.
- `bench-results/phase3-campaign.md`: 20,000 seeded fault-injection runs, zero violations.
- `bench-results/phase4-cluster-sift1m.md`: end-to-end numbers through a 3-node cluster.

## Licence

Apache-2.0

## Contributing and license

Contributions are welcome: see [CONTRIBUTING.md](CONTRIBUTING.md) (sign-off with `git commit -s`),
the [code of conduct](CODE_OF_CONDUCT.md), and [SECURITY.md](SECURITY.md) for reporting
vulnerabilities privately. Cairn is licensed under the [Apache License 2.0](LICENSE).
