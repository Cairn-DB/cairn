# Cairn

**A distributed hybrid search database where a deletion is final.**

Cairn combines vector search, full-text search (BM25) and structured filters in one engine and
one query plan, replicated with Raft. It is written in Rust.

A takedown is a single replicated write. Once it is acknowledged, the document disappears from
every replica, and a client holding its consistency token will never read it again, through
any node. Cairn is built for systems that must be able to prove that property, such as RAG
over regulated data, rights management and permission changes.

> **Status: pre-release.** The engine, replication, HTTP API and Docker image work and are
> measured at 50 million vectors on a 3-machine cluster (below). It is not production-ready yet:
> see [Known limits](#known-limits) and the [roadmap](ROADMAP.md). Interfaces may still change.

## Why

A typical stack today runs a search engine, a vector database and glue code that merges their
results. Filtered queries waste most of their candidates, tail latency adds up across systems,
and a deleted document can stay visible in the vector index long after the source of truth
has dropped it. Cairn replaces that with:

- **One query**: vector legs, a text leg and filters, fused server-side (RRF or weights).
- **Filtered ANN** that stays accurate when filters select 1% of the data or less.
- **Raft-replicated shards**: every replica applies the same log, so a takedown has one
  well-defined moment when it takes effect.
- **Read-your-writes tokens**: every write returns a token. Passing it back on a read makes
  that read reflect the write, on any node, even one that lags.
- **Deterministic simulation testing**: the whole cluster runs in a seeded simulator with
  injected crashes, partitions, slow disks and message loss. 60,000 seeds pass with zero
  safety violations.

## Quick start (Docker)

```bash
docker build -f docker/Dockerfile -t cairn:dev .       # or podman
docker run -p 7200:7200 -v cairn-data:/data cairn:dev  # one development node, HTTP on 7200
docker compose -f docker/compose.cluster.yaml up -d    # three replicated nodes (HTTP 7201-7203)
```

```bash
# Write, then search with the write's token: the result reflects it.
curl -s 127.0.0.1:7200/v1/documents -H content-type:application/json \
  -d '{"documents":[{"id":1,"text":"nuclear energy debate","source":"tv"}]}'
# {"consistency_token":"2.2","count":1}
curl -s 127.0.0.1:7200/v1/search -H content-type:application/json \
  -d '{"text":{"field":"text","query":"nuclear"},"after":"2.2"}'

# Take it down: with the new token, no node returns it again.
curl -s -X DELETE 127.0.0.1:7200/v1/documents/1
```

The HTTP/JSON API is documented in [docs/api/http.md](docs/api/http.md), with an
[OpenAPI description](docs/api/openapi.yaml). Configuration uses environment variables
(`CAIRN_NODE_ID`, `CAIRN_PEERS`, `CAIRN_SCHEMA`, `CAIRN_TLS_DIR`, ...): see
`docker/entrypoint.sh` and [ADR 0022](docs/adr/0022-packaging-and-distribution.md).

## Measured results

50M BigANN vectors (128 dimensions) on 3 × GCP n2-highmem-8 (8 vCPU, 64 GB), every node holding
every shard, 1,000 queries per kind from 8 client threads, k = 10
([bench-results/phase4-gcp-bigann50m-run8.md](bench-results/phase4-gcp-bigann50m-run8.md)):

| metric | result | target |
|---|---|---|
| unfiltered p99, stale / linearizable | 42 / 44 ms | < 100 ms |
| 1%-selective filter p99, stale / linearizable | 42 / 42 ms | < 100 ms |
| throughput, unfiltered | 244 QPS | |
| recall@10, unfiltered / filtered | 0.985 / 0.989 | |
| takedown visible on all 3 machines, p99 | 102 ms | < 1 s |
| ingest, 50M rows through Raft | 8,343 docs/s | |

These queries were measured after a restart. On the node that had just ingested, glibc's
fragmented heap made queries 3 to 4 times slower. The server now uses mimalloc
([ADR 0029](docs/adr/0029-mimalloc.md)), which removes that gap in local tests. The fix is
still to be confirmed at 50M.

Other evidence: [Big-ANN filtered track at 10M](bench-results/phase2-yfcc10m.md) (recall@10
0.9994, p99 ≤ 16.5 ms, one node); [simulation campaigns](bench-results/phase3-campaign.md). Every
report lists the commands, the hardware and the misses.

## Known limits

- **Static membership.** The number of nodes is fixed at startup. Adding a node or rebalancing
  shards needs a reload; dynamic membership is on the roadmap.
- **One region.** Nodes should sit within a few milliseconds of each other (zones of one
  cloud region, or nearby datacenters).
- **The HTTP port has no TLS or authentication.** Put an authenticating TLS proxy in front.
  Node-to-node traffic uses mutual TLS ([ADR 0018](docs/adr/0018-versioned-contract-and-mtls.md)).
- **Protocol versions** are checked on every connection, but upgrading across a protocol
  change needs a full-cluster restart.
- Ingest still stalls for minutes at a time under sustained load, while index builds catch up.

## Build and test

```bash
cargo test --workspace                                              # unit, property, simulator, cluster tests
CAIRN_SEEDS=0..3000 cargo test --release -p cairn-query --test chaos campaign -- --ignored --nocapture
cargo clippy --workspace --all-targets -- -D warnings
```

The Rust client is `cairn-client` (`Client::new(addrs)`, `upsert`, `delete`, `get`, `query`
with `Consistency::{Linearizable, ReadYourWrites, Stale}`). The HTTP API works from any
language.

## Design

[`SPEC.md`](SPEC.md) states the goals and success criteria. Each architecture decision is an
ADR in [`docs/adr/`](docs/adr), with the evidence behind it. Examples: segments are built
once by the shard leader and shipped to followers (0016); searches run off the replica actor
(0025); Raft persistence is asynchronous with the durability rules kept (0027); merges can be
paused per node (0028).

## Contributing

Contributions are welcome, and challenges to the design, with evidence, even more. Start with
[CONTRIBUTING.md](CONTRIBUTING.md) (sign-off with `git commit -s`), the
[code of conduct](CODE_OF_CONDUCT.md), and [SECURITY.md](SECURITY.md) for reporting
vulnerabilities privately. If you believe a deletion was not honoured, please open a
"deletion guarantee" issue: it is the property this project cares about most.

Cairn is licensed under the [Apache License 2.0](LICENSE).
