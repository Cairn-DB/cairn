# Cairn

**A distributed hybrid search database where a deletion is final.**

Cairn combines vector search, full-text search (BM25) and structured filters in one engine and
one query plan, replicated with Raft. It is written in Rust.

A takedown is a single replicated write. Once it is acknowledged, the document disappears from
every replica, and a client holding its consistency token will never read it again, through
any node. Cairn is built for systems that must be able to prove that property, such as RAG
over regulated data, rights management and permission changes.

> **Status: 0.3, developer preview.** The engine, replication, HTTP API, clients and Docker
> image work, and the engine is measured at 50 million vectors on a 3-machine cluster (below).
> It is not production-ready yet: see [Known limits](#known-limits) and the
> [roadmap](ROADMAP.md). Before 1.0, a minor version may change the API or the on-disk format
> ([CHANGELOG](CHANGELOG.md)).

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

## Wiring it into an application

- **Your own ids**: a UUID or `"article/2024-17"`, returned as written.
- **Collections**, created through the API, each with its schema and shards.
- **Deletion by filter**: everything of one customer, one source, or one parent document and
  all its chunks, in one call.
- **Tenants**: an API key scoped to one tenant only ever reads, searches and deletes that
  tenant's documents. Erasing a tenant is one call.
- **Retention**: documents expire at a time you set. They are hidden at once, then deleted.
- **Partial updates**, atomic with respect to other writes.
- **One hit per document** in searches over chunks.
- **Proof of deletion**: every replica is asked whether it still holds a document, and the
  node signs the report.
- **Clients and integrations**: TypeScript ([`clients/typescript`](clients/typescript)),
  Python ([`clients/python`](clients/python)), LangChain for Python and JavaScript, and
  LlamaIndex ([`integrations/`](integrations)).

```python
from cairn_db import Client, eq
db = Client("http://localhost:7200", api_key=KEY)
db.upsert([{"id": "report-9#0", "parent": "report-9", "text": "...", "embedding": [...]}])
db.search(text="nuclear", text_field="text", filter=eq("lang", "en"), group_by="parent")
db.delete(parent="report-9")          # the document and all its chunks, gone on every node
```

## Quick start (Docker)

```bash
docker run -d --name cairn -p 7200:7200 -v cairn-data:/data ghcr.io/cairn-db/cairn:0.3
# or build it: docker build -f docker/Dockerfile -t cairn:dev .   (or podman)
export KEY=$(docker logs cairn 2>&1 | grep -o 'cairn_[A-Za-z0-9_-]*' | head -1)   # admin key, printed once
```

```bash
# Write, then search with the write's token: the result reflects it.
curl -s 127.0.0.1:7200/v1/documents -H "Authorization: Bearer $KEY" -H content-type:application/json \
  -d '{"documents":[{"id":1,"text":"nuclear energy debate","source":"tv"}]}'
# {"consistency_token":"2.2","count":1}
curl -s 127.0.0.1:7200/v1/search -H "Authorization: Bearer $KEY" -H content-type:application/json \
  -d '{"text":{"field":"text","query":"nuclear"},"after":"2.2"}'

# Take it down: with the new token, no node returns it again. The takedown is audited.
curl -s -X DELETE 127.0.0.1:7200/v1/documents/1 -H "Authorization: Bearer $KEY"
```

API keys carry roles (`read`, `write`, `takedown`, `admin`); `cairn-server keygen` creates them,
and HTTPS is one flag away ([ADR 0030](docs/adr/0030-http-authentication.md)). Three replicated
nodes: `CAIRN_HTTP_ADMIN_KEY=<secret> docker compose -f docker/compose.cluster.yaml up -d`
(HTTP 7201-7203).

The HTTP/JSON API is documented in [docs/api/http.md](docs/api/http.md), with an
[OpenAPI description](docs/api/openapi.yaml). Configuration uses environment variables
(`CAIRN_NODE_ID`, `CAIRN_PEERS`, `CAIRN_SCHEMA`, `CAIRN_TLS_DIR`, ...): see
`docker/entrypoint.sh` and [ADR 0022](docs/adr/0022-packaging-and-distribution.md).

## Measured results

50M BigANN vectors (128 dimensions) on 3 × GCP n2-highmem-8 (8 vCPU, 64 GB), every node holding
every shard. Queries ran on the same processes that had just ingested the data, with no
restart: 1,000 per kind from 8 client threads, k = 10
([bench-results/phase4-gcp-bigann50m-run9.md](bench-results/phase4-gcp-bigann50m-run9.md)).

| metric | result | target |
|---|---|---|
| unfiltered p99, stale / linearizable | 35 / 36 ms | < 100 ms |
| 1%-selective filter p99, stale / linearizable | 31 / 30 ms | < 100 ms |
| throughput, unfiltered, stale / linearizable | 435 / 318 QPS | |
| recall@10, unfiltered / filtered | 0.985 / 0.990 | |
| takedown visible on all 3 machines, p99 | 102 ms | < 1 s |
| ingest, 50M rows through Raft | 20,667 docs/s (2,419 s) | |

These figures were measured with 0.1. A local A/B of 0.1 against 0.3 shows no regression
beyond noise ([bench-results/release-0.3-ab.md](bench-results/release-0.3-ab.md)). A 50M run on
0.3 has not been done yet.

Other evidence: [Big-ANN filtered track at 10M](bench-results/phase2-yfcc10m.md) (recall@10
0.9994, p99 ≤ 16.5 ms, one node); [simulation campaigns](bench-results/phase3-campaign.md). Every
report lists the commands, the hardware and the misses: for instance, run 8 missed the latency
targets on freshly ingested nodes until the allocator was changed
([ADR 0029](docs/adr/0029-mimalloc.md)).

## Known limits

- **Static membership.** The number of nodes is fixed at startup. Adding a node or rebalancing
  shards needs a reload; dynamic membership is on the roadmap.
- **A node that loses its disk cannot rejoin yet**: an empty node has forgotten its Raft votes.
  The cluster keeps serving on the other replicas. See the [deployment guide](docs/deployment.md).
- **One region.** Nodes should sit within a few milliseconds of each other (zones of one
  cloud region, or nearby datacenters).
- **API keys are static files per node.** No key rotation service or external identity
  provider (OIDC) yet. Node-to-node traffic uses mutual TLS
  ([ADR 0018](docs/adr/0018-versioned-contract-and-mtls.md)).
- **Protocol versions** are checked on every connection, but upgrading across a protocol
  change needs a full-cluster restart.
- Ingest throughput depends on how fast index builds keep up: about 20k docs/s at 50M on 3
  nodes, measured on two runs.

## Build and test

```bash
cargo test --workspace                                              # unit, property, simulator, cluster tests
CAIRN_SEEDS=0..3000 cargo test --release -p cairn-query --test chaos campaign -- --ignored --nocapture
cargo clippy --workspace --all-targets -- -D warnings
```

To check a running node or cluster end to end with realistic data (authentication, every
field type, search, filters, takedowns on every node), run the
[acceptance suite](examples/acceptance/README.md).

Clients:
- TypeScript and JavaScript: [`clients/typescript`](clients/typescript) (`@cairn-db/client`,
  on `fetch`, no runtime dependency);
- Python: [`clients/python`](clients/python) (`pip install cairn-db-client`, sync and async, on `httpx`);
- Rust: `cairn-client`, over the binary protocol (`Client::new(addrs)`, `upsert`, `delete`,
  `delete_where`, `get`, `query`).

Integrations:
- LangChain for Python: [`integrations/langchain-cairn`](integrations/langchain-cairn);
- LangChain.js: [`integrations/langchain-js`](integrations/langchain-js);
- LlamaIndex: [`integrations/llama-index-vector-stores-cairn`](integrations/llama-index-vector-stores-cairn).

`clients/test-live.sh` runs the clients' and the integrations' tests against a fresh local
node. The HTTP API works from any language.

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
