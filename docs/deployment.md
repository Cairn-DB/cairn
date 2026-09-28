# Deploying Cairn

This guide covers a production-shaped deployment: three nodes in three zones of one region,
running the Docker image. Every figure here is measured, and each links to its report. Read
[Known limits](../README.md#known-limits) first: membership is static and upgrades across
protocol versions need a full restart.

## Topology

```
                 clients (applications, RAG pipelines)
                            │ HTTPS, Authorization: Bearer <key>
               ┌────────────┴────────────┐
               │ load balancer (TCP or HTTP) │
               └──┬──────────┬──────────┬──┘
     zone A       │  zone B  │  zone C  │
  ┌───────────┐ ┌───────────┐ ┌───────────┐
  │ node 1    │ │ node 2    │ │ node 3    │   every shard on every node (replication 3)
  │ :7200 API │ │ :7200 API │ │ :7200 API │   shard leaders spread across nodes
  │ :7100 Raft│ │ :7100 Raft│ │ :7100 Raft│
  │ NVMe /data│ │ NVMe /data│ │ NVMe /data│
  └─────┬─────┘ └─────┬─────┘ └─────┬─────┘
        └── Raft and segment shipping, mutual TLS ──┘
```

- **Any node answers any request** and forwards it to the shard leaders, so the load balancer
  needs no affinity.
- **Placement** (ADR 0015):
  - shard *s* lives on `--replication` consecutive nodes, by node id, starting at *s* mod the
    node count;
  - with 3 nodes and the default replication, every node holds every shard;
  - with 5 or more nodes and `--replication 3`, each node holds about 3/N of the data. Number
    the nodes so that consecutive ids sit in different zones (A, B, C, A, B, ...).
- **Faults**: losing one node, or one zone, loses no acknowledged write and stops no shard,
  since the other two copies form a majority.

## Latency between sites

A write is acknowledged once a majority has it on disk, so it pays one round trip to the
closest other replica. Linearizable reads pay one heartbeat round; stale reads pay nothing.

| round trip between sites | verdict |
|---|---|
| under 5 ms: zones of one region, or datacenters in one metro | recommended |
| 5 to 20 ms | works; writes gain a few milliseconds |
| over 30 ms: another region | not supported yet: slow writes, and the Raft timers (50 ms tick, 500 ms election) would need tuning |

Measure the round trip with `ping` between the machines before you commit to a layout.

## Sizing

Measured on 3 × GCP n2-highmem-8 (8 vCPU, 64 GB), local NVMe, 50M 128-dimension vectors, every
node holding everything ([run 9](../bench-results/phase4-gcp-bigann50m-run9.md)):

| resource | at 50M vectors per node | notes |
|---|---|---|
| memory | 16-17 GB resident | SQ8 vectors in memory (`--sq8-only`); plan for 64 GB at this scale for builds and headroom |
| disk | about 50 GB | use a local NVMe SSD; network disks with low throughput caps slow ingest (run 7: about 38 MB/s) |
| CPU | 8 vCPU | index builds use idle CPU at low priority; serving comes first |

Results at this size:
- p99 of 35 ms unfiltered and 31 ms with a 1% filter;
- 435 queries per second;
- ingest at 20k docs/s through Raft;
- takedowns visible on every node within 102 ms at p99.

Memory grows roughly linearly with the rows a node holds (about 0.35 KB per 128-d row with
SQ8).

## Security

### Between nodes: mutual TLS

```bash
tools/scripts/gen-certs.sh certs 3            # ca.pem, ca.key (keep offline), node<i>.pem/.key
```

Give each node `ca.pem` and its own `node<i>.pem`/`node<i>.key` (for example as a read-only
volume) and set `CAIRN_TLS_DIR` (flags `--tls-ca/--tls-cert/--tls-key`). A node then accepts
only peers with a certificate from the same CA, for the name `node-<id>.cairn`.

### Clients: API keys and HTTPS

- Create one key per client and role:
  ```bash
  docker run --rm --entrypoint cairn-server ghcr.io/cairn-db/cairn keygen rag-api read
  docker run --rm --entrypoint cairn-server ghcr.io/cairn-db/cairn keygen ingest write,read
  docker run --rm --entrypoint cairn-server ghcr.io/cairn-db/cairn keygen compliance takedown
  ```
  Each prints the key once and a `{"id","sha256","roles"}` entry. Collect the entries in a
  keys file (`{"keys":[...]}`), the same on every node, and point `CAIRN_HTTP_KEYS` at it. The
  file holds only digests. An admin key can also come from a secret through
  `CAIRN_HTTP_ADMIN_KEY`.
- Serve HTTPS with `CAIRN_HTTP_TLS_CERT`/`CAIRN_HTTP_TLS_KEY`, or terminate TLS at the load
  balancer on a private network. Never expose plain HTTP with keys on an untrusted network.
- Keep the `takedown` role for the service accountable for deletions. Every takedown is
  logged with the key id, the document ids and the consistency token (log target
  `cairn_server::audit`). Ship those logs to your log store: they are the trail of who
  deleted what, and when.

## Running the nodes

The image is configured by environment variables (`docker/entrypoint.sh`). Extra arguments are
passed to `cairn-server`:

| variable | meaning |
|---|---|
| `CAIRN_NODE_ID` | this node's id: 1, 2, 3, ... |
| `CAIRN_PEERS` | every node, this one included: `"1=10.0.1.10:7100 2=10.0.2.10:7100 3=10.0.3.10:7100"` (IP addresses) |
| `CAIRN_LISTEN` | Raft and binary protocol address (default `0.0.0.0:7100`) |
| `CAIRN_SCHEMA` | the collection schema (JSON; default `/etc/cairn/schema.json`) |
| `CAIRN_SHARDS` | shards (default 4); with more cores, 8 or more |
| `CAIRN_CORES` | executor threads (default: all CPUs) |
| `CAIRN_TLS_DIR` | mutual TLS material between nodes |
| `CAIRN_HTTP_LISTEN` | API address (default `0.0.0.0:7200`) |
| `CAIRN_HTTP_KEYS` / `CAIRN_HTTP_ADMIN_KEY` | API keys (see Security) |
| `CAIRN_HTTP_TLS_CERT` / `CAIRN_HTTP_TLS_KEY` | HTTPS for the API |

For large collections, add `--sq8-only --target-segment-rows 3000000` (vectors kept as SQ8 in
memory; merges up to 3M rows per segment) and give `/data` a local NVMe volume.
[`docker/compose.cluster.yaml`](../docker/compose.cluster.yaml) shows a complete 3-node file.
On separate machines, run one node per machine with the same variables and its own
`CAIRN_NODE_ID`.

## Operating

- **Health.** `GET /health` (no key) for liveness. `GET /v1/status` (admin) shows each replica:
  - its role and term;
  - the commit and applied indexes;
  - the memtable and Raft log sizes;
  - its segments;
  - flushes, and merges running, pending or paused.

  A replica whose applied index stays behind the others is lagging.
- **Merges.** They run in the background at low priority. Pause them for a maintenance window
  or a measurement with `POST /v1/admin/merges {"paused": true}` on every node, and resume
  afterwards (ADR 0028). Merges already decided still finish.
- **Losing a node.** The other two keep serving. When the node comes back with its disk, it
  catches up by itself: log entries, or a snapshot of the segments it missed.
- **Losing a node's disk.** Rebuilding it is not supported yet. An empty node has forgotten the
  votes it cast, which Raft's safety relies on, so restarting it empty with the same id is
  unsafe. Keep serving on the other two, and treat the node as lost until membership changes
  exist ([roadmap](../ROADMAP.md)). Use reliable disks (RAID 1 on bare metal) in the
  meantime.
- **Upgrades.** Stop every node and start them all on the new version. Across protocol
  versions, the connection handshake refuses to mix them. Within one version, rolling upgrades
  should work but are not tested yet.
- **Backups.** There is no online backup yet. Only a full-cluster copy is safe: stop every
  node, copy every `/data` volume, and restore them all together. Restoring one node from an
  older copy is unsafe for the same reason as an empty disk (forgotten votes). Restoring the
  whole cluster brings back documents deleted after the copy was taken: replay those
  takedowns from the audit log. Consistent online backups are on the [roadmap](../ROADMAP.md).
