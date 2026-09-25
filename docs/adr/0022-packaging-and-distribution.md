# ADR 0022: Packaging and distribution

- Status: accepted (delegated 2026-09-25; the owner set the direction: open source, a
  community around deletion guarantees, a publishable Docker image first)
- Date: 2026-09-25

## Context

The owner will take Cairn to production and publish it as open source.
The goal is contributors from RAG governance, AI engineering and
application development, gathered around one property: a takedown is visible everywhere
within about 100 ms, and a client that asked for it never reads the document again. The
priorities, in order:
1. reliability;
2. a Docker image developers can pull;
3. a user interface that feels familiar to users of other vector databases.

## Options considered

1. **Distribution.** An open-source server (binary and container image) first; a managed
   service; an embedded library; on-premises installs. The owner's goals rule out a closed
   product. A managed service needs round-the-clock operations that one person cannot
   provide yet.
2. **License.** Apache-2.0, already in `Cargo.toml`, with an explicit patent grant and
   familiar to companies; source-available licenses (ELv2, BSL) protect against cloud
   resale but put contributors off. Apache-2.0 kept.
3. **Registry.** GitHub Container Registry, next to the code (free for public images,
   permissions follow the repository); Docker Hub (more familiar, pull limits). GHCR first.
   A Docker Hub mirror can follow.

## Decision

- A **multi-stage image** (`docker/Dockerfile`): a Rust builder, then Debian slim with
  `tini`, running as a non-root user (uid 10001), with a `/data` volume and port 7100. It
  holds `cairn-server` and `cairn-bench`, the only operations tool so far.
- **Configuration by environment variables** (`docker/entrypoint.sh`): `CAIRN_NODE_ID`,
  `CAIRN_PEERS`, `CAIRN_LISTEN`, `CAIRN_SCHEMA`, `CAIRN_SHARDS`, `CAIRN_CORES`,
  `CAIRN_TLS_DIR`. Extra arguments go to the server. Without variables the container is
  one development node with a generic schema (`docker/schema.json`): a 384-dimension
  cosine embedding, text, an enum, tags, a date and a payload.
- **Compose files**: one node (`docker/compose.yaml`) and three replicated nodes on one host
  (`docker/compose.cluster.yaml`).
- **Publication** under a GitHub organization owned by the owner's existing account (the
  GitHub terms allow one free personal account per person), with images at
  `ghcr.io/<org>/cairn`. Nothing is published before the owner creates the organization and
  approves the pre-publication checklist below.

## Evidence (2026-09-25, podman 5.8 with the Compose v2 plugin)

- The image builds (122 MB).
- One node starts without configuration and leads its 4 shards.
- Three nodes from `compose.cluster.yaml` elect leaders spread 2/1/1. `cairn-bench cluster`
  ran against them: 100k SIFT vectors, 300 queries per kind, 30 takedowns visible on all
  nodes, p99 108 ms. These are smoke-test figures, taken during builds on a shared host, not
  benchmarks.
- A node killed and restarted converged again in 1.1 s. `compose down` takes 0.4 s.

## Consequences and gaps

- **Developers need a client they already use.** The wire protocol is binary and only the
  Rust client speaks it. An HTTP/JSON API and a Python client come next; without them the
  image mostly serves Rust users and evaluations.
- **Peers are IP addresses.** The server does not resolve host names, so the cluster compose
  file pins IPs. Kubernetes (StatefulSet DNS names, Helm) needs name resolution at dial time.
- **One schema per cluster**, read at startup: no collections yet.
- **Before publishing** (checklist):
  - add a LICENSE file, CONTRIBUTING, SECURITY and a code of conduct, and set the
    repository URL in `Cargo.toml`;
  - remove the names of the owner's other servers and cloud projects from `docs/` and
    `tools/scripts/`;
  - decide whether to publish the history, which carries the owner's personal e-mail in
    every commit, or to rewrite it with a GitHub no-reply address;
  - add CI: format, lint, tests, a short simulation campaign, and the image build and push.
- The image is built from the working tree. Releases should be built by CI from tags, with
  image signatures later.
