# ADR 0018: Versioned wire and segment-format contract, mutual TLS between nodes

- Status: accepted (delegated 2026-09-24; the owner asked for step 6 of ADR 0016 with
  production as the goal)
- Date: 2026-09-24

## Context

ADR 0016 made segment files a cluster-wide contract: a follower installs the file its leader
wrote, and snapshots ship files between replicas. Nothing checked that two nodes spoke the same
protocol or read the same formats. Mixing builds across ADR 0016/0017 would have failed in
obscure ways (unknown command tags, extra fields in `FileChunk` and `PreVoteResp`). All traffic
was plaintext, and a node's id was whatever its first frame claimed. A node that accepts segment
files, Raft messages and takedowns from peers must know who they are.

## Options considered

1. **TLS library:** rustls with the `ring` backend (pure Rust plus a small C and assembly
   part, builds with `cc`); rustls with `aws-lc-rs` (needs cmake, not installed); OpenSSL
   bindings (system library, no `sudo` here). Chosen: rustls + ring.
2. **Node identity:** a certificate per node with the DNS name `node-<id>.cairn`, checked with
   webpki; SPIFFE-style URI names; a pre-shared key per cluster. DNS names work with standard
   tooling (openssl, cert-manager, any CA), and one CA per cluster isolates clusters.
3. **Versioning:** a single build id that must match exactly (no rolling upgrades); version
   ranges negotiated per connection. Chosen: ranges.

## Decision

1. **Hello v2.** Every connection starts with a hello frame. A node sends
   `node id | protocol version | highest segment format it reads`; a client sends
   `u32::MAX | protocol version`. Connections whose protocol version falls outside
   `[PROTOCOL_MIN, PROTOCOL_VERSION]` are closed and logged. The protocol version covers
   everything on the wire and in the replicated log: frames, Raft messages, commands, file
   shipping. This build speaks protocol 2 only. The 4-byte hello of the earlier protocol is
   refused, so upgrading to this build needs a full-cluster restart once.
2. **Segment format contract.** A build reads formats
   `[SEGMENT_VERSION_MIN_READ, SEGMENT_VERSION]` and can write
   `[SEGMENT_VERSION_MIN_WRITE, SEGMENT_VERSION]`. The version covers the container and every
   section encoding. A node writes the newest format every configured peer has announced it
   reads, and the oldest it can write until every peer has said hello
   (`negotiated_segment_version`, re-evaluated on every tick). A rolling upgrade to a new format
   `N+1` therefore needs one release that reads `N+1` and still writes `N`. Nodes switch to
   `N+1` by themselves once the last peer runs that release. All formats are 1 today.
3. **Mutual TLS** on every connection when `--tls-ca/--tls-cert/--tls-key` are given (TLS 1.2
   and 1.3, rustls safe defaults):
   - a node dialing peer `N` requires a server certificate for `node-N.cairn`;
   - a node accepting a node hello that claims id `N` requires a client certificate for
     `node-N.cairn` from the cluster CA, and a configured peer id;
   - clients verify the node they dial the same way, and present a certificate unless the node
     runs with `--tls-anonymous-clients`;
   - without TLS flags the node runs in plaintext and warns at startup (development only).
4. **Tooling.** `tools/scripts/gen-certs.sh` issues a CA and node and client certificates with
   openssl (EC P-256, PKCS#8). `cluster.sh` takes `CAIRN_TLS_DIR`. `cairn-bench` takes
   `--tls-ca/--tls-cert/--tls-key`. `cairn_client::Client::with_tls` and `set_default_tls`
   expose the same settings to applications.

## Evidence

- Transport tests: node and client round trips over mTLS; 3 MB frames both ways; a node
  holding another node's certificate cannot claim its id (with a positive control); a foreign
  CA, a plaintext client and a client without a certificate are refused; protocol 1 and future
  versions are disconnected; the segment format is negotiated from peer hellos.
- Storage test: readers refuse versions outside their range; writers refuse versions they
  cannot write.
- The three-process cluster test runs a second time over mTLS: writes, hybrid queries,
  takedowns, forwarding, and a killed and restarted node.
- Manual check with openssl-generated certificates against the release server.
- Campaign 20,000 seeds, zero violations. Workspace: 99 tests pass.
- Throughput and latency cost: `bench-results/phase4-adr0018-tls.md`.

## Consequences and limits

- Upgrading from earlier builds needs a full restart; later protocol changes need
  `PROTOCOL_MIN` kept below the new version for one release, with the new behaviour gated on
  the lowest version in the cluster. No such gating exists yet.
- **No revocation.** Removing a compromised node means a new CA, or CRL support, which rustls
  offers but this change does not wire in. Certificates load at startup: rotation needs a
  restart.
- **No authorization.** Any client certificate from the cluster CA may read, write and take
  down anything. Roles or per-collection rights are future work.
- Unauthenticated connections are cut at the handshake (10 s timeout), but there is no limit
  on concurrent connections.
- A connection's two halves share TLS state under a lock that is never held across a blocking
  socket read. Sealed records go out in seal order under a second lock; the writer releases the
  TLS state before it blocks on a full socket. The one exception is the reader answering a TLS
  control message (such as a key update): it writes while holding both locks, and can wait
  there for a slow peer. Payload traffic does not produce such messages.
- The segment format and the protocol are now explicit contracts: any change to a section
  encoding, a command or a message must bump the matching version.
