# ADR 0009: Client API and wire protocol — protobuf messages, length-prefixed frames, gRPC gateway later

- Status: proposed
- Date: 2026-09-22
- Resolves: SPEC.md O8

## Context

Clients need a small typed API: create collection (schema), upsert, delete (takedown), query
(hybrid), get by id, and consistency options (ADR 0010). Nodes need Raft traffic and segment
shipping. The runtime is thread-per-core with our own executor (ADR 0002), which rules out
frameworks that require `tokio`.

## Options considered

1. **gRPC (`tonic` 0.14.6)**: ecosystem clients for free, streaming, schema evolution. Cons: needs
   `tokio` and HTTP/2, so it cannot run on our per-core executor; `tonic-build` needs `protoc`
   unless `protox` (pure-Rust protobuf compiler, 0.9.x on crates.io) is used.
2. **Custom binary protocol with `serde` + `postcard`/`bincode`**: tiny and fast; but no schema
   evolution story and no path for non-Rust clients.
3. **Protobuf messages over a custom framing**: `.proto` files are the schema, compiled at build
   time with `prost-build` + `protox` (no system `protoc`) or checked-in generated code; transport
   is `u32 length | u8 message type | protobuf body` over TCP, one connection per client per core,
   request ids for pipelining. The same framing carries Raft messages and shipped segment chunks
   between nodes.

## Decision

Option 3 for v1 (Phase 4, `cairn-proto` crate), and a separate optional gRPC gateway binary later
(Phase 5 territory) that translates to the native protocol for ecosystem clients. Reasons: one
codec for everything, schema evolution via protobuf field rules, runs on our executor, and the
Rust client (`cairn-client`) plus the CLI are the only clients v1 needs.

Which of `protox` or checked-in generated code we use is decided at the Phase 4 kickoff; both avoid
a system `protoc`. `prost` runtime is light. No `tonic` in v1.

## Consequences

- No HTTP endpoint in v1. Operators use the CLI; benchmarks use `cairn-client`.
- A fuzz target on the frame decoder and on every protobuf message parser (docs/verification.md).
- Backward compatibility: every message carries a protocol version in the handshake; unknown
  fields are ignored by protobuf rules; frame type ids are never reused.
- Segment shipping streams chunks with offsets and per-chunk checksums so a resumed transfer is
  verifiable.

## Experiment that confirms or refutes

None needed beyond fuzzing and a throughput benchmark of the frame codec (Phase 4, M4.1).
