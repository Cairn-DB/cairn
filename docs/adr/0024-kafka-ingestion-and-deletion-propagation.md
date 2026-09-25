# ADR 0024: Kafka ingestion and deletion propagation

- Status: **proposed** (2026-09-25, from an idea of the owner; to be discussed in the open
  with the first contributors, and built after the public release)
- Date: 2026-09-25

## Context

Cairn guarantees that a takedown is final: once acknowledged, the document is hidden on
every replica, and a client that passes the takedown's consistency token never reads it
again (ADR 0010, ADR 0023).

Many RAG and search pipelines are not fed by direct API calls. They are fed by Kafka, often
from database change capture (Debezium). There, deletion is a *tombstone* (a record with a
key and a null value). Two gaps follow:

1. **Kafka itself.** Compaction eventually drops older versions of a tombstoned key, with
   no bound on when. The delay depends on `min.compaction.lag.ms`, `segment.ms`, the
   cleaner's schedule, and `delete.retention.ms` for the tombstone itself. Mirrors and
   backups keep their own copies.
2. **Downstream.** Every sink (vector store, search index, cache, lake) holds a copy. No
   one can answer: *"when did document X disappear from every system fed by this topic?"*
   That is the question a right-to-be-forgotten request, a takedown order or a licence
   expiry asks.

Kafka already has consensus (KRaft for metadata, in-sync replicas for partitions). Nothing
here re-implements it. What is missing is a **deletion guarantee that crosses the pipe**.

## Proposal

Two independent parts. The first is the one with a hard guarantee.

### Part 1: Cairn as a Kafka sink with end-to-end read-your-takedown

A separate process, `cairn-kafka` (Rust, `rdkafka` on librdkafka), consumes one or more
topics as a consumer group and writes to Cairn. Keeping it out of the node means the C
dependency and Kafka's failure modes stay out of the engine.

- **Mapping.** A record with a value is an upsert; a tombstone is a takedown. The value is
  a document in the HTTP JSON format (ADR 0023). Avro or Protobuf with a schema registry can
  come later. The key maps to the document id (see open questions).
- **Source positions in the log (the core of the design).** Each `Upsert`/`Delete` command
  carries its source position `(topic, partition, offset)`. Each shard keeps, in its
  manifest, the highest applied offset per source partition, written atomically with the
  data.
  - A replayed record at or below that offset is skipped.
  - This is needed for correctness, not only for efficiency. Suppose the ingester crashes
    after `upsert(k)@10` and `delete(k)@11` were applied, but before it committed its Kafka
    offsets. Replaying from offset 10 would **resurrect k** until 11 is replayed again: a
    visible violation of the guarantee. Deduplication by offset in the log rules it out.
- **Watermarks.** One Kafka partition feeds every shard (a key's partition and its shard
  are unrelated). So a shard learns that it is "caught up to offset o" only if it is told so
  even when no record was for it. The ingester sends every shard a small
  `SourceWatermark { partition, offset }` command, in batches, for example every 100 ms or
  every N records. It commits its Kafka offsets only after every shard has acknowledged its
  records and watermarks up to them.
- **Tokens in Kafka terms.** A consistency token can then be a list of Kafka positions:
  `after=kafka:<topic>/<partition>:<offset>`.
  - A read of one document waits until the owning shard's watermark for that partition is
    at least the offset.
  - A search waits until every shard's is.

  The producer that wrote the tombstone at offset `o` knows `o` from its own produce
  acknowledgement. Reading Cairn with that token, it never sees the document again, on any
  node, with no call to Cairn at write time.
- **Order.** Kafka orders records only within a partition, and a key always maps to the same
  partition. Order per key is therefore preserved, which is all that upsert and takedown
  need.

Server-side cost: a field on two commands, a new command, a per-shard map in the
manifest, and a token variant. That is a log and wire change, hence protocol 5 (ADR 0018).

### Part 2: `forget-watch`, a deletion propagation monitor

A separate tool, useful even without Cairn. It watches tombstones on chosen topics and tells,
for each one, when each downstream system has caught up past it.

- Where it reads each sink's progress:
  - **Verified sinks** (Cairn, or any sink that exposes an "applied up to" endpoint, which
    we would specify): the offset the sink has *applied and made visible*.
  - **Observed sinks** (any other consumer group): the group's committed offset, from the
    Kafka Admin API.
- Outputs:
  - an audit record per tombstone: key, offset, time produced, and the time each sink
    passed it;
  - metrics (Prometheus): deletion propagation delay per sink, tombstones outstanding;
  - an alert when a sink exceeds a deadline (for example, 72 h under a GDPR policy, or
    minutes for takedowns).
- Topic configuration report: compaction and retention settings, and warnings when they
  make the physical erasure time in Kafka unbounded or very long.

**Its limit must be stated plainly.** A committed offset proves that a consumer *read* past
the tombstone, not that the deletion is applied and visible. A sink that commits before it
applies, or that serves from a cache, can be ahead on paper. `forget-watch` must therefore
label each sink as *verified* or *observed*, and never report an observed sink as proof.
It also cannot tell when compaction has physically erased a record inside Kafka: Kafka
exposes no per-record status. It can only report the configuration that bounds that time.

## Options considered

1. **A Kafka Connect sink in Java**, calling the HTTP API. It is the standard way into the
   Kafka ecosystem, but deduplication and watermarks still need the server-side change of
   Part 1. Worth adding later, as a thin wrapper around the same protocol.
2. **Consuming inside each Cairn node.** It saves a process, but brings librdkafka and
   Kafka's rebalancing into the engine, and makes node failure and ingestion failure
   one and the same. Rejected for now.
3. **A generic "consensus plugin" for Kafka scripts.** Rejected: Kafka's replication
   already provides consensus. The gap is propagation to derived stores, not agreement
   inside Kafka.
4. **C++.** No advantage over Rust here: librdkafka is C, and Rust bindings exist and are
   widely used.

## Evidence required before acceptance (CLAUDE.md)

- A simulated Kafka in `cairn-sim`: partitioned logs, consumer groups, offset commits,
  redelivery after a crash. The chaos campaign then extends to ingestion, with the ingester
  crashing between apply and commit. Checks:
  - no resurrection, whatever the replay;
  - read-your-takedown with Kafka tokens;
  - convergence.
- Property tests for the partition-to-shard watermark logic.
- A local benchmark with a real Kafka broker in a container: ingest throughput, and the
  delay from tombstone to invisible on every node.

## Open questions

- **Keys.** Cairn ids are `u64`, Kafka keys are bytes. Parse numeric keys; hash other keys
  (hash collisions would conflate documents, which is unacceptable for deletion); or add
  string ids to Cairn, a larger change. Leaning towards string ids.
- **Several topics into one collection**, and a document that moves between topics.
- **Watermark overhead** with many partitions and shards (P × S small commands per interval).
  Batching into one command per shard per interval should keep it negligible; this needs
  measuring.
- **Change-capture formats** (Debezium envelope: `op`, `before`, `after`). Support them
  natively or require a transform?
- **The "applied up to" endpoint for verified sinks**: a small open specification, so that
  other databases can implement it. This could be the community's first shared artifact.
- **Audit evidence**: whether `forget-watch` records need to be tamper-evident (hash
  chaining, signing) to count as compliance evidence.

## Consequences if accepted

- Cairn's guarantee would extend from "our API" to "your pipeline". Positioning: *the RAG
  retrieval layer that proves deletion end to end*.
- Protocol 5, with the migration rules of ADR 0018.
- Two new artifacts to maintain (`cairn-kafka`, `forget-watch`), and a Kafka broker in CI.
