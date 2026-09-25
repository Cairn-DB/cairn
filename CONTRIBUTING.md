# Contributing to Cairn

Thank you for your interest. Cairn is a distributed database. A bug can lose or expose
data, so its rules are stricter than most projects'. They are short, though.

## What we promise, and what we need from you

Cairn's central promise is about deletions: once a takedown is acknowledged, the document
is gone from every replica within about 100 ms, and a client that asked for the takedown
never reads the document again. Every change must keep that promise and the others in
[`SPEC.md`](SPEC.md), and must show that it does.

## Before you write code

- Read [`SPEC.md`](SPEC.md) and the architecture decisions in [`docs/adr/`](docs/adr).
- For anything larger than a bug fix, open an issue first. A change to an on-disk format,
  the wire protocol, or Raft needs an ADR (copy `docs/adr/0000-template.md`).
- Say so if you think the spec is wrong. Challenging a decision with evidence is welcome.

## Rules for code

1. **Tests first.** Property tests (`proptest`) for data structures, fuzz targets for
   decoders, `loom` for concurrency, Miri for `unsafe`. Anything that touches durability or
   consensus also needs deterministic simulation: it must pass the chaos campaign.
2. **Determinism.** Engine code gets time, network, disk and randomness only through the
   traits in `cairn-core`. No `SystemTime::now`, no global RNG, no raw `std::fs` or sockets.
   This is what lets the simulator replay any failure from its seed.
3. **Compatibility.** A change to a command, a message or a segment encoding bumps the
   protocol or segment-format version (ADR 0018).
4. **Small commits.** One logical change per commit. The message explains *why*.
5. **Honest numbers.** Benchmarks go under `bench-results/` with the exact command and the
   hardware used. A missed target is reported as missed. Never claim a test passed without
   running it.

## Checks before a pull request

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
tools/scripts/campaign.sh 2000 8 /tmp/campaign.md   # if you touched storage, Raft or replication
```

## Sign-off (DCO)

Cairn uses the [Developer Certificate of Origin](https://developercertificate.org/) instead of
a contributor license agreement. Sign every commit with `git commit -s`, which certifies that
you wrote the change or have the right to submit it under the project's license
(Apache-2.0).

## AI-assisted contributions

They are welcome under the same rules. You are responsible for every line you submit: you
understand it, have run the checks, and can explain it in review.

## Conduct and security

Follow the [code of conduct](CODE_OF_CONDUCT.md). Report vulnerabilities privately as
described in [SECURITY.md](SECURITY.md), never in a public issue.
