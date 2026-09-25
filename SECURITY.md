# Security policy

## Reporting a vulnerability

Please **do not open a public issue**. Use GitHub's private vulnerability reporting: on the
repository, open *Security → Report a vulnerability*. Include the version or commit, a
description, and a reproduction if you have one. A simulator seed is ideal.

You will get an acknowledgement within 5 working days. Once a fix is ready, we agree on a
disclosure date together, and credit you unless you prefer otherwise.

## What counts

Anything that breaks a guarantee in [`SPEC.md`](SPEC.md), in particular:

- a deleted (taken-down) document that can still be read after the takedown was
  acknowledged, or that reappears later (after a restart, snapshot or compaction);
- acknowledged writes that are lost, or reads that violate the requested consistency level;
- node impersonation or traffic accepted without a valid certificate when mutual TLS is on;
- memory-safety problems, and crashes a remote peer or client can trigger.

## Supported versions

Cairn is pre-1.0. Only the latest release and `main` receive fixes.

## Known limits (not vulnerabilities)

See ADR 0018. There is no authorization yet: any client with a certificate from the cluster
CA can read, write and take down anything. There is no certificate revocation, and no limit
on concurrent connections. Without the `--tls-*` flags, a node runs in plaintext for
development and says so at startup.
