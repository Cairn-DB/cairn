# 6. Compliance

## The audit trail

Every deletion is logged by the node that received it, on the log target
`cairn_server::audit` (on by default). The line carries the key id, the tenant, the ids or
the filter, the number of documents, and the consistency token. Erasing a tenant, expiring
documents, creating and dropping collections, and requesting proofs are logged too. Ship
these lines to your log store like any other audit log.

## Proofs of deletion

An audit line says a deletion was requested. A proof says it happened: Cairn asks **every
replica** of the documents' shards whether it has applied the deletion and still holds them,
and the node signs the report.

```python
proof = acme.prove_deletion(["kb-invoices#0"])
assert proof["report"]["verdict"] == "deleted everywhere"
```

- The client passes its token, so each replica first applies the deletion you just made.
- The report lists each document with its shard and its verdict: `deleted` only if every
  replica answered, had applied the deletion, and does not hold the document. Otherwise
  `not proven`, with the reasons (a node that did not answer, a replica not caught up, a
  replica that still holds it).
- It needs the `takedown` role, takes at most 10,000 ids per call, and is audited.
- For a customer erased with `forget_tenant`, list the customer's ids before erasing, then
  prove them afterwards (the recipes in chapter 7 do this).

Keep the proof as evidence. Check it against the node's public key, which you fetch once
and store with your records (`client.deletion_key()`, or `GET /v1/deletions/key`). The key
inside a proof only shows that the report was not changed after signing.

**In Python** (`pip install cryptography`):

```python
import base64, json
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

def verify(proof: dict, public_key_b64: str) -> str:
    report = json.dumps(proof["report"], sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    Ed25519PublicKey.from_public_bytes(base64.b64decode(public_key_b64)).verify(
        base64.b64decode(proof["signature"]), report.encode())      # raises if altered
    return proof["report"]["verdict"]
```

**From the command line**, with the image:

```bash
docker run --rm -v "$PWD:/work:ro" --entrypoint cairn-server ghcr.io/cairn-db/cairn:0.3 \
  verify-proof /work/proof.json --public-key "$NODE_PUBLIC_KEY"
# signature OK (with the given public key); verdict: deleted everywhere
```

(The container runs as a non-root user: the file must be readable by others.)

## What a proof does not cover

Be precise with your auditors:
- It covers Cairn's replicas at the time of the check, not copies elsewhere: your backups,
  exports, logs, caches, or the source system.
- A deleted document is never returned again, but its bytes can remain in a segment file until
  a merge rewrites that segment.
- Each node signs with its own key. There is no key rotation or chaining of successive proofs
  yet (tracked in issue #10).

Next: [recipes](07-recipes.md).
