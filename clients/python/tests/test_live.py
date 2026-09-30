"""Against a running node (clients/test-live.sh): CAIRN_URL, CAIRN_ADMIN_KEY, CAIRN_KEY (read,write,takedown),
CAIRN_ACME_KEY (scoped to tenant "acme"), CAIRN_READ_KEY (read only). Skipped without them."""

import asyncio
import os
import time

import pytest

import cairn_db as c

URL = os.environ.get("CAIRN_URL")
pytestmark = pytest.mark.skipif(not URL, reason="CAIRN_URL not set")
RUN = f"py-{os.getpid()}-{int(time.time() * 1000)}"


def vec(i):
    return [1.0, float(i), 0.5, 0.0]


def test_client_against_a_live_node():
    root = c.Client(URL.split(","), os.environ["CAIRN_KEY"])
    db = root.with_tenant(RUN)  # a fresh tenant: this run's data only
    chunks = [{"id": f"doc-1#{i}", "parent": "doc-1", "text": f"ocelot chunk {i}", "embedding": vec(i), "n": i} for i in range(4)]
    chunks += [
        {"id": "doc-2#0", "parent": "doc-2", "text": "ocelot other", "embedding": vec(9), "n": 9, "tags": ["x"]},
        {"id": 42, "text": "integer id", "embedding": vec(4), "n": 42},
        {"id": "007", "text": "digits text id", "embedding": vec(5), "n": 7},
    ]
    assert db.upsert(chunks).count == 7

    assert db.get("doc-1#2") == {"id": "doc-1#2", "parent": "doc-1", "text": "ocelot chunk 2", "embedding": vec(2), "n": 2}
    assert db.get(42)["text"] == "integer id"
    assert db.get("007")["text"] == "digits text id"
    assert db.get("missing") is None
    hits = db.search(k=20, text="ocelot", text_field="text")
    assert sorted(h.id for h in hits) == ["doc-1#0", "doc-1#1", "doc-1#2", "doc-1#3", "doc-2#0"]
    assert all(h.document and h.tenant is None for h in hits)
    hits = db.search(k=3, vector={"field": "embedding", "values": vec(4)}, filter=c.range_("n", gte=4), with_documents=False)
    assert hits[0].id == 42

    other = c.Client(URL, os.environ["CAIRN_KEY"], tenant=RUN, token=db.token)
    assert other.get("doc-2#0")["parent"] == "doc-2"

    assert db.patch("doc-1#2", {"text": "ocelot patched", "n": None}).count == 1
    assert db.get("doc-1#2") == {"id": "doc-1#2", "parent": "doc-1", "text": "ocelot patched", "embedding": vec(2)}
    assert db.patch_many([{"id": 42, "set": {"n": 43}}, {"id": "nope", "set": {"n": 1}}]).count == 1
    assert db.get(42)["n"] == 43
    with pytest.raises(c.InvalidInputError):
        db.patch(42, {"nope": 1})
    assert db.delete(parent="doc-1").deleted == 4
    assert db.get("doc-1#0") is None
    assert [h.id for h in db.search(k=20, text="ocelot", text_field="text")] == ["doc-2#0"]
    assert db.delete(ids=[42, "007"], filter=c.eq("n", 7)).deleted == 1
    assert db.get("007") is None and db.get(42)["n"] == 43
    assert db.delete(ids=[42]).count == 1
    assert db.get(42) is None
    proof = db.prove_deletion(["doc-1#0", 42])
    assert proof["report"]["verdict"] == "deleted everywhere" and proof["algorithm"] == "Ed25519"
    assert db.deletion_key() == proof["public_key"]

    with pytest.raises(c.InvalidInputError):
        db.upsert([{"id": 1, "nope": 1}])
    with pytest.raises(c.AuthenticationError):
        c.Client(URL, "cairn_wrong_key").schema()
    with pytest.raises(c.ForbiddenError):
        c.Client(URL, os.environ["CAIRN_READ_KEY"]).upsert([{"id": 1}])

    acme = c.Client(URL, os.environ["CAIRN_ACME_KEY"])
    acme.upsert([{"id": "shared", "text": "acme's", "embedding": vec(1)}])
    db.upsert([{"id": "shared", "text": "run's", "embedding": vec(1)}])
    assert acme.get("shared")["text"] == "acme's"
    assert db.get("shared")["text"] == "run's"
    with pytest.raises(c.ForbiddenError):
        acme.with_tenant("globex").get("shared")
    with pytest.raises(c.ForbiddenError):
        acme.forget_tenant("acme")

    assert root.forget_tenant(RUN).deleted == 2
    assert db.search(k=10) == []
    assert root.forget_tenant("acme").deleted >= 1
    assert acme.get("shared") is None


def test_async_client_against_a_live_node():
    async def run():
        async with c.AsyncClient(URL, os.environ["CAIRN_KEY"], tenant=RUN + "-async") as db:
            await db.upsert([{"id": f"a{i}", "parent": "p", "text": "lynx", "embedding": vec(i)} for i in range(3)])
            assert (await db.get("a1"))["text"] == "lynx"
            assert len(await db.search(text="lynx", text_field="text")) == 3
            assert (await db.delete(parent="p")).deleted == 3
            assert await db.search(text="lynx", text_field="text") == []

    asyncio.run(run())


def test_collections_against_a_live_node():
    admin = c.Client(URL, os.environ["CAIRN_ADMIN_KEY"])
    name = f"py-notes-{os.getpid()}"
    schema = {"fields": [{"name": "embedding", "kind": {"Vector": {"dims": 2, "metric": "L2"}}},
                         {"name": "body", "kind": "Text"}, {"name": "parent", "kind": "Enum"}]}
    assert admin.create_collection(name, schema, shards=2) == {"name": name, "shards": 2, "schema": schema}
    assert name in [x["name"] for x in admin.list_collections()]
    app = c.Client(URL, os.environ["CAIRN_KEY"])
    notes = app.collection(name)
    notes.upsert([{"id": f"n{i}", "embedding": [float(i), 1.0], "body": f"walrus {i}", "parent": f"p{i % 2}"} for i in range(4)])
    assert notes.get("n1")["body"] == "walrus 1"
    assert app.get("n1") is None, "not in default"
    assert len(notes.search(text="walrus", text_field="body")) == 4
    grouped = notes.search(vector={"field": "embedding", "values": [3.0, 1.0]}, group_by="parent")
    assert [(h.id, h.group) for h in grouped] == [("n3", "p1"), ("n2", "p0")]
    assert notes.delete(parent="p0").deleted == 2
    assert sorted(h.id for h in notes.search(k=10)) == ["n1", "n3"]
    assert notes.schema() == schema
    with pytest.raises(c.ForbiddenError):
        app.create_collection("x", schema)
    admin.drop_collection(name)
    assert name not in [x["name"] for x in admin.list_collections()]
    with pytest.raises(c.NotFoundError):
        notes.search(k=1)


def test_retention_through_the_client():
    admin = c.Client(URL, os.environ["CAIRN_ADMIN_KEY"])
    name = f"py-ttl-{os.getpid()}"
    schema = {"fields": [{"name": "body", "kind": "Text"}, {"name": "until", "kind": "I64"}]}
    assert admin.create_collection(name, schema, expires_field="until")["expires_field"] == "until"
    col = c.Client(URL, os.environ["CAIRN_KEY"]).collection(name)
    now = int(time.time() * 1000)
    col.upsert([{"id": "gone", "body": "x", "until": now - 1000}, {"id": "kept", "body": "x", "until": now + 600_000}])
    assert col.get("gone") is None
    assert [h.id for h in col.search(k=10)] == ["kept"]
    admin.drop_collection(name)
