"""The service against a live node: CAIRN_URL, CAIRN_ADMIN_KEY (and CAIRN_KEY), EMBEDDER=hash."""

import os

import pytest

os.environ.setdefault("EMBEDDER", "hash")
os.environ.setdefault("CAIRN_KEY", os.environ.get("CAIRN_ADMIN_KEY", ""))
pytestmark = pytest.mark.skipif(not os.environ.get("CAIRN_URL"), reason="CAIRN_URL not set")


@pytest.fixture(scope="module")
def api():
    import setup
    from cairn_db import Client
    from fastapi.testclient import TestClient

    admin = Client(os.environ["CAIRN_URL"], api_key=os.environ["CAIRN_ADMIN_KEY"])
    if "articles" in [c["name"] for c in admin.list_collections()]:
        admin.drop_collection("articles")
    admin.create_collection("articles", setup.SCHEMA, shards=2)
    import app

    yield TestClient(app.app)
    admin.drop_collection("articles")


def put(api, customer, article, body, product="auth"):
    r = api.put(f"/customers/{customer}/articles/{article}", json={"title": article, "body": body, "product": product})
    assert r.status_code == 200, r.text
    return r


def test_the_service(api):
    long = "\n\n".join(f"Step {i}: open Settings, then Security, to reset the password." * 5 for i in range(6))
    assert put(api, "acme", "reset-password", long).json()["chunks"] > 1
    put(api, "acme", "invoices", "Invoices are under Billing, then History.", product="billing")
    put(api, "globex", "reset-password", "Globex passwords rotate every 90 days.")

    hits = api.get("/customers/acme/search", params={"q": "reset password"}).json()
    assert hits[0]["article"] == "reset-password"
    assert len({h["article"] for h in hits}) == len(hits), "one hit per article"
    assert all("Globex" not in h["snippet"] for h in hits), "customers are isolated"
    assert [h["article"] for h in api.get("/customers/acme/search", params={"q": "password", "product": "billing"}).json()] == ["invoices"]

    # A shorter version leaves no stale chunk behind.
    token = put(api, "acme", "reset-password", "Open Settings, then Security.").headers["X-Cairn-Token"]
    hits = api.get("/customers/acme/search", params={"q": "Step 5 settings security"}, headers={"X-Cairn-Token": token}).json()
    assert [h["snippet"] for h in hits if h["article"] == "reset-password"] == ["Open Settings, then Security."]

    assert api.post("/customers/acme/articles/invoices/deprecate").status_code == 200
    assert [h["article"] for h in api.get("/customers/acme/search", params={"q": "invoices billing"}).json()] == ["reset-password"]

    r = api.delete("/customers/acme/articles/reset-password")
    assert r.json()["chunks_deleted"] == 1
    assert api.get("/customers/acme/search", params={"q": "settings security"}).json() == []

    r = api.delete("/customers/globex").json()
    assert r["chunks_erased"] == 1 and r["proof"]["report"]["verdict"] == "deleted everywhere"
    assert api.get("/customers/globex/search", params={"q": "passwords"}).json() == []

    bad = api.put("/customers/acme/articles/x", json={"title": "x"})
    assert bad.status_code == 422
