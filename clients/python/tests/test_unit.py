"""Request shapes, token tracking, retries and typed errors, on a mock transport."""

import asyncio
import json

import httpx
import pytest

import cairn_db as c


def mock(db, responses, calls):
    def handler(req: httpx.Request) -> httpx.Response:
        calls.append(req)
        r = responses.pop(0)
        if isinstance(r, Exception):
            raise r
        status, body = r
        return httpx.Response(status, json=body)

    if isinstance(db, c.AsyncClient):
        db._http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    else:
        db._http = httpx.Client(transport=httpx.MockTransport(handler))
    return db


def test_tokens_and_filters():
    assert c.merge_tokens("0.5,1.2", "1.7,2.1", None, "") == "0.5,1.7,2.1"
    with pytest.raises(c.InvalidInputError):
        c.merge_tokens("x")
    assert c.and_(c.eq("s", "tv"), c.or_(c.in_("t", ["a"]), c.not_(c.is_null("d"))), c.range_("n", gte=1, lt=5)) == {
        "and": [
            {"field": "s", "eq": "tv"},
            {"or": [{"field": "t", "in": ["a"]}, {"not": {"field": "d", "is_null": True}}]},
            {"field": "n", "gte": 1, "lt": 5},
        ]
    }


def test_requests_and_token_tracking():
    calls = []
    db = mock(
        c.Client("http://n1/", "k", parent_field="doc"),
        [
            (200, {"count": 2, "consistency_token": "0.4,1.9"}),
            (200, {"id": "12"}),
            (200, {"hits": [{"id": "a", "score": 0.5, "legs": [{"rank": 1, "score": 2.0}, None], "_tenant": "acme"}]}),
            (200, {"deleted": 3, "consistency_token": "1.12"}),
            (404, {"error": "no document"}),
            (200, {"deleted": 5, "consistency_token": "0.20"}),
        ],
        calls,
    )
    assert db.upsert([{"id": "a"}, {"id": 3}]) == c.WriteResult(count=2, token="0.4,1.9")
    assert calls[0].headers["authorization"] == "Bearer k"
    db.get("12")
    assert str(calls[1].url) == "http://n1/v1/documents/12?id_type=text&after=0.4%2C1.9"
    hits = db.search(k=5, text="q", text_field="text", all_terms=True, vector=[{"field": "e", "values": [1.0]}],
                     with_documents=False, filter=c.eq("a", 1))
    assert json.loads(calls[2].content) == {
        "k": 5, "vectors": [{"field": "e", "values": [1.0]}], "text": {"field": "text", "query": "q", "all_terms": True},
        "filter": {"field": "a", "eq": 1}, "with_documents": False, "after": "0.4,1.9"}
    assert hits == [c.Hit(id="a", score=0.5, legs=[c.LegHit(1, 2.0), None], tenant="acme")]
    assert db.delete(parent="doc-9") == c.DeleteResult(token="1.12", deleted=3)
    assert json.loads(calls[3].content) == {"filter": {"field": "doc", "eq": "doc-9"}, "after": "0.4,1.9"}
    assert db.token == "0.4,1.12"
    assert db.get("a/b c") is None
    assert str(calls[4].url).startswith("http://n1/v1/documents/a%2Fb%20c?after=")
    acme = db.with_tenant("acme")
    acme._http = db._http
    acme.forget_tenant("globex")
    assert calls[5].headers["cairn-tenant"] == "acme"
    assert str(calls[5].url) == "http://n1/v1/tenants/globex?after=0.4%2C1.12"
    assert db.token == "0.20,1.12", "tenant views share the token"
    with pytest.raises(c.InvalidInputError):
        db.delete()
    with pytest.raises(c.InvalidInputError):
        db.delete(parent="x", ids=[1])


def test_retries_and_errors():
    calls = []
    db = mock(c.Client(["http://a", "http://b"], retries=3), [
        (503, {"error": "no leader"}), httpx.ConnectError("refused"), (200, {"count": 1, "consistency_token": "0.1"})
    ], calls)
    db.upsert([{"id": 1}])
    assert [r.url.host for r in calls] == ["a", "b", "a"]
    for status, cls in [(400, c.InvalidInputError), (401, c.AuthenticationError), (403, c.ForbiddenError), (500, c.CairnError)]:
        db = mock(c.Client("http://a"), [(status, {"error": "boom"})], [])
        with pytest.raises(cls) as e:
            db.upsert([{"id": 1}])
        assert (e.value.status, e.value.message) == (status, "boom")
    db = mock(c.Client("http://a", retries=1), [(503, {}), (503, {})], [])
    with pytest.raises(c.UnavailableError):
        db.search()


def test_async_client():
    async def run():
        calls = []
        db = mock(c.AsyncClient("http://a"), [
            (503, {}), (200, {"count": 1, "consistency_token": "2.3"}), (200, {"hits": []}),
        ], calls)
        assert (await db.upsert([{"id": "x"}])).token == "2.3"
        assert await db.search(filter=c.eq("a", 1)) == []
        assert json.loads(calls[2].content)["after"] == "2.3"
        await db.aclose()
    asyncio.run(run())
