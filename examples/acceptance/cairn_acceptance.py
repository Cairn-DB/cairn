#!/usr/bin/env python3
"""Cairn acceptance suite: typical inputs against a running node or cluster, over HTTP.

Python 3.9+, standard library only. It loads a deterministic media-archive corpus (the schema
of the Docker image: embedding[384], text, source, tags, created, payload), then checks:
- authentication;
- round trips of every field type;
- text, vector and hybrid search;
- every filter operator against a locally computed ground truth, and filtered recall;
- updates;
- takedowns, never read again through any node;
- input errors;
- administration.

Embeddings are a stand-in: words hashed into 384 dimensions, so texts sharing words are
close. No model download; real embeddings behave the same way through the API.

    python3 cairn_acceptance.py --url http://127.0.0.1:7200 --key "$KEY"
    python3 cairn_acceptance.py --url http://127.0.0.1:7201 --url http://127.0.0.1:7202 \\
        --url http://127.0.0.1:7203 --key "$KEY"      # a cluster: reads go through every node

Exit status 0 when every check passes. Optional role keys (CAIRN_READ_KEY, CAIRN_WRITE_KEY,
CAIRN_TAKEDOWN_KEY) add role checks.
"""

import argparse
import base64
import hashlib
import json
import math
import os
import random
import ssl
import sys
import time
import urllib.error
import urllib.request

DIMS = 384
TOPICS = {
    "energy": "nuclear reactor energy grid solar wind power plant electricity turbine uranium",
    "sport": "football match goal league stadium coach tournament championship player season",
    "health": "hospital vaccine doctor patients epidemic treatment clinic nurse medicine surgery",
    "climate": "climate warming emissions carbon glacier drought flood temperature ocean ice",
    "politics": "parliament election minister vote government senate campaign law debate party",
    "culture": "museum exhibition painting concert festival theatre film director novel artist",
}
FILLER = "the report shows new analysis today said interview archive local national".split()
SOURCES = ["tv", "radio", "press", "web"]


# ------------------------------------------------------------------------------ corpus

def embed(text):
    """Stand-in embedding: hashed bag of words, unit length."""
    v = [0.0] * DIMS
    for w in text.lower().split():
        h = hashlib.sha256(w.encode()).digest()
        for j in range(3):
            i = int.from_bytes(h[4 * j:4 * j + 4], "little") % DIMS
            v[i] += 1.0 if h[20 + j] & 1 else -1.0
    n = math.sqrt(sum(x * x for x in v)) or 1.0
    return [round(x / n, 6) for x in v]


def corpus(n, seed=7):
    rng = random.Random(seed)
    docs = []
    for i in range(1, n + 1):
        topic = rng.choice(sorted(TOPICS))
        words = TOPICS[topic].split()
        text = " ".join(rng.sample(words, 4) + rng.sample(FILLER, 3) + [f"ref{i}"])
        d = {"id": i, "text": text, "source": rng.choice(SOURCES),
             "embedding": embed(text), "_topic": topic}
        if rng.random() > 0.1:  # 10% without tags
            d["tags"] = sorted({topic} | ({"breaking"} if rng.random() < 0.2 else set()))
        if rng.random() > 0.05:  # 5% without a date
            d["created"] = 19000 + rng.randrange(1500)
        if rng.random() < 0.1:  # 10% with an attachment
            d["payload"] = base64.b64encode(rng.randbytes(24)).decode()
        docs.append(d)
    return docs


def public(d):
    return {k: v for k, v in d.items() if not k.startswith("_")}


def cosine(a, b):
    return sum(x * y for x, y in zip(a, b))


def matches(d, f):
    """Local evaluation of a filter, the ground truth for exact comparisons."""
    if not f:
        return True
    if "and" in f:
        return all(matches(d, x) for x in f["and"])
    if "or" in f:
        return any(matches(d, x) for x in f["or"])
    if "not" in f:
        return not matches(d, f["not"])
    v = d.get(f["field"])
    if f.get("is_null"):
        return v is None
    if v is None:
        return False
    vals = v if isinstance(v, list) else [v]
    ok = True
    if "eq" in f:
        ok &= f["eq"] in vals
    if "in" in f:
        ok &= any(x in vals for x in f["in"])
    if "gt" in f:
        ok &= v > f["gt"]
    if "gte" in f:
        ok &= v >= f["gte"]
    if "lt" in f:
        ok &= v < f["lt"]
    if "lte" in f:
        ok &= v <= f["lte"]
    return ok


# ------------------------------------------------------------------------------ client

class Api:
    def __init__(self, urls, key, insecure=False):
        self.urls, self.key = urls, key
        self.ctx = ssl._create_unverified_context() if insecure else None
        self.n = 0

    def call(self, method, path, body=None, key="default", node=None, raw=None):
        url = self.urls[self.n % len(self.urls) if node is None else node] + path
        self.n += 1
        data = raw if raw is not None else (json.dumps(body).encode() if body is not None else None)
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Content-Type", "application/json")
        k = self.key if key == "default" else key
        if k:
            req.add_header("Authorization", f"Bearer {k}")
        try:
            with urllib.request.urlopen(req, timeout=60, context=self.ctx) as r:
                text = r.read()
                return r.status, (json.loads(text) if text else None)
        except urllib.error.HTTPError as e:
            text = e.read()
            try:
                return e.code, json.loads(text)
            except ValueError:
                return e.code, text.decode(errors="replace")


# ------------------------------------------------------------------------------ checks

class Report:
    def __init__(self):
        self.rows = []

    def check(self, name, ok, detail=""):
        self.rows.append((name, bool(ok), detail))
        print(f"{'PASS' if ok else 'FAIL'}  {name}{('  -- ' + detail) if detail else ''}", flush=True)
        return ok

    def failed(self):
        return [r for r in self.rows if not r[1]]


def run(api, n, rep):
    nodes = range(len(api.urls))
    docs = corpus(n)
    by_id = {d["id"]: d for d in docs}
    rng = random.Random(11)

    # --- authentication
    st, _ = api.call("GET", "/health", key=None)
    rep.check("health is open", st == 200, f"status {st}")
    st, _ = api.call("GET", "/v1/schema", key=None)
    rep.check("no key -> 401", st == 401, f"status {st}")
    st, _ = api.call("GET", "/v1/schema", key="cairn_not_a_real_key_000000")
    rep.check("wrong key -> 401", st == 401, f"status {st}")
    st, sch = api.call("GET", "/v1/schema")
    names = [f["name"] for f in (sch or {}).get("fields", [])] if st == 200 else []
    rep.check("schema is the media-archive schema",
              names == ["embedding", "text", "source", "tags", "created", "payload"], str(names))

    # --- ingest
    token, t0 = None, time.time()
    for i in range(0, n, 250):
        body = {"documents": [public(d) for d in docs[i:i + 250]]}
        if token:
            body["after"] = token
        st, ack = api.call("POST", "/v1/documents", body, node=0)
        if not rep.check(f"ingest batch {i // 250 + 1}", st == 200 and ack["count"] == len(body["documents"]),
                         "" if st == 200 else f"{st} {ack}") and st != 200:
            return
        token = ack["consistency_token"]
    rate = n / (time.time() - t0)
    rep.check("ingest", True, f"{n} documents, {rate:.0f} docs/s")

    # --- round trips, through every node
    ok, bad = 0, []
    for i, doc_id in enumerate(rng.sample(sorted(by_id), 30)):
        st, got = api.call("GET", f"/v1/documents/{doc_id}?after={token}", node=i % len(api.urls))
        want = public(by_id[doc_id])
        same = st == 200 and all(
            (abs(a - b) < 1e-5 for a, b in zip(got.get(k) or [], v)) if k == "embedding"
            else (sorted(got.get(k) or []) == sorted(v)) if k == "tags"
            else got.get(k) == v
            for k, v in want.items())
        same = same and all(got.get(k) is None for k in ("tags", "created", "payload") if k not in want)
        ok += same
        if not same:
            bad.append(doc_id)
    rep.check("read-your-writes round trip of every field type, every node", ok == 30, f"mismatch {bad[:5]}")

    def search(body, node=None, after=None):
        body = dict(body)
        body["after"] = after or token
        return api.call("POST", "/v1/search", body, node=node)

    # --- text search
    st, res = search({"k": 10, "text": {"field": "text", "query": "nuclear reactor"}})
    hits = res.get("hits", []) if st == 200 else []
    on_topic = sum(by_id[h["id"]]["_topic"] == "energy" for h in hits)
    rep.check("text search finds the topic", st == 200 and len(hits) == 10 and on_topic == 10,
              f"{on_topic}/10 on topic")
    st, res = search({"k": 50, "text": {"field": "text", "query": "vaccine hospital", "all_terms": True}})
    hits = res.get("hits", []) if st == 200 else []
    rep.check("text search with all_terms", st == 200 and hits and all(
        {"vaccine", "hospital"} <= set(h["document"]["text"].split()) for h in hits), f"{len(hits)} hits")

    # --- vector search: a document finds itself, and recall against brute force
    selves = rng.sample(docs, 20)
    top1 = 0
    for d in selves:
        st, res = search({"k": 1, "vector": {"field": "embedding", "values": d["embedding"]},
                          "with_documents": False})
        top1 += st == 200 and res["hits"] and res["hits"][0]["id"] == d["id"]
    rep.check("vector search: a document is its own nearest neighbour", top1 >= 19, f"{top1}/20")

    def recall(filt, label):
        total, found, viol = 0, 0, 0
        for q in range(20):
            topic = rng.choice(sorted(TOPICS))
            qv = embed(" ".join(rng.sample(TOPICS[topic].split(), 3)))
            truth = sorted((d for d in docs if matches(d, filt)), key=lambda d: -cosine(d["embedding"], qv))[:10]
            body = {"k": 10, "vector": {"field": "embedding", "values": qv}, "with_documents": False}
            if filt:
                body["filter"] = filt
            st, res = search(body)
            got = [h["id"] for h in res.get("hits", [])] if st == 200 else []
            viol += sum(not matches(by_id[i], filt) for i in got)
            total += len(truth)
            found += len({d["id"] for d in truth} & set(got))
        r = found / max(total, 1)
        rep.check(f"vector recall@10, {label}", r >= 0.9 and viol == 0, f"recall {r:.3f}, filter violations {viol}")

    recall(None, "unfiltered")
    recall({"and": [{"field": "source", "eq": "tv"}, {"field": "created", "gte": 19500, "lt": 20200}]},
           "filtered (source tv, date range)")

    # --- hybrid
    qv = embed("election parliament vote")
    st, res = search({"k": 10, "vector": {"field": "embedding", "values": qv},
                      "text": {"field": "text", "query": "election vote"}})
    hits = res.get("hits", []) if st == 200 else []
    rep.check("hybrid search (RRF): two legs per hit", st == 200 and hits and all(len(h["legs"]) == 2 for h in hits)
              and sum(by_id[h["id"]]["_topic"] == "politics" for h in hits) >= 9, f"{len(hits)} hits")
    st, res = search({"k": 10, "vector": {"field": "embedding", "values": qv},
                      "text": {"field": "text", "query": "election vote"}, "fusion": {"weighted": [0.7, 0.3]}})
    rep.check("hybrid search, weighted fusion", st == 200 and len(res["hits"]) == 10, f"status {st}")

    # --- filters, exact against the local truth (pure filters: every match, in id order)
    filters = {
        "enum eq": {"field": "source", "eq": "radio"},
        "enum in": {"field": "source", "in": ["tv", "web"]},
        "date range": {"field": "created", "gte": 19200, "lt": 19400},
        "date gt/lte": {"field": "created", "gt": 20000, "lte": 20400},
        "set contains (eq)": {"field": "tags", "eq": "breaking"},
        "set contains any (in)": {"field": "tags", "in": ["sport", "health"]},
        "is_null (date)": {"field": "created", "is_null": True},
        "not": {"not": {"field": "source", "eq": "press"}},
        "or": {"or": [{"field": "tags", "eq": "climate"}, {"field": "source", "eq": "radio"}]},
        "and + not": {"and": [{"field": "tags", "eq": "culture"}, {"not": {"field": "source", "eq": "web"}}]},
    }
    for label, f in filters.items():
        st, res = search({"k": 10000, "filter": f, "with_documents": False})
        got = [h["id"] for h in res.get("hits", [])] if st == 200 else None
        want = sorted(d["id"] for d in docs if matches(d, f))
        rep.check(f"filter {label}: exact", got == want,
                  f"{len(want)} expected" if got == want else f"status {st}, got {None if got is None else len(got)}")

    # --- update
    target = rng.choice(docs)
    new = dict(public(target), text="zephyrine unique replacement text", source="web")
    new["embedding"] = embed(new["text"])
    st, ack = api.call("POST", "/v1/documents", {"documents": [new], "after": token}, node=0)
    token = ack["consistency_token"] if st == 200 else token
    st, res = search({"k": 5, "text": {"field": "text", "query": "zephyrine"}}, node=len(api.urls) - 1)
    rep.check("update replaces the document (read through another node)",
              st == 200 and [h["id"] for h in res["hits"]] == [target["id"]]
              and res["hits"][0]["document"]["source"] == "web")
    by_id[target["id"]] = dict(new, _topic="none")
    docs = [by_id[i] for i in sorted(by_id)]

    # --- takedowns: never read again with the token, through any node
    victim = rng.choice(docs)
    st, ack = api.call("DELETE", f"/v1/documents/{victim['id']}?after={token}", node=0)
    rep.check("takedown acknowledged", st == 200 and ack["count"] == 1, f"status {st}")
    token = ack["consistency_token"]
    ok = True
    for node in nodes:
        st, _ = api.call("GET", f"/v1/documents/{victim['id']}?after={token}", node=node)
        ok &= st == 404
        _, res = search({"k": 10, "vector": {"field": "embedding", "values": victim["embedding"]}}, node=node)
        ok &= victim["id"] not in [h["id"] for h in res.get("hits", [])]
        _, res = search({"k": 10, "text": {"field": "text", "query": f"ref{victim['id']}"}}, node=node)
        ok &= victim["id"] not in [h["id"] for h in res.get("hits", [])]
    rep.check("taken-down document is gone from reads, vector and text search, on every node", ok)

    bulk = [d["id"] for d in rng.sample(docs, 50) if d["id"] != victim["id"]]
    st, ack = api.call("POST", "/v1/documents/delete", {"ids": bulk, "after": token}, node=len(api.urls) - 1)
    rep.check("bulk takedown acknowledged", st == 200 and ack["count"] == len(bulk), f"status {st}")
    token = ack["consistency_token"]
    gone = set(bulk) | {victim["id"]}
    live = sorted(i for i in by_id if i not in gone)
    ok = True
    for node in nodes:
        st, res = search({"k": 10000, "filter": {"or": [{"field": "source", "in": SOURCES},
                                                         {"field": "source", "is_null": True}]},
                          "with_documents": False}, node=node)
        ok &= st == 200 and [h["id"] for h in res["hits"]] == live
    rep.check("after the bulk takedown every node lists exactly the live documents", ok, f"{len(live)} live")

    # --- consistency levels
    st, _ = api.call("POST", "/v1/search", {"k": 3, "text": {"field": "text", "query": "museum"},
                                            "consistency": "stale"})
    rep.check("stale read", st == 200, f"status {st}")
    st, _ = api.call("POST", "/v1/search", {"k": 3, "text": {"field": "text", "query": "museum"},
                                            "consistency": "linearizable"})
    rep.check("linearizable read", st == 200, f"status {st}")

    # --- input errors
    errors = [
        ("vector with the wrong dimension", "POST", "/v1/search",
         {"vector": {"field": "embedding", "values": [0.1, 0.2]}}, 400),
        ("unknown field in a document", "POST", "/v1/documents", {"documents": [{"id": 999999, "nope": 1}]}, 400),
        ("wrong type for a field", "POST", "/v1/documents", {"documents": [{"id": 999999, "created": "yesterday"}]}, 400),
        ("unknown filter operator", "POST", "/v1/search", {"filter": {"field": "source", "like": "t%"}}, 400),
        ("range on a text field", "POST", "/v1/search", {"filter": {"field": "text", "gte": 3}}, 400),
        ("malformed consistency token", "POST", "/v1/search", {"text": {"field": "text", "query": "x"}, "after": "zz"}, 400),
        ("k = 0", "POST", "/v1/search", {"k": 0, "text": {"field": "text", "query": "x"}}, 400),
        ("text leg on a vector field", "POST", "/v1/search", {"text": {"field": "embedding", "query": "x"}}, 400),
        ("fusion weights of the wrong length", "POST", "/v1/search",
         {"text": {"field": "text", "query": "x"}, "fusion": {"weighted": [1, 2]}}, 400),
        ("unknown consistency level", "POST", "/v1/search", {"text": {"field": "text", "query": "x"}, "consistency": "eventual"}, 400),
    ]
    for label, m, p, b, want in errors:
        st, res = api.call(m, p, b)
        rep.check(f"error: {label} -> {want}", st == want and isinstance(res, dict) and "error" in res,
                  f"status {st} {str(res)[:80]}")
    st, _ = api.call("GET", "/v1/documents/987654321")
    rep.check("missing document -> 404", st == 404, f"status {st}")
    st, res = api.call("POST", "/v1/documents", raw=b"{not json")
    rep.check("invalid JSON -> 4xx", 400 <= st < 500, f"status {st}")

    # --- administration
    st, res = api.call("GET", "/v1/status")
    rep.check("status lists this node's replicas", st == 200 and len(res.get("replicas", [])) > 0, f"status {st}")
    st1, _ = api.call("POST", "/v1/admin/merges", {"paused": True}, node=0)
    st2, res = api.call("GET", "/v1/admin/merges", node=0)
    st3, _ = api.call("POST", "/v1/admin/merges", {"paused": False}, node=0)
    rep.check("merge pause and resume", (st1, st2, st3) == (200, 200, 200) and res == {"paused": True})

    # --- optional role keys
    roles = {r: os.environ.get(f"CAIRN_{r.upper()}_KEY") for r in ("read", "write", "takedown")}
    if roles["read"]:
        st, _ = api.call("POST", "/v1/documents", {"documents": [public(docs[0])]}, key=roles["read"])
        rep.check("read key cannot write -> 403", st == 403, f"status {st}")
        st, _ = api.call("DELETE", f"/v1/documents/{docs[1]['id']}", key=roles["read"])
        rep.check("read key cannot take down -> 403", st == 403, f"status {st}")
    if roles["write"]:
        st, _ = api.call("DELETE", f"/v1/documents/{docs[1]['id']}", key=roles["write"])
        rep.check("write key cannot take down -> 403", st == 403, f"status {st}")
    if roles["takedown"]:
        st, _ = api.call("POST", "/v1/search", {"text": {"field": "text", "query": "x"}}, key=roles["takedown"])
        rep.check("takedown key cannot search -> 403", st == 403, f"status {st}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", action="append", required=True, help="a node's HTTP address (repeat for a cluster)")
    ap.add_argument("--key", default=os.environ.get("CAIRN_KEY"), help="an admin API key (or CAIRN_KEY)")
    ap.add_argument("--docs", type=int, default=3000, help="corpus size (at most 9000)")
    ap.add_argument("--insecure", action="store_true", help="accept a self-signed HTTPS certificate")
    a = ap.parse_args()
    if not a.key:
        sys.exit("an admin key is needed: --key or CAIRN_KEY")
    rep = Report()
    run(Api([u.rstrip("/") for u in a.url], a.key, a.insecure), min(a.docs, 9000), rep)
    bad = rep.failed()
    print(f"\n{len(rep.rows) - len(bad)}/{len(rep.rows)} checks passed")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
