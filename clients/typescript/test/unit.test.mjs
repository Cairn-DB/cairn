// Unit tests with a fake fetch: request shapes, token tracking, retries, typed errors.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  Cairn, mergeTokens, eq, in_, range, and_, or_, not_, isNull,
  AuthenticationError, ForbiddenError, InvalidInputError, UnavailableError, CairnError,
} from "../dist/esm/index.js";
import { createRequire } from "node:module";

function fake(responses) {
  const calls = [];
  const f = async (url, init) => {
    calls.push({ url, method: init.method, headers: init.headers, body: init.body && JSON.parse(init.body) });
    const r = responses.shift();
    if (r instanceof Error) throw r;
    return new Response(JSON.stringify(r.body ?? {}), { status: r.status ?? 200 });
  };
  return { f, calls };
}

test("tokens merge per shard", () => {
  assert.equal(mergeTokens("0.5,1.2", "1.7,2.1", undefined, ""), "0.5,1.7,2.1");
  assert.throws(() => mergeTokens("x"), InvalidInputError);
});

test("filter helpers build the API's filter objects", () => {
  assert.deepEqual(
    and_(eq("source", "tv"), or_(in_("tags", ["a", "b"]), not_(isNull("created"))), range("n", { gte: 1, lt: 5 })),
    { and: [{ field: "source", eq: "tv" },
            { or: [{ field: "tags", in: ["a", "b"] }, { not: { field: "created", is_null: true } }] },
            { field: "n", gte: 1, lt: 5 }] },
  );
});

test("writes are tracked and passed to reads; paths and bodies", async () => {
  const { f, calls } = fake([
    { body: { count: 2, consistency_token: "0.4,1.9" } },
    { body: { id: "12", text: "x" } },
    { body: { hits: [] } },
    { body: { deleted: 3, consistency_token: "1.12" } },
    { status: 404, body: { error: "no document" } },
    { body: { deleted: 5, consistency_token: "0.20" } },
  ]);
  const db = new Cairn({ url: "http://n1/", apiKey: "k", fetch: f, parentField: "doc" });
  const w = await db.upsert([{ id: "a", text: "x" }, { id: 3 }]);
  assert.deepEqual(w, { count: 2, token: "0.4,1.9" });
  assert.equal(calls[0].url, "http://n1/v1/documents");
  assert.equal(calls[0].headers.authorization, "Bearer k");
  await db.get("12");
  assert.equal(calls[1].url, "http://n1/v1/documents/12?id_type=text&after=0.4%2C1.9");
  await db.search({ k: 5, text: { field: "text", query: "q", allTerms: true }, vector: [{ field: "e", values: [1] }],
                    withDocuments: false, filter: eq("a", 1) });
  assert.deepEqual(calls[2].body, { k: 5, vectors: [{ field: "e", values: [1] }],
    text: { field: "text", query: "q", all_terms: true }, filter: { field: "a", eq: 1 },
    with_documents: false, after: "0.4,1.9" });
  const d = await db.delete({ parent: "doc-9" });
  assert.deepEqual(d, { count: undefined, deleted: 3, token: "1.12" });
  assert.deepEqual(calls[3].body, { filter: { field: "doc", eq: "doc-9" }, after: "0.4,1.9" });
  assert.equal(db.token, "0.4,1.12");
  assert.equal(await db.get("a/b c"), null);
  assert.ok(calls[4].url.startsWith("http://n1/v1/documents/a%2Fb%20c?after="));
  const acme = db.withTenant("acme");
  await acme.forgetTenant("globex");
  assert.equal(calls[5].headers["cairn-tenant"], "acme");
  assert.equal(calls[5].url, "http://n1/v1/tenants/globex?after=0.4%2C1.12");
  assert.equal(db.token, "0.20,1.12", "tenant views share the token");
});

test("503 and network errors are retried on the next node; other errors are typed", async () => {
  const { f, calls } = fake([
    { status: 503, body: { error: "no leader" } },
    new TypeError("fetch failed"),
    { body: { count: 1, consistency_token: "0.1" } },
  ]);
  const db = new Cairn({ url: ["http://a", "http://b"], fetch: f, retries: 3 });
  await db.upsert([{ id: 1 }]);
  assert.deepEqual(calls.map((c) => new URL(c.url).host), ["a", "b", "a"]);
  for (const [status, cls] of [[400, InvalidInputError], [401, AuthenticationError], [403, ForbiddenError], [500, CairnError]]) {
    const { f } = fake([{ status, body: { error: "boom" } }]);
    await assert.rejects(new Cairn({ url: "http://a", fetch: f }).upsert([{ id: 1 }]),
      (e) => e instanceof cls && e.status === status && e.message === "boom");
  }
  const { f: down } = fake([{ status: 503 }, { status: 503 }]);
  await assert.rejects(new Cairn({ url: "http://a", fetch: down, retries: 1 }).search({}), UnavailableError);
});

test("CommonJS build loads", () => {
  const require = createRequire(import.meta.url);
  const cjs = require("../dist/cjs/index.js");
  assert.equal(typeof cjs.Cairn, "function");
  assert.equal(cjs.mergeTokens("0.1", "0.2"), "0.2");
});
