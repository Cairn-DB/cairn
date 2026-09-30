// The service against a live node: CAIRN_URL and CAIRN_ADMIN_KEY (and CAIRN_KEY).
import { test } from "node:test";
import assert from "node:assert/strict";
import { Cairn } from "@cairn-db/client";

const url = process.env.CAIRN_URL;
process.env.CAIRN_KEY ??= process.env.CAIRN_ADMIN_KEY;
const SCHEMA = { fields: [
  { name: "embedding", kind: { Vector: { dims: 384, metric: "Cosine" } } },
  { name: "text", kind: "Text" }, { name: "title", kind: "Text" }, { name: "parent", kind: "Enum" },
  { name: "product", kind: "Enum" }, { name: "lang", kind: "Enum" }, { name: "rev", kind: "I64" },
  { name: "deprecated", kind: "Bool" },
] };

test("the service", { skip: !url && "CAIRN_URL not set" }, async () => {
  const admin = new Cairn({ url, apiKey: process.env.CAIRN_ADMIN_KEY });
  if ((await admin.listCollections()).some((c) => c.name === "articles")) await admin.dropCollection("articles");
  await admin.createCollection("articles", SCHEMA, { shards: 2 });
  const { app } = await import("./server.mjs");
  const server = app.listen(0);
  const base = `http://127.0.0.1:${server.address().port}`;
  const call = async (method, path, body, headers = {}) => {
    const r = await fetch(base + path, { method, headers: { "content-type": "application/json", ...headers }, body: body && JSON.stringify(body) });
    return { status: r.status, token: r.headers.get("x-cairn-token"), json: await r.json() };
  };
  try {
    const long = Array.from({ length: 6 }, (_, i) => `Step ${i}: open Settings, then Security, to reset the password. `.repeat(5)).join("\n\n");
    assert.ok((await call("PUT", "/customers/acme/articles/reset-password", { title: "Reset", body: long, product: "auth" })).json.chunks > 1);
    await call("PUT", "/customers/globex/articles/reset-password", { title: "Globex", body: "Globex passwords rotate every 90 days.", product: "auth" });
    let hits = (await call("GET", "/customers/acme/search?q=reset%20password")).json;
    assert.equal(hits[0].article, "reset-password");
    assert.equal(hits.length, 1, "one hit per article, and no other customer's");
    const { token } = await call("PUT", "/customers/acme/articles/reset-password", { title: "Reset", body: "Open Settings, then Security.", product: "auth" });
    hits = (await call("GET", "/customers/acme/search?q=step%205%20settings", undefined, { "X-Cairn-Token": token })).json;
    assert.deepEqual(hits.map((h) => h.snippet), ["Open Settings, then Security."], "no stale chunk");
    assert.equal((await call("DELETE", "/customers/acme/articles/reset-password")).json.chunks_deleted, 1);
    const erased = (await call("DELETE", "/customers/globex")).json;
    assert.equal(erased.proof.report.verdict, "deleted everywhere");
    assert.deepEqual((await call("GET", "/customers/globex/search?q=passwords")).json, []);
  } finally {
    server.close();
    await admin.dropCollection("articles");
  }
});
