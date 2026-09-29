// Against a running node (clients/test-live.sh): CAIRN_URL, CAIRN_KEY (read,write,takedown),
// CAIRN_ACME_KEY (scoped to tenant "acme"), CAIRN_READ_KEY (read only). Skipped without them.
import { test } from "node:test";
import assert from "node:assert/strict";
import { Cairn, eq, range, AuthenticationError, ForbiddenError, InvalidInputError } from "../dist/esm/index.js";

const url = process.env.CAIRN_URL;
const skip = !url && "CAIRN_URL not set";
const run = `ts-${process.pid}-${Date.now()}`;
const vec = (i) => [1, i, 0.5, 0];

test("the client against a live node", { skip }, async () => {
  const root = new Cairn({ url: url.split(","), apiKey: process.env.CAIRN_KEY });
  const db = root.withTenant(run); // a fresh tenant: this run's data only
  const chunks = [0, 1, 2, 3].map((i) => ({ id: `doc-1#${i}`, parent: "doc-1", text: `ocelot chunk ${i}`, embedding: vec(i), n: i }));
  chunks.push({ id: "doc-2#0", parent: "doc-2", text: "ocelot other", embedding: vec(9), n: 9, tags: ["x"] });
  chunks.push({ id: 42, text: "integer id", embedding: vec(4), n: 42 });
  chunks.push({ id: "007", text: "digits text id", embedding: vec(5), n: 7 });
  assert.equal((await db.upsert(chunks)).count, 7);

  // Reads reflect the writes, with ids as written.
  assert.deepEqual(await db.get("doc-1#2"), { id: "doc-1#2", parent: "doc-1", text: "ocelot chunk 2", embedding: vec(2), n: 2 });
  assert.equal((await db.get(42)).text, "integer id");
  assert.equal((await db.get("007")).text, "digits text id");
  assert.equal(await db.get("missing"), null);
  let hits = await db.search({ k: 20, text: { field: "text", query: "ocelot" } });
  assert.deepEqual(hits.map((h) => h.id).sort(), ["doc-1#0", "doc-1#1", "doc-1#2", "doc-1#3", "doc-2#0"]);
  assert.ok(hits.every((h) => h.document && h._tenant === undefined));
  hits = await db.search({ k: 3, vector: { field: "embedding", values: vec(4) }, filter: range("n", { gte: 4 }), withDocuments: false });
  assert.equal(hits[0].id, 42);

  // A token handed to another client: its reads reflect these writes.
  const other = new Cairn({ url, apiKey: process.env.CAIRN_KEY, tenant: run, token: db.token });
  assert.equal((await other.get("doc-2#0")).parent, "doc-2");

  // Deletion by parent, by ids with a filter, by id.
  assert.equal((await db.delete({ parent: "doc-1" })).deleted, 4);
  assert.equal(await db.get("doc-1#0"), null);
  hits = await db.search({ k: 20, text: { field: "text", query: "ocelot" } });
  assert.deepEqual(hits.map((h) => h.id), ["doc-2#0"]);
  assert.equal((await db.delete({ ids: [42, "007"], filter: eq("n", 7) })).deleted, 1);
  assert.equal(await db.get("007"), null);
  assert.equal((await db.get(42)).n, 42);
  assert.equal((await db.delete({ ids: [42] })).count, 1);
  assert.equal(await db.get(42), null);

  // Typed errors.
  await assert.rejects(db.upsert([{ id: 1, nope: 1 }]), InvalidInputError);
  await assert.rejects(new Cairn({ url, apiKey: "cairn_wrong_key" }).schema(), AuthenticationError);
  await assert.rejects(new Cairn({ url, apiKey: process.env.CAIRN_READ_KEY }).upsert([{ id: 1 }]), ForbiddenError);

  // Tenants: a scoped key and an unscoped view keep their own "shared".
  const acme = new Cairn({ url, apiKey: process.env.CAIRN_ACME_KEY });
  await acme.upsert([{ id: "shared", text: "acme's", embedding: vec(1) }]);
  await db.upsert([{ id: "shared", text: "run's", embedding: vec(1) }]);
  assert.equal((await acme.get("shared")).text, "acme's");
  assert.equal((await db.get("shared")).text, "run's");
  await assert.rejects(acme.withTenant("globex").get("shared"), ForbiddenError);
  await assert.rejects(acme.forgetTenant("acme"), ForbiddenError);

  // Erasing the tenants.
  assert.equal((await root.forgetTenant(run)).deleted, 2);
  assert.deepEqual(await db.search({ k: 10 }), []);
  assert.ok((await root.forgetTenant("acme")).deleted >= 1);
  assert.equal(await acme.get("shared"), null);
});
