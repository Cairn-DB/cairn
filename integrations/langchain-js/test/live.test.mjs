// Against a running node (clients/test-live.sh): CAIRN_URL, CAIRN_ADMIN_KEY, CAIRN_KEY.
import { test } from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { Cairn, NotFoundError } from "@cairn-db/client";
import { Document } from "@langchain/core/documents";
import { CairnVectorStore, collectionSchema } from "../dist/index.js";

const url = process.env.CAIRN_URL;
const DIMS = 64;

function wordHash(text) {
  const v = new Array(DIMS).fill(0);
  for (const w of text.toLowerCase().split(/\s+/).filter(Boolean)) {
    const h = createHash("sha1").update(w).digest();
    v[h[0] % DIMS] += h[1] & 1 ? 1 : -1;
  }
  const n = Math.sqrt(v.reduce((s, x) => s + x * x, 0)) || 1;
  return v.map((x) => x / n);
}
const embeddings = {
  embedDocuments: async (texts) => texts.map(wordHash),
  embedQuery: async (text) => wordHash(text),
};

test("LangChain.js store against a live node", { skip: !url && "CAIRN_URL not set" }, async () => {
  const admin = new Cairn({ url, apiKey: process.env.CAIRN_ADMIN_KEY });
  const name = `lcjs-${process.pid}`;
  await admin.createCollection(name, collectionSchema(DIMS, { filterable: { source: "Enum", page: "I64" } }), { shards: 2 });
  const client = new Cairn({ url, apiKey: process.env.CAIRN_KEY }).collection(name);
  const store = new CairnVectorStore(embeddings, { client });
  try {
    const chunks = [
      new Document({ pageContent: "the otter swims in the river", metadata: { source: "a", parent: "doc-a", page: 1, extra: { k: [1, 2] } } }),
      new Document({ pageContent: "an otter eats fish near the river bank", metadata: { source: "a", parent: "doc-a", page: 2 } }),
      new Document({ pageContent: "the stock market fell sharply today", metadata: { source: "b", parent: "doc-b", page: 1 } }),
      new Document({ pageContent: "markets and stocks after the election", metadata: { source: "b", parent: "doc-b", page: 2 } }),
    ];
    assert.deepEqual(await store.addDocuments(chunks, { ids: ["a1", "a2", "b1", "b2"] }), ["a1", "a2", "b1", "b2"]);

    const top = await store.similaritySearch("otter river", 2);
    assert.deepEqual(new Set(top.map((d) => d.id)), new Set(["a1", "a2"]));
    const [got] = await store.getByIds(["a1"]);
    assert.deepEqual(got.metadata, chunks[0].metadata);
    assert.deepEqual(new Set((await store.similaritySearch("otter river", 4, { source: "b" })).map((d) => d.id)), new Set(["b1", "b2"]));
    assert.deepEqual(new Set((await store.similaritySearch("otter", 4, { field: "page", gte: 2 })).map((d) => d.id)), new Set(["a2", "b2"]));
    const scored = await store.similaritySearchWithScore("otter river", 4);
    assert.ok(scored[0][1] >= scored[3][1]);
    const grouped = await store.similaritySearchGrouped("otter river market", 4);
    assert.deepEqual(grouped.map((d) => d.metadata.parent).sort(), ["doc-a", "doc-b"]);
    assert.equal((await store.asRetriever({ k: 1 }).invoke("stock market"))[0].id, "b1");
    assert.equal((await store.hybridSearch("election", 1))[0].id, "b2");

    await store.delete({ parent: "doc-a" });
    assert.deepEqual(new Set((await store.similaritySearch("otter river", 4)).map((d) => d.id)), new Set(["b1", "b2"]));
    assert.deepEqual(await store.getByIds(["a1", "a2"]), []);
    await store.delete({ ids: ["b1"] });
    assert.deepEqual((await store.similaritySearch("market", 4)).map((d) => d.id), ["b2"]);
    const again = await CairnVectorStore.fromTexts(["hello otter"], [{}], embeddings, { client });
    assert.equal((await again.similaritySearch("hello otter", 1))[0].pageContent, "hello otter");
  } finally {
    await admin.dropCollection(name);
  }
  await assert.rejects(store.similaritySearch("otter", 1), NotFoundError);
});
