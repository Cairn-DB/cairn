// A multi-customer knowledge-base search service on Cairn, with Express (docs/guide/).
//
//   CAIRN_URL=http://localhost:7200 CAIRN_KEY=... node server.mjs
//
// CAIRN_KEY needs the read, write and takedown roles, and no tenant: the service acts for each
// customer through withTenant. The collection is the one `examples/backend-fastapi/setup.py`
// creates (same schema).
import express from "express";
import { createHash } from "node:crypto";
import { Cairn, and_, eq, not_, InvalidInputError, CairnError } from "@cairn-db/client";

const COLLECTION = "articles";
const cairn = new Cairn({ url: process.env.CAIRN_URL ?? "http://localhost:7200", apiKey: process.env.CAIRN_KEY });

// Stand-in embeddings: words hashed into 384 dimensions. Replace with your model, for instance
// Transformers.js (`pipeline("feature-extraction", "Xenova/bge-small-en-v1.5")`) or an API.
export async function embed(texts) {
  return texts.map((text) => {
    const v = new Array(384).fill(0);
    for (const w of text.toLowerCase().split(/\s+/).filter(Boolean)) {
      const h = createHash("sha1").update(w).digest();
      v[h.readUInt16LE(0) % 384] += h[2] & 1 ? 1 : -1;
    }
    const n = Math.hypot(...v) || 1;
    return v.map((x) => x / n);
  });
}

function chunks(text, size = 800, overlap = 100) {
  const out = [];
  let cur = "";
  for (const para of text.split("\n\n")) {
    if (cur && cur.length + para.length > size) {
      out.push(cur);
      cur = cur.slice(-overlap);
    }
    cur = (cur + "\n\n" + para).trim();
  }
  if (cur) out.push(cur);
  return out;
}

// The customer's view. A token from the caller (a previous write's X-Cairn-Token) makes this
// request see that write, on any node, behind any load balancer.
function kb(req) {
  const token = req.get("X-Cairn-Token");
  if (token) cairn.observe(token);
  return cairn.collection(COLLECTION).withTenant(req.params.customer);
}

export const app = express();
app.use(express.json({ limit: "2mb" }));

app.put("/customers/:customer/articles/:article", async (req, res, next) => {
  try {
    const db = kb(req);
    const { title, body, product, lang = "en" } = req.body;
    const article = req.params.article;
    const rev = Date.now();
    const parts = chunks(body);
    const vectors = await embed(parts.map((p) => `${title}\n${p}`));
    await db.upsert(parts.map((text, i) => ({
      id: `${article}#${i}`, text, title, parent: article, product, lang, rev, deprecated: false, embedding: vectors[i],
    })));
    // New chunks first, then the old revision's chunks go: never a moment without the article.
    await db.delete({ filter: and_(eq("parent", article), not_(eq("rev", rev))) });
    res.set("X-Cairn-Token", db.token).json({ article, chunks: parts.length });
  } catch (e) { next(e); }
});

app.get("/customers/:customer/search", async (req, res, next) => {
  try {
    const db = kb(req);
    const { q, product, k = "5" } = req.query;
    const conditions = [eq("deprecated", false), ...(product ? [eq("product", product)] : [])];
    const [vector] = await embed([q]);
    const hits = await db.search({
      k: Number(k),
      vector: { field: "embedding", values: vector },
      text: { field: "text", query: q },
      filter: and_(...conditions),
      groupBy: "parent",
    });
    res.json(hits.map((h) => ({ article: h.group, title: h.document.title, snippet: String(h.document.text).slice(0, 300), score: h.score })));
  } catch (e) { next(e); }
});

app.delete("/customers/:customer/articles/:article", async (req, res, next) => {
  try {
    const db = kb(req);
    const r = await db.delete({ parent: req.params.article });
    res.set("X-Cairn-Token", db.token).json({ article: req.params.article, chunks_deleted: r.deleted });
  } catch (e) { next(e); }
});

// GDPR erasure: everything of the customer goes, and the answer carries signed evidence.
app.delete("/customers/:customer", async (req, res, next) => {
  try {
    const db = kb(req);
    const ids = (await db.search({ k: 10000, filter: not_(eq("parent", "")), withDocuments: false })).map((h) => h.id);
    const erased = await cairn.collection(COLLECTION).forgetTenant(req.params.customer);
    const proof = ids.length ? await db.proveDeletion(ids) : null;
    res.set("X-Cairn-Token", db.token).json({ customer: req.params.customer, chunks_erased: erased.deleted, proof });
  } catch (e) { next(e); }
});

app.use((err, _req, res, _next) => {
  if (err instanceof InvalidInputError) return res.status(400).json({ error: err.message });
  if (err instanceof CairnError) return res.status(503).json({ error: "search backend unavailable" });
  res.status(500).json({ error: "internal error" });
});

if (import.meta.url === `file://${process.argv[1]}`) {
  app.listen(Number(process.env.PORT ?? 3000), () => console.log("listening"));
}
