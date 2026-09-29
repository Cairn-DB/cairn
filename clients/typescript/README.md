# @cairn-db/client

TypeScript and JavaScript client for [Cairn](https://github.com/cairn-db/cairn), the hybrid
search database where a deletion is final. It is built on `fetch`, with no runtime
dependency, and runs on Node 18+, Deno, Bun and browsers. It ships as ESM and CommonJS, with
types.

Not published to npm yet. Build it from this directory with `npm install && npm run build`.

```ts
import { Cairn, eq, range, and_ } from "@cairn-db/client";

const db = new Cairn({ url: "http://localhost:7200", apiKey: process.env.CAIRN_KEY });

// Your own ids; chunks carry their parent in an ordinary field ("parent" by default).
await db.upsert([
  { id: "report-9#0", parent: "report-9", text: "…", embedding: [/* … */], lang: "en" },
  { id: "report-9#1", parent: "report-9", text: "…", embedding: [/* … */], lang: "en" },
]);

const hits = await db.search({
  k: 10,
  vector: { field: "embedding", values: queryVector },
  text: { field: "text", query: "nuclear energy" },
  filter: and_(eq("lang", "en"), range("year", { gte: 2020 })),
});

await db.delete({ parent: "report-9" });            // the document and all its chunks
await db.delete({ filter: eq("source", "crawler") }); // everything that matches, now
await db.delete({ ids: ["report-7#0"] });
```

## Read-your-writes and takedowns

The client keeps the consistency token of its writes and takedowns, and sends it with every
read. It therefore reads what it wrote, and never reads back what it deleted, through any
node. To give the same guarantee to another service, pass it `db.token`:
`new Cairn({ …, token })`, or `db.observe(token)` on an existing client.

## Collections

Without `collection()`, calls act on `default`, the collection defined at startup.

```ts
const admin = new Cairn({ url, apiKey: ADMIN_KEY });
await admin.createCollection("notes", { fields: [
  { name: "embedding", kind: { Vector: { dims: 384, metric: "Cosine" } } },
  { name: "body", kind: "Text" },
] }, { shards: 4 });
const notes = db.collection("notes");   // the same calls, on "notes"
await notes.upsert([{ id: "n1", embedding: [/* … */], body: "…" }]);
await admin.dropCollection("notes");   // deletes its data on every node
```

`createCollection` and `dropCollection` need an admin key. `listCollections` needs a read key.

## Tenants

```ts
const acme = db.withTenant("acme");  // an unscoped key acting for tenant "acme"
await acme.upsert([{ id: "note-1", text: "…", embedding: [/* … */] }]);
await db.forgetTenant("acme");       // erase everything of "acme"
```

A key scoped to a tenant (`cairn-server keygen app read,write --tenant acme`) needs nothing
else: every call stays inside its tenant. Ids belong to their tenant.

## Errors and retries

- `InvalidInputError` (400), `AuthenticationError` (401), `ForbiddenError` (403),
  `UnavailableError` (503 or network), all subclasses of `CairnError` with a `status`.
- `get` returns `null` for a missing document.
- 502, 503, 504 and network errors are retried (`retries`, 3 by default) with backoff, on the
  next address when `url` is a list.

## Tests

`npm test` builds and runs the unit tests. `../test-live.sh` also runs the live tests
against a fresh local node.
