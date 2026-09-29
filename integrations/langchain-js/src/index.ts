/**
 * `@cairn-db/langchain`: LangChain.js's vector store interface over a Cairn collection
 * (ADR 0031).
 *
 * Documents are stored with the text in `textField`, the embedding in `vectorField`, the whole
 * metadata as JSON in `metadata` (so any metadata round-trips), and the metadata keys that are
 * also fields of the schema in those fields (so they can be filtered). A chunk's `parent`
 * metadata lets `delete({ parent })` remove a document and all its chunks, everywhere.
 */
import { Cairn, and_, eq, type Filter, type Hit } from "@cairn-db/client";
import { Document, type DocumentInterface } from "@langchain/core/documents";
import type { EmbeddingsInterface } from "@langchain/core/embeddings";
import { VectorStore } from "@langchain/core/vectorstores";

const METADATA_FIELD = "metadata";

/** A schema for a store: text, embedding, `parent`, the whole metadata as JSON, and
 * `filterable` metadata fields (name -> kind: `Enum`, `I64`, `F64`, `Date`, `Bool`, `Set`,
 * `Text`). */
export function collectionSchema(
  dims: number,
  opts: { metric?: "Cosine" | "Dot" | "L2"; textField?: string; vectorField?: string; filterable?: Record<string, string> } = {},
): { fields: { name: string; kind: unknown }[] } {
  const fields: { name: string; kind: unknown }[] = [
    { name: opts.vectorField ?? "embedding", kind: { Vector: { dims, metric: opts.metric ?? "Cosine" } } },
    { name: opts.textField ?? "text", kind: "Text" },
    { name: "parent", kind: "Enum" },
    { name: METADATA_FIELD, kind: "Blob" },
  ];
  for (const [name, kind] of Object.entries(opts.filterable ?? {})) fields.push({ name, kind });
  return { fields };
}

/** A Cairn filter, or `{ field: value, ... }` meaning every field equals its value. */
export type StoreFilter = Filter | Record<string, unknown>;

function toFilter(f: StoreFilter | undefined): Filter | undefined {
  if (!f || Object.keys(f).length === 0) return undefined;
  if (["and", "or", "not", "field"].some((k) => k in f)) return f as Filter;
  const parts = Object.entries(f).map(([k, v]) => eq(k, v));
  return parts.length === 1 ? parts[0] : and_(...parts);
}

function b64encode(s: string): string {
  const bytes = new TextEncoder().encode(s);
  let bin = "";
  for (const b of bytes) bin += String.fromCharCode(b);
  return btoa(bin);
}

function b64decode(s: string): string {
  const bin = atob(s);
  return new TextDecoder().decode(Uint8Array.from(bin, (c) => c.charCodeAt(0)));
}

export interface CairnStoreArgs {
  /** A client, or a collection view of one (`client.collection("docs")`). */
  client: Cairn;
  textField?: string;
  vectorField?: string;
}

export class CairnVectorStore extends VectorStore {
  declare FilterType: StoreFilter;

  readonly client: Cairn;
  readonly textField: string;
  readonly vectorField: string;
  private schema?: Promise<Map<string, unknown>>;

  _vectorstoreType(): string {
    return "cairn";
  }

  constructor(embeddings: EmbeddingsInterface, args: CairnStoreArgs) {
    super(embeddings, args);
    this.client = args.client;
    this.textField = args.textField ?? "text";
    this.vectorField = args.vectorField ?? "embedding";
  }

  private fields(): Promise<Map<string, unknown>> {
    this.schema ??= this.client.schema().then((s) => {
      const m = new Map(s.fields.map((f) => [f.name, f.kind] as [string, unknown]));
      if (!m.has(this.vectorField) || !m.has(this.textField)) {
        throw new Error(`the collection needs a ${this.vectorField} and a ${this.textField} field`);
      }
      return m;
    });
    return this.schema;
  }

  private async metric(): Promise<string> {
    const kind = (await this.fields()).get(this.vectorField) as { Vector: { metric: string } };
    return kind.Vector.metric;
  }

  async addVectors(vectors: number[][], documents: DocumentInterface[], options?: { ids?: string[] }): Promise<string[]> {
    const fields = await this.fields();
    const ids = options?.ids ?? documents.map((d) => d.id ?? crypto.randomUUID());
    const docs = documents.map((d, i) => {
      const out: Record<string, unknown> = { id: ids[i], [this.textField]: d.pageContent, [this.vectorField]: vectors[i] };
      if (fields.has(METADATA_FIELD)) out[METADATA_FIELD] = b64encode(JSON.stringify(d.metadata ?? {}));
      for (const [k, v] of Object.entries(d.metadata ?? {})) {
        if (fields.has(k) && ![this.textField, this.vectorField, METADATA_FIELD].includes(k)) out[k] = v;
      }
      return out as { id: string };
    });
    for (let i = 0; i < docs.length; i += 256) await this.client.upsert(docs.slice(i, i + 256));
    return ids;
  }

  async addDocuments(documents: DocumentInterface[], options?: { ids?: string[] }): Promise<string[]> {
    const vectors = await this.embeddings.embedDocuments(documents.map((d) => d.pageContent));
    return this.addVectors(vectors, documents, options);
  }

  private toDocument(hit: { id: unknown; document?: Record<string, unknown> }): Document {
    const d = hit.document ?? {};
    let metadata: Record<string, unknown> = {};
    if (typeof d[METADATA_FIELD] === "string") {
      try {
        metadata = JSON.parse(b64decode(d[METADATA_FIELD] as string));
      } catch {
        metadata = {};
      }
    }
    return new Document({ id: String(hit.id), pageContent: String(d[this.textField] ?? ""), metadata });
  }

  /** Similarity for Cosine and Dot (higher is better), squared distance for L2. */
  private async score(hit: Hit): Promise<number> {
    const leg = hit.legs[0];
    if (!leg) return Number.NaN;
    return (await this.metric()) === "L2" ? leg.score : -leg.score;
  }

  async similaritySearchVectorWithScore(query: number[], k: number, filter?: StoreFilter): Promise<[Document, number][]> {
    const hits = await this.client.search({ k, vector: { field: this.vectorField, values: query }, filter: toFilter(filter) });
    return Promise.all(hits.map(async (h) => [this.toDocument(h), await this.score(h)] as [Document, number]));
  }

  /** Each document once, at its best chunk (`groupBy: "parent"`). */
  async similaritySearchGrouped(query: string, k: number, groupBy = "parent", filter?: StoreFilter): Promise<Document[]> {
    const hits = await this.client.search({
      k,
      vector: { field: this.vectorField, values: await this.embeddings.embedQuery(query) },
      filter: toFilter(filter),
      groupBy,
    });
    return hits.map((h) => this.toDocument(h));
  }

  /** Vector and BM25 legs fused (RRF): exact words and meaning together. */
  async hybridSearch(query: string, k = 4, filter?: StoreFilter): Promise<Document[]> {
    const hits = await this.client.search({
      k,
      vector: { field: this.vectorField, values: await this.embeddings.embedQuery(query) },
      text: { field: this.textField, query },
      filter: toFilter(filter),
    });
    return hits.map((h) => this.toDocument(h));
  }

  async getByIds(ids: string[]): Promise<Document[]> {
    const out: Document[] = [];
    for (const id of ids) {
      const d = await this.client.get(id);
      if (d) out.push(this.toDocument({ id, document: d }));
    }
    return out;
  }

  /** Deletes ids, a document and all its chunks (`parent`), or everything matching `filter`.
   * Final: later searches never return them, on any node. */
  async delete(params: { ids?: string[]; parent?: string; filter?: StoreFilter }): Promise<void> {
    if (params.ids?.length) await this.client.delete({ ids: params.ids });
    else if (params.parent !== undefined) await this.client.delete({ parent: params.parent });
    else if (params.filter) await this.client.delete({ filter: toFilter(params.filter) as Filter });
    else throw new Error("give ids, parent or filter");
  }

  static async fromTexts(
    texts: string[],
    metadatas: object[] | object,
    embeddings: EmbeddingsInterface,
    args: CairnStoreArgs,
  ): Promise<CairnVectorStore> {
    const docs = texts.map(
      (t, i) => new Document({ pageContent: t, metadata: (Array.isArray(metadatas) ? metadatas[i] : metadatas) as Record<string, unknown> }),
    );
    return CairnVectorStore.fromDocuments(docs, embeddings, args);
  }

  static async fromDocuments(docs: DocumentInterface[], embeddings: EmbeddingsInterface, args: CairnStoreArgs): Promise<CairnVectorStore> {
    const store = new CairnVectorStore(embeddings, args);
    await store.addDocuments(docs);
    return store;
  }
}
