/**
 * `@cairn-db/client`: the Cairn HTTP API from TypeScript and JavaScript (ADR 0031).
 *
 * Built on `fetch`, with no runtime dependency: Node 18+, Deno, Bun and browsers.
 *
 * ```ts
 * import { Cairn, eq } from "@cairn-db/client";
 * const db = new Cairn({ url: "http://localhost:7200", apiKey: process.env.CAIRN_KEY });
 * await db.upsert([{ id: "doc-1#0", parent: "doc-1", text: "…", embedding: [...] }]);
 * const hits = await db.search({ text: { field: "text", query: "nuclear" }, filter: eq("lang", "en") });
 * await db.delete({ parent: "doc-1" });          // the document and all its chunks
 * const acme = db.withTenant("acme");           // unscoped key acting for one tenant
 * await db.forgetTenant("acme");                 // erase a tenant
 * ```
 *
 * Consistency: the client keeps the consistency token of its own writes and takedowns and
 * passes it with every read, so it reads its own writes and never reads back what it deleted,
 * through any node. `client.token` hands that token to another service.
 */

/** A document id: your own string, or an unsigned integer below 2^63. */
export type Id = string | number;

/** A document: an `id` plus fields named as in the schema. */
export interface Document {
  id: Id;
  /** The tenant of a document, shown to unscoped keys only. */
  _tenant?: string;
  [field: string]: unknown;
}

/** A filter: `and`, `or`, `not`, or a condition on one field. Build them with the helpers. */
export type Filter =
  | { and: Filter[] }
  | { or: Filter[] }
  | { not: Filter }
  | FieldCondition;

export interface FieldCondition {
  field: string;
  eq?: unknown;
  in?: unknown[];
  gt?: number;
  gte?: number;
  lt?: number;
  lte?: number;
  is_null?: boolean;
}

/** `field` equals `value` (on a Set field: contains it). */
export const eq = (field: string, value: unknown): Filter => ({ field, eq: value });
/** `field` equals one of `values` (on a Set field: contains one of them). */
export const in_ = (field: string, values: unknown[]): Filter => ({ field, in: values });
/** A numeric or date range; give any of `gt`, `gte`, `lt`, `lte`. */
export const range = (
  field: string,
  bounds: { gt?: number; gte?: number; lt?: number; lte?: number },
): Filter => ({ field, ...bounds });
/** `field` has no value. */
export const isNull = (field: string): Filter => ({ field, is_null: true });
export const and_ = (...filters: Filter[]): Filter => ({ and: filters });
export const or_ = (...filters: Filter[]): Filter => ({ or: filters });
export const not_ = (filter: Filter): Filter => ({ not: filter });

export type Consistency = "linearizable" | "read_your_writes" | "stale";

export interface VectorLeg {
  field: string;
  values: number[];
  ef?: number;
}

export interface SearchRequest {
  /** Number of hits, 1 to 10000 (default 10). */
  k?: number;
  /** One or more vector legs. */
  vector?: VectorLeg | VectorLeg[];
  /** A BM25 text leg. */
  text?: { field: string; query: string; allTerms?: boolean };
  filter?: Filter;
  /** `{ rrf: { k } }` (default) or `{ weighted: [..] }`, one weight per leg, vectors first. */
  fusion?: { rrf: { k: number } } | { weighted: number[] };
  oversample?: number;
  /** Return each hit's document (default true). */
  withDocuments?: boolean;
  consistency?: Consistency;
}

export interface Hit {
  id: Id;
  score: number;
  /** Rank and raw score in each leg, `null` where the document is absent from that leg. */
  legs: ({ rank: number; score: number } | null)[];
  document?: Document;
  _tenant?: string;
}

export interface WriteResult {
  /** Documents written, or ids given to a takedown by id. */
  count: number;
  token: string;
}

export interface DeleteResult {
  /** Documents removed (deletions by filter or parent; `undefined` for a deletion by ids). */
  deleted?: number;
  /** Ids given (deletion by ids). */
  count?: number;
  token: string;
}

/** What to delete: ids, a filter, a parent's chunks, or ids that also match a filter. */
export type DeleteRequest =
  | { ids: Id[]; filter?: Filter }
  | { filter: Filter }
  | { parent: Id };

export interface CairnOptions {
  /** One node's address, or several: requests rotate among them on failure. */
  url: string | string[];
  apiKey?: string;
  /** Act for this tenant (`Cairn-Tenant` header; for unscoped keys). */
  tenant?: string;
  /** Field that holds a chunk's parent id, for `delete({ parent })` (default `"parent"`). */
  parentField?: string;
  /** Per-request timeout in milliseconds (default 30000). */
  timeoutMs?: number;
  /** Retries on 502/503/504 and network errors, with backoff (default 3). */
  retries?: number;
  /** A consistency token to start from (from another service). */
  token?: string;
  /** A `fetch` implementation (default: the global one). */
  fetch?: typeof fetch;
}

/** Any error from the API or the network. */
export class CairnError extends Error {
  constructor(
    message: string,
    /** HTTP status, 0 for a network error. */
    readonly status: number,
  ) {
    super(message);
    this.name = new.target.name;
  }
}
/** 401: missing or invalid API key. */
export class AuthenticationError extends CairnError {}
/** 403: the key lacks the role, or cannot act for that tenant. */
export class ForbiddenError extends CairnError {}
/** 404. */
export class NotFoundError extends CairnError {}
/** 400: invalid input (unknown field, wrong type, bad filter…). */
export class InvalidInputError extends CairnError {}
/** 503 or a network failure after the retries. */
export class UnavailableError extends CairnError {}

function errorFor(status: number, message: string): CairnError {
  switch (status) {
    case 400:
      return new InvalidInputError(message, status);
    case 401:
      return new AuthenticationError(message, status);
    case 403:
      return new ForbiddenError(message, status);
    case 404:
      return new NotFoundError(message, status);
    case 0:
    case 502:
    case 503:
    case 504:
      return new UnavailableError(message, status);
    default:
      return new CairnError(message, status);
  }
}

/** Per-shard maximum of consistency tokens (`shard.index,...`). */
export function mergeTokens(...tokens: (string | undefined)[]): string {
  const max = new Map<number, number>();
  for (const t of tokens) {
    for (const part of (t ?? "").split(",")) {
      if (!part) continue;
      const [s, i] = part.split(".").map(Number);
      if (!Number.isInteger(s) || !Number.isInteger(i)) {
        throw new InvalidInputError(`bad consistency token ${JSON.stringify(part)}`, 400);
      }
      max.set(s, Math.max(max.get(s) ?? 0, i));
    }
  }
  return [...max.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([s, i]) => `${s}.${i}`)
    .join(",");
}

/** The path of a document: digits are an integer id, so a text id made of digits says so. */
function docPath(id: Id): string {
  if (typeof id === "number") {
    if (!Number.isSafeInteger(id) || id < 0) {
      throw new InvalidInputError(`an integer id is a non-negative safe integer: ${id}`, 400);
    }
    return `/v1/documents/${id}`;
  }
  const q = /^[0-9]+$/.test(id) ? "id_type=text" : "";
  return `/v1/documents/${encodeURIComponent(id)}${q ? `?${q}` : ""}`;
}

/** Token state shared by a client and its tenant views. */
class TokenBox {
  constructor(public value = "") {}
}

export class Cairn {
  private readonly urls: string[];
  private readonly opts: CairnOptions;
  private readonly box: TokenBox;
  private next = 0;

  constructor(opts: CairnOptions, box?: TokenBox) {
    const urls = Array.isArray(opts.url) ? opts.url : [opts.url];
    if (urls.length === 0) throw new InvalidInputError("no url", 400);
    this.urls = urls.map((u) => u.replace(/\/+$/, ""));
    this.opts = opts;
    this.box = box ?? new TokenBox(opts.token ? mergeTokens(opts.token) : "");
  }

  /** The consistency token covering every write and takedown of this client (and its views). */
  get token(): string {
    return this.box.value;
  }

  /** Adds a token received from elsewhere: later reads reflect those writes too. */
  observe(token: string): void {
    this.box.value = mergeTokens(this.box.value, token);
  }

  /** A view acting for `tenant` (unscoped keys), sharing this client's token. */
  withTenant(tenant: string): Cairn {
    return new Cairn({ ...this.opts, tenant }, this.box);
  }

  /** Inserts or replaces documents. */
  async upsert(documents: Document[]): Promise<WriteResult> {
    const r = await this.call("POST", "/v1/documents", this.after({ documents }));
    return this.written(r);
  }

  /** One document, or `null` when there is none (or it was taken down). */
  async get(id: Id, opts: { consistency?: Consistency } = {}): Promise<Document | null> {
    let path = docPath(id);
    const params = new URLSearchParams();
    if (this.box.value) params.set("after", this.box.value);
    if (opts.consistency) params.set("consistency", opts.consistency);
    const qs = params.toString();
    if (qs) path += (path.includes("?") ? "&" : "?") + qs;
    try {
      return (await this.call("GET", path)) as Document;
    } catch (e) {
      if (e instanceof NotFoundError) return null;
      throw e;
    }
  }

  /** Hybrid search: vector legs, a text leg and a filter, fused. */
  async search(req: SearchRequest): Promise<Hit[]> {
    const body: Record<string, unknown> = {};
    if (req.k !== undefined) body.k = req.k;
    if (Array.isArray(req.vector)) body.vectors = req.vector;
    else if (req.vector) body.vector = req.vector;
    if (req.text) {
      body.text = { field: req.text.field, query: req.text.query, all_terms: req.text.allTerms ?? false };
    }
    if (req.filter) body.filter = req.filter;
    if (req.fusion) body.fusion = req.fusion;
    if (req.oversample !== undefined) body.oversample = req.oversample;
    if (req.withDocuments !== undefined) body.with_documents = req.withDocuments;
    if (req.consistency) body.consistency = req.consistency;
    const r = (await this.call("POST", "/v1/search", this.after(body))) as { hits: Hit[] };
    return r.hits;
  }

  /**
   * Takes documents down: by ids, by filter (every document that matches when the deletion
   * is applied), by parent (a document's chunks), or ids that also match a filter.
   */
  async delete(req: DeleteRequest): Promise<DeleteResult> {
    let body: Record<string, unknown>;
    if ("parent" in req) {
      body = { filter: eq(this.opts.parentField ?? "parent", req.parent) };
    } else if ("ids" in req) {
      if (req.ids.length === 0) throw new InvalidInputError("no ids", 400);
      body = req.filter ? { ids: req.ids, filter: req.filter } : { ids: req.ids };
    } else {
      body = { filter: req.filter };
    }
    const r = (await this.call("POST", "/v1/documents/delete", this.after(body))) as {
      count?: number;
      deleted?: number;
      consistency_token: string;
    };
    this.observe(r.consistency_token);
    return { count: r.count, deleted: r.deleted, token: r.consistency_token };
  }

  /** Erases a tenant: every document it holds (unscoped keys with the takedown role). */
  async forgetTenant(tenant: string): Promise<DeleteResult> {
    const q = this.box.value ? `?after=${encodeURIComponent(this.box.value)}` : "";
    const r = (await this.call("DELETE", `/v1/tenants/${encodeURIComponent(tenant)}${q}`)) as {
      deleted: number;
      consistency_token: string;
    };
    this.observe(r.consistency_token);
    return { deleted: r.deleted, token: r.consistency_token };
  }

  /** The collection schema (reserved fields hidden). */
  async schema(): Promise<{ fields: { name: string; kind: unknown }[] }> {
    return (await this.call("GET", "/v1/schema")) as { fields: { name: string; kind: unknown }[] };
  }

  private after(body: Record<string, unknown>): Record<string, unknown> {
    return this.box.value ? { ...body, after: this.box.value } : body;
  }

  private written(r: unknown): WriteResult {
    const w = r as { count: number; consistency_token: string };
    this.observe(w.consistency_token);
    return { count: w.count, token: w.consistency_token };
  }

  private async call(method: string, path: string, body?: unknown): Promise<unknown> {
    const f = this.opts.fetch ?? fetch;
    const retries = this.opts.retries ?? 3;
    const headers: Record<string, string> = { "content-type": "application/json" };
    if (this.opts.apiKey) headers.authorization = `Bearer ${this.opts.apiKey}`;
    if (this.opts.tenant) headers["cairn-tenant"] = this.opts.tenant;
    let last: CairnError = new UnavailableError("no attempt", 0);
    for (let attempt = 0; attempt <= retries; attempt++) {
      if (attempt > 0) await new Promise((r) => setTimeout(r, 100 * 2 ** (attempt - 1)));
      const url = this.urls[this.next % this.urls.length] + path;
      let res: Response;
      try {
        res = await f(url, {
          method,
          headers,
          body: body === undefined ? undefined : JSON.stringify(body),
          signal: AbortSignal.timeout(this.opts.timeoutMs ?? 30_000),
        });
      } catch (e) {
        last = new UnavailableError(`${method} ${url}: ${(e as Error).message}`, 0);
        this.next++;
        continue;
      }
      const text = await res.text();
      let json: unknown = null;
      try {
        json = text ? JSON.parse(text) : null;
      } catch {
        json = null;
      }
      if (res.ok) return json;
      const message =
        (json as { error?: string } | null)?.error ?? (text || `${res.status} ${res.statusText}`);
      last = errorFor(res.status, message);
      if (!(last instanceof UnavailableError)) throw last;
      this.next++;
    }
    throw last;
  }
}
