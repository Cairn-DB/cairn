//! Client API messages. Transport framing lives in `cairn-runtime::tcp`; this crate only
//! encodes requests and responses with the workspace codec.

use bytes::Bytes;
use cairn_core::codec::{Reader, Writer};
use cairn_core::{DocId, Document, Error, LogIndex, NodeId, Predicate, Result, ShardId, Term};
use cairn_query::query::LegHit;
use cairn_query::{
    Consistency, Fusion, Hit, LegList, Query, ReplicaStatus, TextLeg, Token, VectorLeg,
};
use cairn_raft::Role;
pub use cairn_storage::{DeleteScope, PatchOp, PatchTarget};

/// A client request.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum Request {
    /// Insert or replace documents (routed to shards by id).
    Upsert(Vec<Document>),
    /// Delete documents by id (a takedown).
    Delete(Vec<DocId>),
    /// Point read.
    Get {
        /// Document id.
        id: DocId,
        /// Consistency level.
        consistency: Consistency,
        /// Per-shard read-your-writes tokens (the one matching the document's shard is used).
        tokens: Vec<Token>,
    },
    /// Hybrid query over every shard.
    Query {
        /// The query.
        query: Query,
        /// Consistency level. For `ReadYourWrites`, `tokens` gives the bound per shard (a shard
        /// without a token has no writes to wait for).
        consistency: Consistency,
        /// Per-shard read-your-writes tokens.
        tokens: Vec<Token>,
    },
    /// Node status.
    Status,
    /// Internal: the legs of a query on one shard (coordinator to shard leader).
    ShardLegs {
        /// Shard.
        shard: ShardId,
        /// Query.
        query: Query,
        /// Consistency for that shard.
        consistency: Consistency,
    },
    /// Internal: a request forwarded by another node; the receiver never forwards it again.
    Forwarded(Box<Request>),
    /// Pause (`true`) or resume merges on the node that receives it (not forwarded): answered
    /// with an empty `Ack`. Merges already running or committed still complete.
    SetMergesPaused(bool),
    /// Insert or replace documents identified by text ids (ADR 0031), held in their `_key`
    /// field; routed by `shard_of_key`.
    UpsertKeyed(Vec<Document>),
    /// Delete documents by text id (a takedown).
    DeleteKeys(Vec<String>),
    /// Point read by text id.
    GetKey {
        /// Text id.
        key: String,
        /// Consistency level.
        consistency: Consistency,
        /// Per-shard read-your-writes tokens.
        tokens: Vec<Token>,
    },
    /// Delete the documents of `scope` that match `filter` (ADR 0031), on one shard or, with
    /// `shard: None`, on every shard the scope reaches. Answered with `Deleted`.
    DeleteWhere {
        /// One shard only (a forwarded part), or all of them.
        shard: Option<ShardId>,
        /// Candidates.
        scope: DeleteScope,
        /// Condition.
        filter: Predicate,
    },
    /// `req`, for the named collection (ADR 0031); a request without it is for `default`.
    In {
        /// Collection name.
        collection: String,
        /// The request.
        req: Box<Request>,
    },
    /// Create a collection: answered with its definition (JSON) in `Collections`.
    CreateCollection {
        /// Name.
        name: String,
        /// Schema, as JSON, without the reserved fields.
        schema: String,
        /// Shard count.
        shards: u32,
        /// Retention field (empty: none).
        expires_field: String,
    },
    /// Drop a collection and delete its data everywhere: answered with its definition.
    DropCollection {
        /// Name.
        name: String,
    },
    /// Live collections: answered with their definitions.
    ListCollections,
    /// Change fields of existing documents (ADR 0031), routed to their shards: answered with
    /// `Patched`. With `shard`, one shard's part (forwarded).
    Patch {
        /// One shard only (a forwarded part), or all the ops' shards.
        shard: Option<ShardId>,
        /// The changes.
        ops: Vec<PatchOp>,
    },
}

/// A response.
#[derive(Debug, Clone, PartialEq)]
pub enum Response {
    /// Write acknowledged; one token per shard touched.
    Ack(Vec<Token>),
    /// Point read result.
    Doc(Option<Document>),
    /// Query hits.
    Hits(Vec<Hit>),
    /// Status of every shard replica on the node.
    Status(Vec<ReplicaStatus>),
    /// Internal: per-leg lists of one shard.
    Legs(Vec<LegList>),
    /// Collection definitions, as JSON.
    Collections(Vec<String>),
    /// Patch acknowledged: documents changed, and one token per shard touched.
    Patched {
        /// Documents changed.
        count: u64,
        /// Tokens.
        tokens: Vec<Token>,
    },
    /// Deletion by filter acknowledged: documents removed, and one token per shard touched.
    Deleted {
        /// Documents removed.
        count: u64,
        /// Tokens.
        tokens: Vec<Token>,
    },
    /// Failure.
    Error {
        /// Message.
        message: String,
        /// Leader hint (node id) when the request must be retried elsewhere.
        leader_hint: Option<NodeId>,
    },
}

fn enc_token(w: &mut Writer, t: &Token) {
    w.u32(t.shard.get()).u64(t.index.get());
}

fn dec_token(r: &mut Reader<'_>) -> Result<Token> {
    Ok(Token {
        shard: ShardId(r.u32()?),
        index: LogIndex(r.u64()?),
    })
}

fn enc_consistency(w: &mut Writer, c: &Consistency) {
    match c {
        Consistency::Linearizable => {
            w.u8(0);
        }
        Consistency::ReadYourWrites(t) => {
            w.u8(1);
            enc_token(w, t);
        }
        Consistency::Stale => {
            w.u8(2);
        }
    }
}

fn dec_consistency(r: &mut Reader<'_>) -> Result<Consistency> {
    Ok(match r.u8()? {
        0 => Consistency::Linearizable,
        1 => Consistency::ReadYourWrites(dec_token(r)?),
        2 => Consistency::Stale,
        t => return Err(Error::corruption(format!("consistency tag {t}"))),
    })
}

/// Encodes a query.
pub fn enc_query(w: &mut Writer, q: &Query) {
    q.filter.encode(w);
    w.u32(q.vectors.len() as u32);
    for v in &q.vectors {
        w.u32(v.field as u32).u32(v.ef).u32(v.vector.len() as u32);
        for x in &v.vector {
            w.f32(*x);
        }
    }
    match &q.text {
        None => {
            w.u8(0);
        }
        Some(t) => {
            w.u8(1)
                .u32(t.field as u32)
                .str(&t.text)
                .u8(u8::from(t.all_terms));
        }
    }
    w.u32(q.k as u32);
    match &q.fusion {
        Fusion::Rrf { k } => {
            w.u8(0).f32(*k);
        }
        Fusion::Weighted { weights } => {
            w.u8(1).u32(weights.len() as u32);
            for x in weights {
                w.f32(*x);
            }
        }
    }
    w.u32(q.oversample as u32)
        .u8(u8::from(q.exact))
        .u8(u8::from(q.with_documents));
}

/// Decodes a query.
pub fn dec_query(r: &mut Reader<'_>) -> Result<Query> {
    let filter = Predicate::decode(r)?;
    let n = r.u32()? as usize;
    if n > 16 {
        return Err(Error::corruption("too many vector legs"));
    }
    let mut vectors = Vec::with_capacity(n);
    for _ in 0..n {
        let field = r.u32()? as usize;
        let ef = r.u32()?;
        let d = r.u32()? as usize;
        if d > 1 << 16 {
            return Err(Error::corruption("vector too long"));
        }
        let mut vector = Vec::with_capacity(d);
        for _ in 0..d {
            vector.push(r.f32()?);
        }
        vectors.push(VectorLeg { field, vector, ef });
    }
    let text = match r.u8()? {
        0 => None,
        _ => Some(TextLeg {
            field: r.u32()? as usize,
            text: r.str()?.to_owned(),
            all_terms: r.u8()? != 0,
        }),
    };
    let k = r.u32()? as usize;
    let fusion = match r.u8()? {
        0 => Fusion::Rrf { k: r.f32()? },
        _ => {
            let n = r.u32()? as usize;
            if n > 64 {
                return Err(Error::corruption("too many weights"));
            }
            let mut weights = Vec::with_capacity(n);
            for _ in 0..n {
                weights.push(r.f32()?);
            }
            Fusion::Weighted { weights }
        }
    };
    let oversample = r.u32()? as usize;
    let exact = r.u8()? != 0;
    let with_documents = r.u8()? != 0;
    Ok(Query {
        filter,
        vectors,
        text,
        k,
        fusion,
        oversample,
        exact,
        with_documents,
    })
}

fn enc_hit(w: &mut Writer, h: &Hit) {
    w.u64(h.doc_id.get()).f32(h.score).u32(h.legs.len() as u32);
    for l in &h.legs {
        match l {
            None => {
                w.u8(0);
            }
            Some(l) => {
                w.u8(1).u32(l.rank).f32(l.score);
            }
        }
    }
    match &h.document {
        None => {
            w.u8(0);
        }
        Some(d) => {
            w.u8(1);
            d.encode(w);
        }
    }
    match &h.key {
        None => {
            w.u8(0);
        }
        Some(k) => {
            w.u8(1).str(k);
        }
    }
}

fn dec_hit(r: &mut Reader<'_>) -> Result<Hit> {
    let doc_id = DocId(r.u64()?);
    let score = r.f32()?;
    let n = r.u32()? as usize;
    if n > 64 {
        return Err(Error::corruption("too many legs"));
    }
    let mut legs = Vec::with_capacity(n);
    for _ in 0..n {
        legs.push(match r.u8()? {
            0 => None,
            _ => Some(LegHit {
                rank: r.u32()?,
                score: r.f32()?,
            }),
        });
    }
    let document = match r.u8()? {
        0 => None,
        _ => Some(Document::decode(r)?),
    };
    let key = match r.u8()? {
        0 => None,
        _ => Some(r.str()?.to_owned()),
    };
    Ok(Hit {
        doc_id,
        score,
        legs,
        document,
        key,
    })
}

fn enc_status(w: &mut Writer, s: &ReplicaStatus) {
    let role = match s.role {
        Role::Follower => 0,
        Role::PreCandidate => 1,
        Role::Candidate => 2,
        Role::Leader => 3,
    };
    w.u32(s.id.get())
        .u8(role)
        .u64(s.term.get())
        .u32(s.leader.map_or(u32::MAX, |l| l.get()))
        .u64(s.commit.get())
        .u64(s.applied.get())
        .u64(s.live_docs)
        .u64(s.memtable_bytes)
        .u64(s.raft_log_bytes)
        .u32(s.queued[0])
        .u32(s.queued[1])
        .u32(s.queued[2])
        .u64(s.net.0)
        .u64(s.net.1)
        .u32(s.segments.len() as u32);
    for x in &s.segments {
        w.u64(*x);
    }
    for x in s.flushes {
        w.u64(x);
    }
    w.u64(s.merges[0])
        .u64(s.merges[1])
        .u8(u8::from(s.merges_paused));
}

fn dec_status(r: &mut Reader<'_>) -> Result<ReplicaStatus> {
    let id = NodeId(r.u32()?);
    let role = match r.u8()? {
        0 => Role::Follower,
        1 => Role::PreCandidate,
        2 => Role::Candidate,
        _ => Role::Leader,
    };
    let term = Term(r.u64()?);
    let leader = match r.u32()? {
        u32::MAX => None,
        l => Some(NodeId(l)),
    };
    let commit = LogIndex(r.u64()?);
    let applied = LogIndex(r.u64()?);
    let live_docs = r.u64()?;
    let memtable_bytes = r.u64()?;
    let raft_log_bytes = r.u64()?;
    let queued = [r.u32()?, r.u32()?, r.u32()?];
    let net = (r.u64()?, r.u64()?);
    let n = r.u32()? as usize;
    let mut segments = Vec::with_capacity(n.min(1 << 16));
    for _ in 0..n {
        segments.push(r.u64()?);
    }
    let flushes = [r.u64()?, r.u64()?, r.u64()?];
    let merges = [r.u64()?, r.u64()?];
    let merges_paused = r.u8()? != 0;
    Ok(ReplicaStatus {
        id,
        role,
        term,
        leader,
        commit,
        applied,
        live_docs,
        memtable_bytes,
        raft_log_bytes,
        queued,
        net,
        segments,
        flushes,
        merges,
        merges_paused,
    })
}

impl Request {
    /// Encodes.
    pub fn to_bytes(&self) -> Bytes {
        let mut w = Writer::new();
        match self {
            Request::Upsert(docs) => {
                w.u8(1).u32(docs.len() as u32);
                for d in docs {
                    d.encode(&mut w);
                }
            }
            Request::Delete(ids) => {
                w.u8(2).u32(ids.len() as u32);
                for id in ids {
                    w.u64(id.get());
                }
            }
            Request::Get {
                id,
                consistency,
                tokens,
            } => {
                w.u8(3).u64(id.get());
                enc_consistency(&mut w, consistency);
                w.u32(tokens.len() as u32);
                for t in tokens {
                    enc_token(&mut w, t);
                }
            }
            Request::Query {
                query,
                consistency,
                tokens,
            } => {
                w.u8(4);
                enc_query(&mut w, query);
                enc_consistency(&mut w, consistency);
                w.u32(tokens.len() as u32);
                for t in tokens {
                    enc_token(&mut w, t);
                }
            }
            Request::Status => {
                w.u8(5);
            }
            Request::ShardLegs {
                shard,
                query,
                consistency,
            } => {
                w.u8(6).u32(shard.get());
                enc_query(&mut w, query);
                enc_consistency(&mut w, consistency);
            }
            Request::Forwarded(inner) => {
                w.u8(7).bytes(&inner.to_bytes());
            }
            Request::SetMergesPaused(paused) => {
                w.u8(8).u8(u8::from(*paused));
            }
            Request::UpsertKeyed(docs) => {
                w.u8(9).u32(docs.len() as u32);
                for d in docs {
                    d.encode(&mut w);
                }
            }
            Request::DeleteKeys(keys) => {
                w.u8(10).u32(keys.len() as u32);
                for k in keys {
                    w.str(k);
                }
            }
            Request::DeleteWhere {
                shard,
                scope,
                filter,
            } => {
                w.u8(12).u32(shard.map_or(u32::MAX, |s| s.get()));
                scope.encode(&mut w);
                filter.encode(&mut w);
            }
            Request::In { collection, req } => {
                w.u8(13).str(collection).bytes(&req.to_bytes());
            }
            Request::CreateCollection {
                name,
                schema,
                shards,
                expires_field,
            } => {
                w.u8(14)
                    .str(name)
                    .str(schema)
                    .u32(*shards)
                    .str(expires_field);
            }
            Request::DropCollection { name } => {
                w.u8(15).str(name);
            }
            Request::ListCollections => {
                w.u8(16);
            }
            Request::Patch { shard, ops } => {
                w.u8(17)
                    .u32(shard.map_or(u32::MAX, |s| s.get()))
                    .u32(ops.len() as u32);
                for op in ops {
                    op.encode(&mut w);
                }
            }
            Request::GetKey {
                key,
                consistency,
                tokens,
            } => {
                w.u8(11).str(key);
                enc_consistency(&mut w, consistency);
                w.u32(tokens.len() as u32);
                for t in tokens {
                    enc_token(&mut w, t);
                }
            }
        }
        w.into_bytes()
    }

    /// Decodes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Request> {
        let mut r = Reader::new(bytes);
        let req = match r.u8()? {
            1 => {
                let n = r.u32()? as usize;
                if n > 1 << 20 {
                    return Err(Error::corruption("upsert too large"));
                }
                let mut docs = Vec::with_capacity(n);
                for _ in 0..n {
                    docs.push(Document::decode(&mut r)?);
                }
                Request::Upsert(docs)
            }
            2 => {
                let n = r.u32()? as usize;
                if n > 1 << 24 {
                    return Err(Error::corruption("delete too large"));
                }
                let mut ids = Vec::with_capacity(n);
                for _ in 0..n {
                    ids.push(DocId(r.u64()?));
                }
                Request::Delete(ids)
            }
            3 => {
                let id = DocId(r.u64()?);
                let consistency = dec_consistency(&mut r)?;
                let n = r.u32()? as usize;
                let mut tokens = Vec::with_capacity(n.min(1 << 12));
                for _ in 0..n {
                    tokens.push(dec_token(&mut r)?);
                }
                Request::Get {
                    id,
                    consistency,
                    tokens,
                }
            }
            4 => {
                let query = dec_query(&mut r)?;
                let consistency = dec_consistency(&mut r)?;
                let n = r.u32()? as usize;
                let mut tokens = Vec::with_capacity(n.min(1 << 12));
                for _ in 0..n {
                    tokens.push(dec_token(&mut r)?);
                }
                Request::Query {
                    query,
                    consistency,
                    tokens,
                }
            }
            5 => Request::Status,
            6 => Request::ShardLegs {
                shard: ShardId(r.u32()?),
                query: dec_query(&mut r)?,
                consistency: dec_consistency(&mut r)?,
            },
            7 => Request::Forwarded(Box::new(Request::from_bytes(r.bytes()?)?)),
            8 => Request::SetMergesPaused(r.u8()? != 0),
            9 => {
                let n = r.u32()? as usize;
                if n > 1 << 20 {
                    return Err(Error::corruption("upsert too large"));
                }
                let mut docs = Vec::with_capacity(n);
                for _ in 0..n {
                    docs.push(Document::decode(&mut r)?);
                }
                Request::UpsertKeyed(docs)
            }
            10 => {
                let n = r.u32()? as usize;
                if n > 1 << 24 {
                    return Err(Error::corruption("delete too large"));
                }
                let mut keys = Vec::with_capacity(n.min(1 << 16));
                for _ in 0..n {
                    keys.push(r.str()?.to_owned());
                }
                Request::DeleteKeys(keys)
            }
            11 => {
                let key = r.str()?.to_owned();
                let consistency = dec_consistency(&mut r)?;
                let n = r.u32()? as usize;
                let mut tokens = Vec::with_capacity(n.min(1 << 12));
                for _ in 0..n {
                    tokens.push(dec_token(&mut r)?);
                }
                Request::GetKey {
                    key,
                    consistency,
                    tokens,
                }
            }
            13 => {
                let collection = r.str()?.to_owned();
                let inner = Request::from_bytes(r.bytes()?)?;
                if matches!(inner, Request::In { .. } | Request::Forwarded(_)) {
                    return Err(Error::corruption("nested collection request"));
                }
                Request::In {
                    collection,
                    req: Box::new(inner),
                }
            }
            14 => Request::CreateCollection {
                name: r.str()?.to_owned(),
                schema: r.str()?.to_owned(),
                shards: r.u32()?,
                expires_field: r.str()?.to_owned(),
            },
            15 => Request::DropCollection {
                name: r.str()?.to_owned(),
            },
            16 => Request::ListCollections,
            17 => {
                let shard = match r.u32()? {
                    u32::MAX => None,
                    s => Some(ShardId(s)),
                };
                let n = r.u32()? as usize;
                if n > 1 << 20 {
                    return Err(Error::corruption("patch batch too large"));
                }
                let mut ops = Vec::with_capacity(n.min(1 << 16));
                for _ in 0..n {
                    ops.push(PatchOp::decode(&mut r)?);
                }
                Request::Patch { shard, ops }
            }
            12 => Request::DeleteWhere {
                shard: match r.u32()? {
                    u32::MAX => None,
                    s => Some(ShardId(s)),
                },
                scope: DeleteScope::decode(&mut r)?,
                filter: Predicate::decode(&mut r)?,
            },
            t => return Err(Error::corruption(format!("request tag {t}"))),
        };
        r.finish()?;
        Ok(req)
    }
}

impl Response {
    /// Encodes.
    pub fn to_bytes(&self) -> Bytes {
        let mut w = Writer::new();
        match self {
            Response::Ack(tokens) => {
                w.u8(1).u32(tokens.len() as u32);
                for t in tokens {
                    enc_token(&mut w, t);
                }
            }
            Response::Doc(d) => {
                w.u8(2);
                match d {
                    None => {
                        w.u8(0);
                    }
                    Some(d) => {
                        w.u8(1);
                        d.encode(&mut w);
                    }
                }
            }
            Response::Hits(hits) => {
                w.u8(3).u32(hits.len() as u32);
                for h in hits {
                    enc_hit(&mut w, h);
                }
            }
            Response::Status(v) => {
                w.u8(4).u32(v.len() as u32);
                for s in v {
                    enc_status(&mut w, s);
                }
            }
            Response::Error {
                message,
                leader_hint,
            } => {
                w.u8(5)
                    .str(message)
                    .u32(leader_hint.map_or(u32::MAX, |l| l.get()));
            }
            Response::Legs(lists) => {
                w.u8(6).u32(lists.len() as u32);
                for l in lists {
                    w.u8(u8::from(l.higher_is_better)).u32(l.hits.len() as u32);
                    for (d, sc) in &l.hits {
                        w.u64(d.get()).f32(*sc);
                    }
                    w.u32(l.keys.len() as u32);
                    for (d, k) in &l.keys {
                        w.u64(d.get()).str(k);
                    }
                }
            }
            Response::Patched { count, tokens } => {
                w.u8(9).u64(*count).u32(tokens.len() as u32);
                for t in tokens {
                    enc_token(&mut w, t);
                }
            }
            Response::Collections(defs) => {
                w.u8(8).u32(defs.len() as u32);
                for d in defs {
                    w.str(d);
                }
            }
            Response::Deleted { count, tokens } => {
                w.u8(7).u64(*count).u32(tokens.len() as u32);
                for t in tokens {
                    enc_token(&mut w, t);
                }
            }
        }
        w.into_bytes()
    }

    /// Decodes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Response> {
        let mut r = Reader::new(bytes);
        let resp = match r.u8()? {
            1 => {
                let n = r.u32()? as usize;
                let mut v = Vec::with_capacity(n.min(1 << 16));
                for _ in 0..n {
                    v.push(dec_token(&mut r)?);
                }
                Response::Ack(v)
            }
            2 => Response::Doc(match r.u8()? {
                0 => None,
                _ => Some(Document::decode(&mut r)?),
            }),
            3 => {
                let n = r.u32()? as usize;
                let mut v = Vec::with_capacity(n.min(1 << 16));
                for _ in 0..n {
                    v.push(dec_hit(&mut r)?);
                }
                Response::Hits(v)
            }
            4 => {
                let n = r.u32()? as usize;
                let mut v = Vec::with_capacity(n.min(1 << 10));
                for _ in 0..n {
                    v.push(dec_status(&mut r)?);
                }
                Response::Status(v)
            }
            5 => Response::Error {
                message: r.str()?.to_owned(),
                leader_hint: match r.u32()? {
                    u32::MAX => None,
                    l => Some(NodeId(l)),
                },
            },
            6 => {
                let n = r.u32()? as usize;
                let mut lists = Vec::with_capacity(n.min(64));
                for _ in 0..n {
                    let higher_is_better = r.u8()? != 0;
                    let m = r.u32()? as usize;
                    let mut hits = Vec::with_capacity(m.min(1 << 16));
                    for _ in 0..m {
                        hits.push((DocId(r.u64()?), r.f32()?));
                    }
                    let nk = r.u32()? as usize;
                    let mut keys = Vec::with_capacity(nk.min(1 << 16));
                    for _ in 0..nk {
                        keys.push((DocId(r.u64()?), r.str()?.to_owned()));
                    }
                    lists.push(LegList {
                        hits,
                        higher_is_better,
                        keys,
                    });
                }
                Response::Legs(lists)
            }
            9 => {
                let count = r.u64()?;
                let n = r.u32()? as usize;
                let mut tokens = Vec::with_capacity(n.min(1 << 16));
                for _ in 0..n {
                    tokens.push(dec_token(&mut r)?);
                }
                Response::Patched { count, tokens }
            }
            8 => {
                let n = r.u32()? as usize;
                let mut defs = Vec::with_capacity(n.min(1 << 12));
                for _ in 0..n {
                    defs.push(r.str()?.to_owned());
                }
                Response::Collections(defs)
            }
            7 => {
                let count = r.u64()?;
                let n = r.u32()? as usize;
                let mut tokens = Vec::with_capacity(n.min(1 << 16));
                for _ in 0..n {
                    tokens.push(dec_token(&mut r)?);
                }
                Response::Deleted { count, tokens }
            }
            t => return Err(Error::corruption(format!("response tag {t}"))),
        };
        r.finish()?;
        Ok(resp)
    }

    /// Converts an engine error into an error response.
    pub fn from_error(e: &Error) -> Response {
        let leader_hint = match e {
            Error::NotLeader { leader_hint, .. } => *leader_hint,
            _ => None,
        };
        Response::Error {
            message: e.to_string(),
            leader_hint,
        }
    }
}

/// Shard of a document: a hash of its id modulo the shard count.
pub fn shard_of(id: DocId, shards: u32) -> ShardId {
    // The internal id of a text id carries its shard (ADR 0031).
    if let Some(s) = id.keyed_shard() {
        return ShardId(s % shards.max(1));
    }
    ShardId((cairn_core::hash::xxh3_64(&id.get().to_le_bytes()) % u64::from(shards.max(1))) as u32)
}

/// Shard owning a text id (ADR 0031).
pub fn shard_of_key(key: &str, shards: u32) -> ShardId {
    ShardId((cairn_core::hash::xxh3_64(key.as_bytes()) % u64::from(shards.max(1))) as u32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_core::Value;

    #[test]
    fn roundtrips() {
        let doc = Document::new(DocId(5), 2)
            .set(0, Value::Vector(vec![1.0, 2.0]))
            .set(1, Value::Text("hi".into()));
        let mut q = Query::new(7);
        q.vectors.push(VectorLeg {
            field: 0,
            vector: vec![0.5, -1.0],
            ef: 32,
        });
        q.text = Some(TextLeg {
            field: 1,
            text: "nuclear energy".into(),
            all_terms: true,
        });
        q.filter = Predicate::Range {
            field: 2,
            lo: Some(Value::I64(1)),
            hi: None,
            lo_inclusive: true,
            hi_inclusive: false,
        };
        q.fusion = Fusion::Weighted {
            weights: vec![1.0, 2.5],
        };
        q.with_documents = true;
        let token = Token {
            shard: ShardId(3),
            index: LogIndex(99),
        };
        let reqs = vec![
            Request::Upsert(vec![doc.clone()]),
            Request::Delete(vec![DocId(1), DocId(2)]),
            Request::Get {
                id: DocId(9),
                consistency: Consistency::ReadYourWrites(token),
                tokens: vec![token],
            },
            Request::Query {
                query: q.clone(),
                consistency: Consistency::Linearizable,
                tokens: vec![token],
            },
            Request::Status,
            Request::ShardLegs {
                shard: ShardId(1),
                query: q.clone(),
                consistency: Consistency::Stale,
            },
            Request::Forwarded(Box::new(Request::Delete(vec![DocId(3)]))),
            Request::SetMergesPaused(true),
            Request::SetMergesPaused(false),
            Request::UpsertKeyed(vec![doc.clone()]),
            Request::DeleteKeys(vec!["doc-1".into(), "é".into()]),
            Request::DeleteWhere {
                shard: None,
                scope: DeleteScope::All,
                filter: Predicate::IsNull { field: 2 },
            },
            Request::In {
                collection: "docs".into(),
                req: Box::new(Request::DeleteKeys(vec!["a".into()])),
            },
            Request::CreateCollection {
                name: "docs".into(),
                schema: "{}".into(),
                shards: 4,
                expires_field: "expires".into(),
            },
            Request::DropCollection {
                name: "docs".into(),
            },
            Request::ListCollections,
            Request::Patch {
                shard: Some(ShardId(2)),
                ops: vec![
                    PatchOp {
                        target: PatchTarget::Id(DocId(4)),
                        set: vec![(1, Some(cairn_core::Value::I64(-3))), (2, None)],
                    },
                    PatchOp {
                        target: PatchTarget::Key("é".into()),
                        set: vec![],
                    },
                ],
            },
            Request::DeleteWhere {
                shard: Some(ShardId(3)),
                scope: DeleteScope::Keys(vec!["k".into()]),
                filter: Predicate::True,
            },
            Request::GetKey {
                key: "doc-2".into(),
                consistency: Consistency::Linearizable,
                tokens: vec![token],
            },
        ];
        for r in reqs {
            assert_eq!(Request::from_bytes(&r.to_bytes()).unwrap(), r);
        }
        let hit = Hit {
            doc_id: DocId(5),
            score: 0.3,
            legs: vec![
                Some(LegHit {
                    rank: 2,
                    score: 1.5,
                }),
                None,
            ],
            document: Some(doc),
            key: Some("doc-5".into()),
        };
        let status = ReplicaStatus {
            id: NodeId(2),
            role: Role::Leader,
            term: Term(4),
            leader: Some(NodeId(2)),
            commit: LogIndex(10),
            applied: LogIndex(9),
            live_docs: 3,
            memtable_bytes: 4096,
            raft_log_bytes: 8192,
            queued: [1, 2, 3],
            net: (100, 4),
            segments: vec![1, 2],
            flushes: [5, 6, 1],
            merges: [1, 2],
            merges_paused: true,
        };
        let resps = vec![
            Response::Ack(vec![token]),
            Response::Deleted {
                count: 7,
                tokens: vec![token],
            },
            Response::Collections(vec!["{}".into(), "{\"name\":\"x\"}".into()]),
            Response::Patched {
                count: 3,
                tokens: vec![token],
            },
            Response::Doc(None),
            Response::Hits(vec![hit]),
            Response::Status(vec![status]),
            Response::Legs(vec![LegList {
                hits: vec![(DocId(1), 0.5), (DocId::keyed(2, 3).unwrap(), 0.25)],
                higher_is_better: true,
                keys: vec![(DocId::keyed(2, 3).unwrap(), "doc-3".into())],
            }]),
            Response::Error {
                message: "nope".into(),
                leader_hint: Some(NodeId(1)),
            },
        ];
        for r in resps {
            assert_eq!(Response::from_bytes(&r.to_bytes()).unwrap(), r);
        }
        assert!(Request::from_bytes(&[9]).is_err());
        assert!((0..1000).all(|i| shard_of(DocId(i), 8).get() < 8));
        // A text id's internal id routes to the shard that assigned it (ADR 0031).
        for shard in 0..8 {
            assert_eq!(
                shard_of(DocId::keyed(shard, 42).unwrap(), 8),
                ShardId(shard)
            );
        }
        assert!((0..1000).all(|i| shard_of_key(&format!("k{i}"), 8).get() < 8));
        assert_eq!(shard_of(DocId(42), 1), ShardId(0));
    }
}
