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
    Ok(Hit {
        doc_id,
        score,
        legs,
        document,
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
        .u32(s.segments.len() as u32);
    for x in &s.segments {
        w.u64(*x);
    }
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
    let n = r.u32()? as usize;
    let mut segments = Vec::with_capacity(n.min(1 << 16));
    for _ in 0..n {
        segments.push(r.u64()?);
    }
    Ok(ReplicaStatus {
        id,
        role,
        term,
        leader,
        commit,
        applied,
        live_docs,
        memtable_bytes,
        segments,
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
                    lists.push(LegList {
                        hits,
                        higher_is_better,
                    });
                }
                Response::Legs(lists)
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
    ShardId((cairn_core::hash::xxh3_64(&id.get().to_le_bytes()) % u64::from(shards.max(1))) as u32)
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
            segments: vec![1, 2],
        };
        let resps = vec![
            Response::Ack(vec![token]),
            Response::Doc(None),
            Response::Hits(vec![hit]),
            Response::Status(vec![status]),
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
        assert_eq!(shard_of(DocId(42), 1), ShardId(0));
    }
}
