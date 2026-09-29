//! Commands carried by log entries.

use cairn_core::codec::{Reader, Writer};
use cairn_core::{DocId, Document, Error, NodeId, Predicate, Result, SegmentId, Value};

/// One replicated command (a log entry payload).
#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    /// No operation (a leader's term marker); advances the applied index only.
    Noop,
    /// Insert or replace whole documents.
    Upsert(Vec<Document>),
    /// Remove documents by id (a takedown).
    Delete(Vec<DocId>),
    /// Insert or replace documents identified by a text id (ADR 0031), held in their `_key`
    /// field. The shard maps each text id to an internal id when it applies the command.
    UpsertKeyed(Vec<Document>),
    /// Remove documents by text id (a takedown).
    DeleteKeys(Vec<String>),
    /// Remove the documents of `scope` that match `filter` (ADR 0031). Each replica resolves
    /// the filter when it applies the entry, against the rows of that log position, so all of
    /// them remove the same documents. Documents written afterwards are not affected.
    DeleteWhere {
        /// Candidate documents.
        scope: DeleteScope,
        /// Condition a candidate must meet to be removed.
        filter: Predicate,
    },
    /// Freeze the memtable for a flush (ADR 0016). The segment id is the entry's log index,
    /// so every replica cuts the same rows under the same id.
    FlushBegin,
    /// The leader merged `inputs` (adjacent segments, in manifest order) into the segment
    /// `id` with the given file length and body hash (ADR 0021). Every replica replaces the
    /// inputs with `id` when it applies this, fetching the file or building it locally.
    CompactCommit {
        /// Merged segment id (unique: the leader's compaction namespace).
        id: SegmentId,
        /// Input segments.
        inputs: Vec<SegmentId>,
        /// File length.
        len: u64,
        /// File body hash.
        hash: u64,
        /// Node that built the file: every other replica fetches it from there.
        from: NodeId,
    },
    /// The leader built the segment of the freeze `id`: its file length and body hash, which
    /// followers check after fetching it.
    FlushCommit {
        /// Segment id (the `FlushBegin` index).
        id: SegmentId,
        /// File length in bytes.
        len: u64,
        /// Body hash recorded in the file's table of contents.
        hash: u64,
        /// Node that built the file: every other replica (a later leader included) fetches
        /// it from there before building it itself.
        from: NodeId,
    },
}

/// The document a [`PatchOp`] changes.
#[derive(Debug, Clone, PartialEq)]
pub enum PatchTarget {
    /// By id.
    Id(DocId),
    /// By text id.
    Key(String),
}

/// One document's changes: each field is set, or cleared with `None` (ADR 0031). Patches never
/// enter the log: the shard leader resolves them into whole documents, in log order (see
/// `ReplicaHandle::patch`), so replaying the log never depends on reading state.
#[derive(Debug, Clone, PartialEq)]
pub struct PatchOp {
    /// The document.
    pub target: PatchTarget,
    /// `(field position, new value)`.
    pub set: Vec<(u32, Option<Value>)>,
}

impl PatchOp {
    /// Encodes one change.
    pub fn encode(&self, w: &mut Writer) {
        match &self.target {
            PatchTarget::Id(id) => {
                w.u8(0).u64(id.get());
            }
            PatchTarget::Key(k) => {
                w.u8(1).str(k);
            }
        }
        w.u32(self.set.len() as u32);
        for (f, v) in &self.set {
            w.u32(*f);
            match v {
                None => {
                    w.u8(0);
                }
                Some(v) => {
                    w.u8(1);
                    v.encode(w);
                }
            }
        }
    }

    /// Decodes one change.
    pub fn decode(r: &mut Reader<'_>) -> Result<PatchOp> {
        let target = match r.u8()? {
            0 => PatchTarget::Id(DocId(r.u64()?)),
            1 => PatchTarget::Key(r.str()?.to_owned()),
            t => return Err(Error::corruption(format!("unknown patch target {t}"))),
        };
        let n = r.u32()? as usize;
        if n > 1 << 16 {
            return Err(Error::corruption("patch too large"));
        }
        let mut set = Vec::with_capacity(n);
        for _ in 0..n {
            let f = r.u32()?;
            let v = match r.u8()? {
                0 => None,
                _ => Some(Value::decode(r)?),
            };
            set.push((f, v));
        }
        Ok(PatchOp { target, set })
    }
}

/// The candidates of a [`Command::DeleteWhere`].
#[derive(Debug, Clone, PartialEq)]
pub enum DeleteScope {
    /// Every live document of the shard.
    All,
    /// These ids only.
    Ids(Vec<DocId>),
    /// These text ids only.
    Keys(Vec<String>),
}

impl DeleteScope {
    /// Encodes the scope.
    pub fn encode(&self, w: &mut Writer) {
        match self {
            DeleteScope::All => {
                w.u8(0);
            }
            DeleteScope::Ids(ids) => {
                w.u8(1).u32(ids.len() as u32);
                for id in ids {
                    w.u64(id.get());
                }
            }
            DeleteScope::Keys(keys) => {
                w.u8(2).u32(keys.len() as u32);
                for k in keys {
                    w.str(k);
                }
            }
        }
    }

    /// Decodes a scope.
    pub fn decode(r: &mut Reader<'_>) -> Result<DeleteScope> {
        match r.u8()? {
            0 => Ok(DeleteScope::All),
            t @ (1 | 2) => {
                let n = r.u32()? as usize;
                if n > 1 << 24 {
                    return Err(Error::corruption("delete batch too large"));
                }
                if t == 1 {
                    let mut ids = Vec::with_capacity(n.min(1 << 16));
                    for _ in 0..n {
                        ids.push(DocId(r.u64()?));
                    }
                    Ok(DeleteScope::Ids(ids))
                } else {
                    let mut keys = Vec::with_capacity(n.min(1 << 16));
                    for _ in 0..n {
                        keys.push(r.str()?.to_owned());
                    }
                    Ok(DeleteScope::Keys(keys))
                }
            }
            t => Err(Error::corruption(format!("unknown delete scope {t}"))),
        }
    }
}

impl Command {
    /// Encodes the command.
    pub fn encode(&self, w: &mut Writer) {
        match self {
            Command::Noop => {}
            Command::Upsert(docs) => {
                w.u8(1).u32(docs.len() as u32);
                for d in docs {
                    d.encode(w);
                }
            }
            Command::Delete(ids) => {
                w.u8(2).u32(ids.len() as u32);
                for id in ids {
                    w.u64(id.get());
                }
            }
            Command::FlushBegin => {
                w.u8(3);
            }
            Command::UpsertKeyed(docs) => {
                w.u8(6).u32(docs.len() as u32);
                for d in docs {
                    d.encode(w);
                }
            }
            Command::DeleteKeys(keys) => {
                w.u8(7).u32(keys.len() as u32);
                for k in keys {
                    w.str(k);
                }
            }
            Command::DeleteWhere { scope, filter } => {
                w.u8(8);
                scope.encode(w);
                filter.encode(w);
            }
            Command::FlushCommit {
                id,
                len,
                hash,
                from,
            } => {
                w.u8(4).u64(id.get()).u64(*len).u64(*hash).u32(from.get());
            }
            Command::CompactCommit {
                id,
                inputs,
                len,
                hash,
                from,
            } => {
                w.u8(5).u64(id.get()).u32(inputs.len() as u32);
                for i in inputs {
                    w.u64(i.get());
                }
                w.u64(*len).u64(*hash).u32(from.get());
            }
        }
    }

    /// Encodes into a fresh buffer.
    pub fn to_bytes(&self) -> bytes::Bytes {
        let mut w = Writer::new();
        self.encode(&mut w);
        w.into_bytes()
    }

    /// Decodes a command.
    pub fn decode(r: &mut Reader<'_>) -> Result<Command> {
        if r.remaining() == 0 {
            return Ok(Command::Noop);
        }
        match r.u8()? {
            1 => {
                let n = r.u32()? as usize;
                if n > 1 << 20 {
                    return Err(Error::corruption("upsert batch too large"));
                }
                let mut docs = Vec::with_capacity(n);
                for _ in 0..n {
                    docs.push(Document::decode(r)?);
                }
                Ok(Command::Upsert(docs))
            }
            2 => {
                let n = r.u32()? as usize;
                if n > 1 << 24 {
                    return Err(Error::corruption("delete batch too large"));
                }
                let mut ids = Vec::with_capacity(n);
                for _ in 0..n {
                    ids.push(DocId(r.u64()?));
                }
                Ok(Command::Delete(ids))
            }
            3 => Ok(Command::FlushBegin),
            6 => {
                let n = r.u32()? as usize;
                if n > 1 << 20 {
                    return Err(Error::corruption("upsert batch too large"));
                }
                let mut docs = Vec::with_capacity(n);
                for _ in 0..n {
                    docs.push(Document::decode(r)?);
                }
                Ok(Command::UpsertKeyed(docs))
            }
            7 => {
                let n = r.u32()? as usize;
                if n > 1 << 24 {
                    return Err(Error::corruption("delete batch too large"));
                }
                let mut keys = Vec::with_capacity(n.min(1 << 16));
                for _ in 0..n {
                    keys.push(r.str()?.to_owned());
                }
                Ok(Command::DeleteKeys(keys))
            }
            8 => {
                let scope = DeleteScope::decode(r)?;
                Ok(Command::DeleteWhere {
                    scope,
                    filter: Predicate::decode(r)?,
                })
            }
            5 => {
                let id = SegmentId(r.u64()?);
                let n = r.u32()? as usize;
                if n > 1 << 16 {
                    return Err(Error::corruption("compaction with too many inputs"));
                }
                let mut inputs = Vec::with_capacity(n);
                for _ in 0..n {
                    inputs.push(SegmentId(r.u64()?));
                }
                Ok(Command::CompactCommit {
                    id,
                    inputs,
                    len: r.u64()?,
                    hash: r.u64()?,
                    from: NodeId(r.u32()?),
                })
            }
            4 => Ok(Command::FlushCommit {
                id: SegmentId(r.u64()?),
                len: r.u64()?,
                hash: r.u64()?,
                from: NodeId(r.u32()?),
            }),
            t => Err(Error::corruption(format!("unknown command tag {t}"))),
        }
    }

    /// Decodes from a whole buffer.
    pub fn from_bytes(bytes: &[u8]) -> Result<Command> {
        let mut r = Reader::new(bytes);
        let c = Command::decode(&mut r)?;
        r.finish()?;
        Ok(c)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flush_commands_roundtrip() {
        for cmd in [
            Command::Noop,
            Command::FlushBegin,
            Command::FlushCommit {
                id: SegmentId(42),
                len: 1 << 40,
                hash: u64::MAX,
                from: NodeId(2),
            },
            Command::Delete(vec![DocId(7), DocId(9)]),
            Command::DeleteKeys(vec!["doc-1".into(), "é/ü".into(), String::new()]),
            Command::UpsertKeyed(vec![Document::new(DocId(0), 2)]),
            Command::DeleteWhere {
                scope: DeleteScope::All,
                filter: Predicate::Eq {
                    field: 1,
                    value: Value::Enum("acme".into()),
                },
            },
            Command::DeleteWhere {
                scope: DeleteScope::Ids(vec![DocId(3), DocId(u64::MAX)]),
                filter: Predicate::True,
            },
            Command::DeleteWhere {
                scope: DeleteScope::Keys(vec!["a".into(), String::new()]),
                filter: Predicate::Not(Box::new(Predicate::IsNull { field: 0 })),
            },
            Command::CompactCommit {
                id: SegmentId((1 << 62) | 3),
                inputs: vec![SegmentId(10), SegmentId(20)],
                len: 4096,
                hash: 77,
                from: NodeId(3),
            },
        ] {
            assert_eq!(Command::from_bytes(&cmd.to_bytes()).unwrap(), cmd);
        }
        // Truncated payloads are errors, not panics (an empty payload is the no-op marker).
        let b = Command::FlushCommit {
            id: SegmentId(1),
            len: 2,
            hash: 3,
            from: NodeId(2),
        }
        .to_bytes();
        for n in 1..b.len() {
            assert!(Command::from_bytes(&b[..n]).is_err());
        }
    }
}
