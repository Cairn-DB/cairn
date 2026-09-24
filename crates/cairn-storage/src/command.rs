//! Commands carried by log entries.

use cairn_core::codec::{Reader, Writer};
use cairn_core::{DocId, Document, Error, NodeId, Result, SegmentId};

/// One replicated command (a log entry payload).
#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    /// No operation (a leader's term marker); advances the applied index only.
    Noop,
    /// Insert or replace whole documents.
    Upsert(Vec<Document>),
    /// Remove documents by id (a takedown).
    Delete(Vec<DocId>),
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
