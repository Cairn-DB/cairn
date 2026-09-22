//! Commands carried by log entries.

use cairn_core::codec::{Reader, Writer};
use cairn_core::{DocId, Document, Error, Result};

/// One replicated command (a log entry payload).
#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    /// Insert or replace whole documents.
    Upsert(Vec<Document>),
    /// Remove documents by id (a takedown).
    Delete(Vec<DocId>),
}

impl Command {
    /// Encodes the command.
    pub fn encode(&self, w: &mut Writer) {
        match self {
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
