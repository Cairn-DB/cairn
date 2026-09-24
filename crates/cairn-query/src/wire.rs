//! Node-to-node frames for one shard: Raft messages and segment-file shipping.

use bytes::Bytes;
use cairn_core::codec::{Reader, Writer};
use cairn_core::{Error, Result, ShardId};
use cairn_raft::Message;

/// Body of a frame.
#[derive(Debug, Clone, PartialEq)]
pub enum FrameBody {
    /// A Raft message.
    Raft(Message),
    /// Request a shard file (relative path) from `offset`.
    FetchFile {
        /// Request id (echoed).
        req: u64,
        /// Path relative to the shard directory.
        path: String,
        /// Byte offset to start at.
        offset: u64,
    },
    /// A chunk of a shard file.
    FileChunk {
        /// Request id.
        req: u64,
        /// Path.
        path: String,
        /// Offset of this chunk.
        offset: u64,
        /// Total file length (`u64::MAX` if the file does not exist).
        total: u64,
        /// The server's applied index when it read the chunk: the file cannot reflect entries
        /// beyond it (a follower installing a snapshot waits for that index before serving).
        applied: u64,
        /// Bytes.
        data: Bytes,
    },
}

/// A frame addressed to a shard replica.
#[derive(Debug, Clone, PartialEq)]
pub struct Frame {
    /// Target shard.
    pub shard: ShardId,
    /// Body.
    pub body: FrameBody,
}

impl Frame {
    /// Encodes the frame.
    pub fn to_bytes(&self) -> Bytes {
        let mut w = Writer::new();
        w.u32(self.shard.get());
        match &self.body {
            FrameBody::Raft(m) => {
                w.u8(1);
                m.encode(&mut w);
            }
            FrameBody::FetchFile { req, path, offset } => {
                w.u8(2).u64(*req).str(path).u64(*offset);
            }
            FrameBody::FileChunk {
                req,
                path,
                offset,
                total,
                applied,
                data,
            } => {
                w.u8(3)
                    .u64(*req)
                    .str(path)
                    .u64(*offset)
                    .u64(*total)
                    .u64(*applied)
                    .bytes(data);
            }
        }
        w.into_bytes()
    }

    /// Decodes a frame.
    pub fn from_bytes(bytes: &[u8]) -> Result<Frame> {
        let mut r = Reader::new(bytes);
        let shard = ShardId(r.u32()?);
        let body = match r.u8()? {
            1 => FrameBody::Raft(Message::decode(&mut r)?),
            2 => FrameBody::FetchFile {
                req: r.u64()?,
                path: r.str()?.to_owned(),
                offset: r.u64()?,
            },
            3 => FrameBody::FileChunk {
                req: r.u64()?,
                path: r.str()?.to_owned(),
                offset: r.u64()?,
                total: r.u64()?,
                applied: r.u64()?,
                data: Bytes::copy_from_slice(r.bytes()?),
            },
            t => return Err(Error::corruption(format!("unknown frame tag {t}"))),
        };
        r.finish()?;
        Ok(Frame { shard, body })
    }
}
