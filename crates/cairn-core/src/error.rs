//! Error type shared by all crates.

use std::fmt;

/// Result alias for Cairn operations.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Kind of I/O failure reported by a [`crate::Disk`] or [`crate::Network`] implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoErrorKind {
    /// The path does not exist.
    NotFound,
    /// The path already exists and the operation required that it did not.
    AlreadyExists,
    /// No space left on the device.
    NoSpace,
    /// A short read or a read past the end of the file.
    UnexpectedEof,
    /// The node is shutting down or the reactor was dropped.
    Shutdown,
    /// A peer is unreachable (network only).
    Unreachable,
    /// Anything else; the message says what.
    Other,
}

/// Errors produced by Cairn crates.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An I/O operation failed.
    #[error("io error ({kind:?}): {message}")]
    Io {
        /// Failure kind.
        kind: IoErrorKind,
        /// Human-readable detail.
        message: String,
    },
    /// Persistent data failed a checksum or structural validation.
    #[error("corruption: {0}")]
    Corruption(String),
    /// The on-disk format version is not supported by this build.
    #[error("unsupported format version {found} (supported: {supported})")]
    UnsupportedVersion {
        /// Version found on disk.
        found: u32,
        /// Version this build reads and writes.
        supported: u32,
    },
    /// A request violated the collection schema.
    #[error("schema error: {0}")]
    Schema(String),
    /// A request was malformed.
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    /// The operation must be sent to the shard leader.
    #[error("not the leader for shard {shard}")]
    NotLeader {
        /// The shard.
        shard: crate::ShardId,
        /// Best-known leader, if any.
        leader_hint: Option<crate::NodeId>,
    },
    /// An internal invariant was violated; this is a bug.
    #[error("internal error: {0}")]
    Internal(String),
}

impl Error {
    /// Builds an I/O error.
    pub fn io(kind: IoErrorKind, message: impl fmt::Display) -> Self {
        Error::Io {
            kind,
            message: message.to_string(),
        }
    }

    /// Builds a corruption error.
    pub fn corruption(message: impl fmt::Display) -> Self {
        Error::Corruption(message.to_string())
    }

    /// The I/O kind, if this is an I/O error.
    pub fn io_kind(&self) -> Option<IoErrorKind> {
        match self {
            Error::Io { kind, .. } => Some(*kind),
            _ => None,
        }
    }
}
