//! Identifier newtypes shared across crates.

use serde::{Deserialize, Serialize};
use std::fmt;

macro_rules! id_type {
    ($(#[$doc:meta])* $name:ident($inner:ty)) => {
        $(#[$doc])*
        #[derive(
            Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
        )]
        pub struct $name(pub $inner);

        impl $name {
            /// The raw value.
            pub const fn get(self) -> $inner {
                self.0
            }
        }

        impl From<$inner> for $name {
            fn from(v: $inner) -> Self {
                $name(v)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({})", stringify!($name), self.0)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

id_type!(
    /// Client-chosen document identifier, stable across upserts.
    DocId(u64)
);

impl DocId {
    /// Set on internal ids of documents written with a text id (ADR 0031). Client-chosen
    /// integer ids never have it.
    pub const KEYED_BIT: u64 = 1 << 63;
    const SHARD_SHIFT: u32 = 40;
    const SHARD_MASK: u64 = (1 << 23) - 1;
    const COUNTER_MASK: u64 = (1 << 40) - 1;

    /// The internal id of the `counter`-th text id of `shard`: the shard is part of the id,
    /// so any path that routes by id finds the document's shard.
    pub fn keyed(shard: u32, counter: u64) -> Option<DocId> {
        (u64::from(shard) <= Self::SHARD_MASK && counter <= Self::COUNTER_MASK)
            .then(|| DocId(Self::KEYED_BIT | (u64::from(shard) << Self::SHARD_SHIFT) | counter))
    }

    /// Whether this id was assigned to a text id.
    pub const fn is_keyed(self) -> bool {
        self.0 & Self::KEYED_BIT != 0
    }

    /// The shard of a keyed id.
    pub const fn keyed_shard(self) -> Option<u32> {
        if self.is_keyed() {
            Some(((self.0 >> Self::SHARD_SHIFT) & Self::SHARD_MASK) as u32)
        } else {
            None
        }
    }
}
id_type!(
    /// Logical shard within a collection; one Raft group per shard.
    ShardId(u32)
);
id_type!(
    /// A node (process) in the cluster.
    NodeId(u32)
);
id_type!(
    /// An immutable segment, unique within a shard.
    SegmentId(u64)
);
id_type!(
    /// Position in a shard's replicated log. Index 0 is reserved (never a real entry).
    LogIndex(u64)
);
id_type!(
    /// Raft term.
    Term(u64)
);

impl LogIndex {
    /// The next index.
    pub const fn next(self) -> LogIndex {
        LogIndex(self.0 + 1)
    }
}
