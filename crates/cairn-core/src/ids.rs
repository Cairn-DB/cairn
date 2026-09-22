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
