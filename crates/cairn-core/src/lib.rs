//! Shared types, errors, and the injectable Clock/Disk/Network abstractions used by every other
//! crate.
//!
//! Engine crates (`cairn-storage`, `cairn-index`, `cairn-query`, `cairn-raft`) depend on this
//! crate only and reach the outside world exclusively through the [`Runtime`] trait family
//! defined in [`runtime`]. See `docs/adr/0011-determinism-rules.md`.

pub mod codec;
pub mod doc;
pub mod error;
pub mod hash;
pub mod ids;
pub mod rng;
pub mod runtime;
pub mod schema;
pub mod time;

pub use doc::{Document, Value};
pub use error::{Error, Result};
pub use hash::{HashMap, HashSet};
pub use ids::{DocId, LogIndex, NodeId, SegmentId, ShardId, Term};
pub use rng::SeededRng;
pub use runtime::{Disk, Network, OpenMode, Runtime};
pub use schema::{FieldDef, FieldKind, Metric, Schema};
pub use time::{Duration, Instant};
