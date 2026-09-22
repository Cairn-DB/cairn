//! Hybrid query planner and executor: filter + vector + full-text in one plan.
//!
//! [`ShardEngine`] owns a [`cairn_storage::Store`] plus the loaded indexes of every segment and
//! answers [`Query`]s: the structured predicate becomes a per-segment bitmap, each vector and
//! text *leg* produces a ranked candidate list over that bitmap (segments and memtable), legs are
//! merged and fused (ADR 0007), and the top `k` documents are returned.

pub mod engine;
pub mod fusion;
pub mod query;
pub mod replica;
pub mod wire;

pub use engine::{EngineConfig, ShardEngine};
pub use fusion::Fusion;
pub use query::{Hit, LegHit, Query, TextLeg, VectorLeg};
pub use replica::{Consistency, Replica, ReplicaConfig, ReplicaHandle, ReplicaStatus, Token};
