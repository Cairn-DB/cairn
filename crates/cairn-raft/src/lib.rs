//! Raft integration: one group per logical shard, snapshot/segment shipping.
//!
//! [`Raft`] is a pure state machine (ADR 0008): it never touches time, disk or network. The
//! driver calls [`Raft::tick`], [`Raft::step`] and [`Raft::propose`], then drains a [`Ready`]:
//! persist the hard state and new entries, *then* send the messages, apply the committed entries,
//! and call [`Raft::advance`]. Membership is static in v1.

pub mod message;
pub mod raft;

pub use message::{Entry, HardState, Message, Snapshot};
pub use raft::{Config, InitialState, Raft, Ready, Role};
