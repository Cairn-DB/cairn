//! The node: one executor thread per core, every shard replica pinned to one core, a node-level
//! dispatcher that routes incoming frames to the owning core, and a client coordinator that
//! fans requests out to shards and merges the answers (per-leg merge then fusion, ADR 0007).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

pub mod http;
pub mod node;

pub use node::{Node, NodeConfig};
