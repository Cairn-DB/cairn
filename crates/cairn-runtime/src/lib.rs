//! Per-core executor with pluggable reactors, and the real (OS-backed) `Runtime` implementation.
//!
//! The executor is the *same code* in production and in the deterministic simulator; only the
//! [`Reactor`] differs (ADR 0002). Engine crates never depend on this crate: they see the
//! [`cairn_core::Runtime`] trait only.

pub mod blocking;
pub mod cross;
pub mod executor;
pub mod pool;
pub mod reactor;
pub mod tcp;

pub use cross::{CrossQueue, CrossReceiver, CrossSender, cross_oneshot};
pub use executor::{Executor, Handle, RunOutcome};
pub use pool::{PoolDisk, PoolRuntime, ThreadReactor};
pub use reactor::Reactor;
pub use tcp::{TcpNetwork, TcpNetworkConfig};
