//! Deterministic simulation harness: seeded network, disk, clock, fault injection, checkers.
//!
//! One [`Simulation`] owns virtual time, an event queue, one simulated disk per node and one
//! simulated network. Every node's tasks run on a single [`cairn_runtime::Executor`] whose
//! reactor is [`SimReactor`]; the seeded scheduler and seeded latencies make a run a pure
//! function of `(seed, scenario)`. A [`Trace`] fingerprints the run so two executions of the same
//! seed can be compared (ADR 0011).

pub mod completion;
pub mod disk;
pub mod net;
pub mod reactor;
pub mod runtime;
pub mod sim;
pub mod trace;

pub use disk::{DiskConfig, SimDisk};
pub use net::{NetConfig, SimNetwork};
pub use reactor::SimReactor;
pub use runtime::SimRuntime;
pub use sim::{SimConfig, Simulation};
pub use trace::Trace;
