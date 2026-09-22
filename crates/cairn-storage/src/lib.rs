//! Storage engine: WAL, memtable, immutable segments, compaction, checksums.
//!
//! Everything here is generic over a [`cairn_core::Runtime`] and touches the disk only through
//! [`cairn_core::Disk`]. Formats are versioned and checksummed (SPEC.md section 10, ADR 0004).

pub mod codec;
pub mod log;
pub mod manifest;

pub use log::{Log, LogConfig, LogEntry};
pub use manifest::{Manifest, ManifestStore};
