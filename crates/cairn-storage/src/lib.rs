//! Storage engine: WAL, memtable, immutable segments, compaction, checksums.
//!
//! Everything here is generic over a [`cairn_core::Runtime`] and touches the disk only through
//! [`cairn_core::Disk`]. Formats are versioned and checksummed (SPEC.md section 10, ADR 0004).

pub mod columns;
pub mod command;
pub mod deletion;
pub mod log;
pub mod manifest;
pub mod memtable;
pub mod segment;
pub mod store;

pub use cairn_core::codec;
pub use columns::DocStore;
pub use command::Command;
pub use deletion::DeletionSet;
pub use log::{Log, LogConfig, LogEntry, SyncPlan};
pub use manifest::{Manifest, ManifestStore};
pub use memtable::Memtable;
pub use segment::{MappedSegment, SectionMeta, SegmentReader, SegmentWriter};
pub use store::{
    CompactJob, FlushJob, NoIndexer, SegmentIndexer, SegmentMeta, SegmentView, ShardManifest,
    Store, StoreConfig,
};
