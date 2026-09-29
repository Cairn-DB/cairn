//! The injectable boundary between engine code and the world.
//!
//! Engine code is generic over a [`Runtime`], which gives it time, task spawning, a [`Disk`] and
//! a [`Network`]. `cairn-runtime` implements it over the OS; `cairn-sim` implements it over a
//! deterministic event queue. Nothing else does.
//!
//! All futures are `!Send`: a task never leaves the core that created it (thread-per-core).

use crate::{Duration, Instant, NodeId, Result};
use bytes::Bytes;
use std::future::Future;

/// How a file is opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenMode {
    /// Must exist; reads only.
    Read,
    /// Must exist; reads and writes.
    ReadWrite,
    /// Created if missing, never truncated; reads and writes.
    CreateOrOpen,
    /// Created, truncated if it exists; reads and writes.
    CreateTruncate,
}

/// Completion-style file and directory operations.
///
/// Paths are `/`-separated and relative to the node's data directory. Durability contract: data
/// written with [`Disk::write_at`] is durable only after [`Disk::sync`] returns; a
/// [`Disk::rename`] is durable when it returns. Implementations may complete operations in any
/// order, but a single file's operations issued from one task complete in issue order.
pub trait Disk {
    /// Handle to an open file; cheap to clone.
    type File: Clone;

    /// Opens `path` with `mode`.
    fn open(&self, path: &str, mode: OpenMode) -> impl Future<Output = Result<Self::File>>;

    /// Reads exactly `len` bytes at `offset`; fails with `UnexpectedEof` if the file is shorter.
    fn read_at(
        &self,
        file: &Self::File,
        offset: u64,
        len: usize,
    ) -> impl Future<Output = Result<Bytes>>;

    /// A read-only view of the whole file, for random access without copying (the real
    /// runtimes map it into memory; the simulator returns its bytes). The file must not change
    /// while a view is alive: only write-once files (published segments) may be mapped.
    fn map(&self, file: &Self::File) -> impl Future<Output = Result<Bytes>>;

    /// A function that hints the OS to start reading a range of a view returned by
    /// [`Disk::map`] (asynchronous readahead), so that several random reads proceed in
    /// parallel instead of one page fault at a time. `None` when views are plain memory.
    fn prefetcher(&self) -> Option<fn(&[u8])> {
        None
    }

    /// Writes all of `data` at `offset`, extending the file if needed.
    fn write_at(
        &self,
        file: &Self::File,
        offset: u64,
        data: Bytes,
    ) -> impl Future<Output = Result<()>>;

    /// Makes every completed write to `file` durable.
    fn sync(&self, file: &Self::File) -> impl Future<Output = Result<()>>;

    /// Current length of the file in bytes.
    fn len(&self, file: &Self::File) -> impl Future<Output = Result<u64>>;

    /// Truncates or extends the file to `len` bytes (extension is zero-filled).
    fn set_len(&self, file: &Self::File, len: u64) -> impl Future<Output = Result<()>>;

    /// Atomically replaces `to` with `from`, durably (the directory is synced).
    fn rename(&self, from: &str, to: &str) -> impl Future<Output = Result<()>>;

    /// As [`Disk::rename`], without syncing the directory: the rename is durable only after a
    /// later [`Disk::sync_dir`] of `to`'s directory (or a durable rename there).
    fn rename_nosync(&self, from: &str, to: &str) -> impl Future<Output = Result<()>> {
        self.rename(from, to)
    }

    /// Makes earlier renames into directory `dir` durable.
    fn sync_dir(&self, dir: &str) -> impl Future<Output = Result<()>>;

    /// Removes a file.
    fn remove(&self, path: &str) -> impl Future<Output = Result<()>>;

    /// Creates a directory and its parents; succeeds if it exists.
    fn create_dir_all(&self, path: &str) -> impl Future<Output = Result<()>>;

    /// Names of the entries directly under `dir`, sorted.
    fn list(&self, dir: &str) -> impl Future<Output = Result<Vec<String>>>;

    /// Whether `path` exists (file or directory).
    fn exists(&self, path: &str) -> impl Future<Output = Result<bool>>;
}

/// Message-oriented, best-effort delivery between nodes.
pub trait Network {
    /// Sends one message; delivery is not guaranteed and may be reordered.
    fn send(&self, to: NodeId, message: Bytes) -> impl Future<Output = Result<()>>;

    /// Waits for the next message addressed to this node.
    fn recv(&self) -> impl Future<Output = Result<(NodeId, Bytes)>>;

    /// This node's id.
    fn local_id(&self) -> NodeId;

    /// Diagnostics: bytes queued for sending to peers, and messages received but not yet
    /// consumed. Zero where the transport has no queues.
    fn queue_stats(&self) -> (u64, u64) {
        (0, 0)
    }

    /// Lowest segment format version any peer reads, once every peer has announced it
    /// (ADR 0018); `None` before, or where the transport does not negotiate (the simulator).
    fn peer_segment_version(&self) -> Option<u32> {
        None
    }
}

/// Everything engine code may ask of its environment.
pub trait Runtime: Clone + 'static {
    /// Disk implementation.
    type Disk: Disk;
    /// Network implementation.
    type Network: Network;

    /// Current monotonic time.
    fn now(&self) -> Instant;

    /// Wall-clock time, in milliseconds since the Unix epoch. Engine code never decides with it:
    /// a node reads it to put a time into a command (retention, ADR 0031), and every replica
    /// applies that command with the time it carries. The simulator starts at a fixed date.
    fn unix_millis(&self) -> i64;

    /// Completes when `now() >= deadline`.
    fn sleep_until(&self, deadline: Instant) -> impl Future<Output = ()>;

    /// Completes after `d` has elapsed.
    fn sleep(&self, d: Duration) -> impl Future<Output = ()> {
        let deadline = self.now() + d;
        async move { self.sleep_until(deadline).await }
    }

    /// Runs `future` as a new task on this core.
    fn spawn(&self, future: impl Future<Output = ()> + 'static);

    /// Lets other tasks on this core run before continuing.
    fn yield_now(&self) -> impl Future<Output = ()>;

    /// Runs CPU-heavy, self-contained work (a segment build) off the core when the runtime can
    /// (a helper thread in production) or inline (the simulator, which must stay deterministic).
    fn offload<T: Send + 'static>(
        &self,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> impl Future<Output = T>;

    /// Runs short CPU work (a query's search, ADR 0025) off the core. Unlike [`Runtime::offload`],
    /// whose builds may take minutes, this work never queues behind builds: production runs it
    /// on a bounded pool of permanent threads, so concurrent searches are bounded too and pay
    /// no thread start. By default it is `offload`.
    fn offload_search<T: Send + 'static>(
        &self,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> impl Future<Output = T> {
        self.offload(work)
    }

    /// The disk.
    fn disk(&self) -> &Self::Disk;

    /// The network.
    fn network(&self) -> &Self::Network;
}
