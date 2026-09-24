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
pub mod tls;

pub use cross::{CrossQueue, CrossReceiver, CrossSender, cross_oneshot};
pub use executor::{Executor, Handle, RunOutcome};
pub use pool::{PoolDisk, PoolRuntime, ThreadReactor};
pub use reactor::Reactor;
pub use tcp::{TcpNetwork, TcpNetworkConfig};

/// Maps a whole file read-only and wraps the mapping in `Bytes` (zero-copy slices; the mapping
/// lives as long as any slice). Random-access advice: vector index blocks are read one page at
/// a time, so readahead would only evict useful pages.
pub(crate) fn mapped(file: &std::fs::File) -> cairn_core::Result<bytes::Bytes> {
    use cairn_core::Error;
    use cairn_core::error::IoErrorKind;
    let len = file
        .metadata()
        .map_err(|e| Error::io(IoErrorKind::Other, e))?
        .len();
    if len == 0 {
        return Ok(bytes::Bytes::new());
    }
    // SAFETY: `Disk::map` is only called on write-once files (published segments, renamed into
    // place after a sync and never written again). A mapping of such a file cannot observe
    // concurrent writes; removal unlinks the name but keeps the pages valid while mapped.
    let map = unsafe { memmap2::Mmap::map(file) }.map_err(|e| Error::io(IoErrorKind::Other, e))?;
    #[cfg(unix)]
    let _ = map.advise(memmap2::Advice::Random);
    Ok(bytes::Bytes::from_owner(map))
}

/// Asks the kernel to read the pages under `data` (part of a file mapping) in the background.
pub(crate) fn will_need(data: &[u8]) {
    const PAGE: usize = 4096;
    if data.is_empty() {
        return;
    }
    let start = data.as_ptr() as usize & !(PAGE - 1);
    let end = data.as_ptr() as usize + data.len();
    // SAFETY: MADV_WILLNEED is advice: it never changes the mapping's contents or validity. The
    // range lies within a live mapping (`data` borrows it), rounded down to its page start,
    // which belongs to the same mapping because mappings are page-aligned.
    unsafe {
        libc::madvise(start as *mut libc::c_void, end - start, libc::MADV_WILLNEED);
    }
}
