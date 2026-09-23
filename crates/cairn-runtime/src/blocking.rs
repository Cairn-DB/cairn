//! The blocking reactor and OS-backed `Runtime`: synchronous `std::fs` behind the `Disk` trait,
//! real time, no network. Good enough for single-node Phase 1 and for tests; the io_uring
//! reactor replaces the disk part later (ADR 0002).
//!
//! This is one of the few modules allowed to touch the OS directly (ADR 0011).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use crate::executor::Handle;
use crate::reactor::Reactor;
use bytes::Bytes;
use cairn_core::error::IoErrorKind;
use cairn_core::{Disk, Duration, Error, Instant, Network, NodeId, OpenMode, Result, Runtime};
use std::fs;
use std::future::Future;
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Component, Path, PathBuf};
use std::rc::Rc;

/// Reactor that sleeps in real time; it never has pending completions because the blocking disk
/// completes every operation inline.
pub struct BlockingReactor {
    origin: std::time::Instant,
}

impl BlockingReactor {
    /// Creates a reactor whose `Instant::ZERO` is now.
    pub fn new() -> Self {
        BlockingReactor {
            origin: std::time::Instant::now(),
        }
    }

    fn now(&self) -> Instant {
        let elapsed = self.origin.elapsed();
        Instant::from_nanos(u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX))
    }
}

impl Default for BlockingReactor {
    fn default() -> Self {
        Self::new()
    }
}

impl Reactor for BlockingReactor {
    fn park(&mut self, now: Instant, deadline: Option<Instant>) -> Instant {
        if let Some(deadline) = deadline {
            let real_now = self.now();
            if deadline > real_now {
                std::thread::sleep(deadline - real_now);
            }
        }
        self.now().max(now)
    }

    fn has_pending(&self) -> bool {
        false
    }
}

fn map_io(e: io::Error) -> Error {
    let kind = match e.kind() {
        io::ErrorKind::NotFound => IoErrorKind::NotFound,
        io::ErrorKind::AlreadyExists => IoErrorKind::AlreadyExists,
        io::ErrorKind::StorageFull => IoErrorKind::NoSpace,
        io::ErrorKind::UnexpectedEof => IoErrorKind::UnexpectedEof,
        _ => IoErrorKind::Other,
    };
    Error::io(kind, e)
}

/// `Disk` over `std::fs`, rooted at a data directory. Every path is validated to stay under the
/// root.
#[derive(Clone)]
pub struct BlockingDisk {
    root: Rc<PathBuf>,
}

impl BlockingDisk {
    /// Creates the root directory if needed and returns a disk rooted there.
    pub fn new(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root).map_err(map_io)?;
        Ok(BlockingDisk {
            root: Rc::new(root),
        })
    }

    /// The root directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn resolve(&self, path: &str) -> Result<PathBuf> {
        let rel = Path::new(path);
        for c in rel.components() {
            match c {
                Component::Normal(_) => {}
                Component::CurDir => {}
                _ => {
                    return Err(Error::io(
                        IoErrorKind::Other,
                        format!("path escapes data directory: {path:?}"),
                    ));
                }
            }
        }
        Ok(self.root.join(rel))
    }
}

impl Disk for BlockingDisk {
    type File = Rc<fs::File>;

    async fn open(&self, path: &str, mode: OpenMode) -> Result<Self::File> {
        let p = self.resolve(path)?;
        let mut o = fs::OpenOptions::new();
        match mode {
            OpenMode::Read => {
                o.read(true);
            }
            OpenMode::ReadWrite => {
                o.read(true).write(true);
            }
            OpenMode::CreateOrOpen => {
                o.read(true).write(true).create(true);
            }
            OpenMode::CreateTruncate => {
                o.read(true).write(true).create(true).truncate(true);
            }
        }
        o.open(&p).map(Rc::new).map_err(map_io)
    }

    fn prefetcher(&self) -> Option<fn(&[u8])> {
        Some(crate::will_need)
    }

    async fn map(&self, file: &Self::File) -> Result<Bytes> {
        crate::mapped(file)
    }

    async fn read_at(&self, file: &Self::File, offset: u64, len: usize) -> Result<Bytes> {
        let mut buf = vec![0u8; len];
        file.read_exact_at(&mut buf, offset).map_err(map_io)?;
        Ok(Bytes::from(buf))
    }

    async fn write_at(&self, file: &Self::File, offset: u64, data: Bytes) -> Result<()> {
        file.write_all_at(&data, offset).map_err(map_io)
    }

    async fn sync(&self, file: &Self::File) -> Result<()> {
        file.sync_data().map_err(map_io)
    }

    async fn len(&self, file: &Self::File) -> Result<u64> {
        file.metadata().map(|m| m.len()).map_err(map_io)
    }

    async fn set_len(&self, file: &Self::File, len: u64) -> Result<()> {
        file.set_len(len).map_err(map_io)
    }

    async fn rename(&self, from: &str, to: &str) -> Result<()> {
        let (f, t) = (self.resolve(from)?, self.resolve(to)?);
        fs::rename(&f, &t).map_err(map_io)?;
        // Make the rename itself durable: fsync the containing directory.
        if let Some(dir) = t.parent() {
            fs::File::open(dir)
                .and_then(|d| d.sync_all())
                .map_err(map_io)?;
        }
        Ok(())
    }

    async fn remove(&self, path: &str) -> Result<()> {
        fs::remove_file(self.resolve(path)?).map_err(map_io)
    }

    async fn create_dir_all(&self, path: &str) -> Result<()> {
        fs::create_dir_all(self.resolve(path)?).map_err(map_io)
    }

    async fn list(&self, dir: &str) -> Result<Vec<String>> {
        let mut names = Vec::new();
        for entry in fs::read_dir(self.resolve(dir)?).map_err(map_io)? {
            let entry = entry.map_err(map_io)?;
            names.push(entry.file_name().to_string_lossy().into_owned());
        }
        names.sort();
        Ok(names)
    }

    async fn exists(&self, path: &str) -> Result<bool> {
        Ok(self.resolve(path)?.exists())
    }
}

/// A network with no peers: sends fail, receives never complete.
#[derive(Clone)]
pub struct NoNetwork {
    id: NodeId,
}

impl NoNetwork {
    /// Creates a network for a lone node.
    pub fn new(id: NodeId) -> Self {
        NoNetwork { id }
    }
}

impl Network for NoNetwork {
    async fn send(&self, to: NodeId, _message: Bytes) -> Result<()> {
        Err(Error::io(
            IoErrorKind::Unreachable,
            format!("no network: cannot reach {to}"),
        ))
    }

    async fn recv(&self) -> Result<(NodeId, Bytes)> {
        std::future::pending().await
    }

    fn local_id(&self) -> NodeId {
        self.id
    }
}

/// The OS-backed runtime for one core.
#[derive(Clone)]
pub struct RealRuntime {
    handle: Handle,
    disk: BlockingDisk,
    network: NoNetwork,
}

impl RealRuntime {
    /// Builds a runtime over `executor`'s handle with a disk rooted at `data_dir`.
    pub fn new(handle: Handle, data_dir: impl Into<PathBuf>, node: NodeId) -> Result<Self> {
        Ok(RealRuntime {
            handle,
            disk: BlockingDisk::new(data_dir)?,
            network: NoNetwork::new(node),
        })
    }
}

impl Runtime for RealRuntime {
    type Disk = BlockingDisk;
    type Network = NoNetwork;

    fn now(&self) -> Instant {
        self.handle.now()
    }

    fn sleep_until(&self, deadline: Instant) -> impl Future<Output = ()> {
        self.handle.sleep_until(deadline)
    }

    fn spawn(&self, future: impl Future<Output = ()> + 'static) {
        self.handle.spawn(future);
    }

    fn yield_now(&self) -> impl Future<Output = ()> {
        self.handle.yield_now()
    }

    fn offload<T: Send + 'static>(
        &self,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> impl Future<Output = T> {
        // Inline: deterministic, and the only option without threads.
        let v = work();
        async move { v }
    }

    fn disk(&self) -> &Self::Disk {
        &self.disk
    }

    fn network(&self) -> &Self::Network {
        &self.network
    }
}

/// Convenience: `Duration` is re-exported for callers building sleeps.
pub type RealDuration = Duration;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::Executor;

    fn tmp_dir(name: &str) -> PathBuf {
        let d =
            std::env::temp_dir().join(format!("cairn-runtime-test-{}-{name}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        d
    }

    #[test]
    fn blocking_disk_roundtrip_rename_list() {
        let dir = tmp_dir("rt");
        let mut ex = Executor::new(BlockingReactor::new());
        let rt = RealRuntime::new(ex.handle(), &dir, NodeId(1)).unwrap();
        let rt2 = rt.clone();
        ex.block_on(async move {
            let d = rt2.disk();
            d.create_dir_all("seg").await.unwrap();
            let f = d.open("seg/a.tmp", OpenMode::CreateTruncate).await.unwrap();
            d.write_at(&f, 0, Bytes::from_static(b"hello"))
                .await
                .unwrap();
            d.write_at(&f, 5, Bytes::from_static(b" world"))
                .await
                .unwrap();
            d.sync(&f).await.unwrap();
            assert_eq!(d.len(&f).await.unwrap(), 11);
            assert_eq!(&d.read_at(&f, 6, 5).await.unwrap()[..], b"world");
            assert!(
                d.read_at(&f, 6, 6).await.unwrap_err().io_kind()
                    == Some(IoErrorKind::UnexpectedEof)
            );
            d.rename("seg/a.tmp", "seg/a").await.unwrap();
            assert_eq!(d.list("seg").await.unwrap(), vec!["a".to_string()]);
            assert!(d.exists("seg/a").await.unwrap());
            assert!(!d.exists("seg/a.tmp").await.unwrap());
            assert!(d.open("../escape", OpenMode::Read).await.is_err());
            assert!(
                d.open("missing", OpenMode::Read)
                    .await
                    .unwrap_err()
                    .io_kind()
                    == Some(IoErrorKind::NotFound)
            );
        });
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn real_time_sleep_advances_clock() {
        let mut ex = Executor::new(BlockingReactor::new());
        let h = ex.handle();
        let elapsed = ex.block_on(async move {
            let t0 = h.now();
            h.sleep(Duration::from_millis(5)).await;
            h.now() - t0
        });
        assert!(elapsed >= Duration::from_millis(5));
    }
}
