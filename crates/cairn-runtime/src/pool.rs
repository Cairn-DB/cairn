//! Thread-pool reactor: blocking work runs on helper threads; completions come back through a
//! channel the executor parks on. Portable and simple; the io_uring reactor planned in ADR 0002
//! would replace the disk half without touching engine code.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use crate::executor::Handle;
use crate::reactor::Reactor;
use bytes::Bytes;
use cairn_core::error::IoErrorKind;
use cairn_core::{Disk, Error, Instant, NodeId, OpenMode, Result, Runtime};
use std::future::Future;
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Component, Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration as StdDuration;

/// A completion: runs on the executor thread when the reactor parks.
pub type Completion = Box<dyn FnOnce() + Send>;

/// Shared half of the reactor that helper threads use to post completions.
#[derive(Clone)]
pub struct Completer {
    tx: Sender<Completion>,
    pending: Arc<AtomicUsize>,
}

impl Completer {
    /// Registers an outstanding operation (call before spawning the work).
    pub fn begin(&self) {
        self.pending.fetch_add(1, Ordering::SeqCst);
    }

    /// Posts a completion for an operation registered with [`Completer::begin`].
    pub fn complete(&self, f: Completion) {
        let _ = self.tx.send(f);
    }

    /// Posts a wake-up that does not correspond to a registered operation (e.g. a network
    /// message arrived); it only interrupts `park`.
    pub fn notify(&self, f: Completion) {
        self.pending.fetch_add(1, Ordering::SeqCst);
        let _ = self.tx.send(f);
    }
}

/// The reactor.
pub struct ThreadReactor {
    rx: Receiver<Completion>,
    completer: Completer,
    origin: std::time::Instant,
}

impl ThreadReactor {
    /// Creates a reactor whose `Instant::ZERO` is now.
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel();
        ThreadReactor {
            rx,
            completer: Completer {
                tx,
                pending: Arc::new(AtomicUsize::new(0)),
            },
            origin: std::time::Instant::now(),
        }
    }

    /// The completer to hand to I/O providers.
    pub fn completer(&self) -> Completer {
        self.completer.clone()
    }

    fn now(&self) -> Instant {
        Instant::from_nanos(u64::try_from(self.origin.elapsed().as_nanos()).unwrap_or(u64::MAX))
    }

    fn run(&self, c: Completion) {
        self.completer.pending.fetch_sub(1, Ordering::SeqCst);
        c();
    }
}

impl Default for ThreadReactor {
    fn default() -> Self {
        Self::new()
    }
}

impl Reactor for ThreadReactor {
    fn park(&mut self, now: Instant, deadline: Option<Instant>) -> Instant {
        let real_now = self.now();
        let wait = match deadline {
            Some(d) if d > real_now => Some(d - real_now),
            Some(_) => Some(StdDuration::ZERO),
            None => None,
        };
        let first = match wait {
            Some(w) => self.rx.recv_timeout(w).ok(),
            None => self.rx.recv().ok(),
        };
        if let Some(c) = first {
            self.run(c);
            while let Ok(c) = self.rx.try_recv() {
                self.run(c);
            }
        }
        self.now().max(now)
    }

    fn has_pending(&self) -> bool {
        self.completer.pending.load(Ordering::SeqCst) > 0
    }

    fn unpark_hook(&self) -> Option<Arc<dyn Fn() + Send + Sync>> {
        let c = self.completer.clone();
        Some(Arc::new(move || c.notify(Box::new(|| {}))))
    }
}

struct SlotInner<T> {
    value: Option<T>,
    waker: Option<Waker>,
}

/// Future that resolves when a helper thread posts its result.
pub struct Slot<T>(Arc<Mutex<SlotInner<T>>>);

impl<T> Slot<T> {
    fn new() -> (Slot<T>, Arc<Mutex<SlotInner<T>>>) {
        let inner = Arc::new(Mutex::new(SlotInner {
            value: None,
            waker: None,
        }));
        (Slot(inner.clone()), inner)
    }
}

impl<T> Future for Slot<T> {
    type Output = T;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let mut g = self.0.lock().expect("slot poisoned");
        match g.value.take() {
            Some(v) => Poll::Ready(v),
            None => {
                g.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

/// Runs `work` on a helper thread and resolves with its result on the executor.
pub fn offload<T: Send + 'static>(
    completer: &Completer,
    work: impl FnOnce() -> T + Send + 'static,
) -> Slot<T> {
    let (slot, inner) = Slot::new();
    completer.begin();
    let c = completer.clone();
    std::thread::Builder::new()
        .name("offload".into())
        .spawn(move || {
            let v = work();
            let inner2 = inner.clone();
            c.complete(Box::new(move || {
                let mut g = inner2.lock().expect("slot poisoned");
                g.value = Some(v);
                if let Some(w) = g.waker.take() {
                    w.wake();
                }
            }));
        })
        .expect("spawn offload");
    slot
}

type Job = Box<dyn FnOnce() + Send>;

/// The process-wide search pool (ADR 0025): one permanent thread per hardware thread, fed by a
/// queue. Searches from every core share it, so their concurrency is bounded by the machine.
fn search_pool() -> &'static Mutex<Sender<Job>> {
    static POOL: std::sync::OnceLock<Mutex<Sender<Job>>> = std::sync::OnceLock::new();
    POOL.get_or_init(|| {
        let (tx, rx) = mpsc::channel::<Job>();
        let rx = Arc::new(Mutex::new(rx));
        let n = std::thread::available_parallelism().map_or(1, |n| n.get());
        for i in 0..n {
            let rx = rx.clone();
            std::thread::Builder::new()
                .name(format!("search-{i}"))
                .spawn(move || {
                    loop {
                        let job = rx.lock().expect("search queue").recv();
                        match job {
                            Ok(job) => job(),
                            Err(_) => return,
                        }
                    }
                })
                .expect("spawn search thread");
        }
        Mutex::new(tx)
    })
}

/// Runs `work` on the search pool and resolves with its result on the executor.
pub fn offload_search<T: Send + 'static>(
    completer: &Completer,
    work: impl FnOnce() -> T + Send + 'static,
) -> Slot<T> {
    let (slot, inner) = Slot::new();
    completer.begin();
    let c = completer.clone();
    let job: Job = Box::new(move || {
        let v = work();
        c.complete(Box::new(move || {
            let mut g = inner.lock().expect("slot poisoned");
            g.value = Some(v);
            if let Some(w) = g.waker.take() {
                w.wake();
            }
        }));
    });
    search_pool()
        .lock()
        .expect("search pool")
        .send(job)
        .expect("search pool stopped");
    slot
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

/// `Disk` whose operations run on helper threads.
#[derive(Clone)]
pub struct PoolDisk {
    root: Arc<PathBuf>,
    completer: Completer,
}

impl PoolDisk {
    /// Disk rooted at `root` (created if missing).
    pub fn new(root: impl Into<PathBuf>, completer: Completer) -> Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root).map_err(map_io)?;
        Ok(PoolDisk {
            root: Arc::new(root),
            completer,
        })
    }

    fn resolve(&self, path: &str) -> Result<PathBuf> {
        for c in Path::new(path).components() {
            if !matches!(c, Component::Normal(_) | Component::CurDir) {
                return Err(Error::io(
                    IoErrorKind::Other,
                    format!("path escapes data directory: {path:?}"),
                ));
            }
        }
        Ok(self.root.join(path))
    }
}

impl Disk for PoolDisk {
    type File = Arc<std::fs::File>;

    fn open(&self, path: &str, mode: OpenMode) -> impl Future<Output = Result<Self::File>> {
        let p = self.resolve(path);
        offload(&self.completer, move || {
            let p = p?;
            let mut o = std::fs::OpenOptions::new();
            match mode {
                OpenMode::Read => o.read(true),
                OpenMode::ReadWrite => o.read(true).write(true),
                OpenMode::CreateOrOpen => o.read(true).write(true).create(true),
                OpenMode::CreateTruncate => o.read(true).write(true).create(true).truncate(true),
            };
            o.open(&p).map(Arc::new).map_err(map_io)
        })
    }

    fn read_at(
        &self,
        file: &Self::File,
        offset: u64,
        len: usize,
    ) -> impl Future<Output = Result<Bytes>> {
        let f = file.clone();
        offload(&self.completer, move || {
            let mut buf = vec![0u8; len];
            f.read_exact_at(&mut buf, offset).map_err(map_io)?;
            Ok(Bytes::from(buf))
        })
    }

    fn prefetcher(&self) -> Option<fn(&[u8])> {
        Some(crate::will_need)
    }

    fn map(&self, file: &Self::File) -> impl Future<Output = Result<Bytes>> {
        let f = file.clone();
        offload(&self.completer, move || crate::mapped(&f))
    }

    fn write_at(
        &self,
        file: &Self::File,
        offset: u64,
        data: Bytes,
    ) -> impl Future<Output = Result<()>> {
        let f = file.clone();
        offload(&self.completer, move || {
            f.write_all_at(&data, offset).map_err(map_io)
        })
    }

    fn sync(&self, file: &Self::File) -> impl Future<Output = Result<()>> {
        let f = file.clone();
        offload(&self.completer, move || f.sync_data().map_err(map_io))
    }

    fn len(&self, file: &Self::File) -> impl Future<Output = Result<u64>> {
        let f = file.clone();
        offload(&self.completer, move || {
            f.metadata().map(|m| m.len()).map_err(map_io)
        })
    }

    fn set_len(&self, file: &Self::File, len: u64) -> impl Future<Output = Result<()>> {
        let f = file.clone();
        offload(&self.completer, move || f.set_len(len).map_err(map_io))
    }

    fn rename(&self, from: &str, to: &str) -> impl Future<Output = Result<()>> {
        let (f, t) = (self.resolve(from), self.resolve(to));
        offload(&self.completer, move || {
            let (f, t) = (f?, t?);
            std::fs::rename(&f, &t).map_err(map_io)?;
            if let Some(dir) = t.parent() {
                std::fs::File::open(dir)
                    .and_then(|d| d.sync_all())
                    .map_err(map_io)?;
            }
            Ok(())
        })
    }

    fn rename_nosync(&self, from: &str, to: &str) -> impl Future<Output = Result<()>> {
        let (f, t) = (self.resolve(from), self.resolve(to));
        offload(&self.completer, move || {
            std::fs::rename(f?, t?).map_err(map_io)
        })
    }

    fn sync_dir(&self, dir: &str) -> impl Future<Output = Result<()>> {
        let d = self.resolve(dir);
        offload(&self.completer, move || {
            std::fs::File::open(d?)
                .and_then(|d| d.sync_all())
                .map_err(map_io)
        })
    }

    fn remove(&self, path: &str) -> impl Future<Output = Result<()>> {
        let p = self.resolve(path);
        offload(&self.completer, move || {
            std::fs::remove_file(p?).map_err(map_io)
        })
    }

    fn create_dir_all(&self, path: &str) -> impl Future<Output = Result<()>> {
        let p = self.resolve(path);
        offload(&self.completer, move || {
            std::fs::create_dir_all(p?).map_err(map_io)
        })
    }

    fn list(&self, dir: &str) -> impl Future<Output = Result<Vec<String>>> {
        let p = self.resolve(dir);
        offload(&self.completer, move || {
            let mut names = Vec::new();
            for e in std::fs::read_dir(p?).map_err(map_io)? {
                names.push(
                    e.map_err(map_io)?
                        .file_name()
                        .to_string_lossy()
                        .into_owned(),
                );
            }
            names.sort();
            Ok(names)
        })
    }

    fn exists(&self, path: &str) -> impl Future<Output = Result<bool>> {
        let p = self.resolve(path);
        offload(&self.completer, move || Ok(p?.exists()))
    }
}

/// Runtime over the thread reactor: pool disk plus a network implementation.
#[derive(Clone)]
pub struct PoolRuntime<N: cairn_core::Network + Clone + 'static> {
    handle: Handle,
    disk: PoolDisk,
    network: N,
}

impl<N: cairn_core::Network + Clone + 'static> PoolRuntime<N> {
    /// Builds the runtime.
    pub fn new(handle: Handle, disk: PoolDisk, network: N) -> Self {
        PoolRuntime {
            handle,
            disk,
            network,
        }
    }

    /// Node id of the network.
    pub fn node(&self) -> NodeId {
        self.network.local_id()
    }

    /// Completer of this core's reactor (for offloading blocking work).
    pub fn completer(&self) -> Completer {
        self.disk.completer.clone()
    }
}

impl<N: cairn_core::Network + Clone + 'static> Runtime for PoolRuntime<N> {
    type Disk = PoolDisk;
    type Network = N;

    fn unix_millis(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
    }

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
        offload(&self.disk.completer, work)
    }

    fn offload_search<T: Send + 'static>(
        &self,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> impl Future<Output = T> {
        offload_search(&self.disk.completer, work)
    }

    fn disk(&self) -> &Self::Disk {
        &self.disk
    }

    fn network(&self) -> &Self::Network {
        &self.network
    }
}

#[cfg(test)]
mod search_pool_tests {
    use super::*;
    use crate::executor::Executor;

    /// Many concurrent searches complete, on the bounded pool of permanent threads.
    #[test]
    fn searches_run_on_a_bounded_pool() {
        let reactor = ThreadReactor::new();
        let completer = reactor.completer();
        let mut ex = Executor::new(reactor);
        let names = Arc::new(Mutex::new(std::collections::BTreeSet::new()));
        let slots: Vec<Slot<u64>> = (0..500u64)
            .map(|i| {
                let names = names.clone();
                offload_search(&completer, move || {
                    let name = std::thread::current().name().unwrap_or("").to_owned();
                    names.lock().unwrap().insert(name);
                    (0..1000u64).fold(i, |a, b| a.wrapping_mul(31).wrapping_add(b))
                })
            })
            .collect();
        let got: Vec<u64> = ex.block_on(async move {
            let mut out = Vec::new();
            for s in slots {
                out.push(s.await);
            }
            out
        });
        for (i, v) in got.iter().enumerate() {
            let want = (0..1000u64).fold(i as u64, |a, b| a.wrapping_mul(31).wrapping_add(b));
            assert_eq!(*v, want);
        }
        let names = names.lock().unwrap();
        let max = std::thread::available_parallelism().map_or(1, |n| n.get());
        assert!(names.iter().all(|n| n.starts_with("search-")), "{names:?}");
        assert!(
            names.len() <= max,
            "{} threads for {max} hardware threads",
            names.len()
        );
    }
}

#[cfg(test)]
mod serving_tests {
    use super::*;
    use crate::cross::CrossQueue;
    use crate::executor::Executor;

    /// A core with nothing scheduled (no timer, no I/O) still serves work pushed later from
    /// another thread. `run` returns at once in that state; before `run_serving`, a node core
    /// that hosted no replica stopped for good, and requests routed to it hung (the dogfooding
    /// test's blocker).
    #[test]
    fn an_idle_core_keeps_serving_its_queue() {
        let queue: CrossQueue<u32> = CrossQueue::new();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let q = queue.clone();
        std::thread::spawn(move || {
            let mut ex = Executor::new(ThreadReactor::new());
            let q2 = q.clone();
            ex.handle().spawn(async move {
                while let Some(v) = q2.pop().await {
                    done_tx.send(v).unwrap();
                }
            });
            ex.run_serving();
        });
        // Long enough for the core to reach its idle state before the push.
        std::thread::sleep(StdDuration::from_millis(200));
        queue.push(7);
        assert_eq!(done_rx.recv_timeout(StdDuration::from_secs(5)), Ok(7));
        queue.close();
    }
}
