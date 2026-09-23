//! Simulated disk with explicit durability semantics.
//!
//! Data written with `write_at` or `set_len` lands in the file's *content* when the operation
//! completes (after a seeded latency) and becomes *durable* only on `sync`. A crash resets every
//! file to its durable state, then, to model the OS having written some dirty pages back on its
//! own, re-applies a random prefix of the unsynced operations and may tear the next one at an
//! arbitrary byte. Directory operations (`open` with create, `rename`, `remove`,
//! `create_dir_all`) take effect when issued and are durable immediately; this matches how the
//! real runtime fsyncs directories after renames.

use crate::completion::completion;
use crate::sim::Simulation;
use bytes::Bytes;
use cairn_core::error::IoErrorKind;
use cairn_core::{Disk, Duration, Error, HashMap, Instant, NodeId, OpenMode, Result, SeededRng};
use std::collections::{BTreeMap, BTreeSet};

/// Disk behaviour.
#[derive(Debug, Clone)]
pub struct DiskConfig {
    /// Minimum operation latency.
    pub latency_min: Duration,
    /// Maximum operation latency.
    pub latency_max: Duration,
    /// On crash, probability that each successive unsynced operation had reached the medium.
    pub persist_unsynced_prob: f64,
    /// On crash, whether the first non-persisted write may be applied partially.
    pub torn_writes: bool,
    /// Probability that a read returns data with one flipped bit.
    pub read_bitflip_prob: f64,
    /// Probability that a write or sync fails with an injected I/O error.
    pub io_error_prob: f64,
}

impl Default for DiskConfig {
    fn default() -> Self {
        DiskConfig {
            latency_min: Duration::from_micros(20),
            latency_max: Duration::from_micros(500),
            persist_unsynced_prob: 0.5,
            torn_writes: true,
            read_bitflip_prob: 0.0,
            io_error_prob: 0.0,
        }
    }
}

#[derive(Clone, Debug)]
enum Pending {
    Write { offset: u64, data: Bytes },
    SetLen(u64),
}

#[derive(Default, Debug)]
struct Inode {
    content: Vec<u8>,
    durable: Vec<u8>,
    pending: Vec<Pending>,
}

fn apply_write(content: &mut Vec<u8>, offset: u64, data: &[u8]) {
    let end = offset as usize + data.len();
    if content.len() < end {
        content.resize(end, 0);
    }
    content[offset as usize..end].copy_from_slice(data);
}

impl Inode {
    fn apply(&mut self, op: &Pending) {
        match op {
            Pending::Write { offset, data } => apply_write(&mut self.content, *offset, data),
            Pending::SetLen(len) => self.content.resize(*len as usize, 0),
        }
    }

    fn crash(&mut self, cfg: &DiskConfig, rng: &mut SeededRng) {
        self.content = self.durable.clone();
        let pending = std::mem::take(&mut self.pending);
        for op in &pending {
            if rng.chance(cfg.persist_unsynced_prob) {
                self.apply(op);
                continue;
            }
            if cfg.torn_writes
                && let Pending::Write { offset, data } = op
            {
                let keep = rng.below(data.len() as u64 + 1) as usize;
                apply_write(&mut self.content, *offset, &data[..keep]);
            }
            break;
        }
        self.durable = self.content.clone();
    }
}

/// Per-node disk state.
#[derive(Default)]
pub struct DiskState {
    inodes: HashMap<u64, Inode>,
    paths: BTreeMap<String, u64>,
    dirs: BTreeSet<String>,
    next_inode: u64,
    /// Completion time of the last operation issued on each inode (keeps per-file order).
    ready_at: HashMap<u64, Instant>,
    /// Counters: writes, syncs, reads.
    pub(crate) writes: u64,
    pub(crate) syncs: u64,
    pub(crate) reads: u64,
}

fn parent_of(path: &str) -> &str {
    match path.rfind('/') {
        Some(i) => &path[..i],
        None => "",
    }
}

fn normalize(path: &str) -> Result<String> {
    let mut parts = Vec::new();
    for c in path.split('/') {
        match c {
            "" | "." => {}
            ".." => {
                return Err(Error::io(
                    IoErrorKind::Other,
                    format!("path escapes data directory: {path:?}"),
                ));
            }
            p => parts.push(p),
        }
    }
    Ok(parts.join("/"))
}

impl DiskState {
    pub(crate) fn crash(&mut self, cfg: &DiskConfig, rng: &mut SeededRng) {
        let mut ids: Vec<u64> = self.inodes.keys().copied().collect();
        ids.sort_unstable();
        for id in ids {
            if let Some(inode) = self.inodes.get_mut(&id) {
                inode.crash(cfg, rng);
            }
        }
        self.ready_at.clear();
    }

    fn dir_exists(&self, dir: &str) -> bool {
        dir.is_empty() || self.dirs.contains(dir)
    }

    fn open(&mut self, path: &str, mode: OpenMode) -> Result<u64> {
        if !self.dir_exists(parent_of(path)) {
            return Err(Error::io(
                IoErrorKind::NotFound,
                format!("no parent directory for {path:?}"),
            ));
        }
        if self.dirs.contains(path) {
            return Err(Error::io(
                IoErrorKind::Other,
                format!("{path:?} is a directory"),
            ));
        }
        match (self.paths.get(path).copied(), mode) {
            (Some(id), OpenMode::Read | OpenMode::ReadWrite | OpenMode::CreateOrOpen) => Ok(id),
            (Some(id), OpenMode::CreateTruncate) => {
                let inode = self.inodes.get_mut(&id).expect("dangling inode");
                inode.content.clear();
                inode.durable.clear();
                inode.pending.clear();
                Ok(id)
            }
            (None, OpenMode::Read | OpenMode::ReadWrite) => Err(Error::io(
                IoErrorKind::NotFound,
                format!("{path:?} not found"),
            )),
            (None, OpenMode::CreateOrOpen | OpenMode::CreateTruncate) => {
                let id = self.next_inode;
                self.next_inode += 1;
                self.inodes.insert(id, Inode::default());
                self.paths.insert(path.to_owned(), id);
                Ok(id)
            }
        }
    }

    fn rename(&mut self, from: &str, to: &str) -> Result<()> {
        let id = self
            .paths
            .remove(from)
            .ok_or_else(|| Error::io(IoErrorKind::NotFound, format!("{from:?} not found")))?;
        if !self.dir_exists(parent_of(to)) {
            self.paths.insert(from.to_owned(), id);
            return Err(Error::io(
                IoErrorKind::NotFound,
                format!("no parent directory for {to:?}"),
            ));
        }
        if let Some(old) = self.paths.insert(to.to_owned(), id) {
            self.inodes.remove(&old);
        }
        Ok(())
    }

    fn remove(&mut self, path: &str) -> Result<()> {
        let id = self
            .paths
            .remove(path)
            .ok_or_else(|| Error::io(IoErrorKind::NotFound, format!("{path:?} not found")))?;
        self.inodes.remove(&id);
        Ok(())
    }

    fn create_dir_all(&mut self, path: &str) {
        let mut cur = String::new();
        for part in path.split('/').filter(|p| !p.is_empty()) {
            if !cur.is_empty() {
                cur.push('/');
            }
            cur.push_str(part);
            self.dirs.insert(cur.clone());
        }
    }

    /// Names of the entries directly under `dir`, sorted.
    pub fn list(&self, dir: &str) -> Result<Vec<String>> {
        if !self.dir_exists(dir) {
            return Err(Error::io(
                IoErrorKind::NotFound,
                format!("{dir:?} not found"),
            ));
        }
        let prefix = if dir.is_empty() {
            String::new()
        } else {
            format!("{dir}/")
        };
        let mut out: Vec<String> = self
            .paths
            .keys()
            .chain(self.dirs.iter())
            .filter_map(|p| p.strip_prefix(&prefix))
            .filter(|rest| !rest.is_empty() && !rest.contains('/'))
            .map(str::to_owned)
            .collect();
        out.sort();
        out.dedup();
        Ok(out)
    }

    /// Durable bytes of `path`, as a restart would see them (tests and checkers).
    pub fn durable_content(&self, path: &str) -> Option<&[u8]> {
        self.paths
            .get(path)
            .and_then(|id| self.inodes.get(id))
            .map(|i| i.durable.as_slice())
    }

    /// Replaces `path` with durable `bytes`, creating parent directories (test fixtures).
    pub fn set_durable_file(&mut self, path: &str, bytes: &[u8]) {
        let parent = parent_of(path).to_owned();
        self.create_dir_all(&parent);
        let id = match self.paths.get(path) {
            Some(id) => *id,
            None => {
                let id = self.next_inode;
                self.next_inode += 1;
                self.paths.insert(path.to_owned(), id);
                id
            }
        };
        self.inodes.insert(
            id,
            Inode {
                content: bytes.to_vec(),
                durable: bytes.to_vec(),
                pending: Vec::new(),
            },
        );
    }

    /// Current (possibly unsynced) bytes of `path`.
    pub fn content(&self, path: &str) -> Option<&[u8]> {
        self.paths
            .get(path)
            .and_then(|id| self.inodes.get(id))
            .map(|i| i.content.as_slice())
    }
}

/// Handle to an open simulated file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SimFile {
    inode: u64,
}

/// One node's view of its simulated disk.
#[derive(Clone)]
pub struct SimDisk {
    pub(crate) sim: Simulation,
    pub(crate) node: NodeId,
}

impl SimDisk {
    fn with_state<T>(&self, f: impl FnOnce(&mut DiskState) -> T) -> T {
        let mut disks = self.sim.inner.disks.borrow_mut();
        f(disks.entry(self.node).or_default())
    }

    /// Runs `f` against the disk state (inspection from tests and checkers).
    pub fn inspect<T>(&self, f: impl FnOnce(&DiskState) -> T) -> T {
        let mut disks = self.sim.inner.disks.borrow_mut();
        f(disks.entry(self.node).or_default())
    }

    /// Runs `f` with mutable access to the disk state (test fixtures).
    pub fn modify<T>(&self, f: impl FnOnce(&mut DiskState) -> T) -> T {
        self.with_state(f)
    }

    /// `(writes, syncs, reads)` completed so far.
    pub fn counters(&self) -> (u64, u64, u64) {
        self.inspect(|d| (d.writes, d.syncs, d.reads))
    }

    /// Completion time for an operation on `inode`, preserving per-file order.
    fn completion_time(&self, inode: Option<u64>) -> Instant {
        let cfg = &self.sim.inner.config.disk;
        let at = self.sim.now() + self.sim.latency(cfg.latency_min, cfg.latency_max);
        match inode {
            None => at,
            Some(id) => self.with_state(|d| {
                let prev = d.ready_at.get(&id).copied().unwrap_or(Instant::ZERO);
                let at = at.max(prev);
                d.ready_at.insert(id, at);
                at
            }),
        }
    }

    /// Completes `op` at a seeded future time on this node's disk.
    async fn deferred<T: 'static>(
        &self,
        inode: Option<u64>,
        kind: &'static str,
        op: impl FnOnce(&mut DiskState, &mut SeededRng) -> Result<T> + 'static,
    ) -> Result<T> {
        let at = self.completion_time(inode);
        let (c, completer) = completion();
        let sim = self.sim.clone();
        let node = self.node;
        self.sim.schedule(
            node,
            at,
            Box::new(move || {
                let mut rng = sim
                    .inner
                    .rng
                    .borrow_mut()
                    .fork(&format!("disk-{}", sim.inner.seq.get()));
                let result = {
                    let mut disks = sim.inner.disks.borrow_mut();
                    op(disks.entry(node).or_default(), &mut rng)
                };
                sim.trace(kind, &format!("node={node} ok={}", result.is_ok()));
                completer.complete(result);
            }),
        );
        c.await
    }

    fn maybe_io_error(cfg: &DiskConfig, rng: &mut SeededRng, what: &str) -> Result<()> {
        if cfg.io_error_prob > 0.0 && rng.chance(cfg.io_error_prob) {
            return Err(Error::io(
                IoErrorKind::Other,
                format!("injected {what} failure"),
            ));
        }
        Ok(())
    }
}

impl Disk for SimDisk {
    type File = SimFile;

    async fn open(&self, path: &str, mode: OpenMode) -> Result<Self::File> {
        let path = normalize(path)?;
        let id = self.with_state(|d| d.open(&path, mode))?;
        self.sim
            .trace("disk.open", &format!("node={} {path} {mode:?}", self.node));
        self.deferred(None, "disk.open.done", move |_, _| {
            Ok(SimFile { inode: id })
        })
        .await
    }

    async fn map(&self, file: &Self::File) -> Result<Bytes> {
        let id = file.inode;
        self.deferred(Some(id), "disk.map", move |d, _| {
            d.reads += 1;
            let inode = d
                .inodes
                .get(&id)
                .ok_or_else(|| Error::io(IoErrorKind::NotFound, "unlinked file"))?;
            Ok(Bytes::from(inode.content.to_vec()))
        })
        .await
    }

    async fn read_at(&self, file: &Self::File, offset: u64, len: usize) -> Result<Bytes> {
        let id = file.inode;
        let cfg = self.sim.inner.config.disk.clone();
        self.deferred(Some(id), "disk.read", move |d, rng| {
            d.reads += 1;
            let inode = d
                .inodes
                .get(&id)
                .ok_or_else(|| Error::io(IoErrorKind::NotFound, "unlinked file"))?;
            let end = offset as usize + len;
            if end > inode.content.len() {
                return Err(Error::io(
                    IoErrorKind::UnexpectedEof,
                    format!("read {offset}+{len} past end {}", inode.content.len()),
                ));
            }
            let mut out = inode.content[offset as usize..end].to_vec();
            if !out.is_empty() && cfg.read_bitflip_prob > 0.0 && rng.chance(cfg.read_bitflip_prob) {
                let bit = rng.below(out.len() as u64 * 8) as usize;
                out[bit / 8] ^= 1 << (bit % 8);
            }
            Ok(Bytes::from(out))
        })
        .await
    }

    async fn write_at(&self, file: &Self::File, offset: u64, data: Bytes) -> Result<()> {
        let id = file.inode;
        let cfg = self.sim.inner.config.disk.clone();
        self.deferred(Some(id), "disk.write", move |d, rng| {
            Self::maybe_io_error(&cfg, rng, "write")?;
            d.writes += 1;
            let inode = d
                .inodes
                .get_mut(&id)
                .ok_or_else(|| Error::io(IoErrorKind::NotFound, "unlinked file"))?;
            let op = Pending::Write { offset, data };
            inode.apply(&op);
            inode.pending.push(op);
            Ok(())
        })
        .await
    }

    async fn sync(&self, file: &Self::File) -> Result<()> {
        let id = file.inode;
        let cfg = self.sim.inner.config.disk.clone();
        self.deferred(Some(id), "disk.sync", move |d, rng| {
            Self::maybe_io_error(&cfg, rng, "sync")?;
            d.syncs += 1;
            let inode = d
                .inodes
                .get_mut(&id)
                .ok_or_else(|| Error::io(IoErrorKind::NotFound, "unlinked file"))?;
            inode.durable = inode.content.clone();
            inode.pending.clear();
            Ok(())
        })
        .await
    }

    async fn len(&self, file: &Self::File) -> Result<u64> {
        let id = file.inode;
        self.deferred(Some(id), "disk.len", move |d, _| {
            d.inodes
                .get(&id)
                .map(|i| i.content.len() as u64)
                .ok_or_else(|| Error::io(IoErrorKind::NotFound, "unlinked file"))
        })
        .await
    }

    async fn set_len(&self, file: &Self::File, len: u64) -> Result<()> {
        let id = file.inode;
        self.deferred(Some(id), "disk.set_len", move |d, _| {
            d.writes += 1;
            let inode = d
                .inodes
                .get_mut(&id)
                .ok_or_else(|| Error::io(IoErrorKind::NotFound, "unlinked file"))?;
            let op = Pending::SetLen(len);
            inode.apply(&op);
            inode.pending.push(op);
            Ok(())
        })
        .await
    }

    async fn rename(&self, from: &str, to: &str) -> Result<()> {
        let (from, to) = (normalize(from)?, normalize(to)?);
        self.with_state(|d| d.rename(&from, &to))?;
        self.sim
            .trace("disk.rename", &format!("node={} {from} -> {to}", self.node));
        self.deferred(None, "disk.rename.done", |_, _| Ok(())).await
    }

    async fn remove(&self, path: &str) -> Result<()> {
        let path = normalize(path)?;
        self.with_state(|d| d.remove(&path))?;
        self.sim
            .trace("disk.remove", &format!("node={} {path}", self.node));
        self.deferred(None, "disk.remove.done", |_, _| Ok(())).await
    }

    async fn create_dir_all(&self, path: &str) -> Result<()> {
        let path = normalize(path)?;
        self.with_state(|d| d.create_dir_all(&path));
        self.deferred(None, "disk.mkdir.done", |_, _| Ok(())).await
    }

    async fn list(&self, dir: &str) -> Result<Vec<String>> {
        let dir = normalize(dir)?;
        self.deferred(None, "disk.list", move |d, _| d.list(&dir))
            .await
    }

    async fn exists(&self, path: &str) -> Result<bool> {
        let path = normalize(path)?;
        self.deferred(None, "disk.exists", move |d, _| {
            Ok(d.paths.contains_key(&path) || d.dirs.contains(&path))
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sim::{SimConfig, Simulation};
    use cairn_core::Runtime;

    fn cfg(persist: f64, torn: bool) -> SimConfig {
        let mut c = SimConfig::default();
        c.disk.persist_unsynced_prob = persist;
        c.disk.torn_writes = torn;
        c
    }

    #[test]
    fn roundtrip_rename_list_and_errors() {
        let (sim, mut ex) = Simulation::new(1, SimConfig::default());
        let rt = sim.runtime(NodeId(1), &ex.handle());
        ex.block_on(async move {
            let d = rt.disk();
            d.create_dir_all("a/b").await.unwrap();
            let f = d.open("a/b/x.tmp", OpenMode::CreateTruncate).await.unwrap();
            d.write_at(&f, 0, Bytes::from_static(b"hello"))
                .await
                .unwrap();
            d.write_at(&f, 5, Bytes::from_static(b" world"))
                .await
                .unwrap();
            assert_eq!(d.len(&f).await.unwrap(), 11);
            assert_eq!(&d.read_at(&f, 6, 5).await.unwrap()[..], b"world");
            assert_eq!(
                d.read_at(&f, 6, 6).await.unwrap_err().io_kind(),
                Some(IoErrorKind::UnexpectedEof)
            );
            d.rename("a/b/x.tmp", "a/b/x").await.unwrap();
            assert_eq!(d.list("a/b").await.unwrap(), vec!["x".to_string()]);
            assert_eq!(d.list("a").await.unwrap(), vec!["b".to_string()]);
            assert!(d.exists("a/b/x").await.unwrap());
            assert!(!d.exists("a/b/x.tmp").await.unwrap());
            assert_eq!(
                d.open("missing", OpenMode::Read)
                    .await
                    .unwrap_err()
                    .io_kind(),
                Some(IoErrorKind::NotFound)
            );
            assert!(d.open("../x", OpenMode::CreateOrOpen).await.is_err());
            assert_eq!(
                d.open("nodir/x", OpenMode::CreateOrOpen)
                    .await
                    .unwrap_err()
                    .io_kind(),
                Some(IoErrorKind::NotFound)
            );
            d.set_len(&f, 3).await.unwrap();
            assert_eq!(&d.read_at(&f, 0, 3).await.unwrap()[..], b"hel");
        });
    }

    #[test]
    fn unsynced_writes_are_lost_on_crash_and_synced_ones_survive() {
        let (sim, mut ex) = Simulation::new(3, cfg(0.0, false));
        let rt = sim.runtime(NodeId(1), &ex.handle());
        ex.block_on(async move {
            let d = rt.disk();
            let f = d.open("f", OpenMode::CreateOrOpen).await.unwrap();
            d.write_at(&f, 0, Bytes::from_static(b"durable"))
                .await
                .unwrap();
            d.sync(&f).await.unwrap();
            d.write_at(&f, 7, Bytes::from_static(b"-volatile"))
                .await
                .unwrap();
            assert_eq!(d.len(&f).await.unwrap(), 16);
        });
        sim.crash(NodeId(1), &mut ex);
        let content = sim
            .disk(NodeId(1))
            .inspect(|d| d.content("f").unwrap().to_vec());
        assert_eq!(content, b"durable");
    }

    #[test]
    fn crash_applies_a_prefix_of_unsynced_ops_possibly_torn() {
        for seed in 0..64u64 {
            let (sim, mut ex) = Simulation::new(seed, cfg(0.5, true));
            let rt = sim.runtime(NodeId(1), &ex.handle());
            ex.block_on(async move {
                let d = rt.disk();
                let f = d.open("f", OpenMode::CreateOrOpen).await.unwrap();
                d.write_at(&f, 0, Bytes::from_static(b"AAAA"))
                    .await
                    .unwrap();
                d.sync(&f).await.unwrap();
                d.write_at(&f, 4, Bytes::from_static(b"BBBB"))
                    .await
                    .unwrap();
                d.write_at(&f, 8, Bytes::from_static(b"CCCC"))
                    .await
                    .unwrap();
            });
            sim.crash(NodeId(1), &mut ex);
            let content = sim
                .disk(NodeId(1))
                .inspect(|d| d.content("f").unwrap().to_vec());
            let allowed = [
                "AAAA",
                "AAAAB",
                "AAAABB",
                "AAAABBB",
                "AAAABBBB",
                "AAAABBBBC",
                "AAAABBBBCC",
                "AAAABBBBCCC",
                "AAAABBBBCCCC",
            ];
            assert!(
                allowed.contains(&std::str::from_utf8(&content).unwrap()),
                "seed {seed}: {content:?}"
            );
        }
    }

    #[test]
    fn writes_issued_in_order_on_one_file_complete_in_order() {
        use std::future::poll_fn;
        use std::pin::pin;
        use std::task::Poll;
        let (sim, mut ex) = Simulation::new(9, SimConfig::default());
        let rt = sim.runtime(NodeId(1), &ex.handle());
        ex.block_on(async move {
            let d = rt.disk();
            let f = d.open("f", OpenMode::CreateOrOpen).await.unwrap();
            // Two overlapping writes issued from one task, awaited together: the second must win
            // even though latencies are random.
            let mut w1 = pin!(d.write_at(&f, 0, Bytes::from_static(b"1111")));
            let mut w2 = pin!(d.write_at(&f, 0, Bytes::from_static(b"2222")));
            let (mut d1, mut d2) = (false, false);
            poll_fn(|cx| {
                if !d1 && w1.as_mut().poll(cx).is_ready() {
                    d1 = true;
                }
                if !d2 && w2.as_mut().poll(cx).is_ready() {
                    d2 = true;
                }
                if d1 && d2 {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
            assert_eq!(&d.read_at(&f, 0, 4).await.unwrap()[..], b"2222");
        });
    }

    #[test]
    fn injected_errors_and_bitflips_happen() {
        let mut c = SimConfig::default();
        c.disk.io_error_prob = 1.0;
        let (sim, mut ex) = Simulation::new(1, c);
        let rt = sim.runtime(NodeId(1), &ex.handle());
        ex.block_on(async move {
            let d = rt.disk();
            let f = d.open("f", OpenMode::CreateOrOpen).await.unwrap();
            assert!(d.write_at(&f, 0, Bytes::from_static(b"x")).await.is_err());
        });
        let mut c = SimConfig::default();
        c.disk.read_bitflip_prob = 1.0;
        let (sim, mut ex) = Simulation::new(1, c);
        let rt = sim.runtime(NodeId(1), &ex.handle());
        ex.block_on(async move {
            let d = rt.disk();
            let f = d.open("f", OpenMode::CreateOrOpen).await.unwrap();
            d.write_at(&f, 0, Bytes::from_static(b"abcd"))
                .await
                .unwrap();
            assert_ne!(&d.read_at(&f, 0, 4).await.unwrap()[..], b"abcd");
        });
    }
}
