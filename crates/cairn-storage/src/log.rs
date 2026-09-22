//! The replicated log, which is also the write-ahead log.
//!
//! Layout: a directory of files named by the index of their first entry
//! (`00000000000000000001.log`). Each file starts with a header (magic, version, first index,
//! crc) followed by records `[len u32][crc32 u32][term u64][index u64][payload]`. Recovery scans
//! each file and truncates it at the first record that is short, has a bad checksum or an
//! unexpected index; a torn tail therefore costs at most the unsynced suffix, never an
//! acknowledged entry. Files roll over at [`LogConfig::max_file_bytes`]; the previous file is
//! synced before the next is created, so a gap between files means the later files are garbage
//! from an interrupted rollover and they are removed.

use crate::codec::{Reader, Writer, crc32};
use bytes::Bytes;
use cairn_core::{Disk, Error, LogIndex, OpenMode, Result, Runtime, Term};

const FILE_MAGIC: &[u8; 8] = b"CRNLOG01";
/// Log file format version. Bump on any layout change (ADR 0004).
pub const LOG_VERSION: u32 = 1;
const HEADER_LEN: u64 = 8 + 4 + 8 + 4;
const RECORD_HEADER_LEN: usize = 8;
const RECORD_FIXED_LEN: usize = 16;

/// One log entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    /// Position in the log.
    pub index: LogIndex,
    /// Term of the leader that appended it.
    pub term: Term,
    /// Opaque payload (a serialized command batch).
    pub payload: Bytes,
}

/// Log tuning.
#[derive(Debug, Clone)]
pub struct LogConfig {
    /// Files roll over once they reach this many bytes (checked before each append).
    pub max_file_bytes: u64,
}

impl Default for LogConfig {
    fn default() -> Self {
        LogConfig {
            max_file_bytes: 64 << 20,
        }
    }
}

struct LogFile<F> {
    name: String,
    file: F,
    first_index: u64,
    /// `(offset, record length)` per entry, in index order starting at `first_index`.
    entries: Vec<(u64, u32)>,
    /// Write offset (end of the last valid record).
    len: u64,
}

impl<F> LogFile<F> {
    fn last_index(&self) -> Option<u64> {
        if self.entries.is_empty() {
            None
        } else {
            Some(self.first_index + self.entries.len() as u64 - 1)
        }
    }

    fn slot(&self, index: u64) -> Option<(u64, u32)> {
        index
            .checked_sub(self.first_index)
            .and_then(|i| self.entries.get(i as usize).copied())
    }
}

fn file_name(first_index: u64) -> String {
    format!("{first_index:020}.log")
}

fn parse_file_name(name: &str) -> Option<u64> {
    name.strip_suffix(".log")
        .filter(|s| s.len() == 20)
        .and_then(|s| s.parse().ok())
}

fn encode_header(first_index: u64) -> Bytes {
    let mut w = Writer::with_capacity(HEADER_LEN as usize);
    w.raw(FILE_MAGIC).u32(LOG_VERSION).u64(first_index);
    let crc = crc32(w.as_slice());
    w.u32(crc);
    w.into_bytes()
}

fn decode_header(bytes: &[u8]) -> Result<u64> {
    if bytes.len() < HEADER_LEN as usize {
        return Err(Error::corruption("log header too short"));
    }
    let (body, crc_bytes) = bytes[..HEADER_LEN as usize].split_at(HEADER_LEN as usize - 4);
    let expected = u32::from_le_bytes([crc_bytes[0], crc_bytes[1], crc_bytes[2], crc_bytes[3]]);
    if crc32(body) != expected {
        return Err(Error::corruption("log header checksum mismatch"));
    }
    let mut r = Reader::new(body);
    if r.raw(8)? != FILE_MAGIC {
        return Err(Error::corruption("log header magic mismatch"));
    }
    let version = r.u32()?;
    if version != LOG_VERSION {
        return Err(Error::UnsupportedVersion {
            found: version,
            supported: LOG_VERSION,
        });
    }
    r.u64()
}

fn encode_record(w: &mut Writer, e: &LogEntry) {
    let body_len = RECORD_FIXED_LEN + e.payload.len();
    let mut body = Writer::with_capacity(body_len);
    body.u64(e.term.get()).u64(e.index.get()).raw(&e.payload);
    w.u32(body_len as u32)
        .u32(crc32(body.as_slice()))
        .raw(body.as_slice());
}

/// Decodes one record at the start of `buf`. Returns the entry and the record's total length,
/// or `None` if the bytes do not form a complete, valid record.
fn decode_record(buf: &[u8]) -> Option<(LogEntry, usize)> {
    if buf.len() < RECORD_HEADER_LEN {
        return None;
    }
    let len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    let crc = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]);
    if len < RECORD_FIXED_LEN || buf.len() < RECORD_HEADER_LEN + len {
        return None;
    }
    let body = &buf[RECORD_HEADER_LEN..RECORD_HEADER_LEN + len];
    if crc32(body) != crc {
        return None;
    }
    let mut r = Reader::new(body);
    let term = r.u64().ok()?;
    let index = r.u64().ok()?;
    let payload = Bytes::copy_from_slice(r.raw(len - RECORD_FIXED_LEN).ok()?);
    Some((
        LogEntry {
            index: LogIndex(index),
            term: Term(term),
            payload,
        },
        RECORD_HEADER_LEN + len,
    ))
}

/// The log of one shard.
pub struct Log<R: Runtime> {
    rt: R,
    dir: String,
    cfg: LogConfig,
    files: Vec<LogFile<<R::Disk as Disk>::File>>,
    /// Last index known to be durable.
    synced: Option<u64>,
    dirty: bool,
}

impl<R: Runtime> Log<R> {
    /// Opens (recovering) or creates the log in `dir`. A new log starts at index 1.
    pub async fn open(rt: R, dir: &str, cfg: LogConfig) -> Result<Self> {
        let disk = rt.disk();
        disk.create_dir_all(dir).await?;
        let mut names: Vec<(u64, String)> = Vec::new();
        for name in disk.list(dir).await? {
            if name.ends_with(".tmp") {
                disk.remove(&format!("{dir}/{name}")).await?;
            } else if let Some(first) = parse_file_name(&name) {
                names.push((first, name));
            }
        }
        names.sort();
        let mut log = Log {
            rt: rt.clone(),
            dir: dir.to_owned(),
            cfg,
            files: Vec::new(),
            synced: None,
            dirty: false,
        };
        let mut expected_next: Option<u64> = None;
        let mut garbage_from = None;
        for (i, (first, name)) in names.iter().enumerate() {
            if let Some(next) = expected_next
                && *first != next
            {
                tracing::warn!(dir, file = %name, expected = next, "log file gap: dropping this and later files");
                garbage_from = Some(i);
                break;
            }
            let lf = log.recover_file(name, *first).await?;
            expected_next = Some(lf.last_index().map_or(lf.first_index, |l| l + 1));
            log.files.push(lf);
        }
        if let Some(from) = garbage_from {
            for (_, name) in &names[from..] {
                disk.remove(&format!("{dir}/{name}")).await?;
            }
        }
        if log.files.is_empty() {
            let lf = log.create_file(1).await?;
            log.files.push(lf);
        }
        log.synced = log.last_index().map(LogIndex::get);
        Ok(log)
    }

    async fn recover_file(
        &self,
        name: &str,
        first_index: u64,
    ) -> Result<LogFile<<R::Disk as Disk>::File>> {
        let disk = self.rt.disk();
        let path = format!("{}/{name}", self.dir);
        let file = disk.open(&path, OpenMode::ReadWrite).await?;
        let len = disk.len(&file).await?;
        let bytes = disk.read_at(&file, 0, len as usize).await?;
        let header_first = decode_header(&bytes)?;
        if header_first != first_index {
            return Err(Error::corruption(format!(
                "log file {name} header index {header_first} != name"
            )));
        }
        let mut entries = Vec::new();
        let mut off = HEADER_LEN as usize;
        let mut expected = first_index;
        while let Some((entry, rec_len)) = decode_record(&bytes[off..]) {
            if entry.index.get() != expected {
                break;
            }
            entries.push((off as u64, rec_len as u32));
            off += rec_len;
            expected += 1;
        }
        if (off as u64) < len {
            tracing::warn!(file = %name, valid = off, len, "truncating log tail");
            disk.set_len(&file, off as u64).await?;
            disk.sync(&file).await?;
        }
        Ok(LogFile {
            name: name.to_owned(),
            file,
            first_index,
            entries,
            len: off as u64,
        })
    }

    async fn create_file(&self, first_index: u64) -> Result<LogFile<<R::Disk as Disk>::File>> {
        let disk = self.rt.disk();
        let name = file_name(first_index);
        let tmp = format!("{}/{name}.tmp", self.dir);
        let path = format!("{}/{name}", self.dir);
        let file = disk.open(&tmp, OpenMode::CreateTruncate).await?;
        disk.write_at(&file, 0, encode_header(first_index)).await?;
        disk.sync(&file).await?;
        disk.rename(&tmp, &path).await?;
        Ok(LogFile {
            name,
            file,
            first_index,
            entries: Vec::new(),
            len: HEADER_LEN,
        })
    }

    /// Index of the first entry the log may hold (entries before it were compacted away).
    pub fn first_index(&self) -> LogIndex {
        LogIndex(self.files[0].first_index)
    }

    /// Index of the last entry, if any.
    pub fn last_index(&self) -> Option<LogIndex> {
        self.files
            .iter()
            .rev()
            .find_map(|f| f.last_index())
            .map(LogIndex)
    }

    /// Index the next appended entry must carry.
    pub fn next_index(&self) -> LogIndex {
        self.last_index().map_or(self.first_index(), LogIndex::next)
    }

    /// Last index known to be durable.
    pub fn synced_index(&self) -> Option<LogIndex> {
        self.synced.map(LogIndex)
    }

    /// Number of entries currently held.
    pub fn len(&self) -> u64 {
        self.files.iter().map(|f| f.entries.len() as u64).sum()
    }

    /// Whether the log holds no entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Appends contiguous entries starting at [`Log::next_index`]. Not durable until
    /// [`Log::sync`].
    pub async fn append(&mut self, entries: &[LogEntry]) -> Result<()> {
        let mut expected = self.next_index();
        for e in entries {
            if e.index != expected {
                return Err(Error::InvalidRequest(format!(
                    "append index {} != expected {expected}",
                    e.index
                )));
            }
            expected = expected.next();
        }
        let disk = self.rt.disk();
        let mut i = 0;
        while i < entries.len() {
            // Roll over when the active file is full (and holds at least one entry).
            let active = self.files.last().expect("at least one log file");
            if active.len >= self.cfg.max_file_bytes && !active.entries.is_empty() {
                if self.dirty {
                    disk.sync(&active.file).await?;
                    self.dirty = false;
                }
                let lf = self.create_file(entries[i].index.get()).await?;
                self.files.push(lf);
            }
            // Batch as many entries as fit into one write.
            let active = self.files.last_mut().expect("at least one log file");
            let mut w = Writer::new();
            let start = i;
            let mut offsets = Vec::new();
            while i < entries.len() {
                let before = w.len();
                encode_record(&mut w, &entries[i]);
                offsets.push((active.len + before as u64, (w.len() - before) as u32));
                i += 1;
                if active.len + w.len() as u64 >= self.cfg.max_file_bytes {
                    break;
                }
            }
            debug_assert!(i > start);
            disk.write_at(&active.file, active.len, w.into_bytes())
                .await?;
            active.len = offsets
                .last()
                .map(|(o, l)| o + *l as u64)
                .unwrap_or(active.len);
            active.entries.extend(offsets);
            self.dirty = true;
        }
        Ok(())
    }

    /// Makes every appended entry durable.
    pub async fn sync(&mut self) -> Result<()> {
        if self.dirty {
            let active = self.files.last().expect("at least one log file");
            self.rt.disk().sync(&active.file).await?;
            self.dirty = false;
        }
        self.synced = self.last_index().map(LogIndex::get);
        Ok(())
    }

    fn locate(&self, index: u64) -> Option<(usize, u64, u32)> {
        let fi = self
            .files
            .partition_point(|f| f.first_index <= index)
            .checked_sub(1)?;
        let (off, len) = self.files[fi].slot(index)?;
        Some((fi, off, len))
    }

    /// Reads one entry.
    pub async fn read(&self, index: LogIndex) -> Result<LogEntry> {
        let mut v = self.read_range(index, index).await?;
        v.pop()
            .ok_or_else(|| Error::InvalidRequest(format!("log index {index} not found")))
    }

    /// Reads entries `from..=to` (inclusive), which must all exist.
    pub async fn read_range(&self, from: LogIndex, to: LogIndex) -> Result<Vec<LogEntry>> {
        if to < from {
            return Ok(Vec::new());
        }
        let disk = self.rt.disk();
        let mut out = Vec::with_capacity((to.get() - from.get() + 1) as usize);
        let mut index = from.get();
        while index <= to.get() {
            let (fi, off, _) = self
                .locate(index)
                .ok_or_else(|| Error::InvalidRequest(format!("log index {index} not found")))?;
            let f = &self.files[fi];
            let last_wanted = to
                .get()
                .min(f.last_index().expect("located file has entries"));
            let (last_off, last_len) = f.slot(last_wanted).expect("in range");
            let end = last_off + last_len as u64;
            let bytes = disk.read_at(&f.file, off, (end - off) as usize).await?;
            let mut pos = 0usize;
            while index <= last_wanted {
                let (entry, rec_len) = decode_record(&bytes[pos..]).ok_or_else(|| {
                    Error::corruption(format!("log record {index} failed validation on read"))
                })?;
                if entry.index.get() != index {
                    return Err(Error::corruption(format!(
                        "log record index {} != {index}",
                        entry.index
                    )));
                }
                out.push(entry);
                pos += rec_len;
                index += 1;
            }
        }
        Ok(out)
    }

    /// Removes every entry with index `>= from` (Raft conflict resolution). Durable on return.
    pub async fn truncate_suffix(&mut self, from: LogIndex) -> Result<()> {
        let from = from.get();
        let disk = self.rt.disk();
        // Drop whole files that start at or after `from`, but never the first file.
        while self.files.len() > 1 && self.files.last().expect("non-empty").first_index >= from {
            let f = self.files.pop().expect("non-empty");
            disk.remove(&format!("{}/{}", self.dir, f.name)).await?;
        }
        let active = self.files.last_mut().expect("at least one log file");
        // `from` below the file's first index (only possible below the compaction point) empties
        // the file.
        let rel = from.saturating_sub(active.first_index) as usize;
        if rel < active.entries.len() {
            let off = if rel == 0 {
                HEADER_LEN
            } else {
                active.entries[rel].0
            };
            active.entries.truncate(rel);
            active.len = off;
            disk.set_len(&active.file, off).await?;
        }
        disk.sync(&active.file).await?;
        self.dirty = false;
        self.synced = self.last_index().map(LogIndex::get);
        Ok(())
    }

    /// Releases entries before `up_to` by deleting whole files (the active file is kept).
    pub async fn truncate_prefix(&mut self, up_to: LogIndex) -> Result<()> {
        let disk = self.rt.disk();
        while self.files.len() > 1 {
            let f = &self.files[0];
            let removable = f.last_index().is_some_and(|l| l < up_to.get());
            if !removable {
                break;
            }
            let f = self.files.remove(0);
            disk.remove(&format!("{}/{}", self.dir, f.name)).await?;
        }
        Ok(())
    }

    /// Number of files currently backing the log.
    pub fn file_count(&self) -> usize {
        self.files.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_core::NodeId;
    use cairn_runtime::Executor;
    use cairn_sim::{SimConfig, SimRuntime, Simulation};
    use proptest::prelude::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    fn entry(i: u64, term: u64, payload: &[u8]) -> LogEntry {
        LogEntry {
            index: LogIndex(i),
            term: Term(term),
            payload: Bytes::copy_from_slice(payload),
        }
    }

    fn small_cfg() -> LogConfig {
        LogConfig {
            max_file_bytes: 512,
        }
    }

    #[test]
    fn append_read_rollover_and_reopen() {
        let (sim, mut ex) = Simulation::new(1, SimConfig::default());
        let rt = sim.runtime(NodeId(1), &ex.handle());
        let rt2 = rt.clone();
        ex.block_on(async move {
            let mut log = Log::open(rt2.clone(), "shard/log", small_cfg())
                .await
                .unwrap();
            assert_eq!(log.first_index(), LogIndex(1));
            assert_eq!(log.last_index(), None);
            let entries: Vec<_> = (1..=100)
                .map(|i| entry(i, 1 + i / 30, &vec![i as u8; (i % 40) as usize]))
                .collect();
            log.append(&entries[..10]).await.unwrap();
            log.append(&entries[10..]).await.unwrap();
            log.sync().await.unwrap();
            assert_eq!(log.last_index(), Some(LogIndex(100)));
            assert!(log.file_count() > 1, "rollover expected");
            assert_eq!(
                log.read_range(LogIndex(1), LogIndex(100)).await.unwrap(),
                entries
            );
            assert_eq!(log.read(LogIndex(57)).await.unwrap(), entries[56]);
            assert!(log.append(&[entry(5, 1, b"x")]).await.is_err());
            drop(log);
            let log = Log::open(rt2.clone(), "shard/log", small_cfg())
                .await
                .unwrap();
            assert_eq!(log.last_index(), Some(LogIndex(100)));
            assert_eq!(log.synced_index(), Some(LogIndex(100)));
            assert_eq!(
                log.read_range(LogIndex(1), LogIndex(100)).await.unwrap(),
                entries
            );
        });
    }

    #[test]
    fn truncate_suffix_and_prefix() {
        let (sim, mut ex) = Simulation::new(2, SimConfig::default());
        let rt = sim.runtime(NodeId(1), &ex.handle());
        ex.block_on(async move {
            let mut log = Log::open(rt.clone(), "log", small_cfg()).await.unwrap();
            let entries: Vec<_> = (1..=60).map(|i| entry(i, 1, &[0u8; 20])).collect();
            log.append(&entries).await.unwrap();
            log.sync().await.unwrap();
            let files_before = log.file_count();
            log.truncate_suffix(LogIndex(45)).await.unwrap();
            assert_eq!(log.last_index(), Some(LogIndex(44)));
            assert!(log.file_count() < files_before);
            log.append(&[entry(45, 2, b"new")]).await.unwrap();
            log.sync().await.unwrap();
            assert_eq!(log.read(LogIndex(45)).await.unwrap().term, Term(2));
            log.truncate_prefix(LogIndex(30)).await.unwrap();
            assert!(log.first_index() <= LogIndex(30));
            assert!(log.first_index() > LogIndex(1));
            assert!(log.read(LogIndex(1)).await.is_err());
            assert_eq!(log.read(LogIndex(44)).await.unwrap(), entries[43]);
            // Truncate everything: the first file is kept and emptied.
            log.truncate_suffix(LogIndex(1)).await.unwrap();
            assert_eq!(log.last_index(), None);
            assert_eq!(log.file_count(), 1);
            drop(log);
            let log = Log::open(rt.clone(), "log", small_cfg()).await.unwrap();
            assert_eq!(log.last_index(), None);
            assert!(log.first_index() > LogIndex(1));
        });
    }

    /// Writes, syncs at random points, crashes at a random time, recovers, repeats. After each
    /// recovery: every acknowledged entry is present and equal, nothing beyond what was written
    /// exists, and the log is a contiguous prefix.
    fn crash_recovery_round(seed: u64) {
        let mut cfg = SimConfig::default();
        cfg.disk.persist_unsynced_prob = 0.5;
        cfg.disk.torn_writes = true;
        let (sim, mut ex) = Simulation::new(seed, cfg);
        let node = NodeId(1);
        let written: Rc<RefCell<Vec<LogEntry>>> = Rc::new(RefCell::new(Vec::new()));
        let acked: Rc<RefCell<u64>> = Rc::new(RefCell::new(0));
        let mut rng = sim.rng("workload");
        for round in 0..6 {
            let rt: SimRuntime = sim.runtime(node, &ex.handle());
            let (w, a) = (written.clone(), acked.clone());
            let round_seed = rng.next_u64();
            let rt_task = rt.clone();
            rt.spawn(async move {
                let rt = rt_task;
                let mut log = Log::open(rt.clone(), "log", small_cfg()).await.unwrap();
                // Recovery checks.
                let last = log.last_index().map_or(0, LogIndex::get);
                let ack = *a.borrow();
                assert!(
                    last >= ack,
                    "seed {seed} round {round}: lost acked entries: last={last} acked={ack}"
                );
                let first = log.first_index().get() as usize;
                assert!(last as usize <= w.borrow().len());
                if last > 0 {
                    let got = log
                        .read_range(log.first_index(), LogIndex(last))
                        .await
                        .unwrap();
                    let expected = w.borrow()[first - 1..last as usize].to_vec();
                    assert_eq!(got, expected, "seed {seed} round {round}");
                }
                // Unacknowledged tail is re-written by the caller (Raft would), so forget it.
                w.borrow_mut().truncate(last as usize);
                let mut r = cairn_core::SeededRng::from_seed(round_seed);
                for _ in 0..40 {
                    let n = 1 + r.below(5);
                    let next = log.next_index().get();
                    let batch: Vec<LogEntry> = (0..n)
                        .map(|k| {
                            let i = next + k;
                            let len = r.below(60) as usize;
                            entry(i, round as u64 + 1, &vec![(i % 251) as u8; len])
                        })
                        .collect();
                    w.borrow_mut().extend(batch.iter().cloned());
                    log.append(&batch).await.unwrap();
                    if r.chance(0.3) {
                        log.sync().await.unwrap();
                        *a.borrow_mut() = log.last_index().unwrap().get();
                    }
                }
                log.sync().await.unwrap();
                *a.borrow_mut() = log.last_index().unwrap().get();
            });
            let delay = cairn_core::Duration::from_micros(rng.below(30_000));
            let h = ex.handle();
            ex.block_on(h.sleep(delay));
            sim.crash(node, &mut ex);
        }
    }

    #[test]
    fn crash_recovery_never_loses_acknowledged_entries() {
        for seed in 0..150u64 {
            crash_recovery_round(seed);
        }
    }

    /// Takes a valid multi-file log image, damages one file at one byte (cut or flip), and checks
    /// that recovery yields a contiguous prefix of the original entries.
    fn log_image(seed: u64) -> (Vec<LogEntry>, Vec<(String, Vec<u8>)>) {
        let (sim, mut ex) = Simulation::new(seed, SimConfig::default());
        let rt = sim.runtime(NodeId(1), &ex.handle());
        let entries: Vec<_> = (1..=30)
            .map(|i| entry(i, 1, &vec![i as u8; (i * 7 % 50) as usize]))
            .collect();
        let e2 = entries.clone();
        ex.block_on(async move {
            let mut log = Log::open(rt.clone(), "log", small_cfg()).await.unwrap();
            log.append(&e2).await.unwrap();
            log.sync().await.unwrap();
        });
        let files = sim.disk(NodeId(1)).inspect(|d| {
            d.list("log")
                .unwrap()
                .into_iter()
                .map(|n| {
                    let c = d.durable_content(&format!("log/{n}")).unwrap().to_vec();
                    (n, c)
                })
                .collect::<Vec<_>>()
        });
        (entries, files)
    }

    fn recover_from_image(files: &[(String, Vec<u8>)]) -> Result<Vec<LogEntry>> {
        let (sim, mut ex) = Simulation::new(0, SimConfig::default());
        let disk = sim.disk(NodeId(1));
        disk.modify(|d| {
            for (n, c) in files {
                d.set_durable_file(&format!("log/{n}"), c);
            }
        });
        let rt = sim.runtime(NodeId(1), &ex.handle());
        ex.block_on(async move {
            let log = Log::open(rt.clone(), "log", small_cfg()).await?;
            match log.last_index() {
                None => Ok(Vec::new()),
                Some(last) => log.read_range(log.first_index(), last).await,
            }
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(400))]
        #[test]
        fn damaged_image_recovers_to_a_prefix(file_pick in 0usize..8, pos in 0usize..4096, flip in any::<bool>()) {
            let (entries, files) = log_image(11);
            let fi = file_pick % files.len();
            let mut damaged = files.clone();
            let content = &mut damaged[fi].1;
            let pos = pos % content.len().max(1);
            if flip {
                if !content.is_empty() {
                    content[pos] ^= 0x40;
                }
            } else {
                content.truncate(pos);
            }
            match recover_from_image(&damaged) {
                Ok(got) => {
                    prop_assert!(got.len() <= entries.len());
                    prop_assert_eq!(&got[..], &entries[..got.len()]);
                }
                Err(e) => {
                    // Header damage of a file is reported as corruption, never silently accepted.
                    prop_assert!(matches!(e, Error::Corruption(_) | Error::UnsupportedVersion { .. }), "{e}");
                }
            }
        }
    }

    #[test]
    fn real_disk_roundtrip() {
        use cairn_runtime::blocking::{BlockingReactor, RealRuntime};
        let dir = std::env::temp_dir().join(format!("cairn-log-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut ex = Executor::new(BlockingReactor::new());
        let rt = RealRuntime::new(ex.handle(), &dir, NodeId(1)).unwrap();
        let entries: Vec<_> = (1..=200)
            .map(|i| entry(i, 3, &vec![7u8; (i % 100) as usize]))
            .collect();
        let e2 = entries.clone();
        ex.block_on(async move {
            let mut log = Log::open(
                rt.clone(),
                "log",
                LogConfig {
                    max_file_bytes: 4096,
                },
            )
            .await
            .unwrap();
            log.append(&e2).await.unwrap();
            log.sync().await.unwrap();
            drop(log);
            let log = Log::open(
                rt.clone(),
                "log",
                LogConfig {
                    max_file_bytes: 4096,
                },
            )
            .await
            .unwrap();
            assert_eq!(
                log.read_range(LogIndex(1), LogIndex(200)).await.unwrap(),
                e2
            );
            assert!(log.file_count() > 1);
        });
        let _ = std::fs::remove_dir_all(&dir);
    }
}
