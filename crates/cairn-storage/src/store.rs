//! The shard store: log, memtable, segments, deletion sets and manifest, with crash recovery.
//!
//! Single-node semantics in Phase 1: [`Store::write`] appends a command to the log, syncs, and
//! applies it. Raft (Phase 3) drives the same pieces through [`Store::log_mut`] and
//! [`Store::apply`]. A flush turns the memtable into a segment, persists dirty deletion sets,
//! publishes the new manifest, and only then advances the manifest's applied index, so a crash
//! at any point is repaired by replaying the log from that index.

use crate::columns::{DocStore, write_columns};
use crate::command::Command;
use crate::deletion::DeletionSet;
use crate::log::{Log, LogConfig, LogEntry};
use crate::manifest::{Manifest, ManifestStore};
use crate::memtable::Memtable;
use crate::segment::{SegmentReader, SegmentWriter};
use cairn_core::codec::{Reader, Writer};
use cairn_core::{
    Disk, DocId, Document, Error, LogIndex, Result, Runtime, Schema, SegmentId, Term,
};

/// Tuning.
#[derive(Debug, Clone)]
pub struct StoreConfig {
    /// Flush the memtable once it holds about this many bytes.
    pub memtable_max_bytes: usize,
    /// Log settings.
    pub log: LogConfig,
}

impl Default for StoreConfig {
    fn default() -> Self {
        StoreConfig {
            memtable_max_bytes: 64 << 20,
            log: LogConfig::default(),
        }
    }
}

/// A published segment, as recorded in the manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentMeta {
    /// Id (also the file name stem).
    pub id: SegmentId,
    /// Rows in the segment.
    pub doc_count: u32,
    /// Last log index folded into the segment.
    pub log_last: LogIndex,
    /// File length in bytes.
    pub file_len: u64,
    /// Body hash from the footer.
    pub file_hash: u64,
}

/// The shard manifest.
#[derive(Debug, Clone, PartialEq)]
pub struct ShardManifest {
    /// Collection schema.
    pub schema: Schema,
    /// Everything up to and including this index is reflected in segments and deletion files.
    pub applied_index: LogIndex,
    /// Live segments, oldest first.
    pub segments: Vec<SegmentMeta>,
    /// Next segment id to allocate.
    pub next_segment_id: u64,
}

impl Manifest for ShardManifest {
    fn encode(&self, w: &mut Writer) {
        self.schema.encode(w);
        w.u64(self.applied_index.get())
            .u64(self.next_segment_id)
            .u32(self.segments.len() as u32);
        for s in &self.segments {
            w.u64(s.id.get())
                .u32(s.doc_count)
                .u64(s.log_last.get())
                .u64(s.file_len)
                .u64(s.file_hash);
        }
    }

    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        let schema = Schema::decode(r)?;
        let applied_index = LogIndex(r.u64()?);
        let next_segment_id = r.u64()?;
        let n = r.u32()? as usize;
        let mut segments = Vec::with_capacity(n.min(1 << 16));
        for _ in 0..n {
            segments.push(SegmentMeta {
                id: SegmentId(r.u64()?),
                doc_count: r.u32()?,
                log_last: LogIndex(r.u64()?),
                file_len: r.u64()?,
                file_hash: r.u64()?,
            });
        }
        Ok(ShardManifest {
            schema,
            applied_index,
            segments,
            next_segment_id,
        })
    }
}

struct OpenSegment<R: Runtime> {
    meta: SegmentMeta,
    reader: SegmentReader<R>,
    docs: DocStore,
    deletions: DeletionSet,
    deletions_dirty: bool,
}

/// One shard's storage.
pub struct Store<R: Runtime> {
    rt: R,
    dir: String,
    cfg: StoreConfig,
    manifest_store: ManifestStore<R>,
    manifest: ShardManifest,
    log: Log<R>,
    segments: Vec<OpenSegment<R>>,
    memtable: Memtable,
    applied: LogIndex,
}

fn seg_path(dir: &str, id: SegmentId) -> String {
    format!("{dir}/segs/{:016x}.seg", id.get())
}

fn del_path(dir: &str, id: SegmentId) -> String {
    format!("{dir}/segs/{:016x}.del", id.get())
}

impl<R: Runtime> Store<R> {
    /// Opens or creates the shard store in `dir`. `schema` is used only when creating; an
    /// existing store's schema must match.
    pub async fn open(rt: R, dir: &str, schema: Schema, cfg: StoreConfig) -> Result<Self> {
        let disk = rt.disk();
        disk.create_dir_all(&format!("{dir}/segs")).await?;
        let manifest_store = ManifestStore::new(rt.clone(), format!("{dir}/MANIFEST"));
        let manifest = match manifest_store.load::<ShardManifest>().await? {
            Some(m) => {
                if m.schema != schema {
                    return Err(Error::Schema(
                        "stored schema differs from the requested schema".into(),
                    ));
                }
                m
            }
            None => {
                let m = ShardManifest {
                    schema,
                    applied_index: LogIndex(0),
                    segments: Vec::new(),
                    next_segment_id: 1,
                };
                manifest_store.store(&m).await?;
                m
            }
        };
        // Remove leftovers: temp files and segment files the manifest does not reference.
        let referenced: Vec<String> = manifest
            .segments
            .iter()
            .flat_map(|s| {
                [
                    format!("{:016x}.seg", s.id.get()),
                    format!("{:016x}.del", s.id.get()),
                ]
            })
            .collect();
        for name in disk.list(&format!("{dir}/segs")).await? {
            if name.ends_with(".tmp")
                || (!referenced.contains(&name)
                    && (name.ends_with(".seg") || name.ends_with(".del")))
            {
                disk.remove(&format!("{dir}/segs/{name}")).await?;
            }
        }
        let mut segments = Vec::with_capacity(manifest.segments.len());
        for meta in &manifest.segments {
            let reader = SegmentReader::open(rt.clone(), &seg_path(dir, meta.id)).await?;
            if reader.len() != meta.file_len || reader.file_hash() != meta.file_hash {
                return Err(Error::corruption(format!(
                    "segment {} does not match the manifest",
                    meta.id
                )));
            }
            let docs = DocStore::open(&reader).await?;
            if docs.doc_count() != meta.doc_count {
                return Err(Error::corruption(format!(
                    "segment {} row count mismatch",
                    meta.id
                )));
            }
            let del_store = ManifestStore::new(rt.clone(), del_path(dir, meta.id));
            let deletions = match del_store.load::<DeletionSet>().await? {
                Some(d) if d.rows() == meta.doc_count => d,
                Some(_) => {
                    return Err(Error::corruption(format!(
                        "deletion set of segment {} has the wrong size",
                        meta.id
                    )));
                }
                None => DeletionSet::new(meta.doc_count),
            };
            segments.push(OpenSegment {
                meta: meta.clone(),
                reader,
                docs,
                deletions,
                deletions_dirty: false,
            });
        }
        let log = Log::open(rt.clone(), &format!("{dir}/log"), cfg.log.clone()).await?;
        let mut store = Store {
            rt,
            dir: dir.to_owned(),
            cfg,
            manifest_store,
            manifest,
            log,
            segments,
            memtable: Memtable::new(),
            applied: LogIndex(0),
        };
        store.applied = store.manifest.applied_index;
        // Replay the log after the manifest's applied index.
        if let Some(last) = store.log.last_index()
            && last > store.applied
        {
            let from = store.applied.next().max(store.log.first_index());
            let entries = store.log.read_range(from, last).await?;
            for e in entries {
                let cmd = Command::from_bytes(&e.payload)?;
                store.apply(e.index, &cmd)?;
            }
        }
        Ok(store)
    }

    /// The schema.
    pub fn schema(&self) -> &Schema {
        &self.manifest.schema
    }

    /// Highest log index applied to the in-memory state.
    pub fn applied_index(&self) -> LogIndex {
        self.applied
    }

    /// The log (Raft drives it directly).
    pub fn log_mut(&mut self) -> &mut Log<R> {
        &mut self.log
    }

    /// The log.
    pub fn log(&self) -> &Log<R> {
        &self.log
    }

    /// Published segments, oldest first.
    pub fn segments(&self) -> impl Iterator<Item = &SegmentMeta> {
        self.segments.iter().map(|s| &s.meta)
    }

    /// Memtable size in bytes.
    pub fn memtable_bytes(&self) -> usize {
        self.memtable.bytes()
    }

    /// Applies a committed command at `index` to the in-memory state.
    pub fn apply(&mut self, index: LogIndex, cmd: &Command) -> Result<()> {
        if index != self.applied.next() && !(self.applied == LogIndex(0) && index >= LogIndex(1)) {
            return Err(Error::Internal(format!(
                "apply {index} after {}",
                self.applied
            )));
        }
        match cmd {
            Command::Upsert(docs) => {
                for d in docs {
                    let mut d = d.clone();
                    d.validate(&self.manifest.schema)?;
                    self.mask_in_segments(d.id);
                    self.memtable.upsert(d, index);
                }
            }
            Command::Delete(ids) => {
                for id in ids {
                    self.mask_in_segments(*id);
                    self.memtable.delete(*id, index);
                }
            }
        }
        self.applied = index;
        Ok(())
    }

    fn mask_in_segments(&mut self, id: DocId) {
        for s in &mut self.segments {
            if let Some(row) = s.docs.row_of(id)
                && s.deletions.set(row)
            {
                s.deletions_dirty = true;
            }
        }
    }

    /// Single-node write path: append, sync, apply, and flush if the memtable is full.
    pub async fn write(&mut self, cmd: &Command) -> Result<LogIndex> {
        let index = self.log.next_index();
        let entry = LogEntry {
            index,
            term: Term(1),
            payload: cmd.to_bytes(),
        };
        self.log.append(std::slice::from_ref(&entry)).await?;
        self.log.sync().await?;
        self.apply(index, cmd)?;
        if self.memtable.bytes() >= self.cfg.memtable_max_bytes {
            self.flush().await?;
        }
        Ok(index)
    }

    /// Point read honoring the memtable, tombstones and deletion sets.
    pub async fn get(&self, id: DocId) -> Result<Option<Document>> {
        match self.memtable.get(id) {
            Some(Some(d)) => return Ok(Some(d.clone())),
            Some(None) => return Ok(None),
            None => {}
        }
        for s in self.segments.iter().rev() {
            if let Some(row) = s.docs.row_of(id) {
                if s.deletions.contains(row) {
                    continue;
                }
                return s.docs.read_doc(&s.reader, row).await.map(Some);
            }
        }
        Ok(None)
    }

    async fn persist_dirty_deletions(&mut self) -> Result<()> {
        for s in &mut self.segments {
            if s.deletions_dirty {
                ManifestStore::new(self.rt.clone(), del_path(&self.dir, s.meta.id))
                    .store(&s.deletions)
                    .await?;
                s.deletions_dirty = false;
            }
        }
        Ok(())
    }

    /// Turns the memtable into a segment and publishes it. No-op when nothing is pending.
    pub async fn flush(&mut self) -> Result<()> {
        let Some((_, last)) = self.memtable.log_range() else {
            return Ok(());
        };
        self.persist_dirty_deletions().await?;
        let docs = self.memtable.sorted_docs();
        let mut new_manifest = self.manifest.clone();
        let mut new_segment = None;
        if !docs.is_empty() {
            let id = SegmentId(self.manifest.next_segment_id);
            let path = seg_path(&self.dir, id);
            let mut w = SegmentWriter::create(self.rt.clone(), &path).await?;
            write_columns(&mut w, &self.manifest.schema, &docs).await?;
            let (file_len, file_hash) = w.finish().await?;
            let meta = SegmentMeta {
                id,
                doc_count: docs.len() as u32,
                log_last: last,
                file_len,
                file_hash,
            };
            let reader = SegmentReader::open(self.rt.clone(), &path).await?;
            let docstore = DocStore::open(&reader).await?;
            new_manifest.segments.push(meta.clone());
            new_manifest.next_segment_id += 1;
            new_segment = Some(OpenSegment {
                meta: meta.clone(),
                reader,
                docs: docstore,
                deletions: DeletionSet::new(meta.doc_count),
                deletions_dirty: false,
            });
        }
        new_manifest.applied_index = last;
        self.manifest_store.store(&new_manifest).await?;
        self.manifest = new_manifest;
        if let Some(s) = new_segment {
            self.segments.push(s);
        }
        self.memtable.clear();
        self.log.truncate_prefix(last.next()).await?;
        Ok(())
    }

    /// Number of live documents across memtable and segments (walks deletion sets).
    pub fn approx_live_docs(&self) -> u64 {
        let seg: u64 = self
            .segments
            .iter()
            .map(|s| (s.meta.doc_count - s.deletions.count()) as u64)
            .sum();
        seg + self.memtable.len() as u64
    }

    /// Runtime handle.
    pub fn runtime(&self) -> &R {
        &self.rt
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use cairn_core::{FieldDef, FieldKind, HashMap, Metric, NodeId, SeededRng, Value};
    use cairn_sim::{SimConfig, Simulation};
    use std::cell::RefCell;
    use std::rc::Rc;

    fn schema() -> Schema {
        Schema::new(vec![
            FieldDef {
                name: "img".into(),
                kind: FieldKind::Vector {
                    dims: 4,
                    metric: Metric::L2,
                },
            },
            FieldDef {
                name: "title".into(),
                kind: FieldKind::Text,
            },
            FieldDef {
                name: "year".into(),
                kind: FieldKind::I64,
            },
            FieldDef {
                name: "tags".into(),
                kind: FieldKind::Set,
            },
            FieldDef {
                name: "ok".into(),
                kind: FieldKind::Bool,
            },
            FieldDef {
                name: "raw".into(),
                kind: FieldKind::Blob,
            },
        ])
        .unwrap()
    }

    fn doc(id: u64, salt: u64) -> Document {
        let f = (id ^ salt) as f32;
        let mut d = Document::new(DocId(id), 6)
            .set(0, Value::Vector(vec![f, f + 1.0, -f, 0.5]))
            .set(2, Value::I64((id as i64) * 3 - salt as i64))
            .set(
                3,
                Value::Set(vec![format!("t{}", id % 3), format!("s{}", salt % 2)]),
            )
            .set(4, Value::Bool(id % 2 == 0));
        if salt % 3 != 0 {
            d = d.set(1, Value::Text(format!("title {id} v{salt}")));
        }
        if salt % 2 == 0 {
            d = d.set(
                5,
                Value::Blob(Bytes::from(vec![salt as u8; (id % 7) as usize])),
            );
        }
        d.validate(&schema()).unwrap();
        d
    }

    fn small_cfg() -> StoreConfig {
        StoreConfig {
            memtable_max_bytes: 2000,
            log: LogConfig {
                max_file_bytes: 4096,
            },
        }
    }

    #[test]
    fn upsert_delete_get_flush_reopen() {
        let (sim, mut ex) = Simulation::new(1, SimConfig::default());
        let rt = sim.runtime(NodeId(1), &ex.handle());
        ex.block_on(async move {
            let mut st = Store::open(rt.clone(), "shard0", schema(), small_cfg())
                .await
                .unwrap();
            for i in 1..=50u64 {
                st.write(&Command::Upsert(vec![doc(i, 1)])).await.unwrap();
            }
            assert!(st.segments().count() >= 2, "flushes expected");
            st.write(&Command::Delete(vec![DocId(3), DocId(40)]))
                .await
                .unwrap();
            st.write(&Command::Upsert(vec![doc(7, 2)])).await.unwrap();
            assert_eq!(st.get(DocId(3)).await.unwrap(), None);
            assert_eq!(st.get(DocId(40)).await.unwrap(), None);
            assert_eq!(st.get(DocId(7)).await.unwrap(), Some(doc(7, 2)));
            assert_eq!(st.get(DocId(8)).await.unwrap(), Some(doc(8, 1)));
            assert_eq!(st.get(DocId(999)).await.unwrap(), None);
            st.flush().await.unwrap();
            assert_eq!(st.get(DocId(7)).await.unwrap(), Some(doc(7, 2)));
            assert_eq!(st.get(DocId(3)).await.unwrap(), None);
            let applied = st.applied_index();
            drop(st);
            let st = Store::open(rt.clone(), "shard0", schema(), small_cfg())
                .await
                .unwrap();
            assert_eq!(st.applied_index(), applied);
            assert_eq!(st.get(DocId(7)).await.unwrap(), Some(doc(7, 2)));
            assert_eq!(st.get(DocId(3)).await.unwrap(), None);
            assert_eq!(st.get(DocId(50)).await.unwrap(), Some(doc(50, 1)));
            assert_eq!(st.approx_live_docs(), 48);
            assert!(
                Store::open(rt.clone(), "shard0", Schema::default(), small_cfg())
                    .await
                    .is_err()
            );
        });
    }

    /// Model: the sequence of commands issued; `acked` = count of commands whose write completed.
    /// After each crash and reopen, the store must equal the model replayed to some prefix that
    /// is at least `acked`.
    fn crash_round(seed: u64) {
        let mut cfg = SimConfig::default();
        cfg.disk.persist_unsynced_prob = 0.5;
        cfg.disk.torn_writes = true;
        let (sim, mut ex) = Simulation::new(seed, cfg);
        let node = NodeId(1);
        let issued: Rc<RefCell<Vec<Command>>> = Rc::new(RefCell::new(Vec::new()));
        let acked: Rc<RefCell<usize>> = Rc::new(RefCell::new(0));
        let mut rng = sim.rng("workload");
        for round in 0..5 {
            let rt = sim.runtime(node, &ex.handle());
            let (iss, ack) = (issued.clone(), acked.clone());
            let round_seed = rng.next_u64();
            let rt_task = rt.clone();
            rt.spawn(async move {
                let rt = rt_task;
                let mut st = Store::open(rt.clone(), "s", schema(), small_cfg())
                    .await
                    .unwrap();
                // Recovery check.
                let applied = st.applied_index().get() as usize;
                let n_acked = *ack.borrow();
                assert!(
                    applied >= n_acked,
                    "seed {seed} round {round}: applied {applied} < acked {n_acked}"
                );
                let cmds: Vec<Command> = iss.borrow().clone();
                assert!(
                    applied <= cmds.len(),
                    "seed {seed} round {round}: applied beyond issued"
                );
                let mut model: HashMap<DocId, Option<Document>> = HashMap::default();
                for c in &cmds[..applied] {
                    match c {
                        Command::Upsert(ds) => {
                            for d in ds {
                                model.insert(d.id, Some(d.clone()));
                            }
                        }
                        Command::Delete(ids) => {
                            for id in ids {
                                model.insert(*id, None);
                            }
                        }
                    }
                }
                for id in 1..=30u64 {
                    let want = model.get(&DocId(id)).cloned().flatten();
                    let got = st.get(DocId(id)).await.unwrap();
                    assert_eq!(got, want, "seed {seed} round {round} doc {id}");
                }
                // The unacknowledged tail beyond `applied` is gone; forget it in the model too.
                iss.borrow_mut().truncate(applied);
                let mut r = SeededRng::from_seed(round_seed);
                for k in 0..30 {
                    let cmd = if r.chance(0.25) {
                        Command::Delete(vec![DocId(1 + r.below(30))])
                    } else {
                        let n = 1 + r.below(3);
                        Command::Upsert(
                            (0..n)
                                .map(|_| doc(1 + r.below(30), round as u64 * 100 + k))
                                .collect(),
                        )
                    };
                    iss.borrow_mut().push(cmd.clone());
                    st.write(&cmd).await.unwrap();
                    *ack.borrow_mut() = iss.borrow().len();
                }
                if r.chance(0.5) {
                    st.flush().await.unwrap();
                }
            });
            let delay = cairn_core::Duration::from_micros(rng.below(60_000));
            let h = ex.handle();
            ex.block_on(h.sleep(delay));
            sim.crash(node, &mut ex);
        }
    }

    #[test]
    fn crash_recovery_matches_model() {
        for seed in 0..120u64 {
            crash_round(seed);
        }
    }

    #[test]
    fn real_disk_store() {
        use cairn_runtime::Executor;
        use cairn_runtime::blocking::{BlockingReactor, RealRuntime};
        let dir = std::env::temp_dir().join(format!("cairn-store-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut ex = Executor::new(BlockingReactor::new());
        let rt = RealRuntime::new(ex.handle(), &dir, NodeId(1)).unwrap();
        ex.block_on(async move {
            let mut st = Store::open(rt.clone(), "s", schema(), small_cfg())
                .await
                .unwrap();
            for i in 1..=40u64 {
                st.write(&Command::Upsert(vec![doc(i, 5)])).await.unwrap();
            }
            st.write(&Command::Delete(vec![DocId(2)])).await.unwrap();
            drop(st);
            let st = Store::open(rt.clone(), "s", schema(), small_cfg())
                .await
                .unwrap();
            assert_eq!(st.get(DocId(2)).await.unwrap(), None);
            assert_eq!(st.get(DocId(39)).await.unwrap(), Some(doc(39, 5)));
        });
        let _ = std::fs::remove_dir_all(&dir);
    }
}
