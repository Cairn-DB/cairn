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
    /// Compact when more than this many segments exist.
    pub max_segments: usize,
    /// Rewrite a segment once this fraction of its rows is deleted.
    pub max_deleted_fraction: f64,
    /// Tiered merging: when non-zero, merge the longest run of adjacent segments whose live
    /// rows add up to at most this many, once the run has `min_merge` segments. Each row is
    /// rewritten about once per size tier instead of once per pairwise merge, and a shard
    /// converges to a few large segments (fewer per-query segment searches).
    pub target_segment_rows: u32,
    /// Shortest run the tiered policy merges (below `max_segments`).
    pub min_merge: usize,
}

impl Default for StoreConfig {
    fn default() -> Self {
        StoreConfig {
            memtable_max_bytes: 64 << 20,
            log: LogConfig::default(),
            max_segments: 8,
            max_deleted_fraction: 0.3,
            target_segment_rows: 0,
            min_merge: 4,
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

/// Builds the index sections of a segment from its documents (rows in the given order).
/// Implemented in `cairn-index`; the store only writes what it returns.
pub trait SegmentIndexer {
    /// Extra sections `(name, bytes)` for a segment holding `docs`.
    fn sections(&self, schema: &Schema, docs: &[&Document]) -> Result<Vec<(String, Vec<u8>)>>;
}

/// Indexer that adds nothing (row store only).
pub struct NoIndexer;

impl SegmentIndexer for NoIndexer {
    fn sections(&self, _schema: &Schema, _docs: &[&Document]) -> Result<Vec<(String, Vec<u8>)>> {
        Ok(Vec::new())
    }
}

/// Compaction outputs carry this bit in their id, so they never collide with flush ids (which
/// are log indexes, ADR 0016).
pub const COMPACT_ID_BIT: u64 = 1 << 62;
/// Bits 40..62 of a compaction id hold the replica's namespace (its node id): compactions are
/// local, so two replicas' outputs with the same id would hold different rows, and a snapshot
/// would replace one with the other under a name the old manifest still references.
const COMPACT_NS_SHIFT: u32 = 40;
const COMPACT_COUNTER_MASK: u64 = (1 << COMPACT_NS_SHIFT) - 1;

/// A frozen memtable waiting to become a segment (ADR 0016): cut by a `FlushBegin` entry (or a
/// local flush), readable until published, published strictly in order.
struct PendingFlush {
    id: SegmentId,
    /// Last log index folded into the segment (the `FlushBegin` index).
    last: LogIndex,
    mem: Memtable,
    /// Ids deleted or replaced after the freeze: masked in the segment when it is published.
    masked: cairn_core::HashSet<DocId>,
    /// `(len, hash)` announced by the leader's `FlushCommit`.
    commit: Option<(u64, u64)>,
    /// `(len, hash)` of the segment file present locally (built here or fetched).
    file: Option<(u64, u64)>,
}

/// What the replica must do for one frozen memtable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingInfo {
    /// Segment id.
    pub id: SegmentId,
    /// `(len, hash)` from the leader's commit, if applied.
    pub commit: Option<(u64, u64)>,
    /// Whether the segment file is present locally.
    pub has_file: bool,
    /// Whether the frozen memtable holds no rows (published without a file).
    pub empty: bool,
}

/// A flush in progress: the frozen documents to turn into a segment.
pub struct FlushJob {
    /// Live documents of the frozen memtable, sorted by id.
    pub docs: Vec<Document>,
    /// Last log index folded into the segment.
    pub last: LogIndex,
    /// Id of the segment to create.
    pub id: SegmentId,
    /// Store generation when the job began (see [`Store::job_is_current`]).
    pub generation: u64,
}

/// A compaction in progress.
pub struct CompactJob {
    /// Segments being merged (adjacent, in order).
    pub inputs: Vec<SegmentId>,
    /// Their live rows, sorted by id.
    pub docs: Vec<Document>,
    /// Max log index of the inputs.
    pub log_last: LogIndex,
    /// Id of the merged segment.
    pub id: SegmentId,
    /// Store generation when the job began (see [`Store::job_is_current`]).
    pub generation: u64,
}

/// Read-only view of one open segment for the query layer.
pub struct SegmentView<'a, R: Runtime> {
    /// Manifest entry.
    pub meta: &'a SegmentMeta,
    /// Container reader.
    pub reader: &'a SegmentReader<R>,
    /// Row store (doc ids).
    pub docs: &'a DocStore,
    /// Current deletions.
    pub deletions: &'a DeletionSet,
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
    indexer: Box<dyn SegmentIndexer>,
    /// Frozen memtables waiting to be published, oldest first (still readable).
    pending: std::collections::VecDeque<PendingFlush>,
    /// Ids deleted or replaced while a compaction was building; masked in the merged segment.
    masked_during_build: cairn_core::HashSet<DocId>,
    /// Whether a compaction job is in progress.
    job_active: bool,
    /// Namespace of this replica's compaction ids (see `COMPACT_NS_SHIFT`).
    compact_ns: u64,
    /// Bumped whenever the store's state is replaced (snapshot install): a build that began
    /// before must be discarded, or it would publish pre-snapshot rows under a segment id the
    /// snapshot may already use.
    generation: u64,
    /// Bumped whenever the memtable changes.
    memtable_version: u64,
    /// Bumped whenever the segment list changes.
    segments_version: u64,
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
        Self::open_with_limit(rt, dir, schema, cfg, None).await
    }

    /// Like [`Store::open`], but replays the log only up to `replay_limit` (inclusive). A
    /// replicated shard passes its persisted commit index: entries beyond it may be uncommitted
    /// and get truncated later, so they must not reach the state machine.
    pub async fn open_with_limit(
        rt: R,
        dir: &str,
        schema: Schema,
        cfg: StoreConfig,
        replay_limit: Option<LogIndex>,
    ) -> Result<Self> {
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
                || name.ends_with(".fetch")
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
            indexer: Box::new(NoIndexer),
            pending: std::collections::VecDeque::new(),
            masked_during_build: cairn_core::HashSet::default(),
            job_active: false,
            compact_ns: 0,
            generation: 0,
            memtable_version: 0,
            segments_version: 0,
        };
        store.applied = store.manifest.applied_index;
        // Replay the log after the manifest's applied index.
        if let Some(mut last) = store.log.last_index()
            && last > store.applied
            && store.log.first_index() <= store.applied.next()
        {
            if let Some(limit) = replay_limit {
                last = last.min(limit);
            }
            // A log starting beyond applied + 1 (crash during a snapshot install) is ignored; the
            // leader will ship the snapshot again.
            let from = store.applied.next().max(store.log.first_index());
            let entries = if last >= from {
                store.log.read_range(from, last).await?
            } else {
                Vec::new()
            };
            for e in entries {
                let cmd = Command::from_bytes(&e.payload)?;
                store.apply(e.index, &cmd)?;
            }
        }
        Ok(store)
    }

    /// Sets the namespace of this replica's compaction ids (a replicated shard passes its node
    /// id; at most 2^22 - 1).
    pub fn set_compaction_namespace(&mut self, ns: u64) {
        self.compact_ns = ns & ((1 << (62 - COMPACT_NS_SHIFT)) - 1);
    }

    /// Abandons the compaction in progress (its output is not written): a replica installing a
    /// snapshot must not change its segments meanwhile.
    pub fn abandon_compact(&mut self) {
        self.job_active = false;
        self.masked_during_build.clear();
    }

    /// Installs the indexer used by future flushes and compactions.
    pub fn set_indexer(&mut self, indexer: Box<dyn SegmentIndexer>) {
        self.indexer = indexer;
    }

    /// The memtable.
    pub fn memtable(&self) -> &Memtable {
        &self.memtable
    }

    /// Changes whenever the memtable changes.
    pub fn memtable_version(&self) -> u64 {
        self.memtable_version
    }

    /// Changes whenever a segment is added or removed.
    pub fn segments_version(&self) -> u64 {
        self.segments_version
    }

    /// View of the segment with `id`.
    pub fn segment(&self, id: SegmentId) -> Option<SegmentView<'_, R>> {
        self.segments
            .iter()
            .find(|s| s.meta.id == id)
            .map(|s| SegmentView {
                meta: &s.meta,
                reader: &s.reader,
                docs: &s.docs,
                deletions: &s.deletions,
            })
    }

    /// Views of every segment, oldest first.
    pub fn segment_views(&self) -> impl Iterator<Item = SegmentView<'_, R>> {
        self.segments.iter().map(|s| SegmentView {
            meta: &s.meta,
            reader: &s.reader,
            docs: &s.docs,
            deletions: &s.deletions,
        })
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
            Command::Noop => self.memtable.note_index(index),
            Command::FlushBegin => {
                self.memtable.note_index(index);
                self.freeze(SegmentId(index.get()), index);
            }
            Command::FlushCommit { id, len, hash } => {
                self.memtable.note_index(index);
                if let Some(p) = self.pending.iter_mut().find(|p| p.id == *id)
                    && p.commit.is_none()
                {
                    p.commit = Some((*len, *hash));
                }
            }
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
        self.memtable_version += 1;
        Ok(())
    }

    fn mask_in_segments(&mut self, id: DocId) {
        if self.job_active {
            self.masked_during_build.insert(id);
        }
        // Frozen memtables stay exactly as cut (ADR 0016): a segment built from one, by any
        // replica at any time, holds the same rows, and a lagging follower that installs it
        // never sees the effect of an entry it has not applied yet. Newer layers (the active
        // memtable, newer freezes) shadow the changed rows for reads; publication masks them.
        for p in &mut self.pending {
            p.masked.insert(id);
        }
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
        for p in self.pending.iter().rev() {
            match p.mem.get(id) {
                Some(Some(d)) => return Ok(Some(d.clone())),
                Some(None) => return Ok(None),
                None => {}
            }
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

    /// Documents visible in memory for queries: the active memtable plus the frozen ones
    /// (the newest version of each id; a tombstone in a newer layer hides older rows).
    pub fn memtable_docs(&self) -> Vec<Document> {
        let mut v: Vec<Document> = self.memtable.docs().cloned().collect();
        let layers: Vec<&Memtable> = self.pending.iter().rev().map(|p| &p.mem).collect();
        for (i, layer) in layers.iter().enumerate() {
            v.extend(
                layer
                    .docs()
                    .filter(|d| {
                        self.memtable.get(d.id).is_none()
                            && layers[..i].iter().all(|newer| newer.get(d.id).is_none())
                    })
                    .cloned(),
            );
        }
        v
    }

    /// Freezes the active memtable under `id` (last folded index `last`).
    fn freeze(&mut self, id: SegmentId, last: LogIndex) {
        let mem = std::mem::take(&mut self.memtable);
        self.pending.push_back(PendingFlush {
            id,
            last,
            mem,
            masked: cairn_core::HashSet::default(),
            commit: None,
            file: None,
        });
        self.memtable_version += 1;
    }

    /// Frozen memtables waiting to be published, oldest first.
    pub fn pending_flushes(&self) -> Vec<PendingInfo> {
        self.pending
            .iter()
            .map(|p| PendingInfo {
                id: p.id,
                commit: p.commit,
                has_file: p.file.is_some(),
                empty: p.mem.docs().next().is_none(),
            })
            .collect()
    }

    /// The build job of the frozen memtable `id` (its rows, sorted by id), if still pending.
    pub fn flush_job(&self, id: SegmentId) -> Option<FlushJob> {
        let p = self.pending.iter().find(|p| p.id == id)?;
        Some(FlushJob {
            docs: p.mem.sorted_docs().into_iter().cloned().collect(),
            last: p.last,
            id,
            generation: self.generation,
        })
    }

    /// Writes the segment file of a flush job built here (not yet published). Returns its
    /// `(len, hash)`, or `None` when the freeze is no longer pending (snapshot installed).
    pub async fn write_flush_file(
        &mut self,
        job: &FlushJob,
        sections: Vec<(String, Vec<u8>)>,
    ) -> Result<Option<(u64, u64)>> {
        if !self.job_is_current(job.generation)
            || !self
                .pending
                .iter()
                .any(|p| p.id == job.id && p.file.is_none())
        {
            return Ok(None);
        }
        let refs: Vec<&Document> = job.docs.iter().collect();
        let mut w = SegmentWriter::create(self.rt.clone(), &seg_path(&self.dir, job.id)).await?;
        write_columns(&mut w, &self.manifest.schema, &refs).await?;
        for (name, bytes) in &sections {
            w.add_section(name, bytes).await?;
        }
        let (len, hash) = w.finish().await?;
        if let Some(p) = self.pending.iter_mut().find(|p| p.id == job.id) {
            p.file = Some((len, hash));
        }
        Ok(Some((len, hash)))
    }

    /// Staging name (relative, before the `.fetch` suffix) of a leader-built segment being
    /// fetched: distinct from the snapshot fetch's, which may run for the same id at the same
    /// time.
    pub fn ship_staging(id: SegmentId) -> String {
        format!("segs/{:016x}.seg.ship", id.get())
    }

    /// Installs a segment file fetched from the leader (streamed to
    /// `<Store::ship_staging(id)>.fetch` and synced)
    /// for the pending freeze `id`, after checking its length and body hash against the
    /// commit. Returns whether it was accepted (on `false` the caller builds locally).
    pub async fn install_fetched_flush(&mut self, id: SegmentId) -> Result<bool> {
        let Some((len, hash)) = self
            .pending
            .iter()
            .find(|p| p.id == id && p.file.is_none())
            .and_then(|p| p.commit)
        else {
            return Ok(false);
        };
        let path = seg_path(&self.dir, id);
        let disk = self.rt.disk();
        disk.rename(
            &format!("{}/{}.fetch", self.dir, Self::ship_staging(id)),
            &path,
        )
        .await?;
        let ok = match SegmentReader::open(self.rt.clone(), &path).await {
            Ok(r) => r.len() == len && r.file_hash() == hash && r.verify_file_hash().await?,
            Err(_) => false,
        };
        if !ok {
            let _ = disk.remove(&path).await;
            return Ok(false);
        }
        if let Some(p) = self.pending.iter_mut().find(|p| p.id == id) {
            p.file = Some((len, hash));
        }
        Ok(true)
    }

    /// Publishes, in order, every frozen memtable at the head of the queue whose segment file is
    /// present (or which holds no rows). Returns how many were published.
    pub async fn publish_ready(&mut self) -> Result<usize> {
        let mut n = 0;
        while self
            .pending
            .front()
            .is_some_and(|p| p.file.is_some() || p.mem.docs().next().is_none())
        {
            self.persist_dirty_deletions().await?;
            let p = self.pending.pop_front().expect("checked");
            let mut new_manifest = self.manifest.clone();
            let mut new_segment = None;
            if let Some((file_len, file_hash)) = p.file {
                let path = seg_path(&self.dir, p.id);
                let reader = SegmentReader::open(self.rt.clone(), &path).await?;
                let docstore = DocStore::open(&reader).await?;
                let doc_count = docstore.doc_count();
                let meta = SegmentMeta {
                    id: p.id,
                    doc_count,
                    log_last: p.last,
                    file_len,
                    file_hash,
                };
                let mut deletions = DeletionSet::new(doc_count);
                let mut dirty = false;
                for id in &p.masked {
                    if let Some(row) = docstore.row_of(*id) {
                        deletions.set(row);
                        dirty = true;
                    }
                }
                if dirty {
                    ManifestStore::new(self.rt.clone(), del_path(&self.dir, p.id))
                        .store(&deletions)
                        .await?;
                }
                new_manifest.segments.push(meta.clone());
                new_segment = Some(OpenSegment {
                    meta,
                    reader,
                    docs: docstore,
                    deletions,
                    deletions_dirty: false,
                });
            }
            new_manifest.applied_index = p.last.max(new_manifest.applied_index);
            self.manifest_store.store(&new_manifest).await?;
            self.manifest = new_manifest;
            if let Some(seg) = new_segment {
                self.segments.push(seg);
            }
            self.memtable_version += 1;
            self.segments_version += 1;
            self.log.truncate_prefix(p.last.next()).await?;
            n += 1;
        }
        Ok(n)
    }

    /// Whether a flush or compaction job is in progress.
    pub fn job_active(&self) -> bool {
        self.job_active
    }

    /// Local flush (single-node path, no log entry): freezes the memtable under the id of its
    /// last log index and returns the build job. `None` when the memtable is empty or a freeze
    /// is already pending.
    pub fn begin_flush(&mut self) -> Option<FlushJob> {
        if !self.pending.is_empty() {
            return None;
        }
        let (_, last) = self.memtable.log_range()?;
        self.freeze(SegmentId(last.get()), last);
        self.flush_job(SegmentId(last.get()))
    }

    /// Builds the index sections for `docs` (pure CPU; run through `Runtime::offload`).
    pub fn build_sections(
        schema: &Schema,
        docs: &[Document],
        indexer: &dyn SegmentIndexer,
    ) -> Result<Vec<(String, Vec<u8>)>> {
        let refs: Vec<&Document> = docs.iter().collect();
        indexer.sections(schema, &refs)
    }

    /// Whether a job that began at `generation` still applies (no snapshot was installed since).
    pub fn job_is_current(&self, generation: u64) -> bool {
        generation == self.generation
    }

    /// Writes the segment of a local flush job and publishes whatever is ready, in order. A job
    /// that began before a snapshot was installed is discarded.
    pub async fn finish_flush(
        &mut self,
        job: FlushJob,
        sections: Vec<(String, Vec<u8>)>,
    ) -> Result<()> {
        if job.docs.is_empty() {
            // Nothing to write: the freeze publishes without a file.
        } else if self.write_flush_file(&job, sections).await?.is_none() {
            return Ok(());
        }
        self.publish_ready().await?;
        Ok(())
    }

    /// Synchronous convenience: flush and compact inline with the installed indexer.
    pub async fn flush(&mut self) -> Result<()> {
        if let Some(job) = self.begin_flush() {
            let sections =
                Self::build_sections(&self.manifest.schema, &job.docs, self.indexer.as_ref())?;
            self.finish_flush(job, sections).await?;
        }
        while let Some(job) = self.begin_compact().await? {
            let sections =
                Self::build_sections(&self.manifest.schema, &job.docs, self.indexer.as_ref())?;
            self.finish_compact(job, sections).await?;
        }
        Ok(())
    }

    /// Picks the next compaction per policy and reads its inputs. `None` when nothing to do or a
    /// job is active.
    pub async fn begin_compact(&mut self) -> Result<Option<CompactJob>> {
        if self.job_active {
            return Ok(None);
        }
        let n = self.segments.len();
        let stale = self.segments.iter().position(|s| {
            s.meta.doc_count > 0
                && f64::from(s.deletions.count()) / f64::from(s.meta.doc_count)
                    > self.cfg.max_deleted_fraction
        });
        let live = |s: &OpenSegment<R>| u64::from(s.meta.doc_count - s.deletions.count());
        let run = self.tiered_run(n, &live);
        let ids: Vec<SegmentId> = if let Some(i) = stale {
            vec![self.segments[i].meta.id]
        } else if let Some((first, len)) = run {
            self.segments[first..first + len]
                .iter()
                .map(|s| s.meta.id)
                .collect()
        } else if n > self.cfg.max_segments {
            let (i, _) = (0..n - 1)
                .map(|i| (i, live(&self.segments[i]) + live(&self.segments[i + 1])))
                .min_by_key(|(_, rows)| *rows)
                .expect("n > 1");
            vec![self.segments[i].meta.id, self.segments[i + 1].meta.id]
        } else {
            return Ok(None);
        };
        self.begin_compact_ids(&ids).await.map(Some)
    }

    /// The run of adjacent segments the tiered policy would merge: the longest run whose live
    /// rows fit `target_segment_rows` (ties: fewest rows), if it has at least `min_merge`
    /// segments, or at least two when the shard holds more than `max_segments`.
    fn tiered_run(
        &self,
        n: usize,
        live: &dyn Fn(&OpenSegment<R>) -> u64,
    ) -> Option<(usize, usize)> {
        let target = u64::from(self.cfg.target_segment_rows);
        if target == 0 || n < 2 {
            return None;
        }
        let mut best: Option<(usize, usize, u64)> = None;
        for first in 0..n {
            let mut total = 0u64;
            let mut len = 0;
            while first + len < n && total + live(&self.segments[first + len]) <= target {
                total += live(&self.segments[first + len]);
                len += 1;
            }
            let better = match best {
                None => true,
                Some((_, bl, bt)) => len > bl || (len == bl && total < bt),
            };
            if len >= 2 && better {
                best = Some((first, len, total));
            }
        }
        let (first, len, _) = best?;
        let need = if n > self.cfg.max_segments {
            2
        } else {
            self.cfg.min_merge.max(2)
        };
        (len >= need).then_some((first, len))
    }

    /// Reads the live rows of the given adjacent segments into a compaction job.
    pub async fn begin_compact_ids(&mut self, ids: &[SegmentId]) -> Result<CompactJob> {
        if self.job_active {
            return Err(Error::Internal("a build job is already active".into()));
        }
        let first = self
            .segments
            .iter()
            .position(|s| s.meta.id == ids[0])
            .ok_or_else(|| Error::InvalidRequest(format!("unknown segment {}", ids[0])))?;
        for (k, id) in ids.iter().enumerate() {
            if self.segments.get(first + k).map(|s| s.meta.id) != Some(*id) {
                return Err(Error::InvalidRequest(
                    "segments to compact must be adjacent and in order".into(),
                ));
            }
        }
        let mut docs: Vec<Document> = Vec::new();
        let mut log_last = LogIndex(0);
        for s in &self.segments[first..first + ids.len()] {
            let dels = &s.deletions;
            docs.extend(s.docs.read_all(&s.reader, |row| dels.contains(row)).await?);
            log_last = log_last.max(s.meta.log_last);
        }
        docs.sort_by_key(|d| d.id);
        self.job_active = true;
        self.masked_during_build.clear();
        Ok(CompactJob {
            generation: self.generation,
            inputs: ids.to_vec(),
            docs,
            log_last,
            id: SegmentId(
                COMPACT_ID_BIT
                    | (self.compact_ns << COMPACT_NS_SHIFT)
                    | (self.manifest.next_segment_id & COMPACT_COUNTER_MASK),
            ),
        })
    }

    /// Publishes the merged segment and removes the inputs.
    pub async fn finish_compact(
        &mut self,
        job: CompactJob,
        sections: Vec<(String, Vec<u8>)>,
    ) -> Result<()> {
        if !self.job_is_current(job.generation) {
            return Ok(());
        }
        let first = self
            .segments
            .iter()
            .position(|s| s.meta.id == job.inputs[0])
            .ok_or_else(|| Error::Internal("compaction inputs vanished".into()))?;
        let range = first..first + job.inputs.len();
        let refs: Vec<&Document> = job.docs.iter().collect();
        let path = seg_path(&self.dir, job.id);
        let mut w = SegmentWriter::create(self.rt.clone(), &path).await?;
        write_columns(&mut w, &self.manifest.schema, &refs).await?;
        for (name, bytes) in &sections {
            w.add_section(name, bytes).await?;
        }
        let (file_len, file_hash) = w.finish().await?;
        let meta = SegmentMeta {
            id: job.id,
            doc_count: refs.len() as u32,
            log_last: job.log_last,
            file_len,
            file_hash,
        };
        let reader = SegmentReader::open(self.rt.clone(), &path).await?;
        let docstore = DocStore::open(&reader).await?;
        let mut deletions = DeletionSet::new(meta.doc_count);
        let mut dirty = false;
        for id in &self.masked_during_build {
            if let Some(row) = docstore.row_of(*id) {
                deletions.set(row);
                dirty = true;
            }
        }
        if dirty {
            ManifestStore::new(self.rt.clone(), del_path(&self.dir, job.id))
                .store(&deletions)
                .await?;
        }
        let mut new_manifest = self.manifest.clone();
        new_manifest.segments.splice(range.clone(), [meta.clone()]);
        new_manifest.next_segment_id = (job.id.get() & COMPACT_COUNTER_MASK) + 1;
        self.manifest_store.store(&new_manifest).await?;
        self.manifest = new_manifest;
        self.segments_version += 1;
        let removed: Vec<OpenSegment<R>> = self
            .segments
            .splice(
                range,
                [OpenSegment {
                    meta,
                    reader,
                    docs: docstore,
                    deletions,
                    deletions_dirty: false,
                }],
            )
            .collect();
        self.job_active = false;
        self.masked_during_build.clear();
        for s in removed {
            let disk = self.rt.disk();
            let _ = disk.remove(&seg_path(&self.dir, s.meta.id)).await;
            if disk.exists(&del_path(&self.dir, s.meta.id)).await? {
                disk.remove(&del_path(&self.dir, s.meta.id)).await?;
            }
        }
        Ok(())
    }

    /// Synchronous convenience: compacts the given segments inline.
    pub async fn compact(&mut self, ids: &[SegmentId]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let job = self.begin_compact_ids(ids).await?;
        let sections =
            Self::build_sections(&self.manifest.schema, &job.docs, self.indexer.as_ref())?;
        self.finish_compact(job, sections).await
    }

    /// The current manifest (a snapshot of everything up to `applied_index` of the manifest).
    pub fn manifest(&self) -> &ShardManifest {
        &self.manifest
    }

    /// Encoded manifest bytes, for shipping as a Raft snapshot.
    pub fn manifest_bytes(&self) -> bytes::Bytes {
        ManifestStore::<R>::encode(&self.manifest)
    }

    /// Files a snapshot receiver must fetch for `manifest`: segment and deletion files.
    pub fn snapshot_files(manifest: &ShardManifest) -> Vec<String> {
        let mut v = Vec::new();
        for s in &manifest.segments {
            v.push(format!("segs/{:016x}.seg", s.id.get()));
            v.push(format!("segs/{:016x}.del", s.id.get()));
        }
        v
    }

    /// Files a follower must fetch to install `manifest`: every deletion checkpoint, and the
    /// segment files it does not already hold with the same length and hash. Segments are
    /// immutable and named by id, so matching metadata means identical bytes; re-shipping them
    /// is what made catch-up copy the whole shard.
    pub fn snapshot_files_needed(&self, manifest: &ShardManifest) -> Vec<String> {
        let mut v = Vec::new();
        for s in &manifest.segments {
            let have = self.segments.iter().any(|o| {
                o.meta.id == s.id
                    && o.meta.file_len == s.file_len
                    && o.meta.file_hash == s.file_hash
            });
            if !have {
                v.push(format!("segs/{:016x}.seg", s.id.get()));
            }
            v.push(format!("segs/{:016x}.del", s.id.get()));
        }
        v
    }

    /// Reads up to `len` bytes at `offset` of a shard file (for shipping in chunks, without
    /// reading the whole file); `None` if the file does not exist. Returns the file length too.
    pub async fn read_file_range(
        &self,
        rel: &str,
        offset: u64,
        len: usize,
    ) -> Result<Option<(u64, bytes::Bytes)>> {
        let disk = self.rt.disk();
        let path = format!("{}/{rel}", self.dir);
        if !disk.exists(&path).await? {
            return Ok(None);
        }
        let f = disk.open(&path, cairn_core::OpenMode::Read).await?;
        let total = disk.len(&f).await?;
        let start = offset.min(total);
        let n = (total - start).min(len as u64) as usize;
        Ok(Some((total, disk.read_at(&f, start, n).await?)))
    }

    /// Writes one fetched chunk of `rel` into its staging file (`<rel>.fetch`); the first
    /// chunk (offset 0) truncates any leftover from an earlier attempt.
    pub async fn write_fetch_chunk(
        &self,
        rel: &str,
        offset: u64,
        data: bytes::Bytes,
    ) -> Result<()> {
        let disk = self.rt.disk();
        let path = format!("{}/{rel}.fetch", self.dir);
        let mode = if offset == 0 {
            cairn_core::OpenMode::CreateTruncate
        } else {
            cairn_core::OpenMode::CreateOrOpen
        };
        let f = disk.open(&path, mode).await?;
        disk.write_at(&f, offset, data).await
    }

    /// Makes a completely fetched staging file durable.
    pub async fn sync_fetched(&self, rel: &str) -> Result<()> {
        let disk = self.rt.disk();
        let f = disk
            .open(
                &format!("{}/{rel}.fetch", self.dir),
                cairn_core::OpenMode::ReadWrite,
            )
            .await?;
        disk.sync(&f).await
    }

    /// Reads a file of this shard (for shipping), relative to the shard directory.
    pub async fn read_file(&self, rel: &str) -> Result<Option<bytes::Bytes>> {
        let disk = self.rt.disk();
        let path = format!("{}/{rel}", self.dir);
        if !disk.exists(&path).await? {
            return Ok(None);
        }
        let f = disk.open(&path, cairn_core::OpenMode::Read).await?;
        let len = disk.len(&f).await?;
        Ok(Some(disk.read_at(&f, 0, len as usize).await?))
    }

    /// Replaces the whole shard state with a snapshot: `fetched` lists the files (relative
    /// paths, from [`Store::snapshot_files_needed`]) already streamed to their `.fetch` staging
    /// files with [`Store::write_fetch_chunk`] and synced; segments the node already held are
    /// reused. `manifest` is the encoded manifest. The log is reset to start after the manifest's applied index
    /// unless it already does.
    pub async fn install_snapshot(&mut self, manifest: &[u8], fetched: Vec<String>) -> Result<()> {
        let m: ShardManifest = ManifestStore::<R>::decode(manifest)?;
        if m.schema != self.manifest.schema {
            return Err(Error::Schema("snapshot schema differs".into()));
        }
        let disk = self.rt.disk();
        // Every segment must be present and match before anything changes: fetched ones in
        // their staging files, the others as local files (a local compaction or a publication
        // may have changed them since the fetch started). On failure the caller fetches again.
        for meta in &m.segments {
            let seg = format!("segs/{:016x}.seg", meta.id.get());
            let path = if fetched.contains(&seg) {
                format!("{}/{seg}.fetch", self.dir)
            } else {
                seg_path(&self.dir, meta.id)
            };
            let ok = disk.exists(&path).await?
                && match SegmentReader::open(self.rt.clone(), &path).await {
                    Ok(r) => r.len() == meta.file_len && r.file_hash() == meta.file_hash,
                    Err(_) => false,
                };
            if !ok {
                return Err(Error::Internal(format!(
                    "snapshot segment {} missing or different locally",
                    meta.id
                )));
            }
        }
        // A local deletion checkpoint is only valid for the local file it was written for:
        // when the segment file itself is replaced (same id, other content: ids of local
        // compactions and fallback builds are per replica) and the leader had no checkpoint to
        // send, the local one must go, or its row numbers would mask the wrong rows.
        for meta in &m.segments {
            let seg = format!("segs/{:016x}.seg", meta.id.get());
            let del = format!("segs/{:016x}.del", meta.id.get());
            if fetched.contains(&seg) && !fetched.contains(&del) {
                let path = del_path(&self.dir, meta.id);
                if disk.exists(&path).await? {
                    disk.remove(&path).await?;
                }
            }
        }
        // Fetched files were streamed to `<rel>.fetch` and synced; publish them by rename.
        for rel in fetched {
            let path = format!("{}/{rel}", self.dir);
            disk.rename(&format!("{path}.fetch"), &path).await?;
        }
        self.manifest_store.store(&m).await?;
        self.segments.clear();
        for meta in &m.segments {
            let reader =
                SegmentReader::open(self.rt.clone(), &seg_path(&self.dir, meta.id)).await?;
            let docs = DocStore::open(&reader).await?;
            let del_store = ManifestStore::new(self.rt.clone(), del_path(&self.dir, meta.id));
            let deletions = match del_store.load::<DeletionSet>().await? {
                Some(d) => d,
                None => DeletionSet::new(meta.doc_count),
            };
            self.segments.push(OpenSegment {
                meta: meta.clone(),
                reader,
                docs,
                deletions,
                deletions_dirty: false,
            });
        }
        self.manifest = m;
        self.memtable.clear();
        self.pending.clear();
        self.job_active = false;
        self.generation += 1;
        self.masked_during_build.clear();
        self.applied = self.manifest.applied_index;
        if self.log.first_index() != self.applied.next()
            || self.log.last_index().is_some_and(|l| l <= self.applied)
        {
            self.log.reset(self.applied.next()).await?;
        }
        self.memtable_version += 1;
        self.segments_version += 1;
        let referenced: Vec<String> = self
            .manifest
            .segments
            .iter()
            .flat_map(|s| {
                [
                    format!("{:016x}.seg", s.id.get()),
                    format!("{:016x}.del", s.id.get()),
                ]
            })
            .collect();
        for name in disk.list(&format!("{}/segs", self.dir)).await? {
            if !referenced.contains(&name) {
                disk.remove(&format!("{}/segs/{name}", self.dir)).await?;
            }
        }
        Ok(())
    }

    /// Number of live documents across memtable and segments (walks deletion sets).
    pub fn approx_live_docs(&self) -> u64 {
        let seg: u64 = self
            .segments
            .iter()
            .map(|s| (s.meta.doc_count - s.deletions.count()) as u64)
            .sum();
        let frozen: u64 = self.pending.iter().map(|p| p.mem.len() as u64).sum();
        seg + self.memtable.len() as u64 + frozen
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
            max_segments: 3,
            max_deleted_fraction: 0.3,
            target_segment_rows: 0,
            min_merge: 4,
        }
    }

    /// Segment row counts after writing `n` documents with the given tiered settings (and a
    /// `max_segments` high enough that the pairwise policy never triggers).
    fn tiered_run(seed: u64, n: u64, target: u32, min_merge: usize) -> Vec<u32> {
        let (sim, mut ex) = Simulation::new(seed, SimConfig::default());
        let rt = sim.runtime(NodeId(1), &ex.handle());
        ex.block_on(async move {
            let cfg = StoreConfig {
                max_segments: 1000,
                target_segment_rows: target,
                min_merge,
                ..small_cfg()
            };
            let mut st = Store::open(rt.clone(), "t", schema(), cfg.clone())
                .await
                .unwrap();
            for i in 1..=n {
                st.write(&Command::Upsert(vec![doc(i, 1)])).await.unwrap();
            }
            st.flush().await.unwrap();
            for i in 1..=n {
                assert_eq!(st.get(DocId(i)).await.unwrap(), Some(doc(i, 1)));
            }
            let rows: Vec<u32> = st.segments().map(|s| s.doc_count).collect();
            drop(st);
            let st = Store::open(rt.clone(), "t", schema(), cfg).await.unwrap();
            assert_eq!(st.approx_live_docs(), n);
            rows
        })
    }

    #[test]
    fn tiered_policy_merges_runs_up_to_the_target() {
        let plain = tiered_run(9, 200, 0, 4);
        assert!(plain.len() > 8, "many flush-sized segments: {plain:?}");
        let flush_rows = *plain.iter().max().unwrap();
        // A target of three flushes: runs of 2-3 segments merge, nothing grows past the target.
        let target = 3 * flush_rows;
        let tiered = tiered_run(9, 200, target, 2);
        assert!(tiered.len() < plain.len(), "{tiered:?} vs {plain:?}");
        assert!(tiered.iter().all(|&r| r <= target), "{tiered:?} > {target}");
        assert_eq!(tiered.iter().sum::<u32>(), 200);
        // A target above the data: everything converges to one segment.
        assert_eq!(tiered_run(9, 200, 10_000, 2), vec![200]);
        // min_merge above the number of segments: the tiered policy never fires.
        assert_eq!(tiered_run(9, 200, 10_000, 1000).len(), plain.len());
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
            assert!(
                st.segments().count() <= 3,
                "compaction policy bounds the segment count"
            );
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
                        Command::Noop | Command::FlushBegin | Command::FlushCommit { .. } => {}
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

    #[test]
    fn compaction_drops_deleted_rows_and_removes_old_files() {
        let (sim, mut ex) = Simulation::new(5, SimConfig::default());
        let rt = sim.runtime(NodeId(1), &ex.handle());
        ex.block_on(async move {
            let mut st = Store::open(rt.clone(), "c", schema(), small_cfg())
                .await
                .unwrap();
            for i in 1..=60u64 {
                st.write(&Command::Upsert(vec![doc(i, 3)])).await.unwrap();
            }
            st.flush().await.unwrap();
            let before: Vec<SegmentId> = st.segments().map(|s| s.id).collect();
            // Delete half of everything: every segment crosses the deleted fraction.
            st.write(&Command::Delete(
                (1..=60).filter(|i| i % 2 == 0).map(DocId).collect(),
            ))
            .await
            .unwrap();
            st.flush().await.unwrap();
            let after: Vec<SegmentId> = st.segments().map(|s| s.id).collect();
            assert!(
                before.iter().all(|id| !after.contains(id)),
                "all stale segments rewritten"
            );
            let rows: u32 = st.segments().map(|s| s.doc_count).sum();
            assert_eq!(rows, 30);
            assert_eq!(st.approx_live_docs(), 30);
            for i in 1..=60u64 {
                let want = if i % 2 == 0 { None } else { Some(doc(i, 3)) };
                assert_eq!(st.get(DocId(i)).await.unwrap(), want);
            }
            let files = rt.disk().list("c/segs").await.unwrap();
            assert_eq!(
                files.iter().filter(|f| f.ends_with(".seg")).count(),
                after.len()
            );
            assert!(!files.iter().any(|f| f.ends_with(".tmp")));
            drop(st);
            let st = Store::open(rt.clone(), "c", schema(), small_cfg())
                .await
                .unwrap();
            assert_eq!(st.approx_live_docs(), 30);
        });
    }

    /// Copies a segment file from one store to another's fetch staging file.
    async fn ship<R: Runtime>(from: &Store<R>, to: &Store<R>, id: SegmentId, corrupt: bool) {
        let rel = format!("segs/{:016x}.seg", id.get());
        let (_, data) = from
            .read_file_range(&rel, 0, 1 << 30)
            .await
            .unwrap()
            .unwrap();
        let mut data = data.to_vec();
        if corrupt {
            let mid = data.len() / 2;
            data[mid] ^= 0xff;
        }
        to.write_fetch_chunk(&Store::<R>::ship_staging(id), 0, Bytes::from(data))
            .await
            .unwrap();
        to.sync_fetched(&Store::<R>::ship_staging(id))
            .await
            .unwrap();
    }

    async fn build_local<R: Runtime>(st: &mut Store<R>, id: SegmentId) -> (u64, u64) {
        let job = st.flush_job(id).unwrap();
        let sections =
            Store::<R>::build_sections(&st.manifest.schema, &job.docs, st.indexer.as_ref())
                .unwrap();
        st.write_flush_file(&job, sections).await.unwrap().unwrap()
    }

    /// ADR 0016: freezes cut by log entries, a leader-built file installed on a follower,
    /// strict publication order, masking of rows changed after the freeze, a corrupt fetch
    /// rejected, and replay of pending freezes after a restart.
    #[test]
    fn two_phase_flush_ships_segments_in_order() {
        let (sim, mut ex) = Simulation::new(11, SimConfig::default());
        let rt = sim.runtime(NodeId(1), &ex.handle());
        ex.block_on(async move {
            let cfg = StoreConfig {
                memtable_max_bytes: usize::MAX,
                ..small_cfg()
            };
            let mut a = Store::open(rt.clone(), "a", schema(), cfg.clone())
                .await
                .unwrap();
            let mut b = Store::open(rt.clone(), "b", schema(), cfg.clone())
                .await
                .unwrap();
            let run = async |a: &mut Store<_>, b: &mut Store<_>, cmd: Command| {
                let ia = a.write(&cmd).await.unwrap();
                let ib = b.write(&cmd).await.unwrap();
                assert_eq!(ia, ib);
                ia
            };
            for i in 1..=20u64 {
                run(&mut a, &mut b, Command::Upsert(vec![doc(i, 1)])).await;
            }
            let f1 = SegmentId(run(&mut a, &mut b, Command::FlushBegin).await.get());
            for i in 21..=25u64 {
                run(&mut a, &mut b, Command::Upsert(vec![doc(i, 1)])).await;
            }
            // Changed after the first freeze: must be masked in its segment.
            run(&mut a, &mut b, Command::Delete(vec![DocId(5)])).await;
            run(&mut a, &mut b, Command::Upsert(vec![doc(6, 9)])).await;
            let f2 = SegmentId(run(&mut a, &mut b, Command::FlushBegin).await.get());
            run(&mut a, &mut b, Command::Delete(vec![DocId(22)])).await;
            assert_eq!(b.pending_flushes().len(), 2);
            for st in [&a, &b] {
                assert_eq!(st.get(DocId(5)).await.unwrap(), None);
                assert_eq!(st.get(DocId(6)).await.unwrap(), Some(doc(6, 9)));
                assert_eq!(st.get(DocId(22)).await.unwrap(), None);
                assert_eq!(st.get(DocId(21)).await.unwrap(), Some(doc(21, 1)));
                let mut ids: Vec<u64> = st.memtable_docs().iter().map(|d| d.id.get()).collect();
                ids.sort_unstable();
                let want: Vec<u64> = (1..=25).filter(|i| *i != 5 && *i != 22).collect();
                assert_eq!(ids, want);
            }
            // Leader: builds both, commits both.
            let (l1, h1) = build_local(&mut a, f1).await;
            let (l2, h2) = build_local(&mut a, f2).await;
            assert_eq!(a.publish_ready().await.unwrap(), 2);
            run(
                &mut a,
                &mut b,
                Command::FlushCommit {
                    id: f1,
                    len: l1,
                    hash: h1,
                },
            )
            .await;
            run(
                &mut a,
                &mut b,
                Command::FlushCommit {
                    id: f2,
                    len: l2,
                    hash: h2,
                },
            )
            .await;
            assert!(b.pending_flushes().iter().all(|p| p.commit.is_some()));
            // Follower: the second freeze is ready first; nothing publishes out of order.
            build_local(&mut b, f2).await;
            assert_eq!(b.publish_ready().await.unwrap(), 0);
            // A corrupt fetch is rejected, a good one installed.
            ship(&a, &b, f1, true).await;
            assert!(!b.install_fetched_flush(f1).await.unwrap());
            ship(&a, &b, f1, false).await;
            assert!(b.install_fetched_flush(f1).await.unwrap());
            assert_eq!(b.publish_ready().await.unwrap(), 2);
            assert_eq!(
                b.segment(f1).unwrap().meta.file_hash,
                a.segment(f1).unwrap().meta.file_hash
            );
            // The freeze is immutable: rows changed after it are in the file, masked.
            let seg = a.segment(f1).unwrap();
            let row = seg.docs.row_of(DocId(5)).expect("row cut by the freeze");
            assert!(seg.deletions.contains(row));
            // Local builds of the same freeze are identical across replicas.
            assert_eq!(
                b.segment(f2).unwrap().meta.file_hash,
                a.segment(f2).unwrap().meta.file_hash
            );
            // A third freeze, left pending across a restart.
            run(&mut a, &mut b, Command::Upsert(vec![doc(30, 1)])).await;
            let f3 = SegmentId(run(&mut a, &mut b, Command::FlushBegin).await.get());
            run(
                &mut a,
                &mut b,
                Command::FlushCommit {
                    id: f3,
                    len: 1,
                    hash: 2,
                },
            )
            .await;
            drop(b);
            let mut b = Store::open(rt.clone(), "b", schema(), cfg.clone())
                .await
                .unwrap();
            let p = b.pending_flushes();
            assert_eq!(p.len(), 1);
            assert_eq!(p[0].id, f3);
            assert_eq!(p[0].commit, Some((1, 2)));
            assert_eq!(b.get(DocId(30)).await.unwrap(), Some(doc(30, 1)));
            // The committed hash does not match anything the leader has: fetch fails, fallback.
            build_local(&mut b, f3).await;
            assert_eq!(b.publish_ready().await.unwrap(), 1);
            let (_, h3) = build_local(&mut a, f3).await;
            a.publish_ready().await.unwrap();
            assert_ne!(h3, 2);
            for i in 1..=31u64 {
                assert_eq!(
                    a.get(DocId(i)).await.unwrap(),
                    b.get(DocId(i)).await.unwrap(),
                    "doc {i}"
                );
            }
            assert_eq!(a.get(DocId(5)).await.unwrap(), None);
            assert_eq!(b.get(DocId(6)).await.unwrap(), Some(doc(6, 9)));
            drop(b);
            let b = Store::open(rt.clone(), "b", schema(), cfg).await.unwrap();
            assert!(b.pending_flushes().is_empty());
            assert_eq!(b.get(DocId(5)).await.unwrap(), None);
            assert_eq!(b.get(DocId(22)).await.unwrap(), None);
            assert_eq!(b.get(DocId(6)).await.unwrap(), Some(doc(6, 9)));
            assert_eq!(b.approx_live_docs(), 24);
        });
    }
}
