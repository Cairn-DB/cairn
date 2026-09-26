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
    Disk, DocId, Document, Error, LogIndex, NodeId, Result, Runtime, Schema, SegmentId, Term,
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
    /// Term of the log entry at `applied_index` (0 when unknown: manifests written before it
    /// was recorded). Stored with the index in one atomic write, so a restart never pairs a
    /// new snapshot index with the term of an older one (ADR 0020, chaos seed 34367).
    pub applied_term: Term,
    /// Compactions committed through the log but not installed yet, in log order (ADR 0021).
    /// Persisted so that neither log truncation nor a restart can lose one.
    pub compactions: Vec<CompactionMeta>,
    /// Index of the last `CompactCommit` this manifest reflects, accepted or rejected. Replay
    /// skips commits up to it: they were decided against the list as it stood then, and the
    /// segments and pending list here may already reflect later merges (chaos seed 11892).
    pub compacted_through: LogIndex,
}

/// A committed compaction waiting to be installed (ADR 0021).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionMeta {
    /// Merged segment id.
    pub id: SegmentId,
    /// Input segments, adjacent and in manifest order.
    pub inputs: Vec<SegmentId>,
    /// Index of the `CompactCommit` entry.
    pub index: LogIndex,
    /// Leader's file length and body hash.
    pub len: u64,
    /// See `len`.
    pub hash: u64,
    /// Node that built the file (fetch it from there).
    pub from: NodeId,
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
        w.u64(self.applied_term.get());
        w.u32(self.compactions.len() as u32);
        for c in &self.compactions {
            w.u64(c.id.get())
                .u64(c.index.get())
                .u64(c.len)
                .u64(c.hash)
                .u32(c.from.get())
                .u32(c.inputs.len() as u32);
            for i in &c.inputs {
                w.u64(i.get());
            }
        }
        w.u64(self.compacted_through.get());
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
        let applied_term = Term(if r.remaining() > 0 { r.u64()? } else { 0 });
        let mut compactions = Vec::new();
        if r.remaining() > 0 {
            let n = r.u32()? as usize;
            for _ in 0..n.min(1 << 16) {
                let id = SegmentId(r.u64()?);
                let index = LogIndex(r.u64()?);
                let len = r.u64()?;
                let hash = r.u64()?;
                let from = NodeId(r.u32()?);
                let k = r.u32()? as usize;
                if k > 1 << 16 {
                    return Err(Error::corruption("compaction with too many inputs"));
                }
                let mut inputs = Vec::with_capacity(k);
                for _ in 0..k {
                    inputs.push(SegmentId(r.u64()?));
                }
                compactions.push(CompactionMeta {
                    id,
                    inputs,
                    index,
                    len,
                    hash,
                    from,
                });
            }
        }
        let compacted_through = LogIndex(if r.remaining() > 0 { r.u64()? } else { 0 });
        Ok(ShardManifest {
            schema,
            applied_index,
            segments,
            next_segment_id,
            applied_term,
            compactions,
            compacted_through,
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

/// Prefix of the error `install_snapshot` returns when a snapshot file is missing or differs
/// from the manifest: the snapshot is stale for this replica (ADR 0021).
pub const SNAPSHOT_MISMATCH: &str = "snapshot segment mismatch";

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
    /// Node that built the committed file.
    commit_from: Option<NodeId>,
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
    /// Node that built the committed file.
    pub commit_from: Option<NodeId>,
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
    /// `(len, hash)` of the local file of each committed, uninstalled compaction that has one
    /// (the compactions themselves live in the manifest, ADR 0021).
    compaction_files: cairn_core::HashMap<SegmentId, (u64, u64)>,
    /// Segments replaced by an installed compaction whose files are kept for a while, so
    /// followers and snapshot installs that still need them can fetch them (ADR 0021).
    retired: Vec<SegmentId>,
    /// The segment list as the log defines it: published segments, plus freezes with rows (in
    /// `FlushBegin` order), with every accepted compaction applied. The same on every replica
    /// at the same applied index, whatever each has installed yet: a `CompactCommit` is
    /// accepted only if its inputs are adjacent here (ADR 0021).
    logical: Vec<SegmentId>,
    /// Ids deleted or replaced while a compaction was building; masked in the merged segment.
    masked_during_build: cairn_core::HashSet<DocId>,
    /// Whether a compaction job is in progress.
    job_active: bool,
    /// Namespace of this replica's compaction ids (see `COMPACT_NS_SHIFT`).
    compact_ns: u64,
    /// Segment format version new segments are written in (ADR 0018).
    segment_version: u32,
    /// Bumped whenever the store's state is replaced (snapshot install): a build that began
    /// before must be discarded, or it would publish pre-snapshot rows under a segment id the
    /// snapshot may already use.
    generation: u64,
    /// Bumped whenever the memtable changes.
    memtable_version: u64,
    /// Bumped whenever the segment list changes.
    segments_version: u64,
}

/// Writes a complete segment file (columns and index sections) at `path`: the part of a flush
/// or merge that runs outside the replica actor (ADR 0021), so large writes never hold back
/// Raft heartbeats. Returns `(len, hash)`.
pub async fn write_segment_file<R: Runtime>(
    rt: R,
    path: &str,
    schema: &Schema,
    version: u32,
    docs: &[Document],
    sections: &[(String, Vec<u8>)],
) -> Result<(u64, u64)> {
    let refs: Vec<&Document> = docs.iter().collect();
    let mut w = SegmentWriter::create_version(rt, path, version).await?;
    write_columns(&mut w, schema, &refs).await?;
    for (name, bytes) in sections {
        w.add_section(name, bytes).await?;
    }
    w.finish().await
}

/// Reads the live rows of merge inputs from their files (outside the replica actor): `sources`
/// from [`Store::compaction_sources`]. Rows are sorted by id.
pub async fn read_compaction_rows<R: Runtime>(
    rt: R,
    sources: &[(String, DeletionSet)],
) -> Result<Vec<Document>> {
    let mut docs: Vec<Document> = Vec::new();
    for (path, dels) in sources {
        let reader = SegmentReader::open(rt.clone(), path).await?;
        let store = DocStore::open(&reader).await?;
        docs.extend(store.read_all(&reader, |row| dels.contains(row)).await?);
    }
    docs.sort_by_key(|d| d.id);
    Ok(docs)
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
                    applied_term: Term(0),
                    compactions: Vec::new(),
                    compacted_through: LogIndex(0),
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
        // Merged segments not installed yet are judged after replay (`adopt_merge_files`): a
        // committed merge's file is kept and reused, so a restart does not make every replica
        // rebuild it (GCP 50M run, 2026-09-25).
        let merge_seg = |name: &str| {
            name.strip_suffix(".seg")
                .and_then(|h| u64::from_str_radix(h, 16).ok())
                .is_some_and(|id| id & COMPACT_ID_BIT != 0)
        };
        for name in disk.list(&format!("{dir}/segs")).await? {
            if name.ends_with(".tmp")
                || name.ends_with(".fetch")
                || name.ends_with(".built")
                || (!referenced.contains(&name)
                    && ((name.ends_with(".seg") && !merge_seg(&name)) || name.ends_with(".del")))
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
            compaction_files: cairn_core::HashMap::default(),
            retired: Vec::new(),
            logical: Vec::new(),
            masked_during_build: cairn_core::HashSet::default(),
            job_active: false,
            compact_ns: 0,
            segment_version: crate::segment::SEGMENT_VERSION,
            generation: 0,
            memtable_version: 0,
            segments_version: 0,
        };
        store.applied = store.manifest.applied_index;
        store.rebuild_logical();
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
        store.adopt_merge_files().await?;
        Ok(store)
    }

    /// After replay: reuses the file of every committed, uninstalled merge found on disk with
    /// the committed length and hash, and removes merge files nothing refers to.
    async fn adopt_merge_files(&mut self) -> Result<()> {
        let disk = self.rt.disk();
        for name in disk.list(&format!("{}/segs", self.dir)).await? {
            let Some(id) = name
                .strip_suffix(".seg")
                .and_then(|h| u64::from_str_radix(h, 16).ok())
                .filter(|id| id & COMPACT_ID_BIT != 0)
                .map(SegmentId)
            else {
                continue;
            };
            if self.segments.iter().any(|s| s.meta.id == id) {
                continue;
            }
            let path = seg_path(&self.dir, id);
            let wanted = self
                .manifest
                .compactions
                .iter()
                .find(|c| c.id == id)
                .cloned();
            let ok = match &wanted {
                Some(c) => match SegmentReader::open(self.rt.clone(), &path).await {
                    Ok(r) => {
                        r.len() == c.len && r.file_hash() == c.hash && r.verify_file_hash().await?
                    }
                    Err(_) => false,
                },
                None => false,
            };
            match wanted {
                Some(c) if ok => {
                    self.compaction_files.insert(id, (c.len, c.hash));
                }
                _ => disk.remove(&path).await?,
            }
        }
        Ok(())
    }

    /// Sets the namespace of this replica's compaction ids (a replicated shard passes its node
    /// id; at most 2^22 - 1).
    pub fn set_compaction_namespace(&mut self, ns: u64) {
        self.compact_ns = ns & ((1 << (62 - COMPACT_NS_SHIFT)) - 1);
    }

    /// Sets the segment format version of segments written from now on (a replicated shard
    /// passes the version negotiated with its peers, see
    /// [`crate::segment::negotiated_segment_version`]).
    pub fn set_segment_version(&mut self, version: u32) {
        self.segment_version = version;
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
            Command::CompactCommit {
                id,
                inputs,
                len,
                hash,
                from,
            } => {
                self.memtable.note_index(index);
                let installed = self.segments.iter().any(|s| s.meta.id == *id);
                let pending = self.manifest.compactions.iter().any(|c| c.id == *id);
                let decided = index <= self.manifest.compacted_through;
                self.manifest.compacted_through = self.manifest.compacted_through.max(index);
                if decided && !pending {
                    // Replay of a commit this manifest already reflects (accepted, installed
                    // and maybe merged again since, or rejected).
                } else if installed {
                    // Replay of a compaction already installed here.
                } else if pending {
                    // Replay after a restart: persisted in the manifest, but its inputs (a freeze
                    // replayed just before) were not in the list when it was rebuilt.
                    if !self.logical.contains(id) {
                        Self::apply_to_logical(&mut self.logical, inputs, *id);
                    }
                // Accepted only if the inputs are adjacent in the log-defined segment list, the
                // same decision on every replica. A second merge of inputs already merged (a new
                // leader repeating one) is dropped everywhere.
                } else if Self::apply_to_logical(&mut self.logical, inputs, *id) {
                    // Kept in the in-memory manifest; persisted with the next manifest write.
                    // Until then the entry stays in the log (it is truncated only after a
                    // manifest carrying this compaction is stored), so replay restores it.
                    self.manifest.compactions.push(CompactionMeta {
                        id: *id,
                        inputs: inputs.clone(),
                        index,
                        len: *len,
                        hash: *hash,
                        from: *from,
                    });
                }
            }
            Command::FlushCommit {
                id,
                len,
                hash,
                from,
            } => {
                self.memtable.note_index(index);
                if let Some(p) = self.pending.iter_mut().find(|p| p.id == *id)
                    && p.commit.is_none()
                {
                    p.commit = Some((*len, *hash));
                    p.commit_from = Some(*from);
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
        // A freeze without rows publishes no segment (the rows are fixed at the freeze).
        if mem.docs().next().is_some() {
            self.logical.push(id);
        }
        self.pending.push_back(PendingFlush {
            id,
            last,
            mem,
            masked: cairn_core::HashSet::default(),
            commit: None,
            commit_from: None,
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
                commit_from: p.commit_from,
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
        let mut w = SegmentWriter::create_version(
            self.rt.clone(),
            &seg_path(&self.dir, job.id),
            self.segment_version,
        )
        .await?;
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
        self.publish_ready_where(|_| true).await
    }

    /// Path of segment `id`'s file.
    pub fn segment_path(&self, id: SegmentId) -> String {
        seg_path(&self.dir, id)
    }

    /// Freezes at the head of the queue that have a file, in order (with empty freezes
    /// skipped): what `publish_ready` would publish.
    pub fn publishable_flushes(&self) -> Vec<SegmentId> {
        self.pending
            .iter()
            .take_while(|p| p.file.is_some() || p.mem.docs().next().is_none())
            .filter(|p| p.file.is_some())
            .map(|p| p.id)
            .collect()
    }

    /// As [`Store::publish_ready`], stopping at the first freeze with a file for which `ready`
    /// is false (its indexes are still being prepared, ADR 0026).
    pub async fn publish_ready_where(
        &mut self,
        ready: impl Fn(SegmentId) -> bool,
    ) -> Result<usize> {
        let mut n = 0;
        while self
            .pending
            .front()
            .is_some_and(|p| p.mem.docs().next().is_none() || (p.file.is_some() && ready(p.id)))
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
            if p.last > new_manifest.applied_index {
                new_manifest.applied_index = p.last;
                // The entry is still in the log: it is truncated only after this manifest.
                new_manifest.applied_term = self.log.read(p.last).await?.term;
            }
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

    /// Current store generation (bumped by snapshot installs).
    pub fn generation(&self) -> u64 {
        self.generation
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

    /// The compaction the policy would run now (adjacent input ids), if any (ADR 0021: the
    /// leader decides, then builds with [`Store::compaction_rows`]).
    pub fn select_compaction(&self) -> Option<Vec<SegmentId>> {
        let n = self.segments.len();
        let stale = self.segments.iter().position(|s| {
            s.meta.doc_count > 0
                && f64::from(s.deletions.count()) / f64::from(s.meta.doc_count)
                    > self.cfg.max_deleted_fraction
        });
        let live = |s: &OpenSegment<R>| u64::from(s.meta.doc_count - s.deletions.count());
        let run = self.tiered_run(n, &live);
        if let Some(i) = stale {
            Some(vec![self.segments[i].meta.id])
        } else if let Some((first, len)) = run {
            Some(
                self.segments[first..first + len]
                    .iter()
                    .map(|s| s.meta.id)
                    .collect(),
            )
        } else if n > self.cfg.max_segments {
            let (i, _) = (0..n - 1)
                .map(|i| (i, live(&self.segments[i]) + live(&self.segments[i + 1])))
                .min_by_key(|(_, rows)| *rows)
                .expect("n > 1");
            Some(vec![self.segments[i].meta.id, self.segments[i + 1].meta.id])
        } else {
            None
        }
    }

    /// Reserves a compaction output id that this replica never used before, persisted before
    /// it is returned (so a restart cannot hand out an id already proposed).
    pub async fn reserve_compaction_id(&mut self) -> Result<SegmentId> {
        // Never reuse an id: past the persisted counter and past every id of this namespace
        // still known (a lagging replica that still holds an old id would ignore a new commit
        // reusing it).
        let ns = self.compact_ns;
        let seen = self
            .manifest
            .segments
            .iter()
            .map(|m| m.id)
            .chain(self.manifest.compactions.iter().map(|c| c.id))
            .filter(|id| {
                id.get() & COMPACT_ID_BIT != 0 && (id.get() >> COMPACT_NS_SHIFT) & 0x3f_ffff == ns
            })
            .map(|id| (id.get() & COMPACT_COUNTER_MASK) + 1)
            .max()
            .unwrap_or(0);
        let counter = (self.manifest.next_segment_id & COMPACT_COUNTER_MASK).max(seen);
        let mut m = self.manifest.clone();
        m.next_segment_id = counter + 1;
        self.manifest_store.store(&m).await?;
        self.manifest = m;
        Ok(SegmentId(
            COMPACT_ID_BIT | (self.compact_ns << COMPACT_NS_SHIFT) | counter,
        ))
    }

    /// Where `ids` sit in the segment list, if they are all present, adjacent and in order.
    fn input_range(&self, ids: &[SegmentId]) -> Option<std::ops::Range<usize>> {
        let head = *ids.first()?;
        let first = self.segments.iter().position(|s| s.meta.id == head)?;
        let ok = ids
            .iter()
            .enumerate()
            .all(|(k, id)| self.segments.get(first + k).map(|s| s.meta.id) == Some(*id));
        ok.then(|| first..first + ids.len())
    }

    /// A compaction job for output `id` over `inputs`: their rows live now, sorted by id.
    /// `None` when the inputs are not (or no longer) all published and adjacent.
    pub async fn compaction_rows(
        &self,
        id: SegmentId,
        inputs: &[SegmentId],
    ) -> Result<Option<CompactJob>> {
        let Some(range) = self.input_range(inputs) else {
            return Ok(None);
        };
        let mut docs: Vec<Document> = Vec::new();
        let mut log_last = LogIndex(0);
        for s in &self.segments[range] {
            let dels = &s.deletions;
            docs.extend(s.docs.read_all(&s.reader, |row| dels.contains(row)).await?);
            log_last = log_last.max(s.meta.log_last);
        }
        docs.sort_by_key(|d| d.id);
        Ok(Some(CompactJob {
            generation: self.generation,
            inputs: inputs.to_vec(),
            docs,
            log_last,
            id,
        }))
    }

    /// Writes the merged segment file of `job` without installing it; returns `(len, hash)`.
    pub async fn write_compaction_file(
        &mut self,
        job: &CompactJob,
        sections: Vec<(String, Vec<u8>)>,
    ) -> Result<(u64, u64)> {
        let refs: Vec<&Document> = job.docs.iter().collect();
        let mut w = SegmentWriter::create_version(
            self.rt.clone(),
            &seg_path(&self.dir, job.id),
            self.segment_version,
        )
        .await?;
        write_columns(&mut w, &self.manifest.schema, &refs).await?;
        for (name, bytes) in &sections {
            w.add_section(name, bytes).await?;
        }
        let file = w.finish().await?;
        if self.manifest.compactions.iter().any(|c| c.id == job.id) {
            self.compaction_files.insert(job.id, file);
        }
        Ok(file)
    }

    /// Where a build outside the actor writes segment `id` (a side name, adopted by
    /// [`Store::adopt_built_file`] only if still wanted), the schema and the format version.
    pub fn segment_write_params(&self, id: SegmentId) -> (String, Schema, u32) {
        (
            format!("{}.built", seg_path(&self.dir, id)),
            self.manifest.schema.clone(),
            self.segment_version,
        )
    }

    /// Moves a segment written by a build outside the actor into place. A late build must
    /// never replace a file installed meanwhile (a snapshot, a fetch): the caller adopts only
    /// results still wanted, and [`Store::discard_built_file`] drops the others.
    pub async fn adopt_built_file(&self, id: SegmentId) -> Result<()> {
        let path = seg_path(&self.dir, id);
        self.rt.disk().rename(&format!("{path}.built"), &path).await
    }

    /// Removes an unwanted build result.
    pub async fn discard_built_file(&self, id: SegmentId) -> Result<()> {
        let path = format!("{}.built", seg_path(&self.dir, id));
        let disk = self.rt.disk();
        if disk.exists(&path).await? {
            disk.remove(&path).await?;
        }
        Ok(())
    }

    /// Whether the pending freeze `id` still needs a file (begun at `generation`).
    pub fn flush_wanted(&self, id: SegmentId, generation: u64) -> bool {
        self.job_is_current(generation)
            && self.pending.iter().any(|p| p.id == id && p.file.is_none())
    }

    /// The inputs of a merge as `(file path, deletion bits now)`, if they are all published and
    /// adjacent; with their highest log index.
    pub fn compaction_sources(
        &self,
        inputs: &[SegmentId],
    ) -> Option<(Vec<(String, DeletionSet)>, LogIndex)> {
        let range = self.input_range(inputs)?;
        let segs = &self.segments[range];
        let log_last = segs
            .iter()
            .map(|s| s.meta.log_last)
            .max()
            .unwrap_or(LogIndex(0));
        Some((
            segs.iter()
                .map(|s| (seg_path(&self.dir, s.meta.id), s.deletions.clone()))
                .collect(),
            log_last,
        ))
    }

    /// Records the file of the pending freeze `id`, written outside the actor. `false` when the
    /// freeze is no longer pending (published, or a snapshot replaced the state since the job
    /// began at `generation`).
    pub fn set_flush_file(&mut self, id: SegmentId, generation: u64, file: (u64, u64)) -> bool {
        if !self.job_is_current(generation) {
            return false;
        }
        match self
            .pending
            .iter_mut()
            .find(|p| p.id == id && p.file.is_none())
        {
            Some(p) => {
                p.file = Some(file);
                true
            }
            None => false,
        }
    }

    /// Whether committed compaction `id` is pending here (not installed).
    pub fn compaction_pending(&self, id: SegmentId) -> bool {
        self.manifest.compactions.iter().any(|c| c.id == id)
    }

    /// Records that the file of committed compaction `id` is present locally (the leader wrote
    /// it before proposing).
    pub fn set_compaction_file(&mut self, id: SegmentId, file: (u64, u64)) {
        if self.manifest.compactions.iter().any(|c| c.id == id) {
            self.compaction_files.insert(id, file);
        }
    }

    /// Committed compactions not installed yet, in log order: `(meta, has local file, inputs
    /// all published)`.
    pub fn pending_compactions(&self) -> Vec<(CompactionMeta, bool, bool)> {
        self.manifest
            .compactions
            .iter()
            .map(|c| {
                (
                    c.clone(),
                    self.compaction_files.contains_key(&c.id),
                    self.input_range(&c.inputs).is_some(),
                )
            })
            .collect()
    }

    /// Replaces adjacent `inputs` with `id` in `logical`; `false` (unchanged) if they are not
    /// there, adjacent and in order.
    fn apply_to_logical(logical: &mut Vec<SegmentId>, inputs: &[SegmentId], id: SegmentId) -> bool {
        let Some(&head) = inputs.first() else {
            return false;
        };
        let Some(first) = logical.iter().position(|s| *s == head) else {
            return false;
        };
        let adjacent = inputs
            .iter()
            .enumerate()
            .all(|(k, i)| logical.get(first + k) == Some(i));
        if adjacent {
            logical.splice(first..first + inputs.len(), [id]);
        }
        adjacent
    }

    /// Recomputes the log-defined segment list from the manifest (open, snapshot install):
    /// published segments with the committed, uninstalled compactions applied in order.
    fn rebuild_logical(&mut self) {
        self.logical = Self::logical_of(&self.manifest);
    }

    fn logical_of(m: &ShardManifest) -> Vec<SegmentId> {
        let mut logical: Vec<SegmentId> = m.segments.iter().map(|s| s.id).collect();
        for c in &m.compactions {
            Self::apply_to_logical(&mut logical, &c.inputs, c.id);
        }
        logical
    }

    /// Whether the compaction `id` was accepted (committed and valid), installed or pending.
    pub fn compaction_accepted(&self, id: SegmentId) -> bool {
        self.logical.contains(&id) || self.segments.iter().any(|s| s.meta.id == id)
    }

    /// Segments replaced since the last call; their files are still on disk until
    /// [`Store::purge_segment_files`].
    pub fn take_retired(&mut self) -> Vec<SegmentId> {
        std::mem::take(&mut self.retired)
    }

    /// Removes the files of a retired segment (no-op if it is live again or gone).
    pub async fn purge_segment_files(&self, id: SegmentId) -> Result<()> {
        if self.segments.iter().any(|s| s.meta.id == id) {
            return Ok(());
        }
        let disk = self.rt.disk();
        for path in [seg_path(&self.dir, id), del_path(&self.dir, id)] {
            if disk.exists(&path).await? {
                disk.remove(&path).await?;
            }
        }
        Ok(())
    }

    /// Installs a fetched merged segment (staged at `<Store::ship_staging(id)>.fetch`) after
    /// checking it against the commit. `false`: rejected (the caller builds locally).
    pub async fn install_fetched_compaction(&mut self, id: SegmentId) -> Result<bool> {
        let Some(c) = self
            .manifest
            .compactions
            .iter()
            .find(|c| c.id == id)
            .cloned()
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
            Ok(r) => r.len() == c.len && r.file_hash() == c.hash && r.verify_file_hash().await?,
            Err(_) => false,
        };
        if !ok {
            let _ = disk.remove(&path).await;
            return Ok(false);
        }
        self.compaction_files.insert(id, (c.len, c.hash));
        Ok(true)
    }

    /// Ways to publish the oldest pending freeze without its own file (ADR 0021): the pending
    /// merges, composed in log order over the published segments and pending freezes, reach
    /// a merge that is then the only output left, consumes that freeze, and consumes only a
    /// prefix of the freeze queue. Each candidate is `(position in the compaction queue, merge
    /// id, freezes it consumes)`, earliest first. Installing one skips building the freezes and intermediate merges
    /// it consumes: a replica that fell behind no longer rebuilds flushes the others merged
    /// away long ago.
    pub fn subsumption_candidates(&self) -> Vec<(usize, SegmentId, Vec<SegmentId>)> {
        let Some(head) = self.pending.front() else {
            return Vec::new();
        };
        if head.file.is_some() || head.mem.docs().next().is_none() {
            return Vec::new();
        }
        let freezes: Vec<SegmentId> = self
            .pending
            .iter()
            .filter(|p| p.mem.docs().next().is_some())
            .map(|p| p.id)
            .collect();
        let mut v: Vec<SegmentId> = self.segments.iter().map(|s| s.meta.id).collect();
        v.extend(freezes.iter().copied());
        let mut produced: Vec<SegmentId> = Vec::new();
        let mut out = Vec::new();
        for (j, c) in self.manifest.compactions.iter().enumerate() {
            if !Self::apply_to_logical(&mut v, &c.inputs, c.id) {
                break;
            }
            produced.push(c.id);
            let single_root =
                produced.iter().filter(|id| v.contains(id)).count() == 1 && v.contains(&c.id);
            let consumed = freezes.iter().take_while(|f| !v.contains(f)).count();
            let prefix = freezes.iter().skip(consumed).all(|f| v.contains(f));
            if single_root && consumed > 0 && prefix {
                out.push((j, c.id, freezes[..consumed].to_vec()));
            }
        }
        out
    }

    /// Installs candidate `j` from [`Store::subsumption_candidates`] (its merged file must be
    /// here): publishes the freezes it consumes without files, drops the intermediate merges,
    /// and replaces the consumed segments with the merge, in one manifest write. A merged row
    /// is masked unless its row is still live in a consumed input: a published segment, or a
    /// consumed freeze's rows not changed since the freeze.
    pub async fn install_subsumed(&mut self, j: usize) -> Result<()> {
        let target = self
            .manifest
            .compactions
            .get(j)
            .cloned()
            .ok_or_else(|| Error::Internal("no such compaction".into()))?;
        let Some(&(file_len, file_hash)) = self.compaction_files.get(&target.id) else {
            return Err(Error::Internal("subsuming merge has no file".into()));
        };
        self.persist_dirty_deletions().await?;
        let freezes: Vec<SegmentId> = self
            .pending
            .iter()
            .filter(|p| p.mem.docs().next().is_some())
            .map(|p| p.id)
            .collect();
        let physical: Vec<SegmentId> = self.segments.iter().map(|s| s.meta.id).collect();
        let mut v = physical.clone();
        v.extend(freezes.iter().copied());
        for c in &self.manifest.compactions[..=j] {
            if !Self::apply_to_logical(&mut v, &c.inputs, c.id) {
                return Err(Error::Internal("subsumption no longer applies".into()));
            }
        }
        let consumed_freezes: Vec<SegmentId> =
            freezes.iter().copied().filter(|f| !v.contains(f)).collect();
        let last_consumed = *consumed_freezes
            .last()
            .ok_or_else(|| Error::Internal("subsumption consumes no freeze".into()))?;
        // Every row still live in a consumed input.
        let mut live: cairn_core::HashSet<u64> = cairn_core::HashSet::default();
        let mut log_last = LogIndex(0);
        for s in self.segments.iter().filter(|s| !v.contains(&s.meta.id)) {
            log_last = log_last.max(s.meta.log_last);
            for (row, id) in s.docs.docids().iter().enumerate() {
                if !s.deletions.contains(row as u32) {
                    live.insert(*id);
                }
            }
        }
        // Publish the freezes up to the last consumed one (empty ones included) without files.
        let pos = self
            .pending
            .iter()
            .position(|p| p.id == last_consumed)
            .expect("consumed freeze is pending");
        let popped: Vec<PendingFlush> = self.pending.drain(..=pos).collect();
        for p in &popped {
            log_last = log_last.max(p.last);
            for d in p.mem.docs() {
                if !p.masked.contains(&d.id) {
                    live.insert(d.id.get());
                }
            }
        }
        let last = popped.last().expect("popped").last;
        let path = seg_path(&self.dir, target.id);
        let reader = SegmentReader::open(self.rt.clone(), &path).await?;
        let docstore = DocStore::open(&reader).await?;
        let doc_count = docstore.doc_count();
        let mut deletions = DeletionSet::new(doc_count);
        let mut dirty = false;
        for (row, id) in docstore.docids().iter().enumerate() {
            if !live.contains(id) {
                deletions.set(row as u32);
                dirty = true;
            }
        }
        if dirty {
            ManifestStore::new(self.rt.clone(), del_path(&self.dir, target.id))
                .store(&deletions)
                .await?;
        }
        let meta = SegmentMeta {
            id: target.id,
            doc_count,
            log_last,
            file_len,
            file_hash,
        };
        // New physical list: the composed list without the freezes still pending.
        let remaining: cairn_core::HashSet<SegmentId> = self.pending.iter().map(|p| p.id).collect();
        let new_ids: Vec<SegmentId> = v.into_iter().filter(|id| !remaining.contains(id)).collect();
        let mut new_manifest = self.manifest.clone();
        new_manifest.segments = new_ids
            .iter()
            .map(|id| {
                if *id == target.id {
                    meta.clone()
                } else {
                    self.segments
                        .iter()
                        .find(|s| s.meta.id == *id)
                        .expect("kept segment")
                        .meta
                        .clone()
                }
            })
            .collect();
        let dropped: Vec<SegmentId> = new_manifest.compactions[..=j]
            .iter()
            .map(|c| c.id)
            .collect();
        new_manifest.compactions.drain(..=j);
        new_manifest.applied_index = last.max(new_manifest.applied_index);
        new_manifest.applied_term = self.log.read(last).await?.term;
        self.manifest_store.store(&new_manifest).await?;
        self.manifest = new_manifest;
        for id in &dropped {
            self.compaction_files.remove(id);
        }
        // Rebuild the open segment list in the new order.
        let mut old: Vec<OpenSegment<R>> = std::mem::take(&mut self.segments);
        let mut next = Vec::with_capacity(new_ids.len());
        let mut new_seg = Some(OpenSegment {
            meta,
            reader,
            docs: docstore,
            deletions,
            deletions_dirty: false,
        });
        for id in &new_ids {
            if *id == target.id {
                next.push(new_seg.take().expect("once"));
            } else if let Some(k) = old.iter().position(|s| s.meta.id == *id) {
                next.push(old.swap_remove(k));
            }
        }
        self.segments = next;
        // Consumed segments and intermediate merges: files kept for the grace period.
        self.retired.extend(old.iter().map(|s| s.meta.id));
        self.retired
            .extend(dropped.iter().copied().filter(|id| *id != target.id));
        self.memtable_version += 1;
        self.segments_version += 1;
        self.log.truncate_prefix(last.next()).await?;
        Ok(())
    }

    /// Installs, in log order, every committed compaction whose file is present and whose
    /// inputs are all published (ADR 0021). A merged row is masked when its input row is
    /// deleted now: input deletion bits carry every change applied here since the rows were
    /// read, and nothing beyond. Returns how many were installed.
    pub async fn install_compactions(&mut self) -> Result<usize> {
        self.install_compactions_where(|_| true).await
    }

    /// The next compaction `install_compactions` would install: file here, inputs published.
    pub fn installable_compaction(&self) -> Option<SegmentId> {
        let c = self.manifest.compactions.first()?;
        (self.compaction_files.contains_key(&c.id) && self.input_range(&c.inputs).is_some())
            .then_some(c.id)
    }

    /// As [`Store::install_compactions`], stopping at the first compaction for which `ready`
    /// is false (ADR 0026).
    pub async fn install_compactions_where(
        &mut self,
        ready: impl Fn(SegmentId) -> bool,
    ) -> Result<usize> {
        let mut n = 0;
        while let Some(c) = self.manifest.compactions.first().cloned() {
            if !ready(c.id) {
                break;
            }
            let (Some(&(file_len, file_hash)), Some(range)) = (
                self.compaction_files.get(&c.id),
                self.input_range(&c.inputs),
            ) else {
                break;
            };
            self.persist_dirty_deletions().await?;
            let path = seg_path(&self.dir, c.id);
            let reader = SegmentReader::open(self.rt.clone(), &path).await?;
            let docstore = DocStore::open(&reader).await?;
            let doc_count = docstore.doc_count();
            // Ids still live in the inputs.
            let mut live: cairn_core::HashSet<u64> = cairn_core::HashSet::default();
            let mut log_last = LogIndex(0);
            for s in &self.segments[range.clone()] {
                log_last = log_last.max(s.meta.log_last);
                for (row, id) in s.docs.docids().iter().enumerate() {
                    if !s.deletions.contains(row as u32) {
                        live.insert(*id);
                    }
                }
            }
            let mut deletions = DeletionSet::new(doc_count);
            let mut dirty = false;
            for (row, id) in docstore.docids().iter().enumerate() {
                if !live.contains(id) {
                    deletions.set(row as u32);
                    dirty = true;
                }
            }
            if dirty {
                ManifestStore::new(self.rt.clone(), del_path(&self.dir, c.id))
                    .store(&deletions)
                    .await?;
            }
            let meta = SegmentMeta {
                id: c.id,
                doc_count,
                log_last,
                file_len,
                file_hash,
            };
            let mut new_manifest = self.manifest.clone();
            new_manifest.segments.splice(range.clone(), [meta.clone()]);
            new_manifest.compactions.remove(0);
            self.manifest_store.store(&new_manifest).await?;
            self.manifest = new_manifest;
            self.compaction_files.remove(&c.id);
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
            // Files stay until the caller purges them (see `take_retired`).
            self.retired.extend(removed.iter().map(|s| s.meta.id));
            n += 1;
        }
        Ok(n)
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
        let mut w =
            SegmentWriter::create_version(self.rt.clone(), &path, self.segment_version).await?;
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

    /// Starts (or restarts) a fetch into the staging file of `rel`: empties it.
    pub async fn reset_fetch_staging(&self, rel: &str) -> Result<()> {
        let path = format!("{}/{rel}.fetch", self.dir);
        let disk = self.rt.disk();
        let f = disk
            .open(&path, cairn_core::OpenMode::CreateTruncate)
            .await?;
        disk.set_len(&f, 0).await
    }

    /// Writes one fetched chunk of `rel` at `offset` into its staging file, in any order (a
    /// pipelined fetch has several chunks in flight, ADR 0021).
    pub async fn write_fetch_chunk_at(
        &self,
        rel: &str,
        offset: u64,
        data: bytes::Bytes,
    ) -> Result<()> {
        let disk = self.rt.disk();
        let path = format!("{}/{rel}.fetch", self.dir);
        let f = disk.open(&path, cairn_core::OpenMode::CreateOrOpen).await?;
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
        let mut m: ShardManifest = ManifestStore::<R>::decode(manifest)?;
        // The compaction id counter is this replica's own (its namespace): keep the higher of
        // the local and the incoming one, or a later leadership here could reuse an id.
        m.next_segment_id = m.next_segment_id.max(self.manifest.next_segment_id);
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
                    "{SNAPSHOT_MISMATCH}: {} missing or different locally",
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
        self.compaction_files.clear();
        self.logical = Self::logical_of(&self.manifest);
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
                        Command::Noop
                        | Command::FlushBegin
                        | Command::FlushCommit { .. }
                        | Command::CompactCommit { .. } => {}
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
                    from: NodeId(1),
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
                    from: NodeId(1),
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
                    from: NodeId(1),
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

    /// ADR 0021: a compaction committed through the log waits for its inputs, masks rows
    /// changed after the merged rows were read (input deletion bits), survives a restart, and
    /// a fetched file must match the commit.
    #[test]
    fn compaction_through_the_log() {
        let (sim, mut ex) = Simulation::new(12, SimConfig::default());
        let rt = sim.runtime(NodeId(1), &ex.handle());
        ex.block_on(async move {
            let cfg = StoreConfig {
                memtable_max_bytes: usize::MAX,
                ..small_cfg()
            };
            let mut a = Store::open(rt.clone(), "ca", schema(), cfg.clone())
                .await
                .unwrap();
            let mut b = Store::open(rt.clone(), "cb", schema(), cfg.clone())
                .await
                .unwrap();
            let run = async |a: &mut Store<_>, b: &mut Store<_>, cmd: Command| {
                let ia = a.write(&cmd).await.unwrap();
                assert_eq!(ia, b.write(&cmd).await.unwrap());
                ia
            };
            // Two flushed segments on both replicas.
            let mut segs = Vec::new();
            for batch in 0..2u64 {
                for i in 1..=10u64 {
                    run(
                        &mut a,
                        &mut b,
                        Command::Upsert(vec![doc(batch * 10 + i, 1)]),
                    )
                    .await;
                }
                let f = SegmentId(run(&mut a, &mut b, Command::FlushBegin).await.get());
                for st in [&mut a, &mut b] {
                    build_local(st, f).await;
                    st.publish_ready().await.unwrap();
                }
                segs.push(f);
            }
            // Leader: picks, reads and builds the merge.
            a.set_compaction_namespace(1);
            let id = a.reserve_compaction_id().await.unwrap();
            let job = a.compaction_rows(id, &segs).await.unwrap().unwrap();
            assert_eq!(job.docs.len(), 20);
            let sections = Store::<cairn_sim::SimRuntime>::build_sections(
                &a.manifest.schema,
                &job.docs,
                a.indexer.as_ref(),
            )
            .unwrap();
            let (len, hash) = a.write_compaction_file(&job, sections).await.unwrap();
            // Changes after the rows were read, before the commit.
            run(&mut a, &mut b, Command::Delete(vec![DocId(3)])).await;
            run(&mut a, &mut b, Command::Upsert(vec![doc(15, 2)])).await;
            let commit = Command::CompactCommit {
                id,
                inputs: segs.clone(),
                len,
                hash,
                from: NodeId(1),
            };
            run(&mut a, &mut b, commit).await;
            // A second leader merging the same inputs: rejected on both replicas, whether or
            // not the first merge is installed yet (b has not installed it).
            let dup = Command::CompactCommit {
                id: SegmentId(id.get() + 1),
                inputs: segs.clone(),
                len,
                hash,
                from: NodeId(2),
            };
            run(&mut a, &mut b, dup).await;
            for st in [&a, &b] {
                assert_eq!(st.pending_compactions().len(), 1);
                assert!(!st.compaction_accepted(SegmentId(id.get() + 1)));
                assert!(st.compaction_accepted(id));
            }
            a.set_compaction_file(id, (len, hash));
            assert_eq!(a.install_compactions().await.unwrap(), 1);
            assert_eq!(a.segments().map(|m| m.id).collect::<Vec<_>>(), vec![id]);
            // Follower: survives a restart before installing, rejects a corrupt fetch.
            drop(b);
            let mut b = Store::open(rt.clone(), "cb", schema(), cfg.clone())
                .await
                .unwrap();
            let p = b.pending_compactions();
            assert_eq!(p.len(), 1);
            assert!(!p[0].1 && p[0].2, "no file yet, inputs published");
            assert_eq!(b.install_compactions().await.unwrap(), 0);
            ship(&a, &b, id, true).await;
            assert!(!b.install_fetched_compaction(id).await.unwrap());
            ship(&a, &b, id, false).await;
            assert!(b.install_fetched_compaction(id).await.unwrap());
            assert_eq!(b.install_compactions().await.unwrap(), 1);
            for st in [&a, &b] {
                assert_eq!(st.segments().count(), 1);
                assert_eq!(st.get(DocId(3)).await.unwrap(), None);
                assert_eq!(st.get(DocId(15)).await.unwrap(), Some(doc(15, 2)));
                assert_eq!(st.get(DocId(4)).await.unwrap(), Some(doc(4, 1)));
                assert_eq!(st.get(DocId(20)).await.unwrap(), Some(doc(20, 1)));
            }
            assert!(b.pending_compactions().is_empty());
            drop(b);
            let b = Store::open(rt.clone(), "cb", schema(), cfg).await.unwrap();
            assert!(b.pending_compactions().is_empty());
            assert_eq!(b.get(DocId(3)).await.unwrap(), None);
            assert_eq!(b.get(DocId(15)).await.unwrap(), Some(doc(15, 2)));
        });
    }

    /// ADR 0021: a lagging replica installs a merge (here a merge of a merge) over a flush it
    /// never built, publishing the flush without a file.
    #[test]
    fn merge_installed_over_a_flush_never_built() {
        let (sim, mut ex) = Simulation::new(13, SimConfig::default());
        let rt = sim.runtime(NodeId(1), &ex.handle());
        ex.block_on(async move {
            let cfg = StoreConfig {
                memtable_max_bytes: usize::MAX,
                ..small_cfg()
            };
            let mut a = Store::open(rt.clone(), "sa", schema(), cfg.clone())
                .await
                .unwrap();
            let mut b = Store::open(rt.clone(), "sb", schema(), cfg.clone())
                .await
                .unwrap();
            let run = async |a: &mut Store<_>, b: &mut Store<_>, cmd: Command| {
                let ia = a.write(&cmd).await.unwrap();
                assert_eq!(ia, b.write(&cmd).await.unwrap());
                ia
            };
            // Segment A on both; freeze F built on a only.
            for i in 1..=10u64 {
                run(&mut a, &mut b, Command::Upsert(vec![doc(i, 1)])).await;
            }
            let sa = SegmentId(run(&mut a, &mut b, Command::FlushBegin).await.get());
            for st in [&mut a, &mut b] {
                build_local(st, sa).await;
                st.publish_ready().await.unwrap();
            }
            for i in 11..=20u64 {
                run(&mut a, &mut b, Command::Upsert(vec![doc(i, 1)])).await;
            }
            let sf = SegmentId(run(&mut a, &mut b, Command::FlushBegin).await.get());
            let (fl, fh) = build_local(&mut a, sf).await;
            a.publish_ready().await.unwrap();
            run(
                &mut a,
                &mut b,
                Command::FlushCommit {
                    id: sf,
                    len: fl,
                    hash: fh,
                    from: NodeId(1),
                },
            )
            .await;
            // Changed after F's freeze, before the merges.
            run(&mut a, &mut b, Command::Delete(vec![DocId(15)])).await;
            // a merges [A, F] into C1, then rewrites C1 alone into C2.
            a.set_compaction_namespace(1);
            let merge = async |a: &mut Store<_>, b: &mut Store<_>, inputs: Vec<SegmentId>| {
                let id = a.reserve_compaction_id().await.unwrap();
                let job = a.compaction_rows(id, &inputs).await.unwrap().unwrap();
                let sections = Store::<cairn_sim::SimRuntime>::build_sections(
                    &a.manifest.schema,
                    &job.docs,
                    a.indexer.as_ref(),
                )
                .unwrap();
                let (len, hash) = a.write_compaction_file(&job, sections).await.unwrap();
                let cmd = Command::CompactCommit {
                    id,
                    inputs,
                    len,
                    hash,
                    from: NodeId(1),
                };
                run(a, b, cmd).await;
                a.set_compaction_file(id, (len, hash));
                assert_eq!(a.install_compactions().await.unwrap(), 1);
                id
            };
            let c1 = merge(&mut a, &mut b, vec![sa, sf]).await;
            // Changed between the two merges.
            run(&mut a, &mut b, Command::Upsert(vec![doc(3, 7)])).await;
            let c2 = merge(&mut a, &mut b, vec![c1]).await;
            run(&mut a, &mut b, Command::Delete(vec![DocId(18)])).await;
            // b never built F: both merges are candidates, only C2's file is shipped.
            let cands = b.subsumption_candidates();
            assert_eq!(
                cands
                    .iter()
                    .map(|(j, id, f)| (*j, *id, f.clone()))
                    .collect::<Vec<_>>(),
                vec![(0, c1, vec![sf]), (1, c2, vec![sf])]
            );
            ship(&a, &b, c2, false).await;
            assert!(b.install_fetched_compaction(c2).await.unwrap());
            b.install_subsumed(1).await.unwrap();
            assert!(b.pending_flushes().is_empty());
            assert!(b.pending_compactions().is_empty());
            assert_eq!(b.segments().map(|m| m.id).collect::<Vec<_>>(), vec![c2]);
            assert_eq!(b.applied_index(), a.applied_index());
            for i in 1..=20u64 {
                assert_eq!(
                    b.get(DocId(i)).await.unwrap(),
                    a.get(DocId(i)).await.unwrap(),
                    "doc {i}"
                );
            }
            assert_eq!(b.get(DocId(15)).await.unwrap(), None);
            assert_eq!(b.get(DocId(18)).await.unwrap(), None);
            assert_eq!(b.get(DocId(3)).await.unwrap(), Some(doc(3, 7)));
            drop(b);
            let b = Store::open(rt.clone(), "sb", schema(), cfg).await.unwrap();
            assert_eq!(b.segments().map(|m| m.id).collect::<Vec<_>>(), vec![c2]);
            for i in 1..=20u64 {
                assert_eq!(
                    b.get(DocId(i)).await.unwrap(),
                    a.get(DocId(i)).await.unwrap(),
                    "doc {i}"
                );
            }
        });
    }

    /// A committed merge not yet installed keeps its file across a restart, and the file is
    /// reused; a merge file nothing refers to is removed. Before this, a restart deleted the
    /// builder's file and every replica rebuilt the merge (GCP 50M run).
    #[test]
    fn committed_merge_file_survives_a_restart() {
        let (sim, mut ex) = Simulation::new(14, SimConfig::default());
        let rt = sim.runtime(NodeId(1), &ex.handle());
        ex.block_on(async move {
            let cfg = StoreConfig {
                memtable_max_bytes: usize::MAX,
                ..small_cfg()
            };
            let mut a = Store::open(rt.clone(), "sm", schema(), cfg.clone())
                .await
                .unwrap();
            a.set_compaction_namespace(1);
            let mut segs = Vec::new();
            for batch in 0..2u64 {
                for i in 1..=10u64 {
                    a.write(&Command::Upsert(vec![doc(batch * 10 + i, 1)]))
                        .await
                        .unwrap();
                }
                let id = SegmentId(a.write(&Command::FlushBegin).await.unwrap().get());
                build_local(&mut a, id).await;
                a.publish_ready().await.unwrap();
                segs.push(id);
            }
            let id = a.reserve_compaction_id().await.unwrap();
            let job = a.compaction_rows(id, &segs).await.unwrap().unwrap();
            let sections = Store::<cairn_sim::SimRuntime>::build_sections(
                &a.manifest.schema,
                &job.docs,
                a.indexer.as_ref(),
            )
            .unwrap();
            let (len, hash) = a.write_compaction_file(&job, sections).await.unwrap();
            a.write(&Command::CompactCommit {
                id,
                inputs: segs.clone(),
                len,
                hash,
                from: NodeId(1),
            })
            .await
            .unwrap();
            a.set_compaction_file(id, (len, hash));
            // A stray merge file (another id, committed nowhere).
            let stray = SegmentId(id.get() + 1);
            let disk = rt.disk();
            let f = disk
                .open(&seg_path("sm", stray), cairn_core::OpenMode::CreateTruncate)
                .await
                .unwrap();
            disk.write_at(&f, 0, Bytes::from_static(b"not a segment"))
                .await
                .unwrap();
            disk.sync(&f).await.unwrap();
            drop(a);
            let mut a = Store::open(rt.clone(), "sm", schema(), cfg).await.unwrap();
            let pending = a.pending_compactions();
            assert_eq!(pending.len(), 1);
            assert!(pending[0].1, "the committed merge's file was not kept");
            assert!(!disk.exists(&seg_path("sm", stray)).await.unwrap());
            assert_eq!(a.install_compactions().await.unwrap(), 1);
            assert_eq!(a.segments().map(|m| m.id).collect::<Vec<_>>(), vec![id]);
            for i in 1..=20u64 {
                assert_eq!(a.get(DocId(i)).await.unwrap(), Some(doc(i, 1)));
            }
        });
    }
}
