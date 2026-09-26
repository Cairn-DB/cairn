//! The per-shard engine: store + loaded indexes + query execution.

pub use crate::fusion::LegList;
use crate::fusion::fuse;
use crate::query::{Hit, Query};
use cairn_core::{
    DocId, Document, Error, FieldKind, HashMap, LogIndex, Result, Runtime, Schema, SegmentId, Value,
};
use cairn_index::text::TextQuery;
use cairn_index::{
    Bitmap, Bm25Params, DefaultIndexer, StructuredIndex, TextIndex, VectorIndex, VectorIndexParams,
    VectorQuery,
};
use cairn_storage::{Command, MappedSegment, SegmentReader, Store, StoreConfig};

/// Engine configuration.
#[derive(Debug, Clone, Default)]
pub struct EngineConfig {
    /// Store settings.
    pub store: StoreConfig,
    /// Vector index settings.
    pub vector: VectorIndexParams,
}

struct SegmentIndexes {
    doc_ids: Vec<u64>,
    structured: StructuredIndex,
    texts: HashMap<usize, TextIndex>,
    vectors: HashMap<usize, VectorIndex>,
}

/// Indexes over the memtable, rebuilt lazily when it changes (scan-only; bounded size).
struct MemtableIndexes {
    version: u64,
    docs: Vec<Document>,
    structured: StructuredIndex,
    texts: HashMap<usize, TextIndex>,
    vectors: HashMap<usize, VectorIndex>,
}

/// One shard's engine.
pub struct ShardEngine<R: Runtime> {
    store: Store<R>,
    cfg: EngineConfig,
    indexes: HashMap<SegmentId, std::sync::Arc<SegmentIndexes>>,
    segments_version: Option<u64>,
    memtable: Option<std::sync::Arc<MemtableIndexes>>,
    /// Indexes decoded off the actor for segments not published yet (ADR 0026).
    prepared: HashMap<SegmentId, PreparedIndexes>,
}

/// Runs every leg of `q` over one segment's (or the memtable's) indexes, restricted to
/// `allowed` rows.
fn run_legs(
    q: &Query,
    per_leg: usize,
    allowed: &Bitmap,
    doc_ids: &[u64],
    vectors: &HashMap<usize, VectorIndex>,
    texts: &HashMap<usize, TextIndex>,
    legs: &mut [Vec<(DocId, f32)>],
) -> Result<()> {
    for (li, leg) in q.vectors.iter().enumerate() {
        let Some(idx) = vectors.get(&leg.field) else {
            continue;
        };
        let mut vq = VectorQuery::new(per_leg);
        vq.exact = q.exact;
        if leg.ef > 0 {
            vq.ef = leg.ef;
        }
        let (res, _) = idx.search(&leg.vector, Some(allowed), vq)?;
        legs[li].extend(
            res.into_iter()
                .map(|(d, row)| (DocId(doc_ids[row as usize]), d)),
        );
    }
    if let Some(t) = &q.text
        && let Some(idx) = texts.get(&t.field)
    {
        let res = idx.search(
            &TextQuery {
                field: t.field,
                text: t.text.clone(),
                all_terms: t.all_terms,
            },
            per_leg,
            Some(allowed),
            Bm25Params::default(),
        );
        let li = q.vectors.len();
        legs[li].extend(
            res.into_iter()
                .map(|(s, row)| (DocId(doc_ids[row as usize]), s)),
        );
    }
    Ok(())
}

/// One segment's indexes decoded off the actor (ADR 0026), waiting for its publication.
pub struct PreparedIndexes {
    structured: StructuredIndex,
    texts: HashMap<usize, TextIndex>,
    vectors: HashMap<usize, VectorIndex>,
}

/// Decodes one segment's indexes from its mapped file, on any thread (ADR 0026).
pub struct IndexJob {
    mapped: MappedSegment,
    schema: Schema,
    vector: VectorIndexParams,
    prefetch: Option<fn(&[u8])>,
}

impl IndexJob {
    /// Checks every section it reads against its hash and decodes the indexes.
    pub fn run(self) -> Result<PreparedIndexes> {
        let m = &self.mapped;
        let structured = StructuredIndex::decode(m, &self.schema)?;
        let mut texts = HashMap::default();
        let mut vectors = HashMap::default();
        for (i, f) in self.schema.fields.iter().enumerate() {
            match f.kind {
                FieldKind::Text if m.has_section(&format!("text.{i}")) => {
                    texts.insert(i, TextIndex::decode(m, i)?);
                }
                FieldKind::Vector { dims, metric } => {
                    vectors.insert(
                        i,
                        VectorIndex::decode(
                            m,
                            i,
                            metric,
                            dims as usize,
                            self.vector,
                            self.prefetch,
                        )?,
                    );
                }
                _ => {}
            }
        }
        Ok(PreparedIndexes {
            structured,
            texts,
            vectors,
        })
    }
}

/// A query's search over a snapshot of one shard (ADR 0025), from
/// [`ShardEngine::prepare_legs`]: the indexes it reads are shared with the engine and
/// immutable, and the allowed rows were fixed at capture time, so takedowns applied by then are
/// honoured. `run` needs nothing else and can run on any thread.
pub struct LegsJob {
    q: Query,
    parts: Vec<(std::sync::Arc<SegmentIndexes>, Bitmap)>,
    mem: Option<(std::sync::Arc<MemtableIndexes>, Bitmap)>,
    ready: Option<Vec<LegList>>,
}

impl LegsJob {
    fn ready(lists: Vec<LegList>) -> Self {
        LegsJob {
            q: Query::new(0),
            parts: Vec::new(),
            mem: None,
            ready: Some(lists),
        }
    }

    /// Runs the search: one ranked list per leg (vectors, then text).
    pub fn run(self) -> Result<Vec<LegList>> {
        if let Some(lists) = self.ready {
            return Ok(lists);
        }
        let q = &self.q;
        let per_leg = q.per_leg();
        let n_legs = q.leg_count();
        let mut legs: Vec<Vec<(DocId, f32)>> = vec![Vec::new(); n_legs];
        for (idx, allowed) in &self.parts {
            run_legs(
                q,
                per_leg,
                allowed,
                &idx.doc_ids,
                &idx.vectors,
                &idx.texts,
                &mut legs,
            )?;
        }
        if let Some((m, allowed)) = &self.mem {
            let ids: Vec<u64> = m.docs.iter().map(|d| d.id.get()).collect();
            run_legs(q, per_leg, allowed, &ids, &m.vectors, &m.texts, &mut legs)?;
        }
        let mut lists = Vec::with_capacity(n_legs);
        for (li, mut hits) in legs.into_iter().enumerate() {
            let higher_is_better = li >= q.vectors.len();
            if higher_is_better {
                hits.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
            } else {
                hits.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
            }
            hits.truncate(per_leg);
            lists.push(LegList {
                hits,
                higher_is_better,
            });
        }
        Ok(lists)
    }
}

impl<R: Runtime> ShardEngine<R> {
    /// Opens the shard in `dir`.
    pub async fn open(rt: R, dir: &str, schema: Schema, cfg: EngineConfig) -> Result<Self> {
        Self::open_with_limit(rt, dir, schema, cfg, None).await
    }

    /// Opens the shard, replaying the log only up to `replay_limit` (see
    /// [`Store::open_with_limit`]).
    pub async fn open_with_limit(
        rt: R,
        dir: &str,
        schema: Schema,
        cfg: EngineConfig,
        replay_limit: Option<LogIndex>,
    ) -> Result<Self> {
        let mut store =
            Store::open_with_limit(rt, dir, schema, cfg.store.clone(), replay_limit).await?;
        store.set_indexer(Box::new(DefaultIndexer {
            vector: cfg.vector,
            parallel: None,
        }));
        let mut engine = ShardEngine {
            store,
            cfg,
            indexes: HashMap::default(),
            segments_version: None,
            memtable: None,
            prepared: HashMap::default(),
        };
        engine.refresh().await?;
        Ok(engine)
    }

    /// The store.
    pub fn store(&self) -> &Store<R> {
        &self.store
    }

    /// The store, mutably (Raft drives the log through it).
    pub fn store_mut(&mut self) -> &mut Store<R> {
        &mut self.store
    }

    /// Schema.
    pub fn schema(&self) -> &Schema {
        self.store.schema()
    }

    /// Single-node write path.
    pub async fn write(&mut self, cmd: &Command) -> Result<LogIndex> {
        let idx = self.store.write(cmd).await?;
        self.refresh().await?;
        Ok(idx)
    }

    /// Flushes the memtable and reloads indexes.
    pub async fn flush(&mut self) -> Result<()> {
        self.store.flush().await?;
        self.refresh().await
    }

    /// Point read.
    pub async fn get(&self, id: DocId) -> Result<Option<Document>> {
        self.store.get(id).await
    }

    /// Loads indexes for new segments and drops indexes of removed ones.
    pub async fn refresh(&mut self) -> Result<()> {
        if self.segments_version == Some(self.store.segments_version()) {
            return Ok(());
        }
        let live: Vec<SegmentId> = self.store.segments().map(|m| m.id).collect();
        self.indexes.retain(|id, _| live.contains(id));
        let schema = self.store.schema().clone();
        for id in live {
            if self.indexes.contains_key(&id) {
                continue;
            }
            let view = self
                .store
                .segment(id)
                .ok_or_else(|| Error::Internal("segment vanished".into()))?;
            let prepared = match self.prepared.remove(&id) {
                Some(p) => p,
                None => {
                    // Not prepared (startup, snapshot install, single-node use): decode now,
                    // off the core, while the actor waits.
                    let job = IndexJob {
                        mapped: view.reader.mapped().await?,
                        schema: schema.clone(),
                        vector: self.cfg.vector,
                        prefetch: view.reader.prefetcher(),
                    };
                    view.reader.runtime().offload(move || job.run()).await?
                }
            };
            let PreparedIndexes {
                structured,
                texts,
                vectors,
            } = prepared;
            let doc_ids = view.docs.docids().to_vec();
            self.indexes.insert(
                id,
                std::sync::Arc::new(SegmentIndexes {
                    doc_ids,
                    structured,
                    texts,
                    vectors,
                }),
            );
        }
        self.segments_version = Some(self.store.segments_version());
        Ok(())
    }

    /// Prepares the indexes of segment `id`, whose file is in place but not published yet
    /// (ADR 0026): cheap here, and the returned job decodes them on any thread. The replica
    /// runs it off its actor and hands the result to [`ShardEngine::adopt_indexes`], so
    /// publishing the segment later loads nothing on the actor.
    pub async fn index_job(&self, id: SegmentId) -> Result<IndexJob> {
        let reader =
            SegmentReader::open(self.store.runtime().clone(), &self.store.segment_path(id)).await?;
        Ok(IndexJob {
            mapped: reader.mapped().await?,
            schema: self.store.schema().clone(),
            vector: self.cfg.vector,
            prefetch: reader.prefetcher(),
        })
    }

    /// Keeps prepared indexes for segment `id` until it is published.
    pub fn adopt_indexes(&mut self, id: SegmentId, indexes: PreparedIndexes) {
        self.prepared.insert(id, indexes);
    }

    /// Whether segment `id` has its indexes: prepared, or already loaded.
    pub fn indexes_ready(&self, id: SegmentId) -> bool {
        self.prepared.contains_key(&id) || self.indexes.contains_key(&id)
    }

    /// Drops prepared indexes whose segment will not be published (merged away, replaced).
    pub fn retain_prepared(&mut self, keep: impl Fn(SegmentId) -> bool) {
        self.prepared.retain(|id, _| keep(*id));
    }

    fn memtable_indexes(&mut self) -> std::sync::Arc<MemtableIndexes> {
        let version = self.store.memtable_version();
        if self.memtable.as_ref().is_none_or(|m| m.version != version) {
            let schema = self.store.schema().clone();
            let mut docs: Vec<Document> = self.store.memtable_docs();
            docs.sort_by_key(|d| d.id);
            let refs: Vec<&Document> = docs.iter().collect();
            let structured = StructuredIndex::build(&schema, &refs);
            let texts = TextIndex::build_all(&schema, &refs)
                .into_iter()
                .map(|t| (t.field(), t))
                .collect();
            let ids: Vec<u64> = docs.iter().map(|d| d.id.get()).collect();
            let mut vectors = HashMap::default();
            for (i, f) in schema.fields.iter().enumerate() {
                if let FieldKind::Vector { dims, metric } = f.kind {
                    let (rows, present) =
                        cairn_index::indexer::vector_column(&refs, i, dims as usize);
                    // Scan-only: no graph for the memtable.
                    let params = VectorIndexParams {
                        scan_max_fraction: 1.0,
                        scan_max_rows: u32::MAX,
                        ..self.cfg.vector
                    };
                    let mut idx = VectorIndex::build_scan_only(
                        i,
                        metric,
                        dims as usize,
                        rows,
                        present,
                        &ids,
                        params,
                    );
                    idx.set_params(params);
                    vectors.insert(i, idx);
                }
            }
            self.memtable = Some(std::sync::Arc::new(MemtableIndexes {
                version,
                docs,
                structured,
                texts,
                vectors,
            }));
        }
        self.memtable.clone().expect("just built")
    }

    /// Executes a hybrid query.
    pub async fn query(&mut self, q: &Query) -> Result<Vec<Hit>> {
        let lists = self.query_legs(q).await?;
        let fused = if q.leg_count() == 0 {
            // One list: the filter's matches in id order (see `query_legs`).
            lists
                .into_iter()
                .flat_map(|l| l.hits)
                .map(|(d, _)| (d, 0.0, Vec::new()))
                .collect()
        } else {
            fuse(&q.fusion, &lists, q.k)
        };
        let mut out = Vec::with_capacity(fused.len());
        for (doc_id, score, legs) in fused {
            let document = if q.with_documents {
                self.store.get(doc_id).await?
            } else {
                None
            };
            out.push(Hit {
                doc_id,
                score,
                legs,
                document,
            });
        }
        Ok(out)
    }

    /// Runs every leg of `q` over this shard and returns the per-leg candidate lists (each of
    /// size `q.per_leg()`), unfused, for a coordinator that merges several shards (ADR 0007).
    pub async fn query_legs(&mut self, q: &Query) -> Result<Vec<LegList>> {
        self.prepare_legs(q).await?.run()
    }

    /// Captures what `q` needs from the shard as it is now (ADR 0025): the loaded indexes, and
    /// per segment the rows allowed now (the filter minus deletions). The returned job runs
    /// the search without the engine, so a replica runs it off its actor, several at a time.
    pub async fn prepare_legs(&mut self, q: &Query) -> Result<LegsJob> {
        let schema = self.store.schema().clone();
        q.filter.validate(&schema)?;
        for leg in &q.vectors {
            match schema.fields.get(leg.field).map(|f| &f.kind) {
                Some(FieldKind::Vector { dims, .. }) if *dims as usize == leg.vector.len() => {}
                _ => {
                    return Err(Error::InvalidRequest(format!(
                        "vector leg field {} invalid",
                        leg.field
                    )));
                }
            }
        }
        if let Some(t) = &q.text
            && schema.fields.get(t.field).map(|f| &f.kind) != Some(&FieldKind::Text)
        {
            return Err(Error::InvalidRequest(format!(
                "text leg field {} is not a text field",
                t.field
            )));
        }
        if q.k == 0 {
            return Ok(LegsJob::ready(Vec::new()));
        }
        self.refresh().await?;
        if q.leg_count() == 0 {
            // A pure filter: one list holding the first `k` matches in id order, which a
            // coordinator merges across shards the same way.
            let hits = self
                .filter_only(q)
                .await?
                .into_iter()
                .map(|(d, _, _)| (d, 0.0))
                .collect();
            return Ok(LegsJob::ready(vec![LegList {
                hits,
                higher_is_better: true,
            }]));
        }
        let mut parts = Vec::new();
        let seg_ids: Vec<SegmentId> = self.store.segments().map(|m| m.id).collect();
        for id in seg_ids {
            let Some(view) = self.store.segment(id) else {
                continue;
            };
            let Some(idx) = self.indexes.get(&id) else {
                continue;
            };
            let mut allowed = idx.structured.evaluate(&q.filter);
            allowed.and_not_with(&Bitmap::from_deletions(view.deletions));
            if allowed.count() > 0 {
                parts.push((idx.clone(), allowed));
            }
        }
        let m = self.memtable_indexes();
        let mem = if m.docs.is_empty() {
            None
        } else {
            let allowed = m.structured.evaluate(&q.filter);
            (allowed.count() > 0).then_some((m, allowed))
        };
        Ok(LegsJob {
            q: q.clone(),
            parts,
            mem,
            ready: None,
        })
    }

    /// Documents matching the filter only (no legs), in id order, up to `q.k`.
    pub async fn filter_only_ids(&mut self, q: &Query) -> Result<Vec<DocId>> {
        self.refresh().await?;
        Ok(self
            .filter_only(q)
            .await?
            .into_iter()
            .map(|x| x.0)
            .collect())
    }

    #[allow(clippy::type_complexity)]
    async fn filter_only(
        &mut self,
        q: &Query,
    ) -> Result<Vec<(DocId, f32, Vec<Option<crate::query::LegHit>>)>> {
        let mut ids: Vec<DocId> = Vec::new();
        let seg_ids: Vec<SegmentId> = self.store.segments().map(|m| m.id).collect();
        for id in seg_ids {
            let (Some(view), Some(idx)) = (self.store.segment(id), self.indexes.get(&id)) else {
                continue;
            };
            let mut allowed = idx.structured.evaluate(&q.filter);
            allowed.and_not_with(&Bitmap::from_deletions(view.deletions));
            ids.extend(allowed.iter().map(|r| DocId(idx.doc_ids[r as usize])));
        }
        let m = self.memtable_indexes();
        let allowed = m.structured.evaluate(&q.filter);
        ids.extend(allowed.iter().map(|r| m.docs[r as usize].id));
        ids.sort_unstable();
        ids.truncate(q.k);
        Ok(ids.into_iter().map(|d| (d, 0.0, Vec::new())).collect())
    }

    /// Number of loaded segment indexes (diagnostics).
    pub fn loaded_segments(&self) -> usize {
        self.indexes.len()
    }

    /// Reference implementation used by tests: evaluates a query over `docs` with exact
    /// distances, naive BM25 per field over the same docs, and the same fusion.
    pub fn reference(schema: &Schema, docs: &[Document], q: &Query) -> Vec<DocId> {
        let refs: Vec<&Document> = docs.iter().filter(|d| q.filter.matches(d)).collect();
        let per_leg = q.per_leg();
        let mut lists = Vec::new();
        for leg in &q.vectors {
            let mut hits: Vec<(DocId, f32)> = refs
                .iter()
                .filter_map(|d| match &d.values[leg.field] {
                    Some(Value::Vector(v)) => {
                        let metric = match &schema.fields[leg.field].kind {
                            FieldKind::Vector { metric, .. } => *metric,
                            _ => unreachable!(),
                        };
                        let dist = match metric {
                            cairn_core::Metric::L2 => {
                                cairn_index::kernels::scalar::l2_sq(&leg.vector, v)
                            }
                            _ => -cairn_index::kernels::scalar::dot(&leg.vector, v),
                        };
                        Some((d.id, dist))
                    }
                    _ => None,
                })
                .collect();
            hits.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
            hits.truncate(per_leg);
            lists.push(LegList {
                hits,
                higher_is_better: false,
            });
        }
        if let Some(t) = &q.text {
            // Like the engine: statistics over every document, the filter applied as a bitmap.
            let all: Vec<&Document> = docs.iter().collect();
            let idx = TextIndex::build(t.field, &all);
            let mut allowed = Bitmap::empty(all.len() as u32);
            for (row, d) in all.iter().enumerate() {
                if q.filter.matches(d) {
                    allowed.set(row as u32);
                }
            }
            let res = idx.search(
                &TextQuery {
                    field: t.field,
                    text: t.text.clone(),
                    all_terms: t.all_terms,
                },
                per_leg,
                Some(&allowed),
                Bm25Params::default(),
            );
            lists.push(LegList {
                hits: res
                    .into_iter()
                    .map(|(s, row)| (all[row as usize].id, s))
                    .collect(),
                higher_is_better: true,
            });
        }
        if lists.is_empty() {
            let mut ids: Vec<DocId> = refs.iter().map(|d| d.id).collect();
            ids.sort_unstable();
            ids.truncate(q.k);
            return ids;
        }
        fuse(&q.fusion, &lists, q.k)
            .into_iter()
            .map(|x| x.0)
            .collect()
    }
}

/// Jobs cross to helper threads (ADR 0025, ADR 0026).
#[allow(dead_code)]
fn _jobs_are_send() {
    fn check<T: Send + 'static>() {}
    check::<LegsJob>();
    check::<IndexJob>();
    check::<PreparedIndexes>();
}
