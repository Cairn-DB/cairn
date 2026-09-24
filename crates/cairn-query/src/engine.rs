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
use cairn_storage::{Command, Store, StoreConfig};

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
    indexes: HashMap<SegmentId, SegmentIndexes>,
    segments_version: Option<u64>,
    memtable: Option<MemtableIndexes>,
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
            let structured = StructuredIndex::load(view.reader, &schema).await?;
            let mut texts = HashMap::default();
            let mut vectors = HashMap::default();
            for (i, f) in schema.fields.iter().enumerate() {
                match f.kind {
                    FieldKind::Text if view.reader.has_section(&format!("text.{i}")) => {
                        texts.insert(i, TextIndex::load(view.reader, i).await?);
                    }
                    FieldKind::Vector { dims, metric } => {
                        vectors.insert(
                            i,
                            VectorIndex::load(
                                view.reader,
                                i,
                                metric,
                                dims as usize,
                                self.cfg.vector,
                            )
                            .await?,
                        );
                    }
                    _ => {}
                }
            }
            let doc_ids = view.docs.docids().to_vec();
            self.indexes.insert(
                id,
                SegmentIndexes {
                    doc_ids,
                    structured,
                    texts,
                    vectors,
                },
            );
        }
        self.segments_version = Some(self.store.segments_version());
        Ok(())
    }

    fn memtable_indexes(&mut self) -> &MemtableIndexes {
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
            self.memtable = Some(MemtableIndexes {
                version,
                docs,
                structured,
                texts,
                vectors,
            });
        }
        self.memtable.as_ref().expect("just built")
    }

    /// Executes a hybrid query.
    pub async fn query(&mut self, q: &Query) -> Result<Vec<Hit>> {
        let lists = self.query_legs(q).await?;
        let fused = if q.leg_count() == 0 {
            self.filter_only(q).await?
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
            return Ok(Vec::new());
        }
        self.refresh().await?;
        let per_leg = q.per_leg();
        let n_legs = q.leg_count();
        let mut legs: Vec<Vec<(DocId, f32)>> = vec![Vec::new(); n_legs];
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
            if allowed.count() == 0 {
                continue;
            }
            Self::run_legs(
                q,
                per_leg,
                &allowed,
                &idx.doc_ids,
                &idx.vectors,
                &idx.texts,
                &mut legs,
            )?;
        }
        {
            let m = self.memtable_indexes();
            if !m.docs.is_empty() {
                let allowed = m.structured.evaluate(&q.filter);
                if allowed.count() > 0 {
                    let ids: Vec<u64> = m.docs.iter().map(|d| d.id.get()).collect();
                    Self::run_legs(q, per_leg, &allowed, &ids, &m.vectors, &m.texts, &mut legs)?;
                }
            }
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
