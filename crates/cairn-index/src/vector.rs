//! Per-field vector index of one segment: vectors + SQ8 + HNSW, with the selectivity-adaptive
//! dispatch of ADR 0003.

use crate::bitmap::Bitmap;
use crate::hnsw::{Hnsw, HnswBuilder, HnswParams, SearchOptions, SearchScratch};
use crate::scan::{TopK, exact_scan};
use crate::vectors::Vectors;
use cairn_core::{Metric, Result, Runtime};
use cairn_storage::SegmentReader;
use std::cell::RefCell;

/// Build-time parameters.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VectorIndexParams {
    /// HNSW parameters.
    pub hnsw: HnswParams,
    /// Keep an SQ8 copy and use it for candidate scoring.
    pub sq8: bool,
    /// Scan exactly when the filter passes at most this many rows...
    pub scan_max_rows: u32,
    /// ...or at most this fraction of the segment.
    pub scan_max_fraction: f32,
    /// Use two-hop expansion when the filter passes less than this fraction.
    pub two_hop_below_fraction: f32,
}

impl Default for VectorIndexParams {
    fn default() -> Self {
        VectorIndexParams {
            hnsw: HnswParams::default(),
            sq8: true,
            scan_max_rows: 20_000,
            scan_max_fraction: 0.10,
            two_hop_below_fraction: 0.30,
        }
    }
}

/// Which path answered a query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Strategy {
    /// Exact scan over the filter.
    Scan,
    /// Graph search with per-candidate filter check.
    Hnsw,
    /// Graph search with two-hop expansion.
    HnswTwoHop,
}

/// Per-query options.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VectorQuery {
    /// Results wanted.
    pub k: usize,
    /// Beam width for the graph path.
    pub ef: u32,
    /// Force a strategy instead of the adaptive choice.
    pub force: Option<Strategy>,
    /// Candidates re-scored exactly after SQ8 scoring.
    pub rerank: usize,
    /// Use f32 everywhere (no SQ8 candidate scoring); the reference for recall measurements.
    pub exact: bool,
    /// Upper bound on distance computations for graph paths (`u32::MAX` = unlimited).
    pub max_visits: u32,
}

impl VectorQuery {
    /// Defaults for `k` results: `ef = max(64, 4k)`, rerank `4k`.
    pub fn new(k: usize) -> Self {
        VectorQuery {
            k,
            ef: (4 * k as u32).max(64),
            force: None,
            rerank: 4 * k,
            exact: false,
            max_visits: u32::MAX,
        }
    }
}

/// The index.
pub struct VectorIndex {
    field: usize,
    vectors: Vectors,
    present: Bitmap,
    hnsw: Option<Hnsw>,
    params: VectorIndexParams,
    scratch: RefCell<SearchScratch>,
}

impl VectorIndex {
    /// Builds the index for schema field `field` from row-major `rows`; `present` marks rows
    /// with a non-null vector. Synchronous (see [`VectorIndex::builder`] for chunked builds).
    pub fn build(
        field: usize,
        metric: Metric,
        dims: usize,
        rows: Vec<f32>,
        present: Bitmap,
        doc_ids: &[u64],
        params: VectorIndexParams,
    ) -> Self {
        let vectors = Vectors::from_rows(metric, dims, rows, params.sq8);
        let hnsw = if vectors.is_empty() {
            None
        } else {
            Some(HnswBuilder::new(&vectors, doc_ids, params.hnsw).finish())
        };
        let n = vectors.len();
        VectorIndex {
            field,
            vectors,
            present,
            hnsw,
            params,
            scratch: RefCell::new(SearchScratch::new(n)),
        }
    }

    /// Sections to add to the segment: `sq8.<field>` (if any) and `hnsw.<field>`.
    pub fn sections(&self) -> Vec<(String, Vec<u8>)> {
        let mut v = Vec::new();
        if let Some(b) = self.vectors.encode_sq8() {
            v.push((format!("sq8.{}", self.field), b));
        }
        if let Some(h) = &self.hnsw {
            v.push((format!("hnsw.{}", self.field), h.encode()));
        }
        v
    }

    /// Loads the index of `field` from a segment (f32 rows from `col.<field>`, presence from
    /// `nulls.<field>`, SQ8 and graph from their sections).
    pub async fn load<R: Runtime>(
        reader: &SegmentReader<R>,
        field: usize,
        metric: Metric,
        dims: usize,
        params: VectorIndexParams,
    ) -> Result<Self> {
        let col = reader.read_section(&format!("col.{field}")).await?;
        let nulls = reader.read_section(&format!("nulls.{field}")).await?;
        let n = nulls.len() as u32;
        if col.len() != n as usize * dims * 4 {
            return Err(cairn_core::Error::corruption(format!(
                "vector column {field} length"
            )));
        }
        let rows: Vec<f32> = col
            .chunks_exact(4)
            .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect();
        let mut present = Bitmap::empty(n);
        for (i, &p) in nulls.iter().enumerate() {
            if p != 0 {
                present.set(i as u32);
            }
        }
        let sq8_name = format!("sq8.{field}");
        let sq8 = if reader.has_section(&sq8_name) {
            Some(reader.read_section(&sq8_name).await?)
        } else {
            None
        };
        let vectors = Vectors::from_rows_and_sq8(metric, dims, rows, sq8.as_deref())?;
        let hnsw_name = format!("hnsw.{field}");
        let hnsw = if reader.has_section(&hnsw_name) {
            let h = Hnsw::decode(&reader.read_section(&hnsw_name).await?)?;
            if h.len() != n {
                return Err(cairn_core::Error::corruption("hnsw row count mismatch"));
            }
            Some(h)
        } else {
            None
        };
        Ok(VectorIndex {
            field,
            vectors,
            present,
            hnsw,
            params,
            scratch: RefCell::new(SearchScratch::new(n)),
        })
    }

    /// Number of rows.
    pub fn len(&self) -> u32 {
        self.vectors.len()
    }

    /// Whether there are no rows.
    pub fn is_empty(&self) -> bool {
        self.vectors.is_empty()
    }

    /// The vectors.
    pub fn vectors(&self) -> &Vectors {
        &self.vectors
    }

    /// The graph, if built.
    pub fn hnsw(&self) -> Option<&Hnsw> {
        self.hnsw.as_ref()
    }

    /// Rows with a non-null vector.
    pub fn present(&self) -> &Bitmap {
        &self.present
    }

    /// Chooses the strategy for a filter passing `count` rows.
    pub fn choose(&self, count: u32) -> Strategy {
        let n = self.vectors.len().max(1);
        let frac = count as f32 / n as f32;
        if self.hnsw.is_none()
            || count <= self.params.scan_max_rows
            || frac <= self.params.scan_max_fraction
        {
            Strategy::Scan
        } else if frac < self.params.two_hop_below_fraction {
            Strategy::HnswTwoHop
        } else {
            Strategy::Hnsw
        }
    }

    /// Searches. `filter` lists allowed rows (deletions already removed); `None` allows all.
    /// Returns `(distance, row)` ascending (lower is better for every metric) and the strategy used.
    pub fn search(
        &self,
        query: &[f32],
        filter: Option<&Bitmap>,
        q: VectorQuery,
    ) -> Result<(Vec<(f32, u32)>, Strategy)> {
        let qv = self.vectors.prepare_query(query)?;
        // Effective filter: requested rows AND present rows.
        let owned;
        let eff: Option<&Bitmap> = match filter {
            Some(f) => {
                let mut b = f.clone();
                b.and_with(&self.present);
                owned = b;
                Some(&owned)
            }
            None if self.present.count() == self.vectors.len() => None,
            None => Some(&self.present),
        };
        let count = eff.map_or(self.vectors.len(), Bitmap::count);
        let strategy = q.force.unwrap_or_else(|| self.choose(count));
        let res = match strategy {
            Strategy::Scan => exact_scan(&self.vectors, &qv, q.k, eff, q.exact, q.rerank),
            Strategy::Hnsw | Strategy::HnswTwoHop => {
                let h = self.hnsw.as_ref().expect("strategy needs a graph");
                let opts = SearchOptions {
                    ef: q.ef.max(q.rerank as u32),
                    two_hop: strategy == Strategy::HnswTwoHop,
                    max_visits: q.max_visits,
                    exact: q.exact,
                };
                let mut scratch = self.scratch.borrow_mut();
                let cands = h.search(
                    &self.vectors,
                    &qv,
                    q.rerank.max(q.k),
                    eff,
                    opts,
                    &mut scratch,
                );
                if self.vectors.has_sq8() && !q.exact {
                    let rows: Vec<u32> = cands.iter().map(|c| c.1).collect();
                    let mut d = vec![0f32; rows.len()];
                    let (mut g8, mut g32) = (Vec::new(), Vec::new());
                    self.vectors
                        .distances_to(&qv, &rows, true, &mut g8, &mut g32, &mut d);
                    let mut top = TopK::new(q.k);
                    for (i, &r) in rows.iter().enumerate() {
                        top.push(d[i], r);
                    }
                    top.into_sorted()
                } else {
                    cands.into_iter().take(q.k).collect()
                }
            }
        };
        Ok((res, strategy))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_core::SeededRng;

    /// Gaussian-ish blobs: `clusters` centers in `dims` dimensions, points around them.
    fn blobs(seed: u64, n: usize, dims: usize, clusters: usize) -> (Vec<f32>, Vec<u32>) {
        let mut r = SeededRng::from_seed(seed);
        let mut unit = || r.unit_f64() as f32 * 2.0 - 1.0;
        let centers: Vec<f32> = (0..clusters * dims).map(|_| unit() * 10.0).collect();
        let mut rows = Vec::with_capacity(n * dims);
        let mut labels = Vec::with_capacity(n);
        for i in 0..n {
            let c = i % clusters;
            labels.push(c as u32);
            for d in 0..dims {
                rows.push(centers[c * dims + d] + unit());
            }
        }
        (rows, labels)
    }

    fn brute(vectors: &Vectors, q: &[f32], k: usize, filter: Option<&Bitmap>) -> Vec<u32> {
        exact_scan(vectors, q, k, filter, true, k)
            .into_iter()
            .map(|(_, r)| r)
            .collect()
    }

    fn recall(got: &[(f32, u32)], truth: &[u32]) -> f64 {
        let hits = got.iter().filter(|(_, r)| truth.contains(r)).count();
        hits as f64 / truth.len() as f64
    }

    fn build(
        seed: u64,
        n: usize,
        dims: usize,
        clusters: usize,
        sq8: bool,
    ) -> (VectorIndex, Vec<u32>) {
        let (rows, labels) = blobs(seed, n, dims, clusters);
        let ids: Vec<u64> = (0..n as u64).map(|i| i * 7 + 3).collect();
        let params = VectorIndexParams {
            sq8,
            scan_max_rows: 500,
            scan_max_fraction: 0.05,
            ..VectorIndexParams::default()
        };
        (
            VectorIndex::build(
                0,
                Metric::L2,
                dims,
                rows,
                Bitmap::full(n as u32),
                &ids,
                params,
            ),
            labels,
        )
    }

    fn queries(index: &VectorIndex, seed: u64, count: usize) -> Vec<Vec<f32>> {
        let mut r = SeededRng::from_seed(seed);
        (0..count)
            .map(|_| {
                let row = r.below(u64::from(index.len())) as u32;
                index
                    .vectors()
                    .row_f32(row)
                    .iter()
                    .map(|x| x + (r.unit_f64() as f32 - 0.5) * 0.2)
                    .collect()
            })
            .collect()
    }

    #[test]
    fn unfiltered_recall_is_high_and_build_is_deterministic() {
        let (index, _) = build(1, 20_000, 32, 50, true);
        let (index2, _) = build(1, 20_000, 32, 50, true);
        assert_eq!(
            index.hnsw().unwrap().encode(),
            index2.hnsw().unwrap().encode()
        );
        let qs = queries(&index, 2, 100);
        let mut total = 0.0;
        for q in &qs {
            let truth = brute(index.vectors(), q, 10, None);
            let (got, s) = index
                .search(
                    q,
                    None,
                    VectorQuery {
                        force: Some(Strategy::Hnsw),
                        ..VectorQuery::new(10)
                    },
                )
                .unwrap();
            assert_eq!(s, Strategy::Hnsw);
            assert_eq!(got.len(), 10);
            assert!(got.windows(2).all(|w| w[0].0 <= w[1].0));
            total += recall(&got, &truth);
        }
        let r = total / qs.len() as f64;
        assert!(r >= 0.9, "unfiltered recall@10 = {r}");
    }

    #[test]
    fn filtered_paths_agree_with_brute_force() {
        let (index, labels) = build(3, 20_000, 32, 50, true);
        let n = index.len();
        let qs = queries(&index, 4, 60);
        // Random 10% filter and clustered 2% filter (one cluster).
        let mut r = SeededRng::from_seed(9);
        let mut random10 = Bitmap::empty(n);
        for i in 0..n {
            if r.chance(0.10) {
                random10.set(i);
            }
        }
        let mut cluster = Bitmap::empty(n);
        for (i, &l) in labels.iter().enumerate() {
            if l == 7 {
                cluster.set(i as u32);
            }
        }
        for (name, f, expect_strategy) in [
            ("random10", &random10, Strategy::HnswTwoHop),
            ("cluster", &cluster, Strategy::Scan),
        ] {
            let mut totals = [0.0f64; 3];
            for q in &qs {
                let truth = brute(index.vectors(), q, 10, Some(f));
                let (adaptive, s) = index.search(q, Some(f), VectorQuery::new(10)).unwrap();
                assert_eq!(s, expect_strategy, "{name}");
                assert!(
                    adaptive.iter().all(|(_, r)| f.contains(*r)),
                    "{name}: result outside filter"
                );
                totals[0] += recall(&adaptive, &truth);
                let (scan, _) = index
                    .search(
                        q,
                        Some(f),
                        VectorQuery {
                            force: Some(Strategy::Scan),
                            ..VectorQuery::new(10)
                        },
                    )
                    .unwrap();
                totals[1] += recall(&scan, &truth);
                let (hnsw, _) = index
                    .search(
                        q,
                        Some(f),
                        VectorQuery {
                            force: Some(Strategy::HnswTwoHop),
                            ..VectorQuery::new(10)
                        },
                    )
                    .unwrap();
                assert!(hnsw.iter().all(|(_, r)| f.contains(*r)));
                totals[2] += recall(&hnsw, &truth);
            }
            let m = qs.len() as f64;
            let (adaptive, scan, hnsw) = (totals[0] / m, totals[1] / m, totals[2] / m);
            assert!(
                scan >= 0.999,
                "{name}: scan with rerank should be exact, got {scan}"
            );
            assert!(adaptive >= 0.85, "{name}: adaptive recall {adaptive}");
            eprintln!("{name}: adaptive={adaptive:.3} scan={scan:.3} hnsw2hop={hnsw:.3}");
        }
    }

    #[test]
    fn sections_roundtrip_through_a_segment() {
        use cairn_core::NodeId;
        use cairn_sim::{SimConfig, Simulation};
        use cairn_storage::columns::write_columns;
        use cairn_storage::{SegmentReader, SegmentWriter};
        let (index, _) = build(5, 3_000, 16, 10, true);
        let n = index.len() as usize;
        let schema = cairn_core::Schema::new(vec![cairn_core::FieldDef {
            name: "v".into(),
            kind: cairn_core::FieldKind::Vector {
                dims: 16,
                metric: Metric::L2,
            },
        }])
        .unwrap();
        let docs: Vec<cairn_core::Document> = (0..n)
            .map(|i| {
                cairn_core::Document::new(cairn_core::DocId(i as u64), 1).set(
                    0,
                    cairn_core::Value::Vector(index.vectors().row_f32(i as u32).to_vec()),
                )
            })
            .collect();
        let (sim, mut ex) = Simulation::new(1, SimConfig::default());
        let rt = sim.runtime(NodeId(1), &ex.handle());
        let sections = index.sections();
        let q = queries(&index, 6, 5);
        let expected: Vec<Vec<(f32, u32)>> = q
            .iter()
            .map(|q| {
                index
                    .search(
                        q,
                        None,
                        VectorQuery {
                            force: Some(Strategy::Hnsw),
                            ..VectorQuery::new(5)
                        },
                    )
                    .unwrap()
                    .0
            })
            .collect();
        ex.block_on(async move {
            let refs: Vec<&cairn_core::Document> = docs.iter().collect();
            let mut w = SegmentWriter::create(rt.clone(), "s.seg").await.unwrap();
            write_columns(&mut w, &schema, &refs).await.unwrap();
            for (name, bytes) in &sections {
                w.add_section(name, bytes).await.unwrap();
            }
            w.finish().await.unwrap();
            let reader = SegmentReader::open(rt.clone(), "s.seg").await.unwrap();
            let loaded =
                VectorIndex::load(&reader, 0, Metric::L2, 16, VectorIndexParams::default())
                    .await
                    .unwrap();
            assert_eq!(loaded.hnsw().unwrap(), index.hnsw().unwrap());
            assert_eq!(loaded.vectors(), index.vectors());
            for (qv, exp) in q.iter().zip(&expected) {
                let got = loaded
                    .search(
                        qv,
                        None,
                        VectorQuery {
                            force: Some(Strategy::Hnsw),
                            ..VectorQuery::new(5)
                        },
                    )
                    .unwrap()
                    .0;
                assert_eq!(&got, exp);
            }
        });
    }

    #[test]
    fn cosine_and_dot_metrics_order_by_similarity() {
        let rows = vec![1.0, 0.0, 0.0, 1.0, 0.7, 0.7, -1.0, 0.0];
        let ids = [1u64, 2, 3, 4];
        for metric in [Metric::Cosine, Metric::Dot] {
            let idx = VectorIndex::build(
                0,
                metric,
                2,
                rows.clone(),
                Bitmap::full(4),
                &ids,
                VectorIndexParams {
                    sq8: false,
                    ..VectorIndexParams::default()
                },
            );
            let (got, _) = idx.search(&[1.0, 0.1], None, VectorQuery::new(4)).unwrap();
            let order: Vec<u32> = got.iter().map(|(_, r)| *r).collect();
            assert_eq!(order[0], 0, "{metric:?}: most similar first");
            assert_eq!(
                *order.last().unwrap(),
                3,
                "{metric:?}: opposite vector last"
            );
        }
    }
}
