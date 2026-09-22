//! Rank fusion (ADR 0007).

use crate::query::LegHit;
use cairn_core::{DocId, HashMap};

/// Fusion method.
#[derive(Debug, Clone, PartialEq)]
pub enum Fusion {
    /// Reciprocal rank fusion: `sum 1 / (k + rank)`.
    Rrf {
        /// The constant (60 in the literature).
        k: f32,
    },
    /// Weighted sum of per-leg min-max normalized scores.
    Weighted {
        /// One weight per leg, in query leg order.
        weights: Vec<f32>,
    },
}

impl Default for Fusion {
    fn default() -> Self {
        Fusion::Rrf { k: 60.0 }
    }
}

/// A leg's ranked list: `(doc, score)` best first; `higher_is_better` says how to read `score`.
#[derive(Debug, Clone, PartialEq)]
pub struct LegList {
    /// Ranked hits.
    pub hits: Vec<(DocId, f32)>,
    /// Whether larger scores are better (text) or smaller (vector distances).
    pub higher_is_better: bool,
}

/// Fuses legs into the top `k` documents (fused score descending, then doc id).
pub fn fuse(method: &Fusion, legs: &[LegList], k: usize) -> Vec<(DocId, f32, Vec<Option<LegHit>>)> {
    let mut acc: HashMap<DocId, (f32, Vec<Option<LegHit>>)> = HashMap::default();
    for (li, leg) in legs.iter().enumerate() {
        let (lo, hi) = leg
            .hits
            .iter()
            .fold((f32::INFINITY, f32::NEG_INFINITY), |(lo, hi), (_, s)| {
                (lo.min(*s), hi.max(*s))
            });
        for (rank, (doc, score)) in leg.hits.iter().enumerate() {
            let contribution = match method {
                Fusion::Rrf { k } => 1.0 / (k + rank as f32 + 1.0),
                Fusion::Weighted { weights } => {
                    let w = weights.get(li).copied().unwrap_or(1.0);
                    let norm = if hi > lo {
                        (score - lo) / (hi - lo)
                    } else {
                        1.0
                    };
                    w * if leg.higher_is_better {
                        norm
                    } else {
                        1.0 - norm
                    }
                }
            };
            let e = acc
                .entry(*doc)
                .or_insert_with(|| (0.0, vec![None; legs.len()]));
            e.0 += contribution;
            e.1[li] = Some(LegHit {
                rank: rank as u32,
                score: *score,
            });
        }
    }
    let mut out: Vec<(DocId, f32, Vec<Option<LegHit>>)> =
        acc.into_iter().map(|(d, (s, l))| (d, s, l)).collect();
    out.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
    out.truncate(k);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rrf_prefers_documents_present_in_both_legs() {
        let a = LegList {
            hits: vec![(DocId(1), 0.1), (DocId(2), 0.2), (DocId(3), 0.3)],
            higher_is_better: false,
        };
        let b = LegList {
            hits: vec![(DocId(3), 9.0), (DocId(2), 5.0), (DocId(9), 1.0)],
            higher_is_better: true,
        };
        let f = fuse(&Fusion::default(), &[a, b], 3);
        let ids: Vec<u64> = f.iter().map(|x| x.0.get()).collect();
        assert_eq!(ids, vec![3, 2, 1]);
        assert!(f[0].2[0].is_some() && f[0].2[1].is_some());
        assert!(f[2].2[1].is_none());
    }

    #[test]
    fn weighted_normalizes_within_each_leg() {
        let a = LegList {
            hits: vec![(DocId(1), 0.0), (DocId(2), 10.0)],
            higher_is_better: false,
        };
        let b = LegList {
            hits: vec![(DocId(2), 1.0), (DocId(1), 0.0)],
            higher_is_better: true,
        };
        let f = fuse(
            &Fusion::Weighted {
                weights: vec![1.0, 1.0],
            },
            &[a, b],
            2,
        );
        // doc1: vector best (1.0) + text worst (0.0) = 1.0; doc2: 0.0 + 1.0 = 1.0 -> tie broken by id.
        assert_eq!(f[0].0, DocId(1));
        assert!((f[0].1 - f[1].1).abs() < 1e-6);
        let f = fuse(
            &Fusion::Weighted {
                weights: vec![1.0, 3.0],
            },
            &[
                LegList {
                    hits: vec![(DocId(1), 0.0), (DocId(2), 10.0)],
                    higher_is_better: false,
                },
                LegList {
                    hits: vec![(DocId(2), 1.0), (DocId(1), 0.0)],
                    higher_is_better: true,
                },
            ],
            2,
        );
        assert_eq!(f[0].0, DocId(2));
    }
}
