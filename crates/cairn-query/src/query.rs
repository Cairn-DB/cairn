//! Query and result types.

use crate::fusion::Fusion;
use cairn_core::{DocId, Document, Predicate};

/// One vector leg.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorLeg {
    /// Vector field position.
    pub field: usize,
    /// Query embedding.
    pub vector: Vec<f32>,
    /// Beam width for graph search (0 = default).
    pub ef: u32,
}

/// The text leg.
#[derive(Debug, Clone, PartialEq)]
pub struct TextLeg {
    /// Text field position.
    pub field: usize,
    /// Query text.
    pub text: String,
    /// Require all terms.
    pub all_terms: bool,
}

/// A hybrid query.
#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    /// Structured filter (applies to every leg).
    pub filter: Predicate,
    /// Vector legs (zero or more).
    pub vectors: Vec<VectorLeg>,
    /// Text leg (optional).
    pub text: Option<TextLeg>,
    /// Results wanted.
    pub k: usize,
    /// Fusion method.
    pub fusion: Fusion,
    /// Each leg keeps `oversample * k` candidates before fusion.
    pub oversample: usize,
    /// Score every leg exactly (f32, no SQ8), for tests and ground truth.
    pub exact: bool,
    /// Fetch documents for the final hits.
    pub with_documents: bool,
}

impl Query {
    /// A query with only a filter and `k` results (legs added by the caller).
    pub fn new(k: usize) -> Self {
        Query {
            filter: Predicate::True,
            vectors: Vec::new(),
            text: None,
            k,
            fusion: Fusion::default(),
            oversample: 4,
            exact: false,
            with_documents: false,
        }
    }

    /// Number of legs.
    pub fn leg_count(&self) -> usize {
        self.vectors.len() + usize::from(self.text.is_some())
    }

    /// Candidates kept per leg.
    pub fn per_leg(&self) -> usize {
        (self.oversample.max(1) * self.k).max(self.k)
    }
}

/// A document's rank and raw score in one leg.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LegHit {
    /// 0-based rank within the leg.
    pub rank: u32,
    /// Raw leg score (distance for vectors, BM25 for text).
    pub score: f32,
}

/// One result.
#[derive(Debug, Clone, PartialEq)]
pub struct Hit {
    /// Document id.
    pub doc_id: DocId,
    /// Fused score (higher is better).
    pub score: f32,
    /// Per-leg hit, in query leg order (vectors then text); `None` if absent from that leg.
    pub legs: Vec<Option<LegHit>>,
    /// The document, when requested.
    pub document: Option<Document>,
    /// The text id, for documents written with one (ADR 0031).
    pub key: Option<String>,
}
