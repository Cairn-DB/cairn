//! The [`SegmentIndexer`] that builds every index section: structured, text, and one vector
//! index per vector field.

use crate::bitmap::Bitmap;
use crate::structured::StructuredIndex;
use crate::text::TextIndex;
use crate::vector::{VectorIndex, VectorIndexParams};
use cairn_core::{Document, FieldKind, Result, Schema, Value};
use cairn_storage::SegmentIndexer;

/// Builds all index sections.
#[derive(Debug, Clone, Default)]
pub struct DefaultIndexer {
    /// Vector index parameters.
    pub vector: VectorIndexParams,
    /// Threads for graph builds (`None`: the calling thread). The sections are identical
    /// either way (ADR 0019).
    pub parallel: Option<std::sync::Arc<dyn cairn_core::Parallel>>,
}

/// Extracts a vector field as a dense matrix (zero rows for nulls) plus its presence bitmap.
pub fn vector_column(docs: &[&Document], field: usize, dims: usize) -> (Vec<f32>, Bitmap) {
    let mut rows = Vec::with_capacity(docs.len() * dims);
    let mut present = Bitmap::empty(docs.len() as u32);
    for (i, d) in docs.iter().enumerate() {
        match &d.values[field] {
            Some(Value::Vector(v)) if v.len() == dims => {
                rows.extend_from_slice(v);
                present.set(i as u32);
            }
            _ => rows.extend(std::iter::repeat_n(0.0, dims)),
        }
    }
    (rows, present)
}

impl SegmentIndexer for DefaultIndexer {
    fn sections(&self, schema: &Schema, docs: &[&Document]) -> Result<Vec<(String, Vec<u8>)>> {
        let mut out = StructuredIndex::build(schema, docs).sections();
        for t in TextIndex::build_all(schema, docs) {
            out.push(t.section());
        }
        let ids: Vec<u64> = docs.iter().map(|d| d.id.get()).collect();
        for (i, f) in schema.fields.iter().enumerate() {
            if let FieldKind::Vector { dims, metric } = f.kind {
                let (rows, present) = vector_column(docs, i, dims as usize);
                let par: &dyn cairn_core::Parallel = match &self.parallel {
                    Some(p) => p.as_ref(),
                    None => &cairn_core::Sequential,
                };
                let idx = VectorIndex::build_with(
                    i,
                    metric,
                    dims as usize,
                    rows,
                    present,
                    &ids,
                    self.vector,
                    par,
                );
                out.extend(idx.sections());
            }
        }
        Ok(out)
    }
}
