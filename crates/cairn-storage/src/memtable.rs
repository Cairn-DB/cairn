//! In-memory tail of a shard: documents applied since the last flush.

use cairn_core::{DocId, Document, HashMap, HashSet, LogIndex};

/// Documents and tombstones not yet in a segment.
#[derive(Debug, Default)]
pub struct Memtable {
    docs: HashMap<DocId, Document>,
    tombstones: HashSet<DocId>,
    bytes: usize,
    first_index: Option<LogIndex>,
    last_index: Option<LogIndex>,
}

fn doc_size(d: &Document) -> usize {
    let mut n = 16;
    for v in d.values.iter().flatten() {
        n += match v {
            cairn_core::Value::Vector(x) => 4 * x.len(),
            cairn_core::Value::Text(s) | cairn_core::Value::Enum(s) => s.len(),
            cairn_core::Value::Set(items) => items.iter().map(|s| s.len() + 4).sum(),
            cairn_core::Value::Blob(b) => b.len(),
            _ => 8,
        } + 8;
    }
    n
}

impl Memtable {
    /// Empty memtable.
    pub fn new() -> Self {
        Memtable::default()
    }

    fn note_index(&mut self, index: LogIndex) {
        if self.first_index.is_none() {
            self.first_index = Some(index);
        }
        self.last_index = Some(index);
    }

    /// Inserts or replaces `doc` (applied at log `index`).
    pub fn upsert(&mut self, doc: Document, index: LogIndex) {
        self.note_index(index);
        self.tombstones.remove(&doc.id);
        self.bytes += doc_size(&doc);
        if let Some(old) = self.docs.insert(doc.id, doc) {
            self.bytes -= doc_size(&old);
        }
    }

    /// Removes `id` (applied at log `index`).
    pub fn delete(&mut self, id: DocId, index: LogIndex) {
        self.note_index(index);
        if let Some(old) = self.docs.remove(&id) {
            self.bytes -= doc_size(&old);
        }
        self.tombstones.insert(id);
    }

    /// Lookup: `Some(Some(doc))` present, `Some(None)` deleted here, `None` unknown here.
    pub fn get(&self, id: DocId) -> Option<Option<&Document>> {
        if let Some(d) = self.docs.get(&id) {
            Some(Some(d))
        } else if self.tombstones.contains(&id) {
            Some(None)
        } else {
            None
        }
    }

    /// Live documents sorted by id.
    pub fn sorted_docs(&self) -> Vec<&Document> {
        let mut v: Vec<&Document> = self.docs.values().collect();
        v.sort_by_key(|d| d.id);
        v
    }

    /// Number of live documents.
    pub fn len(&self) -> usize {
        self.docs.len()
    }

    /// Whether there are neither documents nor tombstones.
    pub fn is_empty(&self) -> bool {
        self.docs.is_empty() && self.tombstones.is_empty()
    }

    /// Approximate bytes held.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Log range covered: `(first, last)`.
    pub fn log_range(&self) -> Option<(LogIndex, LogIndex)> {
        Some((self.first_index?, self.last_index?))
    }

    /// Forgets everything.
    pub fn clear(&mut self) {
        *self = Memtable::default();
    }
}
