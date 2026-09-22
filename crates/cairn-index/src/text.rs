//! Full text: tokenizer, per-segment inverted index, BM25 scoring (ADR 0005).
//!
//! Tokenizer: Unicode letters/digits runs, lowercased (no stemming in v1). Index sections per
//! text field: `text.<field>` holding the sorted term dictionary, per-term postings
//! `(row, term frequency)` sorted by row, and per-row lengths. Scoring is BM25 with `k1 = 1.2`,
//! `b = 0.75` and statistics local to the segment (documented bias, ADR 0007).

use crate::bitmap::Bitmap;
use crate::scan::TopK;
use cairn_core::codec::{Reader, Writer};
use cairn_core::{Document, Error, FieldKind, Result, Runtime, Schema, Value};
use cairn_storage::SegmentReader;

/// BM25 parameters.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bm25Params {
    /// Term-frequency saturation.
    pub k1: f32,
    /// Length normalization.
    pub b: f32,
}

impl Default for Bm25Params {
    fn default() -> Self {
        Bm25Params { k1: 1.2, b: 0.75 }
    }
}

/// Splits `text` into lowercase tokens of letters and digits.
pub fn tokenize(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    for c in text.chars() {
        if c.is_alphanumeric() {
            for l in c.to_lowercase() {
                cur.push(l);
            }
        } else if !cur.is_empty() {
            out.push(std::mem::take(&mut cur));
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

/// Inverted index of one text field in one segment.
#[derive(Debug, Clone, PartialEq)]
pub struct TextIndex {
    field: usize,
    rows: u32,
    terms: Vec<String>,
    /// Per term: `(row, tf)` sorted by row.
    postings: Vec<Vec<(u32, u32)>>,
    /// Tokens per row (0 for null).
    lengths: Vec<u32>,
    avg_len: f32,
}

/// A text query: terms (OR semantics with BM25 sum; `required` demands every term).
#[derive(Debug, Clone, PartialEq)]
pub struct TextQuery {
    /// Field position.
    pub field: usize,
    /// Raw query text; tokenized like documents.
    pub text: String,
    /// Require every term to be present.
    pub all_terms: bool,
}

impl TextIndex {
    /// Builds the index for `field` over `docs` (row = position).
    pub fn build(field: usize, docs: &[&Document]) -> Self {
        Self::build_from_texts(
            field,
            docs.iter().map(|d| match &d.values[field] {
                Some(Value::Text(t)) => Some(t.as_str()),
                _ => None,
            }),
        )
    }

    /// Builds from one optional text per row. Terms are interned to ids while tokenizing so the
    /// intermediate state is three `u32`s per (row, term) pair, not a `String`.
    pub fn build_from_texts<'a>(
        field: usize,
        texts: impl Iterator<Item = Option<&'a str>>,
    ) -> Self {
        let mut intern: cairn_core::HashMap<String, u32> = cairn_core::HashMap::default();
        let mut names: Vec<String> = Vec::new();
        let mut lengths: Vec<u32> = Vec::new();
        // (term id, row, tf)
        let mut triples: Vec<(u32, u32, u32)> = Vec::new();
        let mut toks: Vec<u32> = Vec::new();
        for (row, text) in texts.enumerate() {
            let Some(text) = text else {
                lengths.push(0);
                continue;
            };
            toks.clear();
            for t in tokenize(text) {
                let id = match intern.get(&t) {
                    Some(&id) => id,
                    None => {
                        let id = names.len() as u32;
                        names.push(t.clone());
                        intern.insert(t, id);
                        id
                    }
                };
                toks.push(id);
            }
            lengths.push(toks.len() as u32);
            toks.sort_unstable();
            let mut i = 0;
            while i < toks.len() {
                let mut j = i;
                while j < toks.len() && toks[j] == toks[i] {
                    j += 1;
                }
                triples.push((toks[i], row as u32, (j - i) as u32));
                i = j;
            }
        }
        drop(intern);
        // Sort terms lexicographically and remap ids to sorted positions.
        let mut order: Vec<u32> = (0..names.len() as u32).collect();
        order.sort_by(|a, b| names[*a as usize].cmp(&names[*b as usize]));
        let mut rank = vec![0u32; names.len()];
        for (r, &id) in order.iter().enumerate() {
            rank[id as usize] = r as u32;
        }
        for t in &mut triples {
            t.0 = rank[t.0 as usize];
        }
        triples.sort_unstable();
        let terms: Vec<String> = order
            .iter()
            .map(|&id| std::mem::take(&mut names[id as usize]))
            .collect();
        let mut postings: Vec<Vec<(u32, u32)>> = vec![Vec::new(); terms.len()];
        for (t, row, tf) in triples {
            postings[t as usize].push((row, tf));
        }
        let rows = lengths.len() as u32;
        let total: u64 = lengths.iter().map(|&l| u64::from(l)).sum();
        let non_empty = lengths.iter().filter(|&&l| l > 0).count().max(1);
        TextIndex {
            field,
            rows,
            terms,
            postings,
            lengths,
            avg_len: total as f32 / non_empty as f32,
        }
    }

    /// Builds indexes for every text field of `schema`.
    pub fn build_all(schema: &Schema, docs: &[&Document]) -> Vec<TextIndex> {
        schema
            .fields
            .iter()
            .enumerate()
            .filter(|(_, f)| f.kind == FieldKind::Text)
            .map(|(i, _)| TextIndex::build(i, docs))
            .collect()
    }

    /// Section `text.<field>`.
    pub fn section(&self) -> (String, Vec<u8>) {
        let mut w = Writer::new();
        w.u32(self.rows).u32(self.terms.len() as u32);
        for (t, p) in self.terms.iter().zip(&self.postings) {
            w.str(t).u32(p.len() as u32);
            for (row, tf) in p {
                w.u32(*row).u32(*tf);
            }
        }
        for l in &self.lengths {
            w.u32(*l);
        }
        (format!("text.{}", self.field), w.into_vec())
    }

    /// Loads the index of `field` from a segment.
    pub async fn load<R: Runtime>(reader: &SegmentReader<R>, field: usize) -> Result<Self> {
        let bytes = reader.read_section(&format!("text.{field}")).await?;
        let mut r = Reader::new(&bytes);
        let rows = r.u32()?;
        let n = r.u32()? as usize;
        let mut terms = Vec::with_capacity(n.min(1 << 20));
        let mut postings = Vec::with_capacity(n.min(1 << 20));
        for _ in 0..n {
            let t = r.str()?.to_owned();
            if terms.last().is_some_and(|l: &String| l >= &t) {
                return Err(Error::corruption("term dictionary not sorted"));
            }
            terms.push(t);
            let m = r.u32()? as usize;
            let mut p = Vec::with_capacity(m.min(1 << 20));
            for _ in 0..m {
                let row = r.u32()?;
                let tf = r.u32()?;
                if row >= rows || tf == 0 || p.last().is_some_and(|(l, _)| *l >= row) {
                    return Err(Error::corruption("postings invalid"));
                }
                p.push((row, tf));
            }
            postings.push(p);
        }
        let mut lengths = Vec::with_capacity(rows as usize);
        for _ in 0..rows {
            lengths.push(r.u32()?);
        }
        r.finish()?;
        let total: u64 = lengths.iter().map(|&l| u64::from(l)).sum();
        let non_empty = lengths.iter().filter(|&&l| l > 0).count().max(1);
        Ok(TextIndex {
            field,
            rows,
            terms,
            postings,
            lengths,
            avg_len: total as f32 / non_empty as f32,
        })
    }

    /// Field position.
    pub fn field(&self) -> usize {
        self.field
    }

    /// Number of distinct terms.
    pub fn term_count(&self) -> usize {
        self.terms.len()
    }

    /// Document frequency of `term`.
    pub fn doc_freq(&self, term: &str) -> u32 {
        self.terms
            .binary_search_by(|t| t.as_str().cmp(term))
            .map_or(0, |i| self.postings[i].len() as u32)
    }

    /// BM25 top-`k` rows for `query` among `filter` (all rows when `None`). Scores are positive;
    /// results are `(score, row)` descending.
    pub fn search(
        &self,
        query: &TextQuery,
        k: usize,
        filter: Option<&Bitmap>,
        params: Bm25Params,
    ) -> Vec<(f32, u32)> {
        let mut terms = tokenize(&query.text);
        terms.sort();
        terms.dedup();
        if terms.is_empty() || self.rows == 0 {
            return Vec::new();
        }
        let n = self.rows as f32;
        // Per-row accumulated score and matched-term count (dense arrays; segments are bounded).
        let mut score = vec![0f32; self.rows as usize];
        let mut matched = vec![0u16; self.rows as usize];
        let mut touched: Vec<u32> = Vec::new();
        let mut any = false;
        for t in &terms {
            let Ok(i) = self.terms.binary_search_by(|x| x.as_str().cmp(t)) else {
                if query.all_terms {
                    return Vec::new();
                }
                continue;
            };
            any = true;
            let df = self.postings[i].len() as f32;
            let idf = ((n - df + 0.5) / (df + 0.5) + 1.0).ln();
            for &(row, tf) in &self.postings[i] {
                if filter.is_some_and(|f| !f.contains(row)) {
                    continue;
                }
                let tf = tf as f32;
                let len = self.lengths[row as usize] as f32;
                let denom = tf + params.k1 * (1.0 - params.b + params.b * len / self.avg_len);
                let s = idf * tf * (params.k1 + 1.0) / denom;
                if score[row as usize] == 0.0 {
                    touched.push(row);
                }
                score[row as usize] += s;
                matched[row as usize] += 1;
            }
        }
        if !any {
            return Vec::new();
        }
        // TopK keeps the smallest, so negate.
        let mut top = TopK::new(k);
        let need = terms.len() as u16;
        for &row in &touched {
            if query.all_terms && matched[row as usize] < need {
                continue;
            }
            top.push(-score[row as usize], row);
        }
        top.into_sorted()
            .into_iter()
            .map(|(s, r)| (-s, r))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_core::DocId;

    fn doc(i: u64, text: &str) -> Document {
        Document::new(DocId(i), 1).set(0, Value::Text(text.into()))
    }

    fn naive_bm25(docs: &[Document], query: &str, all: bool, p: Bm25Params) -> Vec<(f32, u32)> {
        let toks: Vec<Vec<String>> = docs
            .iter()
            .map(|d| match &d.values[0] {
                Some(Value::Text(t)) => tokenize(t),
                _ => vec![],
            })
            .collect();
        let n = docs.len() as f32;
        let non_empty = toks.iter().filter(|t| !t.is_empty()).count().max(1) as f32;
        let avg = toks.iter().map(|t| t.len() as f32).sum::<f32>() / non_empty;
        let mut q = tokenize(query);
        q.sort();
        q.dedup();
        let mut out = Vec::new();
        for (row, t) in toks.iter().enumerate() {
            let mut s = 0.0;
            let mut m = 0;
            for term in &q {
                let df = toks.iter().filter(|d| d.contains(term)).count() as f32;
                let tf = t.iter().filter(|x| *x == term).count() as f32;
                if tf > 0.0 {
                    m += 1;
                    let idf = ((n - df + 0.5) / (df + 0.5) + 1.0).ln();
                    s += idf * tf * (p.k1 + 1.0)
                        / (tf + p.k1 * (1.0 - p.b + p.b * t.len() as f32 / avg));
                }
            }
            if s > 0.0 && (!all || m == q.len()) {
                out.push((s, row as u32));
            }
        }
        out.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
        out
    }

    #[test]
    fn tokenizer_lowercases_and_splits_on_non_alphanumerics() {
        assert_eq!(
            tokenize("Hello, World! Énergie nucléaire-2024 ok"),
            vec!["hello", "world", "énergie", "nucléaire", "2024", "ok"]
        );
        assert!(tokenize("  ...  ").is_empty());
    }

    #[test]
    fn bm25_matches_naive_reference() {
        let docs = vec![
            doc(1, "the minister talks about nuclear energy"),
            doc(2, "nuclear power plant opens"),
            doc(3, "energy prices rise as minister speaks about energy"),
            doc(4, "weather report"),
            Document::new(DocId(5), 1),
            doc(6, "nuclear nuclear nuclear"),
        ];
        let refs: Vec<&Document> = docs.iter().collect();
        let idx = TextIndex::build(0, &refs);
        assert_eq!(idx.doc_freq("nuclear"), 3);
        assert_eq!(idx.doc_freq("missing"), 0);
        let p = Bm25Params::default();
        for (q, all) in [
            ("nuclear energy", false),
            ("nuclear energy", true),
            ("Minister", false),
            ("nothing here", false),
            ("energy", true),
        ] {
            let got = idx.search(
                &TextQuery {
                    field: 0,
                    text: q.into(),
                    all_terms: all,
                },
                10,
                None,
                p,
            );
            let want = naive_bm25(&docs, q, all, p);
            assert_eq!(got.len(), want.len(), "{q} all={all}");
            for (g, w) in got.iter().zip(&want) {
                assert_eq!(g.1, w.1, "{q} all={all}");
                assert!((g.0 - w.0).abs() < 1e-4, "{q}: {} vs {}", g.0, w.0);
            }
        }
        let mut f = Bitmap::full(6);
        f.clear(5);
        let got = idx.search(
            &TextQuery {
                field: 0,
                text: "nuclear".into(),
                all_terms: false,
            },
            10,
            Some(&f),
            p,
        );
        assert_eq!(got.len(), 2);
        assert!(got.iter().all(|(_, r)| *r != 5));
    }

    #[test]
    fn section_roundtrip() {
        use cairn_core::NodeId;
        use cairn_sim::{SimConfig, Simulation};
        use cairn_storage::columns::write_columns;
        use cairn_storage::{SegmentReader, SegmentWriter};
        let schema = Schema::new(vec![cairn_core::FieldDef {
            name: "t".into(),
            kind: FieldKind::Text,
        }])
        .unwrap();
        let docs: Vec<Document> = (0..200)
            .map(|i| {
                doc(
                    i,
                    &format!(
                        "doc {} word{} common {}",
                        i,
                        i % 7,
                        if i % 3 == 0 { "rare" } else { "" }
                    ),
                )
            })
            .collect();
        let idx = TextIndex::build(0, &docs.iter().collect::<Vec<_>>());
        let (name, bytes) = idx.section();
        let (sim, mut ex) = Simulation::new(1, SimConfig::default());
        let rt = sim.runtime(NodeId(1), &ex.handle());
        ex.block_on(async move {
            let refs: Vec<&Document> = docs.iter().collect();
            let mut w = SegmentWriter::create(rt.clone(), "s.seg").await.unwrap();
            write_columns(&mut w, &schema, &refs).await.unwrap();
            w.add_section(&name, &bytes).await.unwrap();
            w.finish().await.unwrap();
            let reader = SegmentReader::open(rt.clone(), "s.seg").await.unwrap();
            let loaded = TextIndex::load(&reader, 0).await.unwrap();
            assert_eq!(loaded, idx);
        });
    }
}
