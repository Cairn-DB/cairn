//! Structured indexes of one segment: evaluate a [`Predicate`] to a row [`Bitmap`].
//!
//! Per filterable field: `Enum`, `Set` and `Bool` get a sorted dictionary with a sorted row list
//! per value; `I64`, `Date` and `F64` get sorted `(key, row)` pairs for range scans. Everything
//! is one section `sidx.<field>` per field. Null rows appear in no list; `IsNull` is answered
//! from the `nulls.<field>` column.

use crate::bitmap::Bitmap;
use cairn_core::codec::{Reader, Writer};
use cairn_core::filter::order_key;
use cairn_core::{Document, Error, FieldKind, Predicate, Result, Runtime, Schema, Value};
use cairn_storage::SegmentReader;

#[derive(Debug, Clone, PartialEq)]
enum FieldIndex {
    /// Sorted distinct values and, per value, sorted rows.
    Terms {
        values: Vec<String>,
        rows: Vec<Vec<u32>>,
    },
    /// Sorted `(key, row)`.
    Ordered { keys: Vec<i64>, rows: Vec<u32> },
}

fn term_of(v: &Value) -> Option<String> {
    match v {
        Value::Enum(s) => Some(s.clone()),
        Value::Bool(b) => Some(if *b { "1".into() } else { "0".into() }),
        _ => None,
    }
}

impl FieldIndex {
    fn build(kind: &FieldKind, docs: &[&Document], field: usize) -> Option<FieldIndex> {
        match kind {
            FieldKind::Enum | FieldKind::Bool | FieldKind::Set => {
                let mut pairs: Vec<(String, u32)> = Vec::new();
                for (row, d) in docs.iter().enumerate() {
                    match &d.values[field] {
                        Some(Value::Set(items)) => {
                            pairs.extend(items.iter().map(|s| (s.clone(), row as u32)))
                        }
                        Some(v) => {
                            if let Some(t) = term_of(v) {
                                pairs.push((t, row as u32));
                            }
                        }
                        None => {}
                    }
                }
                pairs.sort();
                let mut values = Vec::new();
                let mut rows: Vec<Vec<u32>> = Vec::new();
                for (t, row) in pairs {
                    if values.last() != Some(&t) {
                        values.push(t);
                        rows.push(Vec::new());
                    }
                    rows.last_mut().expect("pushed").push(row);
                }
                Some(FieldIndex::Terms { values, rows })
            }
            FieldKind::I64 | FieldKind::Date | FieldKind::F64 => {
                let mut pairs: Vec<(i64, u32)> = docs
                    .iter()
                    .enumerate()
                    .filter_map(|(row, d)| {
                        d.values[field]
                            .as_ref()
                            .and_then(order_key)
                            .map(|k| (k, row as u32))
                    })
                    .collect();
                pairs.sort_unstable();
                Some(FieldIndex::Ordered {
                    keys: pairs.iter().map(|p| p.0).collect(),
                    rows: pairs.iter().map(|p| p.1).collect(),
                })
            }
            _ => None,
        }
    }

    fn encode(&self, w: &mut Writer) {
        match self {
            FieldIndex::Terms { values, rows } => {
                w.u8(1).u32(values.len() as u32);
                for (v, r) in values.iter().zip(rows) {
                    w.str(v).u32(r.len() as u32);
                    for x in r {
                        w.u32(*x);
                    }
                }
            }
            FieldIndex::Ordered { keys, rows } => {
                w.u8(2).u32(keys.len() as u32);
                for (k, r) in keys.iter().zip(rows) {
                    w.u64(*k as u64).u32(*r);
                }
            }
        }
    }

    fn decode(r: &mut Reader<'_>, n_rows: u32) -> Result<FieldIndex> {
        match r.u8()? {
            1 => {
                let n = r.u32()? as usize;
                let mut values = Vec::with_capacity(n.min(1 << 20));
                let mut rows = Vec::with_capacity(n.min(1 << 20));
                for _ in 0..n {
                    let v = r.str()?.to_owned();
                    if values.last().is_some_and(|last: &String| last >= &v) {
                        return Err(Error::corruption("term dictionary not sorted"));
                    }
                    values.push(v);
                    let m = r.u32()? as usize;
                    let mut list = Vec::with_capacity(m.min(1 << 20));
                    for _ in 0..m {
                        let row = r.u32()?;
                        if row >= n_rows || list.last().is_some_and(|l| *l >= row) {
                            return Err(Error::corruption("term row list invalid"));
                        }
                        list.push(row);
                    }
                    rows.push(list);
                }
                Ok(FieldIndex::Terms { values, rows })
            }
            2 => {
                let n = r.u32()? as usize;
                let mut keys = Vec::with_capacity(n.min(1 << 20));
                let mut rows = Vec::with_capacity(n.min(1 << 20));
                for _ in 0..n {
                    let k = r.u64()? as i64;
                    let row = r.u32()?;
                    if row >= n_rows || keys.last().is_some_and(|l| *l > k) {
                        return Err(Error::corruption("ordered index invalid"));
                    }
                    keys.push(k);
                    rows.push(row);
                }
                Ok(FieldIndex::Ordered { keys, rows })
            }
            t => Err(Error::corruption(format!("unknown field index tag {t}"))),
        }
    }

    fn eq(&self, term: &str, out: &mut Bitmap) {
        if let FieldIndex::Terms { values, rows } = self
            && let Ok(i) = values.binary_search_by(|v| v.as_str().cmp(term))
        {
            for &r in &rows[i] {
                out.set(r);
            }
        }
    }

    fn range(
        &self,
        lo: Option<i64>,
        hi: Option<i64>,
        lo_inc: bool,
        hi_inc: bool,
        out: &mut Bitmap,
    ) {
        let FieldIndex::Ordered { keys, rows } = self else {
            return;
        };
        let start = match lo {
            None => 0,
            Some(l) => {
                if lo_inc {
                    keys.partition_point(|k| *k < l)
                } else {
                    keys.partition_point(|k| *k <= l)
                }
            }
        };
        let end = match hi {
            None => keys.len(),
            Some(h) => {
                if hi_inc {
                    keys.partition_point(|k| *k <= h)
                } else {
                    keys.partition_point(|k| *k < h)
                }
            }
        };
        for &r in &rows[start..end.max(start)] {
            out.set(r);
        }
    }
}

/// All structured indexes of one segment.
#[derive(Debug, Clone, PartialEq)]
pub struct StructuredIndex {
    rows: u32,
    fields: Vec<Option<FieldIndex>>,
    /// Rows where the field is present (from the `nulls` columns).
    present: Vec<Bitmap>,
}

impl StructuredIndex {
    /// Builds indexes for every filterable field of `schema` over `docs` (row = position).
    pub fn build(schema: &Schema, docs: &[&Document]) -> Self {
        let rows = docs.len() as u32;
        let mut fields = Vec::with_capacity(schema.fields.len());
        let mut present = Vec::with_capacity(schema.fields.len());
        for (i, f) in schema.fields.iter().enumerate() {
            fields.push(FieldIndex::build(&f.kind, docs, i));
            let mut b = Bitmap::empty(rows);
            for (row, d) in docs.iter().enumerate() {
                if d.values[i].is_some() {
                    b.set(row as u32);
                }
            }
            present.push(b);
        }
        StructuredIndex {
            rows,
            fields,
            present,
        }
    }

    /// Sections `sidx.<field>` for indexed fields.
    pub fn sections(&self) -> Vec<(String, Vec<u8>)> {
        self.fields
            .iter()
            .enumerate()
            .filter_map(|(i, f)| {
                f.as_ref().map(|f| {
                    let mut w = Writer::new();
                    f.encode(&mut w);
                    (format!("sidx.{i}"), w.into_vec())
                })
            })
            .collect()
    }

    /// Loads from a segment.
    pub async fn load<R: Runtime>(reader: &SegmentReader<R>, schema: &Schema) -> Result<Self> {
        let mut fields = Vec::with_capacity(schema.fields.len());
        let mut present = Vec::with_capacity(schema.fields.len());
        let mut rows = None;
        for (i, f) in schema.fields.iter().enumerate() {
            let nulls = reader.read_section(&format!("nulls.{i}")).await?;
            let n = nulls.len() as u32;
            if *rows.get_or_insert(n) != n {
                return Err(Error::corruption("nulls columns disagree on row count"));
            }
            let mut b = Bitmap::empty(n);
            for (row, &p) in nulls.iter().enumerate() {
                if p != 0 {
                    b.set(row as u32);
                }
            }
            present.push(b);
            let name = format!("sidx.{i}");
            if f.kind.is_filterable() && reader.has_section(&name) {
                let bytes = reader.read_section(&name).await?;
                let mut r = Reader::new(&bytes);
                let idx = FieldIndex::decode(&mut r, n)?;
                r.finish()?;
                fields.push(Some(idx));
            } else {
                fields.push(None);
            }
        }
        Ok(StructuredIndex {
            rows: rows.unwrap_or(0),
            fields,
            present,
        })
    }

    /// Number of rows.
    pub fn rows(&self) -> u32 {
        self.rows
    }

    /// Evaluates `p` to the set of matching rows. Unindexed fields match nothing (the caller
    /// validates predicates against the schema first).
    pub fn evaluate(&self, p: &Predicate) -> Bitmap {
        match p {
            Predicate::True => Bitmap::full(self.rows),
            Predicate::And(ps) => {
                let mut acc = Bitmap::full(self.rows);
                for q in ps {
                    acc.and_with(&self.evaluate(q));
                }
                acc
            }
            Predicate::Or(ps) => {
                let mut acc = Bitmap::empty(self.rows);
                for q in ps {
                    acc.or_with(&self.evaluate(q));
                }
                acc
            }
            Predicate::Not(q) => {
                let mut b = self.evaluate(q);
                b.negate();
                b
            }
            Predicate::Eq { field, value } => {
                let mut b = Bitmap::empty(self.rows);
                if let Some(Some(idx)) = self.fields.get(*field) {
                    match idx {
                        FieldIndex::Terms { .. } => {
                            if let Some(t) = term_of(value) {
                                idx.eq(&t, &mut b);
                            }
                        }
                        FieldIndex::Ordered { .. } => {
                            if let Some(k) = order_key(value) {
                                idx.range(Some(k), Some(k), true, true, &mut b);
                            }
                        }
                    }
                }
                b
            }
            Predicate::In { field, values } => {
                let mut b = Bitmap::empty(self.rows);
                for v in values {
                    b.or_with(&self.evaluate(&Predicate::Eq {
                        field: *field,
                        value: v.clone(),
                    }));
                }
                b
            }
            Predicate::Range {
                field,
                lo,
                hi,
                lo_inclusive,
                hi_inclusive,
            } => {
                let mut b = Bitmap::empty(self.rows);
                if let Some(Some(idx)) = self.fields.get(*field) {
                    idx.range(
                        lo.as_ref().and_then(order_key),
                        hi.as_ref().and_then(order_key),
                        *lo_inclusive,
                        *hi_inclusive,
                        &mut b,
                    );
                }
                b
            }
            Predicate::IsNull { field } => match self.present.get(*field) {
                Some(p) => {
                    let mut b = p.clone();
                    b.negate();
                    b
                }
                None => Bitmap::full(self.rows),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_core::{DocId, FieldDef, SeededRng};
    use proptest::prelude::*;

    fn schema() -> Schema {
        Schema::new(vec![
            FieldDef {
                name: "year".into(),
                kind: FieldKind::I64,
            },
            FieldDef {
                name: "channel".into(),
                kind: FieldKind::Enum,
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
                name: "score".into(),
                kind: FieldKind::F64,
            },
            FieldDef {
                name: "title".into(),
                kind: FieldKind::Text,
            },
        ])
        .unwrap()
    }

    fn docs(seed: u64, n: usize) -> Vec<Document> {
        let mut r = SeededRng::from_seed(seed);
        (0..n)
            .map(|i| {
                let mut d = Document::new(DocId(i as u64), 6);
                if !r.chance(0.1) {
                    d = d.set(0, Value::I64(1990 + r.below(30) as i64));
                }
                if !r.chance(0.1) {
                    d = d.set(1, Value::Enum(format!("c{}", r.below(5))));
                }
                if !r.chance(0.2) {
                    let k = r.below(4) as usize;
                    d = d.set(
                        2,
                        Value::Set((0..k).map(|_| format!("t{}", r.below(6))).collect()),
                    );
                }
                if !r.chance(0.1) {
                    d = d.set(3, Value::Bool(r.chance(0.5)));
                }
                if !r.chance(0.1) {
                    d = d.set(4, Value::F64(r.unit_f64() * 20.0 - 10.0));
                }
                d.validate(&schema()).unwrap();
                d
            })
            .collect()
    }

    fn predicate(r: &mut SeededRng, depth: u32) -> Predicate {
        let leaf = depth >= 3 || r.chance(0.6);
        if leaf {
            match r.below(6) {
                0 => Predicate::Eq {
                    field: 0,
                    value: Value::I64(1990 + r.below(30) as i64),
                },
                1 => Predicate::Eq {
                    field: 1,
                    value: Value::Enum(format!("c{}", r.below(6))),
                },
                2 => Predicate::Eq {
                    field: 2,
                    value: Value::Enum(format!("t{}", r.below(7))),
                },
                3 => Predicate::Eq {
                    field: 3,
                    value: Value::Bool(r.chance(0.5)),
                },
                4 => {
                    let a = r.unit_f64() * 20.0 - 10.0;
                    let b = a + r.unit_f64() * 5.0;
                    Predicate::Range {
                        field: 4,
                        lo: Some(Value::F64(a)),
                        hi: r.chance(0.3).then_some(Value::F64(b)),
                        lo_inclusive: r.chance(0.5),
                        hi_inclusive: r.chance(0.5),
                    }
                }
                _ => {
                    if r.chance(0.3) {
                        Predicate::IsNull {
                            field: r.below(5) as usize,
                        }
                    } else {
                        let a = 1985 + r.below(40) as i64;
                        Predicate::Range {
                            field: 0,
                            lo: r.chance(0.7).then_some(Value::I64(a)),
                            hi: Some(Value::I64(a + r.below(10) as i64)),
                            lo_inclusive: r.chance(0.5),
                            hi_inclusive: r.chance(0.5),
                        }
                    }
                }
            }
        } else {
            match r.below(4) {
                0 => Predicate::And(
                    (0..1 + r.below(3))
                        .map(|_| predicate(r, depth + 1))
                        .collect(),
                ),
                1 => Predicate::Or(
                    (0..1 + r.below(3))
                        .map(|_| predicate(r, depth + 1))
                        .collect(),
                ),
                2 => Predicate::Not(Box::new(predicate(r, depth + 1))),
                _ => Predicate::In {
                    field: 1,
                    values: (0..1 + r.below(3))
                        .map(|_| Value::Enum(format!("c{}", r.below(6))))
                        .collect(),
                },
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(200))]
        #[test]
        fn index_evaluation_matches_document_semantics(seed in any::<u64>()) {
            let s = schema();
            let ds = docs(seed, 300);
            let refs: Vec<&Document> = ds.iter().collect();
            let idx = StructuredIndex::build(&s, &refs);
            let mut r = SeededRng::from_seed(seed ^ 0xABCD);
            for _ in 0..20 {
                let p = predicate(&mut r, 0);
                p.validate(&s).unwrap();
                let got = idx.evaluate(&p);
                for (row, d) in ds.iter().enumerate() {
                    prop_assert_eq!(got.contains(row as u32), p.matches(d), "predicate {:?} row {}", p, row);
                }
                let mut w = Writer::new();
                p.encode(&mut w);
                let mut rd = Reader::new(w.as_slice());
                prop_assert_eq!(Predicate::decode(&mut rd).unwrap(), p);
            }
        }
    }

    #[test]
    fn sections_roundtrip_through_a_segment() {
        use cairn_core::NodeId;
        use cairn_sim::{SimConfig, Simulation};
        use cairn_storage::columns::write_columns;
        use cairn_storage::{SegmentReader, SegmentWriter};
        let s = schema();
        let ds = docs(3, 500);
        let idx = StructuredIndex::build(&s, &ds.iter().collect::<Vec<_>>());
        let sections = idx.sections();
        assert_eq!(sections.len(), 5);
        let (sim, mut ex) = Simulation::new(1, SimConfig::default());
        let rt = sim.runtime(NodeId(1), &ex.handle());
        ex.block_on(async move {
            let refs: Vec<&Document> = ds.iter().collect();
            let mut w = SegmentWriter::create(rt.clone(), "s.seg").await.unwrap();
            write_columns(&mut w, &s, &refs).await.unwrap();
            for (name, bytes) in &sections {
                w.add_section(name, bytes).await.unwrap();
            }
            w.finish().await.unwrap();
            let reader = SegmentReader::open(rt.clone(), "s.seg").await.unwrap();
            let loaded = StructuredIndex::load(&reader, &s).await.unwrap();
            assert_eq!(loaded, idx);
        });
    }
}
