//! Documents and values.

use crate::codec::{Reader, Writer};
use crate::{DocId, Error, FieldKind, Result, Schema};
use bytes::Bytes;

/// A field value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// Dense vector.
    Vector(Vec<f32>),
    /// Text.
    Text(String),
    /// Integer.
    I64(i64),
    /// Float.
    F64(f64),
    /// Boolean.
    Bool(bool),
    /// Date (ordered like an integer).
    Date(i64),
    /// Enum value.
    Enum(String),
    /// Set of enum values (deduplicated and sorted on validation).
    Set(Vec<String>),
    /// Opaque bytes.
    Blob(Bytes),
}

impl Value {
    fn tag(&self) -> u8 {
        match self {
            Value::Vector(_) => 0,
            Value::Text(_) => 1,
            Value::I64(_) => 2,
            Value::F64(_) => 3,
            Value::Bool(_) => 4,
            Value::Date(_) => 5,
            Value::Enum(_) => 6,
            Value::Set(_) => 7,
            Value::Blob(_) => 8,
        }
    }

    /// Whether this value has the given kind.
    pub fn matches(&self, kind: &FieldKind) -> bool {
        match (self, kind) {
            (Value::Vector(v), FieldKind::Vector { dims, .. }) => v.len() == *dims as usize,
            (Value::Text(_), FieldKind::Text)
            | (Value::I64(_), FieldKind::I64)
            | (Value::F64(_), FieldKind::F64)
            | (Value::Bool(_), FieldKind::Bool)
            | (Value::Date(_), FieldKind::Date)
            | (Value::Enum(_), FieldKind::Enum)
            | (Value::Set(_), FieldKind::Set)
            | (Value::Blob(_), FieldKind::Blob) => true,
            _ => false,
        }
    }

    /// Encodes the value with a kind tag.
    pub fn encode(&self, w: &mut Writer) {
        w.u8(self.tag());
        match self {
            Value::Vector(v) => {
                w.u32(v.len() as u32);
                for x in v {
                    w.f32(*x);
                }
            }
            Value::Text(s) | Value::Enum(s) => {
                w.str(s);
            }
            Value::I64(x) | Value::Date(x) => {
                w.u64(*x as u64);
            }
            Value::F64(x) => {
                w.u64(x.to_bits());
            }
            Value::Bool(b) => {
                w.u8(u8::from(*b));
            }
            Value::Set(items) => {
                w.u32(items.len() as u32);
                for s in items {
                    w.str(s);
                }
            }
            Value::Blob(b) => {
                w.bytes(b);
            }
        }
    }

    /// Decodes a value.
    pub fn decode(r: &mut Reader<'_>) -> Result<Value> {
        Ok(match r.u8()? {
            0 => {
                let n = r.u32()? as usize;
                if n > 1 << 20 {
                    return Err(Error::corruption("vector too long"));
                }
                let mut v = Vec::with_capacity(n);
                for _ in 0..n {
                    v.push(r.f32()?);
                }
                Value::Vector(v)
            }
            1 => Value::Text(r.str()?.to_owned()),
            2 => Value::I64(r.u64()? as i64),
            3 => Value::F64(f64::from_bits(r.u64()?)),
            4 => Value::Bool(r.u8()? != 0),
            5 => Value::Date(r.u64()? as i64),
            6 => Value::Enum(r.str()?.to_owned()),
            7 => {
                let n = r.u32()? as usize;
                if n > 1 << 16 {
                    return Err(Error::corruption("set too large"));
                }
                let mut items = Vec::with_capacity(n);
                for _ in 0..n {
                    items.push(r.str()?.to_owned());
                }
                Value::Set(items)
            }
            8 => Value::Blob(Bytes::copy_from_slice(r.bytes()?)),
            t => return Err(Error::corruption(format!("unknown value tag {t}"))),
        })
    }
}

/// A document: an id and one optional value per schema field, by position.
#[derive(Debug, Clone, PartialEq)]
pub struct Document {
    /// Client-chosen id.
    pub id: DocId,
    /// Values by field position; `None` is null.
    pub values: Vec<Option<Value>>,
}

impl Document {
    /// Creates a document with all fields null.
    pub fn new(id: DocId, field_count: usize) -> Self {
        Document {
            id,
            values: vec![None; field_count],
        }
    }

    /// Sets the value of the field at `pos`.
    pub fn set(mut self, pos: usize, v: Value) -> Self {
        self.values[pos] = Some(v);
        self
    }

    /// The value of the field named `name`.
    pub fn get<'a>(&'a self, schema: &Schema, name: &str) -> Option<&'a Value> {
        schema
            .index_of(name)
            .and_then(|i| self.values.get(i))
            .and_then(Option::as_ref)
    }

    /// Checks the document against `schema`; normalizes sets (sorted, deduplicated).
    pub fn validate(&mut self, schema: &Schema) -> Result<()> {
        if self.values.len() != schema.fields.len() {
            return Err(Error::Schema(format!(
                "document {} has {} values, schema has {} fields",
                self.id,
                self.values.len(),
                schema.fields.len()
            )));
        }
        for (v, f) in self.values.iter_mut().zip(&schema.fields) {
            if let Some(v) = v {
                if !v.matches(&f.kind) {
                    return Err(Error::Schema(format!(
                        "document {}: field {:?} has the wrong kind",
                        self.id, f.name
                    )));
                }
                if let Value::Set(items) = v {
                    items.sort();
                    items.dedup();
                }
                if let Value::Vector(x) = v
                    && x.iter().any(|f| !f.is_finite())
                {
                    return Err(Error::Schema(format!(
                        "document {}: field {:?} has a non-finite component",
                        self.id, f.name
                    )));
                }
            }
        }
        Ok(())
    }

    /// Encodes the document.
    pub fn encode(&self, w: &mut Writer) {
        w.u64(self.id.get()).u32(self.values.len() as u32);
        for v in &self.values {
            match v {
                None => {
                    w.u8(0);
                }
                Some(v) => {
                    w.u8(1);
                    v.encode(w);
                }
            }
        }
    }

    /// Decodes a document.
    pub fn decode(r: &mut Reader<'_>) -> Result<Document> {
        let id = DocId(r.u64()?);
        let n = r.u32()? as usize;
        if n > 4096 {
            return Err(Error::corruption("too many fields"));
        }
        let mut values = Vec::with_capacity(n);
        for _ in 0..n {
            values.push(match r.u8()? {
                0 => None,
                1 => Some(Value::decode(r)?),
                t => return Err(Error::corruption(format!("bad null marker {t}"))),
            });
        }
        Ok(Document { id, values })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FieldDef, Metric};

    fn schema() -> Schema {
        Schema::new(vec![
            FieldDef {
                name: "img".into(),
                kind: FieldKind::Vector {
                    dims: 3,
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
                name: "raw".into(),
                kind: FieldKind::Blob,
            },
        ])
        .unwrap()
    }

    #[test]
    fn document_roundtrip_and_validation() {
        let s = schema();
        let mut d = Document::new(DocId(9), 5)
            .set(0, Value::Vector(vec![1.0, 2.0, 3.0]))
            .set(1, Value::Text("hello".into()))
            .set(3, Value::Set(vec!["b".into(), "a".into(), "b".into()]))
            .set(4, Value::Blob(Bytes::from_static(b"\x00\xff")));
        d.validate(&s).unwrap();
        assert_eq!(d.values[3], Some(Value::Set(vec!["a".into(), "b".into()])));
        let mut w = Writer::new();
        d.encode(&mut w);
        let mut r = Reader::new(w.as_slice());
        assert_eq!(Document::decode(&mut r).unwrap(), d);
        r.finish().unwrap();
        let mut bad = d.clone().set(0, Value::Vector(vec![1.0]));
        assert!(bad.validate(&s).is_err());
        let mut bad = d.clone().set(2, Value::Text("x".into()));
        assert!(bad.validate(&s).is_err());
        let mut w = Writer::new();
        s.encode(&mut w);
        assert_eq!(Schema::decode(&mut Reader::new(w.as_slice())).unwrap(), s);
        assert!(
            Schema::new(vec![
                FieldDef {
                    name: "a".into(),
                    kind: FieldKind::Text
                },
                FieldDef {
                    name: "a".into(),
                    kind: FieldKind::I64
                }
            ])
            .is_err()
        );
    }
}
