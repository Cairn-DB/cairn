//! Structured predicates over document fields.

use crate::codec::{Reader, Writer};
use crate::{Document, Error, FieldKind, Result, Schema, Value};

/// A structured filter. Fields are referenced by schema position.
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    /// Matches everything.
    True,
    /// All must hold.
    And(Vec<Predicate>),
    /// Any must hold.
    Or(Vec<Predicate>),
    /// Must not hold.
    Not(Box<Predicate>),
    /// Field equals value (`Set` fields: contains the value).
    Eq {
        /// Field position.
        field: usize,
        /// Value.
        value: Value,
    },
    /// Field equals any of the values (`Set` fields: contains any).
    In {
        /// Field position.
        field: usize,
        /// Values.
        values: Vec<Value>,
    },
    /// Numeric or date range, either bound optional.
    Range {
        /// Field position.
        field: usize,
        /// Lower bound.
        lo: Option<Value>,
        /// Upper bound.
        hi: Option<Value>,
        /// Whether `lo` is included.
        lo_inclusive: bool,
        /// Whether `hi` is included.
        hi_inclusive: bool,
    },
    /// Field is null.
    IsNull {
        /// Field position.
        field: usize,
    },
}

/// Orderable key of a scalar value, for ranges (`i64`, dates, and `f64` via a total order).
pub fn order_key(v: &Value) -> Option<i64> {
    match v {
        Value::I64(x) | Value::Date(x) => Some(*x),
        Value::F64(f) => {
            // Monotonic mapping of f64 to i64 (total order, NaN excluded by validation).
            let bits = f.to_bits() as i64;
            Some(if bits < 0 { bits ^ i64::MAX } else { bits })
        }
        _ => None,
    }
}

impl Predicate {
    /// Validates field positions and value kinds against `schema`.
    pub fn validate(&self, schema: &Schema) -> Result<()> {
        let field_of = |i: usize| {
            schema
                .fields
                .get(i)
                .ok_or_else(|| Error::InvalidRequest(format!("unknown field position {i}")))
        };
        match self {
            Predicate::True => Ok(()),
            Predicate::And(ps) | Predicate::Or(ps) => {
                ps.iter().try_for_each(|p| p.validate(schema))
            }
            Predicate::Not(p) => p.validate(schema),
            Predicate::Eq { field, value } => {
                let f = field_of(*field)?;
                if !f.kind.is_filterable() {
                    return Err(Error::InvalidRequest(format!(
                        "field {:?} is not filterable",
                        f.name
                    )));
                }
                let ok = match (&f.kind, value) {
                    (FieldKind::Set, Value::Enum(_)) => true,
                    _ => value.matches(&f.kind),
                };
                if ok {
                    Ok(())
                } else {
                    Err(Error::InvalidRequest(format!(
                        "value kind mismatch for {:?}",
                        f.name
                    )))
                }
            }
            Predicate::In { field, values } => values.iter().try_for_each(|v| {
                Predicate::Eq {
                    field: *field,
                    value: v.clone(),
                }
                .validate(schema)
            }),
            Predicate::Range { field, lo, hi, .. } => {
                let f = field_of(*field)?;
                if !matches!(f.kind, FieldKind::I64 | FieldKind::F64 | FieldKind::Date) {
                    return Err(Error::InvalidRequest(format!(
                        "range on non-numeric field {:?}",
                        f.name
                    )));
                }
                for b in [lo, hi].into_iter().flatten() {
                    if !b.matches(&f.kind) {
                        return Err(Error::InvalidRequest(format!(
                            "range bound kind mismatch for {:?}",
                            f.name
                        )));
                    }
                }
                Ok(())
            }
            Predicate::IsNull { field } => field_of(*field).map(|_| ()),
        }
    }

    /// Fields referenced by this predicate.
    pub fn fields(&self, out: &mut Vec<usize>) {
        match self {
            Predicate::True => {}
            Predicate::And(ps) | Predicate::Or(ps) => ps.iter().for_each(|p| p.fields(out)),
            Predicate::Not(p) => p.fields(out),
            Predicate::Eq { field, .. }
            | Predicate::In { field, .. }
            | Predicate::Range { field, .. }
            | Predicate::IsNull { field } => {
                if !out.contains(field) {
                    out.push(*field);
                }
            }
        }
    }

    /// Evaluates the predicate directly on a document (the reference semantics).
    pub fn matches(&self, doc: &Document) -> bool {
        match self {
            Predicate::True => true,
            Predicate::And(ps) => ps.iter().all(|p| p.matches(doc)),
            Predicate::Or(ps) => ps.iter().any(|p| p.matches(doc)),
            Predicate::Not(p) => !p.matches(doc),
            Predicate::Eq { field, value } => match doc.values.get(*field).and_then(Option::as_ref)
            {
                Some(Value::Set(items)) => matches!(value, Value::Enum(e) if items.contains(e)),
                Some(v) => v == value,
                None => false,
            },
            Predicate::In { field, values } => values.iter().any(|v| {
                Predicate::Eq {
                    field: *field,
                    value: v.clone(),
                }
                .matches(doc)
            }),
            Predicate::Range {
                field,
                lo,
                hi,
                lo_inclusive,
                hi_inclusive,
            } => {
                let Some(k) = doc
                    .values
                    .get(*field)
                    .and_then(Option::as_ref)
                    .and_then(order_key)
                else {
                    return false;
                };
                let lo_ok = match lo.as_ref().and_then(order_key) {
                    None => true,
                    Some(l) => {
                        if *lo_inclusive {
                            k >= l
                        } else {
                            k > l
                        }
                    }
                };
                let hi_ok = match hi.as_ref().and_then(order_key) {
                    None => true,
                    Some(h) => {
                        if *hi_inclusive {
                            k <= h
                        } else {
                            k < h
                        }
                    }
                };
                lo_ok && hi_ok
            }
            Predicate::IsNull { field } => doc.values.get(*field).is_none_or(Option::is_none),
        }
    }

    /// Encodes the predicate.
    pub fn encode(&self, w: &mut Writer) {
        match self {
            Predicate::True => {
                w.u8(0);
            }
            Predicate::And(ps) | Predicate::Or(ps) => {
                w.u8(if matches!(self, Predicate::And(_)) {
                    1
                } else {
                    2
                })
                .u32(ps.len() as u32);
                for p in ps {
                    p.encode(w);
                }
            }
            Predicate::Not(p) => {
                w.u8(3);
                p.encode(w);
            }
            Predicate::Eq { field, value } => {
                w.u8(4).u32(*field as u32);
                value.encode(w);
            }
            Predicate::In { field, values } => {
                w.u8(5).u32(*field as u32).u32(values.len() as u32);
                for v in values {
                    v.encode(w);
                }
            }
            Predicate::Range {
                field,
                lo,
                hi,
                lo_inclusive,
                hi_inclusive,
            } => {
                w.u8(6)
                    .u32(*field as u32)
                    .u8(u8::from(*lo_inclusive))
                    .u8(u8::from(*hi_inclusive));
                for b in [lo, hi] {
                    match b {
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
            Predicate::IsNull { field } => {
                w.u8(7).u32(*field as u32);
            }
        }
    }

    /// Decodes a predicate (depth-limited).
    pub fn decode(r: &mut Reader<'_>) -> Result<Predicate> {
        Self::decode_depth(r, 0)
    }

    fn decode_depth(r: &mut Reader<'_>, depth: u32) -> Result<Predicate> {
        if depth > 64 {
            return Err(Error::corruption("predicate nesting too deep"));
        }
        Ok(match r.u8()? {
            0 => Predicate::True,
            t @ (1 | 2) => {
                let n = r.u32()? as usize;
                if n > 4096 {
                    return Err(Error::corruption("predicate too wide"));
                }
                let mut ps = Vec::with_capacity(n);
                for _ in 0..n {
                    ps.push(Self::decode_depth(r, depth + 1)?);
                }
                if t == 1 {
                    Predicate::And(ps)
                } else {
                    Predicate::Or(ps)
                }
            }
            3 => Predicate::Not(Box::new(Self::decode_depth(r, depth + 1)?)),
            4 => Predicate::Eq {
                field: r.u32()? as usize,
                value: Value::decode(r)?,
            },
            5 => {
                let field = r.u32()? as usize;
                let n = r.u32()? as usize;
                if n > 1 << 16 {
                    return Err(Error::corruption("IN list too long"));
                }
                let mut values = Vec::with_capacity(n);
                for _ in 0..n {
                    values.push(Value::decode(r)?);
                }
                Predicate::In { field, values }
            }
            6 => {
                let field = r.u32()? as usize;
                let lo_inclusive = r.u8()? != 0;
                let hi_inclusive = r.u8()? != 0;
                let mut bounds = [None, None];
                for b in &mut bounds {
                    if r.u8()? != 0 {
                        *b = Some(Value::decode(r)?);
                    }
                }
                let [lo, hi] = bounds;
                Predicate::Range {
                    field,
                    lo,
                    hi,
                    lo_inclusive,
                    hi_inclusive,
                }
            }
            7 => Predicate::IsNull {
                field: r.u32()? as usize,
            },
            t => return Err(Error::corruption(format!("unknown predicate tag {t}"))),
        })
    }
}
