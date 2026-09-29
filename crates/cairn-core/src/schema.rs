//! Collection schema: field names and kinds.

use crate::codec::{Reader, Writer};
use crate::{Error, Result};
use serde::{Deserialize, Serialize};

/// Distance metric of a vector field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Metric {
    /// Squared Euclidean distance (lower is better).
    L2,
    /// Inner product (higher is better).
    Dot,
    /// Cosine similarity; vectors are normalized at insert (higher is better).
    Cosine,
}

/// Kind of a field.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FieldKind {
    /// Dense embedding.
    Vector {
        /// Dimensionality.
        dims: u32,
        /// Metric.
        metric: Metric,
    },
    /// Full-text (BM25).
    Text,
    /// 64-bit signed integer.
    I64,
    /// 64-bit float.
    F64,
    /// Boolean.
    Bool,
    /// Date as days or milliseconds since an epoch chosen by the client; ordered like `I64`.
    Date,
    /// One value from a small vocabulary (dictionary-coded).
    Enum,
    /// A set of vocabulary values (tags).
    Set,
    /// Opaque payload; never indexed.
    Blob,
}

impl FieldKind {
    fn tag(&self) -> u8 {
        match self {
            FieldKind::Vector { .. } => 0,
            FieldKind::Text => 1,
            FieldKind::I64 => 2,
            FieldKind::F64 => 3,
            FieldKind::Bool => 4,
            FieldKind::Date => 5,
            FieldKind::Enum => 6,
            FieldKind::Set => 7,
            FieldKind::Blob => 8,
        }
    }

    /// Whether values of this kind can be used in structured filters.
    pub fn is_filterable(&self) -> bool {
        matches!(
            self,
            FieldKind::I64
                | FieldKind::F64
                | FieldKind::Bool
                | FieldKind::Date
                | FieldKind::Enum
                | FieldKind::Set
        )
    }
}

/// One field.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FieldDef {
    /// Name, unique within the schema.
    pub name: String,
    /// Kind.
    pub kind: FieldKind,
}

/// Reserved field holding a document's text id (ADR 0031).
pub const KEY_FIELD: &str = "_key";
/// Reserved field holding a document's tenant (ADR 0031).
pub const TENANT_FIELD: &str = "_tenant";

/// Whether `name` is reserved for Cairn's own fields.
pub fn is_reserved(name: &str) -> bool {
    name.starts_with('_')
}

/// A collection's schema. Field order is significant: documents store values by position.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub struct Schema {
    /// Fields in declaration order.
    pub fields: Vec<FieldDef>,
}

impl Schema {
    /// Builds a schema, rejecting duplicate names and zero-dimension vectors.
    pub fn new(fields: Vec<FieldDef>) -> Result<Self> {
        for (i, f) in fields.iter().enumerate() {
            if f.name.is_empty() {
                return Err(Error::Schema("empty field name".into()));
            }
            if fields[..i].iter().any(|g| g.name == f.name) {
                return Err(Error::Schema(format!("duplicate field {:?}", f.name)));
            }
            if let FieldKind::Vector { dims, .. } = f.kind
                && dims == 0
            {
                return Err(Error::Schema(format!(
                    "field {:?}: zero dimensions",
                    f.name
                )));
            }
        }
        Ok(Schema { fields })
    }

    /// This schema with the reserved fields appended (ADR 0031): `_key`, the text id of
    /// documents written with one (stored, not indexed), and `_tenant`, the tenant a document
    /// belongs to (an enum, so it can be filtered). Names starting with `_` are reserved: a
    /// user schema that uses one is refused.
    pub fn with_reserved(&self) -> Result<Schema> {
        if let Some(f) = self.fields.iter().find(|f| is_reserved(&f.name)) {
            return Err(Error::Schema(format!(
                "field {:?}: names starting with '_' are reserved",
                f.name
            )));
        }
        let mut fields = self.fields.clone();
        fields.push(FieldDef {
            name: KEY_FIELD.into(),
            kind: FieldKind::Blob,
        });
        fields.push(FieldDef {
            name: TENANT_FIELD.into(),
            kind: FieldKind::Enum,
        });
        Schema::new(fields)
    }

    /// Position of the field named `name`.
    pub fn index_of(&self, name: &str) -> Option<usize> {
        self.fields.iter().position(|f| f.name == name)
    }

    /// The field named `name`.
    pub fn field(&self, name: &str) -> Option<&FieldDef> {
        self.fields.iter().find(|f| f.name == name)
    }

    /// Encodes the schema.
    pub fn encode(&self, w: &mut Writer) {
        w.u32(self.fields.len() as u32);
        for f in &self.fields {
            w.str(&f.name).u8(f.kind.tag());
            if let FieldKind::Vector { dims, metric } = &f.kind {
                w.u32(*dims).u8(match metric {
                    Metric::L2 => 0,
                    Metric::Dot => 1,
                    Metric::Cosine => 2,
                });
            }
        }
    }

    /// Decodes a schema.
    pub fn decode(r: &mut Reader<'_>) -> Result<Self> {
        let n = r.u32()?;
        let mut fields = Vec::with_capacity(n.min(1024) as usize);
        for _ in 0..n {
            let name = r.str()?.to_owned();
            let kind = match r.u8()? {
                0 => {
                    let dims = r.u32()?;
                    let metric = match r.u8()? {
                        0 => Metric::L2,
                        1 => Metric::Dot,
                        2 => Metric::Cosine,
                        m => return Err(Error::corruption(format!("unknown metric {m}"))),
                    };
                    FieldKind::Vector { dims, metric }
                }
                1 => FieldKind::Text,
                2 => FieldKind::I64,
                3 => FieldKind::F64,
                4 => FieldKind::Bool,
                5 => FieldKind::Date,
                6 => FieldKind::Enum,
                7 => FieldKind::Set,
                8 => FieldKind::Blob,
                t => return Err(Error::corruption(format!("unknown field kind {t}"))),
            };
            fields.push(FieldDef { name, kind });
        }
        Schema::new(fields).map_err(|e| Error::corruption(format!("invalid stored schema: {e}")))
    }
}
