//! Document columns inside a segment: the row store used for point reads and payload fetches.
//!
//! Sections written per segment: `schema`, `docids` (sorted `u64`), and per field `i`:
//! `nulls.i` (one byte per row, 1 = present) and `col.i`. Fixed-width kinds (vector, i64, f64,
//! bool, date) store one row per slot; variable-width kinds store `[u32 offsets; rows + 1]`
//! followed by the codec-encoded values.

use crate::segment::{SegmentReader, SegmentWriter};
use cairn_core::codec::{Reader, Writer};
use cairn_core::{DocId, Document, Error, FieldKind, Result, Runtime, Schema, Value};

#[derive(Debug, Clone, Copy)]
enum Layout {
    Fixed(usize),
    Var,
}

fn layout(kind: &FieldKind) -> Layout {
    match kind {
        FieldKind::Vector { dims, .. } => Layout::Fixed(4 * *dims as usize),
        FieldKind::I64 | FieldKind::F64 | FieldKind::Date => Layout::Fixed(8),
        FieldKind::Bool => Layout::Fixed(1),
        FieldKind::Text | FieldKind::Enum | FieldKind::Set | FieldKind::Blob => Layout::Var,
    }
}

fn encode_fixed(v: &Value, out: &mut Vec<u8>, width: usize) {
    match v {
        Value::Vector(x) => {
            for f in x {
                out.extend_from_slice(&f.to_le_bytes());
            }
        }
        Value::I64(x) | Value::Date(x) => out.extend_from_slice(&x.to_le_bytes()),
        Value::F64(x) => out.extend_from_slice(&x.to_bits().to_le_bytes()),
        Value::Bool(b) => out.push(u8::from(*b)),
        _ => out.extend(std::iter::repeat_n(0, width)),
    }
}

fn decode_fixed(kind: &FieldKind, bytes: &[u8]) -> Result<Value> {
    let mut r = Reader::new(bytes);
    Ok(match kind {
        FieldKind::Vector { dims, .. } => {
            let mut v = Vec::with_capacity(*dims as usize);
            for _ in 0..*dims {
                v.push(r.f32()?);
            }
            Value::Vector(v)
        }
        FieldKind::I64 => Value::I64(r.u64()? as i64),
        FieldKind::Date => Value::Date(r.u64()? as i64),
        FieldKind::F64 => Value::F64(f64::from_bits(r.u64()?)),
        FieldKind::Bool => Value::Bool(r.u8()? != 0),
        _ => {
            return Err(Error::Internal(
                "decode_fixed on variable-width kind".into(),
            ));
        }
    })
}

/// Writes the row store of `docs` (sorted by id, unique) into `w`.
pub async fn write_columns<R: Runtime>(
    w: &mut SegmentWriter<R>,
    schema: &Schema,
    docs: &[&Document],
) -> Result<()> {
    debug_assert!(docs.windows(2).all(|p| p[0].id < p[1].id));
    let mut sw = Writer::new();
    schema.encode(&mut sw);
    w.add_section("schema", sw.as_slice()).await?;
    let mut ids = Vec::with_capacity(docs.len() * 8);
    for d in docs {
        ids.extend_from_slice(&d.id.get().to_le_bytes());
    }
    w.add_section("docids", &ids).await?;
    for (i, f) in schema.fields.iter().enumerate() {
        let nulls: Vec<u8> = docs
            .iter()
            .map(|d| u8::from(d.values[i].is_some()))
            .collect();
        w.add_section(&format!("nulls.{i}"), &nulls).await?;
        let mut col = Vec::new();
        match layout(&f.kind) {
            Layout::Fixed(width) => {
                col.reserve(width * docs.len());
                for d in docs {
                    match &d.values[i] {
                        Some(v) => encode_fixed(v, &mut col, width),
                        None => col.extend(std::iter::repeat_n(0, width)),
                    }
                }
            }
            Layout::Var => {
                let mut offsets = Writer::with_capacity(4 * (docs.len() + 1));
                let mut bytes = Writer::new();
                for d in docs {
                    offsets.u32(bytes.len() as u32);
                    if let Some(v) = &d.values[i] {
                        v.encode(&mut bytes);
                    }
                }
                offsets.u32(bytes.len() as u32);
                col.extend_from_slice(offsets.as_slice());
                col.extend_from_slice(bytes.as_slice());
            }
        }
        w.add_section(&format!("col.{i}"), &col).await?;
    }
    Ok(())
}

/// Row store reader for one segment.
pub struct DocStore {
    schema: Schema,
    docids: Vec<u64>,
}

impl DocStore {
    /// Loads the doc-id column and schema from an open segment.
    pub async fn open<R: Runtime>(reader: &SegmentReader<R>) -> Result<Self> {
        let sb = reader.read_section("schema").await?;
        let mut r = Reader::new(&sb);
        let schema = Schema::decode(&mut r)?;
        r.finish()?;
        let ids = reader.read_section("docids").await?;
        if ids.len() % 8 != 0 {
            return Err(Error::corruption("docids section length"));
        }
        let docids: Vec<u64> = ids
            .chunks_exact(8)
            .map(|c| u64::from_le_bytes(c.try_into().expect("8 bytes")))
            .collect();
        if docids.windows(2).any(|p| p[0] >= p[1]) {
            return Err(Error::corruption("docids not strictly increasing"));
        }
        Ok(DocStore { schema, docids })
    }

    /// The stored schema.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Number of rows.
    pub fn doc_count(&self) -> u32 {
        self.docids.len() as u32
    }

    /// Sorted document ids.
    pub fn docids(&self) -> &[u64] {
        &self.docids
    }

    /// Row of `id`, if present.
    pub fn row_of(&self, id: DocId) -> Option<u32> {
        self.docids.binary_search(&id.get()).ok().map(|r| r as u32)
    }

    /// Reads the whole document at `row`.
    pub async fn read_doc<R: Runtime>(
        &self,
        reader: &SegmentReader<R>,
        row: u32,
    ) -> Result<Document> {
        let id = DocId(
            *self
                .docids
                .get(row as usize)
                .ok_or_else(|| Error::Internal(format!("row {row} out of range")))?,
        );
        let mut doc = Document::new(id, self.schema.fields.len());
        for (i, f) in self.schema.fields.iter().enumerate() {
            if let Some(v) = self.read_value(reader, row, i, &f.kind).await? {
                doc.values[i] = Some(v);
            }
        }
        Ok(doc)
    }

    /// Reads every row (whole-section reads), skipping rows for which `skip(row)` is true.
    pub async fn read_all<R: Runtime>(
        &self,
        reader: &SegmentReader<R>,
        skip: impl Fn(u32) -> bool,
    ) -> Result<Vec<Document>> {
        let rows = self.docids.len();
        let mut docs: Vec<Document> = (0..rows)
            .filter(|r| !skip(*r as u32))
            .map(|r| Document::new(DocId(self.docids[r]), self.schema.fields.len()))
            .collect();
        let kept: Vec<usize> = (0..rows).filter(|r| !skip(*r as u32)).collect();
        for (i, f) in self.schema.fields.iter().enumerate() {
            let nulls = reader.read_section(&format!("nulls.{i}")).await?;
            if nulls.len() != rows {
                return Err(Error::corruption("nulls section length"));
            }
            let col = reader.read_section(&format!("col.{i}")).await?;
            match layout(&f.kind) {
                Layout::Fixed(width) => {
                    if col.len() != width * rows {
                        return Err(Error::corruption(format!("column {i} length")));
                    }
                    for (out, &row) in docs.iter_mut().zip(&kept) {
                        if nulls[row] != 0 {
                            out.values[i] =
                                Some(decode_fixed(&f.kind, &col[row * width..(row + 1) * width])?);
                        }
                    }
                }
                Layout::Var => {
                    let base = 4 * (rows + 1);
                    if col.len() < base {
                        return Err(Error::corruption(format!("column {i} offsets")));
                    }
                    let off = |r: usize| {
                        u32::from_le_bytes(col[4 * r..4 * r + 4].try_into().expect("4 bytes"))
                            as usize
                    };
                    for (out, &row) in docs.iter_mut().zip(&kept) {
                        if nulls[row] != 0 {
                            let (start, end) = (off(row), off(row + 1));
                            if end < start || base + end > col.len() {
                                return Err(Error::corruption(format!(
                                    "column {i} row {row} range"
                                )));
                            }
                            let mut r = Reader::new(&col[base + start..base + end]);
                            out.values[i] = Some(Value::decode(&mut r)?);
                            r.finish()?;
                        }
                    }
                }
            }
        }
        Ok(docs)
    }

    /// Reads one field of one row.
    pub async fn read_value<R: Runtime>(
        &self,
        reader: &SegmentReader<R>,
        row: u32,
        field: usize,
        kind: &FieldKind,
    ) -> Result<Option<Value>> {
        let present = reader
            .read_range(&format!("nulls.{field}"), row as u64, 1)
            .await?;
        if present[0] == 0 {
            return Ok(None);
        }
        let name = format!("col.{field}");
        match layout(kind) {
            Layout::Fixed(width) => {
                let bytes = reader
                    .read_range(&name, row as u64 * width as u64, width)
                    .await?;
                decode_fixed(kind, &bytes).map(Some)
            }
            Layout::Var => {
                let offs = reader.read_range(&name, 4 * row as u64, 8).await?;
                let mut r = Reader::new(&offs);
                let (start, end) = (r.u32()? as u64, r.u32()? as u64);
                if end < start {
                    return Err(Error::corruption("column offsets not monotonic"));
                }
                let base = 4 * (self.docids.len() as u64 + 1);
                let bytes = reader
                    .read_range(&name, base + start, (end - start) as usize)
                    .await?;
                let mut r = Reader::new(&bytes);
                let v = Value::decode(&mut r)?;
                r.finish()?;
                Ok(Some(v))
            }
        }
    }
}
