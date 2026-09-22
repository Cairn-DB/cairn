//! Minimal bounds-checked binary encoding used by every on-disk structure.
//!
//! Little-endian fixed-width integers, length-prefixed byte strings. Every read reports
//! corruption instead of panicking, which is what makes the readers fuzzable.

use crate::{Error, Result};
use bytes::Bytes;

/// Appends fields to a byte vector.
#[derive(Default)]
pub struct Writer {
    buf: Vec<u8>,
}

impl Writer {
    /// Empty writer.
    pub fn new() -> Self {
        Writer::default()
    }

    /// Writer with reserved capacity.
    pub fn with_capacity(n: usize) -> Self {
        Writer {
            buf: Vec::with_capacity(n),
        }
    }

    /// Appends a `u8`.
    pub fn u8(&mut self, v: u8) -> &mut Self {
        self.buf.push(v);
        self
    }

    /// Appends a `u32`.
    pub fn u32(&mut self, v: u32) -> &mut Self {
        self.buf.extend_from_slice(&v.to_le_bytes());
        self
    }

    /// Appends a `u64`.
    pub fn u64(&mut self, v: u64) -> &mut Self {
        self.buf.extend_from_slice(&v.to_le_bytes());
        self
    }

    /// Appends an `f32`.
    pub fn f32(&mut self, v: f32) -> &mut Self {
        self.buf.extend_from_slice(&v.to_le_bytes());
        self
    }

    /// Appends raw bytes without a length prefix.
    pub fn raw(&mut self, v: &[u8]) -> &mut Self {
        self.buf.extend_from_slice(v);
        self
    }

    /// Appends a `u32` length prefix and the bytes.
    pub fn bytes(&mut self, v: &[u8]) -> &mut Self {
        self.u32(u32::try_from(v.len()).expect("byte string longer than u32::MAX"));
        self.raw(v)
    }

    /// Appends a length-prefixed UTF-8 string.
    pub fn str(&mut self, v: &str) -> &mut Self {
        self.bytes(v.as_bytes())
    }

    /// Bytes written so far.
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// Whether nothing was written.
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// The encoded bytes.
    pub fn as_slice(&self) -> &[u8] {
        &self.buf
    }

    /// Consumes the writer.
    pub fn into_vec(self) -> Vec<u8> {
        self.buf
    }

    /// Consumes the writer into `Bytes`.
    pub fn into_bytes(self) -> Bytes {
        Bytes::from(self.buf)
    }
}

/// Reads fields from a byte slice.
pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    /// Reader over `buf`.
    pub fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }

    fn take(&mut self, n: usize, what: &str) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or_else(|| Error::corruption("length overflow"))?;
        if end > self.buf.len() {
            return Err(Error::corruption(format!(
                "truncated {what}: need {n} bytes at {}, have {}",
                self.pos,
                self.buf.len() - self.pos
            )));
        }
        let s = &self.buf[self.pos..end];
        self.pos = end;
        Ok(s)
    }

    /// Reads a `u8`.
    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1, "u8")?[0])
    }

    /// Reads a `u32`.
    pub fn u32(&mut self) -> Result<u32> {
        let b = self.take(4, "u32")?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    /// Reads a `u64`.
    pub fn u64(&mut self) -> Result<u64> {
        let b = self.take(8, "u64")?;
        Ok(u64::from_le_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }

    /// Reads an `f32`.
    pub fn f32(&mut self) -> Result<f32> {
        let b = self.take(4, "f32")?;
        Ok(f32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    /// Reads `n` raw bytes.
    pub fn raw(&mut self, n: usize) -> Result<&'a [u8]> {
        self.take(n, "raw bytes")
    }

    /// Reads a `u32`-prefixed byte string.
    pub fn bytes(&mut self) -> Result<&'a [u8]> {
        let n = self.u32()? as usize;
        self.take(n, "byte string")
    }

    /// Reads a length-prefixed UTF-8 string.
    pub fn str(&mut self) -> Result<&'a str> {
        std::str::from_utf8(self.bytes()?)
            .map_err(|e| Error::corruption(format!("invalid utf-8: {e}")))
    }

    /// Bytes not yet consumed.
    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    /// Current position.
    pub fn position(&self) -> usize {
        self.pos
    }

    /// Fails unless everything was consumed.
    pub fn finish(self) -> Result<()> {
        if self.remaining() == 0 {
            Ok(())
        } else {
            Err(Error::corruption(format!(
                "{} trailing bytes",
                self.remaining()
            )))
        }
    }
}

/// CRC-32 (IEEE) of `data`; hardware accelerated by `crc32fast`.
pub fn crc32(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}

/// xxh3-64 of `data`, used for section and file fingerprints.
pub fn xxh3(data: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn roundtrip(a in any::<u8>(), b in any::<u32>(), c in any::<u64>(), d in any::<f32>(), s in ".*", v in proptest::collection::vec(any::<u8>(), 0..64)) {
            let mut w = Writer::new();
            w.u8(a).u32(b).u64(c).f32(d).str(&s).bytes(&v);
            let mut r = Reader::new(w.as_slice());
            prop_assert_eq!(r.u8().unwrap(), a);
            prop_assert_eq!(r.u32().unwrap(), b);
            prop_assert_eq!(r.u64().unwrap(), c);
            prop_assert_eq!(r.f32().unwrap().to_bits(), d.to_bits());
            prop_assert_eq!(r.str().unwrap(), s.as_str());
            prop_assert_eq!(r.bytes().unwrap(), v.as_slice());
            prop_assert!(r.finish().is_ok());
        }

        #[test]
        fn never_panics_on_garbage(v in proptest::collection::vec(any::<u8>(), 0..64)) {
            let mut r = Reader::new(&v);
            let _ = r.u32();
            let _ = r.bytes();
            let _ = r.str();
            let _ = r.u64();
            let _ = r.raw(1000);
        }
    }
}
