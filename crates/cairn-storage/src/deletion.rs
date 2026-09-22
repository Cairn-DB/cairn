//! Per-segment deletion bitmap, the only mutable state attached to a segment.

use crate::manifest::Manifest;
use cairn_core::codec::{Reader, Writer};
use cairn_core::{Error, Result};

/// Bit per row; set means deleted.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DeletionSet {
    rows: u32,
    words: Vec<u64>,
    count: u32,
}

impl DeletionSet {
    /// Empty set over `rows` rows.
    pub fn new(rows: u32) -> Self {
        DeletionSet {
            rows,
            words: vec![0; (rows as usize).div_ceil(64)],
            count: 0,
        }
    }

    /// Marks `row` deleted; returns whether it was newly deleted.
    pub fn set(&mut self, row: u32) -> bool {
        debug_assert!(row < self.rows);
        let (w, b) = (row as usize / 64, row % 64);
        let mask = 1u64 << b;
        if self.words[w] & mask == 0 {
            self.words[w] |= mask;
            self.count += 1;
            true
        } else {
            false
        }
    }

    /// Whether `row` is deleted.
    pub fn contains(&self, row: u32) -> bool {
        let (w, b) = (row as usize / 64, row % 64);
        self.words.get(w).is_some_and(|x| x & (1u64 << b) != 0)
    }

    /// Number of deleted rows.
    pub fn count(&self) -> u32 {
        self.count
    }

    /// Number of rows covered.
    pub fn rows(&self) -> u32 {
        self.rows
    }

    /// Whether no row is deleted.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Raw words (for bitmap arithmetic in the index layer).
    pub fn words(&self) -> &[u64] {
        &self.words
    }
}

impl Manifest for DeletionSet {
    fn encode(&self, w: &mut Writer) {
        w.u32(self.rows)
            .u32(self.count)
            .u32(self.words.len() as u32);
        for x in &self.words {
            w.u64(*x);
        }
    }

    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        let rows = r.u32()?;
        let count = r.u32()?;
        let n = r.u32()? as usize;
        if n != (rows as usize).div_ceil(64) {
            return Err(Error::corruption("deletion set word count mismatch"));
        }
        let mut words = Vec::with_capacity(n);
        for _ in 0..n {
            words.push(r.u64()?);
        }
        let actual: u32 = words.iter().map(|w| w.count_ones()).sum();
        if actual != count {
            return Err(Error::corruption("deletion set count mismatch"));
        }
        Ok(DeletionSet { rows, words, count })
    }
}
