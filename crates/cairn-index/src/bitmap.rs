//! Dense row bitmap for one segment.
//!
//! Segments hold at most about a million rows, so a dense `u64` bitset (128 KiB per million
//! rows) is cheaper and simpler than a compressed representation; filter evaluation, deletion
//! masking and candidate acceptance all work on this type.

use cairn_storage::DeletionSet;

/// Bit per row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bitmap {
    rows: u32,
    words: Vec<u64>,
}

impl Bitmap {
    /// All bits clear.
    pub fn empty(rows: u32) -> Self {
        Bitmap {
            rows,
            words: vec![0; (rows as usize).div_ceil(64)],
        }
    }

    /// All bits set.
    pub fn full(rows: u32) -> Self {
        let mut b = Bitmap {
            rows,
            words: vec![u64::MAX; (rows as usize).div_ceil(64)],
        };
        b.trim();
        b
    }

    /// From raw words (extra high bits are cleared).
    pub fn from_words(rows: u32, words: Vec<u64>) -> Self {
        let mut b = Bitmap { rows, words };
        b.words.resize((rows as usize).div_ceil(64), 0);
        b.trim();
        b
    }

    /// Rows deleted in `d`, as a bitmap.
    pub fn from_deletions(d: &DeletionSet) -> Self {
        Bitmap::from_words(d.rows(), d.words().to_vec())
    }

    fn trim(&mut self) {
        let rem = self.rows % 64;
        if rem != 0
            && let Some(last) = self.words.last_mut()
        {
            *last &= (1u64 << rem) - 1;
        }
    }

    /// Number of rows covered.
    pub fn rows(&self) -> u32 {
        self.rows
    }

    /// Sets a bit.
    pub fn set(&mut self, row: u32) {
        self.words[row as usize / 64] |= 1 << (row % 64);
    }

    /// Clears a bit.
    pub fn clear(&mut self, row: u32) {
        self.words[row as usize / 64] &= !(1 << (row % 64));
    }

    /// Whether a bit is set.
    pub fn contains(&self, row: u32) -> bool {
        self.words
            .get(row as usize / 64)
            .is_some_and(|w| w & (1 << (row % 64)) != 0)
    }

    /// Number of set bits.
    pub fn count(&self) -> u32 {
        self.words.iter().map(|w| w.count_ones()).sum()
    }

    /// `self &= other`.
    pub fn and_with(&mut self, other: &Bitmap) {
        for (a, b) in self.words.iter_mut().zip(&other.words) {
            *a &= *b;
        }
    }

    /// `self |= other`.
    pub fn or_with(&mut self, other: &Bitmap) {
        for (a, b) in self.words.iter_mut().zip(&other.words) {
            *a |= *b;
        }
    }

    /// `self &= !other`.
    pub fn and_not_with(&mut self, other: &Bitmap) {
        for (a, b) in self.words.iter_mut().zip(&other.words) {
            *a &= !*b;
        }
    }

    /// Flips every bit.
    pub fn negate(&mut self) {
        for w in &mut self.words {
            *w = !*w;
        }
        self.trim();
    }

    /// Set rows in increasing order.
    pub fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        self.words.iter().enumerate().flat_map(|(i, &w)| {
            let mut w = w;
            std::iter::from_fn(move || {
                if w == 0 {
                    None
                } else {
                    let b = w.trailing_zeros();
                    w &= w - 1;
                    Some(i as u32 * 64 + b)
                }
            })
        })
    }

    /// Raw words.
    pub fn words(&self) -> &[u64] {
        &self.words
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ops_and_iteration() {
        let mut a = Bitmap::empty(130);
        a.set(0);
        a.set(64);
        a.set(129);
        assert_eq!(a.iter().collect::<Vec<_>>(), vec![0, 64, 129]);
        assert_eq!(a.count(), 3);
        let mut f = Bitmap::full(130);
        assert_eq!(f.count(), 130);
        f.and_not_with(&a);
        assert_eq!(f.count(), 127);
        assert!(!f.contains(129));
        f.negate();
        assert_eq!(f, a);
        let mut b = a.clone();
        b.clear(64);
        b.or_with(&a);
        assert_eq!(b, a);
        b.and_with(&Bitmap::empty(130));
        assert_eq!(b.count(), 0);
    }
}
