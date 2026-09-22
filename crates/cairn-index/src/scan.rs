//! Exact scan: distances to every allowed row, top-k by a bounded heap.

use crate::bitmap::Bitmap;
use crate::vectors::Vectors;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

#[derive(PartialEq)]
struct Entry(f32, u32);

impl Eq for Entry {}

impl PartialOrd for Entry {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}

impl Ord for Entry {
    fn cmp(&self, o: &Self) -> Ordering {
        self.0.total_cmp(&o.0).then(self.1.cmp(&o.1))
    }
}

/// Bounded top-k (smallest distances) collector.
pub struct TopK {
    k: usize,
    heap: BinaryHeap<Entry>,
}

impl TopK {
    /// Collector for `k` results.
    pub fn new(k: usize) -> Self {
        TopK {
            k,
            heap: BinaryHeap::with_capacity(k + 1),
        }
    }

    /// Offers a candidate.
    #[inline]
    pub fn push(&mut self, dist: f32, row: u32) {
        if self.heap.len() < self.k {
            self.heap.push(Entry(dist, row));
        } else if let Some(worst) = self.heap.peek()
            && (dist < worst.0 || (dist == worst.0 && row < worst.1))
        {
            self.heap.pop();
            self.heap.push(Entry(dist, row));
        }
    }

    /// Current worst distance, or infinity when not full.
    pub fn threshold(&self) -> f32 {
        if self.heap.len() < self.k {
            f32::INFINITY
        } else {
            self.heap.peek().map_or(f32::INFINITY, |e| e.0)
        }
    }

    /// Results, ascending.
    pub fn into_sorted(self) -> Vec<(f32, u32)> {
        let mut v: Vec<(f32, u32)> = self.heap.into_iter().map(|e| (e.0, e.1)).collect();
        v.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        v
    }
}

const CHUNK: usize = 256;

/// Top-`k` rows by distance among the rows of `filter` (all rows when `None`).
/// With SQ8 available and `exact == false`, candidates are scored with SQ8 and the best
/// `rerank` are re-scored exactly.
pub fn exact_scan(
    vectors: &Vectors,
    q: &[f32],
    k: usize,
    filter: Option<&Bitmap>,
    exact: bool,
    rerank: usize,
) -> Vec<(f32, u32)> {
    let n = vectors.len();
    let use_sq8 = vectors.has_sq8() && !exact;
    let k_first = if use_sq8 { rerank.max(k) } else { k };
    let mut top = TopK::new(k_first);
    let mut rows: Vec<u32> = Vec::with_capacity(CHUNK);
    let mut out = vec![0f32; CHUNK];
    let (mut g8, mut g32) = (Vec::new(), Vec::new());
    match filter {
        None => {
            let mut start = 0u32;
            while start < n {
                let len = (n - start).min(CHUNK as u32) as usize;
                vectors.distances_range(q, start, !use_sq8, &mut out[..len]);
                for (i, &d) in out[..len].iter().enumerate() {
                    top.push(d, start + i as u32);
                }
                start += len as u32;
            }
        }
        Some(f) => {
            let mut it = f.iter();
            loop {
                rows.clear();
                rows.extend(it.by_ref().take(CHUNK));
                if rows.is_empty() {
                    break;
                }
                vectors.distances_to(
                    q,
                    &rows,
                    !use_sq8,
                    &mut g8,
                    &mut g32,
                    &mut out[..rows.len()],
                );
                for (i, &r) in rows.iter().enumerate() {
                    top.push(out[i], r);
                }
            }
        }
    }
    let first = top.into_sorted();
    if !use_sq8 {
        return first;
    }
    let rows: Vec<u32> = first.iter().map(|(_, r)| *r).collect();
    let mut exact_d = vec![0f32; rows.len()];
    vectors.distances_to(q, &rows, true, &mut g8, &mut g32, &mut exact_d);
    let mut top = TopK::new(k);
    for (i, &r) in rows.iter().enumerate() {
        top.push(exact_d[i], r);
    }
    top.into_sorted()
}
