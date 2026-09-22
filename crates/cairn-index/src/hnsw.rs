//! HNSW graph over one segment's vectors (Malkov & Yashunin), built deterministically.
//!
//! Level assignment comes from a hash of the row's document id, not from a random generator, and
//! rows are inserted in row order, so the same rows always produce the same graph (ADR 0003).
//! The builder is incremental ([`HnswBuilder::insert_next`]) so an async caller can yield
//! between chunks.
//!
//! Search accepts an optional row filter: every visited node steers the traversal, but only rows
//! passing the filter enter the result set. With `two_hop`, the neighbors of a rejected node are
//! expanded immediately (ACORN-1 style), which keeps the search moving when the filter is
//! selective.

use crate::bitmap::Bitmap;
use crate::vectors::Vectors;
use cairn_core::codec::{Reader, Writer};
use cairn_core::{Error, Result};
use std::cmp::Ordering;
use std::collections::BinaryHeap;

const NONE: u32 = u32::MAX;

/// Build parameters.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HnswParams {
    /// Max neighbors per node above level 0 (level 0 gets `2 * m`).
    pub m: u32,
    /// Beam width during construction.
    pub ef_construction: u32,
}

impl Default for HnswParams {
    fn default() -> Self {
        HnswParams {
            m: 16,
            ef_construction: 100,
        }
    }
}

/// Search options.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SearchOptions {
    /// Beam width (at least `k`).
    pub ef: u32,
    /// Expand neighbors of filtered-out nodes immediately.
    pub two_hop: bool,
    /// Upper bound on distance computations (safety valve for hostile filters).
    pub max_visits: u32,
}

impl Default for SearchOptions {
    fn default() -> Self {
        SearchOptions {
            ef: 64,
            two_hop: false,
            max_visits: u32::MAX,
        }
    }
}

/// Candidate ordered by distance (min-heap via reversed `Ord`).
#[derive(Clone, Copy, PartialEq)]
struct Cand {
    dist: f32,
    row: u32,
}

impl Eq for Cand {}

impl PartialOrd for Cand {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}

impl Ord for Cand {
    fn cmp(&self, o: &Self) -> Ordering {
        // Total order on floats (NaN sorts last) then by row for determinism.
        self.dist.total_cmp(&o.dist).then(self.row.cmp(&o.row))
    }
}

/// Result entry ordered by distance descending (max-heap keeps the worst on top).
#[derive(Clone, Copy, PartialEq)]
struct Far(Cand);

impl Eq for Far {}

impl PartialOrd for Far {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}

impl Ord for Far {
    fn cmp(&self, o: &Self) -> Ordering {
        self.0.cmp(&o.0)
    }
}

/// Min-heap wrapper.
#[derive(Clone, Copy, PartialEq, Eq)]
struct Near(Cand);

impl PartialOrd for Near {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}

impl Ord for Near {
    fn cmp(&self, o: &Self) -> Ordering {
        o.0.cmp(&self.0)
    }
}

/// The graph.
#[derive(Debug, Clone, PartialEq)]
pub struct Hnsw {
    params: HnswParams,
    n: u32,
    max_level: u32,
    entry: u32,
    /// Level 0: `n * m0` slots, `NONE` = empty.
    level0: Vec<u32>,
    /// Level of each row.
    levels: Vec<u8>,
    /// Upper levels: for level `l >= 1`, `upper[l - 1]` maps row -> neighbor slots (`m` each).
    upper: Vec<cairn_core::HashMap<u32, Vec<u32>>>,
}

/// Reusable search scratch (visited marks with an epoch).
pub struct SearchScratch {
    visited: Vec<u32>,
    epoch: u32,
    gather: Vec<u8>,
    gather_f32: Vec<f32>,
}

impl SearchScratch {
    /// Scratch sized for `n` rows.
    pub fn new(n: u32) -> Self {
        SearchScratch {
            visited: vec![0; n as usize],
            epoch: 0,
            gather: Vec::new(),
            gather_f32: Vec::new(),
        }
    }

    fn next_epoch(&mut self) {
        self.epoch = self.epoch.wrapping_add(1);
        if self.epoch == 0 {
            self.visited.iter_mut().for_each(|v| *v = 0);
            self.epoch = 1;
        }
    }

    #[inline]
    fn visit(&mut self, row: u32) -> bool {
        let v = &mut self.visited[row as usize];
        if *v == self.epoch {
            false
        } else {
            *v = self.epoch;
            true
        }
    }
}

/// Deterministic level for a row from its document id.
fn level_for(doc_id: u64, m: u32) -> u32 {
    let h = cairn_core::hash::xxh3_64(&doc_id.to_le_bytes());
    // u in (0, 1]: never zero, so ln is finite.
    let u = ((h >> 11) as f64 + 1.0) / (1u64 << 53) as f64;
    let ml = 1.0 / f64::from(m).ln();
    ((-u.ln()) * ml).floor().min(31.0) as u32
}

impl Hnsw {
    fn m0(&self) -> usize {
        2 * self.params.m as usize
    }

    /// Number of rows.
    pub fn len(&self) -> u32 {
        self.n
    }

    /// Whether the graph has no rows.
    pub fn is_empty(&self) -> bool {
        self.n == 0
    }

    /// Build parameters.
    pub fn params(&self) -> HnswParams {
        self.params
    }

    /// Highest level in the graph.
    pub fn max_level(&self) -> u32 {
        self.max_level
    }

    fn neighbors(&self, row: u32, level: u32) -> &[u32] {
        if level == 0 {
            let m0 = self.m0();
            let s = &self.level0[row as usize * m0..(row as usize + 1) * m0];
            let end = s.iter().position(|&x| x == NONE).unwrap_or(m0);
            &s[..end]
        } else {
            self.upper[level as usize - 1].get(&row).map_or(&[], |v| {
                let end = v.iter().position(|&x| x == NONE).unwrap_or(v.len());
                &v[..end]
            })
        }
    }

    fn neighbors_mut(&mut self, row: u32, level: u32) -> &mut [u32] {
        if level == 0 {
            let m0 = self.m0();
            &mut self.level0[row as usize * m0..(row as usize + 1) * m0]
        } else {
            let m = self.params.m as usize;
            self.upper[level as usize - 1]
                .entry(row)
                .or_insert_with(|| vec![NONE; m])
        }
    }

    /// Distances from `q` to `rows` into `out`, gathering rows through `scratch`.
    fn dists(
        vectors: &Vectors,
        q: &[f32],
        rows: &[u32],
        exact: bool,
        scratch: &mut SearchScratch,
        out: &mut Vec<f32>,
    ) {
        out.clear();
        out.resize(rows.len(), 0.0);
        vectors.distances_to(
            q,
            rows,
            exact,
            &mut scratch.gather,
            &mut scratch.gather_f32,
            out,
        );
    }

    /// Greedy beam search on one level. Returns up to `ef` closest rows passing `accept`.
    #[allow(clippy::too_many_arguments)]
    fn search_layer(
        &self,
        vectors: &Vectors,
        q: &[f32],
        entry: &[Cand],
        level: u32,
        ef: usize,
        accept: &dyn Fn(u32) -> bool,
        two_hop: bool,
        max_visits: u32,
        exact: bool,
        scratch: &mut SearchScratch,
    ) -> Vec<Cand> {
        scratch.next_epoch();
        let mut cands: BinaryHeap<Near> = BinaryHeap::new();
        let mut results: BinaryHeap<Far> = BinaryHeap::new();
        let mut visits = 0u32;
        for &e in entry {
            scratch.visit(e.row);
            cands.push(Near(e));
            if accept(e.row) {
                results.push(Far(e));
            }
        }
        let mut pending: Vec<u32> = Vec::new();
        let mut dbuf: Vec<f32> = Vec::new();
        while let Some(Near(c)) = cands.pop() {
            let worst = results.peek().map_or(f32::INFINITY, |f| f.0.dist);
            if results.len() >= ef && c.dist > worst {
                break;
            }
            if visits >= max_visits {
                break;
            }
            pending.clear();
            for &nb in self.neighbors(c.row, level) {
                if scratch.visit(nb) {
                    pending.push(nb);
                }
            }
            if two_hop && level == 0 {
                // Expand neighbors of rejected neighbors right away.
                let direct: Vec<u32> = pending.clone();
                for nb in direct {
                    if !accept(nb) {
                        for &nb2 in self.neighbors(nb, 0) {
                            if scratch.visit(nb2) {
                                pending.push(nb2);
                            }
                        }
                    }
                }
            }
            if pending.is_empty() {
                continue;
            }
            Self::dists(vectors, q, &pending, exact, scratch, &mut dbuf);
            visits += pending.len() as u32;
            for (i, &nb) in pending.iter().enumerate() {
                let d = dbuf[i];
                let worst = results.peek().map_or(f32::INFINITY, |f| f.0.dist);
                let cand = Cand { dist: d, row: nb };
                if results.len() < ef || d < worst {
                    cands.push(Near(cand));
                    if accept(nb) {
                        results.push(Far(cand));
                        if results.len() > ef {
                            results.pop();
                        }
                    }
                } else if two_hop && !accept(nb) {
                    // Rejected and not closer than the beam: keep steering only if we are starved.
                    if results.len() < ef {
                        cands.push(Near(cand));
                    }
                }
            }
        }
        let mut out: Vec<Cand> = results.into_iter().map(|f| f.0).collect();
        out.sort();
        out
    }

    /// Heuristic neighbor selection (algorithm 4 of the paper, without pruned-connection reuse).
    fn select_neighbors(
        vectors: &Vectors,
        cands: &[Cand],
        m: usize,
        scratch: &mut SearchScratch,
    ) -> Vec<u32> {
        let mut selected: Vec<Cand> = Vec::with_capacity(m);
        let mut dbuf = Vec::new();
        for &c in cands {
            if selected.len() >= m {
                break;
            }
            let rows: Vec<u32> = selected.iter().map(|s| s.row).collect();
            let mut keep = true;
            if !rows.is_empty() {
                let cv = vectors.row_f32(c.row).to_vec();
                Self::dists(vectors, &cv, &rows, true, scratch, &mut dbuf);
                if dbuf.iter().any(|&d| d < c.dist) {
                    keep = false;
                }
            }
            if keep {
                selected.push(c);
            }
        }
        selected.iter().map(|c| c.row).collect()
    }

    /// k nearest rows to `q` (ascending distance), honoring `filter`.
    pub fn search(
        &self,
        vectors: &Vectors,
        q: &[f32],
        k: usize,
        filter: Option<&Bitmap>,
        opts: SearchOptions,
        scratch: &mut SearchScratch,
    ) -> Vec<(f32, u32)> {
        if self.n == 0 {
            return Vec::new();
        }
        let accept_all = |_: u32| true;
        let accept_filter = |r: u32| filter.is_some_and(|f| f.contains(r));
        let accept: &dyn Fn(u32) -> bool = if filter.is_some() {
            &accept_filter
        } else {
            &accept_all
        };
        let mut dbuf = Vec::new();
        Self::dists(vectors, q, &[self.entry], false, scratch, &mut dbuf);
        let mut ep = vec![Cand {
            dist: dbuf[0],
            row: self.entry,
        }];
        for level in (1..=self.max_level).rev() {
            let r = self.search_layer(
                vectors,
                q,
                &ep,
                level,
                1,
                &accept_all,
                false,
                u32::MAX,
                false,
                scratch,
            );
            if let Some(&c) = r.first() {
                ep = vec![c];
            }
        }
        let ef = (opts.ef as usize).max(k);
        let r = self.search_layer(
            vectors,
            q,
            &ep,
            0,
            ef,
            accept,
            opts.two_hop,
            opts.max_visits,
            false,
            scratch,
        );
        r.into_iter().take(k).map(|c| (c.dist, c.row)).collect()
    }

    /// Serializes the graph.
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.u32(self.params.m)
            .u32(self.params.ef_construction)
            .u32(self.n)
            .u32(self.max_level)
            .u32(self.entry);
        for &x in &self.level0 {
            w.u32(x);
        }
        w.raw(&self.levels);
        for lvl in &self.upper {
            let mut rows: Vec<(&u32, &Vec<u32>)> = lvl.iter().collect();
            rows.sort();
            w.u32(rows.len() as u32);
            for (row, nbs) in rows {
                w.u32(*row);
                for &x in nbs {
                    w.u32(x);
                }
            }
        }
        w.into_vec()
    }

    /// Deserializes a graph.
    pub fn decode(bytes: &[u8]) -> Result<Hnsw> {
        let mut r = Reader::new(bytes);
        let params = HnswParams {
            m: r.u32()?,
            ef_construction: r.u32()?,
        };
        if params.m == 0 || params.m > 256 {
            return Err(Error::corruption("hnsw m"));
        }
        let n = r.u32()?;
        let max_level = r.u32()?;
        let entry = r.u32()?;
        let m0 = 2 * params.m as usize;
        let mut level0 = Vec::with_capacity(n as usize * m0);
        for _ in 0..n as usize * m0 {
            let x = r.u32()?;
            if x != NONE && x >= n {
                return Err(Error::corruption("hnsw neighbor out of range"));
            }
            level0.push(x);
        }
        let levels = r.raw(n as usize)?.to_vec();
        let mut upper = Vec::with_capacity(max_level as usize);
        for _ in 0..max_level {
            let cnt = r.u32()? as usize;
            let mut map = cairn_core::HashMap::default();
            for _ in 0..cnt {
                let row = r.u32()?;
                let mut nbs = Vec::with_capacity(params.m as usize);
                for _ in 0..params.m {
                    let x = r.u32()?;
                    if x != NONE && x >= n {
                        return Err(Error::corruption("hnsw upper neighbor out of range"));
                    }
                    nbs.push(x);
                }
                map.insert(row, nbs);
            }
            upper.push(map);
        }
        r.finish()?;
        if n > 0 && entry >= n {
            return Err(Error::corruption("hnsw entry point"));
        }
        Ok(Hnsw {
            params,
            n,
            max_level,
            entry,
            level0,
            levels,
            upper,
        })
    }
}

/// Incremental builder.
pub struct HnswBuilder<'a> {
    vectors: &'a Vectors,
    doc_ids: &'a [u64],
    graph: Hnsw,
    next: u32,
    scratch: SearchScratch,
}

impl<'a> HnswBuilder<'a> {
    /// Starts a build over `vectors` whose rows carry `doc_ids`.
    pub fn new(vectors: &'a Vectors, doc_ids: &'a [u64], params: HnswParams) -> Self {
        let n = vectors.len();
        assert_eq!(doc_ids.len(), n as usize);
        let graph = Hnsw {
            params,
            n: 0,
            max_level: 0,
            entry: NONE,
            level0: vec![NONE; n as usize * 2 * params.m as usize],
            levels: vec![0; n as usize],
            upper: Vec::new(),
        };
        HnswBuilder {
            vectors,
            doc_ids,
            graph,
            next: 0,
            scratch: SearchScratch::new(n),
        }
    }

    /// Rows inserted so far.
    pub fn inserted(&self) -> u32 {
        self.next
    }

    /// Whether every row is inserted.
    pub fn is_done(&self) -> bool {
        self.next == self.vectors.len()
    }

    /// Inserts the next row. Returns `false` when done.
    pub fn insert_next(&mut self) -> bool {
        if self.is_done() {
            return false;
        }
        let row = self.next;
        self.next += 1;
        let level = level_for(self.doc_ids[row as usize], self.graph.params.m);
        self.graph.levels[row as usize] = level as u8;
        while self.graph.upper.len() < level as usize {
            self.graph.upper.push(cairn_core::HashMap::default());
        }
        let q = self.vectors.row_f32(row).to_vec();
        if self.graph.entry == NONE {
            self.graph.entry = row;
            self.graph.max_level = level;
            self.graph.n = row + 1;
            for l in 1..=level {
                self.graph.neighbors_mut(row, l);
            }
            return true;
        }
        let m = self.graph.params.m as usize;
        let efc = self.graph.params.ef_construction as usize;
        let mut dbuf = Vec::new();
        Hnsw::dists(
            self.vectors,
            &q,
            &[self.graph.entry],
            true,
            &mut self.scratch,
            &mut dbuf,
        );
        let mut ep = vec![Cand {
            dist: dbuf[0],
            row: self.graph.entry,
        }];
        let top = self.graph.max_level;
        for l in ((level + 1)..=top).rev() {
            let r = self.graph.search_layer(
                self.vectors,
                &q,
                &ep,
                l,
                1,
                &|_| true,
                false,
                u32::MAX,
                true,
                &mut self.scratch,
            );
            if let Some(&c) = r.first() {
                ep = vec![c];
            }
        }
        for l in (0..=level.min(top)).rev() {
            let cands = self.graph.search_layer(
                self.vectors,
                &q,
                &ep,
                l,
                efc,
                &|_| true,
                false,
                u32::MAX,
                true,
                &mut self.scratch,
            );
            let cap = if l == 0 { 2 * m } else { m };
            let selected = Hnsw::select_neighbors(self.vectors, &cands, cap, &mut self.scratch);
            {
                let slots = self.graph.neighbors_mut(row, l);
                for (i, s) in slots.iter_mut().enumerate() {
                    *s = selected.get(i).copied().unwrap_or(NONE);
                }
            }
            for &nb in &selected {
                self.link_back(nb, row, l, cap);
            }
            ep = cands;
        }
        if level > top {
            self.graph.max_level = level;
            self.graph.entry = row;
        }
        self.graph.n = row + 1;
        true
    }

    /// Adds `row` to `nb`'s list at `level`, pruning with the heuristic when full.
    fn link_back(&mut self, nb: u32, row: u32, level: u32, cap: usize) {
        let current: Vec<u32> = self.graph.neighbors(nb, level).to_vec();
        if current.len() < cap {
            let slots = self.graph.neighbors_mut(nb, level);
            slots[current.len()] = row;
            return;
        }
        let nbv = self.vectors.row_f32(nb).to_vec();
        let mut all: Vec<u32> = current;
        all.push(row);
        let mut dbuf = Vec::new();
        Hnsw::dists(self.vectors, &nbv, &all, true, &mut self.scratch, &mut dbuf);
        let mut cands: Vec<Cand> = all
            .iter()
            .zip(&dbuf)
            .map(|(&r, &d)| Cand { dist: d, row: r })
            .collect();
        cands.sort();
        let selected = Hnsw::select_neighbors(self.vectors, &cands, cap, &mut self.scratch);
        let slots = self.graph.neighbors_mut(nb, level);
        for (i, s) in slots.iter_mut().enumerate() {
            *s = selected.get(i).copied().unwrap_or(NONE);
        }
    }

    /// Inserts up to `count` rows; returns how many were inserted.
    pub fn insert_batch(&mut self, count: usize) -> usize {
        let mut done = 0;
        while done < count && self.insert_next() {
            done += 1;
        }
        done
    }

    /// Finishes the build (inserting any remaining rows).
    pub fn finish(mut self) -> Hnsw {
        while self.insert_next() {}
        self.graph
    }
}
