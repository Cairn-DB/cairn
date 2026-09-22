//! Synthetic attributes and workloads for Cairn benchmarks (SPEC.md section 7).
//!
//! Everything is a pure function of a seed. Two families of attributes are generated per item:
//!
//! - **Controlled flags** `flag_50`, `flag_10`, `flag_1`, `flag_01`: booleans true for about
//!   50%, 10%, 1% and 0.1% of items, either at random or *correlated* with the item's cluster
//!   (true for whole clusters), which is the hard case for filtered ANN. In clustered mode the
//!   achievable fraction is quantized by the cluster count: use at least `1 / fraction` clusters
//!   (1,000 for `flag_01`), and expect a few percent of deviation on 10% and 50%.
//! - **Realistic attributes**: `date` (days since 1990-01-01), `channel` (Zipf over a small
//!   vocabulary), `rights` (`cleared` / `restricted` / `pending`), `speaker` (Zipf over many ids).
//!
//! Plus a takedown schedule: which items are deleted at which millisecond, at a configured rate.

use cairn_core::SeededRng;
use cairn_core::hash::xxh3_64;

/// How flags relate to vector clusters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Correlation {
    /// Independent of the vectors: easy for filtered search.
    Random,
    /// Whole clusters pass or fail: realistic and hard.
    Clustered,
}

/// Generator settings.
#[derive(Debug, Clone)]
pub struct GenConfig {
    /// Seed.
    pub seed: u64,
    /// Number of items.
    pub n: u64,
    /// Correlation mode for the controlled flags.
    pub correlation: Correlation,
    /// Number of clusters when no cluster labels are supplied.
    pub clusters: u32,
    /// Fraction of items with `rights == cleared`.
    pub cleared_fraction: f64,
    /// Number of channels.
    pub channels: u32,
    /// Number of speakers.
    pub speakers: u32,
}

impl Default for GenConfig {
    fn default() -> Self {
        GenConfig {
            seed: 1,
            n: 1_000_000,
            correlation: Correlation::Random,
            clusters: 1000,
            cleared_fraction: 0.4,
            channels: 12,
            speakers: 5000,
        }
    }
}

/// Attributes of one item.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Attributes {
    /// Item id (0-based row in the vector dataset).
    pub id: u64,
    /// Cluster the item belongs to.
    pub cluster: u32,
    /// True for about 50% of items.
    pub flag_50: bool,
    /// True for about 10% of items.
    pub flag_10: bool,
    /// True for about 1% of items.
    pub flag_1: bool,
    /// True for about 0.1% of items.
    pub flag_01: bool,
    /// Days since 1990-01-01, in `[0, 13149)` (36 years).
    pub date: u32,
    /// Channel index.
    pub channel: u32,
    /// Rights status.
    pub rights: Rights,
    /// Speaker id.
    pub speaker: u32,
}

/// Rights status of an item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum Rights {
    /// Cleared for online broadcast.
    Cleared,
    /// Restricted.
    Restricted,
    /// Pending review.
    Pending,
}

impl Rights {
    /// Name as stored in the `rights` enum field.
    pub fn as_str(self) -> &'static str {
        match self {
            Rights::Cleared => "cleared",
            Rights::Restricted => "restricted",
            Rights::Pending => "pending",
        }
    }
}

/// Zipf-like sample in `0..n` with exponent 1: rank `k` has probability proportional to `1/(k+1)`.
fn zipf(rng: &mut SeededRng, n: u32) -> u32 {
    // Inverse CDF of the continuous approximation P(k) ∝ ln((k+2)/(k+1)) ≈ 1/(k+1):
    // u uniform in [0, ln(n+1)) gives k = floor(exp(u)) - 1 in [0, n).
    let u = rng.unit_f64() * f64::from(n + 1).ln();
    let k = (u.exp().floor() - 1.0).clamp(0.0, f64::from(n - 1));
    k as u32
}

/// Deterministic per-item generator.
pub struct Generator {
    cfg: GenConfig,
    /// Clusters that pass each controlled flag, in clustered mode: `(threshold per flag)`.
    cluster_thresholds: [u64; 4],
}

const FRACTIONS: [f64; 4] = [0.5, 0.1, 0.01, 0.001];

impl Generator {
    /// Creates a generator.
    pub fn new(cfg: GenConfig) -> Self {
        let cluster_thresholds = FRACTIONS.map(|f| (f * u64::MAX as f64) as u64);
        Generator {
            cfg,
            cluster_thresholds,
        }
    }

    /// Settings.
    pub fn config(&self) -> &GenConfig {
        &self.cfg
    }

    fn item_rng(&self, id: u64, purpose: &str) -> SeededRng {
        SeededRng::from_seed(xxh3_64(
            &[self.cfg.seed.to_le_bytes(), id.to_le_bytes()].concat(),
        ))
        .fork(purpose)
    }

    fn flag(&self, id: u64, cluster: u32, k: usize) -> bool {
        match self.cfg.correlation {
            Correlation::Random => {
                let h = xxh3_64(
                    &[
                        self.cfg.seed.to_le_bytes().as_slice(),
                        &id.to_le_bytes(),
                        &[k as u8],
                    ]
                    .concat(),
                );
                h < self.cluster_thresholds[k]
            }
            Correlation::Clustered => {
                let h = xxh3_64(
                    &[
                        self.cfg.seed.to_le_bytes().as_slice(),
                        &cluster.to_le_bytes(),
                        &[k as u8],
                    ]
                    .concat(),
                );
                h < self.cluster_thresholds[k]
            }
        }
    }

    /// Attributes of item `id`, with an optional externally supplied cluster label (for example
    /// from k-means over the real vectors). Without one, a pseudo-random cluster is assigned.
    pub fn attributes(&self, id: u64, cluster: Option<u32>) -> Attributes {
        let cluster = cluster.unwrap_or_else(|| {
            (xxh3_64(
                &[
                    self.cfg.seed.to_le_bytes(),
                    id.to_le_bytes(),
                    0xC1u64.to_le_bytes(),
                ]
                .concat(),
            ) % u64::from(self.cfg.clusters)) as u32
        });
        let mut r = self.item_rng(id, "attrs");
        let rights = {
            let u = r.unit_f64();
            if u < self.cfg.cleared_fraction {
                Rights::Cleared
            } else if u < self.cfg.cleared_fraction + (1.0 - self.cfg.cleared_fraction) * 0.7 {
                Rights::Restricted
            } else {
                Rights::Pending
            }
        };
        Attributes {
            id,
            cluster,
            flag_50: self.flag(id, cluster, 0),
            flag_10: self.flag(id, cluster, 1),
            flag_1: self.flag(id, cluster, 2),
            flag_01: self.flag(id, cluster, 3),
            date: r.below(13149) as u32,
            channel: zipf(&mut r, self.cfg.channels),
            rights,
            speaker: zipf(&mut r, self.cfg.speakers),
        }
    }

    /// Attributes for every item, in id order.
    pub fn all(&self, clusters: Option<&[u32]>) -> Vec<Attributes> {
        (0..self.cfg.n)
            .map(|id| self.attributes(id, clusters.map(|c| c[id as usize])))
            .collect()
    }
}

/// One scheduled deletion.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Takedown {
    /// Milliseconds after the start of the workload.
    pub at_ms: u64,
    /// Item to delete.
    pub id: u64,
}

/// Deletion schedule: `percent_per_minute` of `n` items per minute for `minutes`, spread
/// uniformly in time, each item at most once.
pub fn takedown_schedule(
    seed: u64,
    n: u64,
    percent_per_minute: f64,
    minutes: u64,
) -> Vec<Takedown> {
    let per_minute = ((n as f64) * percent_per_minute / 100.0).round() as u64;
    let total = (per_minute * minutes).min(n);
    let mut rng = SeededRng::from_seed(seed).fork("takedowns");
    // Partial Fisher-Yates over a virtual permutation: sample `total` distinct ids.
    let mut chosen = cairn_core::HashSet::default();
    let mut out = Vec::with_capacity(total as usize);
    while (out.len() as u64) < total {
        let id = rng.below(n);
        if chosen.insert(id) {
            out.push(id);
        }
    }
    let span_ms = minutes * 60_000;
    out.into_iter()
        .enumerate()
        .map(|(k, id)| Takedown {
            at_ms: (k as u64 * span_ms).checked_div(total).unwrap_or(0),
            id,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fraction(attrs: &[Attributes], f: impl Fn(&Attributes) -> bool) -> f64 {
        attrs.iter().filter(|a| f(a)).count() as f64 / attrs.len() as f64
    }

    #[test]
    fn random_flags_hit_their_fractions() {
        let g = Generator::new(GenConfig {
            n: 200_000,
            ..GenConfig::default()
        });
        let a = g.all(None);
        assert!((fraction(&a, |x| x.flag_50) - 0.5).abs() < 0.01);
        assert!((fraction(&a, |x| x.flag_10) - 0.1).abs() < 0.005);
        assert!((fraction(&a, |x| x.flag_1) - 0.01).abs() < 0.002);
        assert!((fraction(&a, |x| x.flag_01) - 0.001).abs() < 0.0005);
        assert!((fraction(&a, |x| x.rights == Rights::Cleared) - 0.4).abs() < 0.01);
        assert!(
            a.iter()
                .all(|x| x.date < 13149 && x.channel < 12 && x.speaker < 5000)
        );
    }

    #[test]
    fn clustered_flags_are_constant_within_a_cluster_and_roughly_right_overall() {
        let g = Generator::new(GenConfig {
            n: 200_000,
            correlation: Correlation::Clustered,
            clusters: 2000,
            ..GenConfig::default()
        });
        let a = g.all(None);
        let mut by_cluster: cairn_core::HashMap<u32, (bool, bool)> = cairn_core::HashMap::default();
        for x in &a {
            let e = by_cluster.entry(x.cluster).or_insert((x.flag_10, x.flag_1));
            assert_eq!(*e, (x.flag_10, x.flag_1), "flags must be per cluster");
        }
        assert!((fraction(&a, |x| x.flag_50) - 0.5).abs() < 0.05);
        assert!((fraction(&a, |x| x.flag_10) - 0.1).abs() < 0.03);
    }

    #[test]
    fn deterministic_and_seed_sensitive() {
        let a = Generator::new(GenConfig {
            n: 1000,
            ..GenConfig::default()
        })
        .all(None);
        let b = Generator::new(GenConfig {
            n: 1000,
            ..GenConfig::default()
        })
        .all(None);
        let c = Generator::new(GenConfig {
            n: 1000,
            seed: 2,
            ..GenConfig::default()
        })
        .all(None);
        assert_eq!(a, b);
        assert_ne!(a, c);
        let t1 = takedown_schedule(1, 10_000, 1.0, 5);
        let t2 = takedown_schedule(1, 10_000, 1.0, 5);
        assert_eq!(t1, t2);
        assert_eq!(t1.len(), 500);
        assert!(t1.windows(2).all(|w| w[0].at_ms <= w[1].at_ms));
        let ids: cairn_core::HashSet<u64> = t1.iter().map(|t| t.id).collect();
        assert_eq!(ids.len(), 500);
        assert!(t1.last().unwrap().at_ms < 5 * 60_000);
    }

    #[test]
    fn zipf_is_skewed() {
        let mut r = SeededRng::from_seed(3);
        let mut counts = [0u32; 12];
        for _ in 0..100_000 {
            counts[zipf(&mut r, 12) as usize] += 1;
        }
        assert!(counts[0] > counts[1] && counts[1] > counts[5] && counts[5] > counts[11]);
    }
}
