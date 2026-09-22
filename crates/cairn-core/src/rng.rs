//! Seeded, forkable randomness.
//!
//! The only entropy source in the system is the seed given to the root generator by the runtime
//! (the OS in production, the run seed in simulation). Every component derives its own stream
//! with [`SeededRng::fork`], so adding a consumer never perturbs another consumer's stream.

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

/// A deterministic random number generator (ChaCha8).
#[derive(Clone, Debug)]
pub struct SeededRng(ChaCha8Rng);

impl SeededRng {
    /// Creates a generator from a 64-bit seed.
    pub fn from_seed(seed: u64) -> Self {
        SeededRng(ChaCha8Rng::seed_from_u64(seed))
    }

    /// Derives an independent generator for `purpose`. The child depends on the parent's seed
    /// and on `purpose` only, never on how much the parent has been used.
    pub fn fork(&self, purpose: &str) -> SeededRng {
        let mut bytes = self.0.get_seed().to_vec();
        bytes.extend_from_slice(purpose.as_bytes());
        SeededRng::from_seed(crate::hash::xxh3_64(&bytes))
    }

    /// Next 64 random bits.
    pub fn next_u64(&mut self) -> u64 {
        self.0.next_u64()
    }

    /// Uniform value in `0..n` (`n` must be non-zero).
    pub fn below(&mut self, n: u64) -> u64 {
        assert!(n > 0, "below(0)");
        // Rejection sampling keeps the distribution exactly uniform.
        let zone = u64::MAX - (u64::MAX % n);
        loop {
            let v = self.next_u64();
            if v < zone {
                return v % n;
            }
        }
    }

    /// Uniform `f64` in `[0, 1)`.
    pub fn unit_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// `true` with probability `p`.
    pub fn chance(&mut self, p: f64) -> bool {
        self.unit_f64() < p
    }

    /// Access to the underlying `rand` generator for adaptors that need the trait.
    pub fn inner(&mut self) -> &mut ChaCha8Rng {
        &mut self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_stream() {
        let mut a = SeededRng::from_seed(7);
        let mut b = SeededRng::from_seed(7);
        for _ in 0..100 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn fork_is_independent_of_parent_usage() {
        let mut a = SeededRng::from_seed(7);
        let b = SeededRng::from_seed(7);
        let _ = a.next_u64();
        assert_eq!(a.fork("x").next_u64(), b.fork("x").next_u64());
        assert_ne!(a.fork("x").next_u64(), a.fork("y").next_u64());
    }

    #[test]
    fn below_is_in_range() {
        let mut r = SeededRng::from_seed(1);
        for n in [1u64, 2, 3, 10, 1 << 40] {
            for _ in 0..50 {
                assert!(r.below(n) < n);
            }
        }
    }
}
