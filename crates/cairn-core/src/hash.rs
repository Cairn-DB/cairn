//! Deterministic hash maps.
//!
//! `std`'s default `RandomState` makes iteration order differ between runs, which breaks
//! deterministic simulation. Engine crates use these aliases (the `std` types are banned by
//! `clippy.toml`, ADR 0011).

#![allow(clippy::disallowed_types)]

use rustc_hash::FxBuildHasher;

/// Hash map with a fixed-seed hasher; iteration order depends only on the inserted keys.
pub type HashMap<K, V> = std::collections::HashMap<K, V, FxBuildHasher>;

/// Hash set with a fixed-seed hasher.
pub type HashSet<K> = std::collections::HashSet<K, FxBuildHasher>;

/// Stable 64-bit hash of arbitrary bytes (xxh3), for seeds and content fingerprints.
pub fn xxh3_64(bytes: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(bytes)
}
