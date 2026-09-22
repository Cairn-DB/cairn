//! Run fingerprint: every observable event is hashed in order.

use cairn_core::Instant;
use std::collections::VecDeque;
use xxhash_rust::xxh3::Xxh3;

/// Ordered digest of a simulation's events, plus a short ring of recent events for diagnostics.
pub struct Trace {
    hasher: Xxh3,
    count: u64,
    recent: VecDeque<String>,
    keep: usize,
}

impl Trace {
    /// Creates a trace keeping the last `keep` events in clear text.
    pub fn new(keep: usize) -> Self {
        Trace {
            hasher: Xxh3::new(),
            count: 0,
            recent: VecDeque::new(),
            keep,
        }
    }

    /// Records an event.
    pub fn record(&mut self, at: Instant, kind: &str, detail: &str) {
        self.hasher.update(&at.as_nanos().to_le_bytes());
        self.hasher.update(kind.as_bytes());
        self.hasher.update(b"\0");
        self.hasher.update(detail.as_bytes());
        self.hasher.update(b"\n");
        self.count += 1;
        if self.keep > 0 {
            if self.recent.len() == self.keep {
                self.recent.pop_front();
            }
            self.recent.push_back(format!("{at:?} {kind} {detail}"));
        }
    }

    /// Fingerprint of everything recorded so far.
    pub fn digest(&self) -> u64 {
        self.hasher.digest()
    }

    /// Number of events recorded.
    pub fn len(&self) -> u64 {
        self.count
    }

    /// Whether nothing was recorded.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// The most recent events, oldest first.
    pub fn recent(&self) -> impl Iterator<Item = &str> {
        self.recent.iter().map(String::as_str)
    }
}
