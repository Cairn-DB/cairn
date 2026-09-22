//! Time types that carry no operating-system semantics.
//!
//! [`Instant`] is a monotonic nanosecond counter whose origin is the runtime's start. The
//! simulator advances it explicitly; the real runtime derives it from the OS clock once, inside
//! `cairn-runtime`. Engine code never asks the OS for time.

use std::fmt;
use std::ops::{Add, AddAssign, Sub};

/// Re-export of the standard duration type; it carries no OS semantics.
pub use core::time::Duration;

/// A monotonic point in time, in nanoseconds since the runtime started.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Instant(u64);

impl Instant {
    /// The runtime's origin.
    pub const ZERO: Instant = Instant(0);

    /// Builds an instant from nanoseconds since the origin.
    pub const fn from_nanos(nanos: u64) -> Self {
        Instant(nanos)
    }

    /// Nanoseconds since the origin.
    pub const fn as_nanos(self) -> u64 {
        self.0
    }

    /// Time elapsed between `earlier` and `self`; zero if `earlier` is later.
    pub fn duration_since(self, earlier: Instant) -> Duration {
        Duration::from_nanos(self.0.saturating_sub(earlier.0))
    }

    /// `self + d`, saturating at `u64::MAX` nanoseconds.
    pub fn checked_add(self, d: Duration) -> Option<Instant> {
        u64::try_from(d.as_nanos())
            .ok()
            .and_then(|n| self.0.checked_add(n))
            .map(Instant)
    }
}

impl Add<Duration> for Instant {
    type Output = Instant;
    fn add(self, d: Duration) -> Instant {
        self.checked_add(d).expect("Instant overflow")
    }
}

impl AddAssign<Duration> for Instant {
    fn add_assign(&mut self, d: Duration) {
        *self = *self + d;
    }
}

impl Sub<Instant> for Instant {
    type Output = Duration;
    fn sub(self, earlier: Instant) -> Duration {
        self.duration_since(earlier)
    }
}

impl fmt::Debug for Instant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "t+{:?}", Duration::from_nanos(self.0))
    }
}
