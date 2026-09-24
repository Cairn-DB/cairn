//! Thread-backed [`Parallel`] for segment builds (ADR 0019).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use cairn_core::Parallel;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Runs tasks on up to `threads` scoped threads (the caller's thread is one of them). Workers
/// pull task indexes from a shared counter, so uneven tasks balance themselves.
#[derive(Debug, Clone, Copy)]
pub struct ThreadParallel {
    threads: usize,
}

impl ThreadParallel {
    /// Up to `threads` workers (at least one).
    pub fn new(threads: usize) -> Self {
        ThreadParallel {
            threads: threads.max(1),
        }
    }

    /// As many workers as the machine has hardware threads.
    pub fn available() -> Self {
        Self::new(std::thread::available_parallelism().map_or(1, |n| n.get()))
    }
}

impl Parallel for ThreadParallel {
    fn threads(&self) -> usize {
        self.threads
    }

    fn run(&self, n: usize, f: &(dyn Fn(usize, usize) + Sync)) {
        let workers = self.threads.min(n);
        if workers <= 1 {
            for i in 0..n {
                f(0, i);
            }
            return;
        }
        let next = AtomicUsize::new(0);
        let work = |w: usize| {
            loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i >= n {
                    break;
                }
                f(w, i);
            }
        };
        std::thread::scope(|s| {
            for w in 1..workers {
                let work = &work;
                s.spawn(move || work(w));
            }
            work(0);
        });
    }
}
