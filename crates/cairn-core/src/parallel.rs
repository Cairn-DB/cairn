//! Data parallelism for CPU-bound builds, injected like the rest of the environment (ADR 0011,
//! ADR 0019).
//!
//! Engine code never starts threads. A build that can use several cores takes a
//! [`Parallel`] and calls [`Parallel::run`]; the real runtime implements it with a thread pool,
//! the simulator and tests with [`Sequential`]. Algorithms using it must produce the same
//! result whatever the thread count, so builds stay deterministic and replicas stay identical.

/// Runs independent tasks, possibly on several threads.
pub trait Parallel: Send + Sync + std::fmt::Debug {
    /// Upper bound on concurrent workers (worker indexes are `0..threads()`).
    fn threads(&self) -> usize;

    /// Calls `f(worker, i)` once for every `i` in `0..n` and returns when all calls are done.
    /// Calls with the same `worker` never overlap, so per-worker scratch needs no locking
    /// beyond an uncontended mutex.
    fn run(&self, n: usize, f: &(dyn Fn(usize, usize) + Sync));
}

/// Runs every task on the calling thread, in order.
#[derive(Debug, Clone, Copy, Default)]
pub struct Sequential;

impl Parallel for Sequential {
    fn threads(&self) -> usize {
        1
    }

    fn run(&self, n: usize, f: &(dyn Fn(usize, usize) + Sync)) {
        for i in 0..n {
            f(0, i);
        }
    }
}
