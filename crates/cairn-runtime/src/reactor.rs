//! The reactor: what the executor calls when every task is blocked.

use cairn_core::Instant;

/// Source of time and of I/O completions for an [`crate::Executor`].
///
/// The executor calls [`Reactor::park`] only when no task is runnable. The reactor must either
/// advance time to `deadline` (the earliest timer) or complete some outstanding operation that
/// wakes a task, and return the new current time. It must not return a time earlier than `now`.
pub trait Reactor {
    /// Block (real time) or advance (virtual time) until something can make progress.
    fn park(&mut self, now: Instant, deadline: Option<Instant>) -> Instant;

    /// Whether an outstanding operation exists that will eventually wake a task. When this is
    /// `false` and no timer is pending, blocked tasks are stalled forever.
    fn has_pending(&self) -> bool;

    /// A hook the executor calls from `Waker::wake` (possibly on another thread) so a wake-up
    /// interrupts [`Reactor::park`]. Reactors that block in real time must provide one.
    fn unpark_hook(&self) -> Option<std::sync::Arc<dyn Fn() + Send + Sync>> {
        None
    }
}
