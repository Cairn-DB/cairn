//! Single-threaded task executor.
//!
//! Tasks are `!Send` boxed futures stored in a slab. Wakers push task ids on a shared ready
//! queue; timers live in an ordered map keyed by `(deadline, sequence)` so that equal deadlines
//! fire in registration order. Scheduling order is FIFO unless a seeded generator is installed,
//! in which case the next task is chosen from the ready set by that generator, which is how the
//! simulator explores interleavings reproducibly.

use crate::reactor::Reactor;
use cairn_core::{Duration, Instant, SeededRng};
use slab::Slab;
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

type BoxFuture = Pin<Box<dyn Future<Output = ()>>>;

/// Ready queue shared with wakers. Wakers may be `Send` by contract, so this lives behind a
/// (never contended) mutex.
#[derive(Default)]
struct ReadyQueue {
    queue: Mutex<VecDeque<usize>>,
}

impl ReadyQueue {
    fn push(&self, id: usize) {
        self.queue
            .lock()
            .expect("ready queue poisoned")
            .push_back(id);
    }

    fn take_all(&self, into: &mut VecDeque<usize>) {
        let mut q = self.queue.lock().expect("ready queue poisoned");
        into.append(&mut q);
    }
}

struct TaskWaker {
    id: usize,
    queued: AtomicBool,
    ready: Arc<ReadyQueue>,
}

impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if !self.queued.swap(true, Ordering::AcqRel) {
            self.ready.push(self.id);
        }
    }
}

struct Shared {
    now: Cell<Instant>,
    ready: Arc<ReadyQueue>,
    new_tasks: RefCell<Vec<(u32, BoxFuture)>>,
    timers: RefCell<BTreeMap<(Instant, u64), Waker>>,
    timer_seq: Cell<u64>,
    spawned_total: Cell<u64>,
}

/// Cheap handle to an executor, used by `Runtime` implementations to spawn, sleep and read time.
#[derive(Clone)]
pub struct Handle(Rc<Shared>);

impl Handle {
    /// Current time as last set by the reactor.
    pub fn now(&self) -> Instant {
        self.0.now.get()
    }

    /// Queues `future` as a new task with tag 0; it starts on the next scheduling step.
    pub fn spawn(&self, future: impl Future<Output = ()> + 'static) {
        self.spawn_tagged(0, future);
    }

    /// Queues `future` as a new task carrying `tag` (the simulator tags tasks by node so a
    /// crash can cancel them with [`Executor::cancel_tagged`]).
    pub fn spawn_tagged(&self, tag: u32, future: impl Future<Output = ()> + 'static) {
        self.0.new_tasks.borrow_mut().push((tag, Box::pin(future)));
        self.0.spawned_total.set(self.0.spawned_total.get() + 1);
    }

    /// Future that completes once `now() >= deadline`.
    pub fn sleep_until(&self, deadline: Instant) -> Sleep {
        Sleep {
            shared: self.0.clone(),
            deadline,
            key: None,
        }
    }

    /// Future that completes after `d`.
    pub fn sleep(&self, d: Duration) -> Sleep {
        self.sleep_until(self.now() + d)
    }

    /// Future that yields once to other tasks.
    pub fn yield_now(&self) -> YieldNow {
        YieldNow { yielded: false }
    }

    /// Earliest pending timer deadline, if any.
    pub fn next_timer(&self) -> Option<Instant> {
        self.0.timers.borrow().keys().next().map(|(t, _)| *t)
    }

    /// Number of tasks ever spawned (diagnostics).
    pub fn spawned_total(&self) -> u64 {
        self.0.spawned_total.get()
    }
}

/// Timer future returned by [`Handle::sleep_until`].
pub struct Sleep {
    shared: Rc<Shared>,
    deadline: Instant,
    key: Option<(Instant, u64)>,
}

impl Future for Sleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.shared.now.get() >= self.deadline {
            if let Some(key) = self.key.take() {
                self.shared.timers.borrow_mut().remove(&key);
            }
            return Poll::Ready(());
        }
        let mut timers = self.shared.timers.borrow_mut();
        match self.key {
            Some(key) => {
                // Re-register the (possibly new) waker.
                timers.insert(key, cx.waker().clone());
            }
            None => {
                let seq = self.shared.timer_seq.get();
                self.shared.timer_seq.set(seq + 1);
                let key = (self.deadline, seq);
                timers.insert(key, cx.waker().clone());
                drop(timers);
                self.key = Some(key);
            }
        }
        Poll::Pending
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            self.shared.timers.borrow_mut().remove(&key);
        }
    }
}

/// Future returned by [`Handle::yield_now`].
pub struct YieldNow {
    yielded: bool,
}

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.yielded {
            Poll::Ready(())
        } else {
            self.yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

/// Why [`Executor::run`] returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunOutcome {
    /// Every task completed.
    Finished,
    /// Tasks remain but none can ever be woken: no timer, no pending I/O.
    Stalled,
}

struct TaskSlot {
    tag: u32,
    future: BoxFuture,
    waker: Arc<TaskWaker>,
}

/// The executor. One per core.
pub struct Executor<R: Reactor> {
    shared: Rc<Shared>,
    tasks: Slab<TaskSlot>,
    local_ready: VecDeque<usize>,
    reactor: R,
    scheduler_rng: Option<SeededRng>,
    polls: u64,
}

impl<R: Reactor> Executor<R> {
    /// Creates an executor starting at `Instant::ZERO`.
    pub fn new(reactor: R) -> Self {
        Executor {
            shared: Rc::new(Shared {
                now: Cell::new(Instant::ZERO),
                ready: Arc::new(ReadyQueue::default()),
                new_tasks: RefCell::new(Vec::new()),
                timers: RefCell::new(BTreeMap::new()),
                timer_seq: Cell::new(0),
                spawned_total: Cell::new(0),
            }),
            tasks: Slab::new(),
            local_ready: VecDeque::new(),
            reactor,
            scheduler_rng: None,
            polls: 0,
        }
    }

    /// Installs a seeded generator that picks the next runnable task; without one, scheduling is
    /// FIFO.
    pub fn set_scheduler_rng(&mut self, rng: SeededRng) {
        self.scheduler_rng = Some(rng);
    }

    /// Sets the current time (reactors and tests). Time never goes backwards.
    pub fn set_now(&mut self, now: Instant) {
        assert!(now >= self.shared.now.get(), "time went backwards");
        self.shared.now.set(now);
    }

    /// A handle for spawning and timers.
    pub fn handle(&self) -> Handle {
        Handle(self.shared.clone())
    }

    /// The reactor.
    pub fn reactor(&mut self) -> &mut R {
        &mut self.reactor
    }

    /// Number of live tasks.
    pub fn task_count(&self) -> usize {
        self.tasks.len() + self.shared.new_tasks.borrow().len()
    }

    /// Drops every task (queued or running) carrying `tag`, without polling them again. Their
    /// timers are unregistered when the futures drop.
    pub fn cancel_tagged(&mut self, tag: u32) -> usize {
        self.admit_new_tasks();
        let ids: Vec<usize> = self
            .tasks
            .iter()
            .filter(|(_, t)| t.tag == tag)
            .map(|(id, _)| id)
            .collect();
        for id in &ids {
            self.tasks.remove(*id);
        }
        ids.len()
    }

    /// Total number of task polls so far (diagnostics and trace fingerprints).
    pub fn polls(&self) -> u64 {
        self.polls
    }

    fn admit_new_tasks(&mut self) {
        let new = std::mem::take(&mut *self.shared.new_tasks.borrow_mut());
        for (tag, future) in new {
            let entry = self.tasks.vacant_entry();
            let id = entry.key();
            let waker = Arc::new(TaskWaker {
                id,
                queued: AtomicBool::new(true),
                ready: self.shared.ready.clone(),
            });
            entry.insert(TaskSlot { tag, future, waker });
            self.local_ready.push_back(id);
        }
    }

    fn fire_due_timers(&mut self) {
        let now = self.shared.now.get();
        let mut timers = self.shared.timers.borrow_mut();
        while let Some(entry) = timers.first_entry() {
            if entry.key().0 > now {
                break;
            }
            entry.remove().wake();
        }
    }

    fn pick_ready(&mut self) -> Option<usize> {
        self.shared.ready.take_all(&mut self.local_ready);
        if self.local_ready.is_empty() {
            return None;
        }
        match &mut self.scheduler_rng {
            None => self.local_ready.pop_front(),
            Some(rng) => {
                let idx = rng.below(self.local_ready.len() as u64) as usize;
                self.local_ready.remove(idx)
            }
        }
    }

    /// Runs one runnable task once. Returns `false` if nothing was runnable.
    fn poll_one(&mut self) -> bool {
        let Some(id) = self.pick_ready() else {
            return false;
        };
        let Some(slot) = self.tasks.get_mut(id) else {
            return true; // stale wake for a finished task
        };
        slot.waker.queued.store(false, Ordering::Release);
        let waker = Waker::from(slot.waker.clone());
        let mut cx = Context::from_waker(&waker);
        self.polls += 1;
        if slot.future.as_mut().poll(&mut cx).is_ready() {
            self.tasks.remove(id);
        }
        true
    }

    /// Runs until every task has completed or the tasks are stalled.
    pub fn run(&mut self) -> RunOutcome {
        loop {
            self.admit_new_tasks();
            self.fire_due_timers();
            if self.poll_one() {
                continue;
            }
            if self.tasks.is_empty() && self.shared.new_tasks.borrow().is_empty() {
                return RunOutcome::Finished;
            }
            let deadline = self.handle().next_timer();
            if deadline.is_none() && !self.reactor.has_pending() {
                return RunOutcome::Stalled;
            }
            let now = self.shared.now.get();
            let new_now = self.reactor.park(now, deadline);
            assert!(new_now >= now, "reactor moved time backwards");
            self.shared.now.set(new_now);
        }
    }

    /// Runs `future` to completion alongside existing tasks.
    ///
    /// # Panics
    /// If the executor stalls before `future` completes.
    pub fn block_on<T: 'static>(&mut self, future: impl Future<Output = T> + 'static) -> T {
        let slot: Rc<RefCell<Option<T>>> = Rc::new(RefCell::new(None));
        let out = slot.clone();
        self.handle().spawn(async move {
            let v = future.await;
            *out.borrow_mut() = Some(v);
        });
        loop {
            self.admit_new_tasks();
            self.fire_due_timers();
            if let Some(v) = slot.borrow_mut().take() {
                return v;
            }
            if self.poll_one() {
                continue;
            }
            let deadline = self.handle().next_timer();
            if deadline.is_none() && !self.reactor.has_pending() {
                panic!("block_on: executor stalled with {} tasks", self.tasks.len());
            }
            let now = self.shared.now.get();
            let new_now = self.reactor.park(now, deadline);
            assert!(new_now >= now, "reactor moved time backwards");
            self.shared.now.set(new_now);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    /// Reactor with virtual time and no I/O: parking jumps straight to the deadline.
    struct VirtualReactor;

    impl Reactor for VirtualReactor {
        fn park(&mut self, now: Instant, deadline: Option<Instant>) -> Instant {
            deadline.unwrap_or(now)
        }
        fn has_pending(&self) -> bool {
            false
        }
    }

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn timers_fire_in_deadline_order_with_virtual_time() {
        let mut ex = Executor::new(VirtualReactor);
        let h = ex.handle();
        let log = Rc::new(RefCell::new(Vec::new()));
        for (name, delay) in [("c", 30u64), ("a", 10), ("b", 20), ("a2", 10)] {
            let h2 = h.clone();
            let log = log.clone();
            h.spawn(async move {
                h2.sleep(ms(delay)).await;
                log.borrow_mut().push((name, h2.now()));
            });
        }
        assert_eq!(ex.run(), RunOutcome::Finished);
        let got: Vec<_> = log.borrow().iter().map(|(n, _)| *n).collect();
        assert_eq!(got, vec!["a", "a2", "b", "c"]);
        assert_eq!(log.borrow()[3].1, Instant::ZERO + ms(30));
    }

    #[test]
    fn yield_now_interleaves_tasks() {
        let mut ex = Executor::new(VirtualReactor);
        let h = ex.handle();
        let log = Rc::new(RefCell::new(Vec::new()));
        for name in ["x", "y"] {
            let h2 = h.clone();
            let log = log.clone();
            h.spawn(async move {
                for i in 0..3 {
                    log.borrow_mut().push(format!("{name}{i}"));
                    h2.yield_now().await;
                }
            });
        }
        ex.run();
        assert_eq!(log.borrow().join(","), "x0,y0,x1,y1,x2,y2");
    }

    #[test]
    fn block_on_returns_value_and_runs_spawned_tasks() {
        let mut ex = Executor::new(VirtualReactor);
        let h = ex.handle();
        let v = ex.block_on(async move {
            let done = Rc::new(RefCell::new(false));
            let d = done.clone();
            let h2 = h.clone();
            h.spawn(async move {
                h2.sleep(ms(5)).await;
                *d.borrow_mut() = true;
            });
            h.sleep(ms(10)).await;
            *done.borrow()
        });
        assert!(v);
    }

    #[test]
    fn stalled_when_waiting_on_nothing() {
        let mut ex = Executor::new(VirtualReactor);
        ex.handle().spawn(std::future::pending());
        assert_eq!(ex.run(), RunOutcome::Stalled);
    }

    #[test]
    fn dropped_sleep_unregisters_timer() {
        let ex = Executor::new(VirtualReactor);
        let h = ex.handle();
        let s = h.sleep(ms(100));
        drop(s);
        assert!(h.next_timer().is_none());
    }

    #[test]
    fn cancel_tagged_drops_tasks_and_their_timers() {
        let mut ex = Executor::new(VirtualReactor);
        let h = ex.handle();
        let h2 = h.clone();
        h.spawn_tagged(7, async move { h2.sleep(ms(50)).await });
        let h3 = h.clone();
        h.spawn_tagged(8, async move { h3.sleep(ms(10)).await });
        ex.admit_new_tasks();
        assert!(ex.poll_one());
        assert!(ex.poll_one());
        assert_eq!(ex.cancel_tagged(7), 1);
        assert_eq!(ex.task_count(), 1);
        assert_eq!(h.next_timer(), Some(Instant::ZERO + ms(10)));
        assert_eq!(ex.run(), RunOutcome::Finished);
    }

    #[test]
    fn seeded_scheduler_is_reproducible() {
        fn run(seed: u64) -> Vec<u32> {
            let mut ex = Executor::new(VirtualReactor);
            ex.set_scheduler_rng(SeededRng::from_seed(seed));
            let h = ex.handle();
            let log = Rc::new(RefCell::new(Vec::new()));
            for i in 0..8u32 {
                let h2 = h.clone();
                let log = log.clone();
                h.spawn(async move {
                    for _ in 0..3 {
                        log.borrow_mut().push(i);
                        h2.yield_now().await;
                    }
                });
            }
            ex.run();
            let v = log.borrow().clone();
            drop(log);
            v
        }
        assert_eq!(run(1), run(1));
        assert_ne!(
            run(1),
            run(2),
            "different seeds should give different orders"
        );
    }
}
