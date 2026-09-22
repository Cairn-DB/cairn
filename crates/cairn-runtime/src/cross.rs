//! Cross-thread queue: the only structure that carries data between cores.
//!
//! Producers on any thread push; the single consumer (a task on the owning core) pops
//! asynchronously. The consumer's waker is stored under the same mutex as the queue so a push
//! can never be missed. Verified with `loom` (`cargo test -p cairn-runtime --features loom`
//! is not needed: the loom test runs under `cfg(loom)`; see `tests/loom_cross.rs`).

#[cfg(loom)]
use loom::sync::{Arc, Mutex};
use std::collections::VecDeque;
use std::future::Future;
#[cfg(not(loom))]
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

struct Inner<T> {
    items: VecDeque<T>,
    waker: Option<Waker>,
    closed: bool,
}

/// Multi-producer, single-consumer queue usable across threads.
pub struct CrossQueue<T> {
    inner: Arc<Mutex<Inner<T>>>,
}

impl<T> Clone for CrossQueue<T> {
    fn clone(&self) -> Self {
        CrossQueue {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Default for CrossQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> CrossQueue<T> {
    /// Empty queue.
    pub fn new() -> Self {
        CrossQueue {
            inner: Arc::new(Mutex::new(Inner {
                items: VecDeque::new(),
                waker: None,
                closed: false,
            })),
        }
    }

    /// Pushes from any thread and wakes the consumer. Returns `false` if the queue is closed.
    pub fn push(&self, item: T) -> bool {
        let waker = {
            let mut g = self.inner.lock().expect("cross queue poisoned");
            if g.closed {
                return false;
            }
            g.items.push_back(item);
            g.waker.take()
        };
        if let Some(w) = waker {
            w.wake();
        }
        true
    }

    /// Closes the queue: pushes fail and `pop` returns `None` once drained.
    pub fn close(&self) {
        let waker = {
            let mut g = self.inner.lock().expect("cross queue poisoned");
            g.closed = true;
            g.waker.take()
        };
        if let Some(w) = waker {
            w.wake();
        }
    }

    /// Non-blocking pop.
    pub fn try_pop(&self) -> Option<T> {
        self.inner
            .lock()
            .expect("cross queue poisoned")
            .items
            .pop_front()
    }

    /// Polls for the next item, registering `cx`'s waker when empty.
    pub fn poll_pop(&self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let mut g = self.inner.lock().expect("cross queue poisoned");
        match g.items.pop_front() {
            Some(v) => Poll::Ready(Some(v)),
            None if g.closed => Poll::Ready(None),
            None => {
                g.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }

    /// Async pop (single consumer). Cancel-safe.
    #[cfg(not(loom))]
    pub fn pop(&self) -> impl Future<Output = Option<T>> + '_ {
        std::future::poll_fn(move |cx| self.poll_pop(cx))
    }

    /// Items waiting.
    pub fn len(&self) -> usize {
        self.inner.lock().expect("cross queue poisoned").items.len()
    }

    /// Whether empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

struct OneshotInner<T> {
    value: Option<T>,
    waker: Option<Waker>,
    closed: bool,
}

/// Sending half of a cross-thread one-shot.
pub struct CrossSender<T>(Arc<Mutex<OneshotInner<T>>>);

/// Receiving half of a cross-thread one-shot.
pub struct CrossReceiver<T>(Arc<Mutex<OneshotInner<T>>>);

/// Creates a one-shot channel usable across threads.
pub fn cross_oneshot<T>() -> (CrossSender<T>, CrossReceiver<T>) {
    let inner = Arc::new(Mutex::new(OneshotInner {
        value: None,
        waker: None,
        closed: false,
    }));
    (CrossSender(inner.clone()), CrossReceiver(inner))
}

impl<T> CrossSender<T> {
    /// Delivers the value and wakes the receiver.
    pub fn send(self, v: T) {
        let waker = {
            let mut g = self.0.lock().expect("oneshot poisoned");
            g.value = Some(v);
            g.waker.take()
        };
        if let Some(w) = waker {
            w.wake();
        }
    }
}

impl<T> Drop for CrossSender<T> {
    fn drop(&mut self) {
        let waker = {
            let mut g = self.0.lock().expect("oneshot poisoned");
            g.closed = true;
            g.waker.take()
        };
        if let Some(w) = waker {
            w.wake();
        }
    }
}

impl<T> CrossReceiver<T> {
    /// Polls for the value; `None` if the sender was dropped without sending.
    pub fn poll_recv(&self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let mut g = self.0.lock().expect("oneshot poisoned");
        if let Some(v) = g.value.take() {
            return Poll::Ready(Some(v));
        }
        if g.closed {
            return Poll::Ready(None);
        }
        g.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

#[cfg(not(loom))]
impl<T> Future for CrossReceiver<T> {
    type Output = Option<T>;
    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        self.poll_recv(cx)
    }
}
