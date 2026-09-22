//! Single-threaded async primitives: a one-shot channel and a wakeable queue.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

struct OneshotInner<T> {
    value: Option<T>,
    waker: Option<Waker>,
    closed: bool,
}

/// Sending half of a one-shot channel.
pub struct Sender<T>(Rc<RefCell<OneshotInner<T>>>);

/// Receiving half of a one-shot channel.
pub struct Receiver<T>(Rc<RefCell<OneshotInner<T>>>);

/// Creates a one-shot channel.
pub fn oneshot<T>() -> (Sender<T>, Receiver<T>) {
    let inner = Rc::new(RefCell::new(OneshotInner {
        value: None,
        waker: None,
        closed: false,
    }));
    (Sender(inner.clone()), Receiver(inner))
}

impl<T> Sender<T> {
    /// Delivers the value.
    pub fn send(self, v: T) {
        let mut i = self.0.borrow_mut();
        i.value = Some(v);
        if let Some(w) = i.waker.take() {
            w.wake();
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let mut i = self.0.borrow_mut();
        i.closed = true;
        if let Some(w) = i.waker.take() {
            w.wake();
        }
    }
}

impl<T> Future for Receiver<T> {
    /// `None` if the sender was dropped without sending.
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let mut i = self.0.borrow_mut();
        if let Some(v) = i.value.take() {
            return Poll::Ready(Some(v));
        }
        if i.closed {
            return Poll::Ready(None);
        }
        i.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

struct QueueInner<T> {
    items: VecDeque<T>,
    waker: Option<Waker>,
}

/// Unbounded FIFO with an async `pop`, for actor inboxes.
pub struct LocalQueue<T>(Rc<RefCell<QueueInner<T>>>);

impl<T> Clone for LocalQueue<T> {
    fn clone(&self) -> Self {
        LocalQueue(self.0.clone())
    }
}

impl<T> Default for LocalQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> LocalQueue<T> {
    /// Empty queue.
    pub fn new() -> Self {
        LocalQueue(Rc::new(RefCell::new(QueueInner {
            items: VecDeque::new(),
            waker: None,
        })))
    }

    /// Enqueues an item and wakes the consumer.
    pub fn push(&self, item: T) {
        let mut q = self.0.borrow_mut();
        q.items.push_back(item);
        if let Some(w) = q.waker.take() {
            w.wake();
        }
    }

    /// Dequeues the next item, waiting if empty. Cancel-safe.
    pub fn pop(&self) -> impl Future<Output = T> + '_ {
        std::future::poll_fn(move |cx| {
            let mut q = self.0.borrow_mut();
            match q.items.pop_front() {
                Some(v) => Poll::Ready(v),
                None => {
                    q.waker = Some(cx.waker().clone());
                    Poll::Pending
                }
            }
        })
    }

    /// Dequeues without waiting.
    pub fn try_pop(&self) -> Option<T> {
        self.0.borrow_mut().items.pop_front()
    }

    /// Items waiting.
    pub fn len(&self) -> usize {
        self.0.borrow().items.len()
    }

    /// Whether nothing is waiting.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
