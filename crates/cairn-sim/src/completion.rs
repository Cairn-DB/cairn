//! A one-shot completion slot: an event fills it later and wakes the waiting task.

use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

struct Inner<T> {
    value: Option<T>,
    waker: Option<Waker>,
}

/// Future half of a completion.
pub struct Completion<T> {
    inner: Rc<RefCell<Inner<T>>>,
}

/// Filling half of a completion.
pub struct Completer<T> {
    inner: Rc<RefCell<Inner<T>>>,
}

/// Creates a linked pair.
pub fn completion<T>() -> (Completion<T>, Completer<T>) {
    let inner = Rc::new(RefCell::new(Inner {
        value: None,
        waker: None,
    }));
    (
        Completion {
            inner: inner.clone(),
        },
        Completer { inner },
    )
}

impl<T> Completer<T> {
    /// Stores the value and wakes the waiter.
    pub fn complete(self, value: T) {
        let mut inner = self.inner.borrow_mut();
        inner.value = Some(value);
        if let Some(w) = inner.waker.take() {
            w.wake();
        }
    }
}

impl<T> Future for Completion<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let mut inner = self.inner.borrow_mut();
        match inner.value.take() {
            Some(v) => Poll::Ready(v),
            None => {
                inner.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}
