//! Loom model of the cross-thread queue: a push concurrent with a poll never loses the item or
//! the wake-up. Run with `RUSTFLAGS="--cfg loom" cargo test -p cairn-runtime --test loom_cross --release`.
#![cfg(loom)]

use cairn_runtime::CrossQueue;
use loom::sync::Arc;
use loom::sync::atomic::{AtomicBool, Ordering};
use loom::thread;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

fn flag_waker(flag: Arc<AtomicBool>) -> Waker {
    unsafe fn clone(p: *const ()) -> RawWaker {
        let a = unsafe { Arc::from_raw(p as *const AtomicBool) };
        let b = a.clone();
        std::mem::forget(a);
        RawWaker::new(Arc::into_raw(b) as *const (), &VTABLE)
    }
    unsafe fn wake(p: *const ()) {
        let a = unsafe { Arc::from_raw(p as *const AtomicBool) };
        a.store(true, Ordering::SeqCst);
    }
    unsafe fn wake_by_ref(p: *const ()) {
        let a = unsafe { &*(p as *const AtomicBool) };
        a.store(true, Ordering::SeqCst);
    }
    unsafe fn drop(p: *const ()) {
        drop(unsafe { Arc::from_raw(p as *const AtomicBool) });
    }
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);
    // SAFETY: the vtable functions uphold the RawWaker contract for an Arc<AtomicBool>.
    unsafe { Waker::from_raw(RawWaker::new(Arc::into_raw(flag) as *const (), &VTABLE)) }
}

#[test]
fn push_and_poll_never_lose_a_wakeup() {
    loom::model(|| {
        let q: CrossQueue<u32> = CrossQueue::new();
        let producer = q.clone();
        let t = thread::spawn(move || {
            assert!(producer.push(7));
        });
        let woken = Arc::new(AtomicBool::new(false));
        let waker = flag_waker(woken.clone());
        let mut cx = Context::from_waker(&waker);
        let first = q.poll_pop(&mut cx);
        t.join().unwrap();
        match first {
            Poll::Ready(Some(7)) => {}
            Poll::Ready(other) => panic!("unexpected {other:?}"),
            Poll::Pending => {
                // The push must have woken us and the item must be there now.
                assert!(woken.load(Ordering::SeqCst), "wake-up lost");
                assert_eq!(q.poll_pop(&mut cx), Poll::Ready(Some(7)));
            }
        }
    });
}
