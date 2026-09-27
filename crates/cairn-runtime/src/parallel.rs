//! Thread-backed [`Parallel`] for segment builds (ADR 0019).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use cairn_core::Parallel;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Runs tasks on up to `threads` scoped threads (the caller's thread is one of them). Workers
/// pull task indexes from a shared counter, so uneven tasks balance themselves.
#[derive(Debug, Clone, Copy)]
pub struct ThreadParallel {
    threads: usize,
    /// Nice value of the workers, or `None` to run at the caller's priority.
    nice: Option<i32>,
}

impl ThreadParallel {
    /// Up to `threads` workers (at least one).
    pub fn new(threads: usize) -> Self {
        ThreadParallel {
            threads: threads.max(1),
            nice: None,
        }
    }

    /// Up to `threads` workers at a low priority (nice `nice`, Linux only): they take the CPU
    /// that serving leaves idle and give it back at once when serving needs it. The caller's
    /// thread then only waits, since a thread cannot raise its priority back without privilege.
    pub fn background(threads: usize, nice: i32) -> Self {
        ThreadParallel {
            threads: threads.max(1),
            nice: Some(nice),
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
        if let Some(nice) = self.nice {
            let next = AtomicUsize::new(0);
            let work = |w: usize| {
                lower_priority(nice);
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= n {
                        break;
                    }
                    f(w, i);
                }
            };
            std::thread::scope(|s| {
                for w in 0..workers.max(1) {
                    let work = &work;
                    std::thread::Builder::new()
                        .name("build-w".into())
                        .spawn_scoped(s, move || work(w))
                        .expect("spawn build worker");
                }
            });
            return;
        }
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
                std::thread::Builder::new()
                    .name("build-w".into())
                    .spawn_scoped(s, move || work(w))
                    .expect("spawn build worker");
            }
            work(0);
        });
    }
}

/// Sets the calling thread's nice value (Linux: priorities are per thread). Best effort: a
/// failure only leaves the thread at its current priority.
fn lower_priority(nice: i32) {
    #[cfg(target_os = "linux")]
    // SAFETY: plain syscalls on the calling thread, no memory is passed.
    unsafe {
        let tid = libc::syscall(libc::SYS_gettid) as libc::id_t;
        libc::setpriority(libc::PRIO_PROCESS, tid, nice);
    }
    #[cfg(not(target_os = "linux"))]
    let _ = nice;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[test]
    fn background_workers_run_every_task_once_at_low_priority() {
        let seen = Mutex::new(vec![0u32; 100]);
        let nice = Mutex::new(Vec::new());
        ThreadParallel::background(4, 10).run(100, &|_, i| {
            seen.lock().unwrap()[i] += 1;
            #[cfg(target_os = "linux")]
            // SAFETY: plain syscalls on the calling thread.
            unsafe {
                let tid = libc::syscall(libc::SYS_gettid) as libc::id_t;
                nice.lock()
                    .unwrap()
                    .push(libc::getpriority(libc::PRIO_PROCESS, tid));
            }
        });
        assert!(seen.into_inner().unwrap().iter().all(|&c| c == 1));
        // At least 10: a test runner that is already nicer keeps its own value.
        assert!(nice.into_inner().unwrap().iter().all(|&n| n >= 10));
        // The caller's priority is unchanged.
        #[cfg(target_os = "linux")]
        // SAFETY: as above.
        unsafe {
            let tid = libc::syscall(libc::SYS_gettid) as libc::id_t;
            assert!(libc::getpriority(libc::PRIO_PROCESS, tid) < 10);
        }
    }
}
