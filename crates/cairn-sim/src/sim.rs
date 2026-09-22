//! The simulation core shared by reactor, disks, network and runtimes.

use crate::disk::{DiskConfig, DiskState, SimDisk};
use crate::net::{NetConfig, NetState, SimNetwork};
use crate::reactor::SimReactor;
use crate::runtime::SimRuntime;
use crate::trace::Trace;
use cairn_core::{Duration, HashMap, Instant, NodeId, SeededRng};
use cairn_runtime::{Executor, Handle};
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::rc::Rc;

/// Everything that can vary between scenarios.
#[derive(Debug, Clone)]
pub struct SimConfig {
    /// Disk behaviour (latency, crash semantics, faults).
    pub disk: DiskConfig,
    /// Network behaviour (delay, drops).
    pub net: NetConfig,
    /// How many recent trace events to keep in clear text.
    pub trace_keep: usize,
}

impl Default for SimConfig {
    fn default() -> Self {
        SimConfig {
            disk: DiskConfig::default(),
            net: NetConfig::default(),
            trace_keep: 64,
        }
    }
}

pub(crate) type Event = Box<dyn FnOnce()>;

pub(crate) struct SimInner {
    pub(crate) config: SimConfig,
    pub(crate) now: Cell<Instant>,
    pub(crate) events: RefCell<BTreeMap<(Instant, u64), (NodeId, Event)>>,
    pub(crate) seq: Cell<u64>,
    pub(crate) rng: RefCell<SeededRng>,
    pub(crate) trace: RefCell<Trace>,
    pub(crate) disks: RefCell<HashMap<NodeId, DiskState>>,
    pub(crate) net: RefCell<NetState>,
}

/// Handle to a simulation; cheap to clone.
#[derive(Clone)]
pub struct Simulation {
    pub(crate) inner: Rc<SimInner>,
}

impl Simulation {
    /// Creates a simulation and its executor for `seed`.
    pub fn new(seed: u64, config: SimConfig) -> (Simulation, Executor<SimReactor>) {
        let root = SeededRng::from_seed(seed);
        let sim = Simulation {
            inner: Rc::new(SimInner {
                trace: RefCell::new(Trace::new(config.trace_keep)),
                config,
                now: Cell::new(Instant::ZERO),
                events: RefCell::new(BTreeMap::new()),
                seq: Cell::new(0),
                rng: RefCell::new(root.fork("sim")),
                disks: RefCell::new(HashMap::default()),
                net: RefCell::new(NetState::default()),
            }),
        };
        let mut executor = Executor::new(SimReactor { sim: sim.clone() });
        executor.set_scheduler_rng(root.fork("scheduler"));
        (sim, executor)
    }

    /// Current virtual time.
    pub fn now(&self) -> Instant {
        self.inner.now.get()
    }

    /// Records a trace event.
    pub fn trace(&self, kind: &str, detail: &str) {
        self.inner
            .trace
            .borrow_mut()
            .record(self.now(), kind, detail);
    }

    /// Fingerprint of the run so far.
    pub fn digest(&self) -> u64 {
        self.inner.trace.borrow().digest()
    }

    /// Number of trace events.
    pub fn trace_len(&self) -> u64 {
        self.inner.trace.borrow().len()
    }

    /// Recent trace events in clear text, for failure reports.
    pub fn recent_events(&self) -> Vec<String> {
        self.inner
            .trace
            .borrow()
            .recent()
            .map(str::to_owned)
            .collect()
    }

    /// A fresh generator derived from the run seed and `purpose`.
    pub fn rng(&self, purpose: &str) -> SeededRng {
        self.inner.rng.borrow().fork(purpose)
    }

    /// Uniform duration in `[lo, hi]` from the simulation's own generator.
    pub(crate) fn latency(&self, lo: Duration, hi: Duration) -> Duration {
        let (lo, hi) = (lo.as_nanos() as u64, hi.as_nanos() as u64);
        if hi <= lo {
            return Duration::from_nanos(lo);
        }
        let r = self.inner.rng.borrow_mut().below(hi - lo + 1);
        Duration::from_nanos(lo + r)
    }

    /// Uniform `f64` in `[0, 1)` from the simulation's generator.
    pub(crate) fn unit(&self) -> f64 {
        self.inner.rng.borrow_mut().unit_f64()
    }

    /// Schedules `event` to run at `at` on behalf of `node`.
    pub(crate) fn schedule(&self, node: NodeId, at: Instant, event: Event) {
        let seq = self.inner.seq.get();
        self.inner.seq.set(seq + 1);
        let at = at.max(self.now());
        self.inner
            .events
            .borrow_mut()
            .insert((at, seq), (node, event));
    }

    /// Ensures `node` has a disk and returns a runtime for it bound to `handle`.
    pub fn runtime(&self, node: NodeId, handle: &Handle) -> SimRuntime {
        self.inner.disks.borrow_mut().entry(node).or_default();
        self.inner.net.borrow_mut().ensure_node(node);
        SimRuntime::new(self.clone(), node, handle.clone())
    }

    /// Simulated disk of `node` (for inspection and crash injection).
    pub fn disk(&self, node: NodeId) -> SimDisk {
        self.inner.disks.borrow_mut().entry(node).or_default();
        SimDisk {
            sim: self.clone(),
            node,
        }
    }

    /// Simulated network endpoint of `node`.
    pub fn network(&self, node: NodeId) -> SimNetwork {
        self.inner.net.borrow_mut().ensure_node(node);
        SimNetwork {
            sim: self.clone(),
            node,
        }
    }

    /// Crashes `node`: cancels its tasks, drops its in-flight events and inbox, and applies the
    /// disk's crash semantics (unsynced data lost, possibly torn). The disk survives for restart.
    pub fn crash(&self, node: NodeId, executor: &mut Executor<SimReactor>) {
        let cancelled = executor.cancel_tagged(node.get());
        let dropped = {
            let mut events = self.inner.events.borrow_mut();
            let before = events.len();
            events.retain(|_, (n, _)| *n != node);
            before - events.len()
        };
        self.inner.net.borrow_mut().clear_inbox(node);
        let mut rng = self
            .rng("crash")
            .fork(&format!("{node}-{}", self.now().as_nanos()));
        if let Some(d) = self.inner.disks.borrow_mut().get_mut(&node) {
            d.crash(&self.inner.config.disk, &mut rng);
        }
        self.trace(
            "crash",
            &format!("node={node} tasks={cancelled} events={dropped}"),
        );
    }

    /// Number of events waiting in the queue.
    pub fn pending_events(&self) -> usize {
        self.inner.events.borrow().len()
    }
}
