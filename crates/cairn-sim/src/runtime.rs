//! `Runtime` implementation for one simulated node.

use crate::disk::SimDisk;
use crate::net::SimNetwork;
use crate::sim::Simulation;
use cairn_core::{Instant, NodeId, Runtime};
use cairn_runtime::Handle;
use std::future::Future;

/// The environment a simulated node's engine code sees.
#[derive(Clone)]
pub struct SimRuntime {
    sim: Simulation,
    node: NodeId,
    handle: Handle,
    disk: SimDisk,
    net: SimNetwork,
}

impl SimRuntime {
    pub(crate) fn new(sim: Simulation, node: NodeId, handle: Handle) -> Self {
        SimRuntime {
            disk: SimDisk {
                sim: sim.clone(),
                node,
            },
            net: SimNetwork {
                sim: sim.clone(),
                node,
            },
            sim,
            node,
            handle,
        }
    }

    /// The node this runtime belongs to.
    pub fn node(&self) -> NodeId {
        self.node
    }

    /// The simulation.
    pub fn simulation(&self) -> &Simulation {
        &self.sim
    }
}

impl Runtime for SimRuntime {
    type Disk = SimDisk;
    type Network = SimNetwork;

    fn now(&self) -> Instant {
        self.handle.now()
    }

    fn sleep_until(&self, deadline: Instant) -> impl Future<Output = ()> {
        self.handle.sleep_until(deadline)
    }

    fn spawn(&self, future: impl Future<Output = ()> + 'static) {
        self.handle.spawn_tagged(self.node.get(), future);
    }

    fn yield_now(&self) -> impl Future<Output = ()> {
        self.handle.yield_now()
    }

    fn offload<T: Send + 'static>(
        &self,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> impl Future<Output = T> {
        // Inline: deterministic, and the only option without threads. An optional delay
        // stands for the time the work would take (`SimConfig::offload_delay`).
        let v = work();
        let delay = self.sim.inner.config.offload_delay;
        let done = (!delay.is_zero()).then(|| self.handle.sleep_until(self.handle.now() + delay));
        async move {
            if let Some(d) = done {
                d.await;
            }
            v
        }
    }

    fn offload_search<T: Send + 'static>(
        &self,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> impl Future<Output = T> {
        // Inline and immediate: `offload_delay` models build time, not searches.
        let v = work();
        async move { v }
    }

    fn disk(&self) -> &Self::Disk {
        &self.disk
    }

    fn network(&self) -> &Self::Network {
        &self.net
    }
}
