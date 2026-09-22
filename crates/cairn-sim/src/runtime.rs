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

    fn disk(&self) -> &Self::Disk {
        &self.disk
    }

    fn network(&self) -> &Self::Network {
        &self.net
    }
}
