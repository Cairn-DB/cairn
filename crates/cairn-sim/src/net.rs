//! Simulated network: per-node inboxes, seeded delay, drops, partitions.

use crate::completion::{Completer, completion};
use crate::sim::Simulation;
use bytes::Bytes;
use cairn_core::error::IoErrorKind;
use cairn_core::{Duration, Error, HashMap, HashSet, Network, NodeId, Result};
use std::collections::VecDeque;

/// Network behaviour.
#[derive(Debug, Clone)]
pub struct NetConfig {
    /// Minimum one-way delay.
    pub delay_min: Duration,
    /// Maximum one-way delay.
    pub delay_max: Duration,
    /// Probability that a message is dropped.
    pub drop_prob: f64,
}

impl Default for NetConfig {
    fn default() -> Self {
        NetConfig {
            delay_min: Duration::from_micros(50),
            delay_max: Duration::from_millis(2),
            drop_prob: 0.0,
        }
    }
}

#[derive(Default)]
pub(crate) struct NetState {
    inboxes: HashMap<NodeId, VecDeque<(NodeId, Bytes)>>,
    waiters: HashMap<NodeId, VecDeque<Completer<(NodeId, Bytes)>>>,
    /// Directed pairs `(from, to)` that cannot communicate.
    blocked: HashSet<(NodeId, NodeId)>,
    sent: u64,
    delivered: u64,
    dropped: u64,
}

impl NetState {
    pub(crate) fn ensure_node(&mut self, node: NodeId) {
        self.inboxes.entry(node).or_default();
        self.waiters.entry(node).or_default();
    }

    pub(crate) fn clear_inbox(&mut self, node: NodeId) {
        if let Some(q) = self.inboxes.get_mut(&node) {
            q.clear();
        }
        // Waiters belong to cancelled tasks; dropping the completers is fine.
        if let Some(w) = self.waiters.get_mut(&node) {
            w.clear();
        }
    }

    fn deliver(&mut self, from: NodeId, to: NodeId, msg: Bytes) {
        self.delivered += 1;
        if let Some(c) = self.waiters.get_mut(&to).and_then(VecDeque::pop_front) {
            c.complete((from, msg));
        } else {
            self.inboxes.entry(to).or_default().push_back((from, msg));
        }
    }
}

/// One node's endpoint on the simulated network.
#[derive(Clone)]
pub struct SimNetwork {
    pub(crate) sim: Simulation,
    pub(crate) node: NodeId,
}

impl Simulation {
    /// Blocks messages from `a` to `b` (one direction). Use twice for a symmetric partition.
    pub fn block(&self, a: NodeId, b: NodeId) {
        self.inner.net.borrow_mut().blocked.insert((a, b));
        self.trace("net.block", &format!("{a}->{b}"));
    }

    /// Restores messages from `a` to `b`.
    pub fn unblock(&self, a: NodeId, b: NodeId) {
        self.inner.net.borrow_mut().blocked.remove(&(a, b));
        self.trace("net.unblock", &format!("{a}->{b}"));
    }

    /// `(sent, delivered, dropped)` message counts.
    pub fn net_counters(&self) -> (u64, u64, u64) {
        let n = self.inner.net.borrow();
        (n.sent, n.delivered, n.dropped)
    }
}

impl Network for SimNetwork {
    async fn send(&self, to: NodeId, message: Bytes) -> Result<()> {
        let cfg = self.sim.inner.config.net.clone();
        let from = self.node;
        let known = self.sim.inner.net.borrow().inboxes.contains_key(&to);
        if !known {
            return Err(Error::io(
                IoErrorKind::Unreachable,
                format!("unknown node {to}"),
            ));
        }
        {
            let mut n = self.sim.inner.net.borrow_mut();
            n.sent += 1;
            if n.blocked.contains(&(from, to)) {
                n.dropped += 1;
                drop(n);
                self.sim.trace(
                    "net.drop",
                    &format!("{from}->{to} blocked len={}", message.len()),
                );
                return Ok(());
            }
        }
        if cfg.drop_prob > 0.0 && self.sim.unit() < cfg.drop_prob {
            self.sim.inner.net.borrow_mut().dropped += 1;
            self.sim.trace(
                "net.drop",
                &format!("{from}->{to} random len={}", message.len()),
            );
            return Ok(());
        }
        let delay = self.sim.latency(cfg.delay_min, cfg.delay_max);
        let at = self.sim.now() + delay;
        self.sim.trace(
            "net.send",
            &format!("{from}->{to} len={} at={at:?}", message.len()),
        );
        let sim = self.sim.clone();
        // The delivery event belongs to the receiver: a receiver crash drops it.
        self.sim.schedule(
            to,
            at,
            Box::new(move || {
                // A partition raised after sending still drops the message in flight.
                let blocked = sim.inner.net.borrow().blocked.contains(&(from, to));
                if blocked {
                    sim.inner.net.borrow_mut().dropped += 1;
                    sim.trace("net.drop", &format!("{from}->{to} in-flight"));
                } else {
                    sim.trace("net.deliver", &format!("{from}->{to}"));
                    sim.inner.net.borrow_mut().deliver(from, to, message);
                }
            }),
        );
        Ok(())
    }

    async fn recv(&self) -> Result<(NodeId, Bytes)> {
        let queued = self
            .sim
            .inner
            .net
            .borrow_mut()
            .inboxes
            .get_mut(&self.node)
            .and_then(VecDeque::pop_front);
        if let Some(m) = queued {
            return Ok(m);
        }
        let (c, completer) = completion();
        self.sim
            .inner
            .net
            .borrow_mut()
            .waiters
            .entry(self.node)
            .or_default()
            .push_back(completer);
        Ok(c.await)
    }

    fn local_id(&self) -> NodeId {
        self.node
    }
}
