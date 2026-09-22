//! Virtual-time reactor: parking pops the next event.

use crate::sim::Simulation;
use cairn_core::Instant;
use cairn_runtime::Reactor;

/// Reactor over a [`Simulation`]'s event queue.
pub struct SimReactor {
    pub(crate) sim: Simulation,
}

impl SimReactor {
    /// The simulation this reactor drives.
    pub fn simulation(&self) -> &Simulation {
        &self.sim
    }
}

impl Reactor for SimReactor {
    fn park(&mut self, now: Instant, deadline: Option<Instant>) -> Instant {
        let next = {
            let mut events = self.sim.inner.events.borrow_mut();
            match events.first_entry() {
                Some(e) if deadline.is_none_or(|d| e.key().0 <= d) => {
                    let (key, (_, ev)) = e.remove_entry();
                    Some((key.0, ev))
                }
                _ => None,
            }
        };
        match next {
            Some((at, event)) => {
                let at = at.max(now);
                self.sim.inner.now.set(at);
                event();
                at
            }
            None => {
                let at = deadline.unwrap_or(now).max(now);
                self.sim.inner.now.set(at);
                at
            }
        }
    }

    fn has_pending(&self) -> bool {
        !self.sim.inner.events.borrow().is_empty()
    }
}
