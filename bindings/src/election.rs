//! The election loop of the worker: it steps the election node whenever the
//! node's next deadline comes, carries each step out through
//! `kabudachi_core::election::carry_out` (the step's leadership grant to the
//! scheduler), tells everyone watching each state the worker moves into, and
//! drains the worker when told to stop.

use std::sync::Arc;

use kabudachi_core::election::{Input, Output, Step};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::{Instant, RealClock};
use tokio::sync::{Notify, watch};

use crate::door::{Closed, SchedulerDoor};
use crate::local_node::LocalNode;
use crate::timers::{sleep_for, time_until};

/// Announces what each step of the election changed: to the scheduler first,
/// so its leader gate has moved before anyone who sees the new state acts on
/// it, and then to everyone watching the worker's state.
pub struct Publisher {
    pub state: watch::Sender<WorkerState>,
    /// The way into the scheduler. Its scheduler must read the election
    /// node's clock: the grants it is handed end at instants of that clock.
    pub door: Arc<SchedulerDoor<RealClock>>,
}

impl Publisher {
    /// Carries `step`, one `node` has taken, out through the door (see
    /// `carry_out`): the scheduler is handed what it asks of it, then every
    /// state the step reports the worker moving into is published, in order.
    /// Its messages, sent or published, are dropped: a lone node has no peer
    /// to reach, and its only messages are the roll call it publishes and the
    /// election certificate it sends itself on winning. Returns the node's
    /// next deadline, or `Closed` if the door has shut, in which case
    /// nothing is published.
    fn publish_changes(
        &self,
        node: &mut LocalNode<RealClock>,
        step: Step,
    ) -> Result<Option<Instant>, Closed> {
        let mut states = Vec::new();
        let next_deadline = self.door.carry_out(node, step, |step| {
            for output in &step.outputs {
                if let Output::StateChanged(state) = output {
                    states.push(*state);
                }
            }
        })?;
        for state in states {
            self.state.send_replace(state);
        }
        Ok(next_deadline)
    }

    /// Publishes each state `outputs` report the worker moving into, in
    /// order, to everyone watching the worker's state.
    fn publish_states(&self, outputs: &[Output]) {
        for output in outputs {
            if let Output::StateChanged(state) = output {
                self.state.send_replace(*state);
            }
        }
    }
}

/// Carries out `first`, the step `node` was started with, then steps the
/// election whenever the node's next deadline comes, and publishes what each
/// step changed (see [`Publisher`]), until told to stop or until the door
/// closes; then drains the worker so it leaves the shard cleanly. `clock`
/// must be the node's own: its deadlines are instants of that clock. A node
/// with no deadline, such as a lone leader, is not woken until it is told to
/// stop.
pub async fn run_election(
    mut node: LocalNode<RealClock>,
    first: Step,
    clock: RealClock,
    stop: Arc<Notify>,
    publisher: Publisher,
) {
    let mut step = first;
    while let Ok(next_deadline) = publisher.publish_changes(&mut node, step) {
        tokio::select! {
            _ = sleep_for(time_until(&clock, next_deadline)) => step = node.step(Input::Tick),
            _ = stop.notified() => break,
        }
    }
    // `NativeRuntime::shutdown` closes the door before it notifies `stop`, so
    // what draining changes reaches only the worker's watchers: the
    // scheduler behind the closed door is discarded with the runtime.
    let outputs = drain(&mut node, clock).await;
    publisher.publish_states(&outputs);
}

/// Asks `node` to drain, then steps it at the deadlines it reports until it
/// is `Stopped`, and returns everything it produced. A lone node drains at
/// once from `Active` or `Leader`; one suspecting a leader instead starts a
/// roll call at its next `Tick`, due at once, elects itself at that call's
/// deadline, and drains in that same step.
async fn drain(node: &mut LocalNode<RealClock>, clock: RealClock) -> Vec<Output> {
    // Stepping by hand, not through `carry_out`, is safe only because a
    // local node has no authority: no step asks for a reply to feed back.
    let mut step = node.step(Input::Drain);
    let mut outputs = std::mem::take(&mut step.outputs);
    while node.state() != WorkerState::Stopped {
        // Every state a lone node can still be waiting in here has a
        // deadline (see above), so this never waits for ever.
        let deadline = step
            .next_deadline
            .expect("a lone node that has not drained yet always has a deadline");
        sleep_for(time_until(&clock, Some(deadline))).await;
        step = node.step(Input::Tick);
        outputs.append(&mut step.outputs);
    }
    outputs
}
