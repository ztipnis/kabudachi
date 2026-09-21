//! The election loop of the worker: it ticks the election node, tells the
//! scheduler and everyone watching what state the worker is in, and drains the
//! worker when told to stop.

use std::sync::Arc;
use std::time::Duration;

use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::single_node::SingleNode;
use tokio::sync::{Notify, watch};

use crate::clock::RealClock;
use crate::work::{SharedScheduler, Wake, lock_scheduler};

/// Enough to walk a node from any state to `Stopped`: one tick can be needed
/// to leave the single tick it spends suspecting a leader, then one drain.
const DRAIN_ATTEMPTS: usize = 4;

/// Announces changes of the worker's state: to the scheduler first, so its
/// leader gate has flipped before anyone who sees the new state acts on it,
/// then to watchers, then to anyone waiting for something to change.
pub struct Publisher {
    pub state: watch::Sender<WorkerState>,
    pub scheduler: SharedScheduler,
    pub wake: Wake,
    /// Told when leadership changes, which decides whether deadlines matter.
    pub timers: Arc<Notify>,
}

impl Publisher {
    /// Tells the scheduler, then everyone watching, that the worker is now in `new`.
    fn publish(&self, new: WorkerState) {
        if *self.state.borrow() == new {
            return;
        }
        lock_scheduler(&self.scheduler).set_worker_state(new);
        self.state.send_replace(new);
        self.wake.notify();
        self.timers.notify_one();
    }
}

/// Advances the election every `tick` and publishes the worker's state, until
/// told to stop; then drains the worker so it leaves the shard cleanly.
pub async fn run_election(
    mut node: SingleNode<RealClock>,
    tick: Duration,
    stop: Arc<Notify>,
    publisher: Publisher,
) {
    let mut interval = tokio::time::interval(tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = interval.tick() => {
                node.tick();
                publisher.publish(node.state());
            }
            _ = stop.notified() => {
                drain(&mut node);
                publisher.publish(node.state());
                return;
            }
        }
    }
}

/// Drives `node` to `Stopped` from any state. A node that is suspecting a
/// leader ignores a drain request, so it is ticked past that state and asked
/// again.
fn drain(node: &mut SingleNode<RealClock>) {
    for _ in 0..DRAIN_ATTEMPTS {
        if node.state() == WorkerState::Stopped {
            return;
        }
        node.begin_drain();
        node.tick();
    }
    debug_assert_eq!(node.state(), WorkerState::Stopped);
}

#[cfg(test)]
mod tests {
    use kabudachi_core::protocol::ids::{IncarnationId, ShardId, WorkerId};
    use kabudachi_core::single_node::single_node;

    use super::*;

    fn new_node() -> SingleNode<RealClock> {
        single_node(
            WorkerId::new("worker-1"),
            IncarnationId::new("incarnation-1"),
            ShardId::new("local"),
            RealClock::new(),
        )
    }

    /// Lets real time pass so the node's next tick moves it forward.
    fn let_time_pass() {
        std::thread::sleep(Duration::from_millis(3));
    }

    #[test]
    fn a_fresh_node_drains_to_stopped() {
        let mut node = new_node();

        drain(&mut node);

        assert_eq!(node.state(), WorkerState::Stopped);
    }

    #[test]
    fn a_node_suspecting_a_leader_drains_to_stopped() {
        let mut node = new_node();
        let_time_pass();
        node.tick();
        assert_eq!(node.state(), WorkerState::LeaderSuspect);

        drain(&mut node);

        assert_eq!(node.state(), WorkerState::Stopped);
    }

    #[test]
    fn a_leader_drains_to_stopped() {
        let mut node = new_node();
        let_time_pass();
        node.tick();
        node.tick();
        assert_eq!(node.state(), WorkerState::Leader);

        drain(&mut node);

        assert_eq!(node.state(), WorkerState::Stopped);
    }

    #[test]
    fn draining_a_stopped_node_changes_nothing() {
        let mut node = new_node();
        drain(&mut node);

        drain(&mut node);

        assert_eq!(node.state(), WorkerState::Stopped);
    }
}
