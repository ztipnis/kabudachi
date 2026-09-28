//! The election loop of the worker: it steps the election node whenever the
//! node's next deadline comes, hands each step's leadership grant to the
//! scheduler, tells everyone watching each state the worker moves into, and
//! drains the worker when told to stop.

use std::sync::Arc;

use kabudachi_core::election::{Input, Output, apply_to_scheduler};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::RealClock;
use tokio::sync::{Notify, watch};

use crate::local_node::LocalNode;
use crate::timers::{sleep_for, time_until};
use crate::wakeups::Wakeups;
use crate::work::SharedScheduler;

/// Announces what each step of the election changed: to the scheduler first,
/// so its leader gate has moved before anyone who sees the new state acts on
/// it, and then to everyone watching the worker's state.
pub struct Publisher {
    pub state: watch::Sender<WorkerState>,
    /// Must read the election node's clock: the grants it is handed end at
    /// instants of that clock.
    pub scheduler: SharedScheduler,
    /// Leadership decides what claims and deadlines do, so a change wakes
    /// everyone waiting on either.
    pub wakeups: Wakeups,
}

impl Publisher {
    /// Hands the scheduler what `outputs` ask of it, then publishes, in
    /// order, every state they report the worker moving into. Their messages,
    /// sent or published, are dropped: a lone node has no peer to reach, and
    /// its only messages are the roll call it publishes and the election
    /// certificate it sends itself on winning.
    fn publish_changes(&self, outputs: &[Output]) {
        self.wakeups.with_scheduler(&self.scheduler, |scheduler| {
            apply_to_scheduler(outputs, scheduler)
        });
        for output in outputs {
            if let Output::StateChanged(state) = output {
                self.state.send_replace(*state);
            }
        }
    }
}

/// Steps the election whenever the node's next deadline comes, and publishes
/// what each step changed (see [`Publisher`]), until told to stop; then
/// drains the worker so it leaves the shard cleanly. `clock` must be the
/// node's own: its deadlines are instants of that clock. A node with no
/// deadline, such as a lone leader, is not woken until it is told to stop.
pub async fn run_election(
    mut node: LocalNode<RealClock>,
    clock: RealClock,
    stop: Arc<Notify>,
    publisher: Publisher,
) {
    // A node reports its deadline only when stepped, so the loop starts with
    // a `Tick`, which is harmless whenever it comes: the node checks its
    // timers against its own clock.
    let mut step = node.step(Input::Tick);
    loop {
        publisher.publish_changes(&step.outputs);
        tokio::select! {
            _ = sleep_for(time_until(clock, step.next_deadline)) => {
                step = node.step(Input::Tick);
            }
            _ = stop.notified() => {
                publisher.publish_changes(&drain(&mut node, clock).await);
                return;
            }
        }
    }
}

/// Asks `node` to drain, then steps it at the deadlines it reports until it
/// is `Stopped`, and returns everything it produced. A lone node drains at
/// once from `Active` or `Leader`; one suspecting a leader instead starts a
/// roll call at its next `Tick`, due at once, elects itself at that call's
/// deadline, and drains in that same step.
async fn drain(node: &mut LocalNode<RealClock>, clock: RealClock) -> Vec<Output> {
    let mut step = node.step(Input::Drain);
    let mut outputs = std::mem::take(&mut step.outputs);
    while node.state() != WorkerState::Stopped {
        // Every state a lone node can still be waiting in here has a
        // deadline (see above), so this never waits for ever.
        let deadline = step
            .next_deadline
            .expect("a lone node that has not drained yet always has a deadline");
        sleep_for(time_until(clock, Some(deadline))).await;
        step = node.step(Input::Tick);
        outputs.append(&mut step.outputs);
    }
    outputs
}

#[cfg(test)]
mod tests {
    use kabudachi_core::protocol::ids::{IncarnationId, ShardId, Uuid7Ids, WorkerId};
    use kabudachi_core::scheduler::Scheduler;
    use kabudachi_core::time::Duration as CoreDuration;
    use std::sync::Mutex;
    use std::time::Duration;

    use super::*;
    use crate::local_node::local_node;
    use crate::work::{Wake, lock_scheduler};

    const WAIT_LIMIT: Duration = Duration::from_secs(5);

    fn new_node(clock: RealClock, suspect_timeout: CoreDuration) -> LocalNode<RealClock> {
        local_node(
            WorkerId::new("worker-1"),
            IncarnationId::new("incarnation-1"),
            ShardId::new("local"),
            clock,
            suspect_timeout,
        )
    }

    /// A node that elects itself as soon as a millisecond has passed.
    fn eager_node(clock: RealClock) -> LocalNode<RealClock> {
        new_node(clock, CoreDuration::from_millis(0))
    }

    /// Lets real time pass so the node's next tick moves it forward.
    fn let_time_pass() {
        std::thread::sleep(Duration::from_millis(3));
    }

    #[tokio::test]
    async fn a_drain_from_active_ends_stopped() {
        let clock = RealClock::new();
        let mut node = new_node(clock, CoreDuration::from_secs(60));
        assert_eq!(node.state(), WorkerState::Active);

        drain(&mut node, clock).await;

        assert_eq!(node.state(), WorkerState::Stopped);
    }

    #[tokio::test]
    async fn a_drain_while_suspecting_a_leader_ends_stopped() {
        let clock = RealClock::new();
        let mut node = eager_node(clock);
        let_time_pass();
        let _ = node.step(Input::Tick);
        assert_eq!(node.state(), WorkerState::LeaderSuspect);

        drain(&mut node, clock).await;

        assert_eq!(node.state(), WorkerState::Stopped);
    }

    #[tokio::test]
    async fn a_drain_from_leader_ends_stopped() {
        let clock = RealClock::new();
        let mut node = eager_node(clock);
        let_time_pass();
        let _ = node.step(Input::Tick);
        let _ = node.step(Input::Tick);
        let_time_pass();
        let _ = node.step(Input::Tick);
        assert_eq!(node.state(), WorkerState::Leader);

        drain(&mut node, clock).await;

        assert_eq!(node.state(), WorkerState::Stopped);
    }

    #[tokio::test]
    async fn a_second_drain_changes_nothing() {
        let clock = RealClock::new();
        let mut node = eager_node(clock);
        drain(&mut node, clock).await;

        let outputs = drain(&mut node, clock).await;

        assert!(outputs.is_empty(), "{outputs:?}");
        assert_eq!(node.state(), WorkerState::Stopped);
    }

    #[tokio::test]
    async fn the_election_and_its_scheduler_lead_on_the_nodes_own_timers_until_stopped() {
        let clock = RealClock::new();
        let node = eager_node(clock);
        let (state_sender, mut state) = watch::channel(node.state());
        let scheduler: SharedScheduler = Arc::new(Mutex::new(Scheduler::new(clock, Uuid7Ids)));
        let publisher = Publisher {
            state: state_sender,
            scheduler: Arc::clone(&scheduler),
            wakeups: Wakeups::new(Wake::new(), Wake::new(), Arc::new(Notify::new())),
        };
        let stop = Arc::new(Notify::new());

        let election = tokio::spawn(run_election(node, clock, Arc::clone(&stop), publisher));
        tokio::time::timeout(
            WAIT_LIMIT,
            state.wait_for(|current| *current == WorkerState::Leader),
        )
        .await
        .expect("the lone node leads within the limit")
        .expect("the election is still running");
        assert!(lock_scheduler(&scheduler).is_leading());

        stop.notify_one();
        tokio::time::timeout(WAIT_LIMIT, election)
            .await
            .expect("the election stops within the limit")
            .expect("the election did not panic");
        assert_eq!(*state.borrow(), WorkerState::Stopped);
        assert!(!lock_scheduler(&scheduler).is_leading());
    }
}
