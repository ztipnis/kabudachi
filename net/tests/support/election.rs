//! Election helpers shared by the `net/tests/*.rs` files.

use std::time::Duration as StdDuration;

use kabudachi_core::election::{Output, WorkerNode};
use kabudachi_core::protocol::ids::{Uuid7Ids, WorkerId};
use kabudachi_core::protocol::messages::{ElectionMessage, election_message};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::Scheduler;
use kabudachi_core::time::{Clock, Instant, RealClock};
use kabudachi_net::driver::{SharedAuthority, run_driver};
use kabudachi_net::messenger::Net;
use tokio::sync::watch;

/// Calls `build` until `clock` reads the same tick before and after the
/// call, and returns what that call built. A node starts its leader-contact
/// timer on the tick it is built, so the nodes one such call builds all
/// start theirs on the same tick.
///
/// A node counts its leader contact as fresh for exactly one suspicion
/// timeout, and suspects no sooner. Built on one tick, every node's contact
/// has gone stale by the time any node's roll call can reach it, so the
/// first roll call is answered and the test's election settles at once.
/// Built a tick apart, the first call could reach a node whose contact is
/// still fresh, which refuses it; the election would still settle, but
/// only once a later call is retried a roll-call deadline and a suspicion
/// timeout on.
pub fn built_on_one_tick<C: Clock, T>(clock: &C, mut build: impl FnMut() -> T) -> T {
    loop {
        let before = clock.now();
        let built = build();
        if clock.now() == before {
            return built;
        }
    }
}

/// A clock that reads the same monotonic time as `clock` but a wall clock
/// `ahead_by_millis` ahead of it. A roll call is ranked by its initiator's
/// wall-clock timestamp first, so a node on this clock loses every tie to a
/// node on `clock` that starts its roll call at the same moment: a test uses
/// it to decide which of two racing nodes wins. Its monotonic time is
/// `clock`'s own, so it can share instants with nodes and schedulers on
/// `clock` (see `run_driver`'s doc).
#[derive(Debug, Clone, Copy)]
pub struct WallClockAhead {
    pub clock: RealClock,
    pub ahead_by_millis: u64,
}

impl Clock for WallClockAhead {
    fn now(&self) -> Instant {
        self.clock.now()
    }

    fn wall_clock_millis(&self) -> u64 {
        self.clock.wall_clock_millis() + self.ahead_by_millis
    }
}

/// One batch a node's driver ran that produced any output (see
/// `run_driver`'s `observe`), stamped with the node's clock when `observe`
/// saw it: every output in it was produced at or before `at`.
#[derive(Debug, Clone)]
pub struct Batch {
    pub at: Instant,
    /// The node's state after the batch.
    pub state: WorkerState,
    /// The node's recovery epoch after the batch.
    pub recovery_epoch: u64,
    pub outputs: Vec<Output>,
}

/// An `observe` for `run_driver` that appends each batch with any output to
/// `timeline`, stamped by `clock`, which must be the node's own.
pub fn recorder(
    clock: RealClock,
    timeline: watch::Sender<Vec<Batch>>,
) -> impl FnMut(&WorkerNode<RealClock>, &[Output]) {
    move |node, outputs| {
        if outputs.is_empty() {
            return;
        }
        let batch = Batch {
            at: clock.now(),
            state: node.state(),
            recovery_epoch: node.recovery_epoch(),
            outputs: outputs.to_vec(),
        };
        timeline.send_modify(|batches| batches.push(batch));
    }
}

/// Runs the driver of each of the three `nodes` on its net, scheduler,
/// authority and `observe`, until `until` completes, and returns what it
/// returned. Every node, scheduler and `clock` must read one clock (see
/// `run_driver`).
pub async fn drive_three_until<T, F>(
    nodes: &mut [WorkerNode<RealClock>; 3],
    nets: [&Net; 3],
    schedulers: &mut [Scheduler<RealClock, Uuid7Ids>; 3],
    clock: RealClock,
    authorities: [Option<SharedAuthority>; 3],
    observers: [F; 3],
    until: impl Future<Output = T>,
) -> T
where
    F: FnMut(&WorkerNode<RealClock>, &[Output]),
{
    let [node_a, node_b, node_c] = nodes;
    let [scheduler_a, scheduler_b, scheduler_c] = schedulers;
    let [observe_a, observe_b, observe_c] = observers;
    let [authority_a, authority_b, authority_c] = authorities;
    tokio::select! {
        _ = run_driver(node_a, nets[0], scheduler_a, clock, authority_a, observe_a) => {
            unreachable!("run_driver never returns")
        }
        _ = run_driver(node_b, nets[1], scheduler_b, clock, authority_b, observe_b) => {
            unreachable!("run_driver never returns")
        }
        _ = run_driver(node_c, nets[2], scheduler_c, clock, authority_c, observe_c) => {
            unreachable!("run_driver never returns")
        }
        output = until => output,
    }
}

/// Polls `condition` every few milliseconds until it holds. Callers bound
/// the wait with a timeout.
pub async fn wait_until(mut condition: impl FnMut() -> bool) {
    while !condition() {
        tokio::time::sleep(StdDuration::from_millis(5)).await;
    }
}

/// Whether the node that recorded `timeline` leads with a grant: its latest
/// state is `Leader` and the latest grant it reported is `Some`.
pub fn leads_with_grant(timeline: &[Batch]) -> bool {
    let latest_grant = timeline
        .iter()
        .flat_map(|batch| &batch.outputs)
        .filter_map(|output| match output {
            Output::Grant(grant) => Some(grant.is_some()),
            _ => None,
        })
        .next_back();
    timeline
        .last()
        .is_some_and(|batch| batch.state == WorkerState::Leader)
        && latest_grant == Some(true)
}

/// The abort deadline the node that recorded `timeline` last reported in a
/// batch stamped at or before `at` (see `Output::AbortDeadline`); `None` if
/// it reported none by then.
pub fn abort_deadline_as_of(timeline: &[Batch], at: Instant) -> Option<Option<Instant>> {
    timeline
        .iter()
        .filter(|batch| batch.at <= at)
        .flat_map(|batch| &batch.outputs)
        .filter_map(|output| match output {
            Output::AbortDeadline(deadline) => Some(*deadline),
            _ => None,
        })
        .next_back()
}

/// Whether `leader`, while holding a grant, acked a heartbeat of
/// `follower`'s in a batch stamped at or after `since`, and `follower` then
/// accepted that ack: a heartbeat it sent `leader` echoes the ack's send
/// token. A leader's ack carries the heartbeat's token only while it holds
/// a grant, and that echo is what lets the follower count the leader as
/// hearing it (see `Output::AbortDeadline`).
pub fn heard_by_granted_leader(
    leader_timeline: &[Batch],
    leader: &WorkerId,
    follower_timeline: &[Batch],
    follower: &WorkerId,
    since: Instant,
) -> bool {
    let token_acks: Vec<u64> = sent(leader_timeline, follower, since)
        .filter_map(|payload| match payload {
            election_message::Payload::HeartbeatAck(ack) if ack.heartbeat_token.is_some() => {
                Some(ack.send_token)
            }
            _ => None,
        })
        .collect();
    sent(follower_timeline, leader, since).any(|payload| match payload {
        election_message::Payload::Heartbeat(heartbeat) => heartbeat
            .newest_accepted_ack
            .is_some_and(|echo| token_acks.contains(&echo.send_token)),
        _ => false,
    })
}

/// The payloads of every message sent to `to` in a batch of `timeline`
/// stamped at or after `since`.
fn sent<'a>(
    timeline: &'a [Batch],
    to: &'a WorkerId,
    since: Instant,
) -> impl Iterator<Item = &'a election_message::Payload> {
    timeline
        .iter()
        .filter(move |batch| batch.at >= since)
        .flat_map(|batch| &batch.outputs)
        .filter_map(move |output| match output {
            Output::Send {
                to: recipient,
                message:
                    ElectionMessage {
                        payload: Some(payload),
                    },
            } if recipient == to => Some(payload),
            _ => None,
        })
}
