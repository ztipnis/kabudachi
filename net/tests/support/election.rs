//! Election helpers shared by the `net/tests/<area>/` crates.

use std::time::Duration as StdDuration;

use kabudachi_core::election::{Input, Step, WorkerNode};
use kabudachi_core::protocol::ids::Uuid7Ids;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::Scheduler;
use kabudachi_core::task_record::RecordOutbox;
use kabudachi_core::time::{Clock, RealClock};
use kabudachi_net::authority::AuthorityClient;
use kabudachi_net::driver::{DriverConfig, SharedAuthority, run_driver};
use kabudachi_net::messenger::Net;
use tokio::sync::watch;

/// A step that asks for nothing and is due now: the first step of a node
/// started as a founder or inside a known configuration, and what a node
/// driven before hands `run_driver` when it is driven again.
pub fn due_now(clock: &impl Clock) -> Step {
    Step {
        outputs: Vec::new(),
        next_deadline: Some(clock.now()),
    }
}

/// Drives `node` until it leads (its grant is then with `scheduler`) and
/// returns, so a test can give the leader's scheduler work before driving on
/// with [`due_now`].
pub async fn drive_until_leading(
    node: &mut WorkerNode<RealClock>,
    first: Step,
    net: &Net,
    scheduler: &mut Scheduler<RealClock, Uuid7Ids, RecordOutbox>,
    clock: RealClock,
) {
    let (state_sender, mut state) = watch::channel(node.state());
    tokio::select! {
        _ = run_driver(node, first, net, scheduler, clock, None, DriverConfig::default(), |node, _, _| {
            let _ = state_sender.send(node.state());
        }) => unreachable!("run_driver never returns"),
        led = state.wait_for(|state| *state == WorkerState::Leader) => {
            led.expect("the driver is still running");
        }
    }
}

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
/// timeout on. The gossip roll call, unlike the ring roll call it replaced,
/// does not need the winner to suspect first.
///
/// Should two nodes suspect together, the tie-break ranks their calls the
/// same way on both, the worse initiator answers the better call and
/// abandons its own, and the better one wins. Which one wins depends on the
/// jitter, the wall clock and the `WorkerId`s, so a test that does not fix
/// them must not name the winner.
///
/// Equal suspicion timeouts (or a follower's longer than its leader's) are
/// the safe way round: the leader's lease runs out before a follower could
/// suspect it (see `ElectionTimings::suspect_timeout`).
pub fn built_on_one_tick<C: Clock, T>(clock: &C, mut build: impl FnMut() -> T) -> T {
    loop {
        let before = clock.now();
        let built = build();
        if clock.now() == before {
            return built;
        }
    }
}

/// Polls `condition` every few milliseconds until it holds. Callers bound
/// the wait with a timeout.
pub async fn wait_until(mut condition: impl FnMut() -> bool) {
    while !condition() {
        tokio::time::sleep(StdDuration::from_millis(5)).await;
    }
}

/// Runs the driver of each of the three `nodes` on its net, scheduler,
/// authority and `observe`, until `until` completes, and returns what it
/// returned. Every node, scheduler and `clock` must read one clock (see
/// `run_driver`).
pub async fn drive_three_until<T, F>(
    nodes: &mut [WorkerNode<RealClock>; 3],
    nets: [&Net; 3],
    schedulers: &mut [Scheduler<RealClock, Uuid7Ids, RecordOutbox>; 3],
    clock: RealClock,
    authorities: [Option<SharedAuthority>; 3],
    observers: [F; 3],
    until: impl Future<Output = T>,
) -> T
where
    F: FnMut(&WorkerNode<RealClock>, Option<&Input>, &Step),
{
    let [node_a, node_b, node_c] = nodes;
    let [scheduler_a, scheduler_b, scheduler_c] = schedulers;
    let [observe_a, observe_b, observe_c] = observers;
    let [authority_a, authority_b, authority_c] = authorities;
    let client = |net: &Net, node: &WorkerNode<RealClock>, authority: Option<SharedAuthority>| {
        authority.map(|authority| AuthorityClient::new(net, node.shard_id().clone(), authority))
    };
    let authority_a = client(nets[0], node_a, authority_a);
    let authority_b = client(nets[1], node_b, authority_b);
    let authority_c = client(nets[2], node_c, authority_c);
    tokio::select! {
        _ = run_driver(node_a, due_now(&clock), nets[0], scheduler_a, clock, authority_a, DriverConfig::default(), observe_a) => {
            unreachable!("run_driver never returns")
        }
        _ = run_driver(node_b, due_now(&clock), nets[1], scheduler_b, clock, authority_b, DriverConfig::default(), observe_b) => {
            unreachable!("run_driver never returns")
        }
        _ = run_driver(node_c, due_now(&clock), nets[2], scheduler_c, clock, authority_c, DriverConfig::default(), observe_c) => {
            unreachable!("run_driver never returns")
        }
        output = until => output,
    }
}
