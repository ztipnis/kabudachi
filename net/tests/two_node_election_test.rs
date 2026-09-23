//! Chunk C3's walking-skeleton milestone: two real `WorkerNode`s, each
//! wired to its own real libp2p swarm (`kabudachi_net::messenger::Net`) over
//! real loopback TCP sockets, each driven by its own concurrently-running
//! `kabudachi_net::driver::run_driver` task — no simulator, no fakes. Proof
//! that `core`'s existing, unmodified election state machine converges to a
//! leader under real async/real sockets, not just `FakeClock`/`FakeNetwork`.
//!
//! Both electorates are statically seeded with both `WorkerId`s from the
//! start: no join protocol yet (that's chunk C4).
//!
//! ## Why this reliably converges, and why that is *not* a structural
//! ## guarantee from `core`
//!
//! `core::election::WorkerNode::process_roll_call` only lets a node become
//! `Candidate` from an inbound roll call if that node is *already* in
//! `WorkerState::RollCall` at the moment it processes that specific call
//! (`winner == self.my_id && self.state == WorkerState::RollCall`), and
//! `RollCall`/`Candidate` have no timeout or retry anywhere in `core` — each
//! node's own roll call gets exactly one shot at ever reaching a verdict. So
//! convergence here depends on both `WorkerNode`s already being in
//! `RollCall` (i.e. each having already crossed its own suspicion timeout)
//! by the time the *other* node's forwarded roll call reaches it. Nothing in
//! `core` enforces that ordering by itself.
//!
//! This test gets away with it for two purely test-level reasons, neither of
//! which `core` provides or checks:
//!
//! 1. Both `WorkerNode`s are constructed synchronously, back to back, with
//!    no `.await` between them (see `two_real_nodes_over_real_sockets_converge_to_one_leader`
//!    below) — after `connected_pair` returns, `WorkerNode::new` for `node_a`
//!    and `node_b` run essentially simultaneously, so their suspicion timers
//!    (both seeded from the same `SUSPECT_TIMEOUT_MS`) start within
//!    sub-millisecond real-wall-clock distance of each other.
//! 2. `TICK_INTERVAL_MS` (30ms) is wide enough relative to that sub-millisecond
//!    gap that inbound-message processing latency comfortably outlasts it: by
//!    the time either node's driver loop would deliver a peer's forwarded
//!    roll call, the receiving node has already ticked past its own timeout
//!    and is in `RollCall` too.
//!
//! This is an *engineering margin*, not a proof. If the node that crosses its
//! suspicion timeout first (by real wall-clock timing) is not the
//! `choose_candidate` hash-winner, and the timing margin above doesn't hold,
//! both nodes' one-shot roll calls can die without either reaching
//! `Candidate` — a permanent deadlock that nothing in `core` recovers from
//! automatically. (The simulator's `election_leader_heartbeat_test.rs`
//! avoids this differently: `Cluster::advance` in
//! `core/tests/support/harness.rs` drives every node's `on_message` + `tick()`
//! inside the same call against one shared `FakeClock`, which structurally
//! guarantees both nodes cross into `RollCall` in the same simulated round
//! before either's forwarded call is judged — a harness-level guarantee this
//! real-socket test has no equivalent of.)
//!
//! **This matters for chunk C4 (bootstrap join protocol) and beyond:** C4
//! will not have this test's synchronous, two-sided `WorkerNode`
//! construction — a joining node starts up asynchronously and later than the
//! node(s) already running. Do not assume the convergence pattern here is
//! safe under arbitrary/asymmetric startup timing just because this test
//! passed repeatedly (28/28 runs during C3 development); re-derive whether
//! the timing margin still holds for whatever startup ordering C4 actually
//! introduces, or make `core`'s roll call/candidate path tolerate the race
//! (e.g. retry/timeout) before relying on it.

mod support;

use std::collections::BTreeSet;
use std::time::Duration as StdDuration;

use kabudachi_core::election::WorkerNode;
use kabudachi_core::membership::RingMembership;
use kabudachi_core::protocol::ids::{IncarnationId, ShardId, Uuid7Ids, WorkerId};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::Scheduler;
use kabudachi_core::time::Duration;
use kabudachi_net::driver::run_driver;
use kabudachi_net::messenger::Net;
use kabudachi_net::swarm::build_swarm;
use libp2p::identity;
use tokio::sync::watch;
use tokio::time::timeout;

use support::authority::AlwaysUnavailableAuthority;
use support::clock::RealClock;

const SHARD: &str = "shard-1";

/// Real-clock suspicion timeout, in milliseconds (`RealClock` ticks once per
/// ms — see `support::clock`). 300ms comfortably exceeds real loopback TCP
/// connect + noise/yamux handshake time (single-digit milliseconds locally,
/// still well under 300ms even on a loaded CI/Docker host), so neither node
/// can cross into `LeaderSuspect` before both sides are already connected
/// and `reachable_peers` reflects it — `connected_pair` below waits for
/// exactly that before either `WorkerNode` is even constructed.
const SUSPECT_TIMEOUT_MS: u64 = 300;

/// The driver's poll/tick cadence. 1/10th of `SUSPECT_TIMEOUT_MS`, echoing
/// the simulator tests' ~2:1 tick_size:suspect_timeout ratio (see
/// `core/tests/scenario_election_test.rs`'s `suspect_timeout =
/// Duration::from_ticks(10)` / `tick_size = Duration::from_ticks(5)`) but
/// with a wider safety margin: real wall-clock scheduling jitter (tokio's
/// timer resolution, a loaded Docker host) is much less predictable than the
/// simulator's exact tick counts, so more, finer-grained ticks reduce the
/// chance of a suspicion boundary being crossed by more than one interval's
/// worth of slack.
const TICK_INTERVAL_MS: u64 = 30;

/// Generous upper bound on the whole test, including connection setup;
/// actual convergence is expected in a few hundred ms (see
/// `SUSPECT_TIMEOUT_MS`'s doc) — this is a "something is actually broken"
/// backstop, not the expected runtime.
const TEST_TIMEOUT: StdDuration = StdDuration::from_secs(20);

/// Connects `net_a` and `net_b` over a real loopback TCP socket (mirrors
/// `kabudachi_net::messenger`'s own `connected_pair` test helper, duplicated
/// here because that one is private to `messenger`'s `#[cfg(test)]` module)
/// and returns each side's `WorkerId`.
async fn connected_pair(net_a: &Net, net_b: &Net) -> (WorkerId, WorkerId) {
    let listen_addr = timeout(
        TEST_TIMEOUT,
        net_a.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
    )
    .await
    .expect("net_a produced a listen address within the timeout");

    net_b.dial(listen_addr);

    let worker_a = net_a.local_worker_id();
    let worker_b = net_b.local_worker_id();

    timeout(TEST_TIMEOUT, async {
        loop {
            use kabudachi_core::transport::PeerMessenger;
            if net_a.reachable_peers(worker_a.clone()).contains(&worker_b) {
                return;
            }
            tokio::time::sleep(StdDuration::from_millis(5)).await;
        }
    })
    .await
    .expect("net_a saw net_b as reachable within the timeout");
    timeout(TEST_TIMEOUT, async {
        loop {
            use kabudachi_core::transport::PeerMessenger;
            if net_b.reachable_peers(worker_b.clone()).contains(&worker_a) {
                return;
            }
            tokio::time::sleep(StdDuration::from_millis(5)).await;
        }
    })
    .await
    .expect("net_b saw net_a as reachable within the timeout");

    (worker_a, worker_b)
}

#[allow(clippy::type_complexity)]
fn make_node<'a>(
    my_id: WorkerId,
    electorate: &BTreeSet<WorkerId>,
    transport: &'a Net,
) -> WorkerNode<RealClock, &'a Net, RingMembership, AlwaysUnavailableAuthority> {
    WorkerNode::new(
        my_id.clone(),
        IncarnationId::new(format!("{}-incarnation-0", my_id.as_str())),
        ShardId::new(SHARD),
        RealClock::new(),
        transport,
        RingMembership::new(electorate.clone()),
        AlwaysUnavailableAuthority,
        Duration::from_ticks(SUSPECT_TIMEOUT_MS),
    )
}

/// Blocks until `rx_a`/`rx_b` report exactly one of {Leader, Active} in the
/// pattern chunk C3 expects: one node `Leader`, the other `Active` (a happy
/// follower — see `core::election::WorkerNode::on_leader_ack`'s doc: an
/// accepted heartbeat ack returns a `RollCall` node to `Active`, and that is
/// the follower's whole journey here, since neither node ever had a leader
/// to be `Active` under in the first place until this election produces
/// one).
async fn wait_for_convergence(
    mut rx_a: watch::Receiver<WorkerState>,
    mut rx_b: watch::Receiver<WorkerState>,
) -> (WorkerState, WorkerState) {
    loop {
        let (a, b) = (*rx_a.borrow(), *rx_b.borrow());
        let converged = matches!(
            (a, b),
            (WorkerState::Leader, WorkerState::Active) | (WorkerState::Active, WorkerState::Leader)
        );
        if converged {
            return (a, b);
        }
        tokio::select! {
            result = rx_a.changed() => { result.expect("node_a's driver task is still running"); }
            result = rx_b.changed() => { result.expect("node_b's driver task is still running"); }
        }
    }
}

#[tokio::test]
async fn two_real_nodes_over_real_sockets_converge_to_one_leader() {
    let net_a = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
    let net_b = Net::new(build_swarm(identity::Keypair::generate_ed25519()));

    // Connect first: `WorkerNode::new` starts each node's leader-contact
    // timer at construction time (see `core::election::WorkerNode::new`'s
    // doc), so constructing only after the swarms are already connected
    // guarantees `suspect_timeout` cannot elapse before `reachable_peers`
    // is populated — see `SUSPECT_TIMEOUT_MS`'s doc for the timing argument
    // this depends on.
    let (worker_a, worker_b) = connected_pair(&net_a, &net_b).await;
    let electorate: BTreeSet<WorkerId> = [worker_a.clone(), worker_b.clone()].into_iter().collect();

    let mut node_a = make_node(worker_a.clone(), &electorate, &net_a);
    let mut node_b = make_node(worker_b.clone(), &electorate, &net_b);

    let (tx_a, rx_a) = watch::channel(node_a.state());
    let (tx_b, rx_b) = watch::channel(node_b.state());

    let tick_interval = StdDuration::from_millis(TICK_INTERVAL_MS);
    // This test only exercises election convergence, not claim arbitration,
    // but every driven node carries a Scheduler regardless — see
    // run_driver's doc.
    let mut scheduler_a = Scheduler::new(RealClock::new(), Uuid7Ids);
    let mut scheduler_b = Scheduler::new(RealClock::new(), Uuid7Ids);

    let started = std::time::Instant::now();

    // `run_driver` never returns (see its doc), so this must race the two
    // driver loops against the convergence check rather than `tokio::join!`
    // them: `select!` returns (dropping, and thereby stopping, the other
    // branches) as soon as `wait_for_convergence` does.
    let (state_a, state_b) = timeout(TEST_TIMEOUT, async {
        tokio::select! {
            _ = run_driver(
                &mut node_a,
                &net_a,
                tick_interval,
                |s| { let _ = tx_a.send(s); },
                |_| {},
                &mut scheduler_a,
            ) => {
                unreachable!("run_driver never returns")
            }
            _ = run_driver(
                &mut node_b,
                &net_b,
                tick_interval,
                |s| { let _ = tx_b.send(s); },
                |_| {},
                &mut scheduler_b,
            ) => {
                unreachable!("run_driver never returns")
            }
            result = wait_for_convergence(rx_a, rx_b) => result,
        }
    })
    .await
    .expect(
        "expected exactly one Leader and one Active follower to emerge within the test timeout",
    );

    let elapsed = started.elapsed();
    eprintln!(
        "two_real_nodes_over_real_sockets_converge_to_one_leader: converged in {elapsed:?} \
         (state_a={state_a:?}, state_b={state_b:?})"
    );

    let leader_count = [state_a, state_b]
        .iter()
        .filter(|s| **s == WorkerState::Leader)
        .count();
    let follower_count = [state_a, state_b]
        .iter()
        .filter(|s| **s == WorkerState::Active)
        .count();
    assert_eq!(leader_count, 1, "expected exactly one Leader");
    assert_eq!(follower_count, 1, "expected exactly one Active follower");
}
