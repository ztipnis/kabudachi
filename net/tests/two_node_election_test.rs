//! Two real `WorkerNode`s, each wired to its own real libp2p swarm
//! (`kabudachi_net::messenger::Net`) over real loopback TCP sockets, each
//! driven by its own concurrently-running `kabudachi_net::driver::run_driver`
//! — no simulator, no fakes. Proof that `core`'s election state machine
//! converges to a leader under real async and real sockets, not just
//! `FakeClock`/`FakeNetwork`.
//!
//! Both nodes are statically built as the two voters of one configuration;
//! the join protocol has its own test (`three_node_join_test.rs`).
//!
//! ## Why both nodes start on one tick, with one suspicion timeout
//!
//! Each node suspects after its own jittered suspicion timeout (see
//! `ElectionTimings::suspect_timeout`), so one usually suspects first and
//! publishes a roll call for term 1, which the other answers and the first
//! wins. Should both suspect together, the tie-break ranks the two calls the
//! same way on both nodes, the worse initiator answers the better call and
//! abandons its own, and the better one wins. Which one wins depends on the
//! jitter, the wall clock and the two `WorkerId`s, so the test does not name
//! it.
//!
//! The first roll call wins at once only if the other node no longer counts
//! its leader contact as fresh when the call reaches it: a node refuses a
//! roll call for a suspicion timeout after it was built. With one suspicion
//! timeout and both nodes built on the same clock tick (`built_on_one_tick`),
//! each is stale by the time the other suspects. Otherwise the refused call
//! closes short of a quorum and a later one elects a leader, which only
//! takes longer. The ring roll call this replaced needed the winner to
//! suspect first; the gossip roll call does not.
//!
//! A follower whose suspicion timeout is longer than its leader's is the
//! safe way round (see `ElectionTimings::suspect_timeout`): the leader's
//! lease runs out before the follower could suspect it. Equal timeouts are
//! that too.

mod support;

use std::time::Duration as StdDuration;

use kabudachi_core::configuration::{Configuration, Generation, Single};
use kabudachi_core::election::{ElectionTimings, KnownConfiguration, WorkerNode};
use kabudachi_core::protocol::ids::{IncarnationId, ShardId, Uuid7Ids, WorkerId};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::Scheduler;
use kabudachi_core::time::{Duration, RealClock};
use kabudachi_net::driver::run_driver;
use kabudachi_net::messenger::Net;
use kabudachi_net::swarm::build_swarm;
use tokio::sync::watch;
use tokio::time::timeout;

use libp2p::identity::Keypair;
use support::election::built_on_one_tick;
use support::net::wait_until_registered;

const SHARD: &str = "shard-1";

/// Both nodes' suspicion timeout, in milliseconds (`RealClock` ticks once
/// per ms — see `kabudachi_core::time`). 300ms comfortably exceeds real
/// loopback TCP connect + noise/yamux handshake time and the gossipsub
/// subscription exchange (single-digit milliseconds locally, still well
/// under 300ms even on a loaded CI/Docker host), and `connected_pair` waits
/// for both sides to register the connection before either `WorkerNode` is
/// even constructed.
const SUSPECT_TIMEOUT_MS: u64 = 300;

/// How often a follower heartbeats its leader: well inside every suspicion
/// timeout this file uses.
const HEARTBEAT_INTERVAL_MS: u64 = 10;

/// How long a roll call runs: well above the time a roll call takes to
/// reach a loopback peer and its reply to come back.
const ROLL_CALL_DEADLINE_MS: u64 = 100;

/// Generous upper bound on the whole test, including connection setup;
/// actual convergence is expected just after `SUSPECT_TIMEOUT_MS` —
/// this is a "something is actually broken" backstop, not the expected
/// runtime.
const TEST_TIMEOUT: StdDuration = StdDuration::from_secs(20);

/// Connects `net_a` and `net_b` over a real loopback TCP socket, waits
/// until each has registered the connection, and returns each side's
/// `WorkerId`.
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
    // Neither net's inputs are taken: the nodes built afterwards are fed
    // them.
    wait_until_registered(net_a, &worker_b).await;
    wait_until_registered(net_b, &worker_a).await;

    (worker_a, worker_b)
}

/// `my_id`'s node, one of the two voters of the shard's configuration.
fn make_node(clock: RealClock, my_id: WorkerId) -> WorkerNode<RealClock> {
    WorkerNode::new(
        my_id.clone(),
        IncarnationId::new(format!("{}-incarnation-0", my_id.as_str())),
        ShardId::new(SHARD),
        clock,
        KnownConfiguration {
            configuration: Configuration::single(Single {
                generation: Generation::genesis(0),
                base: Generation::genesis(0),
                voter_count: 2,
            }),
            admission: Some(Generation::genesis(0)),
        },
        None,
        ElectionTimings::new(
            Duration::from_millis(SUSPECT_TIMEOUT_MS),
            Duration::from_millis(HEARTBEAT_INTERVAL_MS),
        )
        .with_roll_call_deadline(Duration::from_millis(ROLL_CALL_DEADLINE_MS)),
    )
}

/// Blocks until `rx_a`/`rx_b` report one node `Leader` and the other
/// `Active`: a follower that answered the winner's roll call while still
/// `Active` stays so, and one that had started a roll call of its own
/// returns to `Active` on the winner's ack (see
/// `core::election::WorkerNode::on_leader_ack`'s doc).
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
    let net_a = Net::new(build_swarm(Keypair::generate_ed25519()));
    let net_b = Net::new(build_swarm(Keypair::generate_ed25519()));

    // Connect first: `WorkerNode::new` starts each node's leader-contact
    // timer at construction time (see `core::election::WorkerNode::new`'s
    // doc), so constructing only after the swarms are already connected
    // guarantees `suspect_timeout` cannot elapse before each node is told of
    // the connection.
    let (worker_a, worker_b) = connected_pair(&net_a, &net_b).await;

    // One clock for both nodes and their schedulers (see run_driver's doc).
    let clock = RealClock::new();
    let (mut node_a, mut node_b) = built_on_one_tick(&clock, || {
        (
            make_node(clock, worker_a.clone()),
            make_node(clock, worker_b.clone()),
        )
    });

    let (tx_a, rx_a) = watch::channel(node_a.state());
    let (tx_b, rx_b) = watch::channel(node_b.state());

    // This test only exercises election convergence, not claim arbitration,
    // but every driven node carries a Scheduler regardless — see
    // run_driver's doc.
    let mut scheduler_a = Scheduler::new(clock, Uuid7Ids);
    let mut scheduler_b = Scheduler::new(clock, Uuid7Ids);

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
                &mut scheduler_a,
                clock,
                None,
                |node, _| { let _ = tx_a.send(node.state()); },
            ) => {
                unreachable!("run_driver never returns")
            }
            _ = run_driver(
                &mut node_b,
                &net_b,
                &mut scheduler_b,
                clock,
                None,
                |node, _| { let _ = tx_b.send(node.state()); },
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
}
