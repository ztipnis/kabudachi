//! The bootstrap join protocol end to end over real libp2p swarms — real
//! `WorkerNode`s, real `kabudachi_net::messenger::Net`s, real loopback TCP
//! sockets, no simulator, no fakes. A JOIN answers with a pointer to the
//! shard's leader; the joiner dials that leader and becomes a pending member
//! that no quorum counts yet.
//!
//! ## Topology
//!
//! - `node_a` and `node_b` are constructed as the two voters of one `Active`
//!   configuration and left to elect a leader: built on one clock tick, with
//!   one suspicion timeout (see `support::election::built_on_one_tick`).
//! - `node_b` always wins, by construction. `node_a`'s driver is held back
//!   until `node_b` reports `RollCall`, so `node_b`'s call has been started
//!   and published before `node_a` is stepped at all. `node_a`, stale by
//!   then, either answers that call first, and so never starts its own, or
//!   ticks first and starts its own for the same term. In that case the
//!   ranking decides, and `node_a`'s wall clock reads ahead of `node_b`'s
//!   (`WallClockAhead`), so its call ranks worse.
//! - That makes the leader the node `node_a` knows only through an inbound
//!   connection, which is the case the leader's address has to be right for
//!   (see the connectivity check in the first test).
//! - `node_c` is a bare `Net` seeded with only
//!   `node_a`'s listen address, so `node_a` answers its JOIN by pointing at
//!   `node_b`.
//!
//! `node_a` and `node_b` keep driving (via `run_driver`, including its
//! join-request-answering side — see `kabudachi_net::driver`) while `node_c`
//! joins: `tokio::select!` polls all three concurrently in one task.


use std::time::Duration as StdDuration;

use kabudachi_core::configuration::{Configuration, Generation, Single};
use kabudachi_core::election::{
    ElectionTimings, Entry, Identity, Input, KnownConfiguration, WorkerNode,
};
use kabudachi_core::protocol::ids::{IncarnationId, ShardId, Uuid7Ids, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::Scheduler;
use kabudachi_core::time::{Clock, Duration, RealClock};
use kabudachi_net::driver::{DriverConfig, run_driver};
use kabudachi_net::messenger::Net;
use tokio::sync::watch;
use tokio::time::timeout;

use crate::support::election::{WallClockAhead, built_on_one_tick, due_now};
use crate::support::net::{
    ask_until_pointed_at_a_leader, listening_net, take_inputs_until, wait_until_registered,
    wait_until_subscribed,
};

const SHARD: &str = "shard-1";

/// Every node's suspicion timeout. Comfortably above loopback connect
/// and gossip-subscription time.
const SUSPECT_TIMEOUT_MS: u64 = 300;

/// How far ahead `node_a`'s wall clock reads, so that if it starts a roll
/// call of its own, the call ranks worse than `node_b`'s: far more than any
/// gap between the instants the two start theirs.
const NODE_A_WALL_CLOCK_AHEAD_MS: u64 = 60_000;

/// How often a follower heartbeats its leader: well inside every suspicion
/// timeout this file uses.
const HEARTBEAT_INTERVAL_MS: u64 = 10;

/// How long a roll call runs: well above the time a roll call takes to
/// reach a loopback peer and its reply to come back.
const ROLL_CALL_DEADLINE_MS: u64 = 100;

/// How long `node_c`'s join is allowed to take once it starts.
const JOIN_TIMEOUT: StdDuration = StdDuration::from_secs(10);

/// Generous whole-test backstop: actual convergence plus a join is expected in well under a
/// second; this is a "something is actually broken" ceiling, not the
/// expected runtime.
const TEST_TIMEOUT: StdDuration = StdDuration::from_secs(30);

/// `my_id`'s node, a voter of a configuration of `voter_count`.
fn make_active_node<C: Clock>(clock: C, my_id: WorkerId, voter_count: usize) -> WorkerNode<C> {
    WorkerNode::start(
        Identity {
            id: my_id.clone(),
            incarnation: IncarnationId::new(format!("{}-incarnation-0", my_id.as_str())),
            shard: ShardId::new(SHARD),
            timings: ElectionTimings::new(
                Duration::from_millis(SUSPECT_TIMEOUT_MS),
                Duration::from_millis(HEARTBEAT_INTERVAL_MS),
            )
            .with_roll_call_deadline(Duration::from_millis(ROLL_CALL_DEADLINE_MS)),
        },
        Entry::Known(KnownConfiguration {
            configuration: Configuration::single(Single {
                generation: Generation::genesis(0),
                base: Generation::genesis(0),
                voter_count,
            }).expect("valid"),
            admission: Some(Generation::genesis(0)),
        }),
        clock,
        None,
    )
    .0
}

/// Blocks until `rx_a`/`rx_b` report exactly one of {Leader, Active} in the
/// pattern where a follower that answered the winner's roll call stays
/// `Active`, and one that started a roll call of its own returns to `Active`
/// on the winner's ack.
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_third_node_joins_a_converged_two_member_shard_as_a_pending_member_of_its_leader() {
    let net_a = Net::new();
    let net_b = Net::new();

    // Connect first (a node starts its leader-contact timer when built), then
    // construct both WorkerNodes on one tick (`built_on_one_tick`). See this file's "Topology" doc
    // for why node_a's wall clock reads ahead.
    // `net_a` listens and `net_b` dials it. Neither net's inputs are taken:
    // the nodes built afterwards are fed them. `listen_addr_a` doubles as
    // `node_c`'s seed address later in this test.
    let listen_addr_a = timeout(
        TEST_TIMEOUT,
        net_a.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
    )
    .await
    .expect("net_a produced a listen address within the timeout");
    net_b.dial(listen_addr_a.clone());
    let (worker_a, worker_b) = (net_a.local_worker_id(), net_b.local_worker_id());
    wait_until_registered(&net_a, &worker_b).await;
    wait_until_registered(&net_b, &worker_a).await;
    // net_b starts listening only *after* its outbound connection to net_a is
    // up. Nothing orders a real worker's listener against its outbound seed
    // dial either, so this is a legitimate ordering, not a contrived one.
    timeout(
        TEST_TIMEOUT,
        net_b.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
    )
    .await
    .expect("net_b produced a listen address within the timeout");

    // run_driver subscribes its net to the shard, but node_a's driver is
    // held back until node_b's roll call is out (see the gate below), so
    // both subscribe here: node_b's call must reach net_a, which queues it
    // for node_a.
    let shard = ShardId::new(SHARD);
    net_a.subscribe_to_shard(&shard);
    net_b.subscribe_to_shard(&shard);
    wait_until_subscribed(&net_a, &[&worker_b]).await;
    wait_until_subscribed(&net_b, &[&worker_a]).await;

    // One monotonic clock for every node and scheduler (see run_driver's doc).
    let clock = RealClock::new();
    let clock_a = WallClockAhead {
        clock,
        ahead_by_millis: NODE_A_WALL_CLOCK_AHEAD_MS,
    };
    let (mut node_a, mut node_b) = built_on_one_tick(&clock, || {
        (
            make_active_node(clock_a, worker_a.clone(), 2),
            make_active_node(clock, worker_b.clone(), 2),
        )
    });

    let (tx_a, rx_a) = watch::channel(node_a.state());
    let (tx_b, rx_b) = watch::channel(node_b.state());
    let mut node_b_calling = rx_b.clone();

    // This test only exercises the bootstrap join, not claim arbitration,
    // but every driven node carries a Scheduler regardless — see
    // run_driver's doc.
    let mut scheduler_a = Scheduler::new(clock_a, Uuid7Ids);
    let mut scheduler_b = Scheduler::new(clock, Uuid7Ids);

    let net_c = Net::new();

    // Only the third branch (converge, then join) is expected to complete;
    // node_a must still be driving to answer node_c's JOIN_REQUEST.
    let (converged, pointer) = timeout(TEST_TIMEOUT, async {
        tokio::select! {
            // The gate: node_b's roll call is started before node_a is
            // stepped, so node_b wins (see this file's "Topology" doc).
            // node_b's call stays open until its deadline, long after
            // node_a's reply reaches it over loopback.
            _ = async {
                node_b_calling
                    .wait_for(|state| *state == WorkerState::RollCall)
                    .await
                    .expect("node_b's driver task is still running");
                run_driver(
                    &mut node_a,
                    due_now(&clock_a),
                    &net_a,
                    &mut scheduler_a,
                    clock_a,
                    None,
                    DriverConfig::default(),
                    |node, _, _| { let _ = tx_a.send(node.state()); },
                )
                .await
            } => {
                unreachable!("run_driver never returns")
            }
            _ = run_driver(
                &mut node_b,
                due_now(&clock),
                &net_b,
                &mut scheduler_b,
                clock,
                None,
                DriverConfig::default(),
                |node, _, _| { let _ = tx_b.send(node.state()); },
            ) => {
                unreachable!("run_driver never returns")
            }
            joined = async {
                let converged = wait_for_convergence(rx_a, rx_b).await;
                let pointer = ask_until_pointed_at_a_leader(
                    &net_c,
                    std::slice::from_ref(&listen_addr_a),
                    JOIN_TIMEOUT,
                )
                .await;
                (converged, pointer)
            } => joined,
        }
    })
    .await
    .expect("A/B converged and node_c's join completed within the test timeout");

    assert_eq!(
        converged,
        (WorkerState::Active, WorkerState::Leader),
        "node_b's roll call is started first, so node_b wins"
    );
    assert_eq!(pointer.leader_id(), Some(worker_b.clone()));
    assert_eq!(pointer.term, node_b.term());
    assert_eq!(pointer.recovery_epoch, node_b.recovery_epoch());

    // The id above only proves node_c was *told* who leads — nothing about
    // whether the address it was told is one anything can dial.
    // `ask_for_leader` dials the leader it is pointed at, so an actual
    // connection to node_b is the end-to-end check of that, and it matters
    // precisely here: net_b dials net_a, so from net_a's
    // side node_b is a `ConnectedPoint::Listener`, whose remote address is
    // node_b's ephemeral source address rather than anything node_b listens
    // on. Only Identify gives net_a node_b's real listen address to hand
    // out (see `kabudachi_net::messenger`'s "Where a peer's address comes
    // from").
    //
    // net_b's dial leaves from a port of its own, never from its listen port
    // (see `kabudachi_net::swarm`'s `NewPortTcp`), so its source address is
    // never an address it listens on, as for a multi-homed or NATed host:
    // an un-dialable address handed out here would not go unnoticed.
    // node_c is never driven, so net_c's inputs are the test's to take.
    take_inputs_until(&net_c, &Input::PeerConnected(worker_b.clone())).await;
}

/// A responder that knows no leader — here `node_a`, one of two voters whose
/// other voter never answers, so it can never elect one — answers "no
/// leader known", and the joiner moves on to its next seed rather than
/// adopting it. `node_x` is the one voter of its shard and elects itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_joiner_passes_over_a_seed_that_knows_no_leader() {
    let (net_a, listen_addr_a) = listening_net().await;
    let (net_x, listen_addr_x) = listening_net().await;
    let worker_a = net_a.local_worker_id();
    let worker_x = net_x.local_worker_id();

    // One clock for every node and scheduler (see run_driver's doc).
    let clock = RealClock::new();
    let mut node_a = make_active_node(clock, worker_a, 2);
    let mut node_x = make_active_node(clock, worker_x.clone(), 1);
    let (tx_x, mut rx_x) = watch::channel(node_x.state());
    let mut scheduler_a = Scheduler::new(clock, Uuid7Ids);
    let mut scheduler_x = Scheduler::new(clock, Uuid7Ids);

    let net_c = Net::new();

    let pointer = timeout(TEST_TIMEOUT, async {
        tokio::select! {
            _ = run_driver(
                &mut node_a,
                due_now(&clock),
                &net_a,
                &mut scheduler_a,
                clock,
                None,
                DriverConfig::default(),
                |_, _, _| {},
            ) => {
                unreachable!("run_driver never returns")
            }
            _ = run_driver(
                &mut node_x,
                due_now(&clock),
                &net_x,
                &mut scheduler_x,
                clock,
                None,
                DriverConfig::default(),
                |node, _, _| { let _ = tx_x.send(node.state()); },
            ) => {
                unreachable!("run_driver never returns")
            }
            pointer = async {
                while *rx_x.borrow() != WorkerState::Leader {
                    rx_x.changed().await.expect("node_x's driver task is still running");
                }
                ask_until_pointed_at_a_leader(
                    &net_c,
                    &[listen_addr_a, listen_addr_x.clone()],
                    JOIN_TIMEOUT,
                )
                .await
            } => pointer,
        }
    })
    .await
    .expect("node_x elected itself and node_c's join completed within the test timeout");

    assert_eq!(pointer.leader_id(), Some(worker_x));
    assert_eq!(pointer.leader_multiaddr, listen_addr_x.to_string());
}
