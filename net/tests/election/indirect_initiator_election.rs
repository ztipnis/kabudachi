//! An election whose winner reaches one of its voters only through the
//! gossip mesh, over real libp2p swarms and real loopback TCP sockets.
//!
//! ## Topology
//!
//! A line of three swarms, `a` - `b` - `c`: `a` and `c` each connect to `b`
//! and never to each other. All three subscribe to the shard's gossip topic,
//! so `b` relays what either end publishes to the other.
//!
//! - `node_a` and `node_c` are two voters of one configuration of three,
//!   driven by `run_driver`: built on one clock tick with one suspicion
//!   timeout (see `support::election::built_on_one_tick`).
//! - `b` is the third voter's swarm, but no node is driven on it: it relays
//!   gossip and answers nothing. Whichever of `node_a` and `node_c` wins,
//!   its returning quorum of two is therefore itself and the other.
//!
//! ## Why either winner will do
//!
//! Which roll call wins depends on the jitter on the two nodes' suspicion
//! timeouts, the wall clock, the two `WorkerId`s and which call reaches the
//! other node first, so the test does not name the winner. It does not need
//! to: the two ends are symmetric. The loser hears the winner's roll call
//! only through `b`, and answers it with a direct reply to a peer it holds no
//! connection to. That reply arrives only if the loser dials the winner at
//! the address the winner stamped on its roll call (see
//! `kabudachi_net::messenger`'s "Where a peer's address comes from"). So any
//! winner proves the stamp was recorded and dialed, and the loser, now its
//! follower, holds the winner's listen address.


use std::time::Duration as StdDuration;

use crate::support::net::driven_scheduler;
use kabudachi_core::configuration::{Configuration, Generation, Single};
use kabudachi_core::election::{ElectionTimings, Entry, Identity, KnownConfiguration, WorkerNode};
use kabudachi_core::protocol::ids::{IncarnationId, ShardId, WorkerId};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::{Duration, RealClock};
use kabudachi_net::driver::{DriverConfig, run_driver};
use kabudachi_net::messenger::Net;
use libp2p::Multiaddr;
use tokio::sync::watch;
use tokio::time::timeout;

use crate::support::election::{built_on_one_tick, due_now};
use crate::support::net::{wait_until_registered, wait_until_subscribed};

const SHARD: &str = "shard-1";

/// Both driven nodes' suspicion timeout. Comfortably above
/// loopback connect and gossip-subscription time.
const SUSPECT_TIMEOUT_MS: u64 = 300;

/// How often a follower heartbeats its leader: well inside the suspicion
/// timeout.
const HEARTBEAT_INTERVAL_MS: u64 = 10;

/// How long a roll call runs: well above the time a roll call takes to
/// reach a loopback peer and its reply to come back.
const ROLL_CALL_DEADLINE_MS: u64 = 100;

/// A "something is actually broken" backstop; convergence is expected just
/// after `SUSPECT_TIMEOUT_MS`.
const TEST_TIMEOUT: StdDuration = StdDuration::from_secs(20);

fn new_net() -> Net {
    Net::new()
}

async fn listen(net: &Net) -> Multiaddr {
    timeout(
        TEST_TIMEOUT,
        net.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
    )
    .await
    .expect("the net produced a listen address within the timeout")
}

/// `my_id`'s node, one of the three voters of the shard's configuration.
fn make_node(clock: RealClock, my_id: WorkerId) -> WorkerNode<RealClock> {
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
                voter_count: 3,
            }).expect("valid"),
            admission: Some(Generation::genesis(0)),
        }),
        clock,
        None,
    )
    .0
}

/// A driven node's state, with the leader it knows of.
type Seen = (WorkerState, Option<WorkerId>);

fn seen(node: &WorkerNode<RealClock>) -> Seen {
    (node.state(), node.known_leader().map(|(leader, _)| leader))
}

/// Blocks until one of `rx_a` and `rx_c` reports `Leader` and the other
/// `Active` under a known leader, and returns the two states. A node that
/// answered the winner's roll call while still `Active` stays `Active`
/// throughout, so its state alone does not show that the winner's ack has
/// reached it.
async fn wait_for_convergence(
    mut rx_a: watch::Receiver<Seen>,
    mut rx_c: watch::Receiver<Seen>,
) -> (WorkerState, WorkerState) {
    loop {
        let (a, c) = (rx_a.borrow().clone(), rx_c.borrow().clone());
        let following = |seen: &Seen| seen.0 == WorkerState::Active && seen.1.is_some();
        if (a.0 == WorkerState::Leader && following(&c))
            || (c.0 == WorkerState::Leader && following(&a))
        {
            return (a.0, c.0);
        }
        tokio::select! {
            result = rx_a.changed() => { result.expect("node_a's driver task is still running"); }
            result = rx_c.changed() => { result.expect("node_c's driver task is still running"); }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_initiator_reachable_only_through_the_mesh_collects_a_direct_reply_and_leads() {
    let (net_a, net_b, net_c) = (new_net(), new_net(), new_net());
    let addr_b = listen(&net_b).await;
    let addr_c = listen(&net_c).await;
    let addr_a = listen(&net_a).await;
    let (worker_a, worker_b, worker_c) = (
        net_a.local_worker_id(),
        net_b.local_worker_id(),
        net_c.local_worker_id(),
    );

    // Neither end's inputs are taken: the nodes built afterwards are fed
    // them.
    net_a.dial(addr_b.clone());
    net_c.dial(addr_b);
    wait_until_registered(&net_a, &worker_b).await;
    wait_until_registered(&net_c, &worker_b).await;
    wait_until_registered(&net_b, &worker_a).await;
    wait_until_registered(&net_b, &worker_c).await;

    let shard = ShardId::new(SHARD);
    for net in [&net_a, &net_b, &net_c] {
        net.subscribe_to_shard(&shard);
    }
    wait_until_subscribed(&net_a, &[&worker_b]).await;
    wait_until_subscribed(&net_b, &[&worker_a, &worker_c]).await;
    wait_until_subscribed(&net_c, &[&worker_b]).await;

    // Every connection records its peer's address, so an empty record means
    // the two ends have never connected.
    assert!(!net_a.diagnostics().await.peer_addresses.contains_key(&worker_c));
    assert!(!net_c.diagnostics().await.peer_addresses.contains_key(&worker_a));

    // One monotonic clock for every node and scheduler (see run_driver's
    // doc).
    let clock = RealClock::new();
    let (mut node_a, mut node_c) = built_on_one_tick(&clock, || {
        (
            make_node(clock, worker_a.clone()),
            make_node(clock, worker_c.clone()),
        )
    });
    let (tx_a, rx_a) = watch::channel(seen(&node_a));
    let (tx_c, rx_c) = watch::channel(seen(&node_c));
    let (last_a, last_c) = (rx_a.clone(), rx_c.clone());
    let mut scheduler_a = driven_scheduler(clock);
    let mut scheduler_c = driven_scheduler(clock);

    let converged = timeout(TEST_TIMEOUT, async {
        tokio::select! {
            _ = run_driver(
                &mut node_a,
                due_now(&clock),
                &net_a,
                &mut scheduler_a,
                clock,
                None,
                DriverConfig::default(),
                |node, _, _| { let _ = tx_a.send(seen(node)); },
            ) => {
                unreachable!("run_driver never returns")
            }
            _ = run_driver(
                &mut node_c,
                due_now(&clock),
                &net_c,
                &mut scheduler_c,
                clock,
                None,
                DriverConfig::default(),
                |node, _, _| { let _ = tx_c.send(seen(node)); },
            ) => {
                unreachable!("run_driver never returns")
            }
            states = wait_for_convergence(rx_a, rx_c) => states,
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "one of node_a and node_c should lead with the other following within the \
             timeout; last seen node_a {:?}, node_c {:?}",
            last_a.borrow(),
            last_c.borrow(),
        )
    });

    // The follower answered the leader's roll call by dialing it, so it
    // holds the leader's listen address (see this file's "Why either winner
    // will do").
    let (follower_net, follower_node_leader, leader, leader_addr) = match converged {
        (WorkerState::Active, WorkerState::Leader) => {
            (&net_a, node_a.known_leader(), worker_c, addr_c)
        }
        _ => (&net_c, node_c.known_leader(), worker_a, addr_a),
    };
    assert_eq!(follower_node_leader.map(|(id, _)| id), Some(leader.clone()));
    assert_eq!(follower_net.dialable_address(&leader).await, Some(leader_addr));
}
