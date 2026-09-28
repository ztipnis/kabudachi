//! A surviving leader notices its followers dropping out from under it one
//! at a time, over real sockets, while it stays fully up itself: with one of
//! its two followers gone it keeps its quorum (quorum(3) = 2 is still met by
//! itself plus the other), and with both gone it demotes itself to
//! `NoQuorum`.
//!
//! The leader notices through heartbeats, not connections: a follower that
//! stops heartbeating confirms none of its acks, and once too few followers
//! confirm them, its quorum-contact lease runs out.
//!
//! A dropped follower here is a stopped one: its driver stops, so it sends
//! the leader nothing more, and the leader closes its connection to it.
//! Closing the connection alone would not drop it: a follower that keeps
//! running keeps heartbeating its leader, and a heartbeat to a peer it holds
//! no connection to dials that peer again.
//!
//! ## Topology
//!
//! One configuration of three voters, `a`, `b` and `c`, each pre-seeded via
//! `WorkerNode::new` (not the join cascade: a joiner is a pending member no
//! quorum counts, so a chained join would leave the shard at its bootstrap
//! configuration rather than the three voters this test needs to agree on
//! quorum math). All three are built on one clock tick with one suspicion
//! timeout, as in `two_node_election_test.rs`; the jitter on each node's
//! timeout decides which suspects first and elects itself, and the test
//! follows whichever node that is.

mod support;

use std::time::Duration as StdDuration;

use kabudachi_core::configuration::{Configuration, Generation, Single};
use kabudachi_core::election::{ElectionTimings, KnownConfiguration, Output, WorkerNode};
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
use support::net::connect_full_mesh;

const SHARD: &str = "shard-1";

/// Every node's suspicion timeout, and so the leader's lease's (see this
/// file's module doc).
const SUSPECT_TIMEOUT_MS: u64 = 300;

/// Slack for a driver that runs late on a loaded host, added to a wait that
/// outlasts a leader's lease so the wait still does. Every swarm now also
/// runs `kad` (peer routing): a small shard's node count sits under
/// Kademlia's own bucket-size target, so each newly connected peer can
/// retrigger a routing-table bootstrap sweep across many buckets, adding
/// real, if brief, contention on the same single-threaded event loop that
/// processes this test's heartbeats and disconnects. Measured directly: the
/// original 90ms let this test's first assertion fail on a loaded host at a
/// real, non-negligible rate; 2000ms did not, in the same repeated runs,
/// though a host loaded heavily enough (this one, briefly, mid-measurement)
/// can still turn up a failure at any finite slack — that ceiling is this
/// file's own, not new here.
const LATE_DRIVER_SLACK_MS: u64 = 2000;

/// How often a follower heartbeats its leader: well inside every suspicion
/// timeout this file uses.
const HEARTBEAT_INTERVAL_MS: u64 = 10;

/// How long a roll call runs: well above the time a roll call takes to
/// reach a loopback peer and its reply to come back.
const ROLL_CALL_DEADLINE_MS: u64 = 100;

const TEST_TIMEOUT: StdDuration = StdDuration::from_secs(10);

type Node = WorkerNode<RealClock>;

/// `my_id`'s node, one of the three voters of the shard's configuration.
fn make_node(clock: RealClock, my_id: WorkerId) -> Node {
    WorkerNode::new(
        my_id.clone(),
        IncarnationId::new(format!("{}-incarnation-0", my_id.as_str())),
        ShardId::new(SHARD),
        clock,
        KnownConfiguration {
            configuration: Configuration::single(Single {
                generation: Generation::genesis(0),
                base: Generation::genesis(0),
                voter_count: 3,
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

/// Blocks until `rxs` report exactly one `Leader` and two `Active` among the
/// three nodes.
async fn wait_for_convergence(rxs: &[watch::Receiver<WorkerState>]) -> Vec<WorkerState> {
    loop {
        let states: Vec<WorkerState> = rxs.iter().map(|rx| *rx.borrow()).collect();
        let leaders = states.iter().filter(|s| **s == WorkerState::Leader).count();
        let actives = states.iter().filter(|s| **s == WorkerState::Active).count();
        if leaders == 1 && actives == 2 {
            return states;
        }
        tokio::time::sleep(StdDuration::from_millis(5)).await;
    }
}

#[tokio::test]
async fn a_leader_keeps_its_quorum_through_one_dropped_follower_and_loses_it_with_both() {
    // ---- Setup: 3 nodes, full mesh, one shared configuration of 3 ----
    let net_a = Net::new(build_swarm(Keypair::generate_ed25519()));
    let net_b = Net::new(build_swarm(Keypair::generate_ed25519()));
    let net_c = Net::new(build_swarm(Keypair::generate_ed25519()));
    let nets = [&net_a, &net_b, &net_c];
    let ids = connect_full_mesh(&nets).await;

    // One clock for every node and scheduler (see run_driver's doc), and
    // every node built on one tick of it (see the module doc).
    let clock = RealClock::new();
    let mut nodes = built_on_one_tick(&clock, || {
        [0, 1, 2].map(|i| make_node(clock, ids[i].clone()))
    });
    let channels = nodes.each_ref().map(|node| watch::channel(node.state()));
    let txs = channels.each_ref().map(|(tx, _)| tx.clone());
    let rxs = channels.map(|(_, rx)| rx);
    let mut schedulers = [(); 3].map(|()| Scheduler::new(clock, Uuid7Ids));

    // ---- Phase 1: converge to exactly one Leader, two Active ----
    let states = timeout(
        TEST_TIMEOUT,
        drive_until(
            &mut nodes,
            nets,
            &mut schedulers,
            clock,
            &txs,
            [true; 3],
            wait_for_convergence(&rxs),
        ),
    )
    .await
    .expect("all 3 nodes converged to exactly one Leader and two Active followers");

    let leader_index = states
        .iter()
        .position(|s| *s == WorkerState::Leader)
        .expect("wait_for_convergence guarantees exactly one Leader");
    let follower_indices: Vec<usize> = (0..3).filter(|i| *i != leader_index).collect();
    let leader_id = ids[leader_index].clone();
    let leader_net = nets[leader_index];
    let follower1_id = ids[follower_indices[0]].clone();
    let follower2_id = ids[follower_indices[1]].clone();
    eprintln!(
        "phase 1: {leader_id:?} elected leader; followers = {follower1_id:?}, {follower2_id:?}"
    );

    // ---- Phase 2: drop ONE follower. Its driver stops, so it sends the
    // ---- leader nothing more, and the leader closes its connection to it.
    // ---- The leader must stay Leader: quorum(3) = 2 is still met by itself
    // ---- plus the follower that keeps heartbeating.
    let mut driven = [true; 3];
    driven[follower_indices[0]] = false;
    leader_net.disconnect(follower1_id.clone());
    let mut rx_leader_state = rxs[leader_index].clone();

    timeout(
        TEST_TIMEOUT,
        drive_until(
            &mut nodes,
            nets,
            &mut schedulers,
            clock,
            &txs,
            driven,
            async {
                // Wait out a whole suspicion timeout of the leader's: its lease
                // lasts at most nine tenths of one, so anything the dropped
                // follower confirmed before it stopped has lapsed by then, and
                // only the remaining follower can be keeping its quorum.
                tokio::time::sleep(StdDuration::from_millis(
                    SUSPECT_TIMEOUT_MS + LATE_DRIVER_SLACK_MS,
                ))
                .await;
                assert_eq!(
                    *rx_leader_state.borrow_and_update(),
                    WorkerState::Leader,
                    "the leader must remain Leader after losing exactly one of two followers \
                     — quorum(3) = 2 is still met by itself plus the one remaining follower"
                );
            },
        ),
    )
    .await
    .expect("the leader stayed Leader through the first drop");

    // ---- Phase 3: drop the SECOND follower too. With no follower left to
    // ---- keep its quorum, the leader must demote itself to NoQuorum.
    driven[follower_indices[1]] = false;
    leader_net.disconnect(follower2_id.clone());

    timeout(
        TEST_TIMEOUT,
        drive_until(
            &mut nodes,
            nets,
            &mut schedulers,
            clock,
            &txs,
            driven,
            async {
                rx_leader_state
                    .wait_for(|s| *s == WorkerState::NoQuorum)
                    .await
                    .expect("the leader's driver task is still running");
            },
        ),
    )
    .await
    .expect("the leader transitioned to NoQuorum on the second drop within the timeout");
}

/// Runs the driver of each node whose `driven` flag is set until `until`
/// completes, and returns what it returned. A node left out is frozen, as a
/// stopped worker would be: it sends nothing, heartbeats included, so
/// nothing it sends can redial the leader that dropped it.
async fn drive_until<T>(
    nodes: &mut [Node; 3],
    nets: [&Net; 3],
    schedulers: &mut [Scheduler<RealClock, Uuid7Ids>; 3],
    clock: RealClock,
    txs: &[watch::Sender<WorkerState>; 3],
    driven: [bool; 3],
    until: impl Future<Output = T>,
) -> T {
    let publisher = |i: usize| {
        let tx = txs[i].clone();
        move |node: &Node, _: &[Output]| {
            let _ = tx.send(node.state());
        }
    };
    let [node_a, node_b, node_c] = nodes;
    let [scheduler_a, scheduler_b, scheduler_c] = schedulers;
    tokio::select! {
        _ = run_driver(node_a, nets[0], scheduler_a, clock, None, publisher(0)),
            if driven[0] => unreachable!(),
        _ = run_driver(node_b, nets[1], scheduler_b, clock, None, publisher(1)),
            if driven[1] => unreachable!(),
        _ = run_driver(node_c, nets[2], scheduler_c, clock, None, publisher(2)),
            if driven[2] => unreachable!(),
        output = until => output,
    }
}
