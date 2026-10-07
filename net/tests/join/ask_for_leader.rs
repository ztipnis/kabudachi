//! A joiner asks its peers who leads the shard, over real sockets, and ends
//! up connected to a leader it can reach: the newest one the peers point at,
//! the one its floor accepts, not one that cannot be reached, and past a seed
//! that never answers.

use std::sync::Arc;
use std::time::Duration;

use kabudachi_core::coordination_authority::RecoveryEpoch;
use kabudachi_core::election::JoinFloor;
use kabudachi_core::protocol::ids::WorkerId;
use kabudachi_core::protocol::messages::JoinResponse;
use kabudachi_net::join::{LeaderSearch, ask_for_leader};
use kabudachi_net::messenger::Net;
use libp2p::{Multiaddr, identity};
use tokio::time::timeout;

use crate::support::net::{JoinResponder, listening_net, pointer_to};

const TEST_TIMEOUT: Duration = Duration::from_secs(20);
const PER_PEER_TIMEOUT: Duration = Duration::from_secs(5);
/// Far longer than a loopback answer takes, so a slow host never cuts a pass
/// short.
const GRACE: Duration = Duration::from_secs(5);

fn pointer_at(
    leader: &WorkerId,
    at: &Multiaddr,
    recovery_epoch: u64,
    term: u64,
) -> JoinResponse {
    JoinResponse {
        recovery_epoch,
        term,
        ..pointer_to(leader, at)
    }
}

/// A worker that is nowhere: the id of a keypair no `Net` was built from.
fn worker_that_never_runs() -> WorkerId {
    let peer = identity::Keypair::generate_ed25519().public().to_peer_id();
    WorkerId::new(peer.to_string())
}

/// Where nothing listens, so dialing it fails to connect.
fn nowhere() -> Multiaddr {
    "/ip4/127.0.0.1/tcp/1".parse().unwrap()
}

async fn search(joiner: &Net, seeds: &[Multiaddr], floor: JoinFloor) -> LeaderSearch {
    timeout(
        TEST_TIMEOUT,
        ask_for_leader(joiner, seeds, floor, PER_PEER_TIMEOUT, GRACE),
    )
    .await
    .expect("the search completed within the timeout")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_newest_pointer_among_the_peers_is_the_one_found() {
    // Each pair: what the first seed points at, what the second points at.
    // The second is newer each time: a later epoch whatever the term, or a
    // later term at the same epoch.
    for ((first_epoch, first_term), (second_epoch, second_term)) in
        [((0, 5), (1, 1)), ((2, 2), (2, 3))]
    {
        let (net_a, addr_a) = listening_net().await;
        let (net_b, addr_b) = listening_net().await;
        let joiner = Net::new();
        let (worker_a, worker_b) = (net_a.local_worker_id(), net_b.local_worker_id());
        let older = pointer_at(&worker_a, &addr_a, first_epoch, first_term);
        let newer = pointer_at(&worker_b, &addr_b, second_epoch, second_term);
        let _responder_a = JoinResponder::start(Arc::new(net_a), Some(older));
        let _responder_b = JoinResponder::start(Arc::new(net_b), Some(newer.clone()));

        let found = search(&joiner, &[addr_a, addr_b], JoinFloor::none()).await;

        assert_eq!(found, LeaderSearch::Found(newer));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_pointer_the_floor_accepts_beats_a_higher_term_of_another_lineage() {
    // The floor is epoch 5 of lineage 1. Seed A, listed first, points at
    // epoch 5 of lineage 2 at a much later term: a leader the floor
    // refuses. Seed B points at the floor's own lineage.
    let (net_a, addr_a) = listening_net().await;
    let (net_b, addr_b) = listening_net().await;
    let joiner = Net::new();
    let (worker_a, worker_b) = (net_a.local_worker_id(), net_b.local_worker_id());
    let other_lineage = JoinResponse {
        recovery_epoch_lineage: 2,
        ..pointer_at(&worker_a, &addr_a, 5, 10)
    };
    let own_lineage = JoinResponse {
        recovery_epoch_lineage: 1,
        ..pointer_at(&worker_b, &addr_b, 5, 1)
    };
    let _responder_a = JoinResponder::start(Arc::new(net_a), Some(other_lineage));
    let _responder_b = JoinResponder::start(Arc::new(net_b), Some(own_lineage.clone()));

    let found = search(&joiner, &[addr_a, addr_b], JoinFloor::at(RecoveryEpoch::new(5, 1))).await;

    assert_eq!(found, LeaderSearch::Found(own_lineage));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_leader_that_cannot_be_reached_is_passed_over_for_one_that_can() {
    let (net_a, addr_a) = listening_net().await;
    let (net_b, addr_b) = listening_net().await;
    let (_net_z, addr_z) = listening_net().await;
    let joiner = Net::new();
    let worker_b = net_b.local_worker_id();

    // Seed A names a leader that never runs, at an address where some other
    // worker (_net_z) answers: the dial connects, but not to that leader.
    let _responder_a = JoinResponder::start(
        Arc::new(net_a),
        Some(pointer_to(&worker_that_never_runs(), &addr_z)),
    );
    let response_b = pointer_to(&worker_b, &addr_b);
    let _responder_b = JoinResponder::start(Arc::new(net_b), Some(response_b.clone()));

    let found = search(&joiner, &[addr_a, addr_b], JoinFloor::none()).await;

    assert_eq!(found, LeaderSearch::Found(response_b));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_seed_that_never_answers_does_not_stop_the_next_seed_being_heard() {
    let (net_a, addr_a) = listening_net().await;
    let joiner = Net::new();
    let response = pointer_to(&net_a.local_worker_id(), &addr_a);
    let _responder = JoinResponder::start(Arc::new(net_a), Some(response.clone()));

    let found = search(&joiner, &[nowhere(), addr_a], JoinFloor::none()).await;

    assert_eq!(found, LeaderSearch::Found(response));
}
