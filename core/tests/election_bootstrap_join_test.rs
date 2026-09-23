//! Chunk C4: `WorkerNode::bootstrapping` + `WorkerNode::finish_joining`, the
//! core-side half of the bootstrap join protocol (README §27 Phase 2). The
//! wire handshake that resolves a membership list (dialing seeds, sending
//! `JOIN_REQUEST`, taking the first `JOIN_RESPONSE`) is `net`'s concern
//! (`net/tests/three_node_join_test.rs` covers that end to end); these tests
//! only exercise the plain state-machine surface `net`'s driver calls once it
//! has a resolved membership.

mod support;

use std::collections::BTreeSet;

use kabudachi_core::election::WorkerNode;
use kabudachi_core::membership::RingMembership;
use kabudachi_core::protocol::ids::{IncarnationId, ShardId, WorkerId};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::Duration;

use support::clock::FakeClock;
use support::coordination_authority::FakeCoordinationAuthority;
use support::network::FakeNetwork;

const SHARD: &str = "shard-1";
const SUSPECT_TIMEOUT_TICKS: u64 = 10;

fn worker(id: &str) -> WorkerId {
    WorkerId::new(id)
}

#[allow(clippy::type_complexity)]
fn bootstrapping_node(
    id: WorkerId,
    clock: &FakeClock,
    network: &FakeNetwork,
) -> WorkerNode<FakeClock, FakeNetwork, RingMembership, FakeCoordinationAuthority> {
    network.register(id.clone());
    WorkerNode::bootstrapping(
        id.clone(),
        IncarnationId::new(format!("{}-incarnation-0", id.as_str())),
        ShardId::new(SHARD),
        clock.clone(),
        network.clone(),
        RingMembership::new(BTreeSet::new()),
        FakeCoordinationAuthority::new(),
        Duration::from_ticks(SUSPECT_TIMEOUT_TICKS),
    )
}

#[allow(clippy::type_complexity)]
fn active_node(
    id: WorkerId,
    electorate: &BTreeSet<WorkerId>,
    clock: &FakeClock,
    network: &FakeNetwork,
) -> WorkerNode<FakeClock, FakeNetwork, RingMembership, FakeCoordinationAuthority> {
    network.register(id.clone());
    WorkerNode::new(
        id.clone(),
        IncarnationId::new(format!("{}-incarnation-0", id.as_str())),
        ShardId::new(SHARD),
        clock.clone(),
        network.clone(),
        RingMembership::new(electorate.clone()),
        FakeCoordinationAuthority::new(),
        Duration::from_ticks(SUSPECT_TIMEOUT_TICKS),
    )
}

#[test]
fn bootstrapping_constructs_a_node_in_the_bootstrapping_state() {
    let clock = FakeClock::new();
    let network = FakeNetwork::new(std::rc::Rc::new(clock.clone()));

    let node = bootstrapping_node(worker("joiner"), &clock, &network);

    assert_eq!(node.state(), WorkerState::Bootstrapping);
    assert_eq!(node.electorate(), BTreeSet::new());
}

#[test]
fn finish_joining_drives_bootstrapping_to_active_with_self_included() {
    let clock = FakeClock::new();
    let network = FakeNetwork::new(std::rc::Rc::new(clock.clone()));
    let mut node = bootstrapping_node(worker("joiner"), &clock, &network);

    let discovered: BTreeSet<WorkerId> = [worker("a"), worker("b")].into_iter().collect();
    node.finish_joining(discovered);

    assert_eq!(node.state(), WorkerState::Active);
    assert_eq!(
        node.electorate(),
        [worker("joiner"), worker("a"), worker("b")]
            .into_iter()
            .collect(),
        "the joining node's own id must be part of its adopted electorate, matching the \
         effective_electorate convention every other node relies on (see WorkerNode::tick_as_leader)"
    );
}

#[test]
fn finish_joining_is_a_noop_outside_bootstrapping() {
    let clock = FakeClock::new();
    let network = FakeNetwork::new(std::rc::Rc::new(clock.clone()));
    let electorate: BTreeSet<WorkerId> = [worker("a"), worker("b")].into_iter().collect();
    let mut node = active_node(worker("a"), &electorate, &clock, &network);

    node.finish_joining([worker("c")].into_iter().collect());

    assert_eq!(node.state(), WorkerState::Active);
    assert_eq!(
        node.electorate(),
        electorate,
        "finish_joining must not touch the membership of a node that was never Bootstrapping"
    );
}

#[test]
fn tick_does_not_move_a_bootstrapping_node_even_past_the_suspicion_timeout() {
    let clock = FakeClock::new();
    let network = FakeNetwork::new(std::rc::Rc::new(clock.clone()));
    let mut node = bootstrapping_node(worker("joiner"), &clock, &network);

    clock.advance(Duration::from_ticks(SUSPECT_TIMEOUT_TICKS * 5));
    for _ in 0..5 {
        node.tick();
    }

    assert_eq!(
        node.state(),
        WorkerState::Bootstrapping,
        "tick() is a deliberate no-op in Bootstrapping/Joining (core/src/election.rs); only \
         finish_joining moves a bootstrapping node forward"
    );
}

#[test]
fn finish_joining_resets_the_leader_contact_timer_so_the_new_follower_is_not_immediately_suspect() {
    let clock = FakeClock::new();
    let network = FakeNetwork::new(std::rc::Rc::new(clock.clone()));
    let mut node = bootstrapping_node(worker("joiner"), &clock, &network);

    // If finish_joining did not reset last_leader_contact, this much elapsed
    // time before joining would make the node suspect the instant it ticks.
    clock.advance(Duration::from_ticks(SUSPECT_TIMEOUT_TICKS * 5));
    node.finish_joining([worker("a")].into_iter().collect());
    node.tick();

    assert_eq!(node.state(), WorkerState::Active);
}

#[test]
fn a_freshly_joined_node_still_becomes_leader_suspect_once_its_own_timeout_elapses() {
    let clock = FakeClock::new();
    let network = FakeNetwork::new(std::rc::Rc::new(clock.clone()));
    let mut node = bootstrapping_node(worker("joiner"), &clock, &network);

    node.finish_joining([worker("a")].into_iter().collect());
    clock.advance(Duration::from_ticks(SUSPECT_TIMEOUT_TICKS + 1));
    node.tick();

    assert_eq!(node.state(), WorkerState::LeaderSuspect);
}
