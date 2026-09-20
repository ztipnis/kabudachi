mod support;

use support::builders::{shard, worker};

use std::collections::BTreeSet;
use std::rc::Rc;

use kabudachi_core::election::WorkerNode;
use kabudachi_core::membership::RingMembership;
use kabudachi_core::protocol::ids::{IncarnationId, WorkerId};
use kabudachi_core::protocol::messages::LeaderHeartbeatAck;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::Duration;
use support::clock::FakeClock;
use support::coordination_authority::FakeCoordinationAuthority;
use support::network::FakeNetwork;

const SHARD: &str = "shard-1";
const OTHER_SHARD: &str = "shard-2";

fn heartbeat_ack(shard_id: &str, recovery_epoch: u64, term: u64) -> LeaderHeartbeatAck {
    LeaderHeartbeatAck {
        shard_id: Some(shard(shard_id).into()),
        leader_id: Some(worker("leader-1").into()),
        recovery_epoch,
        term,
        membership_generation: 0,
    }
}

/// A node with its own network, membership and authority. These tests don't use
/// them; they exist to satisfy `WorkerNode`'s generics. `clock` is a shared
/// handle so the test keeps advancing it.
fn make_node(
    clock: &FakeClock,
    my_id: WorkerId,
    suspect_timeout: Duration,
) -> WorkerNode<FakeClock, FakeNetwork, RingMembership, FakeCoordinationAuthority> {
    let network = FakeNetwork::new(Rc::new(clock.clone()));
    network.register(my_id.clone());
    WorkerNode::new(
        my_id,
        IncarnationId::new("incarnation-1"),
        shard(SHARD),
        clock.clone(),
        network,
        RingMembership::new(BTreeSet::new()),
        FakeCoordinationAuthority::new(),
        suspect_timeout,
    )
}

// Verified through `tick()`: without the ack the node would suspect the leader
// by tick 15; with `last_leader_contact` bumped it must not. `WorkerNode` has
// no test-only accessor for it.
#[test]
fn valid_ack_advances_last_leader_contact_so_tick_does_not_suspect() {
    let clock = FakeClock::new();
    let suspect_timeout = Duration::from_ticks(10);
    let mut node = make_node(&clock, worker("w1"), suspect_timeout);

    // Receive a valid ack at tick 8.
    clock.advance(Duration::from_ticks(8));
    node.on_leader_ack(&heartbeat_ack(SHARD, 0, 0));

    // At tick 15 only 7 ticks have passed since the ack, so the node stays Active (from construction it would be 15 > 10).
    clock.advance(Duration::from_ticks(7));
    node.tick();

    assert_eq!(node.state(), WorkerState::Active);
}

// A fresh node's recovery_epoch is 0, so the stale-epoch branch can't be
// reached here (forced recovery raises it; see
// `election_forced_recovery_test.rs`). This covers the boundary: an equal epoch
// is accepted.
#[test]
fn matching_recovery_epoch_is_not_treated_as_stale() {
    let clock = FakeClock::new();
    let suspect_timeout = Duration::from_ticks(10);
    let mut node = make_node(&clock, worker("w1"), suspect_timeout);

    clock.advance(Duration::from_ticks(8));
    // Epoch 0 equals the node's own, so the ack is accepted.
    node.on_leader_ack(&heartbeat_ack(SHARD, 0, 0));

    clock.advance(Duration::from_ticks(7));
    node.tick();

    // Same differential proof as test 1.
    assert_eq!(node.state(), WorkerState::Active);
}

// Stale term is ignored: raise highest_term_seen to 5, then send term 3 with a
// fresh epoch and check via `tick()` that the stale ack did not bump
// last_leader_contact.
#[test]
fn stale_term_is_ignored() {
    let clock = FakeClock::new();
    let suspect_timeout = Duration::from_ticks(10);
    let mut node = make_node(&clock, worker("w1"), suspect_timeout);

    // Accepted: raises highest_term_seen to 5, last_leader_contact -> tick 1.
    clock.advance(Duration::from_ticks(1));
    node.on_leader_ack(&heartbeat_ack(SHARD, 0, 5));

    // Stale term (3 < 5): must be ignored entirely, including no
    // last_leader_contact update, even though recovery_epoch here is fine.
    clock.advance(Duration::from_ticks(2));
    node.on_leader_ack(&heartbeat_ack(SHARD, 0, 3));

    // last_leader_contact is 1 (correct): at tick 12 that is 11 > 10, so
    // Suspect. Had the stale ack been applied it would be 3, giving 9 and Active.
    clock.advance(Duration::from_ticks(9));
    node.tick();

    assert_eq!(node.state(), WorkerState::LeaderSuspect);
}

// Same differential technique with a different shard_id.
#[test]
fn mismatched_shard_id_is_ignored() {
    let clock = FakeClock::new();
    let suspect_timeout = Duration::from_ticks(10);
    let mut node = make_node(&clock, worker("w1"), suspect_timeout);

    // Otherwise-valid ack, but for a different shard: must be ignored.
    clock.advance(Duration::from_ticks(3));
    node.on_leader_ack(&heartbeat_ack(OTHER_SHARD, 0, 0));

    // Elapsed since construction is 11 > 10, so Suspect; had the mismatched
    // ack been accepted it would be 8 and Active.
    clock.advance(Duration::from_ticks(8));
    node.tick();

    assert_eq!(node.state(), WorkerState::LeaderSuspect);
}

#[test]
fn tick_before_timeout_leaves_state_active() {
    let clock = FakeClock::new();
    let suspect_timeout = Duration::from_ticks(10);
    let mut node = make_node(&clock, worker("w1"), suspect_timeout);

    clock.advance(Duration::from_ticks(5));
    node.tick();

    assert_eq!(node.state(), WorkerState::Active);
}

#[test]
fn tick_after_timeout_transitions_to_leader_suspect() {
    let clock = FakeClock::new();
    let suspect_timeout = Duration::from_ticks(10);
    let mut node = make_node(&clock, worker("w1"), suspect_timeout);

    clock.advance(Duration::from_ticks(11));
    node.tick();

    assert_eq!(node.state(), WorkerState::LeaderSuspect);
}

// The single-worker ring has no successors, so the node's own observation
// meets quorum 1 and it reaches `Candidate` (see `election_roll_call_test.rs`).
// This only checks the second tick() is not a no-op.
#[test]
fn tick_while_leader_suspect_now_begins_a_roll_call() {
    let clock = FakeClock::new();
    let suspect_timeout = Duration::from_ticks(10);
    let mut node = make_node(&clock, worker("w1"), suspect_timeout);

    clock.advance(Duration::from_ticks(11));
    node.tick();
    assert_eq!(node.state(), WorkerState::LeaderSuspect);

    clock.advance(Duration::from_ticks(100));
    node.tick();

    assert_ne!(node.state(), WorkerState::LeaderSuspect);
}

// A node never adopts a newer epoch from an ack, so it must not treat a
// newer-epoch leader as its own: accepting it would keep this node `Active`
// under that leader while it still acts on its old epoch.
#[test]
fn newer_recovery_epoch_is_ignored() {
    let clock = FakeClock::new();
    let suspect_timeout = Duration::from_ticks(10);
    let mut node = make_node(&clock, worker("w1"), suspect_timeout);

    clock.advance(Duration::from_ticks(8));
    node.on_leader_ack(&heartbeat_ack(SHARD, 1, 0));

    // last_leader_contact is still 0, so tick 15 is 15 > 10, Suspect. Had the
    // ack been applied it would be 8, giving 7 and Active.
    clock.advance(Duration::from_ticks(7));
    node.tick();

    assert_eq!(node.state(), WorkerState::LeaderSuspect);
}
