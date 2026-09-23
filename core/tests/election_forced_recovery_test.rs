mod support;

use support::builders::{
    SharedMembership, make_network, observation, roll_call, roll_call_message, shard, vote_grant,
    worker,
};

use support::candidate::predict_winner;

use std::collections::BTreeSet;

use kabudachi_core::coordination_authority::{AuthorityError, CoordinationAuthority};
use kabudachi_core::election::WorkerNode;
use kabudachi_core::membership::MembershipView;
use kabudachi_core::protocol::ids::{IncarnationId, ShardId, WorkerId};
use kabudachi_core::protocol::messages::election_message;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::Duration;
use kabudachi_core::transport::PeerMessenger;
use support::clock::FakeClock;
use support::coordination_authority::FakeCoordinationAuthority;
use support::network::FakeNetwork;

const SHARD: &str = "shard-1";

/// A node with an electorate of exactly `electorate`, plus the
/// `SharedMembership` handle for inspecting its membership. `authority` is
/// cloned in so callers can pre-seed or wrap it first.
#[allow(clippy::type_complexity)]
fn make_node_with_ring<A: CoordinationAuthority + Clone>(
    clock: &FakeClock,
    network: &FakeNetwork,
    authority: &A,
    my_id: WorkerId,
    electorate: &[WorkerId],
    suspect_timeout: Duration,
) -> (
    WorkerNode<FakeClock, FakeNetwork, SharedMembership, A>,
    SharedMembership,
) {
    let membership = SharedMembership::new(electorate.iter().cloned().collect());
    let node = WorkerNode::new(
        my_id,
        IncarnationId::new("incarnation-1"),
        shard(SHARD),
        clock.clone(),
        network.clone(),
        membership.clone(),
        authority.clone(),
        suspect_timeout,
    );
    (node, membership)
}

/// Drives a fresh node to a real `Leader` through roll call and voting, with no
/// state shortcuts. `authority` is generic so the CAS-conflict test can pass a
/// decorator; this setup never touches it.
///
/// Returns `(node, self_id, peer_a, peer_b, helper_peer, network, membership)`.
/// `self_id`, `peer_a` and `peer_b` are the 3-member electorate (quorum 2):
/// `peer_a` casts the deciding grant and `peer_b` is there so a partition can
/// drop the leader below quorum. `helper_peer` is on the network but not in the
/// electorate.
#[allow(clippy::type_complexity)]
fn leader_with_electorate<A: CoordinationAuthority + Clone>(
    clock: &FakeClock,
    authority: &A,
) -> (
    WorkerNode<FakeClock, FakeNetwork, SharedMembership, A>,
    WorkerId,
    WorkerId,
    WorkerId,
    WorkerId,
    FakeNetwork,
    SharedMembership,
) {
    let suspect_timeout = Duration::from_ticks(10);

    // self_id is whichever of two candidate labels wins at term 1.
    let candidate_x = worker("candidate-x");
    let candidate_y = worker("candidate-y");
    let next_term = 1; // both observations below carry highest_term_seen: 0.
    let winner = predict_winner(
        &shard(SHARD),
        0,
        next_term,
        &[candidate_x.clone(), candidate_y.clone()],
    );
    let (self_id, peer_a) = if winner == candidate_x {
        (candidate_x, candidate_y)
    } else {
        (candidate_y, candidate_x)
    };
    let peer_b = worker("peer-b");
    let helper_peer = worker("helper-peer");

    let network = make_network(
        clock,
        &[
            self_id.clone(),
            peer_a.clone(),
            peer_b.clone(),
            helper_peer.clone(),
        ],
    );
    let (mut node, membership) = make_node_with_ring(
        clock,
        &network,
        authority,
        self_id.clone(),
        &[self_id.clone(), peer_a.clone(), peer_b.clone()],
        suspect_timeout,
    );

    clock.advance(Duration::from_ticks(11));
    node.tick(); // Active -> LeaderSuspect
    node.tick(); // LeaderSuspect -> RollCall (self-only response: 1 < quorum-of-2, forwards).
    assert_eq!(node.state(), WorkerState::RollCall, "test setup invariant");

    // A synthetic roll call carrying peer_a's observation reaches quorum 2;
    // self_id was chosen to win, so RollCall -> Candidate.
    let call = roll_call(
        "external-call-1",
        peer_a.clone(),
        vec![observation(peer_a.clone(), 0)],
    );
    node.on_message(peer_a.clone(), roll_call_message(call));
    assert_eq!(node.state(), WorkerState::Candidate, "test setup invariant");

    // The self-vote plus this grant reaches quorum 2.
    node.on_vote_grant(&vote_grant(self_id.clone(), peer_a.clone(), next_term));
    assert_eq!(node.state(), WorkerState::Leader, "test setup invariant");

    (
        node,
        self_id,
        peer_a,
        peer_b,
        helper_peer,
        network,
        membership,
    )
}

/// Drives a fresh node to `NoQuorum` (README §10.2/§10.4 peer-loss edge): it
/// reaches `Leader`, then is partitioned from both electorate peers (not from
/// `helper_peer`) so `tick()` detects lost quorum. Returns the same tuple as
/// `leader_with_electorate`.
#[allow(clippy::type_complexity)]
fn leader_in_no_quorum<A: CoordinationAuthority + Clone>(
    clock: &FakeClock,
    authority: &A,
) -> (
    WorkerNode<FakeClock, FakeNetwork, SharedMembership, A>,
    WorkerId,
    WorkerId,
    WorkerId,
    WorkerId,
    FakeNetwork,
    SharedMembership,
) {
    let (mut node, self_id, peer_a, peer_b, helper_peer, network, membership) =
        leader_with_electorate(clock, authority);

    network.partition(
        [self_id.clone()].into_iter().collect(),
        [peer_a.clone(), peer_b.clone()].into_iter().collect(),
    );
    node.tick(); // Leader -> NoQuorum.
    assert_eq!(node.state(), WorkerState::NoQuorum, "test setup invariant");

    (
        node,
        self_id,
        peer_a,
        peer_b,
        helper_peer,
        network,
        membership,
    )
}

#[test]
fn tick_leader_with_full_quorum_visibility_stays_leader() {
    let clock = FakeClock::new();
    let authority = FakeCoordinationAuthority::new();
    let (mut node, ..) = leader_with_electorate(&clock, &authority);
    assert_eq!(node.state(), WorkerState::Leader);

    // All 3 electorate members are reachable, so quorum is confirmed (3 >= 2).
    node.tick();

    assert_eq!(
        node.state(),
        WorkerState::Leader,
        "a leader that can still see enough of its electorate must remain Leader"
    );
}

#[test]
fn tick_leader_losing_quorum_visibility_transitions_to_no_quorum() {
    let clock = FakeClock::new();
    let authority = FakeCoordinationAuthority::new();
    let (mut node, self_id, peer_a, peer_b, _helper_peer, network, _membership) =
        leader_with_electorate(&clock, &authority);
    assert_eq!(node.state(), WorkerState::Leader);

    // Partitioned from both peers: the visible count is 1, below quorum 2.
    network.partition(
        [self_id].into_iter().collect(),
        [peer_a, peer_b].into_iter().collect(),
    );
    node.tick();

    assert_eq!(
        node.state(),
        WorkerState::NoQuorum,
        "a leader that can no longer see enough of its electorate must lose leadership"
    );
}

// A no-op outside `NoQuorum` makes zero authority calls. A prior bump (0 -> 1)
// is seeded; if the guard failed, another bump would make the epoch 2.
#[test]
fn attempt_forced_recovery_is_a_no_op_when_state_is_not_no_quorum() {
    let clock = FakeClock::new();
    let suspect_timeout = Duration::from_ticks(10);
    let self_id = worker("w1");
    let peer = worker("w2");
    let network = make_network(&clock, &[self_id.clone(), peer.clone()]);
    let authority = FakeCoordinationAuthority::new();
    authority
        .force_reconfigure(
            &shard(SHARD),
            0,
            [self_id.clone(), peer.clone()].into_iter().collect(),
        )
        .expect("seed a single prior bump: epoch 0 -> 1");
    let (mut node, membership) = make_node_with_ring(
        &clock,
        &network,
        &authority,
        self_id.clone(),
        &[self_id.clone(), peer.clone()],
        suspect_timeout,
    );
    assert_eq!(node.state(), WorkerState::Active);
    let generation_before = membership.membership_generation();

    node.attempt_forced_recovery();

    assert_eq!(
        node.state(),
        WorkerState::Active,
        "attempt_forced_recovery from a non-NoQuorum state must be a complete no-op"
    );
    assert_eq!(
        authority.read_recovery_epoch(&shard(SHARD)).unwrap(),
        1,
        "the authority must never be called at all when the guard short-circuits"
    );
    assert_eq!(membership.membership_generation(), generation_before);
    network.pump();
    assert!(
        network.poll_inbox(peer).is_empty(),
        "no messages should be sent by a no-op attempt_forced_recovery"
    );
}

#[test]
fn attempt_forced_recovery_fails_when_reachable_intersection_is_empty() {
    let clock = FakeClock::new();
    let authority = FakeCoordinationAuthority::new();
    let (mut node, _self_id, peer_a, _peer_b, _helper_peer, _network, membership) =
        leader_in_no_quorum(&clock, &authority);

    // The authority confirms only `peer_a`, which is partitioned away, so the intersection with reachable peers is empty.
    authority
        .force_reconfigure(&shard(SHARD), 0, [peer_a].into_iter().collect())
        .expect("seed authority state");
    let generation_before = membership.membership_generation();

    node.attempt_forced_recovery();

    assert_eq!(
        node.state(),
        WorkerState::NoQuorum,
        "an empty reachable intersection must leave the node stuck in NoQuorum"
    );
    assert_eq!(membership.membership_generation(), generation_before);
    assert_eq!(
        authority.read_recovery_epoch(&shard(SHARD)).unwrap(),
        1,
        "a failed (empty-intersection) attempt must never call force_reconfigure at all"
    );
}

#[test]
fn attempt_forced_recovery_fails_when_authority_unavailable() {
    let clock = FakeClock::new();
    let authority = FakeCoordinationAuthority::new();
    let (mut node, _self_id, _peer_a, _peer_b, _helper_peer, _network, membership) =
        leader_in_no_quorum(&clock, &authority);
    authority.set_available(false);
    let generation_before = membership.membership_generation();

    node.attempt_forced_recovery();

    assert_eq!(
        node.state(),
        WorkerState::NoQuorum,
        "an unavailable authority must leave the node stuck in NoQuorum"
    );
    assert_eq!(membership.membership_generation(), generation_before);
}

/// Wraps a `FakeCoordinationAuthority` to inject a genuine compare-and-swap
/// race: each `read_recovery_epoch` reports the current epoch, then immediately
/// does another successful `force_reconfigure`, as if a concurrent actor won.
/// In a synchronous harness that is the only way to make the epoch stale between
/// the two back-to-back calls. Other methods delegate unchanged.
#[derive(Clone)]
struct RacingCoordinationAuthority {
    inner: FakeCoordinationAuthority,
    /// Membership the simulated racer reconfigures to; any successful bump makes the node's CAS stale.
    racer_replacement: BTreeSet<WorkerId>,
}

impl CoordinationAuthority for RacingCoordinationAuthority {
    fn discover_workers(&self, shard_id: &ShardId) -> Result<BTreeSet<WorkerId>, AuthorityError> {
        self.inner.discover_workers(shard_id)
    }

    fn read_recovery_epoch(&self, shard_id: &ShardId) -> Result<u64, AuthorityError> {
        let observed = self.inner.read_recovery_epoch(shard_id)?;
        // The racer's bump always succeeds: nothing else can change the epoch in between.
        self.inner
            .force_reconfigure(shard_id, observed, self.racer_replacement.clone())
            .expect("racer's own force_reconfigure must succeed exactly once per read");
        Ok(observed)
    }

    fn force_reconfigure(
        &self,
        shard_id: &ShardId,
        expected_recovery_epoch: u64,
        replacement_members: BTreeSet<WorkerId>,
    ) -> Result<u64, AuthorityError> {
        self.inner
            .force_reconfigure(shard_id, expected_recovery_epoch, replacement_members)
    }
}

#[test]
fn attempt_forced_recovery_fails_on_cas_conflict() {
    let clock = FakeClock::new();
    let inner_authority = FakeCoordinationAuthority::new();
    let racing_authority = RacingCoordinationAuthority {
        inner: inner_authority.clone(),
        racer_replacement: BTreeSet::new(),
    };
    let (mut node, _self_id, _peer_a, _peer_b, helper_peer, _network, membership) =
        leader_in_no_quorum(&clock, &racing_authority);

    // Seed the authority to confirm `helper_peer`, or recovery would fail at
    // the empty-intersection guard before reaching the CAS.
    inner_authority
        .force_reconfigure(&shard(SHARD), 0, [helper_peer].into_iter().collect())
        .expect("seed authority state");
    let generation_before = membership.membership_generation();

    node.attempt_forced_recovery();

    assert_eq!(
        node.state(),
        WorkerState::NoQuorum,
        "a CAS conflict on force_reconfigure must leave the node stuck in NoQuorum"
    );
    assert_eq!(
        membership.membership_generation(),
        generation_before,
        "membership must be untouched when force_reconfigure fails"
    );
    // Only the seed bump (0 -> 1) and the racer's bump (1 -> 2) succeeded; the
    // node's rejected attempt applied nothing.
    assert_eq!(
        inner_authority.read_recovery_epoch(&shard(SHARD)).unwrap(),
        2
    );
}

#[test]
fn attempt_forced_recovery_succeeds_and_begins_a_real_roll_call() {
    let clock = FakeClock::new();
    let authority = FakeCoordinationAuthority::new();
    let (mut node, self_id, _peer_a, _peer_b, helper_peer, network, membership) =
        leader_in_no_quorum(&clock, &authority);

    // The authority confirms only `helper_peer`, which is reachable.
    authority
        .force_reconfigure(
            &shard(SHARD),
            0,
            [helper_peer.clone()].into_iter().collect(),
        )
        .expect("seed authority state");

    node.attempt_forced_recovery();

    assert_eq!(
        node.state(),
        WorkerState::RollCall,
        "a successful forced recovery must transition NoQuorum -> RollCall"
    );
    assert_eq!(
        authority.read_recovery_epoch(&shard(SHARD)).unwrap(),
        2,
        "the authority's stored epoch must have advanced again, past the test's own seed bump"
    );
    assert_eq!(
        membership.effective_electorate(),
        [self_id, helper_peer.clone()]
            .into_iter()
            .collect::<BTreeSet<_>>(),
    );

    // begin_roll_call() ran: the rebuilt 2-member electorate (quorum 2) has only
    // this node's observation, so the call is forwarded to `helper_peer`.
    network.pump();
    let mut inbox = network.poll_inbox(helper_peer);
    assert_eq!(inbox.len(), 1, "expected exactly one forwarded RollCall");
    match inbox.remove(0).1.payload {
        Some(election_message::Payload::RollCall(call)) => {
            assert_eq!(
                call.recovery_epoch, 2,
                "the RollCall must carry the freshly-updated recovery_epoch"
            );
            // This node led a term before losing quorum. Its own call must
            // start from that term, or it would re-contest the term its old
            // voters already granted and they would drop the call.
            assert!(node.term() >= 1, "setup invariant: the node led a real term");
            assert_eq!(
                call.highest_term_seen,
                node.term(),
                "the ex-leader's roll call must carry the term it led as highest_term_seen"
            );
        }
        other => panic!("expected RollCall payload, got {other:?}"),
    }
}

#[test]
fn attempt_forced_recovery_rebuilds_membership_excluding_stale_members() {
    let clock = FakeClock::new();
    let authority = FakeCoordinationAuthority::new();
    let (mut node, _self_id, peer_a, peer_b, helper_peer, _network, membership) =
        leader_in_no_quorum(&clock, &authority);

    authority
        .force_reconfigure(
            &shard(SHARD),
            0,
            [helper_peer.clone()].into_iter().collect(),
        )
        .expect("seed authority state");

    node.attempt_forced_recovery();

    let after = membership.effective_electorate();
    assert!(
        !after.contains(&peer_a),
        "a member from the OLD electorate absent from the new reachable set must be excluded"
    );
    assert!(
        !after.contains(&peer_b),
        "a member from the OLD electorate absent from the new reachable set must be excluded"
    );
    assert!(
        after.contains(&helper_peer),
        "a member confirmed reachable but absent from the OLD electorate must be included"
    );
}

#[test]
fn attempt_forced_recovery_always_includes_self_in_rebuilt_electorate() {
    let clock = FakeClock::new();
    let authority = FakeCoordinationAuthority::new();
    let (mut node, self_id, _peer_a, _peer_b, helper_peer, _network, membership) =
        leader_in_no_quorum(&clock, &authority);

    // discover_workers's returned set does not explicitly list self_id.
    authority
        .force_reconfigure(&shard(SHARD), 0, [helper_peer].into_iter().collect())
        .expect("seed authority state");

    node.attempt_forced_recovery();

    assert!(
        membership.effective_electorate().contains(&self_id),
        "self must always be included in the rebuilt electorate, even when \
         discover_workers doesn't explicitly list it"
    );
}

// Local epoch is raised to 3 by a real recovery, then the authority is wiped
// and reseeded back down to epoch 1. Without the guard the CAS (1 -> 2) would
// succeed and pull the node from epoch 3 back to 2.
#[test]
fn attempt_forced_recovery_refuses_to_move_the_epoch_backwards() {
    let clock = FakeClock::new();
    let authority = FakeCoordinationAuthority::new();
    let (mut node, self_id, peer_a, peer_b, helper_peer, network, membership) =
        leader_in_no_quorum(&clock, &authority);

    for epoch in 0..2 {
        authority
            .force_reconfigure(
                &shard(SHARD),
                epoch,
                [helper_peer.clone()].into_iter().collect(),
            )
            .expect("seed authority state");
    }
    node.attempt_forced_recovery();
    assert_eq!(node.recovery_epoch(), 3, "test setup invariant");
    assert_eq!(node.state(), WorkerState::RollCall, "test setup invariant");

    // Win an election in the recovered 2-member electorate at epoch 3. The
    // helper's reported term is varied until this node is the predicted
    // winner, since epoch and term both feed the candidate ranking. It starts
    // at this node's own term: a call behind a term this node already holds
    // is dropped as stale.
    let helper_term = (node.term()..64)
        .find(|term| {
            predict_winner(
                &shard(SHARD),
                3,
                term + 1,
                &[self_id.clone(), helper_peer.clone()],
            ) == self_id
        })
        .expect("some term must favour this node");
    let mut call = roll_call(
        "recovery-call-1",
        helper_peer.clone(),
        vec![observation(helper_peer.clone(), helper_term)],
    );
    call.recovery_epoch = 3;
    call.highest_term_seen = helper_term;
    node.on_message(helper_peer.clone(), roll_call_message(call));
    assert_eq!(node.state(), WorkerState::Candidate, "test setup invariant");
    let mut grant = vote_grant(self_id.clone(), helper_peer.clone(), helper_term + 1);
    grant.recovery_epoch = 3;
    node.on_vote_grant(&grant);
    assert_eq!(node.state(), WorkerState::Leader, "test setup invariant");

    // Lose quorum again, this time to `helper_peer`, leaving the old peers reachable.
    network.partition(
        [self_id].into_iter().collect(),
        [helper_peer].into_iter().collect(),
    );
    node.tick();
    assert_eq!(node.state(), WorkerState::NoQuorum, "test setup invariant");

    authority.flush_all();
    authority
        .force_reconfigure(&shard(SHARD), 0, [peer_a, peer_b].into_iter().collect())
        .expect("reseed the wiped authority at a lower epoch");
    let generation_before = membership.membership_generation();

    node.attempt_forced_recovery();

    assert_eq!(node.state(), WorkerState::NoQuorum);
    assert_eq!(
        node.recovery_epoch(),
        3,
        "the epoch must never go backwards"
    );
    assert_eq!(membership.membership_generation(), generation_before);
    assert_eq!(
        authority.read_recovery_epoch(&shard(SHARD)).unwrap(),
        1,
        "the stale authority must not be bumped on behalf of a node that is ahead of it"
    );
}
