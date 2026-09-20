mod support;

use support::builders::{
    make_network, observation, roll_call, roll_call_message, shard, vote_grant, vote_request,
    worker,
};

use support::candidate::predict_winner;

use kabudachi_core::election::WorkerNode;
use kabudachi_core::membership::RingMembership;
use kabudachi_core::protocol::ids::{IncarnationId, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::messages::{
    ElectionMessage, SelfRemove, VoteGrant, VoteReject, VoteRejectReason, VoteRequest,
    election_message,
};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::Duration;
use kabudachi_core::transport::PeerMessenger;
use support::clock::FakeClock;
use support::coordination_authority::FakeCoordinationAuthority;
use support::network::FakeNetwork;

const SHARD: &str = "shard-1";

fn vote_request_for_shard(candidate: WorkerId, shard_id: &str, term: u64) -> VoteRequest {
    let mut req = vote_request(candidate, 0, term);
    req.shard_id = Some(shard(shard_id).into());
    req
}

fn make_node_with_ring(
    clock: &FakeClock,
    network: &FakeNetwork,
    my_id: WorkerId,
    electorate: &[WorkerId],
    suspect_timeout: Duration,
) -> WorkerNode<FakeClock, FakeNetwork, RingMembership, FakeCoordinationAuthority> {
    WorkerNode::new(
        my_id,
        IncarnationId::new("incarnation-1"),
        shard(SHARD),
        clock.clone(),
        network.clone(),
        RingMembership::new(electorate.iter().cloned().collect()),
        FakeCoordinationAuthority::new(),
        suspect_timeout,
    )
}

fn expect_single_vote_reject(mut inbox: Vec<(WorkerId, ElectionMessage)>) -> VoteReject {
    assert_eq!(inbox.len(), 1, "expected exactly one message in the inbox");
    match inbox.remove(0).1.payload {
        Some(election_message::Payload::VoteReject(reject)) => reject,
        other => panic!("expected VoteReject payload, got {other:?}"),
    }
}

fn expect_single_vote_grant(mut inbox: Vec<(WorkerId, ElectionMessage)>) -> VoteGrant {
    assert_eq!(inbox.len(), 1, "expected exactly one message in the inbox");
    match inbox.remove(0).1.payload {
        Some(election_message::Payload::VoteGrant(grant)) => grant,
        other => panic!("expected VoteGrant payload, got {other:?}"),
    }
}

// A `Candidate` rejects vote requests as `NotVoter`. It needs a 2-member
// electorate: with one member the self-vote would win outright.
#[test]
fn on_vote_request_rejects_not_voter_when_already_candidate() {
    let clock = FakeClock::new();
    let suspect_timeout = Duration::from_ticks(10);
    let candidate_a = worker("candidate-a");
    let candidate_b = worker("candidate-b");
    let next_term = 1; // both observations below carry highest_term_seen: 0.
    let winner = predict_winner(
        &shard(SHARD),
        0,
        next_term,
        &[candidate_a.clone(), candidate_b.clone()],
    );
    let (self_id, peer) = if winner == candidate_a {
        (candidate_a, candidate_b)
    } else {
        (candidate_b, candidate_a)
    };
    let network = make_network(&clock, &[self_id.clone(), peer.clone()]);
    let mut node = make_node_with_ring(
        &clock,
        &network,
        self_id.clone(),
        &[self_id.clone(), peer.clone()],
        suspect_timeout,
    );

    // Start the node's own roll call (quorum 2) and drain the forward.
    clock.advance(Duration::from_ticks(11));
    node.tick();
    node.tick();
    assert_eq!(node.state(), WorkerState::RollCall);
    network.pump();
    network.poll_inbox(peer.clone());

    // A call reaching quorum 2 with this node as the winner makes it Candidate; the self-vote (1) is short of quorum 2.
    let call = roll_call(
        "external-call-1",
        peer.clone(),
        vec![observation(peer.clone(), 0)],
    );
    node.on_message(peer.clone(), roll_call_message(call));
    assert_eq!(node.state(), WorkerState::Candidate);
    network.pump();
    network.poll_inbox(peer.clone()); // drain the VoteRequest it sends.

    node.on_vote_request(&vote_request(peer.clone(), 0, 99));

    network.pump();
    let reject = expect_single_vote_reject(network.poll_inbox(peer));
    assert_eq!(reject.reason, VoteRejectReason::NotVoter as i32);
}

#[test]
fn on_vote_request_rejects_wrong_recovery_epoch() {
    let clock = FakeClock::new();
    let suspect_timeout = Duration::from_ticks(10);
    let self_id = worker("w1");
    let requester = worker("candidate-a");
    let network = make_network(&clock, &[self_id.clone(), requester.clone()]);
    let mut node = make_node_with_ring(
        &clock,
        &network,
        self_id.clone(),
        &[self_id.clone(), requester.clone()],
        suspect_timeout,
    );

    node.on_vote_request(&vote_request(requester.clone(), 1, 5));

    network.pump();
    let reject = expect_single_vote_reject(network.poll_inbox(requester));
    assert_eq!(reject.reason, VoteRejectReason::WrongRecoveryEpoch as i32);
}

#[test]
fn on_vote_request_rejects_stale_term() {
    let clock = FakeClock::new();
    let suspect_timeout = Duration::from_ticks(10);
    let self_id = worker("w1");
    let requester = worker("candidate-a");
    let network = make_network(&clock, &[self_id.clone(), requester.clone()]);
    let mut node = make_node_with_ring(
        &clock,
        &network,
        self_id.clone(),
        &[self_id.clone(), requester.clone()],
        suspect_timeout,
    );

    // Fresh node's highest_term_seen is 0; term 0 is <= that.
    node.on_vote_request(&vote_request(requester.clone(), 0, 0));

    network.pump();
    let reject = expect_single_vote_reject(network.poll_inbox(requester));
    assert_eq!(reject.reason, VoteRejectReason::StaleTerm as i32);
}

#[test]
fn on_vote_request_rejects_already_voted_for_a_different_candidate() {
    let clock = FakeClock::new();
    let suspect_timeout = Duration::from_ticks(10);
    let self_id = worker("w1");
    let candidate_a = worker("candidate-a");
    let candidate_b = worker("candidate-b");
    let network = make_network(
        &clock,
        &[self_id.clone(), candidate_a.clone(), candidate_b.clone()],
    );
    let mut node = make_node_with_ring(
        &clock,
        &network,
        self_id.clone(),
        &[self_id.clone(), candidate_a.clone(), candidate_b.clone()],
        suspect_timeout,
    );

    // Age last_leader_contact so the first request can be granted.
    clock.advance(Duration::from_ticks(11));

    node.on_vote_request(&vote_request(candidate_a.clone(), 0, 5));
    network.pump();
    expect_single_vote_grant(network.poll_inbox(candidate_a));

    node.on_vote_request(&vote_request(candidate_b.clone(), 0, 5));
    network.pump();
    let reject = expect_single_vote_reject(network.poll_inbox(candidate_b));
    assert_eq!(reject.reason, VoteRejectReason::AlreadyVoted as i32);
}

#[test]
fn on_vote_request_rejects_leader_still_valid() {
    let clock = FakeClock::new();
    let suspect_timeout = Duration::from_ticks(10);
    let self_id = worker("w1");
    let requester = worker("candidate-a");
    let network = make_network(&clock, &[self_id.clone(), requester.clone()]);
    let mut node = make_node_with_ring(
        &clock,
        &network,
        self_id.clone(),
        &[self_id.clone(), requester.clone()],
        suspect_timeout,
    );

    // Clock never advanced: last_leader_contact (set at construction) is
    // still within suspect_timeout of "now".
    node.on_vote_request(&vote_request(requester.clone(), 0, 1));

    network.pump();
    let reject = expect_single_vote_reject(network.poll_inbox(requester));
    assert_eq!(reject.reason, VoteRejectReason::LeaderStillValid as i32);
}

#[test]
fn on_vote_request_grants_when_all_guards_pass() {
    let clock = FakeClock::new();
    let suspect_timeout = Duration::from_ticks(10);
    let self_id = worker("w1");
    let candidate_a = worker("candidate-a");
    let network = make_network(&clock, &[self_id.clone(), candidate_a.clone()]);
    let mut node = make_node_with_ring(
        &clock,
        &network,
        self_id.clone(),
        &[self_id.clone(), candidate_a.clone()],
        suspect_timeout,
    );

    clock.advance(Duration::from_ticks(11));

    node.on_vote_request(&vote_request(candidate_a.clone(), 0, 7));
    network.pump();
    let grant = expect_single_vote_grant(network.poll_inbox(candidate_a.clone()));
    assert_eq!(grant.term, 7);
    assert_eq!(grant.candidate_id(), candidate_a);
    assert_eq!(grant.voter_id(), self_id);

    // `voted_for` is private, so check it indirectly: a second request for the
    // same term must now be AlreadyVoted.
    node.on_vote_request(&vote_request(candidate_a.clone(), 0, 7));
    network.pump();
    let reject = expect_single_vote_reject(network.poll_inbox(candidate_a));
    assert_eq!(reject.reason, VoteRejectReason::AlreadyVoted as i32);
}

#[test]
fn on_vote_request_mismatched_shard_id_is_silently_ignored() {
    let clock = FakeClock::new();
    let suspect_timeout = Duration::from_ticks(10);
    let self_id = worker("w1");
    let candidate_a = worker("candidate-a");
    let network = make_network(&clock, &[self_id.clone(), candidate_a.clone()]);
    let mut node = make_node_with_ring(
        &clock,
        &network,
        self_id.clone(),
        &[self_id.clone(), candidate_a.clone()],
        suspect_timeout,
    );

    clock.advance(Duration::from_ticks(11));

    node.on_vote_request(&vote_request_for_shard(
        candidate_a.clone(),
        "other-shard",
        5,
    ));
    network.pump();
    assert!(
        network.poll_inbox(candidate_a.clone()).is_empty(),
        "a shard_id mismatch must produce no reply at all"
    );

    // No state or vote was recorded: a correctly-scoped request for the same term still succeeds.
    node.on_vote_request(&vote_request(candidate_a.clone(), 0, 5));
    network.pump();
    let grant = expect_single_vote_grant(network.poll_inbox(candidate_a));
    assert_eq!(grant.term, 5);
}

/// A node in a 5-member electorate (quorum 3) that became `Candidate` for term
/// 1 through a real 3-member roll call, having sent `VoteRequest`s to the two
/// responders. The two other electorate members ("extra voters") are on the
/// network so tests can grant votes from them to control how close the
/// candidacy is to quorum. Returns `(node, self_id, responder_1, responder_2,
/// extra_voter_1, extra_voter_2, network, term)`.
#[allow(clippy::type_complexity)]
fn candidate_with_five_member_electorate() -> (
    WorkerNode<FakeClock, FakeNetwork, RingMembership, FakeCoordinationAuthority>,
    WorkerId,
    WorkerId,
    WorkerId,
    WorkerId,
    WorkerId,
    FakeNetwork,
    u64,
) {
    let clock = FakeClock::new();
    let suspect_timeout = Duration::from_ticks(10);

    // self_id is whichever of three labels wins at term 1.
    let candidate_a = worker("candidate-a");
    let candidate_b = worker("candidate-b");
    let candidate_c = worker("candidate-c");
    let next_term = 1; // all observations below carry highest_term_seen: 0.
    let winner = predict_winner(
        &shard(SHARD),
        0,
        next_term,
        &[
            candidate_a.clone(),
            candidate_b.clone(),
            candidate_c.clone(),
        ],
    );
    let mut others: Vec<WorkerId> = [candidate_a, candidate_b, candidate_c]
        .into_iter()
        .filter(|c| *c != winner)
        .collect();
    let self_id = winner;
    let responder_1 = others.remove(0);
    let responder_2 = others.remove(0);
    let extra_voter_1 = worker("peer-4");
    let extra_voter_2 = worker("peer-5");

    let network = make_network(
        &clock,
        &[
            self_id.clone(),
            responder_1.clone(),
            responder_2.clone(),
            extra_voter_1.clone(),
            extra_voter_2.clone(),
        ],
    );
    let mut node = make_node_with_ring(
        &clock,
        &network,
        self_id.clone(),
        &[
            self_id.clone(),
            responder_1.clone(),
            responder_2.clone(),
            extra_voter_1.clone(),
            extra_voter_2.clone(),
        ],
        suspect_timeout,
    );

    // Start the node's own roll call (quorum 3 needs more than its own
    // observation) and drain the forward from every other member's inbox.
    clock.advance(Duration::from_ticks(11));
    node.tick();
    node.tick();
    assert_eq!(node.state(), WorkerState::RollCall);
    network.pump();
    for member in [&responder_1, &responder_2, &extra_voter_1, &extra_voter_2] {
        network.poll_inbox(member.clone());
    }

    // A roll call carrying the two responders' observations reaches quorum 3;
    // self_id was chosen as the winner of {self, responder_1, responder_2}.
    let call = roll_call(
        "external-call-1",
        responder_1.clone(),
        vec![
            observation(responder_1.clone(), 0),
            observation(responder_2.clone(), 0),
        ],
    );
    node.on_message(responder_1.clone(), roll_call_message(call));
    assert_eq!(node.state(), WorkerState::Candidate);

    // Drain the VoteRequests sent to the responders.
    network.pump();
    network.poll_inbox(responder_1.clone());
    network.poll_inbox(responder_2.clone());

    (
        node,
        self_id,
        responder_1,
        responder_2,
        extra_voter_1,
        extra_voter_2,
        network,
        next_term,
    )
}

#[test]
fn becoming_candidate_sends_vote_requests_to_every_roll_call_responder() {
    let clock = FakeClock::new();
    let suspect_timeout = Duration::from_ticks(10);

    let candidate_a = worker("candidate-a");
    let candidate_b = worker("candidate-b");
    let candidate_c = worker("candidate-c");
    let next_term = 1;
    let winner = predict_winner(
        &shard(SHARD),
        0,
        next_term,
        &[
            candidate_a.clone(),
            candidate_b.clone(),
            candidate_c.clone(),
        ],
    );
    let mut others: Vec<WorkerId> = [candidate_a, candidate_b, candidate_c]
        .into_iter()
        .filter(|c| *c != winner)
        .collect();
    let self_id = winner;
    let other_1 = others.remove(0);
    let other_2 = others.remove(0);

    let network = make_network(&clock, &[self_id.clone(), other_1.clone(), other_2.clone()]);
    let mut node = make_node_with_ring(
        &clock,
        &network,
        self_id.clone(),
        &[self_id.clone(), other_1.clone(), other_2.clone()],
        suspect_timeout,
    );

    clock.advance(Duration::from_ticks(11));
    node.tick();
    node.tick();
    assert_eq!(node.state(), WorkerState::RollCall);
    network.pump();
    // The roll call goes to whichever ring successor sorts first, so drain both.
    network.poll_inbox(other_1.clone());
    network.poll_inbox(other_2.clone());

    let call = roll_call(
        "external-call-1",
        other_1.clone(),
        vec![
            observation(other_1.clone(), 0),
            observation(other_2.clone(), 0),
        ],
    );
    node.on_message(other_1.clone(), roll_call_message(call));
    assert_eq!(node.state(), WorkerState::Candidate);

    network.pump();
    for other in [other_1, other_2] {
        let inbox = network.poll_inbox(other.clone());
        assert_eq!(
            inbox.len(),
            1,
            "expected exactly one VoteRequest sent to roll-call responder {other:?}"
        );
        match &inbox[0].1.payload {
            Some(election_message::Payload::VoteRequest(req)) => {
                assert_eq!(req.candidate_id(), self_id);
                assert_eq!(req.term, next_term);
            }
            other_payload => panic!("expected VoteRequest payload, got {other_payload:?}"),
        }
    }
}

#[test]
fn reaching_quorum_via_vote_grant_transitions_to_leader_and_broadcasts_certificate() {
    let (mut node, self_id, _r1, _r2, extra_voter_1, extra_voter_2, network, term) =
        candidate_with_five_member_electorate();

    // Self-vote (1) plus two grants reaches quorum 3.
    node.on_vote_grant(&vote_grant(self_id.clone(), extra_voter_1.clone(), term));
    assert_eq!(
        node.state(),
        WorkerState::Candidate,
        "one grant (2 total votes) must not yet reach quorum-of-3"
    );

    node.on_vote_grant(&vote_grant(self_id.clone(), extra_voter_2.clone(), term));
    assert_eq!(
        node.state(),
        WorkerState::Leader,
        "the second grant (3 total votes) must reach quorum and elect a Leader directly, \
         not leave the node sitting in LeaderReconciling"
    );

    network.pump();
    for voter in [self_id, extra_voter_1, extra_voter_2] {
        let inbox = network.poll_inbox(voter.clone());
        assert_eq!(
            inbox.len(),
            1,
            "expected exactly one ElectionCertificate broadcast to granting voter {voter:?}"
        );
        match &inbox[0].1.payload {
            Some(election_message::Payload::ElectionCertificate(cert)) => {
                assert_eq!(cert.term, term);
            }
            other => panic!("expected ElectionCertificate payload, got {other:?}"),
        }
    }
}

#[test]
fn not_reaching_quorum_leaves_candidate_in_candidate_state() {
    let (mut node, self_id, _r1, _r2, extra_voter_1, _extra_voter_2, _network, term) =
        candidate_with_five_member_electorate();

    // Self-vote plus one grant (2) is short of quorum 3.
    node.on_vote_grant(&vote_grant(self_id, extra_voter_1, term));

    assert_eq!(node.state(), WorkerState::Candidate);
}

#[test]
fn stale_term_vote_grant_is_ignored() {
    let (mut node, self_id, _r1, _r2, extra_voter_1, extra_voter_2, _network, term) =
        candidate_with_five_member_electorate();

    // A grant for another term must not count.
    node.on_vote_grant(&vote_grant(
        self_id.clone(),
        extra_voter_1.clone(),
        term + 1,
    ));
    assert_eq!(node.state(), WorkerState::Candidate);

    // It didn't count: both remaining correct-term grants are still needed for quorum 3.
    node.on_vote_grant(&vote_grant(self_id.clone(), extra_voter_1, term));
    assert_eq!(
        node.state(),
        WorkerState::Candidate,
        "only one correctly-termed grant recorded so far (2 total votes) — the stale-term \
         grant must not have silently contributed a vote"
    );

    node.on_vote_grant(&vote_grant(self_id, extra_voter_2, term));
    assert_eq!(
        node.state(),
        WorkerState::Leader,
        "the second correctly-termed grant should now reach quorum-of-3"
    );
}

#[test]
fn grant_from_a_worker_outside_the_electorate_is_ignored() {
    let (mut node, self_id, _r1, _r2, extra_voter_1, _extra_voter_2, _network, term) =
        candidate_with_five_member_electorate();

    node.on_vote_grant(&vote_grant(self_id.clone(), extra_voter_1, term));
    node.on_vote_grant(&vote_grant(self_id, worker("outsider"), term));

    assert_eq!(
        node.state(),
        WorkerState::Candidate,
        "an outsider's grant must not supply the third vote needed for quorum"
    );
}

#[test]
fn repeated_grant_from_the_same_voter_counts_once() {
    let (mut node, self_id, _r1, _r2, extra_voter_1, _extra_voter_2, _network, term) =
        candidate_with_five_member_electorate();

    node.on_vote_grant(&vote_grant(self_id.clone(), extra_voter_1.clone(), term));
    node.on_vote_grant(&vote_grant(self_id, extra_voter_1, term));

    assert_eq!(node.state(), WorkerState::Candidate);
}

#[test]
fn grant_addressed_to_another_candidate_is_ignored() {
    let (mut node, self_id, _r1, _r2, extra_voter_1, extra_voter_2, _network, term) =
        candidate_with_five_member_electorate();

    node.on_vote_grant(&vote_grant(self_id, extra_voter_1, term));
    node.on_vote_grant(&vote_grant(worker("someone-else"), extra_voter_2, term));

    assert_eq!(node.state(), WorkerState::Candidate);
}

#[test]
fn grant_for_another_shard_or_recovery_epoch_is_ignored() {
    let (mut node, self_id, _r1, _r2, extra_voter_1, extra_voter_2, _network, term) =
        candidate_with_five_member_electorate();
    node.on_vote_grant(&vote_grant(self_id.clone(), extra_voter_1, term));

    let mut other_shard = vote_grant(self_id.clone(), extra_voter_2.clone(), term);
    other_shard.shard_id = Some(shard("other-shard").into());
    node.on_vote_grant(&other_shard);

    let mut other_epoch = vote_grant(self_id, extra_voter_2, term);
    other_epoch.recovery_epoch = 1;
    node.on_vote_grant(&other_epoch);

    assert_eq!(node.state(), WorkerState::Candidate);
}

#[test]
fn voted_for_keeps_first_candidate_even_on_repeat_request_from_it() {
    let clock = FakeClock::new();
    let suspect_timeout = Duration::from_ticks(10);
    let self_id = worker("w1");
    let candidate_a = worker("candidate-a");
    let candidate_b = worker("candidate-b");
    let network = make_network(
        &clock,
        &[self_id.clone(), candidate_a.clone(), candidate_b.clone()],
    );
    let mut node = make_node_with_ring(
        &clock,
        &network,
        self_id.clone(),
        &[self_id.clone(), candidate_a.clone(), candidate_b.clone()],
        suspect_timeout,
    );
    clock.advance(Duration::from_ticks(11));

    node.on_vote_request(&vote_request(candidate_a.clone(), 0, 5));
    network.pump();
    expect_single_vote_grant(network.poll_inbox(candidate_a.clone()));

    node.on_vote_request(&vote_request(candidate_b.clone(), 0, 5));
    network.pump();
    let reject = expect_single_vote_reject(network.poll_inbox(candidate_b));
    assert_eq!(reject.reason, VoteRejectReason::AlreadyVoted as i32);

    // The same candidate again is still rejected: there is no "same
    // candidate" exception, and the first vote was never overwritten.
    node.on_vote_request(&vote_request(candidate_a.clone(), 0, 5));
    network.pump();
    let reject = expect_single_vote_reject(network.poll_inbox(candidate_a));
    assert_eq!(reject.reason, VoteRejectReason::AlreadyVoted as i32);
}

fn self_remove_message(departing: &WorkerId) -> ElectionMessage {
    ElectionMessage {
        payload: Some(election_message::Payload::SelfRemove(SelfRemove {
            worker_id: Some(departing.clone().into()),
            incarnation_id: Some(IncarnationId::new("incarnation-1").into()),
            shard_id: Some(shard(SHARD).into()),
            membership_generation: 0,
        })),
    }
}

fn vote_request_message(request: VoteRequest) -> ElectionMessage {
    ElectionMessage {
        payload: Some(election_message::Payload::VoteRequest(request)),
    }
}

fn vote_grant_message(grant: VoteGrant) -> ElectionMessage {
    ElectionMessage {
        payload: Some(election_message::Payload::VoteGrant(grant)),
    }
}

#[test]
fn vote_request_from_a_sender_other_than_the_named_candidate_is_ignored() {
    let clock = FakeClock::new();
    let self_id = worker("w1");
    let candidate = worker("candidate-a");
    let impostor = worker("impostor");
    let network = make_network(
        &clock,
        &[self_id.clone(), candidate.clone(), impostor.clone()],
    );
    let mut node = make_node_with_ring(
        &clock,
        &network,
        self_id.clone(),
        &[self_id.clone(), candidate.clone(), impostor.clone()],
        Duration::from_ticks(10),
    );
    // Past the suspect timeout, so the node would otherwise grant a vote.
    clock.advance(Duration::from_ticks(11));

    node.on_message(
        impostor,
        vote_request_message(vote_request(candidate.clone(), 0, 5)),
    );

    network.pump();
    assert!(
        network.poll_inbox(candidate.clone()).is_empty(),
        "a request forged in another worker's name must get neither a grant nor a reject"
    );

    // Nothing was recorded: the genuine request for the same term is still granted.
    node.on_message(
        candidate.clone(),
        vote_request_message(vote_request(candidate.clone(), 0, 5)),
    );
    network.pump();
    expect_single_vote_grant(network.poll_inbox(candidate));
}

#[test]
fn vote_grant_from_a_sender_other_than_the_named_voter_is_ignored() {
    let (mut node, self_id, _r1, _r2, extra_voter_1, extra_voter_2, _network, term) =
        candidate_with_five_member_electorate();

    node.on_message(
        extra_voter_1.clone(),
        vote_grant_message(vote_grant(self_id.clone(), extra_voter_1, term)),
    );
    // Sent by `extra_voter_1`, but claims to be `extra_voter_2`'s vote.
    let forger = worker("peer-4");
    node.on_message(
        forger,
        vote_grant_message(vote_grant(self_id, extra_voter_2, term)),
    );

    assert_eq!(
        node.state(),
        WorkerState::Candidate,
        "a forged grant must not supply the third vote needed for quorum"
    );
}

#[test]
fn self_remove_discards_the_departed_workers_vote() {
    let (mut node, self_id, _r1, _r2, extra_voter_1, extra_voter_2, _network, term) =
        candidate_with_five_member_electorate();

    // Self-vote plus extra_voter_1 is 2 of quorum 3.
    node.on_vote_grant(&vote_grant(self_id.clone(), extra_voter_1.clone(), term));
    // The electorate shrinks to 4 (quorum still 3), and that vote must go with it.
    node.on_message(extra_voter_1.clone(), self_remove_message(&extra_voter_1));
    node.on_vote_grant(&vote_grant(self_id, extra_voter_2, term));

    assert_eq!(
        node.state(),
        WorkerState::Candidate,
        "a removed worker's vote must not count towards the current quorum"
    );
}

#[test]
fn self_remove_that_shrinks_quorum_lets_a_candidate_with_enough_votes_win() {
    let (
        mut node,
        self_id,
        responder_1,
        responder_2,
        extra_voter_1,
        _extra_voter_2,
        _network,
        term,
    ) = candidate_with_five_member_electorate();

    // Self-vote plus extra_voter_1 is 2 votes; quorum is 3 of 5.
    node.on_vote_grant(&vote_grant(self_id, extra_voter_1, term));
    node.on_message(responder_1.clone(), self_remove_message(&responder_1));
    assert_eq!(
        node.state(),
        WorkerState::Candidate,
        "2 votes, quorum 3 of 4"
    );

    // With 3 members left the quorum is 2, which the 2 recorded votes meet.
    node.on_message(responder_2.clone(), self_remove_message(&responder_2));

    assert_eq!(node.state(), WorkerState::Leader);
}

#[test]
fn vote_request_from_a_candidate_outside_the_electorate_is_ignored() {
    let clock = FakeClock::new();
    let self_id = worker("w1");
    let member = worker("candidate-a");
    let outsider = worker("outsider");
    let network = make_network(&clock, &[self_id.clone(), member.clone(), outsider.clone()]);
    let mut node = make_node_with_ring(
        &clock,
        &network,
        self_id.clone(),
        &[self_id.clone(), member.clone()],
        Duration::from_ticks(10),
    );
    // Past the suspect timeout, so the node would otherwise grant a vote.
    clock.advance(Duration::from_ticks(11));

    node.on_message(
        outsider.clone(),
        vote_request_message(vote_request(outsider.clone(), 0, 5)),
    );

    network.pump();
    assert!(
        network.poll_inbox(outsider).is_empty(),
        "a non-member candidate gets neither a grant nor a reject"
    );

    // No vote was recorded for term 5: a member's request for it is still granted.
    node.on_message(
        member.clone(),
        vote_request_message(vote_request(member.clone(), 0, 5)),
    );
    network.pump();
    expect_single_vote_grant(network.poll_inbox(member));
}
