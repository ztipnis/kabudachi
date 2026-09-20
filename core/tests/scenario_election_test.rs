//! Scenario tests for election-message timing edge cases (README §26.1) and
//! `SELF_REMOVE` through the network layer (README §26.2), built on the
//! `Cluster` harness.
//!
//! Known gap: with 4 or more mutually reachable nodes that start suspecting at
//! the same instant, two leaders can be elected in different terms (see
//! `scenario_partition_test.rs`), so scenarios use at most 3 such nodes or
//! start a single roll call by hand through `Cluster::node()`.
//!
//! A node only becomes `Candidate` from `RollCall` and never re-accepts a roll
//! call it started, so when only one node is ever in `RollCall`, its roll call
//! cannot create a candidate wherever it travels.

mod support;

use support::builders::{
    SharedMembership, make_network, observation, roll_call, roll_call_message, shard, vote_request,
    worker,
};

use support::candidate::predict_winner;

use kabudachi_core::coordination_authority::CoordinationAuthority;
use kabudachi_core::election::WorkerNode;
use kabudachi_core::membership::{MembershipView, RingMembership};
use kabudachi_core::protocol::ids::{IncarnationId, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::messages::{
    ElectionMessage, LeaderHeartbeatAck, VoteGrant, VoteReject, VoteRejectReason, VoteRequest,
    election_message,
};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::Duration;
use kabudachi_core::transport::PeerMessenger;
use support::clock::FakeClock;
use support::coordination_authority::FakeCoordinationAuthority;
use support::harness::Cluster;
use support::network::FakeNetwork;

const SHARD: &str = "shard-1"; // Matches Cluster::bootstrap's own documented shard-1 scheme.

fn heartbeat_ack_message(leader_id: WorkerId, recovery_epoch: u64, term: u64) -> ElectionMessage {
    ElectionMessage {
        payload: Some(election_message::Payload::HeartbeatAck(
            LeaderHeartbeatAck {
                shard_id: Some(shard(SHARD).into()),
                leader_id: Some(leader_id.into()),
                recovery_epoch,
                term,
                membership_generation: 0,
            },
        )),
    }
}

fn vote_request_message(req: VoteRequest) -> ElectionMessage {
    ElectionMessage {
        payload: Some(election_message::Payload::VoteRequest(req)),
    }
}

fn expect_vote_grant(msg: ElectionMessage) -> VoteGrant {
    match msg.payload {
        Some(election_message::Payload::VoteGrant(grant)) => grant,
        other => panic!("expected a VoteGrant payload, got {other:?}"),
    }
}

fn expect_vote_reject(msg: ElectionMessage) -> VoteReject {
    match msg.payload {
        Some(election_message::Payload::VoteReject(reject)) => reject,
        other => panic!("expected a VoteReject payload, got {other:?}"),
    }
}

/// Bootstraps 3 nodes and elects a leader inside a temporary 2-vs-1
/// partition: a connected pair can only produce one candidate, whereas 3
/// simultaneous suspecters may leave a stuck `Candidate`. Returns `(cluster,
/// leader, followers)` fully settled.
fn bootstrap_and_elect_leader_n3(
    suspect_timeout: Duration,
    tick_size: Duration,
) -> (Cluster, WorkerId, Vec<WorkerId>) {
    let mut cluster = Cluster::bootstrap(3, suspect_timeout);
    let ids: Vec<WorkerId> = cluster.node_ids().into_iter().collect();
    cluster.partition(
        ids[..2].iter().cloned().collect(),
        ids[2..].iter().cloned().collect(),
    );

    for _ in 0..3 {
        cluster.advance(tick_size);
    }
    cluster.run_until_quiescent(tick_size, 60);
    cluster.heal();
    let iterations = cluster.run_until_quiescent(tick_size, 60);
    assert!(
        iterations < 60,
        "expected quiescence well before max_ticks, ran all {iterations}"
    );

    let leader = cluster
        .leader()
        .expect("the connected pair must elect a leader");
    let followers: Vec<WorkerId> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != leader)
        .collect();
    assert_eq!(followers.len(), 2);
    for follower in &followers {
        assert_eq!(
            cluster.states()[follower],
            WorkerState::Active,
            "setup invariant"
        );
    }

    (cluster, leader, followers)
}

// Scenario 1: a follower cut off from its leader starts its own roll call.
// Healing lets the leader's next real heartbeat return it to `Active` without
// the leader being challenged. Only f1 is ever in `RollCall`, so its roll call
// cannot create a candidate.
#[test]
fn leader_return_cancels_election() {
    let suspect_timeout = Duration::from_ticks(10);
    let tick_size = Duration::from_ticks(5);

    let (mut cluster, leader, followers) =
        bootstrap_and_elect_leader_n3(suspect_timeout, tick_size);
    let f1 = followers[0].clone();
    let f2 = followers[1].clone();

    cluster.partition(
        [f1.clone()].into_iter().collect(),
        [leader.clone()].into_iter().collect(),
    );

    // Flush the heartbeat scheduled before the cut.
    cluster.advance(tick_size);

    // Cross f1's suspicion boundary (one transition: Active -> LeaderSuspect).
    for _ in 0..3 {
        cluster.advance(tick_size);
    }
    assert_eq!(
        cluster.states()[&f1],
        WorkerState::LeaderSuspect,
        "f1 must have timed out, having received no heartbeat since the partition"
    );
    assert_eq!(
        cluster.states()[&leader],
        WorkerState::Leader,
        "setup invariant: leader unaffected"
    );
    assert_eq!(
        cluster.states()[&f2],
        WorkerState::Active,
        "setup invariant: f2 kept receiving real heartbeats throughout"
    );

    // Start only f1's roll call by hand; its forward stays scheduled, undelivered.
    cluster.node(&f1).tick();
    assert_eq!(
        cluster.states()[&f1],
        WorkerState::RollCall,
        "f1 must have begun its own roll call"
    );
    assert_ne!(
        cluster.states()[&f1],
        WorkerState::Candidate,
        "f1 must not have reached quorum/become Candidate from its own lone observation"
    );

    // Heal before the network is pumped again.
    cluster.heal();
    for _ in 0..3 {
        cluster.advance(tick_size);
    }

    assert_eq!(
        cluster.states()[&f1],
        WorkerState::Active,
        "a real, network-delivered heartbeat from the leader must return f1 RollCall -> Active"
    );
    assert_eq!(
        cluster.leader(),
        Some(leader.clone()),
        "the original leader must never have been displaced or challenged"
    );
    cluster.assert_at_most_one_leader();
}

// Scenario 2: after the old leader is isolated and replaced, a stale ack
// carrying its old term is rejected by a follower now under the new leader
// (`ack.term < highest_term_seen`), through the full harness.
#[test]
fn stale_message_after_replacement_is_rejected() {
    let suspect_timeout = Duration::from_ticks(10);
    let tick_size = Duration::from_ticks(5);

    let (mut cluster, old_leader, followers) =
        bootstrap_and_elect_leader_n3(suspect_timeout, tick_size);
    let a = followers[0].clone();
    let b = followers[1].clone();

    // The first roll call of a fresh cluster contests term 1.
    let old_term = 1;

    cluster.partition(
        [old_leader.clone()].into_iter().collect(),
        [a.clone(), b.clone()].into_iter().collect(),
    );
    cluster.advance(tick_size); // Flush one in-flight heartbeat (established idiom).
    for _ in 0..3 {
        cluster.advance(tick_size);
    }
    cluster.run_until_quiescent(tick_size, 60);

    let new_leader = cluster
        .leader()
        .expect("the 2-node majority (a, b) must independently elect a new leader");
    assert_ne!(new_leader, old_leader);
    let probe = if new_leader == a {
        b.clone()
    } else {
        a.clone()
    };
    assert_eq!(
        cluster.states()[&probe],
        WorkerState::Active,
        "setup invariant: the non-leader majority member must have settled Active"
    );

    // Cut probe off from the new leader. old_leader goes with new_leader so
    // the new leader still sees a quorum (2 of 3) and stays Leader.
    // `FakeNetwork` holds one partition, so this replaces the previous one.
    cluster.partition(
        [probe.clone()].into_iter().collect(),
        [old_leader.clone(), new_leader.clone()]
            .into_iter()
            .collect(),
    );
    assert_eq!(
        cluster.states()[&old_leader],
        WorkerState::NoQuorum,
        "setup invariant: the isolated old leader must have detected its own peer loss"
    );

    cluster.advance(tick_size); // Flush one in-flight heartbeat to probe.
    for _ in 0..3 {
        cluster.advance(tick_size);
    }
    assert_eq!(
        cluster.states()[&probe],
        WorkerState::LeaderSuspect,
        "setup invariant"
    );

    // Start only probe's roll call by hand (as in scenario 1).
    cluster.node(&probe).tick();
    assert_eq!(
        cluster.states()[&probe],
        WorkerState::RollCall,
        "setup invariant"
    );

    // A stale ack from the old leader: term 1, while probe's
    // highest_term_seen is 2. It must be rejected.
    cluster.node(&probe).on_message(
        old_leader.clone(),
        heartbeat_ack_message(old_leader.clone(), 0, old_term),
    );
    assert_eq!(
        cluster.states()[&probe],
        WorkerState::RollCall,
        "a stale LeaderHeartbeatAck (term 1, below the real current term 2) must be rejected \
         outright and must NOT flip probe out of RollCall"
    );

    // Positive control: a genuine current-term heartbeat still gets through.
    cluster.heal();
    cluster.run_until_quiescent(tick_size, 60);
    assert_eq!(
        cluster.states()[&probe],
        WorkerState::Active,
        "a genuine, current-term heartbeat must still correctly flip probe RollCall -> Active"
    );

    cluster.assert_at_most_one_leader();
    assert_eq!(cluster.leader(), Some(new_leader));
}

// Scenario 3: two candidates for the same term send crossing vote requests to
// a shared voter z. Whichever request arrives second is rejected
// `AlreadyVoted`, in either order, so at most one candidate reaches quorum.
//
// z is the lowest-priority worker, so x and y each beat it in a hand-fed
// 2-candidate roll call and both become real `Candidate`s for term 1. After
// starting x and y the test never calls `Cluster::advance()`: it pumps the
// network and processes only z's inbox, so stray forwards are never handled.
fn find_two_that_beat_third(
    candidates: &[WorkerId],
    next_term: u64,
) -> (WorkerId, WorkerId, WorkerId) {
    let z = candidates
        .iter()
        .find(|candidate| {
            candidates.iter().all(|other| {
                other == *candidate
                    || predict_winner(
                        &shard(SHARD),
                        0,
                        next_term,
                        &[(*candidate).clone(), other.clone()],
                    ) != **candidate
            })
        })
        .cloned()
        .expect("3 distinct candidates always have a strict total ranking by fixed hash score");
    let others: Vec<WorkerId> = candidates.iter().filter(|id| **id != z).cloned().collect();
    (others[0].clone(), others[1].clone(), z)
}

/// Runs the scenario once, delivering x's request to z before y's if `x_first`.
fn run_two_candidates_scenario(x_first: bool) {
    let suspect_timeout = Duration::from_ticks(10);
    let tick_size = Duration::from_ticks(5);
    let next_term = 1;

    let mut cluster = Cluster::bootstrap(3, suspect_timeout);
    let ids: Vec<WorkerId> = cluster.node_ids().into_iter().collect();
    let (x, y, z) = find_two_that_beat_third(&ids, next_term);

    for _ in 0..3 {
        cluster.advance(tick_size);
    }
    for id in &ids {
        assert_eq!(
            cluster.states()[id],
            WorkerState::LeaderSuspect,
            "setup invariant"
        );
    }

    // Start only x and y; z stays parked in LeaderSuspect, which can still vote.
    cluster.node(&x).tick();
    cluster.node(&y).tick();
    assert_eq!(
        cluster.states()[&x],
        WorkerState::RollCall,
        "setup invariant"
    );
    assert_eq!(
        cluster.states()[&y],
        WorkerState::RollCall,
        "setup invariant"
    );

    // Hand-deliver a roll call carrying z's observation to x and y; each beats
    // z and becomes Candidate.
    let call_for_x = roll_call(
        "crossing-call-x",
        z.clone(),
        vec![observation(z.clone(), 0)],
    );
    cluster
        .node(&x)
        .on_message(z.clone(), roll_call_message(call_for_x));
    assert_eq!(
        cluster.states()[&x],
        WorkerState::Candidate,
        "x must win its own 2-candidate race against z"
    );

    let call_for_y = roll_call(
        "crossing-call-y",
        z.clone(),
        vec![observation(z.clone(), 0)],
    );
    cluster
        .node(&y)
        .on_message(z.clone(), roll_call_message(call_for_y));
    assert_eq!(
        cluster.states()[&y],
        WorkerState::Candidate,
        "y must win its own 2-candidate race against z"
    );

    // Both candidates have now sent z a real VoteRequest.
    let delivered = cluster.network().pump();
    assert!(
        delivered >= 2,
        "expected at least x's and y's real VoteRequests to become due"
    );
    let z_inbox = cluster.network().poll_inbox(z.clone());

    let mut req_from_x: Option<(WorkerId, VoteRequest)> = None;
    let mut req_from_y: Option<(WorkerId, VoteRequest)> = None;
    for (from, msg) in z_inbox {
        if let Some(election_message::Payload::VoteRequest(req)) = &msg.payload {
            if req.candidate_id() == x {
                req_from_x = Some((from, req.clone()));
            } else if req.candidate_id() == y {
                req_from_y = Some((from, req.clone()));
            }
        }
        // Any other message (e.g. a stray roll-call forward) is dropped.
    }
    let (from_x, req_x) = req_from_x.expect("z's inbox must contain a real VoteRequest from x");
    let (from_y, req_y) = req_from_y.expect("z's inbox must contain a real VoteRequest from y");
    assert_eq!(req_x.term, next_term);
    assert_eq!(req_y.term, next_term);

    let (first_from, first_req, first_id, second_from, second_req, second_id) = if x_first {
        (from_x, req_x, x.clone(), from_y, req_y, y.clone())
    } else {
        (from_y, req_y, y.clone(), from_x, req_x, x.clone())
    };

    cluster
        .node(&z)
        .on_message(first_from, vote_request_message(first_req));
    cluster.network().pump();
    let first_reply = cluster
        .network()
        .poll_inbox(first_id.clone())
        .pop()
        .map(|(_, msg)| msg)
        .expect("the first-arriving candidate must receive a reply");
    let grant = expect_vote_grant(first_reply);
    assert_eq!(
        grant.candidate_id(),
        first_id,
        "the first request must be GRANTED"
    );
    assert_eq!(grant.voter_id(), z);

    cluster
        .node(&z)
        .on_message(second_from, vote_request_message(second_req));
    cluster.network().pump();
    let second_reply = cluster
        .network()
        .poll_inbox(second_id.clone())
        .pop()
        .map(|(_, msg)| msg)
        .expect("the second-arriving candidate must receive a reply");
    let reject = expect_vote_reject(second_reply);
    assert_eq!(
        reject.candidate_id(),
        second_id,
        "the second request must be REJECTED"
    );
    assert_eq!(
        reject.reason,
        VoteRejectReason::AlreadyVoted as i32,
        "the second request must be rejected specifically as AlreadyVoted"
    );

    // The winner reaches quorum with the grant; the reject is a no-op.
    cluster.node(&first_id).on_message(
        z.clone(),
        ElectionMessage {
            payload: Some(election_message::Payload::VoteGrant(grant)),
        },
    );
    cluster.node(&second_id).on_message(
        z.clone(),
        ElectionMessage {
            payload: Some(election_message::Payload::VoteReject(reject)),
        },
    );

    assert_eq!(
        cluster.states()[&first_id],
        WorkerState::Leader,
        "the granted candidate must reach real, quorum-granted Leader status"
    );
    assert_eq!(
        cluster.states()[&second_id],
        WorkerState::Candidate,
        "the rejected candidate must remain stuck at Candidate — never Leader — for this term"
    );
    cluster.assert_at_most_one_leader();
    assert_eq!(cluster.leader(), Some(first_id));
}

#[test]
fn two_candidates_crossing_vote_requests() {
    run_two_candidates_scenario(true);
    run_two_candidates_scenario(false);
}

// Scenario 4: stale election messages after a new recovery epoch are
// rejected. Built on a bare `WorkerNode` because `Cluster` exposes no
// authority accessor and the authority must be seeded before forced recovery
// can succeed.

#[allow(clippy::type_complexity)]
fn make_node_with_ring<A: CoordinationAuthority + Clone>(
    clock: &FakeClock,
    network: &FakeNetwork,
    authority: &A,
    my_id: WorkerId,
    electorate: &[WorkerId],
    suspect_timeout: Duration,
) -> WorkerNode<FakeClock, FakeNetwork, RingMembership, A> {
    let membership = RingMembership::new(electorate.iter().cloned().collect());
    WorkerNode::new(
        my_id,
        IncarnationId::new("incarnation-1"),
        shard(SHARD),
        clock.clone(),
        network.clone(),
        membership,
        authority.clone(),
        suspect_timeout,
    )
}

/// Drives a fresh node to a real `Leader` with a 3-member electorate through roll call and voting.
#[allow(clippy::type_complexity)]
fn leader_with_electorate<A: CoordinationAuthority + Clone>(
    clock: &FakeClock,
    authority: &A,
) -> (
    WorkerNode<FakeClock, FakeNetwork, RingMembership, A>,
    WorkerId,
    WorkerId,
    WorkerId,
    FakeNetwork,
) {
    let suspect_timeout = Duration::from_ticks(10);

    let candidate_x = worker("candidate-x");
    let candidate_y = worker("candidate-y");
    let next_term = 1;
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

    let network = make_network(clock, &[self_id.clone(), peer_a.clone(), peer_b.clone()]);
    let mut node = make_node_with_ring(
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

    let call = roll_call(
        "external-call-1",
        peer_a.clone(),
        vec![observation(peer_a.clone(), 0)],
    );
    node.on_message(peer_a.clone(), roll_call_message(call));
    assert_eq!(node.state(), WorkerState::Candidate, "test setup invariant");

    let grant = VoteGrant {
        shard_id: Some(shard(SHARD).into()),
        recovery_epoch: 0,
        term: next_term,
        candidate_id: Some(self_id.clone().into()),
        voter_id: Some(peer_a.clone().into()),
    };
    node.on_vote_grant(&grant);
    assert_eq!(node.state(), WorkerState::Leader, "test setup invariant");

    network.pump();
    for id in [self_id.clone(), peer_a.clone(), peer_b.clone()] {
        let _ = network.poll_inbox(id);
    }

    (node, self_id, peer_a, peer_b, network)
}

/// Drives a fresh node to `NoQuorum` through the leader's peer-loss edge.
#[allow(clippy::type_complexity)]
fn leader_in_no_quorum<A: CoordinationAuthority + Clone>(
    clock: &FakeClock,
    authority: &A,
) -> (
    WorkerNode<FakeClock, FakeNetwork, RingMembership, A>,
    WorkerId,
    WorkerId,
    WorkerId,
    FakeNetwork,
) {
    let (mut node, self_id, peer_a, peer_b, network) = leader_with_electorate(clock, authority);

    network.partition(
        [self_id.clone()].into_iter().collect(),
        [peer_a.clone(), peer_b.clone()].into_iter().collect(),
    );
    node.tick(); // Leader -> NoQuorum.
    assert_eq!(node.state(), WorkerState::NoQuorum, "test setup invariant");

    (node, self_id, peer_a, peer_b, network)
}

#[test]
fn stale_election_messages_after_new_epoch() {
    let clock = FakeClock::new();
    let authority = FakeCoordinationAuthority::new();
    let (mut node, _self_id, _peer_a, _peer_b, network) = leader_in_no_quorum(&clock, &authority);

    let old_recovery_epoch = 0;

    // Seed the authority so the reachable-intersection guard passes.
    // helper_peer is the stand-in candidate because self_id stays partitioned
    // from peer_a/peer_b, so replies to them would be dropped.
    let helper_peer = worker("helper-peer");
    network.register(helper_peer.clone());
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
        "setup invariant: the authority's epoch must have advanced past the seed bump"
    );

    // Drain the roll call forced recovery just forwarded, so the reply count
    // below only sees the reply to the stale request.
    network.pump();
    let _ = network.poll_inbox(helper_peer.clone());

    // A stale request: pre-recovery epoch 0 against the node's freshly bumped
    // epoch 2. The epoch check comes before any term logic, so it is rejected
    // WrongRecoveryEpoch.
    let stale_term = 1;
    node.on_vote_request(&vote_request(
        helper_peer.clone(),
        old_recovery_epoch,
        stale_term,
    ));

    network.pump();
    let mut helper_peer_inbox = network.poll_inbox(helper_peer.clone());
    assert_eq!(
        helper_peer_inbox.len(),
        1,
        "expected exactly one reply to the stale request"
    );
    let stale_reject = expect_vote_reject(helper_peer_inbox.remove(0).1);
    assert_eq!(
        stale_reject.reason,
        VoteRejectReason::WrongRecoveryEpoch as i32,
        "the stale (pre-recovery) recovery_epoch must be rejected specifically as WrongRecoveryEpoch"
    );
    assert_eq!(
        node.state(),
        WorkerState::RollCall,
        "processing the stale request must leave state completely unchanged"
    );

    // Positive control: a legitimate request for the same term at the current
    // epoch is still granted, so the stale one left no trace in `voted_for`.
    let legit_candidate = helper_peer.clone();
    node.on_vote_request(&vote_request(legit_candidate.clone(), 2, stale_term));

    network.pump();
    let mut legit_inbox = network.poll_inbox(legit_candidate.clone());
    assert_eq!(
        legit_inbox.len(),
        1,
        "expected exactly one reply to the legitimate request"
    );
    let grant = expect_vote_grant(legit_inbox.remove(0).1);
    assert_eq!(
        grant.candidate_id(),
        legit_candidate,
        "a legitimate, current-epoch request for the same term must be genuinely GRANTED, proving \
         the earlier stale request left no trace in voted_for"
    );
}

// Scenario 5: a delayed `SelfRemove` must not cause a lasting quorum
// miscalculation: the receiver's quorum reflects the old electorate until the
// message is processed and the shrunk one afterwards. Electorate size is
// inferred from whether a synthetic roll call with one external response
// reaches quorum (declined before, accepted after).
#[test]
fn self_remove_delayed() {
    let suspect_timeout = Duration::from_ticks(10);
    let tick_size = Duration::from_ticks(5);
    let delay = Duration::from_ticks(20); // > suspect_timeout crossing, so the drain never races it.

    let mut cluster = Cluster::bootstrap(4, suspect_timeout);
    let ids: Vec<WorkerId> = cluster.node_ids().into_iter().collect();
    let drainer = ids[0].clone();
    let rest: Vec<WorkerId> = ids[1..].to_vec(); // 3 remaining nodes: quorum(4)=3, quorum(3)=2.
    // The receiver is the highest-priority of the three, so it wins any
    // 2-candidate race it is part of at term 1.
    let receiver = predict_winner(&shard(SHARD), 0, 1, &rest);
    let mut others = rest.iter().filter(|id| **id != receiver).cloned();
    let other_a = others.next().unwrap();
    let other_b = others.next().unwrap();

    cluster.network().set_delay(delay);
    cluster.drain(&drainer); // Active -> Stopped; broadcasts a SelfRemove delayed by 20 ticks.
    assert_eq!(cluster.states()[&drainer], WorkerState::Stopped);

    // Cross suspicion for the remaining nodes; 15 ticks is short of the
    // 20-tick delay, so the SelfRemove is not yet due.
    for _ in 0..3 {
        cluster.advance(tick_size);
    }
    for id in &rest {
        assert_eq!(
            cluster.states()[id],
            WorkerState::LeaderSuspect,
            "setup invariant"
        );
    }

    // Start only receiver's roll call; its own sends share the 20-tick delay
    // and stay undelivered.
    cluster.node(&receiver).tick();
    assert_eq!(
        cluster.states()[&receiver],
        WorkerState::RollCall,
        "setup invariant"
    );

    // Before: the SelfRemove is undelivered, so the electorate is still 4
    // (quorum 3); one external response (2 total) must be declined.
    let before_call = roll_call(
        "before-self-remove",
        other_a.clone(),
        vec![observation(other_a.clone(), 0)],
    );
    cluster
        .node(&receiver)
        .on_message(other_a.clone(), roll_call_message(before_call));
    assert_eq!(
        cluster.states()[&receiver],
        WorkerState::RollCall,
        "with the OLD (undiminished, 4-member) electorate still in effect, 2 total responses must \
         be insufficient for quorum-3 — this directly proves the pre-delivery quorum size is still 4"
    );

    // Cross the delay. The other two nodes also start roll calls, but their
    // sends are delayed too and receiver's state is driven only by the direct
    // on_message calls.
    cluster.advance(Duration::from_ticks(5)); // t: 15 -> 20, delivering the SelfRemove.
    assert_eq!(
        cluster.states()[&receiver],
        WorkerState::RollCall,
        "processing the SelfRemove itself causes no direct state transition"
    );

    // After: receiver processed the SelfRemove, so the electorate is 3
    // (quorum 2); the same call shape must now be accepted, which can only be
    // due to the shrink.
    let next_term = 1; // both observations carry highest_term_seen: 0.
    let winner = predict_winner(
        &shard(SHARD),
        0,
        next_term,
        &[receiver.clone(), other_b.clone()],
    );
    assert_eq!(
        winner, receiver,
        "test construction invariant: receiver must win its 2-candidate race against other_b at term 1 \
         (adjust the participant choice above if this ever fails for a different candidate set)"
    );
    let after_call = roll_call(
        "after-self-remove",
        other_b.clone(),
        vec![observation(other_b.clone(), 0)],
    );
    cluster
        .node(&receiver)
        .on_message(other_b.clone(), roll_call_message(after_call));
    assert_eq!(
        cluster.states()[&receiver],
        WorkerState::Candidate,
        "with the NEW (shrunk, 3-member) electorate now in effect, the SAME 2-total-response shape \
         that was insufficient before is now sufficient for quorum-2 — this directly proves the \
         post-delivery quorum size is now 3, reflecting the SelfRemove exactly once"
    );
}

// =============================================================================
// Scenario 6: self_remove_duplicated
// =============================================================================
//
// With `FakeNetwork::set_duplicate_rate(1.0)` every send is scheduled twice.
// `SelfRemove` handling must be idempotent through the network layer: both
// copies are delivered and dispatched through `on_message`, and the
// electorate shrink and generation bump are then read straight off the
// receiver's `MembershipView` (via `SharedMembership`), before and after.
//
// Reading the membership directly matters: `quorum(k) = k/2 + 1` gives the
// same value for adjacent sizes, so no quorum-threshold check can tell "did
// not shrink", "shrank once" and "shrank twice" apart. The test asserts
// `effective_electorate().len() == 2` and `membership_generation() == 1`.
#[allow(clippy::type_complexity)]
fn make_node_with_shared_membership<A: CoordinationAuthority + Clone>(
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

#[test]
fn self_remove_duplicated() {
    let clock = FakeClock::new();
    let suspect_timeout = Duration::from_ticks(10);
    let authority = FakeCoordinationAuthority::new();

    let drainer = worker("drainer");
    let receiver = worker("receiver");
    let third = worker("third"); // Present purely so the electorate has size 3, not 2.
    let electorate = [drainer.clone(), receiver.clone(), third.clone()];
    let network = make_network(&clock, &electorate);
    network.set_duplicate_rate(1.0);

    let (mut drainer_node, _drainer_membership) = make_node_with_shared_membership(
        &clock,
        &network,
        &authority,
        drainer.clone(),
        &electorate,
        suspect_timeout,
    );
    let (mut receiver_node, receiver_membership) = make_node_with_shared_membership(
        &clock,
        &network,
        &authority,
        receiver.clone(),
        &electorate,
        suspect_timeout,
    );

    assert_eq!(receiver_membership.effective_electorate().len(), 3);
    assert_eq!(receiver_membership.membership_generation(), 0);

    drainer_node.begin_drain(); // Active -> Stopped; broadcasts a SelfRemove, duplicated to every reachable peer.
    assert_eq!(drainer_node.state(), WorkerState::Stopped);

    // Confirm duplication really happened: rate 1.0 delivers exactly 2 copies.
    network.pump();
    let inbox = network.poll_inbox(receiver.clone());
    assert_eq!(
        inbox.len(),
        2,
        "receiver must have received exactly 2 real copies of the SelfRemove"
    );
    for (from, msg) in &inbox {
        assert_eq!(*from, drainer);
        match &msg.payload {
            Some(election_message::Payload::SelfRemove(sr)) => assert_eq!(sr.worker_id(), drainer),
            other => panic!("expected a SelfRemove payload, got {other:?}"),
        }
    }
    for (from, msg) in inbox {
        receiver_node.on_message(from, msg);
    }

    // Exactly one shrink and one generation bump despite two deliveries.
    assert_eq!(
        receiver_membership.effective_electorate().len(),
        2,
        "the electorate must have shrunk by EXACTLY one member"
    );
    assert!(
        !receiver_membership
            .effective_electorate()
            .contains(&drainer),
        "the drainer specifically must be the member removed"
    );
    assert_eq!(
        receiver_membership.membership_generation(),
        1,
        "membership_generation must have incremented EXACTLY once despite the duplicate delivery"
    );
}
