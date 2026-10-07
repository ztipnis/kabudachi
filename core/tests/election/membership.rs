//! Membership changes while a leader lives, at one node: a draining worker
//! tells only its leader, the leader applies a removal under the term guard,
//! and a draining leader counts routing crawls by the admission it holds each
//! voter at. The multi-node behaviour of the same rules is in
//! `scenario_membership_test`.

use crate::support::builders::{
    ack_message, committed_from_g0, configuration_of, g0, heartbeat, heartbeat_message, leader_ack,
    past_any_suspicion, roll_call, roll_call_message, roll_call_reply, self_remove,
    self_remove_message, vote_grant, vote_grant_message, vote_request, vote_request_message,
    worker,
};
use crate::support::builders::checked;
use crate::support::clock::FakeClock;
use kabudachi_core::protocol::checked::{Checked, CheckedPayload};
use crate::support::node::{
    TestNode, close_roll_call, commit_founding, connect, deliver, elect, finish_reconciling,
    published_roll_calls, sent, sent_to, voter_node,
};
use kabudachi_core::configuration::{Configuration, Generation, Single};
use kabudachi_core::election::{Input, Output};
use kabudachi_core::protocol::ids::WorkerId;
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::messages::{
    AckEcho, LeaderHeartbeatAck, SelfRemove, WorkerHeartbeat,
};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::Clock;

const SHARD: &str = "shard-1";

/// Every node here suspects its leader after this many ticks.
const SUSPECT: u64 = 10;

/// `w1`, elected in term 1 leader of a configuration of 3 by `p1` and `p2`,
/// connected to both, having committed what its election founded.
fn leader_of_three(clock: &FakeClock) -> TestNode {
    let mut node = voter_node(clock, &worker("w1"), 3, SUSPECT);
    let peers = [worker("p1"), worker("p2")];
    connect(&mut node, &peers);
    elect(&mut node, clock, SUSPECT, &peers);
    commit_founding(&mut node, clock, &peers);
    node
}

/// What `leader_of_three` leads: its founding of three, committed and
/// re-based at (0, 1, 2).
fn three_voters_founded_in_term_1() -> Configuration {
    committed_from_g0(1, 1, 3)
}

/// `leader_of_three`'s configuration once one voter has left: two voters,
/// at and based at (0, 1, 3).
fn two_voters_at_the_next_generation() -> Configuration {
    single_at(Generation::new(0, 1, 3), 2)
}

fn single_at(generation: Generation, voter_count: usize) -> Configuration {
    Configuration::single(Single {
        generation,
        base: generation,
        voter_count,
    }).expect("valid")
}

/// The acks among `outputs` sent to `to`.
fn acks_to(outputs: &[Output], to: &WorkerId) -> Vec<Checked<LeaderHeartbeatAck>> {
    sent_to(outputs, to)
        .into_iter()
        .filter_map(|message| match checked(message).into_payload() {
            Some(CheckedPayload::HeartbeatAck(ack)) => Some(ack),
            _ => None,
        })
        .collect()
}

/// The one ack among `outputs` sent to `to`.
fn ack_to(outputs: &[Output], to: &WorkerId) -> Checked<LeaderHeartbeatAck> {
    let mut acks = acks_to(outputs, to);
    assert_eq!(acks.len(), 1, "expected one ack to {to:?}");
    acks.remove(0)
}

/// The self-removes among `outputs`, each with its recipient.
fn self_removes(outputs: &[Output]) -> Vec<(WorkerId, Checked<SelfRemove>)> {
    sent(outputs)
        .into_iter()
        .filter_map(|(to, message)| match checked(message).into_payload() {
            Some(CheckedPayload::SelfRemove(msg)) => Some((to, msg)),
            _ => None,
        })
        .collect()
}

/// The configuration `leader` acks `p1`'s next heartbeat with.
fn announced(leader: &mut TestNode) -> Configuration {
    let p1 = worker("p1");
    let outputs = deliver(leader, &p1, heartbeat_message(heartbeat(&p1, None)));
    ack_to(&outputs, &p1).configuration()
}

/// `departing`'s self-remove for `shard-1`, having seen at most `term_seen`.
fn remove_having_seen(node: &mut TestNode, departing: &WorkerId, term_seen: u64) -> Vec<Output> {
    let mut msg = self_remove(departing, SHARD);
    msg.term_seen = term_seen;
    deliver(node, departing, self_remove_message(msg))
}

/// A heartbeat from `sender` confirming the ack `leader` sent it at the
/// current instant, in the leader's term, and holding `held` (if any).
fn confirming_heartbeat(
    clock: &FakeClock,
    leader: &TestNode,
    sender: &WorkerId,
    held: Option<Generation>,
) -> WorkerHeartbeat {
    let mut beat = heartbeat(
        sender,
        Some(AckEcho {
            term: leader.term(),
            send_token: clock.now().as_ticks(),
        }),
    );
    beat.configuration_generation = held.map(Into::into);
    beat
}

/// A follower's heartbeat confirming its leader's latest ack and reporting a
/// routing crawl since its admission.
fn crawled(clock: &FakeClock, leader: &mut TestNode, sender: &WorkerId) -> Vec<Output> {
    let founding = leader
        .configuration()
        .map(Configuration::generation)
        .expect("a leader holds a configuration");
    crawled_at(clock, leader, sender, founding)
}

/// `crawled`, for a crawl completed while `sender` held the admission
/// `admission`.
fn crawled_at(
    clock: &FakeClock,
    leader: &mut TestNode,
    sender: &WorkerId,
    admission: Generation,
) -> Vec<Output> {
    let held = leader.configuration().map(Configuration::generation);
    let mut beat = confirming_heartbeat(clock, leader, sender, held);
    beat.routing_crawled = true;
    beat.crawl_admission = Some(admission.into());
    deliver(leader, sender, heartbeat_message(beat))
}

// ---- Draining ----

/// A follower tells only its leader that it leaves, and the message carries
/// the highest term it has seen, which a vote it granted raises. A roll call
/// it only answered does not: it counts for no quorum, and a dead one would
/// block every removal until the next election.
#[test]
fn a_follower_drains_with_one_self_remove_to_its_leader_carrying_the_highest_term_it_has_seen() {
    let clock = FakeClock::new();
    let me = worker("w1");
    let (leader, caller, candidate) = (worker("leader"), worker("caller"), worker("candidate"));
    let mut node = voter_node(&clock, &me, 3, SUSPECT);
    connect(
        &mut node,
        &[leader.clone(), caller.clone(), candidate.clone()],
    );
    // Stale leader contact: it grants `candidate` its vote in term 2 and
    // answers `caller`'s roll call for term 3; then its leader of term 2
    // reaches it again.
    clock.advance(crate::support::builders::past_any_suspicion(SUSPECT));
    deliver(
        &mut node,
        &candidate,
        vote_request_message(vote_request(candidate.clone(), 0, 2)),
    );
    deliver(
        &mut node,
        &caller,
        roll_call_message(roll_call(&caller, 3, &configuration_of(3), 0)),
    );
    deliver(
        &mut node,
        &leader,
        ack_message(leader_ack(&leader, 2, &configuration_of(3), Some(g0()))),
    );
    assert_eq!(node.state(), WorkerState::Active, "setup invariant");

    let outputs = node.step(Input::Drain).outputs;

    assert_eq!(node.state(), WorkerState::Stopped);
    let removes = self_removes(&outputs);
    assert_eq!(
        removes.len(),
        1,
        "one self-remove, to the leader: {removes:?}"
    );
    let (to, msg) = &removes[0];
    assert_eq!(*to, leader);
    assert_eq!(msg.worker_id(), me);
    assert_eq!(
        msg.term_seen, 2,
        "the vote it granted counts; the roll call it answered not"
    );
    assert_eq!(msg.leader_term, 2, "addressed to its leader's term");
}

// A heartbeat delayed from before a voter's re-admission can still say it
// has crawled: only a crawl at the admission the leader counts the voter
// by frees the leader to leave.
#[test]
fn a_draining_leader_ignores_a_crawl_report_from_an_earlier_admission() {
    let clock = FakeClock::new();
    let mut leader = leader_of_three(&clock);
    let _ = leader.step(Input::Drain);
    let _ = crawled(&clock, &mut leader, &worker("p1"));

    let stale = Generation::new(0, 0, 1);
    let _ = crawled_at(&clock, &mut leader, &worker("p2"), stale);
    assert_eq!(leader.state(), WorkerState::Leader, "p2's crawl is stale");

    let _ = crawled(&clock, &mut leader, &worker("p2"));
    assert_eq!(leader.state(), WorkerState::Stopped);
}

// A crawl reported while the founding configuration was still joint counted
// at the admission the roster held then; the commit re-admits every voter,
// so that crawl does not free the leader to leave.
#[test]
fn a_draining_leader_ignores_a_crawl_counted_before_the_commit_re_admitted_everyone() {
    let clock = FakeClock::new();
    let mut leader = voter_node(&clock, &worker("w1"), 3, SUSPECT);
    let peers = [worker("p1"), worker("p2")];
    connect(&mut leader, &peers);
    elect(&mut leader, &clock, SUSPECT, &peers);
    // p1's report arrives with the confirmation that commits the founding.
    let _ = crawled(&clock, &mut leader, &worker("p1"));
    assert!(
        leader.configuration().is_some_and(|c| !c.is_joint()),
        "setup invariant: the commit re-admitted p1"
    );
    let _ = leader.step(Input::Drain);

    let _ = crawled(&clock, &mut leader, &worker("p2"));
    assert_eq!(
        leader.state(),
        WorkerState::Leader,
        "p1's crawl was made at its earlier admission"
    );

    let _ = crawled(&clock, &mut leader, &worker("p1"));
    assert_eq!(leader.state(), WorkerState::Stopped);
}

/// A leader asked to drain that loses its lease keeps the request, and when
/// it wins office again it waits again: a crawl reported to the lost office
/// frees nothing, and it leaves only on the crawls of the new one.
#[test]
fn a_leader_that_regains_office_after_a_kept_drain_request_waits_again() {
    let clock = FakeClock::new();
    let mut leader = leader_of_three(&clock);
    let peers = [worker("p1"), worker("p2")];
    let _ = leader.step(Input::Drain);
    let _ = crawled(&clock, &mut leader, &peers[0]);
    assert_eq!(leader.state(), WorkerState::Leader, "p2 has not crawled");

    // The lease runs out with no confirmation: office is lost, the request kept.
    let lease_end = leader.step(Input::Tick).next_deadline.expect("a lease end");
    clock.advance(lease_end - clock.now());
    let _ = leader.step(Input::Tick);
    assert_eq!(leader.state(), WorkerState::NoQuorum, "setup invariant");

    // Its next roll call wins it a new term.
    clock.advance(past_any_suspicion(SUSPECT));
    let call = published_roll_calls(&leader.step(Input::Tick).outputs).remove(0);
    let me = call.initiator_id();
    let admission = leader.admission();
    for peer in &peers {
        let _ = deliver(
            &mut leader,
            peer,
            roll_call_reply(&me, call.term, peer, admission),
        );
    }
    let _ = close_roll_call(&mut leader, &clock, SUSPECT);
    assert_eq!(leader.state(), WorkerState::Candidate, "setup invariant");
    for peer in &peers {
        if leader.state() != WorkerState::Candidate {
            break;
        }
        let _ = deliver(
            &mut leader,
            peer,
            vote_grant_message(vote_grant(me.clone(), peer.clone(), call.term)),
        );
    }
    let _ = finish_reconciling(&mut leader);
    assert_eq!(
        leader.state(),
        WorkerState::Leader,
        "it leads again and waits again, though p1 crawled under the lost office"
    );
    // p2 alone commits the founding, so p1 sends the new office nothing.
    commit_founding(&mut leader, &clock, &peers[1..]);

    let _ = crawled(&clock, &mut leader, &peers[1]);
    assert_eq!(
        leader.state(),
        WorkerState::Leader,
        "p1's crawl was reported to the lost office"
    );
    let outputs = crawled(&clock, &mut leader, &peers[0]);
    assert_eq!(leader.state(), WorkerState::Stopped, "{outputs:?}");
}

// ---- The leader applies a removal ----

/// A leader applies a removal only when it is addressed to its own term, from
/// a worker that has seen no term later than it (the term guard). A worker
/// that has may have voted in a rival election of that term, where shrinking N
/// here would let two quorums miss each other; the next founding, or the
/// authority path, drops it instead.
#[test]
fn a_leader_applies_a_self_remove_only_for_its_term_from_a_worker_that_has_seen_no_later_one() {
    let clock = FakeClock::new();
    let mut leader = leader_of_three(&clock);
    let p2 = worker("p2");

    remove_having_seen(&mut leader, &p2, 2);
    let mut to_a_later_leadership = self_remove(&p2, SHARD);
    to_a_later_leadership.leader_term = 2;
    deliver(&mut leader, &p2, self_remove_message(to_a_later_leadership));
    assert_eq!(announced(&mut leader), three_voters_founded_in_term_1());

    remove_having_seen(&mut leader, &worker("p2"), 1);
    assert_eq!(announced(&mut leader), two_voters_at_the_next_generation());
}

/// A removal the leader accepted but has not applied was never announced, so
/// a leader that a later term's heartbeat deposes keeps the last
/// configuration it did announce, not the shrunk one.
#[test]
fn a_leader_deposed_with_a_removal_pending_keeps_its_last_announced_configuration() {
    let clock = FakeClock::new();
    let mut leader = leader_of_three(&clock);
    let (p1, p2) = (worker("p1"), worker("p2"));
    let announced = leader.configuration().cloned().expect("a configuration");
    deliver(
        &mut leader,
        &p2,
        self_remove_message(self_remove(&p2, SHARD)),
    );

    deliver(
        &mut leader,
        &p1,
        heartbeat_message(WorkerHeartbeat {
            term_seen: 2,
            ..heartbeat(&p1, None)
        }),
    );

    assert_eq!(leader.state(), WorkerState::LeaderSuspect);
    assert_eq!(leader.configuration(), Some(&announced));
}

#[test]
fn a_self_remove_for_another_shard_or_not_from_the_departing_worker_is_ignored() {
    let clock = FakeClock::new();
    let mut leader = leader_of_three(&clock);
    let p2 = worker("p2");

    deliver(
        &mut leader,
        &p2,
        self_remove_message(self_remove(&p2, "shard-2")),
    );
    deliver(
        &mut leader,
        &worker("p1"),
        self_remove_message(self_remove(&p2, SHARD)),
    );

    assert_eq!(announced(&mut leader), three_voters_founded_in_term_1());
}
