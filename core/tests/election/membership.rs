//! Membership changes while a leader lives, at one node (README §12.3,
//! ADR-0001 decisions 9 and 10): a draining worker tells only its leader,
//! the leader applies a removal under the term guard (E4c-R1) and announces
//! the shrunk configuration on its acks, a draining leader announces its own
//! departure (E4c-R3c), and a leader admits pending joiners in batches. The
//! multi-node behaviour of the same rules is in `scenario_membership_test`.

use crate::support::builders::{
    ack_message, committed_from_g0, configuration_of, g0, heartbeat, heartbeat_message, leader_ack,
    roll_call, roll_call_message, self_remove, self_remove_message, vote_request,
    vote_request_message, worker,
};
use crate::support::builders::checked;
use crate::support::clock::FakeClock;
use kabudachi_core::protocol::checked::{Checked, CheckedPayload};
use crate::support::node::{
    TestNode, commit_founding, connect, deliver, elect, sent, sent_to, state_changes, voter_node,
};
use kabudachi_core::configuration::{Configuration, Generation, Joint, Single};
use kabudachi_core::election::{Input, Output};
use kabudachi_core::protocol::ids::WorkerId;
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::messages::{
    AckEcho, LeaderHeartbeatAck, SelfRemove, WorkerHeartbeat,
};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::{Clock, Duration};

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

// ---- Draining ----

/// A follower tells only its leader that it leaves (E4c: "SELF_REMOVE to
/// the leader only"), and the message carries the highest term it has seen
/// (E4c-R1), which a vote it granted raises. A roll call it only answered
/// does not: it counts for no quorum, and a dead one would block every
/// removal until the next election.
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

#[test]
fn a_node_that_knows_no_leader_drains_without_telling_anyone() {
    let clock = FakeClock::new();
    let mut node = voter_node(&clock, &worker("w1"), 3, SUSPECT);
    connect(&mut node, &[worker("w2"), worker("w3")]);

    let outputs = node.step(Input::Drain).outputs;

    assert_eq!(node.state(), WorkerState::Stopped);
    assert_eq!(
        state_changes(&outputs),
        vec![WorkerState::Draining, WorkerState::Stopped]
    );
    assert!(
        sent(&outputs).is_empty(),
        "the next founding, or the authority path, drops it"
    );
}

/// A draining leader applies its own removal, which no other leader can
/// (E4c-R3c), and announces the configuration without itself on a final
/// ack to every peer before it stops.
#[test]
fn a_draining_leader_announces_the_configuration_without_itself_on_final_acks() {
    let clock = FakeClock::new();
    let mut leader = leader_of_three(&clock);

    let outputs = leader.step(Input::Drain).outputs;

    assert_eq!(leader.state(), WorkerState::Stopped);
    for follower in [worker("p1"), worker("p2")] {
        let ack = ack_to(&outputs, &follower);
        assert_eq!(ack.configuration(), two_voters_at_the_next_generation());
        assert_eq!(
            ack.recipient_admission(),
            Some(Generation::new(0, 1, 3)),
            "each remaining voter re-admitted where the removal re-based"
        );
    }
    assert!(self_removes(&outputs).is_empty());
}

#[test]
fn a_lone_leader_drains_without_announcing_anything() {
    let clock = FakeClock::new();
    let mut node = voter_node(&clock, &worker("solo"), 1, SUSPECT);
    connect(&mut node, &[worker("pending")]);
    elect(&mut node, &clock, SUSPECT, &[]);

    let outputs = node.step(Input::Drain).outputs;

    assert_eq!(node.state(), WorkerState::Stopped);
    assert!(
        acks_to(&outputs, &worker("pending")).is_empty() && self_removes(&outputs).is_empty(),
        "no configuration of zero voters is ever announced"
    );
}

// ---- The leader applies a removal ----

#[test]
fn a_leader_drops_a_removed_member_and_announces_one_fewer_voter_at_the_next_generation() {
    let clock = FakeClock::new();
    let mut leader = leader_of_three(&clock);
    assert_eq!(
        announced(&mut leader),
        three_voters_founded_in_term_1(),
        "setup invariant"
    );

    remove_having_seen(&mut leader, &worker("p2"), 1);

    assert_eq!(announced(&mut leader), two_voters_at_the_next_generation());
    assert_eq!(leader.admission(), Some(Generation::new(0, 1, 3)));
    let _ = leader.step(Input::PeerDisconnected(worker("p2")));
    let reconnect = leader.step(Input::PeerConnected(worker("p2"))).outputs;
    assert_eq!(
        ack_to(&reconnect, &worker("p2")).recipient_admission(),
        None,
        "a removed worker is no longer in the roster"
    );
}

/// A leader applies a removal only when it is addressed to its own term,
/// from a worker that has seen no term later than it (ADR-0001 decision
/// 10's term guard). A worker that has may have voted in a rival election
/// of that term, where shrinking N here would let two quorums miss each
/// other; the next founding, or the authority path, drops it instead.
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

/// A leader whose lease would run out only because a departed worker's
/// last confirmation has aged applies the removals it has accepted first,
/// and keeps leading the shrunk configuration if that still has its quorum.
/// Of four voters it needs two others' confirmations; once one of them has
/// left, three need only one.
#[test]
fn a_leader_applies_pending_removals_before_it_would_lose_its_quorum() {
    let clock = FakeClock::new();
    let mut leader = voter_node(&clock, &worker("w1"), 4, SUSPECT);
    let peers = [worker("p1"), worker("p2"), worker("p3")];
    connect(&mut leader, &peers);
    elect(&mut leader, &clock, SUSPECT, &peers);
    commit_founding(&mut leader, &clock, &peers);
    let committed = leader
        .configuration()
        .expect("a configuration")
        .generation();
    let lease = crate::support::builders::timings(Duration::from_ticks(SUSPECT))
        .lease_length()
        .as_ticks();

    clock.advance(Duration::from_ticks(lease - 1));
    let beat = confirming_heartbeat(&clock, &leader, &worker("p1"), Some(committed));
    deliver(&mut leader, &worker("p1"), heartbeat_message(beat));
    remove_having_seen(&mut leader, &worker("p2"), 1);
    clock.advance(Duration::from_ticks(2));
    let _ = leader.step(Input::Tick);

    assert_eq!(leader.state(), WorkerState::Leader);
    assert_eq!(
        leader.configuration(),
        Some(&single_at(committed.next_change(1), 3))
    );
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

// ---- Admission batches ----

/// The batch `leader_of_three` starts for one joiner: a joint configuration
/// at (0, 1, 3), its old side the committed three, its new side those three
/// re-admitted and the joiner.
fn batch_of_one_joiner() -> Configuration {
    let (committed, batch) = (Generation::new(0, 1, 2), Generation::new(0, 1, 3));
    Configuration::joint(Joint {
        generation: batch,
        base: batch,
        batch_generation: batch,
        old_base: committed,
        old_generation: committed,
        old_voter_count: 3,
        new_voter_count: 4,
    }).expect("valid")
}

/// A leader admits a pending joiner once the joiner has confirmed one of its
/// acks, so the batch's new side never costs the leader its lease (ADR-0001
/// decision 9): a joiner heard of but not yet confirming waits pending.
#[test]
fn a_leader_admits_a_pending_joiner_that_confirms_its_ack_in_a_batch() {
    let clock = FakeClock::new();
    let mut leader = leader_of_three(&clock);
    let joiner = worker("joiner");
    connect(&mut leader, std::slice::from_ref(&joiner));

    let first = deliver(
        &mut leader,
        &joiner,
        heartbeat_message(heartbeat(&joiner, None)),
    );
    assert_eq!(
        ack_to(&first, &joiner).recipient_admission(),
        None,
        "pending"
    );
    assert_eq!(
        leader.configuration(),
        Some(&three_voters_founded_in_term_1())
    );

    let beat = confirming_heartbeat(&clock, &leader, &joiner, None);
    let admitted = deliver(&mut leader, &joiner, heartbeat_message(beat));

    let ack = ack_to(&admitted, &joiner);
    assert_eq!(ack.configuration(), batch_of_one_joiner());
    assert_eq!(ack.recipient_admission(), Some(Generation::new(0, 1, 3)));
    assert_eq!(ack.recipient_prior_admission(), None);
    assert_eq!(leader.configuration(), Some(&batch_of_one_joiner()));
    assert_eq!(leader.prior_admission(), Some(Generation::new(0, 1, 2)));
}

/// A leader admits a joiner only on a recent confirmation: of an ack sent
/// within the last two heartbeat intervals. A lone leader's lease is
/// unbounded, and a batch bounds it by the joiner's confirmation, so one
/// held back by a stall would leave it almost no lease at all.
#[test]
fn a_lone_leader_admits_no_joiner_on_a_stale_confirmation() {
    let clock = FakeClock::new();
    let mut leader = voter_node(&clock, &worker("solo"), 1, SUSPECT);
    let joiner = worker("joiner");
    connect(&mut leader, std::slice::from_ref(&joiner));
    elect(&mut leader, &clock, SUSPECT, &[]);
    let sent_at = clock.now().as_ticks();
    let heartbeat_interval = crate::support::builders::timings(Duration::from_ticks(SUSPECT))
        .heartbeat_interval
        .as_ticks();

    clock.advance(Duration::from_ticks(2 * heartbeat_interval + 1));
    let stale = heartbeat(
        &joiner,
        Some(AckEcho {
            term: leader.term(),
            send_token: sent_at,
        }),
    );
    let held_back = deliver(&mut leader, &joiner, heartbeat_message(stale));
    assert_eq!(
        ack_to(&held_back, &joiner).recipient_admission(),
        None,
        "still pending"
    );

    let recent = confirming_heartbeat(&clock, &leader, &joiner, None);
    let admitted = deliver(&mut leader, &joiner, heartbeat_message(recent));
    assert!(ack_to(&admitted, &joiner).recipient_admission().is_some());
}

/// While a leader's lease is bounded, a joiner is admitted on a confirmation
/// older than two heartbeat intervals so long as it is no older than the
/// quorum-contact time: the batch then still leaves the lease worth having,
/// and a joiner heartbeating out of phase with the voters does not wait
/// round after round (`Lease::admissible`).
#[test]
fn a_leader_admits_a_joiner_confirmed_before_two_heartbeat_intervals_but_since_the_quorum_contact() {
    let clock = FakeClock::new();
    let mut leader = leader_of_three(&clock);
    let joiner = worker("joiner");
    connect(&mut leader, std::slice::from_ref(&joiner));
    // The voters confirmed the founding's ack now: the quorum-contact time.
    let quorum_contact = clock.now().as_ticks();
    let heartbeat_interval = crate::support::builders::timings(Duration::from_ticks(SUSPECT))
        .heartbeat_interval
        .as_ticks();
    clock.advance(Duration::from_ticks(2 * heartbeat_interval + 1));
    let term = leader.term();
    let echoing = |send_token| {
        heartbeat_message(heartbeat(
            &joiner,
            Some(AckEcho {
                term,
                send_token,
            }),
        ))
    };

    let before_contact = deliver(&mut leader, &joiner, echoing(quorum_contact - 1));
    assert_eq!(
        ack_to(&before_contact, &joiner).recipient_admission(),
        None,
        "older than the quorum contact: still pending"
    );

    let since_contact = deliver(&mut leader, &joiner, echoing(quorum_contact));
    assert_eq!(
        ack_to(&since_contact, &joiner).recipient_admission(),
        Some(Generation::new(0, 1, 3))
    );
}

/// A batch commits once a majority of each side holds it, and a joiner
/// arriving meanwhile waits for the next batch, which the commit starts.
#[test]
fn a_batch_commits_on_a_majority_of_each_side_and_the_next_one_admits_those_who_waited() {
    let clock = FakeClock::new();
    let mut leader = leader_of_three(&clock);
    let (joiner, late) = (worker("joiner"), worker("late"));
    connect(&mut leader, &[joiner.clone(), late.clone()]);
    let beat = confirming_heartbeat(&clock, &leader, &joiner, None);
    deliver(&mut leader, &joiner, heartbeat_message(beat));
    let batch = batch_of_one_joiner().generation();

    let beat = confirming_heartbeat(&clock, &leader, &late, None);
    let waiting = deliver(&mut leader, &late, heartbeat_message(beat));
    assert_eq!(
        ack_to(&waiting, &late).recipient_admission(),
        None,
        "one change at a time"
    );

    for member in [&joiner, &worker("p1")] {
        let beat = confirming_heartbeat(&clock, &leader, member, Some(batch));
        deliver(&mut leader, member, heartbeat_message(beat));
    }

    let committed = Generation::new(0, 1, 4);
    let next_batch = Generation::new(0, 1, 5);
    assert_eq!(
        leader.configuration(),
        Some(&Configuration::joint(Joint {
            generation: next_batch,
            base: next_batch,
            batch_generation: next_batch,
            old_base: committed,
            old_generation: committed,
            old_voter_count: 4,
            new_voter_count: 5,
        }).expect("valid")),
        "the commit of four, then at once the batch that admits `late`"
    );
}

/// A follower that adopts a newer configuration heartbeats its leader at
/// once, not a heartbeat interval later, so the echo that commits it
/// arrives as soon as it can.
#[test]
fn a_follower_heartbeats_at_once_on_adopting_a_newer_configuration() {
    let clock = FakeClock::new();
    let me = worker("p1");
    let leader = worker("w1");
    let mut node = voter_node(&clock, &me, 3, SUSPECT);
    let first = three_voters_founded_in_term_1();
    deliver(
        &mut node,
        &leader,
        ack_message(leader_ack(&leader, 1, &first, Some(first.generation()))),
    );
    let settled = node.step(Input::Tick).outputs;
    assert!(
        sent_to(&settled, &leader).is_empty(),
        "setup invariant: its first heartbeat already went"
    );

    let next = batch_of_one_joiner();
    let outputs = deliver(
        &mut node,
        &leader,
        ack_message(leader_ack(&leader, 1, &next, Some(next.generation()))),
    );

    let beats: Vec<Checked<WorkerHeartbeat>> = sent_to(&outputs, &leader)
        .into_iter()
        .filter_map(|message| match checked(message).into_payload() {
            Some(CheckedPayload::Heartbeat(beat)) => Some(beat),
            _ => None,
        })
        .collect();
    assert_eq!(beats.len(), 1, "a heartbeat in the same step");
    assert_eq!(beats[0].configuration_generation(), Some(next.generation()));
}
