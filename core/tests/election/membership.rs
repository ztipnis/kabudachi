//! Membership changes while a leader lives, at one node: a draining worker tells only its leader,
//! the leader applies a removal under the term guard and announces
//! the shrunk configuration on its acks, a draining leader announces its own
//! departure, and a leader admits pending joiners in batches. The
//! multi-node behaviour of the same rules is in `scenario_membership_test`.

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
    published_roll_calls, sent, sent_to, state_changes, voter_node,
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

/// `leader_of_three`, asked to drain once both followers have reported a
/// routing crawl.
fn crawled_leader_of_three(clock: &FakeClock) -> TestNode {
    let mut leader = leader_of_three(clock);
    for follower in [worker("p1"), worker("p2")] {
        let _ = crawled(clock, &mut leader, &follower);
    }
    leader
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

/// A draining leader applies its own removal, which no other leader can,
/// and announces the configuration without itself on a final
/// ack to every peer before it stops.
#[test]
fn a_draining_leader_announces_the_configuration_without_itself_on_final_acks() {
    let clock = FakeClock::new();
    let mut leader = crawled_leader_of_three(&clock);

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

// Leaving before every other voter has crawled could strand workers that
// know only this leader: it leads on until the last one reports.
#[test]
fn a_draining_leader_leads_on_until_every_other_voter_reports_a_routing_crawl() {
    let clock = FakeClock::new();
    let mut leader = leader_of_three(&clock);

    let asked = leader.step(Input::Drain).outputs;
    assert_eq!(leader.state(), WorkerState::Leader, "{asked:?}");

    let _ = crawled(&clock, &mut leader, &worker("p1"));
    assert_eq!(leader.state(), WorkerState::Leader, "p2 has not crawled");

    let outputs = crawled(&clock, &mut leader, &worker("p2"));
    assert_eq!(leader.state(), WorkerState::Stopped);
    assert_eq!(
        ack_to(&outputs, &worker("p1")).configuration(),
        two_voters_at_the_next_generation()
    );
}

#[test]
fn a_draining_leader_leaves_at_its_drain_wait_limit_without_every_crawl() {
    let clock = FakeClock::new();
    let mut leader = leader_of_three(&clock);
    let limit = leader.timings().drain_wait_limit;
    let interval = leader.timings().heartbeat_interval;
    let asked_at = clock.now();
    let _ = leader.step(Input::Drain);

    // Its followers keep confirming, so it keeps its lease, but report no crawl.
    while clock.now() + interval < asked_at + limit {
        clock.advance(interval);
        for follower in [worker("p1"), worker("p2")] {
            let held = leader.configuration().map(Configuration::generation);
            let beat = confirming_heartbeat(&clock, &leader, &follower, held);
            let _ = deliver(&mut leader, &follower, heartbeat_message(beat));
        }
        let _ = leader.step(Input::Tick);
        assert_eq!(leader.state(), WorkerState::Leader);
    }
    clock.advance((asked_at + limit) - clock.now());
    let _ = leader.step(Input::Tick);

    assert_eq!(leader.state(), WorkerState::Stopped);
}

/// A leader asked to drain that loses office before it may leave has not
/// withdrawn its request: once it follows the leader that deposed it, it
/// tells that leader it leaves, and it stops.
#[test]
fn a_drain_request_kept_across_lost_office_is_honoured_under_the_new_leader() {
    let clock = FakeClock::new();
    let mut leader = leader_of_three(&clock);
    let new_leader = worker("leader-2");
    let asked = leader.step(Input::Drain).outputs;
    assert_eq!(leader.state(), WorkerState::Leader, "{asked:?}");

    // No crawl was reported, so it still leads when a later term's leader acks it.
    let newer = committed_from_g0(2, 2, 3);
    let outputs = deliver(
        &mut leader,
        &new_leader,
        ack_message(leader_ack(&new_leader, 2, &newer, Some(newer.generation()))),
    );

    assert_eq!(leader.state(), WorkerState::Stopped);
    assert_eq!(
        state_changes(&outputs),
        vec![
            WorkerState::Active,
            WorkerState::Draining,
            WorkerState::Stopped
        ]
    );
    let removes = self_removes(&outputs);
    assert_eq!(removes.len(), 1, "{removes:?}");
    assert_eq!(removes[0].0, new_leader);
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
fn a_leader_names_its_voters_and_a_follower_names_none() {
    let clock = FakeClock::new();
    let leader = leader_of_three(&clock);
    let follower = voter_node(&clock, &worker("p1"), 3, SUSPECT);

    let mut named = leader.voters();
    named.sort();

    assert_eq!(named, vec![worker("p1"), worker("p2"), worker("w1")]);
    assert!(
        follower.voters().is_empty(),
        "only the leader's configuration names members"
    );
}

#[test]
fn a_leader_knows_its_voters_and_pending_members_and_no_one_else() {
    let clock = FakeClock::new();
    let mut leader = leader_of_three(&clock);
    let follower = voter_node(&clock, &worker("p1"), 3, SUSPECT);
    let joiner = worker("joiner");
    connect(&mut leader, std::slice::from_ref(&joiner));
    deliver(
        &mut leader,
        &joiner,
        heartbeat_message(heartbeat(&joiner, None)),
    );

    for voter in ["w1", "p1", "p2", "joiner"] {
        assert!(leader.is_voter_or_pending(&worker(voter)), "{voter}");
    }
    assert!(!leader.is_voter_or_pending(&worker("stranger")));
    assert!(
        !follower.is_voter_or_pending(&worker("p2")),
        "only a leader's roster names members"
    );
}

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
/// acks, so the batch's new side never costs the leader its lease: a joiner
/// heard of but not yet confirming waits pending.
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
