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
    published_roll_calls, sent, sent_to, voter_node, voter_node_reconnecting,
};
use kabudachi_core::configuration::{Configuration, Generation, Single};
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

/// How long past a suspicion timeout the leaders of the loss tests report a
/// worker lost.
const RECONNECT: u64 = 100;

/// `w1`, elected in term 1 leader of a configuration of 3 by `p1` and `p2`,
/// connected to both, having committed what its election founded.
fn leader_of_three(clock: &FakeClock) -> TestNode {
    leader_of_three_reconnecting(clock, None)
}

/// `leader_of_three`, reporting a worker lost `RECONNECT` ticks past the
/// suspicion timeout rather than the default.
fn leader_of_three_losing_quickly(clock: &FakeClock) -> TestNode {
    leader_of_three_reconnecting(clock, Some(RECONNECT))
}

fn leader_of_three_reconnecting(clock: &FakeClock, reconnect: Option<u64>) -> TestNode {
    let mut node = voter_node_reconnecting(clock, &worker("w1"), 3, SUSPECT, reconnect);
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
    beat.admission_generation = Some(admission.into());
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

/// A joiner promised its admission that keeps heartbeating but never says it
/// holds the promise, and whose heartbeats echo only the ack it confirmed
/// long ago, confirms nothing new. A round waiting for it would hold back a
/// joiner that arrives later forever, so once its confirmation is a
/// suspicion timeout old the leader replaces the round and promises the
/// joiner that does confirm.
#[test]
fn a_promised_joiner_that_heartbeats_but_confirms_nothing_new_does_not_hold_back_a_later_one() {
    let clock = FakeClock::new();
    let mut leader = leader_of_three(&clock);
    let (silent, late) = (worker("silent"), worker("late"));
    let peers = [worker("p1"), worker("p2")];
    connect(&mut leader, &[silent.clone(), late.clone()]);
    // The founding's members echo the committed generation, so the leader
    // admits a joiner at once.
    let committed = leader.configuration().map(Configuration::generation);
    for peer in &peers {
        let beat = confirming_heartbeat(&clock, &leader, peer, committed);
        deliver(&mut leader, peer, heartbeat_message(beat));
    }
    let beat = confirming_heartbeat(&clock, &leader, &silent, None);
    let stale_echo = beat.newest_accepted_ack;
    let promised = deliver(&mut leader, &silent, heartbeat_message(beat));
    assert!(
        ack_to(&promised, &silent).recipient_admission().is_some(),
        "setup: the silent joiner is promised"
    );

    let mut promised_late = false;
    for _ in 0..SUSPECT * 3 {
        clock.advance(Duration::from_ticks(1));
        let mut beats = vec![(silent.clone(), heartbeat(&silent, stale_echo))];
        beats.push((
            late.clone(),
            confirming_heartbeat(&clock, &leader, &late, None),
        ));
        for peer in &peers {
            beats.push((
                peer.clone(),
                confirming_heartbeat(&clock, &leader, peer, committed),
            ));
        }
        for (from, beat) in beats {
            let outputs = deliver(&mut leader, &from, heartbeat_message(beat));
            if from == late {
                promised_late |= ack_to(&outputs, &late).recipient_admission().is_some();
            }
        }
    }

    assert!(promised_late, "the joiner heard from every tick is never promised");
}

/// Runs `leader` of three for `ticks` in heartbeat-sized steps: `p2` confirms
/// each ack and holds the leader's latest configuration; `p1` heartbeats too,
/// echoing an ack `p1_echo_age` old (`None`: it echoes none). Returns every
/// output the ticks produced, and checks after each step that the node's next
/// deadline lies ahead of the clock, so a driver never ticks it in a busy loop.
fn run_with_a_peer_echoing_late(
    clock: &FakeClock,
    leader: &mut TestNode,
    ticks: u64,
    p1_echo_age: Option<u64>,
) -> Vec<Output> {
    let heartbeat_interval = crate::support::builders::timings(Duration::from_ticks(SUSPECT))
        .heartbeat_interval
        .as_ticks();
    let (p1, p2) = (worker("p1"), worker("p2"));
    let mut outputs = Vec::new();
    for _ in 0..ticks / heartbeat_interval {
        clock.advance(Duration::from_ticks(heartbeat_interval));
        let now = clock.now().as_ticks();
        let held = leader.configuration().map(Configuration::generation);
        let echo = p1_echo_age.map(|age| AckEcho {
            term: leader.term(),
            send_token: now.saturating_sub(age),
        });
        let mut beat = heartbeat(&p1, echo);
        beat.configuration_generation = held.map(Into::into);
        deliver(leader, &p1, heartbeat_message(beat));
        let beat = confirming_heartbeat(clock, leader, &p2, held);
        deliver(leader, &p2, heartbeat_message(beat));
        let step = leader.step(Input::Tick);
        assert!(
            step.next_deadline.is_none_or(|deadline| deadline > clock.now()),
            "a deadline already past at {now}: {:?}",
            step.next_deadline
        );
        outputs.extend(step.outputs);
    }
    outputs
}

fn loss_timeout() -> u64 {
    SUSPECT + RECONNECT
}

/// A voter whose heartbeats keep arriving but which echoes no ack is removed
/// once it has gone a loss timeout without, and the leader's timer then has
/// nothing already due even though the removed worker keeps heartbeating.
#[test]
fn a_removed_voter_that_keeps_heartbeating_leaves_the_leaders_timer_idle() {
    let clock = FakeClock::new();
    let mut leader = leader_of_three_losing_quickly(&clock);
    let committed = three_voters_founded_in_term_1().generation();
    for peer in [worker("p1"), worker("p2")] {
        let beat = confirming_heartbeat(&clock, &leader, &peer, Some(committed));
        deliver(&mut leader, &peer, heartbeat_message(beat));
    }

    // Past a second loss timeout, when a stale record of the removed worker
    // would fall due.
    run_with_a_peer_echoing_late(&clock, &mut leader, 2 * loss_timeout() + 2_000, None);

    assert_eq!(
        leader.configuration().and_then(Configuration::voter_count),
        Some(2),
        "p1 is removed"
    );
}

/// A follower whose echoes are always a few heartbeat intervals old, well
/// within the suspicion timeout, as on a slow link, is a healthy member: it
/// is never reported lost nor removed.
#[test]
fn a_follower_whose_echo_is_always_a_few_intervals_old_is_never_removed() {
    let clock = FakeClock::new();
    let mut leader = leader_of_three_losing_quickly(&clock);
    let committed = three_voters_founded_in_term_1().generation();
    for peer in [worker("p1"), worker("p2")] {
        let beat = confirming_heartbeat(&clock, &leader, &peer, Some(committed));
        deliver(&mut leader, &peer, heartbeat_message(beat));
    }
    let heartbeat_interval = crate::support::builders::timings(Duration::from_ticks(SUSPECT))
        .heartbeat_interval
        .as_ticks();

    let outputs = run_with_a_peer_echoing_late(
        &clock,
        &mut leader,
        loss_timeout() + 2_000,
        Some(3 * heartbeat_interval),
    );

    assert_eq!(
        leader.configuration().and_then(Configuration::voter_count),
        Some(3)
    );
    assert!(
        !outputs.iter().any(|output| matches!(output, Output::WorkerLost(_))),
        "{outputs:?}"
    );
}

/// Of five voters, two whose heartbeats confirm no ack are removed in turn
/// while the other three keep confirming and echo each new generation: each
/// removal finds a majority of the five holding the generation it moves from.
/// The two are not both taken at once, and the leader stays in office.
#[test]
fn two_of_five_voters_that_confirm_no_ack_are_removed_one_after_the_other() {
    let clock = FakeClock::new();
    let mut leader = voter_node_reconnecting(&clock, &worker("w1"), 5, SUSPECT, Some(RECONNECT));
    let peers: Vec<WorkerId> = (1..=4).map(|n| worker(&format!("p{n}"))).collect();
    connect(&mut leader, &peers);
    elect(&mut leader, &clock, SUSPECT, &peers);
    commit_founding(&mut leader, &clock, &peers);
    let committed = leader.configuration().map(Configuration::generation);
    for peer in &peers {
        let beat = confirming_heartbeat(&clock, &leader, peer, committed);
        deliver(&mut leader, peer, heartbeat_message(beat));
    }
    let heartbeat_interval = crate::support::builders::timings(Duration::from_ticks(SUSPECT))
        .heartbeat_interval
        .as_ticks();

    for _ in 0..(loss_timeout() + 2_000) / heartbeat_interval {
        clock.advance(Duration::from_ticks(heartbeat_interval));
        let held = leader.configuration().map(Configuration::generation);
        for (index, peer) in peers.iter().enumerate() {
            let beat = if index < 2 {
                // Heartbeats arrive, echoing an ack long since confirmed.
                let mut beat = heartbeat(
                    peer,
                    Some(AckEcho {
                        term: leader.term(),
                        send_token: 0,
                    }),
                );
                beat.configuration_generation = committed.map(Into::into);
                beat
            } else {
                confirming_heartbeat(&clock, &leader, peer, held)
            };
            deliver(&mut leader, peer, heartbeat_message(beat));
        }
        let outputs = leader.step(Input::Tick).outputs;
        assert!(
            !outputs.contains(&Output::StateChanged(WorkerState::NoQuorum)),
            "the leader keeps its quorum"
        );
    }

    assert_eq!(
        leader.configuration().and_then(Configuration::voter_count),
        Some(3),
        "both are removed, one change at a time"
    );
}

/// A voter whose heartbeats arrive but which confirms no ack is reported lost
/// once, even while the removal it earns stays blocked, so its tasks are
/// replayed once; after it confirms an ack in between, going quiet reports it
/// again. Here the other voter keeps the lease but has not echoed the current
/// generation, so no majority holds it and the removal waits.
#[test]
fn a_voter_blocked_from_removal_is_reported_lost_once_until_it_confirms_again() {
    let clock = FakeClock::new();
    let mut leader = leader_of_three_losing_quickly(&clock);
    let (p1, p2) = (worker("p1"), worker("p2"));
    let committed = three_voters_founded_in_term_1().generation();
    let beat = confirming_heartbeat(&clock, &leader, &p1, Some(committed));
    deliver(&mut leader, &p1, heartbeat_message(beat));
    let heartbeat_interval = crate::support::builders::timings(Duration::from_ticks(SUSPECT))
        .heartbeat_interval
        .as_ticks();
    let lost = |outputs: &[Output]| {
        outputs.iter().filter(|output| **output == Output::WorkerLost(p1.clone())).count()
    };
    // p1's heartbeats: confirming an ack, confirming none, or none at all.
    #[derive(Clone, Copy, PartialEq)]
    enum Beat {
        Confirming,
        Stale,
        Silent,
    }
    let run = |leader: &mut TestNode, p1_beats: Beat, ticks: u64| {
        let mut reported = 0;
        for _ in 0..ticks / heartbeat_interval {
            clock.advance(Duration::from_ticks(heartbeat_interval));
            // p2 keeps the lease alive but echoes no generation: no holder.
            let beat = confirming_heartbeat(&clock, leader, &p2, None);
            deliver(leader, &p2, heartbeat_message(beat));
            if p1_beats != Beat::Silent {
                let mut beat = if p1_beats == Beat::Confirming {
                    confirming_heartbeat(&clock, leader, &p1, Some(committed))
                } else {
                    heartbeat(&p1, None)
                };
                beat.configuration_generation = Some(committed.into());
                deliver(leader, &p1, heartbeat_message(beat));
            }
            let step = leader.step(Input::Tick);
            assert!(
                step.next_deadline.is_none_or(|deadline| deadline > clock.now()),
                "a deadline already past: {:?}",
                step.next_deadline
            );
            reported += lost(&step.outputs);
        }
        reported
    };

    let first = run(&mut leader, Beat::Stale, 3 * loss_timeout());
    assert_eq!(first, 1, "reported once while the removal is blocked");
    assert_eq!(
        leader.configuration().and_then(Configuration::voter_count),
        Some(3),
        "setup invariant: the removal is blocked"
    );
    let silent = run(&mut leader, Beat::Silent, 3 * loss_timeout());
    assert_eq!(silent, 0, "a queued voter that goes silent is not reported again");
    run(&mut leader, Beat::Confirming, 2 * SUSPECT);
    let again = run(&mut leader, Beat::Stale, 3 * loss_timeout());
    assert_eq!(again, 1, "reported again after it confirmed in between");
}

/// Five voters; `d` echoes the current generation and then confirms no more,
/// while `b` holds it and `c` and `e` keep the lease but have echoed no
/// generation. `d` is queued for removal, but the voters that hold the
/// generation, `d` not counted among them since it is the one leaving, are two
/// of five: no majority, so it stays. Counting `d` would make three.
#[test]
fn a_queued_voter_does_not_count_among_those_that_hold_the_generation() {
    let clock = FakeClock::new();
    let mut leader = voter_node_reconnecting(&clock, &worker("w1"), 5, SUSPECT, Some(RECONNECT));
    let peers: Vec<WorkerId> = ["b", "c", "d", "e"].iter().map(|name| worker(name)).collect();
    connect(&mut leader, &peers);
    elect(&mut leader, &clock, SUSPECT, &peers);
    commit_founding(&mut leader, &clock, &[peers[0].clone(), peers[2].clone()]);
    let (b, c, d, e) = (&peers[0], &peers[1], &peers[2], &peers[3]);
    let current = leader.configuration().expect("a configuration").generation();
    let heartbeat_interval = crate::support::builders::timings(Duration::from_ticks(SUSPECT))
        .heartbeat_interval
        .as_ticks();

    for round in 0..(loss_timeout() + 200) / heartbeat_interval {
        clock.advance(Duration::from_ticks(heartbeat_interval));
        for voter in [b, c, e] {
            let held = (voter == b).then_some(current);
            let beat = confirming_heartbeat(&clock, &leader, voter, held);
            deliver(&mut leader, voter, heartbeat_message(beat));
        }
        let beat = if round < 3 {
            confirming_heartbeat(&clock, &leader, d, Some(current))
        } else {
            let mut beat = heartbeat(d, None);
            beat.configuration_generation = Some(current.into());
            beat
        };
        deliver(&mut leader, d, heartbeat_message(beat));
        let _ = leader.step(Input::Tick);
    }

    assert_eq!(
        leader.configuration().and_then(Configuration::voter_count),
        Some(5),
        "d stays: only the leader and b hold the generation"
    );
}

/// A leader of three admits two joiners `j1` and `j2` in one batch that `p1`
/// never echoes, so `p1` stays anchored at the three-voter configuration. It
/// then queues `j2`, which it removes, and `p1`, which it cannot while the
/// voters that hold the new generation, `j1` lagging, are only the leader and
/// `p2`: two of three at `p1`'s anchor, but two of the five `j2` last
/// echoed. Only the record of the removed `j2` holds `p1` back.
#[test]
fn a_removal_waits_for_a_majority_of_the_configuration_a_removed_voter_last_echoed() {
    let clock = FakeClock::new();
    let mut leader = leader_of_three_losing_quickly(&clock);
    let (p1, p2, j1, j2) = (worker("p1"), worker("p2"), worker("j1"), worker("j2"));
    let heartbeat_interval = crate::support::builders::timings(Duration::from_ticks(SUSPECT))
        .heartbeat_interval
        .as_ticks();
    let current = |leader: &TestNode| {
        leader.configuration().expect("a configuration").generation()
    };
    let committed = current(&leader);

    // Both joiners confirm an ack first, but no one is promised anything
    // before every voter holds the committed configuration; once p1 and p2
    // say they do, both are promised the batch's generation together.
    connect(&mut leader, &[j1.clone(), j2.clone()]);
    for joiner in [&j1, &j2] {
        let beat = confirming_heartbeat(&clock, &leader, joiner, None);
        deliver(&mut leader, joiner, heartbeat_message(beat));
    }
    for peer in [&p1, &p2] {
        let beat = confirming_heartbeat(&clock, &leader, peer, Some(committed));
        deliver(&mut leader, peer, heartbeat_message(beat));
    }
    let mut promised = None;
    for joiner in [&j1, &j2] {
        let beat = confirming_heartbeat(&clock, &leader, joiner, None);
        let outputs = deliver(&mut leader, joiner, heartbeat_message(beat));
        promised = ack_to(&outputs, joiner).recipient_admission().or(promised);
    }
    let batch = promised.expect("the joiners are promised a generation");
    for joiner in [&j1, &j2] {
        let mut beat = confirming_heartbeat(&clock, &leader, joiner, None);
        beat.admission_generation = Some(batch.into());
        deliver(&mut leader, joiner, heartbeat_message(beat));
    }
    assert!(leader.configuration().is_some_and(Configuration::is_joint), "a batch starts");
    // It commits on the leader, p2, j1 and j2; p1 echoes nothing newer.
    for member in [&p2, &j1, &j2] {
        let beat = confirming_heartbeat(&clock, &leader, member, Some(batch));
        deliver(&mut leader, member, heartbeat_message(beat));
    }
    let five = current(&leader);
    assert_eq!(
        leader.configuration().and_then(Configuration::voter_count),
        Some(5),
        "setup invariant: the batch committed"
    );

    // j2 echoes the five-voter generation before going quiet, so that is
    // what its record will say.
    for member in [&p2, &j1, &j2] {
        let beat = confirming_heartbeat(&clock, &leader, member, Some(five));
        deliver(&mut leader, member, heartbeat_message(beat));
    }

    let mut removed_j2_at = None;
    for _ in 0..(3 * loss_timeout()) / heartbeat_interval {
        clock.advance(Duration::from_ticks(heartbeat_interval));
        let now_current = current(&leader);
        let beat = confirming_heartbeat(&clock, &leader, &p2, Some(now_current));
        deliver(&mut leader, &p2, heartbeat_message(beat));
        // A heartbeat delayed in the network arrives after it, naming an
        // older generation: it takes nothing back.
        let beat = confirming_heartbeat(&clock, &leader, &p2, Some(committed));
        deliver(&mut leader, &p2, heartbeat_message(beat));
        // j1 keeps the lease but echoes the five-voter generation only.
        let beat = confirming_heartbeat(&clock, &leader, &j1, Some(five));
        deliver(&mut leader, &j1, heartbeat_message(beat));
        for silent in [&p1, &j2] {
            let mut beat = heartbeat(silent, None);
            beat.configuration_generation = Some(if *silent == p1 { committed } else { five }.into());
            deliver(&mut leader, silent, heartbeat_message(beat));
        }
        let _ = leader.step(Input::Tick);
        if removed_j2_at.is_none() && leader.configuration().and_then(Configuration::voter_count) == Some(4) {
            removed_j2_at = Some(clock.now());
        }
    }

    assert!(removed_j2_at.is_some(), "j2 is removed first");
    assert_eq!(
        leader.configuration().and_then(Configuration::voter_count),
        Some(4),
        "p1 stays: two of the five j2 last echoed hold the generation"
    );
}

/// A voter that echoed the committed generation, then sends only heartbeats
/// delayed in the network that name the genesis one, is anchored at what it
/// held at most, not at the older one a late heartbeat names: it is still
/// removable once it has confirmed no ack for a loss timeout.
#[test]
fn a_late_heartbeat_naming_an_older_generation_does_not_move_a_voters_anchor_back() {
    let clock = FakeClock::new();
    let mut leader = leader_of_three_losing_quickly(&clock);
    let (p1, p2) = (worker("p1"), worker("p2"));
    let committed = three_voters_founded_in_term_1().generation();
    let beat = confirming_heartbeat(&clock, &leader, &p1, Some(committed));
    deliver(&mut leader, &p1, heartbeat_message(beat));
    let heartbeat_interval = crate::support::builders::timings(Duration::from_ticks(SUSPECT))
        .heartbeat_interval
        .as_ticks();

    for _ in 0..(loss_timeout() + 200) / heartbeat_interval {
        clock.advance(Duration::from_ticks(heartbeat_interval));
        let beat = confirming_heartbeat(&clock, &leader, &p2, Some(committed));
        deliver(&mut leader, &p2, heartbeat_message(beat));
        let mut late = heartbeat(
            &p1,
            Some(AckEcho {
                term: leader.term(),
                send_token: 0,
            }),
        );
        late.configuration_generation = Some(g0().into());
        deliver(&mut leader, &p1, heartbeat_message(late));
        let _ = leader.step(Input::Tick);
    }

    assert_eq!(
        leader.configuration().and_then(Configuration::voter_count),
        Some(2),
        "p1 is removed"
    );
}
