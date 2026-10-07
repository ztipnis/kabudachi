//! A leader's half of leader liveness: the quorum-contact lease it computes
//! from the ack confirmations its followers' heartbeats echo back, which
//! decides when it goes `NoQuorum` and what leadership grant it reports to
//! its driver; the heartbeats it ignores; and the abort deadlines of the
//! workers it loses.
//!
//! These tests need a real leader of several voters, driven all the way to
//! `Leader` through a roll call and a vote. The lease counts only the members
//! of the roster that election built, each at its admission generation. The
//! last tests run end to end on the `Cluster` harness.

use crate::support::builders::{
    message_input, g0, heartbeat, heartbeat_message, roll_call_reply, self_remove,
    self_remove_message, shard, timings, vote_grant, vote_grant_message, voter_of, worker,
};

use kabudachi_core::configuration::Generation;
use std::collections::BTreeSet;

use crate::support::clock::FakeClock;
use crate::support::node::{
    close_roll_call, connect, deliver, finish_reconciling, grants, published_roll_calls,
    stand_as_candidate, start_roll_call,
};
use crate::support::scenarios::{
    abort_deadline_at, assert_aborts_by, bootstrap_5_and_elect_leader, reported_lost_at,
};
use kabudachi_core::election::{
    ElectionTimings, Entry, Identity, Input, Output, Step, WorkerNode,
};
use kabudachi_core::protocol::ids::{IncarnationId, WorkerId};
use kabudachi_core::protocol::messages::{
    AckEcho, WorkerHeartbeat,
};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::{LeadershipGrant, LeaseEnd};
use kabudachi_core::time::{Clock, Duration, Instant};

const SHARD: &str = "shard-1";
const OTHER_SHARD: &str = "shard-2";
const SUSPECT_TIMEOUT_TICKS: u64 = 10;
/// The suspicion timeout less a tenth for clock drift.
const LEASE_TICKS: u64 = 9;

type TestNode = WorkerNode<FakeClock>;

/// A node that has just won term 1 of a configuration, connected to every
/// other voter, every one of which answered its roll call.
struct Won {
    node: TestNode,
    /// Every other member of its roster, those that voted for it first.
    others: Vec<WorkerId>,
    won_at: Instant,
}

/// Drives a fresh node to a real `Leader` of a configuration of `size`
/// voters (at least 3) through a roll call every voter answers, and a vote
/// that the fewest of them needed grant.
fn leader_of(clock: &FakeClock, size: usize) -> Won {
    leader_with_timings(
        clock,
        size,
        timings(Duration::from_ticks(SUSPECT_TIMEOUT_TICKS)),
    )
}

/// `leader_of` for a node that runs `timings`.
fn leader_with_timings(clock: &FakeClock, size: usize, timings: ElectionTimings) -> Won {
    assert!(size >= 3, "leader_of needs a configuration of at least 3");
    let quorum = size / 2 + 1;
    let me = worker("leader");
    let others: Vec<WorkerId> = (0..size - 1)
        .map(|i| worker(&format!("peer-{i}")))
        .collect();

    let mut node = WorkerNode::start(
        Identity {
            id: me.clone(),
            incarnation: IncarnationId::new("incarnation-1"),
            shard: shard(SHARD),
            timings,
        },
        Entry::Known(voter_of(size)),
        clock.clone(),
        None,
    )
    .0;
    connect(&mut node, &others);

    stand_as_candidate(
        &mut node,
        clock,
        timings.suspect_timeout.as_ticks(),
        &others,
    );

    for voter in &others[..quorum - 1] {
        deliver(
            &mut node,
            voter,
            vote_grant_message(vote_grant(me.clone(), voter.clone(), 1)),
        );
    }
    finish_reconciling(&mut node);
    assert_eq!(node.state(), WorkerState::Leader, "setup invariant");

    Won {
        node,
        others,
        won_at: clock.now(),
    }
}

/// Hands `node` `heartbeat` from `from` and returns the whole step.
fn receive(node: &mut TestNode, from: &WorkerId, heartbeat: WorkerHeartbeat) -> Step {
    node.step(message_input(&from, heartbeat_message(heartbeat)))
}

/// A heartbeat from `from` confirming the term-1 ack sent at `send_token`.
fn confirm(node: &mut TestNode, from: &WorkerId, send_token: Instant) -> Step {
    receive(
        node,
        from,
        heartbeat(
            from,
            Some(AckEcho {
                term: 1,
                send_token: send_token.as_ticks(),
            }),
        ),
    )
}

fn ticks_after(start: Instant, ticks: u64) -> Instant {
    start + Duration::from_ticks(ticks)
}

// ---- Acking heartbeats ----

#[test]
fn a_heartbeat_from_another_sender_shard_or_recovery_epoch_gets_no_ack() {
    let clock = FakeClock::new();
    let mut won = leader_of(&clock, 3);
    let (sender, named) = (won.others[0].clone(), won.others[1].clone());

    let naming_another_worker = heartbeat(&named, None);
    let from_another_shard = WorkerHeartbeat {
        shard_id: Some(shard(OTHER_SHARD).into()),
        ..heartbeat(&sender, None)
    };
    let at_another_epoch = WorkerHeartbeat {
        recovery_epoch_seen: 1,
        ..heartbeat(&sender, None)
    };

    for rejected in [naming_another_worker, from_another_shard, at_another_epoch] {
        let outputs = receive(&mut won.node, &sender, rejected.clone()).outputs;
        assert!(outputs.is_empty(), "{rejected:?} got {outputs:?}");
    }
}

// ---- The quorum-contact lease ----

// A tenth of 15 ticks is 1.5, and the margin rounds up to 2, so the lease is
// 13; a quarter of 15 is 3.75, rounded up to 4, so the lease is 11.
#[test]
fn a_leases_drift_margin_rounds_up_and_widens_with_a_smaller_divisor() {
    for (divisor, lease) in [(10, 13), (4, 11)] {
        let clock = FakeClock::new();
        let mut won = leader_with_timings(
            &clock,
            3,
            ElectionTimings {
                clock_drift_divisor: divisor,
                ..timings(Duration::from_ticks(15))
            },
        );
        let follower = won.others[0].clone();
        let w = won.won_at;
        assert_eq!(
            won.node.step(Input::Tick).next_deadline,
            Some(ticks_after(w, lease)),
            "the win instant stands in for the quorum contact"
        );

        clock.advance(Duration::from_ticks(1));
        let confirmed = confirm(&mut won.node, &follower, w);

        assert_eq!(
            grants(&confirmed.outputs),
            vec![Some(term_1_grant(LeaseEnd::At(ticks_after(w, lease))))],
            "divisor {divisor}"
        );
        assert_eq!(confirmed.next_deadline, Some(ticks_after(w, lease)));
    }
}

#[test]
fn confirmations_from_different_members_combine_into_the_quorum_contact() {
    let clock = FakeClock::new();
    let mut won = leader_of(&clock, 5);
    let w = won.won_at;
    let others = won.others.clone();

    clock.advance(Duration::from_ticks(5));
    let _ = confirm(&mut won.node, &others[0], ticks_after(w, 3));
    // Itself, plus acks sent at w+3 and w+5: the majority has heard from it
    // as late as w+3.
    let two = confirm(&mut won.node, &others[1], ticks_after(w, 5));
    assert_eq!(two.next_deadline, Some(ticks_after(w, 3 + LEASE_TICKS)));

    let three = confirm(&mut won.node, &others[2], ticks_after(w, 4));
    assert_eq!(three.next_deadline, Some(ticks_after(w, 4 + LEASE_TICKS)));

    let newer = confirm(&mut won.node, &others[0], ticks_after(w, 5));
    assert_eq!(newer.next_deadline, Some(ticks_after(w, 5 + LEASE_TICKS)));
}

#[test]
fn stale_future_and_pending_member_echoes_never_extend_the_lease() {
    let clock = FakeClock::new();
    let mut won = leader_of(&clock, 3);
    let w = won.won_at;
    let follower = won.others[0].clone();
    let stand_in_end = Some(ticks_after(w, LEASE_TICKS));
    clock.advance(Duration::from_ticks(3));

    let stale_term = heartbeat(
        &follower,
        Some(AckEcho {
            term: 0,
            send_token: ticks_after(w, 3).as_ticks(),
        }),
    );
    assert_eq!(
        receive(&mut won.node, &follower, stale_term).next_deadline,
        stand_in_end
    );
    let from_the_future = confirm(&mut won.node, &follower, ticks_after(w, 4));
    assert_eq!(from_the_future.next_deadline, stand_in_end);
    let joiner = worker("joiner");
    let pending = confirm(&mut won.node, &joiner, ticks_after(w, 3));
    assert_eq!(pending.next_deadline, stand_in_end);

    // The same follower's real confirmation does extend it, and an older one
    // arriving late does not take the extension back.
    let real = confirm(&mut won.node, &follower, ticks_after(w, 3));
    assert_eq!(real.next_deadline, Some(ticks_after(w, 3 + LEASE_TICKS)));
    let late = confirm(&mut won.node, &follower, ticks_after(w, 1));
    assert_eq!(late.next_deadline, Some(ticks_after(w, 3 + LEASE_TICKS)));
}

// ---- The leadership grant ----

/// The grant for term 1 at recovery epoch 0, the term every leader here wins.
fn term_1_grant(valid_until: LeaseEnd) -> LeadershipGrant {
    LeadershipGrant {
        term: 1,
        recovery_epoch: kabudachi_core::coordination_authority::RecoveryEpoch::new(0, 0),
        valid_until,
    }
}

#[test]
fn losing_a_majority_confirmation_to_a_removal_withdraws_the_grant() {
    // Five members need two others' confirmations. Once the newer of the
    // two confirmers leaves, four members still need two, and only one
    // is left: the leader has no lease, though it is still `Leader`.
    let clock = FakeClock::new();
    let mut won = leader_of(&clock, 5);
    let (leaving, staying) = (won.others[0].clone(), won.others[1].clone());
    let w = won.won_at;
    clock.advance(Duration::from_ticks(3));
    let _ = confirm(&mut won.node, &leaving, ticks_after(w, 3));
    let held = confirm(&mut won.node, &staying, ticks_after(w, 1));
    let held_end = ticks_after(w, 1 + LEASE_TICKS);
    assert_eq!(
        grants(&held.outputs),
        vec![Some(term_1_grant(LeaseEnd::At(held_end)))],
        "setup invariant"
    );

    // The removal takes effect with the leader's next announcement (every
    // pending SELF_REMOVE lands in the next generation), here
    // the ack answering `staying`'s next heartbeat; until then `leaving`'s
    // confirmation still stands.
    let accepted = deliver(
        &mut won.node,
        &leaving,
        self_remove_message(self_remove(&leaving, SHARD)),
    );
    assert!(grants(&accepted).is_empty());
    let removed = confirm(&mut won.node, &staying, ticks_after(w, 1));

    assert_eq!(won.node.state(), WorkerState::Leader);
    assert_eq!(grants(&removed.outputs), vec![None]);
}

// ---- End to end ----

#[test]
fn a_respondent_that_was_no_voter_of_the_roll_call_counts_toward_the_lease_on_the_new_side_only() {
    let clock = FakeClock::new();
    let (me, voter, left_out) = (worker("leader"), worker("peer-0"), worker("peer-1"));
    let mut node: TestNode = WorkerNode::start(
        Identity {
            id: me.clone(),
            incarnation: IncarnationId::new("incarnation-1"),
            shard: shard(SHARD),
            timings: timings(Duration::from_ticks(SUSPECT_TIMEOUT_TICKS)),
        },
        Entry::Known(voter_of(3)),
        clock.clone(),
        None,
    )
    .0;
    let call =
        published_roll_calls(&start_roll_call(&mut node, &clock, SUSPECT_TIMEOUT_TICKS)).remove(0);
    let outside = Generation::new(0, 0, 7);
    deliver(
        &mut node,
        &left_out,
        roll_call_reply(&me, call.term, &left_out, Some(outside)),
    );
    deliver(
        &mut node,
        &voter,
        roll_call_reply(&me, call.term, &voter, Some(g0())),
    );
    close_roll_call(&mut node, &clock, SUSPECT_TIMEOUT_TICKS);
    deliver(
        &mut node,
        &voter,
        vote_grant_message(vote_grant(me.clone(), voter.clone(), call.term)),
    );
    finish_reconciling(&mut node);
    assert_eq!(node.state(), WorkerState::Leader, "setup invariant");
    let w = clock.now();
    clock.advance(Duration::from_ticks(3));

    let from_left_out = confirm(&mut node, &left_out, ticks_after(w, 3));
    assert_eq!(
        from_left_out.next_deadline,
        Some(ticks_after(w, LEASE_TICKS)),
        "two of the founded three, but one of the roll call's three: no lease yet"
    );
    let from_voter = confirm(&mut node, &voter, ticks_after(w, 3));

    assert_eq!(
        from_voter.next_deadline,
        Some(ticks_after(w, 3 + LEASE_TICKS)),
        "a majority of each side"
    );
}

#[test]
fn a_follower_cut_off_as_a_new_leader_takes_over_aborts_before_that_leader_replays_its_runs() {
    let suspect_timeout = Duration::from_secs(2);
    let (mut cluster, old_leader) =
        bootstrap_5_and_elect_leader(suspect_timeout, Duration::from_secs(1));
    let rest: BTreeSet<WorkerId> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != old_leader)
        .collect();
    cluster.record_steps();
    cluster.partition([old_leader.clone()].into_iter().collect(), rest.clone());
    let new_leader = loop {
        cluster.advance(Duration::from_ticks(10));
        if let Some(leader) = rest
            .iter()
            .find(|id| cluster.states()[*id] == WorkerState::Leader)
        {
            break leader.clone();
        }
    };

    // Cut off with the old leader, before or just after it hears the new one.
    let cut_off = rest
        .iter()
        .find(|id| **id != new_leader)
        .expect("four survivors")
        .clone();
    cluster.partition(
        [old_leader.clone(), cut_off.clone()].into_iter().collect(),
        rest.into_iter().filter(|id| *id != cut_off).collect(),
    );
    let lost_after = suspect_timeout.as_ticks() + ElectionTimings::DEFAULT_RECONNECT_TIMEOUT.as_ticks();
    cluster.advance(Duration::from_ticks(lost_after + 1_000));

    let steps = cluster.take_steps();
    let lost_at = reported_lost_at(&steps, &new_leader, &cut_off);
    assert_aborts_by(&steps, &cut_off, lost_at);
    // The old leader aborts its own runs before the new leader could replay
    // them, which is no sooner than a suspicion and a reconnect timeout
    // after it won (it does not track the old leader here, having won
    // without its answer): no rival won before the old grant ended.
    let won_at = steps
        .iter()
        .find(|step| {
            step.node == new_leader
                && step
                    .outputs
                    .contains(&Output::StateChanged(WorkerState::Leader))
        })
        .map(|step| step.at)
        .expect("the new leader's win was recorded");
    assert_aborts_by(
        &steps,
        &old_leader,
        won_at + Duration::from_ticks(lost_after),
    );
    // The new leader lost the old one before it won, so it had a deadline to
    // abort its own runs; holding office, it has none.
    assert_eq!(
        abort_deadline_at(&steps, &new_leader, cluster.now()).flatten(),
        None,
        "a winner withdraws the abort deadline it held as a follower"
    );
}
