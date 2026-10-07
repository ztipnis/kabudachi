//! Scenario tests for partitions and isolation, built on the
//! `Cluster` harness, including the regression for the split brain the ring
//! roll call produced when every survivor of a lost leader raced, and races
//! of four or more roll calls started at the same instant.

use crate::support::scenarios::{
    bootstrap_5_and_elect_leader, elect_new_leader_among, run_out_cut_off_leaders_lease,
    suspect_leader_by_hand,
};

use std::collections::{BTreeMap, BTreeSet};

use crate::support::harness::Cluster;
use crate::support::node::published_roll_calls;
use kabudachi_core::election::{ElectionTimings, Input};
use kabudachi_core::protocol::ids::{TaskDefinitionId, WorkerId};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::{ClaimRejection, Submission};
use kabudachi_core::time::Duration;

/// Partitions `leader` from everyone else, checks the isolated leader detects
/// its peer loss once its lease runs out (`Leader -> NoQuorum`), crosses
/// `suspect_timeout` for the 4-member majority by hand, then elects a new
/// leader among the majority with `elect_new_leader_among`. Returns the new
/// leader.
fn isolate_leader_and_elect_new(cluster: &mut Cluster, leader: &WorkerId) -> WorkerId {
    let others: Vec<WorkerId> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| id != leader)
        .collect();
    let leader_group: BTreeSet<WorkerId> = [leader.clone()].into_iter().collect();
    let others_group: BTreeSet<WorkerId> = others.iter().cloned().collect();
    cluster.partition(leader_group, others_group);

    // Holds the end of the leader's lease but none of the others' suspicion
    // deadlines.
    run_out_cut_off_leaders_lease(cluster);

    assert_eq!(
        cluster.states()[leader],
        WorkerState::NoQuorum,
        "the isolated leader must detect its own peer loss (Leader -> NoQuorum)"
    );

    // Every one of the others is moved to LeaderSuspect by hand, and only
    // the first goes further, so the scenario knows who wins.
    suspect_leader_by_hand(cluster, &others);

    elect_new_leader_among(cluster, &others)
}

#[test]
fn leader_isolated_with_minority() {
    let suspect_timeout = Duration::from_ticks(10);
    let tick_size = Duration::from_ticks(5);

    let (mut cluster, original_leader) = bootstrap_5_and_elect_leader(suspect_timeout, tick_size);
    let followers: Vec<WorkerId> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != original_leader)
        .collect();
    assert_eq!(followers.len(), 4);

    let companion = followers[0].clone();
    let majority: Vec<WorkerId> = followers[1..].to_vec();
    assert_eq!(majority.len(), 3);

    let isolated_group: BTreeSet<WorkerId> = [original_leader.clone(), companion.clone()]
        .into_iter()
        .collect();
    let majority_group: BTreeSet<WorkerId> = majority.iter().cloned().collect();
    cluster.partition(isolated_group, majority_group.clone());

    // The isolated leader+1 pair caps at 2 responses, below quorum 3, like
    // test 1; the majority-of-3 is safe to drive with plain advance().
    for _ in 0..3 {
        cluster.advance(tick_size);
    }
    cluster.run_until_quiescent(tick_size, 60);

    assert_ne!(
        cluster.states()[&original_leader],
        WorkerState::Leader,
        "the isolated original leader must not remain an unchallenged authority"
    );
    assert_ne!(
        cluster.states()[&companion],
        WorkerState::Leader,
        "the 2-node isolated minority (leader+1) can never reach quorum-of-3 on its own"
    );

    let new_leader = cluster
        .leader()
        .expect("the majority-of-3 side must independently elect a new leader");
    assert!(
        majority_group.contains(&new_leader),
        "the new leader must be a majority-side node"
    );
    assert_ne!(new_leader, original_leader);

    // At most one node is Leader across both groups: only one side of a
    // partition can have a legitimate leader.
    cluster.assert_at_most_one_in_leader_state();
}

#[test]
fn rapid_leader_crash_restart() {
    let suspect_timeout = Duration::from_ticks(10);
    let tick_size = Duration::from_ticks(5);

    let (mut cluster, original_leader) = bootstrap_5_and_elect_leader(suspect_timeout, tick_size);

    // "Crash" the leader by fully partitioning it, and let the remaining
    // 4-node majority elect a new one.
    let new_leader = isolate_leader_and_elect_new(&mut cluster, &original_leader);
    assert_ne!(new_leader, original_leader);
    assert_eq!(
        cluster.states()[&original_leader],
        WorkerState::NoQuorum,
        "the original, now-isolated leader must remain in NoQuorum, never regaining Leader status"
    );

    // Heal, then restart the crashed worker: its process comes back under a
    // fresh WorkerId, a pending joiner.
    cluster.heal();
    let restarted = cluster.restart_node(&original_leader);

    // The fresh incarnation inherits none of the old election state; check the
    // one field observable through the public API.
    assert_eq!(
        cluster.states()[&restarted],
        WorkerState::Active,
        "a freshly-restarted incarnation must start Active, not resume its old Leader/NoQuorum state"
    );

    cluster.run_until_quiescent(tick_size, 60);
    assert_eq!(
        cluster.leader(),
        Some(new_leader.clone()),
        "the new leader elected during the crash must remain the sole leader after the restart rejoins"
    );
    assert_eq!(
        cluster.states()[&restarted],
        WorkerState::Active,
        "the restarted worker must have settled into an ordinary Active follower via the new \
         leader's real heartbeats"
    );

    cluster.assert_at_most_one_in_leader_state();
    assert_eq!(cluster.leader(), Some(new_leader));
}

/// Advances `cluster` one tick at a time for `ticks` ticks, checking after
/// every tick that at most one node is `Leader`, and records every leader
/// seen under the term it leads in.
fn advance_watching_leaders(
    cluster: &mut Cluster,
    ticks: u64,
    leaders_by_term: &mut BTreeMap<u64, BTreeSet<WorkerId>>,
) {
    for _ in 0..ticks {
        cluster.advance(Duration::from_ticks(1));
        cluster.assert_at_most_one_in_leader_state();
        if let Some(leader) = cluster.leader() {
            leaders_by_term
                .entry(cluster.node(&leader).term())
                .or_default()
                .insert(leader);
        }
    }
}

/// Panics unless `leader`, cut off from a quorum, has lost it: it is
/// `NoQuorum`, or in one of the roll calls it retries from there.
fn assert_cut_off_and_retrying(cluster: &Cluster, leader: &WorkerId) {
    assert!(
        matches!(
            cluster.states()[leader],
            WorkerState::NoQuorum | WorkerState::RollCall
        ),
        "{:?}",
        cluster.states()
    );
}

/// Starts a roll call on every one of `initiators` at the same instant, by
/// hand, then lets the cluster run for three suspicion timeouts, watching
/// the leaders. Every initiator contests the same term, and the lowest
/// `WorkerId` makes the best call (the nodes read one wall clock), so every
/// other initiator abandons its own call for that one, and it alone wins.
fn every_initiator_racing_at_once_elects_the_best_call(
    cluster: &mut Cluster,
    initiators: &[WorkerId],
) {
    suspect_leader_by_hand(cluster, initiators);
    let mut contested_terms = BTreeSet::new();
    for initiator in initiators {
        let started = cluster.step(initiator, Input::Tick);
        assert_eq!(
            cluster.states()[initiator],
            WorkerState::RollCall,
            "setup invariant"
        );
        contested_terms.extend(published_roll_calls(&started).iter().map(|call| call.term));
    }
    assert_eq!(
        contested_terms.len(),
        1,
        "setup invariant: every initiator contests the same term, {contested_terms:?}"
    );

    let mut leaders_by_term = BTreeMap::new();
    let suspect_ticks = cluster.suspect_timeout().as_ticks();
    advance_watching_leaders(cluster, 3 * suspect_ticks, &mut leaders_by_term);

    let best = initiators.iter().min().expect("at least one initiator");
    assert_eq!(
        cluster.leader().as_ref(),
        Some(best),
        "{:?}",
        cluster.states()
    );
    for initiator in initiators.iter().filter(|id| *id != best) {
        assert_eq!(cluster.states()[initiator], WorkerState::Active);
        assert_eq!(
            cluster.node(initiator).known_leader().map(|(id, _)| id),
            Some(best.clone())
        );
    }
    assert_eq!(
        leaders_by_term.len(),
        1,
        "one election, won at the first try: {leaders_by_term:?}"
    );
    assert_eq!(cluster.first_grant_overlap(), None);
}

#[test]
fn four_survivors_starting_roll_calls_at_once_elect_exactly_one_leader() {
    let suspect_timeout = Duration::from_ticks(10);
    let tick_size = Duration::from_ticks(5);
    let (mut cluster, old_leader) = bootstrap_5_and_elect_leader(suspect_timeout, tick_size);
    let survivors: Vec<WorkerId> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != old_leader)
        .collect();
    cluster.partition(
        [old_leader.clone()].into_iter().collect(),
        survivors.iter().cloned().collect(),
    );
    run_out_cut_off_leaders_lease(&mut cluster);

    every_initiator_racing_at_once_elects_the_best_call(&mut cluster, &survivors);
    assert_cut_off_and_retrying(&cluster, &old_leader);
}

#[test]
fn no_replacement_run_is_claimable_until_the_reconnect_timeout_has_run_out_under_a_partition() {
    let suspect_timeout = Duration::from_secs(2);
    let (mut cluster, leader) =
        bootstrap_5_and_elect_leader(suspect_timeout, Duration::from_secs(1));
    let followers: Vec<WorkerId> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != leader)
        .collect();
    let (cut_off, other) = (followers[0].clone(), followers[1].clone());

    let scheduler = cluster.scheduler_mut(&leader);
    let task = scheduler
        .submit(Submission::new(
            TaskDefinitionId::new("demo.task"),
            1,
            b"payload".to_vec(),
            "default",
        ))
        .expect("the leader's scheduler leads");
    let first = scheduler
        .request_claim(&cut_off, &task)
        .expect("the task is queued");
    assert_eq!(first.attempt_number, 1);
    scheduler
        .report_started(&cut_off, &first.task_run_id)
        .expect("the claim is fresh");

    let rest: BTreeSet<WorkerId> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != cut_off)
        .collect();
    cluster.partition(rest, [cut_off.clone()].into_iter().collect());

    // The cut-off follower was last heard at most a heartbeat interval before
    // the cut, so the leader loses it no earlier than a suspicion timeout and
    // a reconnect timeout less that interval after it. Another worker's claim
    // for the task is refused at the start of that window and again a little
    // before its end.
    let window = suspect_timeout.as_ticks() + ElectionTimings::DEFAULT_RECONNECT_TIMEOUT.as_ticks();
    let heartbeat_interval = suspect_timeout.as_ticks() / 4;
    let assert_still_selected = |cluster: &mut Cluster| {
        assert_eq!(
            cluster.states()[&leader],
            WorkerState::Leader,
            "the leader keeps its quorum of the four connected nodes"
        );
        assert_eq!(
            cluster.scheduler_mut(&leader).request_claim(&other, &task),
            Err(ClaimRejection::AlreadySelected),
            "before the reconnect timeout runs out, the original claim must still stand"
        );
    };
    assert_still_selected(&mut cluster);
    cluster.advance(Duration::from_ticks(window - heartbeat_interval - 1));
    assert_still_selected(&mut cluster);

    // Well past the window, the loss has been replayed and the task is
    // claimable again, as a second attempt.
    cluster.advance(Duration::from_ticks(2 * heartbeat_interval + 1));
    let replacement = cluster
        .scheduler_mut(&leader)
        .request_claim(&other, &task)
        .expect("the lost run's task is claimable once the reconnect timeout has run out");
    assert_eq!(replacement.attempt_number, 2);
    assert_ne!(replacement.task_run_id, first.task_run_id);
    assert_eq!(cluster.first_grant_overlap(), None);
}
