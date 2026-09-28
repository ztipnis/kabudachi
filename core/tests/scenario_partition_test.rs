//! Scenario tests for partitions and isolation (README §26.2), built on the
//! `Cluster` harness, including the regression for the split brain the ring
//! roll call produced when every survivor of a lost leader raced, and races
//! of four or more roll calls started at the same instant.

mod support;

use support::scenarios::{
    bootstrap_5_and_elect_leader, elect_new_leader_among, run_out_cut_off_leaders_lease,
    suspect_leader_by_hand,
};

use support::builders::{ack_message, configuration_of, g0, leader_ack};

use std::collections::{BTreeMap, BTreeSet};

use kabudachi_core::election::Input;
use kabudachi_core::protocol::ids::WorkerId;
use kabudachi_core::protocol::messages::{ElectionMessage, LeaderHeartbeatAck};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::Duration;
use support::harness::Cluster;
use support::node::published_roll_calls;

fn heartbeat_ack_message(leader_id: WorkerId, recovery_epoch: u64, term: u64) -> ElectionMessage {
    ack_message(LeaderHeartbeatAck {
        recovery_epoch,
        ..leader_ack(&leader_id, term, &configuration_of(5), Some(g0()))
    })
}

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
fn partition_50_50_neither_side_reaches_quorum() {
    let suspect_timeout = Duration::from_ticks(10);
    let tick_size = Duration::from_ticks(5);

    let mut cluster = Cluster::bootstrap(4, suspect_timeout);
    let ids: Vec<WorkerId> = cluster.node_ids().into_iter().collect();
    let group_a: BTreeSet<WorkerId> = ids[..2].iter().cloned().collect();
    let group_b: BTreeSet<WorkerId> = ids[2..].iter().cloned().collect();

    // Quorum for 4 nodes is 3, and each 2-node group caps at 2 responses.
    cluster.partition(group_a.clone(), group_b.clone());

    for _ in 0..3 {
        cluster.advance(tick_size);
    }
    let iterations = cluster.run_until_quiescent(tick_size, 60);
    assert!(
        iterations < 60,
        "expected quiescence well before max_ticks, ran all {iterations}"
    );

    assert_eq!(
        cluster.leader(),
        None,
        "neither 2-node group can ever reach quorum-of-3; states: {:?}",
        cluster.states()
    );
    for (id, state) in cluster.states() {
        assert_ne!(
            state,
            WorkerState::Leader,
            "node {id:?} must never become Leader"
        );
    }
}

#[test]
fn partition_51_49_majority_side_elects_new_leader() {
    let suspect_timeout = Duration::from_ticks(10);
    let tick_size = Duration::from_ticks(5);

    let mut cluster = Cluster::bootstrap(5, suspect_timeout);
    let ids: Vec<WorkerId> = cluster.node_ids().into_iter().collect();
    let majority: BTreeSet<WorkerId> = ids[..3].iter().cloned().collect();
    let minority: BTreeSet<WorkerId> = ids[3..].iter().cloned().collect();

    // Quorum for 5 nodes is 3: the majority-of-3 side reaches it alone, the
    // minority-of-2 side cannot.
    cluster.partition(majority.clone(), minority.clone());

    for _ in 0..3 {
        cluster.advance(tick_size);
    }
    cluster.run_until_quiescent(tick_size, 60);

    let leader = cluster
        .leader()
        .expect("the majority-of-3 side must elect a leader while partitioned");
    assert!(
        majority.contains(&leader),
        "the leader must be a majority-side node, got {leader:?}"
    );

    for id in &minority {
        assert_ne!(
            cluster.states()[id],
            WorkerState::Leader,
            "the minority-of-2 side can never reach quorum-of-3 and so must never elect a leader"
        );
    }
    cluster.assert_at_most_one_leader();
}

#[test]
fn leader_isolated_alone() {
    let suspect_timeout = Duration::from_ticks(10);
    let tick_size = Duration::from_ticks(5);

    let (mut cluster, original_leader) = bootstrap_5_and_elect_leader(suspect_timeout, tick_size);

    // Partition the leader from all 4 followers; the majority elects a new one.
    let new_leader = isolate_leader_and_elect_new(&mut cluster, &original_leader);

    assert_ne!(
        new_leader, original_leader,
        "the newly-elected leader must be a genuinely different WorkerId"
    );
    assert_eq!(
        cluster.states()[&original_leader],
        WorkerState::NoQuorum,
        "the original, now-isolated leader must remain in NoQuorum, never regaining Leader status"
    );
    cluster.assert_at_most_one_leader();
    assert_eq!(cluster.leader(), Some(new_leader));
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
    cluster.assert_at_most_one_leader();
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

    // Heal, then restart the crashed worker: its process comes back under a
    // fresh WorkerId (ADR-0001, amended 2026-09-27), a pending joiner.
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

    // Stale-message rejection through the full harness: `state()` cannot tell a
    // rejected ack from an accepted one for an Active/Leader node, so drive one
    // follower ("probe") out of Active first, where the difference is visible.
    let probe = majority_non_leader(&cluster, &new_leader, &restarted);

    // Cut probe off from the real leader only, so it stops receiving
    // heartbeats and suspects but can still receive injected messages. Past
    // its suspicion timeout it starts its own roll call at once, which the
    // other followers, still hearing from the leader, refuse, so the call
    // closes short of a quorum.
    cluster.partition(
        [probe.clone()].into_iter().collect(),
        [new_leader.clone()].into_iter().collect(),
    );
    for _ in 0..4 {
        cluster.advance(tick_size);
    }

    assert!(
        matches!(
            cluster.states()[&probe],
            WorkerState::RollCall | WorkerState::NoQuorum
        ),
        "probe's own roll call is refused: {:?}",
        cluster.states()
    );

    // A stale ack from the old leader (term 1) while every real participant's
    // highest_term_seen is 2 (from the post-crash election): it must be ignored.
    cluster.network().send(
        original_leader.clone(),
        probe.clone(),
        heartbeat_ack_message(original_leader.clone(), 0, 1),
    );
    cluster.advance(tick_size);
    assert_ne!(
        cluster.states()[&probe],
        WorkerState::Active,
        "a stale LeaderHeartbeatAck (term 1, below the real current term 2) must be rejected \
         outright and must NOT return probe to Active"
    );

    // Positive control: a genuine current-term heartbeat still reaches probe.
    cluster.heal();
    cluster.run_until_quiescent(tick_size, 60);
    assert_eq!(
        cluster.states()[&probe],
        WorkerState::Active,
        "a genuine, current-term ack must still return probe to Active"
    );

    cluster.assert_at_most_one_leader();
    assert_eq!(cluster.leader(), Some(new_leader));
}

/// A follower that is neither the current leader nor the restarted worker.
fn majority_non_leader(cluster: &Cluster, leader: &WorkerId, restarted: &WorkerId) -> WorkerId {
    cluster
        .node_ids()
        .into_iter()
        .find(|id| id != leader && id != restarted)
        .expect(
            "a 5-node cluster must have at least one ordinary follower besides leader/restarted",
        )
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
        cluster.assert_at_most_one_leader();
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

// The ring roll call's split brain (a since-deleted `net` test documented
// it): with every survivor of a lost leader suspecting at the same instant,
// independent ring calls elected a permanent second leader. With the gossip
// roll call, the survivors' calls for the same term are ranked, every
// survivor answers the best one, and exactly one of them leads; the same
// holds when that one is lost in turn and three are left.
#[test]
fn every_survivor_of_a_lost_leader_racing_elects_exactly_one_leader() {
    let suspect_timeout = Duration::from_ticks(10);
    let tick_size = Duration::from_ticks(5);
    let mut leaders_by_term = BTreeMap::new();

    let (mut cluster, first_leader) = bootstrap_5_and_elect_leader(suspect_timeout, tick_size);
    let survivors: BTreeSet<WorkerId> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != first_leader)
        .collect();

    // Cut the leader off. Every survivor last heard from it at the same
    // instant, so each one's leader contact is stale by the time the first
    // suspects it, after its jittered suspicion timeout.
    cluster.partition(
        [first_leader.clone()].into_iter().collect(),
        survivors.clone(),
    );
    advance_watching_leaders(
        &mut cluster,
        6 * suspect_timeout.as_ticks(),
        &mut leaders_by_term,
    );

    let second_leader = cluster
        .leader()
        .expect("the four survivors must elect a leader");
    assert!(survivors.contains(&second_leader));
    for survivor in survivors.iter().filter(|id| **id != second_leader) {
        assert_eq!(
            cluster.states()[survivor],
            WorkerState::Active,
            "every other survivor follows the one leader"
        );
        assert_eq!(
            cluster.node(survivor).known_leader().map(|(id, _)| id),
            Some(second_leader.clone())
        );
    }
    assert_cut_off_and_retrying(&cluster, &first_leader);

    // Cut the second leader off too. The three left are exactly a quorum of
    // the configuration of five, so all three must answer the one call that
    // wins.
    let last_three: BTreeSet<WorkerId> = survivors
        .iter()
        .filter(|id| **id != second_leader)
        .cloned()
        .collect();
    cluster.partition(
        [first_leader.clone(), second_leader.clone()]
            .into_iter()
            .collect(),
        last_three.clone(),
    );
    advance_watching_leaders(
        &mut cluster,
        6 * suspect_timeout.as_ticks(),
        &mut leaders_by_term,
    );

    let third_leader = cluster
        .leader()
        .expect("the three left must elect a leader");
    assert!(last_three.contains(&third_leader));
    for id in last_three.iter().filter(|id| **id != third_leader) {
        assert_eq!(cluster.states()[id], WorkerState::Active);
    }
    assert_cut_off_and_retrying(&cluster, &second_leader);

    for (term, leaders) in &leaders_by_term {
        assert_eq!(
            leaders.len(),
            1,
            "term {term} had more than one leader: {leaders:?}"
        );
    }
    // A roll call that failed on the way takes its term, so the survivors'
    // leaders need not lead the very next terms.
    assert_eq!(
        leaders_by_term.len(),
        3,
        "the first leader until its lease ran out, then one leader for the four survivors \
         and one for the three: {leaders_by_term:?}"
    );
    assert_eq!(leaders_by_term.keys().next(), Some(&1));
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
fn seven_nodes_starting_roll_calls_at_once_elect_exactly_one_leader() {
    let mut cluster = Cluster::bootstrap(7, Duration::from_ticks(10));
    let everyone: Vec<WorkerId> = cluster.node_ids().into_iter().collect();

    every_initiator_racing_at_once_elects_the_best_call(&mut cluster, &everyone);
}
