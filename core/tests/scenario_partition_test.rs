//! Scenario tests for partitions, isolation and ring topology (README §26.2),
//! built on the `Cluster` harness.
//!
//! Known gap: with 4 or more mutually reachable nodes that start suspecting at
//! the same instant, independent roll calls can compute different next terms
//! from partial response sets and elect two leaders, because nothing demotes
//! an elected `Leader`/`Candidate` on seeing a higher-term certificate. No two
//! nodes start a roll call at the same instant here: either the partition caps
//! the group at 3, or `elect_new_leader_among` starts exactly one pre-computed
//! winner by hand and completes its election through real `VoteRequest`/
//! `VoteGrant` traffic.

mod support;

use support::scenarios::{
    bootstrap_5_and_elect_leader, drain_pending_messages, elect_new_leader_among,
};

use support::builders::shard;

use support::candidate::predict_winner;

use std::collections::BTreeSet;

use kabudachi_core::membership::{MembershipView, RingMembership};
use kabudachi_core::protocol::ids::{IncarnationId, WorkerId};
use kabudachi_core::protocol::messages::{ElectionMessage, LeaderHeartbeatAck, election_message};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::Duration;
use kabudachi_core::transport::PeerMessenger;
use support::harness::Cluster;

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

/// Partitions `leader` from everyone else, crosses `suspect_timeout` for the
/// 4-member majority, checks the isolated leader detects its peer loss (`Leader
/// -> NoQuorum`), then elects a new leader among the majority with
/// `elect_new_leader_among`. Returns the new leader.
fn isolate_leader_and_elect_new(
    cluster: &mut Cluster,
    leader: &WorkerId,
    prior_highest_term_seen: u64,
    tick_size: Duration,
) -> WorkerId {
    let others: Vec<WorkerId> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| id != leader)
        .collect();
    let leader_group: BTreeSet<WorkerId> = [leader.clone()].into_iter().collect();
    let others_group: BTreeSet<WorkerId> = others.iter().cloned().collect();
    cluster.partition(leader_group, others_group);

    // `partition()` only blocks new sends, so flush the heartbeat already scheduled before the cut.
    cluster.advance(tick_size);

    for _ in 0..3 {
        cluster.advance(tick_size);
    }

    assert_eq!(
        cluster.states()[leader],
        WorkerState::NoQuorum,
        "the isolated leader must detect its own peer loss (Leader -> NoQuorum)"
    );
    for id in &others {
        assert_eq!(
            cluster.states()[id],
            WorkerState::LeaderSuspect,
            "isolate_leader_and_elect_new setup invariant"
        );
    }

    elect_new_leader_among(cluster, &others, prior_highest_term_seen)
}

#[test]
fn partition_50_50_neither_side_reaches_quorum() {
    let suspect_timeout = Duration::from_ticks(10);
    let tick_size = Duration::from_ticks(5);

    let mut cluster = Cluster::bootstrap(4, suspect_timeout);
    let ids: Vec<WorkerId> = cluster.node_ids().into_iter().collect();
    let group_a: BTreeSet<WorkerId> = ids[..2].iter().cloned().collect();
    let group_b: BTreeSet<WorkerId> = ids[2..].iter().cloned().collect();

    // Quorum for 4 nodes is 3, and each 2-node group caps at 2 responses. Only
    // 2 nodes suspect on each side, so plain advance()/run_until_quiescent is
    // safe.
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
    let new_leader = isolate_leader_and_elect_new(&mut cluster, &original_leader, 1, tick_size);

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
    let new_leader = isolate_leader_and_elect_new(&mut cluster, &original_leader, 1, tick_size);
    assert_ne!(new_leader, original_leader);

    // Heal, then "restart" the crashed worker as a fresh node: same WorkerId, new IncarnationId.
    cluster.heal();
    let new_incarnation = IncarnationId::new(format!("{}-incarnation-1", original_leader.as_str()));
    cluster.restart_node(&original_leader, new_incarnation);

    // The fresh incarnation inherits none of the old election state; check the
    // one field observable through the public API.
    assert_eq!(
        cluster.states()[&original_leader],
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
        cluster.states()[&original_leader],
        WorkerState::Active,
        "the restarted worker must have settled into an ordinary Active follower via the new \
         leader's real heartbeats"
    );

    // Stale-message rejection through the full harness: `state()` cannot tell a
    // rejected ack from an accepted one for an Active/Leader node, so drive one
    // follower ("probe") into RollCall first, where the difference is visible.
    let probe = majority_non_leader(&cluster, &new_leader, &original_leader);

    // Cut probe off from the real leader only, so it stops receiving
    // heartbeats and suspects but can still receive injected messages.
    cluster.partition(
        [probe.clone()].into_iter().collect(),
        [new_leader.clone()].into_iter().collect(),
    );
    // Flush the heartbeat already scheduled before the cut.
    cluster.advance(tick_size);
    for _ in 0..3 {
        cluster.advance(tick_size);
    }
    assert_eq!(
        cluster.states()[&probe],
        WorkerState::LeaderSuspect,
        "setup invariant"
    );
    cluster.node(&probe).tick(); // LeaderSuspect -> RollCall (its own roll call is harmless here: see below).
    assert_eq!(
        cluster.states()[&probe],
        WorkerState::RollCall,
        "setup invariant"
    );
    let all_ids: Vec<WorkerId> = cluster.node_ids().into_iter().collect();
    drain_pending_messages(&mut cluster, &all_ids);

    assert_eq!(
        cluster.states()[&probe],
        WorkerState::RollCall,
        "probe must still be in RollCall after its own (harmless) roll call dead-ends"
    );

    // A stale ack from the old leader (term 1) while every real participant's
    // highest_term_seen is 2 (from the post-crash election): it must be ignored.
    cluster.network().send(
        original_leader.clone(),
        probe.clone(),
        heartbeat_ack_message(original_leader.clone(), 0, 1),
    );
    cluster.advance(tick_size);
    assert_eq!(
        cluster.states()[&probe],
        WorkerState::RollCall,
        "a stale LeaderHeartbeatAck (term 1, below the real current term 2) must be rejected \
         outright and must NOT flip probe out of RollCall"
    );

    // Positive control: a genuine current-term heartbeat still reaches probe.
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

#[test]
fn ring_fragmentation() {
    let suspect_timeout = Duration::from_ticks(10);
    let tick_size = Duration::from_ticks(5);

    let mut cluster = Cluster::bootstrap(5, suspect_timeout);
    let ids: Vec<WorkerId> = cluster.node_ids().into_iter().collect(); // worker-0..worker-4, ascending.
    // The fragmented node must not win the final 5-response election at term 2, whatever the hash.
    let term_2_winner = predict_winner(&shard(SHARD), 0, 2, &ids);
    let w0 = ids.iter().find(|id| **id != term_2_winner).unwrap().clone();
    let rest: Vec<WorkerId> = ids.iter().filter(|id| **id != w0).cloned().collect();

    // Worker-0's ring successors, from the same ring logic the nodes use.
    let membership = RingMembership::new(ids.iter().cloned().collect());
    let successors = membership.ring_successors(w0.clone());
    assert_eq!(
        successors.len(),
        3,
        "sanity check: a 5-node ring's RING_FANOUT=3 successors list should have exactly 3 entries"
    );

    // Sever worker-0 from every one of its ring successors, but not from
    // worker-4, so its own roll call dead-ends while it can still receive one
    // forwarded through worker-4.
    let group_a: BTreeSet<WorkerId> = [w0.clone()].into_iter().collect();
    let group_b: BTreeSet<WorkerId> = successors.iter().cloned().collect();
    cluster.partition(group_a, group_b);

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

    //
    // A dead-ended roll call leaves a node parked in `RollCall`, where it can
    // accept another node's later roll call once quorum is satisfied (intended).
    // So worker-0 is the only node to start a real roll call here; the others
    // take part through `elect_new_leader_among`.
    cluster.node(&w0).tick();
    drain_pending_messages(&mut cluster, &ids);
    assert_eq!(
        cluster.states()[&w0],
        WorkerState::RollCall,
        "worker-0's own roll call must dead-end (no reachable successor to forward to), leaving \
         it parked in RollCall — never Candidate/Leader from its OWN attempt"
    );
    for id in &rest {
        assert_eq!(
            cluster.states()[id],
            WorkerState::LeaderSuspect,
            "worker-0's dead-ended roll call must have zero observable effect on any other node"
        );
    }

    //
    // The remainder is elected through `elect_new_leader_among`. Its winner's
    // own roll call is forwarded on to worker-0 (the worker-4 -> worker-0 link
    // is intact).
    let new_leader = elect_new_leader_among(&mut cluster, &rest, 0);
    assert!(
        rest.contains(&new_leader),
        "the new leader must come from the healthy 4-node remainder"
    );

    // Deliver the winner's forwarded roll call to worker-0 too.
    drain_pending_messages(&mut cluster, &ids);

    // worker-0 sees all 5 responses at next_term 2. It was chosen not to win
    // that set, so it declines, dead-ends again and stays parked in `RollCall`.
    assert_ne!(
        cluster.states()[&w0],
        WorkerState::Candidate,
        "worker-0 must correctly decline to become Candidate when it processes the healthy \
         remainder's forwarded roll call — it isn't the deterministic winner over the resulting \
         5-response set"
    );
    assert_eq!(
        cluster.states()[&w0],
        WorkerState::RollCall,
        "having correctly declined (not won the tie-break) and having no reachable successor of \
         its own to forward to, worker-0 dead-ends again and remains parked in RollCall"
    );

    // worker-0's forwarding is broken, yet the rest of the ring converges on one leader.
    assert_eq!(cluster.leader(), Some(new_leader.clone()));
    assert_ne!(w0, new_leader);
    cluster.assert_at_most_one_leader();
}
