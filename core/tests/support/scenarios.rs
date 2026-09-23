//! Election helpers shared by the scenario tests. They let a scenario elect a
//! leader among 4 or more nodes without two of them starting a roll call at the
//! same instant, which can elect two leaders in different terms.

use std::collections::BTreeSet;

use kabudachi_core::protocol::ids::WorkerId;
use kabudachi_core::protocol::messages::RollCallObservation;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::Duration;
use kabudachi_core::transport::PeerMessenger;

use crate::support::builders::{observation, roll_call, roll_call_message, shard};
use crate::support::candidate::predict_winner;
use crate::support::harness::Cluster;

/// Pumps the network and delivers every pending message to its addressee
/// through `on_message` until none is left. Unlike `Cluster::advance` it never
/// calls `tick()`, so no node can start a competing roll call meanwhile.
pub fn drain_pending_messages(cluster: &mut Cluster, ids: &[WorkerId]) {
    loop {
        let delivered = cluster.network().pump();
        let mut any = delivered > 0;
        for id in ids {
            let inbox = cluster.network().poll_inbox(id.clone());
            if !inbox.is_empty() {
                any = true;
            }
            for (from, msg) in inbox {
                cluster.node(id).on_message(from, msg);
            }
        }
        if !any {
            break;
        }
    }
}

/// Elects a real, quorum-granted `Leader` among `members` (all `LeaderSuspect`
/// and mutually reachable) while letting only one of them start a roll call.
///
/// It predicts which member a real roll call would pick and ticks only that
/// one into `RollCall`. Its own roll call is harmless because every other
/// member is `LeaderSuspect` and cannot accept it. The election is completed
/// by delivering a synthetic roll call carrying the others' observations at
/// `prior_highest_term_seen` (which must be every member's actual value), then
/// draining the real `VoteRequest`/`VoteGrant` traffic.
pub fn elect_new_leader_among(
    cluster: &mut Cluster,
    members: &[WorkerId],
    prior_highest_term_seen: u64,
) -> WorkerId {
    for id in members {
        assert_eq!(
            cluster.states()[id],
            WorkerState::LeaderSuspect,
            "every member must start LeaderSuspect"
        );
    }

    let next_term = prior_highest_term_seen + 1;
    let winner = predict_winner(&shard("shard-1"), 0, next_term, members);
    let others: Vec<WorkerId> = members
        .iter()
        .filter(|id| **id != winner)
        .cloned()
        .collect();

    cluster.node(&winner).tick();
    assert_eq!(
        cluster.states()[&winner],
        WorkerState::RollCall,
        "the winner must start its own roll call"
    );

    let synthetic_initiator = others[0].clone();
    let responses: Vec<RollCallObservation> = others
        .iter()
        .map(|id| observation(id.clone(), prior_highest_term_seen))
        .collect();
    let mut call = roll_call(
        "synthetic-reelection-call",
        synthetic_initiator.clone(),
        responses,
    );
    // A real roll call's own `highest_term_seen` is fixed once at its origin
    // (`begin_roll_call`) to the originator's `highest_term_seen` at that
    // moment — this synthetic call must carry the same value, since
    // `choose_candidate` now derives the contested term from exactly this
    // field (chunk C7-fix), not from the accumulated observations' own
    // values as it used to. `roll_call`'s default (0) would otherwise
    // silently contest term 1 regardless of `prior_highest_term_seen`.
    call.highest_term_seen = prior_highest_term_seen;
    cluster
        .node(&winner)
        .on_message(synthetic_initiator, roll_call_message(call));
    assert_eq!(
        cluster.states()[&winner],
        WorkerState::Candidate,
        "the winner must become Candidate once the synthetic call reaches quorum"
    );

    drain_pending_messages(cluster, members);

    assert_eq!(
        cluster.states()[&winner],
        WorkerState::Leader,
        "the winner must reach quorum-granted Leader"
    );
    winner
}

/// Bootstraps 5 nodes and elects a real leader with heartbeats flowing, inside
/// a temporary 3-vs-2 partition: the 3-node side reaches quorum alone while the
/// 2-node side cannot. After healing, the minority's stuck `RollCall` nodes pick
/// up the leader's heartbeat and return to `Active`. Returns `(cluster,
/// leader)` settled.
pub fn bootstrap_5_and_elect_leader(
    suspect_timeout: Duration,
    tick_size: Duration,
) -> (Cluster, WorkerId) {
    let mut cluster = Cluster::bootstrap(5, suspect_timeout);
    let ids: Vec<WorkerId> = cluster.node_ids().into_iter().collect();
    let majority: BTreeSet<WorkerId> = ids[..3].iter().cloned().collect();
    let minority: BTreeSet<WorkerId> = ids[3..].iter().cloned().collect();

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
        "the leader must be a majority-side node"
    );

    cluster.heal();
    cluster.run_until_quiescent(tick_size, 60);

    for id in &ids {
        let expected = if *id == leader {
            WorkerState::Leader
        } else {
            WorkerState::Active
        };
        assert_eq!(
            cluster.states()[id],
            expected,
            "the whole 5-node cluster must settle after healing"
        );
    }

    (cluster, leader)
}
