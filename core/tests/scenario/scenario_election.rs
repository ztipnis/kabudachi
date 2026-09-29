//! Scenario tests for election-message timing edge cases (README §26.1) and
//! `SELF_REMOVE` through the network layer (README §26.2), built on the
//! `Cluster` harness.

use crate::support::builders::{
    configuration_of, heartbeat, heartbeat_message, leader_ack, make_network, message, timings,
    worker,
};

use crate::support::clock::FakeClock;
use crate::support::harness::Cluster;
use crate::support::node::{commit_founding, connect, deliver, elect, sent, sent_to};
use crate::support::scenarios::{
    bootstrap_5_and_elect_leader, elect_new_leader_among, run_out_cut_off_leaders_lease,
    suspect_leader_by_hand,
};
use kabudachi_core::configuration::{Configuration, Generation, Single};
use kabudachi_core::election::Input;
use kabudachi_core::protocol::ids::WorkerId;
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::messages::{ElectionMessage, LeaderHeartbeatAck, election_message};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::Duration;

fn heartbeat_ack_message(leader_id: WorkerId, recovery_epoch: u64, term: u64) -> ElectionMessage {
    message(election_message::Payload::HeartbeatAck(
        LeaderHeartbeatAck {
            recovery_epoch,
            ..leader_ack(&leader_id, term, &configuration_of(5), None)
        },
    ))
}

/// Bootstraps 3 nodes and lets them elect a leader: the first to suspect
/// after its jittered suspicion timeout wins. Returns `(cluster, leader,
/// followers)` fully settled.
///
/// Not inside a partition: a node cut off during the election misses the
/// winning roll call, so it is not in the leader's roster, and its
/// confirmations never count toward the leader's lease until an election
/// founds a configuration that admits it. The scenarios below then cut one
/// follower off, and the leader of 3 must keep its quorum with the other.
fn bootstrap_and_elect_leader_n3(
    suspect_timeout: Duration,
    tick_size: Duration,
) -> (Cluster, WorkerId, Vec<WorkerId>) {
    let mut cluster = Cluster::bootstrap(3, suspect_timeout);

    for _ in 0..3 {
        cluster.advance(tick_size);
    }
    let iterations = cluster.run_until_quiescent(tick_size, 60);
    assert!(
        iterations < 60,
        "expected quiescence well before max_ticks, ran all {iterations}"
    );

    let leader = cluster.leader().expect("the three must elect a leader");
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

// A false suspicion refused, then a real leader loss: f1's refused roll call
// took a term, but no node keeps contesting it. f1 is the lower `WorkerId`,
// so its call ranks better than any f2 makes for the same term, and f1
// passes over f2's; both must still move on to a later term and elect.
#[test]
fn a_refused_false_suspicion_then_a_real_leader_loss_still_elects() {
    let suspect_timeout = Duration::from_ticks(10);
    let tick_size = Duration::from_ticks(5);
    let (mut cluster, leader, followers) =
        bootstrap_and_elect_leader_n3(suspect_timeout, tick_size);
    let (f1, f2) = (followers[0].clone(), followers[1].clone());

    // f1 alone loses its leader for a while: f2 refuses its roll call.
    cluster.partition(
        [f1.clone()].into_iter().collect(),
        [leader.clone()].into_iter().collect(),
    );
    for _ in 0..4 {
        cluster.advance(tick_size);
    }
    assert_eq!(
        cluster.states()[&leader],
        WorkerState::Leader,
        "setup invariant: the leader is never challenged"
    );
    assert_eq!(
        cluster.states()[&f2],
        WorkerState::Active,
        "setup invariant: f2 kept receiving real heartbeats throughout"
    );
    assert!(
        matches!(
            cluster.states()[&f1],
            WorkerState::RollCall | WorkerState::NoQuorum
        ),
        "f1 must have timed out and begun its own roll call: {:?}",
        cluster.states()
    );
    cluster.heal();
    for _ in 0..3 {
        cluster.advance(tick_size);
    }
    assert_eq!(cluster.leader(), Some(leader.clone()), "setup invariant");
    assert_eq!(
        cluster.states()[&f1],
        WorkerState::Active,
        "setup invariant"
    );

    // Now the leader is lost for real.
    cluster.partition(
        [leader.clone()].into_iter().collect(),
        [f1.clone(), f2.clone()].into_iter().collect(),
    );
    for _ in 0..20 {
        cluster.advance(tick_size);
    }

    let new_leader = cluster
        .leader()
        .filter(|id| *id != leader)
        .unwrap_or_else(|| panic!("f1 and f2 must elect a leader: {:?}", cluster.states()));
    let follower = if new_leader == f1 { &f2 } else { &f1 };
    assert_eq!(cluster.states()[follower], WorkerState::Active);
}

// Scenario 2: after the old leader is isolated and replaced, a stale ack
// carrying its old term is rejected by a follower now under the new leader
// (`ack.term < highest_term_seen`), through the full harness.
#[test]
fn stale_message_after_replacement_is_rejected() {
    let suspect_timeout = Duration::from_ticks(10);
    let tick_size = Duration::from_ticks(5);

    // Five nodes, not three: the old leader, once `NoQuorum`, follows no one
    // and confirms no ack, so with probe cut off a three-node new leader
    // would lose its lease before probe even suspected it. With five, the
    // two other followers keep the new leader's quorum.
    let (mut cluster, old_leader) = bootstrap_5_and_elect_leader(suspect_timeout, tick_size);
    let others: Vec<WorkerId> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != old_leader)
        .collect();

    // The first roll call of a fresh cluster contests term 1.
    let old_term = 1;
    assert_eq!(
        cluster.node(&old_leader).term(),
        old_term,
        "setup invariant"
    );

    cluster.partition(
        [old_leader.clone()].into_iter().collect(),
        others.iter().cloned().collect(),
    );
    run_out_cut_off_leaders_lease(&mut cluster);
    suspect_leader_by_hand(&mut cluster, &others);
    let new_leader = elect_new_leader_among(&mut cluster, &others);
    assert_ne!(new_leader, old_leader);
    let probe = others
        .iter()
        .find(|id| **id != new_leader)
        .expect("four nodes elected one of themselves")
        .clone();
    assert_eq!(
        cluster.states()[&probe],
        WorkerState::Active,
        "setup invariant: a non-leader majority member must have settled Active"
    );

    // Cut probe off from the new leader. old_leader goes with new_leader, and
    // the two other followers still reach both sides, so the new leader
    // keeps its quorum (3 of 5) and stays Leader. `FakeNetwork` holds one
    // partition, so this replaces the previous one.
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

    // Once suspicious, probe starts its own roll call (as in scenario 1),
    // which the two other followers, still hearing from the new leader,
    // refuse.
    for _ in 0..4 {
        cluster.advance(tick_size);
    }
    assert!(
        matches!(
            cluster.states()[&probe],
            WorkerState::RollCall | WorkerState::NoQuorum
        ),
        "setup invariant: {:?}",
        cluster.states()
    );

    // A stale ack from the old leader: term 1, while probe's
    // highest_term_seen is 2. It must be rejected.
    let before = cluster.states()[&probe];
    cluster.step(
        &probe,
        Input::Message {
            from: old_leader.clone(),
            message: heartbeat_ack_message(old_leader.clone(), 0, old_term),
        },
    );
    assert_eq!(
        cluster.states()[&probe],
        before,
        "a stale LeaderHeartbeatAck (term 1, below the real current term 2) must be rejected \
         outright and must NOT return probe to Active"
    );

    // Positive control: a genuine current-term heartbeat still gets through.
    cluster.heal();
    cluster.run_until_quiescent(tick_size, 60);
    assert_eq!(
        cluster.states()[&probe],
        WorkerState::Active,
        "a genuine, current-term ack must still return probe to Active"
    );

    cluster.assert_at_most_one_in_leader_state();
    assert_eq!(cluster.leader(), Some(new_leader));
}

// Scenario 3: two initiators race for the same term. Every voter answers
// the better call (the lower `WorkerId`, as the nodes read one wall clock),
// the worse initiator abandons its own call for it, and the better one wins,
// whichever call each node hears first.
fn run_two_initiators_scenario(better_first: bool) {
    let suspect_timeout = Duration::from_ticks(10);
    let mut cluster = Cluster::bootstrap(3, suspect_timeout);
    let ids: Vec<WorkerId> = cluster.node_ids().into_iter().collect();
    let (better, worse) = (ids[0].clone(), ids[1].clone());

    suspect_leader_by_hand(&mut cluster, &ids);

    // Start only the two initiators; the third stays in LeaderSuspect, where
    // it answers roll calls and votes.
    let order = if better_first {
        [&better, &worse]
    } else {
        [&worse, &better]
    };
    for initiator in order {
        cluster.step(initiator, Input::Tick);
        assert_eq!(
            cluster.states()[initiator],
            WorkerState::RollCall,
            "setup invariant"
        );
    }

    cluster.deliver_messages();
    cluster.advance_clock_only(timings(suspect_timeout).roll_call_deadline);
    cluster.step(&better, Input::Tick);
    cluster.deliver_messages();

    assert_eq!(
        cluster.states()[&better],
        WorkerState::Leader,
        "the better call's initiator wins"
    );
    assert_eq!(cluster.node(&better).term(), 1);
    assert_ne!(
        cluster.states()[&worse],
        WorkerState::Leader,
        "the worse initiator never stands for its abandoned call"
    );
    assert_ne!(cluster.states()[&worse], WorkerState::Candidate);
    cluster.assert_at_most_one_in_leader_state();
}

#[test]
fn two_initiators_racing_for_the_same_term() {
    run_two_initiators_scenario(true);
    run_two_initiators_scenario(false);
}

// Scenario 6: with `FakeNetwork::set_duplicate_rate(1.0)` every send is
// scheduled twice. The leader's handling of a `SelfRemove` must be
// idempotent through the network layer: both copies are delivered and
// dispatched as messages, and the configuration it announces shrinks by
// exactly one voter at exactly one generation.
#[test]
fn self_remove_duplicated() {
    let clock = FakeClock::new();
    let suspect_timeout = 10;

    let leader_id = worker("leader");
    let drainer = worker("drainer");
    let third = worker("third"); // Present purely so the configuration has 3 voters, not 2.
    let everyone = [leader_id.clone(), drainer.clone(), third.clone()];
    let network = make_network(&clock, &everyone);
    network.set_duplicate_rate(1.0);

    let mut leader = crate::support::node::voter_node(&clock, &leader_id, 3, suspect_timeout);
    connect(&mut leader, &[drainer.clone(), third.clone()]);
    elect(
        &mut leader,
        &clock,
        suspect_timeout,
        &[drainer.clone(), third.clone()],
    );
    commit_founding(&mut leader, &clock, &[drainer.clone(), third.clone()]);
    let mut drainer_node = crate::support::node::voter_node(&clock, &drainer, 3, suspect_timeout);
    connect(&mut drainer_node, &[leader_id.clone(), third.clone()]);
    // The drainer follows the leader, from the leader's ack of its heartbeat.
    let acked = deliver(
        &mut leader,
        &drainer,
        heartbeat_message(heartbeat(&drainer, None)),
    );
    for message in sent_to(&acked, &drainer) {
        deliver(&mut drainer_node, &leader_id, message);
    }

    // Active -> Stopped; the SelfRemove to its leader goes through the
    // network, which duplicates it.
    let drained = drainer_node.step(Input::Drain);
    assert_eq!(drainer_node.state(), WorkerState::Stopped);
    for (to, message) in sent(&drained.outputs) {
        network.send(drainer.clone(), to, message);
    }

    // Confirm duplication really happened: rate 1.0 delivers exactly 2 copies.
    let inbox: Vec<(WorkerId, ElectionMessage)> = network
        .take_due()
        .into_iter()
        .filter(|due| due.to == leader_id)
        .map(|due| (due.from, due.message))
        .collect();
    assert_eq!(
        inbox.len(),
        2,
        "the leader must have received exactly 2 real copies of the SelfRemove"
    );
    for (from, msg) in inbox {
        assert!(matches!(
            &msg.payload,
            Some(election_message::Payload::SelfRemove(sr)) if sr.worker_id() == drainer
        ));
        deliver(&mut leader, &from, msg);
    }
    // The removal takes effect with the leader's next ack.
    let acked = deliver(
        &mut leader,
        &third,
        heartbeat_message(heartbeat(&third, None)),
    );
    assert_eq!(sent_to(&acked, &third).len(), 1);

    let shrunk_at = Generation::new(0, leader.term(), 1)
        .next_change(leader.term())
        .next_change(leader.term());
    assert_eq!(
        leader.configuration(),
        Some(&Configuration::single(Single {
            generation: shrunk_at,
            base: shrunk_at,
            voter_count: 2,
        })),
        "the configuration must have shrunk by EXACTLY one voter, at EXACTLY one generation, \
         despite the duplicate delivery"
    );
}
