//! Tests of the `Cluster` simulation harness itself, including that a full
//! election cycle can be driven through the harness alone (test 3 uses
//! `Cluster::network()` to check message-level delivery and drops directly),
//! and that it tells each node which peers it is connected to.

mod support;

use support::builders::{configuration_of, g0, past_any_suspicion, timings, worker};

use std::collections::{BTreeMap, BTreeSet};

use kabudachi_core::election::Input;
use kabudachi_core::protocol::ids::{ShardId, WorkerId};
use kabudachi_core::protocol::messages::{ElectionMessage, LeaderHeartbeatAck, election_message};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::Duration;
use support::harness::Cluster;
use support::scenarios::{
    bootstrap_5_and_elect_leader, run_out_cut_off_leaders_lease, suspect_leader_by_hand,
};

/// A `LeaderHeartbeatAck` message from `leader`, at epoch and term 0 so any node accepts it.
fn heartbeat_ack(shard_id: &str, leader: &WorkerId) -> ElectionMessage {
    ElectionMessage {
        payload: Some(election_message::Payload::HeartbeatAck(
            LeaderHeartbeatAck {
                shard_id: Some(ShardId::new(shard_id).into()),
                leader_id: Some(leader.clone().into()),
                recovery_epoch: 0,
                term: 0,
                configuration: Some((&support::builders::configuration_of(3)).into()),
                recipient_admission: None,
                send_token: 0,
                recipient_prior_admission: None,
                heartbeat_token: None,
                recovery_epoch_lineage: None,
            },
        )),
    }
}

/// Hands `follower` an ack from `leader`, so it follows `leader` and a drain
/// sends `leader` its `SelfRemove`.
fn follow(cluster: &mut Cluster, follower: &WorkerId, leader: &WorkerId) {
    cluster.step(
        follower,
        Input::Message {
            from: leader.clone(),
            message: heartbeat_ack("shard-1", leader),
        },
    );
}

/// The connection changes the harness handed each node while `act` ran:
/// `(node, peer, opened)`.
fn connection_changes(
    cluster: &mut Cluster,
    act: impl FnOnce(&mut Cluster),
) -> BTreeSet<(WorkerId, WorkerId, bool)> {
    cluster.record_steps();
    act(cluster);
    cluster
        .take_steps()
        .into_iter()
        .filter_map(|record| match record.input {
            Some(Input::PeerConnected(peer)) => Some((record.node, peer, true)),
            Some(Input::PeerDisconnected(peer)) => Some((record.node, peer, false)),
            _ => None,
        })
        .collect()
}

#[test]
fn bootstrap_produces_exactly_n_distinct_active_registered_nodes() {
    let cluster = Cluster::bootstrap(3, Duration::from_ticks(50));

    let ids = cluster.node_ids();
    let expected: BTreeSet<WorkerId> = ["worker-0", "worker-1", "worker-2"]
        .iter()
        .map(|s| worker(s))
        .collect();
    assert_eq!(
        ids, expected,
        "bootstrap(3, ..) must produce exactly these 3 IDs"
    );

    let states = cluster.states();
    assert_eq!(states.len(), 3);
    for (id, state) in &states {
        assert_eq!(
            *state,
            WorkerState::Active,
            "node {id:?} must start Active — Phase 0 has no BOOTSTRAPPING/JOINING modeling"
        );
    }
}

// Tuning: `suspect_timeout` is 10 ticks and `tick_size` 5. The test first
// advances 15 ticks by hand because a freshly bootstrapped cluster is already a
// fixed point of `run_until_quiescent`, which cannot see a timer that hasn't
// fired; crossing the timeout (15 > 10) starts the election.
#[test]
fn full_election_cycle_converges_through_the_harness_alone() {
    let suspect_timeout = Duration::from_ticks(10);
    let tick_size = Duration::from_ticks(5);
    let max_ticks = 60;

    let mut cluster = Cluster::bootstrap(3, suspect_timeout);

    // No leader exists, so no heartbeats arrive: that absence alone must
    // trigger suspicion, roll call, voting and a leader.
    for _ in 0..3 {
        cluster.advance(tick_size);
    }

    let iterations = cluster.run_until_quiescent(tick_size, max_ticks);
    assert!(
        iterations < max_ticks,
        "expected the cluster to reach a fixed point well before max_ticks, ran all {iterations}"
    );

    assert!(
        cluster.leader().is_some(),
        "expected a leader to have been elected after the cluster settled; states: {:?}",
        cluster.states()
    );
    cluster.assert_at_most_one_leader();
}

const SHARD: &str = "shard-1"; // Matches Cluster::bootstrap's own documented shard-1 scheme.

#[test]
fn partition_and_heal_are_correctly_wired_through() {
    let suspect_timeout = Duration::from_ticks(10);
    let tick_size = Duration::from_ticks(5);

    let mut cluster = Cluster::bootstrap(4, suspect_timeout);
    let ids: Vec<WorkerId> = cluster.node_ids().into_iter().collect();
    let group_a: BTreeSet<WorkerId> = ids[..1].iter().cloned().collect(); // 1 node: minority.
    let group_b: BTreeSet<WorkerId> = ids[1..].iter().cloned().collect(); // 3 nodes: majority.
    let minority_id = ids[0].clone();

    cluster.partition(group_a.clone(), group_b.clone());

    // Cross the suspicion boundary by hand (see the tuning note on test 2).
    for _ in 0..3 {
        cluster.advance(tick_size);
    }

    // Each side runs its own election. The minority (1 node, against its own
    // configuration of 4) can never reach quorum 3; the majority can.
    cluster.run_until_quiescent(tick_size, 60);

    // While still partitioned, the majority must have elected its own leader.
    let leader_while_partitioned = cluster
        .leader()
        .expect("the 3-node majority side must elect a leader while still partitioned");
    assert!(
        group_b.contains(&leader_while_partitioned),
        "the elected leader while partitioned must be a majority-side node, got {leader_while_partitioned:?}"
    );

    let minority_state = cluster.states()[&minority_id];
    assert_ne!(
        minority_state,
        WorkerState::Leader,
        "the 1-node minority side can never reach quorum (needs 3 of 4) and so must never elect \
         a leader while partitioned"
    );

    // A synthetic ack sent across the active partition must be dropped: the
    // minority node is stuck in `RollCall`, so delivery would flip it to Active.
    let state_before_send = cluster.states()[&minority_id];
    cluster.network().send(
        leader_while_partitioned.clone(),
        minority_id.clone(),
        heartbeat_ack(SHARD, &leader_while_partitioned),
    );
    cluster.advance(tick_size);
    assert_eq!(
        cluster.states()[&minority_id],
        state_before_send,
        "a message sent across an ACTIVE partition must be dropped, not delivered — the \
         minority node's state must be unaffected"
    );

    // After heal, the identical send must be delivered on the next advance(),
    // flipping the minority from `RollCall` to `Active`.
    cluster.heal();
    cluster.network().send(
        leader_while_partitioned.clone(),
        minority_id.clone(),
        heartbeat_ack(SHARD, &leader_while_partitioned),
    );
    cluster.advance(tick_size);
    assert_eq!(
        cluster.states()[&minority_id],
        WorkerState::Active,
        "a message sent after heal() must be delivered on the next advance() — the previously- \
         isolated minority node must transition RollCall -> Active via on_leader_ack, proving \
         connectivity actually resumed"
    );

    // Let everything settle; the cluster-wide invariant must hold.
    cluster.run_until_quiescent(tick_size, 60);
    cluster.assert_at_most_one_leader();
}

#[test]
fn drain_works_through_the_harness() {
    let mut cluster = Cluster::bootstrap(3, Duration::from_ticks(50));
    let target = worker("worker-0");

    // Every node is Active, which allows `Active -> Draining`.
    assert_eq!(cluster.states()[&target], WorkerState::Active);

    cluster.drain(&target);

    assert_eq!(
        cluster.states()[&target],
        WorkerState::Stopped,
        "a drain from Active synchronously reaches Stopped with no wire round-trip (Draining \
         is momentary and unobservable), per Input::Drain's own doc comment"
    );
}

#[test]
#[should_panic(expected = "is not a known node ID")]
fn drain_panics_on_an_unknown_worker_id() {
    let mut cluster = Cluster::bootstrap(3, Duration::from_ticks(50));
    cluster.drain(&worker("does-not-exist"));
}

#[test]
fn run_until_quiescent_waits_for_delayed_messages() {
    let tick_size = Duration::from_ticks(5);
    // A suspect timeout far beyond this test, so no election interferes.
    let mut cluster = Cluster::bootstrap(3, Duration::from_ticks(1000));
    cluster.network().set_delay(Duration::from_ticks(30));

    let (drainer, its_leader) = (worker("worker-0"), worker("worker-1"));
    follow(&mut cluster, &drainer, &its_leader);
    cluster.drain(&drainer);
    assert_eq!(
        cluster.network().pending().len(),
        2,
        "its heartbeat to the leader it follows, and its SelfRemove"
    );

    let iterations = cluster.run_until_quiescent(tick_size, 60);

    assert_eq!(
        cluster.network().pending().len(),
        0,
        "quiescence must not be reported while a delayed SelfRemove is still in flight"
    );
    assert!(
        iterations > 1 && iterations < 60,
        "ran {iterations} iterations"
    );
}

#[test]
fn run_until_quiescent_settles_a_healed_partition_without_extra_advances() {
    let tick_size = Duration::from_ticks(5);
    let mut cluster = Cluster::bootstrap(3, Duration::from_ticks(10));
    let ids: Vec<WorkerId> = cluster.node_ids().into_iter().collect();
    cluster.partition(
        ids[..2].iter().cloned().collect(),
        ids[2..].iter().cloned().collect(),
    );
    for _ in 0..3 {
        cluster.advance(tick_size);
    }
    cluster.run_until_quiescent(tick_size, 60);
    assert!(matches!(
        cluster.states()[&ids[2]],
        WorkerState::NoQuorum | WorkerState::RollCall
    ));

    cluster.heal();
    cluster.run_until_quiescent(tick_size, 60);

    assert_eq!(
        cluster.states()[&ids[2]],
        WorkerState::Active,
        "the isolated node must have received the leader's heartbeat before quiescence"
    );
}

// A roll call's replies arrive at once, but the call closes only at its
// deadline, many ticks later: nothing is delivered or changes state in
// between, yet the election has not finished.
#[test]
fn run_until_quiescent_waits_for_a_roll_call_or_vote_to_reach_its_deadline() {
    let tick_size = Duration::from_ticks(1);
    let suspect_timeout = Duration::from_ticks(40);
    let mut cluster = Cluster::bootstrap(3, suspect_timeout);
    assert!(
        timings(suspect_timeout).roll_call_deadline > tick_size,
        "setup invariant: the deadline must lie beyond the next tick"
    );
    let is_electing = |cluster: &Cluster| {
        cluster
            .states()
            .values()
            .any(|state| matches!(state, WorkerState::RollCall | WorkerState::Candidate))
    };
    while !is_electing(&cluster) {
        cluster.advance(tick_size);
    }

    cluster.run_until_quiescent(tick_size, 100);

    assert!(
        !is_electing(&cluster),
        "quiescence must not be reported mid-election: {:?}",
        cluster.states()
    );
    assert!(cluster.leader().is_some(), "{:?}", cluster.states());
}

#[test]
fn run_until_quiescent_terminates_quickly_once_converged() {
    let suspect_timeout = Duration::from_ticks(10);
    let tick_size = Duration::from_ticks(5);
    let max_ticks = 60;

    let mut cluster = Cluster::bootstrap(3, suspect_timeout);

    // Cross the suspicion boundary by hand: otherwise the first call would converge after 1 iteration on an all-Active cluster.
    for _ in 0..3 {
        cluster.advance(tick_size);
    }

    let first_run = cluster.run_until_quiescent(tick_size, max_ticks);
    assert!(
        first_run < max_ticks,
        "expected the initial election to settle well before max_ticks, ran all {first_run}"
    );
    assert!(
        cluster.leader().is_some(),
        "expected a leader to have been elected before checking re-quiescence; states: {:?}",
        cluster.states()
    );

    // The cluster is settled (its only traffic is heartbeats and the acks
    // answering them), so a second call must find the fixed point at once.
    let second_run = cluster.run_until_quiescent(tick_size, max_ticks);
    assert_eq!(
        second_run, 1,
        "an already-settled cluster must be detected as quiescent on the very first check"
    );
}

#[test]
fn partition_and_heal_tell_each_node_which_connections_closed_and_reopened() {
    let mut cluster = Cluster::bootstrap(3, Duration::from_ticks(50));
    let ids: Vec<WorkerId> = cluster.node_ids().into_iter().collect();
    let (first, second) = (ids[0].clone(), ids[1].clone());

    let cut = connection_changes(&mut cluster, |cluster| {
        cluster.partition(
            BTreeSet::from([first.clone()]),
            BTreeSet::from([second.clone()]),
        );
    });
    assert_eq!(
        cut,
        BTreeSet::from([
            (first.clone(), second.clone(), false),
            (second.clone(), first.clone(), false),
        ])
    );

    let healed = connection_changes(&mut cluster, Cluster::heal);
    assert_eq!(
        healed,
        BTreeSet::from([(first.clone(), second.clone(), true), (second, first, true)])
    );
}

#[test]
fn bootstrap_with_pending_adds_pending_members_holding_the_voters_configuration() {
    let cluster = Cluster::bootstrap_with_pending(3, 2, Duration::from_ticks(10));

    for id in ["worker-0", "worker-1", "worker-2"].map(worker) {
        assert_eq!(cluster.node(&id).admission(), Some(g0()), "{id:?}");
    }
    for id in ["worker-3", "worker-4"].map(worker) {
        let node = cluster.node(&id);
        assert_eq!(node.state(), WorkerState::Active, "{id:?}");
        assert_eq!(node.admission(), None, "{id:?} is pending");
        assert_eq!(node.configuration(), Some(&configuration_of(3)), "{id:?}");
    }
}

#[test]
fn an_election_admits_the_pending_members_that_answer_it() {
    let mut cluster = Cluster::bootstrap_with_pending(3, 1, Duration::from_ticks(10));
    let joiner = worker("worker-3");

    cluster.advance(Duration::from_ticks(60));

    let leader = cluster.leader().expect("the voters elect a leader");
    assert_ne!(leader, joiner, "a pending member is no returning voter");
    let admission = cluster.node(&joiner).admission();
    assert!(
        admission.is_some_and(|admission| cluster
            .node(&leader)
            .configuration()
            .is_some_and(|configuration| configuration.is_voter(Some(admission)))),
        "the joiner answered the roll call and is a voter now: {admission:?}"
    );
}

/// Messages still in flight to a restarted node's old `WorkerId` are
/// dropped when they come due, and the quiescence check ignores them rather
/// than trip over the id being gone: here a leader's ack answering the old
/// incarnation's last heartbeat.
#[test]
fn run_until_quiescent_settles_with_an_ack_in_flight_to_a_restarted_node() {
    let tick_size = Duration::from_ticks(5);
    // A suspicion timeout long enough that the leader keeps its lease over
    // the delay below.
    let (mut cluster, leader) =
        bootstrap_5_and_elect_leader(Duration::from_ticks(40), Duration::from_ticks(20));
    let follower = cluster
        .node_ids()
        .into_iter()
        .find(|id| *id != leader)
        .expect("a 5-node cluster has followers");
    // Longer than one `tick_size`, so the ack is still in flight at the
    // quiescence check after the first advance.
    cluster.network().set_delay(Duration::from_ticks(8));
    let ack_in_flight_to = |cluster: &Cluster, to: &WorkerId| {
        cluster
            .network()
            .pending()
            .iter()
            .any(|(pending_to, message)| {
                pending_to == to
                    && matches!(
                        message.payload,
                        Some(election_message::Payload::HeartbeatAck(_))
                    )
            })
    };
    for _ in 0..20 {
        if ack_in_flight_to(&cluster, &follower) {
            break;
        }
        cluster.advance(Duration::from_ticks(1));
    }
    assert!(ack_in_flight_to(&cluster, &follower), "setup invariant");

    cluster.restart_node(&follower);
    cluster.network().set_delay(Duration::from_ticks(0));
    let iterations = cluster.run_until_quiescent(tick_size, 60);

    assert!(iterations < 60, "ran {iterations} iterations");
    // The old incarnation's last heartbeats may still draw acks to its id;
    // once every message it sent has come due, the last of them is dropped.
    cluster.advance(Duration::from_ticks(16));
    assert!(
        !ack_in_flight_to(&cluster, &follower),
        "acks to the old id are dropped when they come due"
    );
}

/// The restarted process keeps its host's place in the network: here, the
/// side of a partition its old incarnation was on.
#[test]
fn a_restarted_node_is_connected_to_the_peers_it_can_reach() {
    let mut cluster = Cluster::bootstrap(3, Duration::from_ticks(50));
    let ids: Vec<WorkerId> = cluster.node_ids().into_iter().collect();
    cluster.partition(
        [ids[0].clone()].into_iter().collect(),
        [ids[1].clone()].into_iter().collect(),
    );

    let mut restarted = None;
    let changes = connection_changes(&mut cluster, |cluster| {
        restarted = Some(cluster.restart_node(&ids[0]));
    });
    let restarted = restarted.expect("restart_node names the new node");

    let opened_to_it: BTreeSet<WorkerId> = changes
        .into_iter()
        .filter(|(node, _, opened)| *node == restarted && *opened)
        .map(|(_, peer, _)| peer)
        .collect();
    assert_eq!(
        opened_to_it,
        BTreeSet::from([ids[2].clone()]),
        "only ids[2] is reachable from the restarted node"
    );
}

#[test]
fn a_node_no_connection_event_reaches_still_runs_on_its_own_timers() {
    let mut cluster = Cluster::bootstrap(1, Duration::from_ticks(10));
    let solo = worker("worker-0");

    // A one-member cluster reports no connection to its node, so only the
    // harness's own first step can have learnt the node's deadline.
    cluster.advance(past_any_suspicion(10));

    assert_eq!(
        cluster.states()[&solo],
        WorkerState::Leader,
        "a lone node elects itself once its suspicion timer runs out"
    );
}

#[test]
fn a_publish_reaches_every_other_node_as_a_message_from_its_publisher() {
    let mut cluster = Cluster::bootstrap(3, Duration::from_ticks(10));
    let publisher = worker("worker-0");
    suspect_leader_by_hand(&mut cluster, std::slice::from_ref(&publisher));

    // Its roll call is published. A node answers a roll call only from the
    // initiator it names, so the publisher stands at the call's deadline
    // only if the others' replies reached it, and wins once they grant it
    // their votes.
    cluster.step(&publisher, Input::Tick);
    cluster.deliver_messages();
    cluster.advance_clock_only(timings(Duration::from_ticks(10)).roll_call_deadline);
    cluster.step(&publisher, Input::Tick);
    cluster.deliver_messages();

    assert_eq!(cluster.states()[&publisher], WorkerState::Leader);
    let known_leaders: BTreeMap<WorkerId, Option<(WorkerId, u64)>> = cluster
        .node_ids()
        .into_iter()
        .map(|id| {
            let leader = cluster.node(&id).known_leader();
            (id, leader)
        })
        .collect();
    assert_eq!(
        known_leaders,
        BTreeMap::from([
            (publisher.clone(), Some((publisher.clone(), 1))),
            (worker("worker-1"), Some((publisher.clone(), 1))),
            (worker("worker-2"), Some((publisher, 1))),
        ])
    );
}

#[test]
fn each_nodes_scheduler_leads_exactly_while_its_node_holds_its_grant() {
    let suspect_timeout = Duration::from_ticks(10);
    let tick_size = Duration::from_ticks(5);
    let (mut cluster, leader) = bootstrap_5_and_elect_leader(suspect_timeout, tick_size);

    assert_eq!(
        cluster.valid_grant_holders(),
        BTreeSet::from([leader.clone()]),
        "once a quorum confirmed its acks, only the leader's scheduler leads"
    );

    let others: BTreeSet<WorkerId> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| *id != leader)
        .collect();
    cluster.partition(BTreeSet::from([leader.clone()]), others);
    run_out_cut_off_leaders_lease(&mut cluster);

    assert_eq!(
        cluster.states()[&leader],
        WorkerState::NoQuorum,
        "setup invariant"
    );
    assert!(!cluster.holds_valid_grant(&leader));
    assert_eq!(cluster.first_grant_overlap(), None);
}

// The leader's driver stalls, so its node is never ticked at its lease end
// and stays `Leader`; its scheduler, reading the clock itself, stops leading
// there all the same, before the others can elect a new leader.
#[test]
fn a_stalled_leaders_scheduler_stops_leading_at_its_lease_end() {
    let suspect_timeout = Duration::from_ticks(10);
    let tick_size = Duration::from_ticks(5);
    let (mut cluster, leader) = bootstrap_5_and_elect_leader(suspect_timeout, tick_size);

    assert!(cluster.holds_valid_grant(&leader), "setup invariant");
    cluster.stall(&leader, Duration::from_ticks(40));
    // Past any lease the leader held when it stalled, yet short of any
    // follower's suspicion timeout.
    cluster.advance(Duration::from_ticks(9));

    assert_eq!(cluster.states()[&leader], WorkerState::Leader);
    assert!(
        !cluster.holds_valid_grant(&leader),
        "the stalled leader's grant must lapse at its lease end"
    );
    assert!(cluster.valid_grant_holders().is_empty());

    cluster.advance(Duration::from_ticks(31));
    cluster.run_until_quiescent(tick_size, 60);

    let new_leader = cluster.leader().expect("the four must elect a leader");
    assert_ne!(new_leader, leader);
    assert_eq!(
        cluster.states()[&leader],
        WorkerState::Active,
        "once its stall ends, the old leader handles what was held and follows the new one"
    );
    assert_eq!(cluster.first_grant_overlap(), None);
}

#[test]
fn a_stalled_node_is_handed_what_reached_it_once_its_stall_ends() {
    let mut cluster = Cluster::bootstrap(3, Duration::from_ticks(1000));
    let (drainer, stalled) = (worker("worker-0"), worker("worker-1"));
    follow(&mut cluster, &drainer, &stalled);
    cluster.stall(&stalled, Duration::from_ticks(10));
    cluster.record_steps();

    cluster.drain(&drainer);
    cluster.advance(Duration::from_ticks(9));
    let before_the_end: Vec<WorkerId> = cluster
        .take_steps()
        .into_iter()
        .map(|record| record.node)
        .collect();
    cluster.advance(Duration::from_ticks(1));
    let at_the_end: Vec<WorkerId> = cluster
        .take_steps()
        .into_iter()
        .map(|record| record.node)
        .collect();

    assert!(
        !before_the_end.contains(&stalled),
        "a stalled node takes no step: {before_the_end:?}"
    );
    assert!(
        at_the_end.contains(&stalled),
        "the held SelfRemove reaches it once the stall ends: {at_the_end:?}"
    );
}

#[test]
fn a_drain_asked_of_a_stalled_node_waits_for_its_stall_to_end() {
    let mut cluster = Cluster::bootstrap(3, Duration::from_ticks(1000));
    let stalled = worker("worker-0");
    cluster.stall(&stalled, Duration::from_ticks(10));

    cluster.drain(&stalled);
    assert_eq!(cluster.states()[&stalled], WorkerState::Active);
    cluster.advance(Duration::from_ticks(10));

    assert_eq!(cluster.states()[&stalled], WorkerState::Stopped);
}

#[test]
fn an_advance_after_the_clock_alone_passed_a_stalls_end_hands_over_what_was_held() {
    let mut cluster = Cluster::bootstrap(3, Duration::from_ticks(1000));
    let (drainer, stalled) = (worker("worker-0"), worker("worker-1"));
    follow(&mut cluster, &drainer, &stalled);
    cluster.deliver_messages();
    cluster.stall(&stalled, Duration::from_ticks(5));
    cluster.drain(&drainer);
    cluster.deliver_messages();
    cluster.advance_clock_only(Duration::from_ticks(10));
    cluster.record_steps();

    cluster.advance(Duration::from_ticks(1));

    let stepped: Vec<WorkerId> = cluster
        .take_steps()
        .into_iter()
        .map(|record| record.node)
        .collect();
    assert!(
        stepped.contains(&stalled),
        "the SelfRemove held for it reaches it: {stepped:?}"
    );
}

#[test]
fn a_connection_change_after_a_stall_ended_unseen_first_hands_over_what_was_held() {
    let mut cluster = Cluster::bootstrap(3, Duration::from_ticks(1000));
    let stalled = worker("worker-2");
    let others = BTreeSet::from([worker("worker-0"), worker("worker-1")]);
    cluster.stall(&stalled, Duration::from_ticks(5));
    // Two disconnections and two reconnections are held for it.
    cluster.partition(others.clone(), BTreeSet::from([stalled.clone()]));
    cluster.heal();
    cluster.advance_clock_only(Duration::from_ticks(10));
    cluster.record_steps();

    cluster.partition(others, BTreeSet::from([stalled.clone()]));

    let steps_taken = cluster
        .take_steps()
        .into_iter()
        .filter(|record| record.node == stalled)
        .count();
    assert_eq!(
        steps_taken, 6,
        "the four held changes, then the two new disconnections"
    );
}
