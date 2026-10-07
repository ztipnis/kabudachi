//! In the simulator: a drained worker's records outlive it even when no
//! leader ever repairs them, and a leader places records again whenever the
//! voters it can place them on change.

use std::collections::BTreeSet;

use kabudachi_core::protocol::generated::TaskRecord;
use kabudachi_core::protocol::ids::{TaskId, WorkerId};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::task_record::{VersionOrder, identify};
use kabudachi_core::time::Duration;

use super::scenario_records::{STEP, SUSPECT, elected_among, plain, submitted};
use crate::support::harness::{Answer, Cluster};

fn advance_until(cluster: &mut Cluster, reached: impl Fn(&Cluster) -> bool) {
    for _ in 0..(SUSPECT.as_ticks() * 60 / STEP.as_ticks()) {
        if reached(cluster) {
            return;
        }
        cluster.advance(STEP);
    }
    assert!(reached(cluster), "the cluster never got there");
}

/// A node that is not one of `excluded`.
fn some_other(cluster: &Cluster, excluded: &[&WorkerId]) -> WorkerId {
    cluster
        .node_ids()
        .into_iter()
        .find(|id| !excluded.contains(&id))
        .expect("the cluster has a node left over")
}

/// Submits to `leader` until `holder` holds `count` records, and returns
/// their tasks.
fn submitted_until_held_by(
    cluster: &mut Cluster,
    leader: &WorkerId,
    holder: &WorkerId,
    count: usize,
) -> Vec<TaskId> {
    let mut held = Vec::new();
    for _ in 0..200 {
        let task = submitted(cluster, leader);
        if cluster.records().held_by(holder, &task).is_some() {
            held.push(task);
            if held.len() == count {
                return held;
            }
        }
    }
    panic!("{holder:?} never held {count} records");
}

/// The leader elected once `deposed` is cut off from the rest.
fn leader_loss(cluster: &mut Cluster, deposed: &WorkerId) -> WorkerId {
    let rest = cluster.node_ids().into_iter().filter(|id| id != deposed).collect();
    cluster.partition(BTreeSet::from([deposed.clone()]), rest);
    advance_until(cluster, |cluster| {
        cluster.states().iter().any(|(id, state)| {
            id != deposed && *state == WorkerState::Leader
        })
    });
    cluster
        .states()
        .into_iter()
        .find(|(id, state)| {
            id != deposed && *state == WorkerState::Leader
        })
        .map(|(id, _)| id)
        .expect("the others elected a leader")
}

/// The only holder of a record drains while its leader's writes never land,
/// so nothing but the drainer's own hand-off can keep the record: the next
/// leader, elected after the old one is lost, still knows every task.
#[test]
fn records_handed_off_by_their_only_holder_survive_a_leader_that_never_repaired_them() {
    let (mut cluster, leader) = elected_among(5);
    cluster.set_replication_factor(1);
    let drainer = some_other(&cluster, &[&leader]);
    let tasks = submitted_until_held_by(&mut cluster, &leader, &drainer, 2);
    cluster.records().hold_writes_from(&leader);

    cluster.drain(&drainer);
    advance_until(&mut cluster, |cluster| cluster.is_down(&drainer));
    let next = leader_loss(&mut cluster, &leader);

    let claimant = some_other(&cluster, &[&leader, &drainer, &next]);
    for task in tasks {
        let ticket = cluster.claim(&next, &claimant, &task);
        cluster.advance(STEP);
        assert!(
            matches!(cluster.answer(ticket), Some(Answer::Claimed(_))),
            "{task:?}: {:?}",
            cluster.answer(ticket)
        );
    }
}

/// How long a worker may stay silent before its leader reports it lost, in
/// the scenarios that wait for that.
const RECONNECT: Duration = Duration::from_millis(2_000);

/// The newest revision of `task` any node holds, as its holders see it.
fn newest_record(cluster: &Cluster, task: &TaskId) -> TaskRecord {
    cluster
        .node_ids()
        .iter()
        .filter_map(|holder| cluster.records().held_by(holder, task))
        .reduce(|newest, record| {
            let (_, held) = identify(&newest).expect("a held record is identified");
            let (_, other) = identify(&record).expect("a held record is identified");
            if held.order(&other) == VersionOrder::Newer { record } else { newest }
        })
        .expect("some node holds the task")
}

fn placement_of(record: &TaskRecord) -> Vec<WorkerId> {
    record.placement.iter().cloned().map(WorkerId::from).collect()
}

/// A voter that dies is never removed while the others keep a quorum, but its
/// leader reports it lost: the records it held are written again, to a full
/// placement among the voters the leader hears, and the dead voter stays in
/// the configuration all the while.
#[test]
fn a_dead_holders_records_are_written_to_a_full_placement_without_it() {
    let mut cluster = Cluster::bootstrap_with_reconnect_timeout(5, 0, SUSPECT, RECONNECT);
    for _ in 0..(SUSPECT.as_ticks() * 2 / STEP.as_ticks()) {
        cluster.advance(STEP);
    }
    cluster.run_until_quiescent(STEP, 200);
    let leader = cluster.leader().expect("the voters elect a leader");
    let victim = some_other(&cluster, &[&leader]);
    let tasks = submitted_until_held_by(&mut cluster, &leader, &victim, 2);

    cluster.set_down(&victim);
    for _ in 0..((SUSPECT.as_ticks() * 5 + RECONNECT.as_ticks()) / STEP.as_ticks()) {
        cluster.advance(STEP);
    }

    assert!(cluster.node(&leader).voters().contains(&victim), "nothing removed it");
    for task in tasks {
        let placement = placement_of(&newest_record(&cluster, &task));
        assert!(!placement.contains(&victim), "{task:?} is still placed on the dead holder");
        assert_eq!(placement.len(), 3, "{task:?} has a full placement");
        let holding = placement
            .iter()
            .filter(|holder| cluster.records().held_by(holder, &task).is_some())
            .count();
        assert!(holding >= 2, "{task:?} is held by {holding} of its placement");
    }
}

/// Voters admitted to a shard take their share of the records, and the
/// holders they displace drop their copies: once the admission has settled,
/// each record is held by its placement and by no one else.
#[test]
fn records_move_to_the_voters_admitted_and_leave_the_holders_they_displace() {
    let voters: BTreeSet<WorkerId> = (0..3).map(|n| WorkerId::new(format!("worker-{n}"))).collect();
    let joiners: BTreeSet<WorkerId> = (3..5).map(|n| WorkerId::new(format!("worker-{n}"))).collect();
    let mut cluster = Cluster::bootstrap_with_pending(3, 2, SUSPECT);
    cluster.partition(voters, joiners.clone());
    advance_until(&mut cluster, |cluster| {
        cluster.leader().is_some_and(|leader| {
            cluster.node(&leader).configuration().is_some_and(|configuration| !configuration.is_joint())
        })
    });
    let leader = cluster.leader().expect("the voters elected a leader");
    let tasks: Vec<TaskId> = (0..12).map(|_| submitted(&mut cluster, &leader)).collect();

    cluster.heal();
    advance_until(&mut cluster, |cluster| {
        let node = cluster.node(&leader);
        node.configuration().is_some_and(|configuration| {
            !configuration.is_joint() && configuration.voter_count() == Some(5)
        })
    });
    for _ in 0..(SUSPECT.as_ticks() * 4 / STEP.as_ticks()) {
        cluster.advance(STEP);
    }

    let mut moved = 0;
    for task in tasks {
        let placement = placement_of(&newest_record(&cluster, &task));
        let holders: BTreeSet<WorkerId> = cluster
            .node_ids()
            .into_iter()
            .filter(|node| cluster.records().held_by(node, &task).is_some())
            .collect();
        assert_eq!(holders, placement.iter().cloned().collect::<BTreeSet<_>>(), "{task:?}");
        moved += usize::from(placement.iter().any(|holder| joiners.contains(holder)));
    }
    assert!(moved > 0, "no record was placed on an admitted voter");
}

/// A revision whose write found no quorum may or may not have landed, so the
/// leader answers `NotLeader` about the task until a newer revision of it is
/// stored: it publishes the record again by itself once the holders answer.
#[test]
fn a_revision_whose_write_was_refused_is_published_again_until_it_is_stored() {
    let (mut cluster, leader) = elected_among(5);
    let others: Vec<WorkerId> = cluster.node_ids().into_iter().filter(|id| *id != leader).collect();
    for other in &others {
        cluster.records().set_up(other, false);
    }
    let ticket = cluster.submit(&leader, plain());
    cluster.advance(STEP);
    assert_eq!(cluster.answer(ticket), Some(&Answer::NotLeader), "setup invariant");
    let task = TaskId::new("task-1");
    assert!(cluster.scheduler_mut(&leader).holds(&task), "setup invariant");
    for other in &others {
        cluster.records().set_up(other, true);
    }

    for _ in 0..(SUSPECT.as_ticks() / STEP.as_ticks()) {
        cluster.advance(STEP);
    }

    let placement = placement_of(&newest_record(&cluster, &task));
    let holding = placement
        .iter()
        .filter(|holder| cluster.records().held_by(holder, &task).is_some())
        .count();
    assert!(holding >= 2, "the record is held by {holding} of its placement");
}
