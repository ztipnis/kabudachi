//! A new leader whose republished record cannot reach a quorum keeps its
//! office and keeps writing: the refused write again after a delay, and placed
//! anew on the voters whenever they change. It leads once the record is
//! stored.

use std::collections::BTreeSet;

use kabudachi_core::protocol::ids::{TaskId, WorkerId};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::Duration;
use kabudachi_testkit::RecordSpace;

use super::scenario_records::{STEP, SUSPECT, submitted};
use crate::support::builders::worker;
use crate::support::harness::Cluster;

const VOTERS: usize = 5;
const REPLICATION_FACTOR: usize = 3;

fn voter(index: usize) -> WorkerId {
    worker(&format!("worker-{index}"))
}

fn holders(task: &TaskId, voters: &[WorkerId]) -> Vec<WorkerId> {
    RecordSpace::placement(task, voters, REPLICATION_FACTOR).0
}

fn advance_for(cluster: &mut Cluster, span: Duration) {
    for _ in 0..(span.as_ticks() / STEP.as_ticks()) {
        cluster.advance(STEP);
    }
}

fn advance_until(cluster: &mut Cluster, reached: impl Fn(&Cluster) -> bool) {
    for _ in 0..(SUSPECT.as_ticks() * 20 / STEP.as_ticks()) {
        if reached(cluster) {
            return;
        }
        cluster.advance(STEP);
    }
    assert!(reached(cluster), "the cluster never got there");
}

fn held_term(cluster: &Cluster, holder: &WorkerId, task: &TaskId) -> Option<u64> {
    cluster
        .records()
        .held_by(holder, task)
        .and_then(|record| record.version)
        .map(|version| version.leader_term)
}

/// A leader that took office with a record to republish, whose holders
/// answered its reconciliation and then went away (all but, if a voter is to
/// leave, the one that the replacement holder will make a quorum with).
struct Stuck {
    cluster: Cluster,
    /// The leader that is reconciling.
    leader: WorkerId,
    /// The record the leader republishes.
    task: TaskId,
    /// Its holders.
    held: Vec<WorkerId>,
    /// The holder that is to leave the voters, if one is to.
    leaving: Option<WorkerId>,
    /// The holder that is to return as the placement changes.
    returning: Option<WorkerId>,
    /// The voter the record is placed on once `leaving` has left.
    replacement: Option<WorkerId>,
    /// A holder that stays up and stores the first write, and holds the
    /// record when the placement changes.
    staying: Option<WorkerId>,
}

impl Stuck {
    fn is_stored_by(&self, holder: &WorkerId) -> bool {
        let term = self.cluster.node(&self.leader).office_term().expect("it holds office").term;
        held_term(&self.cluster, holder, &self.task) == Some(term)
    }
}

/// Elects a leader among five voters, which stores a record of one task and
/// is cut off. Another leader is elected and starts to reconcile. The holders
/// of the record answer it, and go down before it writes the record again.
/// If `shrinking`, one of them is to leave the voters, which places the
/// record on a voter that is up in its stead; another holder is to return, so
/// that the new placement has a quorum and the old one has not.
///
/// If `staying`, no holder returns: another holder stays up throughout
/// instead, so it stores the first write, under the placement that has
/// changed by the time the record is written again.
fn stuck_republish(shrinking: bool, staying: bool) -> Stuck {
    let voters: Vec<WorkerId> = (0..VOTERS).map(voter).collect();
    let mut cluster = Cluster::bootstrap(VOTERS, SUSPECT);
    advance_for(&mut cluster, Duration::from_ticks(SUSPECT.as_ticks() * 2));
    cluster.run_until_quiescent(STEP, 200);
    let deposed = cluster.leader().expect("the voters elect a leader");
    let task = submitted(&mut cluster, &deposed);

    // No voter answers the next leader's questions until it is elected, so it
    // waits for a quorum of them.
    for voter in &voters {
        cluster.records().set_up(voter, false);
    }
    let rest: BTreeSet<WorkerId> = voters.iter().filter(|id| **id != deposed).cloned().collect();
    cluster.partition(BTreeSet::from([deposed.clone()]), rest);
    advance_until(&mut cluster, |cluster| {
        cluster.states().values().any(|state| *state == WorkerState::LeaderReconciling)
    });
    let leader = cluster
        .states()
        .into_iter()
        .find(|(_, state)| *state == WorkerState::LeaderReconciling)
        .map(|(id, _)| id)
        .expect("a node reconciles");

    // The leader places the record on the voters it knows, which no longer
    // include the leader that was cut off.
    let known = cluster.node(&leader).voters();
    let held = holders(&task, &known);

    // Of the holders, one leaves the voters, and the record is then placed on
    // a voter in its stead; with the holder that returns it makes the quorum
    // of the new placement, which the old placement lacks.
    let leaving = shrinking.then(|| held.iter().find(|id| **id != leader).cloned().expect("a holder"));
    let returning = leaving
        .as_ref()
        .filter(|_| !staying)
        .map(|leaving| held.iter().find(|id| *id != leaving).cloned().expect("another holder"));
    let staying = leaving.as_ref().filter(|_| staying).map(|leaving| {
        held.iter()
            .find(|id| *id != leaving && **id != leader)
            .cloned()
            .expect("another holder")
    });
    let replacement = leaving.as_ref().map(|leaving| {
        let left: Vec<WorkerId> = known.iter().filter(|id| *id != leaving).cloned().collect();
        holders(&task, &left)
            .into_iter()
            .find(|id| !held.contains(id))
            .expect("the record is placed on another voter")
    });
    let stays_up: Vec<WorkerId> = replacement.iter().chain(&staying).cloned().collect();

    // Every voter but one answers, which is a quorum but not all of them (the
    // cut off leader is not counted), so the leader waits out its grace; and
    // each goes down before the grace ends and the leader writes.
    let silent = voters
        .iter()
        .find(|id| {
            **id != leader
                && **id != deposed
                && !stays_up.contains(id)
                && Some(*id) != leaving.as_ref()
                && Some(*id) != returning.as_ref()
                && Some(*id) != staying.as_ref()
        })
        .expect("a voter stays silent")
        .clone();
    for voter in voters.iter().filter(|id| **id != silent) {
        cluster.records().set_up(voter, true);
    }
    advance_for(&mut cluster, Duration::from_ticks(SUSPECT.as_ticks() / 2));
    for voter in voters.iter().filter(|id| !stays_up.contains(id)) {
        cluster.records().set_up(voter, false);
    }
    advance_for(&mut cluster, SUSPECT);
    Stuck { cluster, leader, task, held, leaving, returning, replacement, staying }
}

#[test]
fn a_refused_republish_is_written_again_after_the_delay_until_its_holders_store_it() {
    let mut stuck = stuck_republish(false, false);
    advance_for(&mut stuck.cluster, Duration::from_ticks(SUSPECT.as_ticks() * 3));
    assert_eq!(
        stuck.cluster.states()[&stuck.leader],
        WorkerState::LeaderReconciling,
        "it keeps office, with no quorum for its write"
    );
    assert!(stuck.held.iter().all(|holder| !stuck.is_stored_by(holder)));

    // A quorum of the holders returns: the next write of the record reaches
    // them, within one delay.
    let returned = stuck.cluster.now();
    for holder in &stuck.held[..2] {
        stuck.cluster.records().set_up(holder, true);
    }
    let leader = stuck.leader.clone();
    advance_until(&mut stuck.cluster, |cluster| cluster.states()[&leader] == WorkerState::Leader);

    assert!((stuck.cluster.now() - returned).as_ticks() <= SUSPECT.as_ticks() / 4 + STEP.as_ticks());
    assert!(stuck.held[..2].iter().all(|holder| stuck.is_stored_by(holder)));
}

#[test]
fn writes_waiting_for_a_quorum_are_placed_again_on_the_voters_left_when_a_holder_leaves() {
    let mut stuck = stuck_republish(true, false);
    let replacement = stuck.replacement.clone().expect("a replacement");
    advance_for(&mut stuck.cluster, Duration::from_ticks(SUSPECT.as_ticks() * 3));
    assert_eq!(stuck.cluster.states()[&stuck.leader], WorkerState::LeaderReconciling);
    assert!(!stuck.is_stored_by(&replacement));

    // A holder leaves the voters, and the record is placed on the voters left.
    // Only then does another holder return: the old placement's holders could
    // not make a quorum of it, the new placement's can.
    let known = stuck.cluster.node(&stuck.leader).voters().len();
    stuck.cluster.drain(stuck.leaving.as_ref().expect("a holder leaves"));
    let leader = stuck.leader.clone();
    advance_until(&mut stuck.cluster, |cluster| cluster.node(&leader).voters().len() < known);
    assert_eq!(stuck.cluster.states()[&leader], WorkerState::LeaderReconciling, "still no quorum");
    stuck.cluster.records().set_up(stuck.returning.as_ref().expect("a holder returns"), true);
    advance_until(&mut stuck.cluster, |cluster| cluster.states()[&leader] == WorkerState::Leader);

    assert!(stuck.is_stored_by(&replacement));
    assert!(!stuck.is_stored_by(stuck.leaving.as_ref().expect("a holder leaves")), "the holder that left never stored it");
}

#[test]
fn a_holder_that_stored_the_record_under_the_old_placement_counts_towards_the_quorum_of_the_new_one() {
    let mut stuck = stuck_republish(true, true);
    let (replacement, staying) = (
        stuck.replacement.clone().expect("a replacement"),
        stuck.staying.clone().expect("a holder stays"),
    );
    advance_for(&mut stuck.cluster, Duration::from_ticks(SUSPECT.as_ticks() * 3));
    assert_eq!(stuck.cluster.states()[&stuck.leader], WorkerState::LeaderReconciling);
    assert!(stuck.is_stored_by(&staying), "it stored the first write, short of a quorum");
    assert!(!stuck.is_stored_by(&replacement));

    // A holder leaves the voters and the record is placed on the voters left:
    // the holder that stayed and the replacement make its quorum.
    let known = stuck.cluster.node(&stuck.leader).voters().len();
    stuck.cluster.drain(stuck.leaving.as_ref().expect("a holder leaves"));
    let leader = stuck.leader.clone();
    advance_until(&mut stuck.cluster, |cluster| cluster.node(&leader).voters().len() < known);
    advance_until(&mut stuck.cluster, |cluster| cluster.states()[&leader] == WorkerState::Leader);

    assert!(stuck.is_stored_by(&replacement));
}
