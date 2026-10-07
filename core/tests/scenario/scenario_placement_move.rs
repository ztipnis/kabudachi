//! In the simulator: a record that moves to other holders keeps its released
//! writes visible to the next leader, however the old and the new holders are
//! cut apart.

use std::collections::BTreeSet;

use kabudachi_core::protocol::ids::{TaskId, WorkerId};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::Duration;
use kabudachi_testkit::RecordSpace;

use super::scenario_records::{STEP, SUSPECT, submitted};
use crate::support::builders::worker;
use crate::support::harness::{Answer, Cluster};

const FACTOR: usize = 3;

/// How long a silent voter stays in the configuration: longer than any
/// scenario here, so a voter that is lost is still one whose holders count.
const LOST_AFTER: Duration = Duration::from_millis(600_000);

fn advance_until(cluster: &mut Cluster, reached: impl Fn(&Cluster) -> bool) {
    for _ in 0..(SUSPECT.as_ticks() * 40 / STEP.as_ticks()) {
        if reached(cluster) {
            return;
        }
        cluster.advance(STEP);
    }
    assert!(reached(cluster), "the cluster never got there: {:?}", cluster.states());
}

/// Who a task is written to, with three voters and then with all five.
struct Move {
    task: TaskId,
    /// The voter in both placements besides the writer, then the voter left
    /// out of the second, the joiner in it, and the joiner left out of it.
    kept: WorkerId,
    dropped: WorkerId,
    gained: WorkerId,
    unplaced: WorkerId,
}

/// Submits to `leader` until a task is placed on the three voters and, once
/// the joiners are admitted, on `leader`, one other voter and one joiner.
fn submitted_to_move(cluster: &mut Cluster, leader: &WorkerId) -> Move {
    let voters: Vec<WorkerId> = (0..3).map(|n| worker(&format!("worker-{n}"))).collect();
    let all: Vec<WorkerId> = (0..5).map(|n| worker(&format!("worker-{n}"))).collect();
    for _ in 0..200 {
        let task = submitted(cluster, leader);
        let after = RecordSpace::placement(&task, &all, FACTOR).0;
        let kept: Vec<&WorkerId> = after.iter().filter(|id| voters.contains(id) && *id != leader).collect();
        let gained: Vec<&WorkerId> = after.iter().filter(|id| !voters.contains(id)).collect();
        if after.contains(leader) && kept.len() == 1 && gained.len() == 1 {
            let dropped = voters.iter().find(|id| !after.contains(id)).expect("a voter is left out");
            let unplaced = all.iter().find(|id| !voters.contains(id) && !after.contains(id));
            return Move {
                task,
                kept: kept[0].clone(),
                dropped: dropped.clone(),
                gained: gained[0].clone(),
                unplaced: unplaced.expect("a joiner is left out").clone(),
            };
        }
    }
    panic!("no submitted task moves from the voters onto one joiner");
}

/// Three voters elect a leader and write a task; two joiners are admitted
/// while `down` of the old holders (the voters of the task's placement
/// besides the leader) cannot be reached, which moves the record. Returns the
/// cluster, the old leader and the task's move.
fn moved_while_down(down: &dyn Fn(&Move) -> Vec<WorkerId>) -> (Cluster, WorkerId, Move) {
    let voters: BTreeSet<WorkerId> = (0..3).map(|n| worker(&format!("worker-{n}"))).collect();
    let joiners: BTreeSet<WorkerId> = (3..5).map(|n| worker(&format!("worker-{n}"))).collect();
    let mut cluster = Cluster::bootstrap_with_reconnect_timeout(3, 2, SUSPECT, LOST_AFTER);
    cluster.partition(voters, joiners);
    advance_until(&mut cluster, |cluster| {
        cluster.leader().is_some_and(|leader| {
            cluster.node(&leader).configuration().is_some_and(|configuration| !configuration.is_joint())
        })
    });
    let old = cluster.leader().expect("the voters elected a leader");
    let moving = submitted_to_move(&mut cluster, &old);
    for holder in down(&moving) {
        cluster.records().set_up(&holder, false);
    }
    cluster.heal();
    advance_until(&mut cluster, |cluster| {
        cluster.node(&old).configuration().is_some_and(|configuration| {
            !configuration.is_joint() && configuration.voter_count() == Some(5)
        })
    });
    for _ in 0..(SUSPECT.as_ticks() * 4 / STEP.as_ticks()) {
        cluster.advance(STEP);
    }
    (cluster, old, moving)
}

/// A claim released by the holders its record moved to is not granted a
/// second time by a leader elected among voters that hold only the placement
/// before the move: the holders it left tell the new leader where the record
/// went.
#[test]
fn a_claim_released_after_a_move_is_not_granted_again_by_a_leader_that_hears_only_the_old_holders() {
    // The holder both placements share besides the leader is down while the
    // joiners are admitted and the record moves.
    let (mut cluster, old, moving) = moved_while_down(&|moving| vec![moving.kept.clone()]);

    let claim = cluster.claim(&old, &moving.unplaced, &moving.task);
    cluster.advance(STEP);
    assert!(matches!(cluster.answer(claim), Some(Answer::Claimed(_))), "setup invariant");

    // The old leader and the joiner that stored the claim are cut off, and
    // the voters that never saw it elect a leader.
    cluster.records().set_up(&moving.kept, true);
    let (cut, rest): (BTreeSet<WorkerId>, BTreeSet<WorkerId>) = (
        BTreeSet::from([old.clone(), moving.gained.clone()]),
        BTreeSet::from([moving.kept.clone(), moving.dropped.clone(), moving.unplaced.clone()]),
    );
    cluster.partition(cut, rest.clone());
    advance_until(&mut cluster, |cluster| {
        cluster.states().iter().any(|(id, state)| rest.contains(id) && *state == WorkerState::Leader)
    });
    let next = cluster
        .states()
        .into_iter()
        .find(|(id, state)| rest.contains(id) && *state == WorkerState::Leader)
        .map(|(id, _)| id)
        .expect("the others elected a leader");
    for _ in 0..(SUSPECT.as_ticks() * 4 / STEP.as_ticks()) {
        cluster.advance(STEP);
    }

    let claimant = rest.iter().find(|id| **id != next).expect("another node is left").clone();
    let again = cluster.claim(&next, &claimant, &moving.task);
    cluster.advance(STEP);
    assert!(
        !matches!(cluster.answer(again), Some(Answer::Claimed(_))),
        "the released claim was granted again: {:?}",
        cluster.answer(again)
    );
}

/// A record that moves is not released on the new holders alone: while the
/// other old holders are down, a quorum of the old placement cannot store the
/// write, so the leader answers `NotLeader` instead of releasing a claim that
/// a reader of the old placement would never find.
#[test]
fn a_claim_on_a_moved_record_is_not_released_while_the_old_placement_has_no_quorum() {
    let (mut cluster, old, moving) =
        moved_while_down(&|moving| vec![moving.kept.clone(), moving.dropped.clone()]);

    let refused = cluster.claim(&old, &moving.unplaced, &moving.task);
    cluster.advance(STEP);
    assert!(
        !matches!(cluster.answer(refused), Some(Answer::Claimed(_))),
        "released with the new placement's quorum alone: {:?}",
        cluster.answer(refused)
    );

    cluster.records().set_up(&moving.kept, true);
    for _ in 0..(SUSPECT.as_ticks() * 4 / STEP.as_ticks()) {
        cluster.advance(STEP);
    }
    // The write is published again and reaches the old placement's quorum.
    let version = |holder: &WorkerId| {
        cluster.records().held_by(holder, &moving.task).and_then(|record| record.version)
    };
    assert!(version(&moving.kept).is_some(), "the old holder that came back stores the record");
    assert_eq!(version(&moving.kept), version(&moving.gained));
}

/// Submits to `leader` until a task is placed on three joiners once all eight
/// are admitted, and returns it with those three.
fn submitted_to_three_joiners(cluster: &mut Cluster, leader: &WorkerId) -> (TaskId, Vec<WorkerId>) {
    let all: Vec<WorkerId> = (0..11).map(|n| worker(&format!("worker-{n}"))).collect();
    for _ in 0..400 {
        let task = submitted(cluster, leader);
        let after = RecordSpace::placement(&task, &all, FACTOR).0;
        if after.iter().all(|id| all[3..].contains(id)) {
            return (task, after);
        }
    }
    panic!("no submitted task moves from the voters onto three joiners");
}

/// A move whose write is refused (the old placement has no quorum) and whose
/// leader is lost is still a move for the leader after: it certifies the
/// refused revision on the new holders, and its own revision must reach the
/// old placement too, or a leader elected among the old holders alone takes
/// the task for what it was before the move and grants a released claim again.
#[test]
fn a_move_that_was_refused_before_the_leader_was_lost_is_still_reached_by_the_next_leaders_writes() {
    let voters: BTreeSet<WorkerId> = (0..3).map(|n| worker(&format!("worker-{n}"))).collect();
    let joiners: BTreeSet<WorkerId> = (3..11).map(|n| worker(&format!("worker-{n}"))).collect();
    let mut cluster = Cluster::bootstrap_with_pending(3, 8, SUSPECT);
    cluster.partition(voters.clone(), joiners.clone());
    advance_until(&mut cluster, |cluster| {
        cluster.leader().is_some_and(|leader| {
            cluster.node(&leader).configuration().is_some_and(|configuration| !configuration.is_joint())
        })
    });
    let old = cluster.leader().expect("the voters elected a leader");
    let (task, new_holders) = submitted_to_three_joiners(&mut cluster, &old);
    let (idle, storing) = (new_holders[0].clone(), &new_holders[1..]);

    // Every old holder and one new holder cannot store while the joiners are
    // admitted and the record moves: the old placement has no quorum, so the
    // write is refused, though two of the three new holders have it.
    for holder in voters.iter().chain([&idle]) {
        cluster.records().set_up(holder, false);
    }
    cluster.heal();
    advance_until(&mut cluster, |cluster| {
        cluster.node(&old).configuration().is_some_and(|configuration| {
            !configuration.is_joint() && configuration.voter_count() == Some(11)
        })
    });
    for _ in 0..(SUSPECT.as_ticks() * 4 / STEP.as_ticks()) {
        cluster.advance(STEP);
    }
    assert!(
        storing.iter().all(|holder| cluster.records().held_by(holder, &task).is_some()),
        "setup invariant: the new holders that were up have the record"
    );

    // The old leader is lost; the others elect one, which hears the new
    // holders, certifies the moved record and releases a claim of it.
    let rest: BTreeSet<WorkerId> = cluster.node_ids().into_iter().filter(|id| *id != old).collect();
    cluster.partition(BTreeSet::from([old.clone()]), rest.clone());
    for holder in voters.iter().filter(|holder| **holder != old).chain([&idle]) {
        cluster.records().set_up(holder, true);
    }
    advance_until(&mut cluster, |cluster| {
        cluster.states().iter().any(|(id, state)| rest.contains(id) && *state == WorkerState::Leader)
    });
    let second = cluster
        .states()
        .into_iter()
        .find(|(id, state)| rest.contains(id) && *state == WorkerState::Leader)
        .map(|(id, _)| id)
        .expect("the others elected a leader");
    for _ in 0..(SUSPECT.as_ticks() * 4 / STEP.as_ticks()) {
        cluster.advance(STEP);
    }
    let claimant = rest.iter().find(|id| **id != second).expect("another node is left").clone();
    let claim = cluster.claim(&second, &claimant, &task);
    cluster.advance(STEP);
    assert!(matches!(cluster.answer(claim), Some(Answer::Claimed(_))), "setup invariant");

    // The new holders and their leader are cut off from the old holders, which
    // elect a leader of their own and hear nothing of the new placement.
    let (with_new, with_old): (BTreeSet<WorkerId>, BTreeSet<WorkerId>) = {
        let mut with_new: BTreeSet<WorkerId> = new_holders.iter().cloned().collect();
        with_new.insert(second.clone());
        let with_old = cluster.node_ids().into_iter().filter(|id| !with_new.contains(id)).collect();
        (with_new, with_old)
    };
    cluster.partition(with_new, with_old.clone());
    advance_until(&mut cluster, |cluster| {
        cluster.states().iter().any(|(id, state)| with_old.contains(id) && *state == WorkerState::Leader)
    });
    let third = cluster
        .states()
        .into_iter()
        .find(|(id, state)| with_old.contains(id) && *state == WorkerState::Leader)
        .map(|(id, _)| id)
        .expect("the old holders elected a leader");
    for _ in 0..(SUSPECT.as_ticks() * 4 / STEP.as_ticks()) {
        cluster.advance(STEP);
    }
    let claimant = with_old.iter().find(|id| **id != third).expect("another node is left").clone();
    let again = cluster.claim(&third, &claimant, &task);
    cluster.advance(STEP);
    assert!(
        !matches!(cluster.answer(again), Some(Answer::Claimed(_))),
        "the released claim was granted again: {:?}",
        cluster.answer(again)
    );
}

/// A move that has been stored is over: the leader then writes the record
/// plainly on its new placement, so no later leader has to reach the
/// placement it moved from, whose holders may be gone for good, to write the
/// record again.
#[test]
fn a_stored_move_is_followed_by_a_plain_write_that_names_no_earlier_placement() {
    let (mut cluster, old, moving) = moved_while_down(&|_| Vec::new());

    let held = |holder: &WorkerId| cluster.records().held_by(holder, &moving.task).expect("a new holder has the record");
    let version = |holder: &WorkerId| held(holder).version;
    assert_eq!(version(&old), version(&moving.kept));
    assert_eq!(version(&old), version(&moving.gained));
    for holder in [&old, &moving.kept, &moving.gained] {
        assert!(
            held(holder).prior_placements.is_empty(),
            "{holder:?} holds a record that still names the placement it moved from"
        );
    }
    // The record is not moved again by that write: it is on the same holders.
    assert_eq!(held(&old).placement, held(&moving.kept).placement);
}
