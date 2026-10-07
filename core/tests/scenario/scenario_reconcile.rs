//! A new leader schedules nothing until it has reconciled, and rebuilds
//! only what the shard's records and its workers' answers say.

use std::collections::BTreeSet;

use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::generated::TaskRecord;
use kabudachi_core::protocol::ids::{TaskId, WorkerId};
use kabudachi_core::protocol::records::TaskRunRecord;
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::election::ElectionTimings;
use kabudachi_core::scheduler::ClaimRejection;
use kabudachi_core::task_record::{VersionOrder, identify};
use kabudachi_core::time::Duration;

use super::scenario_records::{
    STEP, SUSPECT, elected_among, plain, running, submitted, submitted_with, submitted_with_key,
};
use crate::support::builders::past_any_suspicion;
use crate::support::harness::{Answer, Cluster};

/// A node that is not one of `excluded`.
fn some_other(cluster: &Cluster, excluded: &[&WorkerId]) -> WorkerId {
    cluster
        .node_ids()
        .into_iter()
        .find(|id| !excluded.contains(&id))
        .expect("the cluster has a node left over")
}

fn advance_until(cluster: &mut Cluster, reached: impl Fn(&Cluster) -> bool) {
    for _ in 0..(SUSPECT.as_ticks() * 60 / STEP.as_ticks()) {
        if reached(cluster) {
            return;
        }
        cluster.advance(STEP);
    }
    assert!(reached(cluster), "the cluster never got there");
}

/// Cuts `leader` off from the rest and advances until the others have
/// elected a leader and it leads; returns it.
fn leader_loss(cluster: &mut Cluster, leader: &WorkerId) -> WorkerId {
    cut_off(cluster, leader);
    advance_until(cluster, |cluster| new_leader(cluster, leader).is_some());
    new_leader(cluster, leader).expect("the others elected a leader")
}

fn cut_off(cluster: &mut Cluster, leader: &WorkerId) {
    let rest: BTreeSet<WorkerId> = cluster
        .node_ids()
        .into_iter()
        .filter(|id| id != leader)
        .collect();
    cluster.partition(BTreeSet::from([leader.clone()]), rest);
}

/// The node, other than `deposed`, that leads.
fn new_leader(cluster: &Cluster, deposed: &WorkerId) -> Option<WorkerId> {
    cluster
        .states()
        .into_iter()
        .find(|(id, state)| id != deposed && *state == WorkerState::Leader)
        .map(|(id, _)| id)
}

/// Makes `workers` unreachable for the leader's reconciliation and record
/// writes while they stay in the cluster (and in its roster).
fn mute(cluster: &Cluster, workers: &[WorkerId], muted: bool) {
    for worker in workers {
        cluster.records().set_up(worker, !muted);
    }
}

/// The newest revision of `task` any node holds.
fn newest_record(cluster: &Cluster, task: &TaskId) -> Option<TaskRecord> {
    cluster
        .node_ids()
        .iter()
        .filter_map(|holder| cluster.records().held_by(holder, task))
        .reduce(|newest, record| {
            let (_, held) = identify(&newest).expect("a held record is identified");
            let (_, other) = identify(&record).expect("a held record is identified");
            if held.order(&other) == VersionOrder::Newer {
                record
            } else {
                newest
            }
        })
}

fn run_states(record: &TaskRecord) -> Vec<TaskRunState> {
    record.runs.iter().map(TaskRunRecord::current_state).collect()
}

/// The state of every run in the newest revision of `task` any node holds.
fn held_states(cluster: &Cluster, task: &TaskId) -> Vec<TaskRunState> {
    run_states(&newest_record(cluster, task).expect("some node holds the task"))
}

/// The leader term of the revision `holder` holds of `task`.
fn held_term(cluster: &Cluster, holder: &WorkerId, task: &TaskId) -> Option<u64> {
    cluster
        .records()
        .held_by(holder, task)
        .and_then(|record| record.version)
        .map(|version| version.leader_term)
}

fn placement_of(cluster: &Cluster, task: &TaskId) -> Vec<WorkerId> {
    newest_record(cluster, task)
        .expect("some node holds the task")
        .placement
        .into_iter()
        .map(WorkerId::from)
        .collect()
}

fn term_of(cluster: &Cluster, leader: &WorkerId) -> u64 {
    cluster
        .node(leader)
        .office_term()
        .expect("a leader holds an office")
        .term
}

fn refused(rejection: ClaimRejection) -> Option<Answer> {
    Some(Answer::Refused(rejection.to_string()))
}

#[test]
fn no_claim_is_answered_before_the_new_leader_has_reconciled() {
    let (mut cluster, leader) = elected_among(5);
    let task = submitted(&mut cluster, &leader);
    // The survivors cannot answer the new leader's questions yet.
    let survivors: Vec<WorkerId> = cluster.node_ids().into_iter().filter(|id| *id != leader).collect();
    mute(&cluster, &survivors, true);
    cut_off(&mut cluster, &leader);
    advance_until(&mut cluster, |cluster| {
        cluster.states().values().any(|state| *state == WorkerState::LeaderReconciling)
    });
    let next = cluster
        .states()
        .into_iter()
        .find(|(_, state)| *state == WorkerState::LeaderReconciling)
        .map(|(id, _)| id)
        .unwrap();
    let claimant = some_other(&cluster, &[&leader, &next]);

    let early = cluster.claim(&next, &claimant, &task);
    cluster.advance(STEP);
    assert_eq!(
        cluster.answer(early).cloned(),
        refused(ClaimRejection::NotLeader),
        "asked while reconciling"
    );

    mute(&cluster, &survivors, false);
    advance_until(&mut cluster, |cluster| cluster.states()[&next] == WorkerState::Leader);
    let late = cluster.claim(&next, &claimant, &task);
    cluster.advance(STEP);
    assert!(matches!(cluster.answer(late), Some(Answer::Claimed(_))));
}

#[test]
fn the_newest_lost_generation_of_a_key_is_replayed_after_a_leader_loss_and_a_stale_one_is_not() {
    let (mut cluster, leader) = elected_among(5);
    let worker = some_other(&cluster, &[&leader]);
    let alone = submitted_with_key(&mut cluster, &leader, "alone");
    let stale = submitted_with_key(&mut cluster, &leader, "pair");
    // The claims are stored but their answers never reach the worker, which
    // holds neither run when it is asked.
    cluster
        .records()
        .set_ack_delay(Duration::from_ticks(SUSPECT.as_ticks() * 100));
    cluster.claim(&leader, &worker, &alone);
    cluster.claim(&leader, &worker, &stale);
    cluster.records().set_ack_delay(Duration::from_ticks(0));
    let newer = submitted_with_key(&mut cluster, &leader, "pair"); // waits behind the claimed one

    let next = leader_loss(&mut cluster, &leader);

    assert_eq!(held_states(&cluster, &alone), [TaskRunState::Lost, TaskRunState::Queued]);
    assert_eq!(held_states(&cluster, &stale), [TaskRunState::Lost]);
    let claim = cluster.claim(&next, &some_other(&cluster, &[&leader, &next]), &newer);
    cluster.advance(STEP);
    assert!(matches!(cluster.answer(claim), Some(Answer::Claimed(_))));
}

#[test]
fn a_result_sent_before_the_leader_died_is_certified_by_the_next() {
    let (mut cluster, leader) = elected_among(5);
    let worker = some_other(&cluster, &[&leader]);
    let task = submitted(&mut cluster, &leader);
    let run = running(&mut cluster, &leader, &worker, &task);
    cluster.records().hold_writes_from(&leader); // the certification never reaches a holder
    let ticket = cluster.complete(&leader, &worker, &run, Digest::blake3(b"out"));

    let next = leader_loss(&mut cluster, &leader);

    assert_eq!(cluster.answer(ticket), Some(&Answer::NotLeader));
    let record = newest_record(&cluster, &task).unwrap();
    assert_eq!(run_states(&record), [TaskRunState::Succeeded]);
    assert_eq!(
        record.version.unwrap().leader_term,
        term_of(&cluster, &next),
        "certified by the new leader, from the worker's report"
    );
}

#[test]
fn a_certification_stored_before_the_leader_died_stands_whether_or_not_it_was_acknowledged() {
    for acknowledged in [false, true] {
        let (mut cluster, leader) = elected_among(5);
        let worker = some_other(&cluster, &[&leader]);
        let task = submitted(&mut cluster, &leader);
        let run = running(&mut cluster, &leader, &worker, &task);
        if !acknowledged {
            cluster
                .records()
                .set_ack_delay(Duration::from_ticks(SUSPECT.as_ticks() * 100));
        }
        let ticket = cluster.complete(&leader, &worker, &run, Digest::blake3(b"out"));
        cluster.records().set_ack_delay(Duration::from_ticks(0));
        cluster.advance(STEP);
        assert_eq!(
            matches!(cluster.answer(ticket), Some(Answer::Certified(_))),
            acknowledged
        );

        let next = leader_loss(&mut cluster, &leader);

        assert_eq!(
            held_states(&cluster, &task),
            [TaskRunState::Succeeded],
            "acknowledged: {acknowledged}"
        );
        assert_eq!(
            newest_record(&cluster, &task).unwrap().version.unwrap().leader_term,
            term_of(&cluster, &next),
            "republished by the new leader, acknowledged: {acknowledged}"
        );
    }
}

#[test]
fn a_non_retriable_run_on_a_worker_that_answers_survives_the_election() {
    let (mut cluster, leader) = elected_among(5);
    let worker = some_other(&cluster, &[&leader]);
    let task = submitted_with(&mut cluster, &leader, plain().non_retriable());
    let run = running(&mut cluster, &leader, &worker, &task);

    let next = leader_loss(&mut cluster, &leader);

    let ticket = cluster.complete(&next, &worker, &run, Digest::blake3(b"out"));
    cluster.advance(STEP);
    assert!(matches!(cluster.answer(ticket), Some(Answer::Certified(_))));
}

#[test]
fn a_worker_that_died_with_the_old_leader_is_lost_a_reconnect_timeout_after_the_new_one_took_office() {
    for retriable in [true, false] {
        let (mut cluster, leader) = elected_among(5);
        let worker = some_other(&cluster, &[&leader]);
        let submission = if retriable { plain() } else { plain().non_retriable() };
        let task = submitted_with(&mut cluster, &leader, submission);
        running(&mut cluster, &leader, &worker, &task);
        // The worker and the old leader are cut off from the other three
        // together, so the worker never answers the new leader's roll call.
        let rest: BTreeSet<WorkerId> = cluster
            .node_ids()
            .into_iter()
            .filter(|id| *id != leader && *id != worker)
            .collect();
        cluster.partition(BTreeSet::from([leader.clone(), worker.clone()]), rest);
        advance_until(&mut cluster, |cluster| new_leader(cluster, &leader).is_some());
        let took_office = cluster.now();
        let lost_after = (SUSPECT.as_ticks() + ElectionTimings::DEFAULT_RECONNECT_TIMEOUT.as_ticks())
            as i64;

        let mut lost_in = None;
        for _ in 0..(lost_after as u64 * 2 / STEP.as_ticks()) {
            if held_states(&cluster, &task) != [TaskRunState::Running] {
                lost_in = Some((cluster.now() - took_office).as_ticks() as i64);
                break;
            }
            cluster.advance(STEP);
        }

        let lost_in = lost_in.unwrap_or_else(|| panic!("its run stayed running, retriable: {retriable}"));
        assert!(
            (lost_in - lost_after).abs() <= SUSPECT.as_ticks() as i64,
            "lost {lost_in} ticks after the takeover, retriable: {retriable}"
        );
        let expected: &[TaskRunState] = if retriable {
            &[TaskRunState::Lost, TaskRunState::Queued]
        } else {
            &[TaskRunState::Orphaned]
        };
        assert_eq!(held_states(&cluster, &task), expected, "retriable: {retriable}");
    }
}

/// A new leader that cannot yet tell what became of a task: its newest
/// revision is held by two voters that do not answer, and the third holder
/// has only an older one.
struct SilentHolders {
    cluster: Cluster,
    deposed: WorkerId,
    leader: WorkerId,
    task: TaskId,
    /// The holder of the older revision, which answers.
    stale: WorkerId,
    /// The two holders of the newest revision, which do not answer yet.
    silent: Vec<WorkerId>,
}

/// Seven voters; the task's placement is three of them. Its claim was stored
/// by two placement holders while the third was down; then the leader is lost
/// and those two cannot answer the new leader, which leads without them.
fn leader_missing_two_holders() -> SilentHolders {
    let (cluster, deposed) = elected_among(7);
    leader_missing_two_holders_in(cluster, deposed)
}

/// [`leader_missing_two_holders`] in a cluster that is already elected.
fn leader_missing_two_holders_in(mut cluster: Cluster, deposed: WorkerId) -> SilentHolders {
    let task = submitted(&mut cluster, &deposed);
    let placement = placement_of(&cluster, &task);
    // The deposed leader, if it holds a copy, is the one that missed the
    // claim, so the two silent holders are never the deposed leader.
    let stale = placement
        .iter()
        .find(|holder| **holder == deposed)
        .or_else(|| placement.first())
        .cloned()
        .expect("a placement holder");
    let silent: Vec<WorkerId> = placement.iter().filter(|holder| **holder != stale).cloned().collect();
    assert!(!silent.contains(&deposed), "the silent holders are not the deposed leader");
    let claimant = some_other(&cluster, &[&deposed, &stale, &silent[0], &silent[1]]);
    mute(&cluster, &[stale.clone()], true);
    let claim = cluster.claim(&deposed, &claimant, &task);
    cluster.advance(STEP);
    assert!(matches!(cluster.answer(claim), Some(Answer::Claimed(_))));
    mute(&cluster, &[stale.clone()], false);
    mute(&cluster, &silent, true);

    let leader = leader_loss(&mut cluster, &deposed);
    SilentHolders { cluster, deposed, leader, task, stale, silent }
}

#[test]
fn two_silent_holders_of_the_newest_revision_leave_the_task_unscheduled_until_one_answers() {
    let SilentHolders { mut cluster, deposed, leader: next, task, stale, silent } = leader_missing_two_holders();
    let placement = placement_of(&cluster, &task);

    let term = term_of(&cluster, &next);
    let other = some_other(&cluster, &[&deposed, &stale, &silent[0], &silent[1], &next]);
    let asked = cluster.claim(&next, &other, &task);
    cluster.advance(STEP);
    assert_eq!(cluster.answer(asked).cloned(), refused(ClaimRejection::NotReady));
    assert!(
        placement.iter().all(|holder| held_term(&cluster, holder, &task) < Some(term)),
        "an uncertain task is not republished"
    );

    // One of the silent holders answers late.
    mute(&cluster, &silent[..1], false);
    advance_until(&mut cluster, |cluster| held_term(cluster, &stale, &task) == Some(term));
    assert_eq!(
        held_states(&cluster, &task),
        [TaskRunState::Claimed],
        "the claim the silent holders stored is the one that stands"
    );
}

#[test]
fn an_answer_that_comes_after_the_leader_lost_its_lease_is_adopted_by_the_next_office() {
    let SilentHolders { mut cluster, deposed, leader, task, stale, silent } = leader_missing_two_holders();
    let first_term = term_of(&cluster, &leader);

    // Its lease ends before the silent holder answers, with no tick between.
    let lease = cluster.node(&leader).timings().lease_length();
    cluster.advance_clock_only(Duration::from_ticks(lease.as_ticks() + STEP.as_ticks()));
    mute(&cluster, &silent[..1], false);
    cluster.advance(STEP);

    // The answer was not acted on by the leader whose lease had ended.
    assert_ne!(cluster.states()[&leader], WorkerState::Leader);
    assert!(
        held_term(&cluster, &stale, &task) < Some(first_term),
        "no record was republished at the lapsed office's term"
    );

    // Once the cluster has a leader again, it adopts what the answer taught.
    mute(&cluster, &silent[1..], false);
    advance_until(&mut cluster, |cluster| new_leader(cluster, &deposed).is_some());
    let next = new_leader(&cluster, &deposed).expect("a leader");
    let term = term_of(&cluster, &next);
    advance_until(&mut cluster, |cluster| held_term(cluster, &stale, &task) == Some(term));
    assert_eq!(held_states(&cluster, &task), [TaskRunState::Claimed], "the claim the silent holders stored stands");
}

#[test]
fn a_late_write_of_the_deposed_leader_loses_to_the_new_terms_republish() {
    let (mut cluster, leader) = elected_among(5);
    let task = submitted(&mut cluster, &leader);
    let claimant = some_other(&cluster, &[&leader]);
    cluster.records().hold_writes_from(&leader);
    cluster.claim(&leader, &claimant, &task); // decided, written, held on the way

    let next = leader_loss(&mut cluster, &leader);
    let term = term_of(&cluster, &next);
    cluster.records().release_writes_from(&leader, cluster.now());

    // The deposed leader's own store, cut off when the new leader wrote,
    // takes the late write; every holder the new leader reached keeps its
    // republish.
    let reached: Vec<WorkerId> = placement_of(&cluster, &task)
        .into_iter()
        .filter(|holder| *holder != leader)
        .collect();
    assert!(reached.len() >= 2);
    for holder in reached {
        assert_eq!(held_term(&cluster, &holder, &task), Some(term), "{holder:?}");
        let held = cluster.records().held_by(&holder, &task).unwrap();
        assert_eq!(run_states(&held), [TaskRunState::Queued], "{holder:?} kept the new term's queued task, not the late claim");
    }
}

#[test]
fn a_supersession_split_across_the_leader_loss_is_finished_by_the_new_leader() {
    let (mut cluster, leader) = elected_among(5);
    let older = submitted_with_key(&mut cluster, &leader, "k");
    cluster.records().set_ack_delay(STEP);
    let ticket = cluster.submit(&leader, plain().with_coalescing_key("k"));
    // The newer generation's first revision has landed; the older one's
    // superseded revision waits for that write's acknowledgement, and the
    // leader is cut off before it is delivered.
    cluster.records().hold_writes_from(&leader);
    cluster.records().set_ack_delay(Duration::from_ticks(0));
    let newer = cluster
        .node_ids()
        .iter()
        .flat_map(|holder| cluster.records().held_records(holder))
        .filter_map(|record| identify(&record).ok().map(|(task, _)| task))
        .find(|task| *task != older)
        .expect("the newer generation's first revision landed");

    let next = leader_loss(&mut cluster, &leader);

    assert_eq!(cluster.answer(ticket), Some(&Answer::NotLeader), "the client was never told");
    assert_eq!(held_states(&cluster, &older), [TaskRunState::Superseded]);
    let claimant = some_other(&cluster, &[&leader, &next]);
    let stale = cluster.claim(&next, &claimant, &older);
    let current = cluster.claim(&next, &claimant, &newer);
    cluster.advance(STEP);
    assert_eq!(cluster.answer(stale).cloned(), refused(ClaimRejection::Superseded));
    assert!(matches!(cluster.answer(current), Some(Answer::Claimed(_))));
}

#[test]
fn an_answer_that_comes_while_the_fence_is_lapsed_is_adopted_once_it_is_renewed() {
    let mut cluster = Cluster::bootstrap_with_authority(7, 0, SUSPECT);
    cluster.advance(past_any_suspicion(SUSPECT.as_ticks()));
    cluster.run_until_quiescent(Duration::from_millis(500), 100);
    let deposed = cluster.leader().expect("the voters elect a leader");
    let SilentHolders { mut cluster, leader, task, stale, silent, .. } =
        leader_missing_two_holders_in(cluster, deposed);
    let term = term_of(&cluster, &leader);

    // The new leader reaches the authority no more, so it holds office
    // without a fence, and its scheduler does not lead.
    cluster.node_authority(&leader).set_reachable(false);
    mute(&cluster, &silent[..1], false);
    cluster.advance(STEP);
    cluster.advance(STEP);
    assert_eq!(cluster.states()[&leader], WorkerState::Leader);
    assert!(!cluster.holds_valid_grant(&leader), "no fence, no grant");
    assert_ne!(held_term(&cluster, &stale, &task), Some(term), "the answer is not adopted without a grant");

    // The fence is renewed, and the answer that came meanwhile is adopted.
    cluster.node_authority(&leader).set_reachable(true);
    advance_until(&mut cluster, |cluster| held_term(cluster, &stale, &task) == Some(term));
    assert!(cluster.holds_valid_grant(&leader));
    assert_eq!(held_states(&cluster, &task), [TaskRunState::Claimed], "the claim the silent holders stored stands");
}
