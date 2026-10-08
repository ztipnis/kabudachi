//! A scheduler whose node took office rebuilds from the shard's records
//! alone, replacing whatever it held, holds back tasks whose newest record
//! is not known, and republishes everything at its own term before it is
//! given its grant.

use std::collections::{BTreeMap, BTreeSet};

use kabudachi_core::election::ElectionTimings;
use kabudachi_core::protocol::generated::TaskRecord;
use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::records::TaskRunRecord;
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::reconcile::{
    CoalescingKey,
    ANSWER_IN_FLIGHT, Rebuild, ReconcileTerm, ReportedRun, ReportedState, WorkerRuns,
};
use kabudachi_core::scheduler::{
    CancelRejection, Claim, ClaimRejection, Completion, HeldCancel, MemoryLimits,
    ReconcileRejection, ReportRejection, Submission, SubmitRejection,
};
use kabudachi_core::task_record::RecordVersion;
use kabudachi_core::time::{Clock, WallTime};

use crate::support::scheduler::{
    Fixture, OFFICE, grant_of, newest_records, reconciling, reconciling_after, ticks,
};

fn plain(input: &[u8]) -> Submission {
    Submission::new(
        TaskDefinitionId::new("billing.charge"),
        0,
        input.to_vec(),
        "default",
    )
}

fn worker(id: &str) -> WorkerId {
    WorkerId::new(id)
}

#[test]
fn a_rebuilt_scheduler_holds_what_the_records_say_and_republishes_them_at_its_term() {
    let mut old = Fixture::leading();
    let queued = old.scheduler.submit(plain(b"q")).unwrap();
    let claimed = old.scheduler.submit(plain(b"c")).unwrap();
    old.scheduler
        .request_claim(&worker("w1"), &claimed)
        .unwrap();
    let done = old.scheduler.submit(plain(b"d")).unwrap();
    old.scheduler.cancel(&done).unwrap();
    let mut new = reconciling_after(&old);

    let rebuilt = new
        .scheduler
        .reconcile(Rebuild {
            records: newest_records(&old),
            ..Rebuild::default()
        })
        .unwrap();

    assert_eq!(rebuilt.republished, 3);
    let republished = new.spy.revisions();
    assert!(
        republished.iter().all(|record| {
            let version = RecordVersion::from(record.version.as_ref().unwrap());
            (version.recovery_epoch, version.leader_term) == (OFFICE.recovery_epoch, OFFICE.term)
        }),
        "every record is written again at the new term"
    );
    for task in [&queued, &claimed, &done] {
        let before = old.spy.revisions_of(task).last().cloned().unwrap();
        let after = new.spy.revisions_of(task).last().cloned().unwrap();
        assert_eq!(
            (after.runs, after.task, after.finished),
            (before.runs, before.task, before.finished)
        );
    }
    assert_eq!(
        new.scheduler.request_claim(&worker("w2"), &queued),
        Err(ClaimRejection::NotLeader),
        "no grant yet"
    );

    new.scheduler.set_leadership_grant(Some(grant_of(OFFICE)));

    assert!(new.scheduler.request_claim(&worker("w2"), &queued).is_ok());
    assert_eq!(
        new.scheduler.request_claim(&worker("w2"), &claimed),
        Err(ClaimRejection::AlreadySelected)
    );
    assert_eq!(
        new.scheduler.request_claim(&worker("w2"), &done),
        Err(ClaimRejection::Finished)
    );
}

#[test]
fn a_rebuild_replaces_what_the_scheduler_held_and_never_merges() {
    let mut fixture = Fixture::leading();
    let held_before = fixture.scheduler.submit(plain(b"old")).unwrap();
    fixture.scheduler.set_leadership_grant(None);
    fixture.scheduler.begin_reconcile(OFFICE);

    fixture.scheduler.reconcile(Rebuild::default()).unwrap();
    fixture
        .scheduler
        .set_leadership_grant(Some(grant_of(OFFICE)));

    assert_eq!(
        fixture.scheduler.request_claim(&worker("w1"), &held_before),
        Err(ClaimRejection::TaskUnknown)
    );
}

#[test]
fn an_uncertain_task_is_not_scheduled_cancelled_or_republished() {
    let task = TaskId::new("unknown-newest");
    let mut fixture = reconciling();

    let rebuilt = fixture
        .scheduler
        .reconcile(Rebuild {
            uncertain: BTreeMap::from([(task.clone(), BTreeSet::new())]),
            ..Rebuild::default()
        })
        .unwrap();
    fixture
        .scheduler
        .set_leadership_grant(Some(grant_of(OFFICE)));

    assert_eq!(rebuilt.uncertain, 1);
    assert!(
        fixture.spy.revisions().is_empty(),
        "nothing it does not know is written"
    );
    assert_eq!(
        fixture.scheduler.request_claim(&worker("w1"), &task),
        Err(ClaimRejection::NotReady)
    );
    assert_eq!(
        fixture.scheduler.cancel(&task),
        Err(CancelRejection::NotReady)
    );
    assert!(
        fixture
            .scheduler
            .claim_oldest(&worker("w1"), 10)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn a_delay_counts_from_submission_by_the_wall_clock_and_errs_late() {
    let mut old = Fixture::leading();
    old.clock.set_wall_clock_millis(100_000);
    let task = old
        .scheduler
        .submit(plain(b"d").with_delay(ticks(5_000)))
        .unwrap();
    let mut new = reconciling_after(&old);
    new.clock.set_wall_clock_millis(102_000);

    new.scheduler
        .reconcile(Rebuild {
            records: newest_records(&old),
            ..Rebuild::default()
        })
        .unwrap();
    new.scheduler.set_leadership_grant(Some(grant_of(OFFICE)));

    // 2 s of the 5 s delay had passed by the wall clock; the margin adds 1 s.
    new.clock.advance(ticks(3_999));
    assert_eq!(
        new.scheduler.request_claim(&worker("w1"), &task),
        Err(ClaimRejection::NotReady)
    );
    new.clock.advance(ticks(1));
    assert!(new.scheduler.request_claim(&worker("w1"), &task).is_ok());
}

#[test]
fn a_worker_silent_while_reconciling_loses_its_runs_once_the_grant_arrives() {
    let mut old = Fixture::leading();
    let task = old.scheduler.submit(plain(b"t")).unwrap();
    old.scheduler.request_claim(&worker("w1"), &task).unwrap();
    let mut new = reconciling_after(&old);

    // Its office reports the worker silent while the scheduler reconciles,
    // and the worker's reconnect timeout passes before the grant.
    new.scheduler.note_silence(&worker("w1"), Some(new.clock.now()));
    new.clock.advance(ElectionTimings::DEFAULT_RECONNECT_TIMEOUT);
    assert!(
        new.scheduler.take_events().is_empty() && new.spy.revisions_of(&task).is_empty(),
        "kept, not applied: the scheduler does not lead"
    );
    new.scheduler
        .reconcile(Rebuild {
            records: newest_records(&old),
            ..Rebuild::default()
        })
        .unwrap();
    new.scheduler.set_leadership_grant(Some(grant_of(OFFICE)));

    let record = new.spy.revisions_of(&task).last().cloned().unwrap();
    let states: Vec<TaskRunState> = record
        .runs
        .iter()
        .map(TaskRunRecord::current_state)
        .collect();
    assert_eq!(
        states,
        [TaskRunState::Lost, TaskRunState::Queued],
        "lost and replayed at the grant"
    );
}

#[test]
fn occupancy_memory_use_and_losses_are_rebuilt_from_records() {
    let limits = MemoryLimits {
        soft: 150,
        hard: 200,
    };
    let mut old = Fixture::leading_with_limits(limits);
    let holder = old
        .scheduler
        .submit(plain(&[1; 60]).with_coalescing_key("k"))
        .unwrap();
    old.scheduler.request_claim(&worker("w1"), &holder).unwrap();
    let waiting = old
        .scheduler
        .submit(plain(&[2; 60]).with_coalescing_key("k"))
        .unwrap();
    let mut new = reconciling_after(&old);
    new.scheduler.set_memory_limits(Some(limits));

    new.scheduler
        .reconcile(Rebuild {
            records: newest_records(&old),
            ..Rebuild::default()
        })
        .unwrap();
    new.scheduler.set_leadership_grant(Some(grant_of(OFFICE)));

    assert_eq!(
        new.scheduler.request_claim(&worker("w2"), &waiting),
        Err(ClaimRejection::KeyBusy),
        "the key's running generation holds it after the rebuild: no second one runs"
    );
    assert!(
        matches!(
            new.scheduler.submit(plain(&[3; 90])),
            Err(SubmitRejection::Backpressure { .. })
        ),
        "the 120 bytes the two generations hold still count"
    );
}

#[test]
fn a_run_of_an_uncertain_task_is_asked_again_while_an_unknown_run_is_refused() {
    let mut old = Fixture::leading();
    let task = old.scheduler.submit(plain(b"t")).unwrap();
    old.scheduler.request_claim(&worker("w1"), &task).unwrap();
    let run = old.scheduler.runs_of(&task)[0].clone();
    let mut new = reconciling_after(&old);

    new.scheduler
        .reconcile(Rebuild {
            uncertain: BTreeMap::from([(task, BTreeSet::from([run.clone()]))]),
            ..Rebuild::default()
        })
        .unwrap();
    new.scheduler.set_leadership_grant(Some(grant_of(OFFICE)));

    assert_eq!(
        new.scheduler.report_started(&worker("w1"), &run),
        Err(ReportRejection::NotReady)
    );
    let stranger = kabudachi_core::protocol::ids::TaskRunId::new("never-heard-of");
    assert_eq!(
        new.scheduler.report_started(&worker("w1"), &stranger),
        Err(ReportRejection::UnknownRun)
    );
}

#[test]
fn a_waiting_generation_keeps_what_it_absorbed_across_the_rebuild() {
    let mut old = Fixture::leading();
    let older = old
        .scheduler
        .submit(plain(b"first").with_coalescing_key("k"))
        .unwrap();
    let newer = old
        .scheduler
        .submit(plain(b"second").with_coalescing_key("k"))
        .unwrap();
    let mut new = reconciling_after(&old);

    new.scheduler
        .reconcile(Rebuild {
            records: newest_records(&old),
            ..Rebuild::default()
        })
        .unwrap();
    new.scheduler.set_leadership_grant(Some(grant_of(OFFICE)));

    assert_eq!(
        new.scheduler.request_claim(&worker("w1"), &older),
        Err(ClaimRejection::Superseded)
    );
    let claim = new.scheduler.request_claim(&worker("w1"), &newer).unwrap();
    assert_eq!(
        claim.chain,
        [b"first".to_vec()],
        "the worker still folds the generation it replaced"
    );
}

fn report(worker_id: &str, runs: Vec<ReportedRun>) -> BTreeMap<WorkerId, WorkerRuns> {
    BTreeMap::from([(
        worker(worker_id),
        WorkerRuns {
            runs,
            asked_at: None,
        },
    )])
}

fn reported(claim: &Claim, state: ReportedState) -> ReportedRun {
    ReportedRun {
        claim: Claim {
            chain: Vec::new(),
            ..claim.clone()
        },
        state,
    }
}

/// The run states of `task`'s newest published revision.
fn last_states(fixture: &Fixture, task: &TaskId) -> Vec<TaskRunState> {
    fixture
        .spy
        .revisions_of(task)
        .last()
        .expect("published")
        .runs
        .iter()
        .map(TaskRunRecord::current_state)
        .collect()
}

#[test]
fn a_late_record_names_its_holder_unless_that_worker_answered() {
    let mut old = Fixture::leading();
    let silent = old.scheduler.submit(plain(b"s")).unwrap();
    let answered = old.scheduler.submit(plain(b"a")).unwrap();
    let empty = old.scheduler.submit(plain(b"e")).unwrap();
    old.scheduler.request_claim(&worker("w3"), &silent).unwrap();
    let answered_claim = old
        .scheduler
        .request_claim(&worker("w4"), &answered)
        .unwrap();
    old.scheduler.request_claim(&worker("w5"), &empty).unwrap();
    let records = newest_records(&old);
    let mut new = reconciling_after(&old);
    let rebuilt = new
        .scheduler
        .reconcile(Rebuild {
            uncertain: BTreeMap::from([
                (silent.clone(), BTreeSet::new()),
                (answered.clone(), BTreeSet::new()),
                (empty.clone(), BTreeSet::new()),
            ]),
            // w5 answered the rebuild in full, without the run of its task.
            reports: report("w5", Vec::new()),
            ..Rebuild::default()
        })
        .unwrap();
    new.scheduler.set_leadership_grant(Some(grant_of(OFFICE)));
    assert!(rebuilt.silent_holders.is_empty(), "no record is installed yet");

    let adopted = new
        .scheduler
        .adopt(Rebuild {
            records,
            reports: report("w4", vec![reported(&answered_claim, ReportedState::Running)]),
            ..Rebuild::default()
        })
        .unwrap();

    assert_eq!(adopted.silent_holders, BTreeSet::from([worker("w3")]));
}

// A worker reported silent answers the rebuild late, with a record that
// arrives after the grant: its report counts as hearing it, so its silence
// ends and its run is kept past the reconnect timeout that silence counted.
#[test]
fn a_silent_worker_whose_late_report_is_adopted_is_heard_and_keeps_its_run() {
    let mut old = Fixture::leading();
    let task = old.scheduler.submit(plain(b"late")).unwrap();
    let claim = old.scheduler.request_claim(&worker("w1"), &task).unwrap();
    old.scheduler
        .report_started(&worker("w1"), &claim.task_run_id)
        .unwrap();
    let records = newest_records(&old);
    let mut new = reconciling_after(&old);
    new.scheduler
        .reconcile(Rebuild {
            uncertain: BTreeMap::from([(task.clone(), BTreeSet::new())]),
            ..Rebuild::default()
        })
        .unwrap();
    new.scheduler.set_leadership_grant(Some(grant_of(OFFICE)));
    new.scheduler.note_silence(&worker("w1"), Some(new.clock.now()));

    let adopted = new
        .scheduler
        .adopt(Rebuild {
            records,
            reports: report("w1", vec![reported(&claim, ReportedState::Running)]),
            ..Rebuild::default()
        })
        .unwrap();
    new.clock.advance(ElectionTimings::DEFAULT_RECONNECT_TIMEOUT);
    new.clock.advance(ticks(1));
    new.scheduler.catch_up();

    assert!(
        adopted.answered.contains(&worker("w1")),
        "its node is told the worker was heard"
    );
    assert_eq!(last_states(&new, &task), [TaskRunState::Running]);
}

#[test]
fn a_run_its_worker_still_holds_is_adopted_and_one_it_answered_without_is_lost_and_replayed() {
    let mut old = Fixture::leading();
    let kept = old.scheduler.submit(plain(b"kept")).unwrap();
    let dropped = old.scheduler.submit(plain(b"dropped")).unwrap();
    let kept_claim = old.scheduler.request_claim(&worker("w1"), &kept).unwrap();
    old.scheduler
        .report_started(&worker("w1"), &kept_claim.task_run_id)
        .unwrap();
    old.scheduler
        .request_claim(&worker("w1"), &dropped)
        .unwrap();
    let mut new = reconciling_after(&old);

    new.scheduler
        .reconcile(Rebuild {
            records: newest_records(&old),
            reports: report("w1", vec![reported(&kept_claim, ReportedState::Running)]),
            ..Rebuild::default()
        })
        .unwrap();
    new.scheduler.set_leadership_grant(Some(grant_of(OFFICE)));

    assert_eq!(last_states(&new, &kept), [TaskRunState::Running]);
    assert_eq!(
        last_states(&new, &dropped),
        [TaskRunState::Lost, TaskRunState::Queued]
    );
    assert_eq!(
        new.scheduler.active_runs_of(&worker("w1")),
        [kept_claim.task_run_id],
        "what the worker is believed to hold: the run it reported, not the lost one"
    );
}

#[test]
fn a_running_non_retriable_run_its_worker_answered_without_is_orphaned_and_an_ephemeral_one_lost() {
    let mut old = Fixture::leading();
    let risky = old.scheduler.submit(plain(b"r").non_retriable()).unwrap();
    let fleeting = old.scheduler.submit(plain(b"f").ephemeral()).unwrap();
    for task in [&risky, &fleeting] {
        let claim = old.scheduler.request_claim(&worker("w1"), task).unwrap();
        old.scheduler
            .report_started(&worker("w1"), &claim.task_run_id)
            .unwrap();
    }
    let mut new = reconciling_after(&old);

    new.scheduler
        .reconcile(Rebuild {
            records: newest_records(&old),
            reports: report("w1", Vec::new()),
            ..Rebuild::default()
        })
        .unwrap();

    assert_eq!(last_states(&new, &risky), [TaskRunState::Orphaned]);
    assert_eq!(last_states(&new, &fleeting), [TaskRunState::Lost]);
}

#[test]
fn a_reported_failure_is_applied_and_its_retry_queued() {
    let mut old = Fixture::leading();
    let failed = old.scheduler.submit(plain(b"f").with_retries(1)).unwrap();
    let f = old.scheduler.request_claim(&worker("w1"), &failed).unwrap();
    old.scheduler
        .report_started(&worker("w1"), &f.task_run_id)
        .unwrap();
    let mut new = reconciling_after(&old);

    new.scheduler
        .reconcile(Rebuild {
            records: newest_records(&old),
            reports: report(
                "w1",
                vec![reported(
                    &f,
                    ReportedState::Failed {
                        failure_kind: "ValueError".into(),
                    },
                )],
            ),
            ..Rebuild::default()
        })
        .unwrap();

    assert_eq!(
        last_states(&new, &failed),
        [TaskRunState::Failed, TaskRunState::Queued]
    );
}

#[test]
fn a_run_whose_task_has_no_known_record_is_rebuilt_from_its_claim() {
    let mut old = Fixture::leading();
    let task = old.scheduler.submit(plain(b"orphan-record")).unwrap();
    let claim = old.scheduler.request_claim(&worker("w1"), &task).unwrap();
    let mut new = reconciling_after(&old);

    new.scheduler
        .reconcile(Rebuild {
            reports: report("w1", vec![reported(&claim, ReportedState::Running)]),
            ..Rebuild::default()
        })
        .unwrap();
    new.scheduler.set_leadership_grant(Some(grant_of(OFFICE)));

    let rebuilt = new
        .spy
        .revisions_of(&task)
        .last()
        .cloned()
        .expect("published at the new term");
    assert_eq!(
        rebuilt.task.as_ref().map(|t| t.serialized_input.clone()),
        Some(b"orphan-record".to_vec())
    );
    assert_eq!(rebuilt.runs[0].current_state(), TaskRunState::Running);
    assert!(
        new.scheduler
            .complete(
                &worker("w1"),
                &claim.task_run_id,
                Digest::blake3(b"out"),
                Completion::Final
            )
            .is_ok()
    );
}

/// The records a shard holds when the leader died between writing the newer
/// generation's first revision and the older generation's superseded one.
fn interrupted_supersession() -> (Fixture, TaskId, TaskId, Vec<TaskRecord>) {
    let mut old = Fixture::leading();
    let older = old
        .scheduler
        .submit(plain(b"o").with_coalescing_key("k"))
        .unwrap();
    let before_supersession = newest_records(&old);
    let newer = old
        .scheduler
        .submit(plain(b"n").with_coalescing_key("k"))
        .unwrap();
    let mut records = before_supersession;
    records.push(old.spy.revisions_of(&newer).last().cloned().unwrap());
    (old, older, newer, records)
}

#[test]
fn an_interrupted_supersession_is_finished_by_the_new_leader_older_generation_last() {
    let (old, older, newer, records) = interrupted_supersession();
    let mut new = reconciling_after(&old);

    let rebuilt = new
        .scheduler
        .reconcile(Rebuild {
            records,
            ..Rebuild::default()
        })
        .unwrap();
    new.scheduler.set_leadership_grant(Some(grant_of(OFFICE)));

    assert_eq!(rebuilt.uncertain, 0);
    let older_record = new.spy.revisions_of(&older).last().cloned().unwrap();
    assert_eq!(
        older_record.runs[0].current_state(),
        TaskRunState::Superseded
    );
    assert_eq!(
        older_record
            .link
            .and_then(|link| link.superseded_by)
            .map(TaskId::from),
        Some(newer.clone()),
        "the older record names its successor"
    );
    let written: Vec<TaskId> = new
        .spy
        .revisions()
        .iter()
        .map(|record| record.task.as_ref().unwrap().task_id())
        .collect();
    assert_eq!(written, [newer.clone(), older.clone()], "successor first");
    assert_eq!(
        new.scheduler.request_claim(&worker("w1"), &older),
        Err(ClaimRejection::Superseded)
    );
    assert_eq!(
        new.scheduler
            .request_claim(&worker("w1"), &newer)
            .unwrap()
            .chain,
        [b"o".to_vec()]
    );
}

#[test]
fn a_newer_generation_over_a_claimed_older_one_leaves_the_key_uncertain() {
    let (old, older, newer, mut records) = interrupted_supersession();
    // A pending-only supersession never absorbs a claimed generation, so a
    // record pair that says otherwise cannot be trusted.
    let claimed_older = records
        .iter_mut()
        .find(|record| record.task.as_ref().unwrap().task_id() == older)
        .unwrap();
    claimed_older.runs[0]
        .transition_to(TaskRunState::Claimed, WallTime::from_unix_millis(1))
        .unwrap();
    claimed_older.runs[0].selected_worker = Some(worker("w1").into());
    let mut new = reconciling_after(&old);

    let rebuilt = new
        .scheduler
        .reconcile(Rebuild {
            records,
            ..Rebuild::default()
        })
        .unwrap();
    new.scheduler.set_leadership_grant(Some(grant_of(OFFICE)));

    assert_eq!(rebuilt.uncertain, 2);
    for task in [&older, &newer] {
        assert_eq!(
            new.scheduler.request_claim(&worker("w2"), task),
            Err(ClaimRejection::NotReady)
        );
    }
}

#[test]
fn a_newer_generation_whose_absorbed_record_is_unknown_waits_for_it_and_then_runs() {
    let (old, older, newer, records) = interrupted_supersession();
    let older_record = records[0].clone();
    let newer_only = vec![records[1].clone()];
    let mut new = reconciling_after(&old);

    let rebuilt = new
        .scheduler
        .reconcile(Rebuild {
            records: newer_only,
            ..Rebuild::default()
        })
        .unwrap();
    new.scheduler.set_leadership_grant(Some(grant_of(OFFICE)));

    assert_eq!(rebuilt.uncertain, 1);
    assert!(new.spy.revisions().is_empty(), "nothing half-known is written");
    assert_eq!(
        new.scheduler.request_claim(&worker("w1"), &newer),
        Err(ClaimRejection::NotReady)
    );

    let adopted = new
        .scheduler
        .adopt(Rebuild {
            records: vec![older_record],
            ..Rebuild::default()
        })
        .unwrap();

    assert_eq!(adopted.installed, 2);
    assert_eq!(
        new.scheduler.request_claim(&worker("w1"), &older),
        Err(ClaimRejection::Superseded)
    );
    assert!(new.scheduler.request_claim(&worker("w1"), &newer).is_ok());
}

fn key_k() -> CoalescingKey {
    CoalescingKey {
        definition: TaskDefinitionId::new("billing.charge"),
        key: "k".into(),
    }
}

#[test]
fn a_generation_installed_earlier_is_held_back_once_another_of_its_key_turns_up_uncertain() {
    let (old, older, newer, records) = interrupted_supersession();
    let mut new = reconciling_after(&old);
    new.scheduler
        .reconcile(Rebuild {
            records: vec![records[0].clone()],
            ..Rebuild::default()
        })
        .unwrap();
    new.scheduler.set_leadership_grant(Some(grant_of(OFFICE)));

    new.scheduler
        .adopt(Rebuild {
            uncertain: BTreeMap::from([(newer.clone(), BTreeSet::new())]),
            uncertain_keys: BTreeMap::from([(newer.clone(), key_k())]),
            ..Rebuild::default()
        })
        .unwrap();

    assert_eq!(
        new.scheduler.request_claim(&worker("w1"), &older),
        Err(ClaimRejection::NotReady),
        "no generation of the key runs while one is unknown"
    );
    assert!(
        new.scheduler
            .claim_oldest(&worker("w1"), 1)
            .unwrap_or_default()
            .is_empty(),
        "the queue does not hand it out either"
    );
}

#[test]
fn a_certain_predecessor_waits_for_a_silent_newer_generation_of_its_key_and_the_pair_then_settles() {
    let (old, older, newer, records) = interrupted_supersession();
    let older_record = records[0].clone();
    let newer_record = records[1].clone();
    let mut new = reconciling_after(&old);

    let rebuilt = new
        .scheduler
        .reconcile(Rebuild {
            records: vec![older_record],
            uncertain: BTreeMap::from([(newer.clone(), BTreeSet::new())]),
            uncertain_keys: BTreeMap::from([(newer.clone(), key_k())]),
            ..Rebuild::default()
        })
        .unwrap();
    new.scheduler.set_leadership_grant(Some(grant_of(OFFICE)));

    assert_eq!(rebuilt.republished, 0, "the predecessor is not installed");
    assert!(new.spy.revisions().is_empty());
    for task in [&older, &newer] {
        assert_eq!(
            new.scheduler.request_claim(&worker("w1"), task),
            Err(ClaimRejection::NotReady),
            "no generation of the key runs while one is unknown"
        );
    }

    let adopted = new
        .scheduler
        .adopt(Rebuild {
            records: vec![newer_record],
            ..Rebuild::default()
        })
        .unwrap();

    assert_eq!(adopted.installed, 2);
    assert_eq!(
        new.scheduler.request_claim(&worker("w1"), &older),
        Err(ClaimRejection::Superseded)
    );
    assert_eq!(
        new.scheduler
            .request_claim(&worker("w1"), &newer)
            .unwrap()
            .chain,
        [b"o".to_vec()]
    );
}

#[test]
fn a_newer_generation_arriving_late_over_an_installed_pending_predecessor_finishes_the_supersession() {
    let (old, older, newer, records) = interrupted_supersession();
    let older_record = records[0].clone();
    let newer_record = records[1].clone();
    let mut new = reconciling_after(&old);
    new.scheduler
        .reconcile(Rebuild {
            records: vec![older_record],
            ..Rebuild::default()
        })
        .unwrap();
    new.scheduler.set_leadership_grant(Some(grant_of(OFFICE)));

    new.scheduler
        .adopt(Rebuild {
            records: vec![newer_record],
            ..Rebuild::default()
        })
        .unwrap();

    assert_eq!(
        new.scheduler.request_claim(&worker("w1"), &older),
        Err(ClaimRejection::Superseded)
    );
    assert_eq!(
        new.scheduler
            .request_claim(&worker("w1"), &newer)
            .unwrap()
            .chain,
        [b"o".to_vec()]
    );
    let older_record = new.spy.revisions_of(&older).last().cloned().unwrap();
    assert_eq!(
        older_record.runs[0].current_state(),
        TaskRunState::Superseded,
        "the finished supersession is published"
    );
}

#[test]
fn a_task_without_a_coalescing_key_is_not_held_back_by_an_uncertain_one() {
    let mut old = Fixture::leading();
    let plain_task = old.scheduler.submit(plain(b"p")).unwrap();
    let records = newest_records(&old);
    let mut new = reconciling_after(&old);

    new.scheduler
        .reconcile(Rebuild {
            records,
            uncertain: BTreeMap::from([(TaskId::new("unknown"), BTreeSet::new())]),
            uncertain_keys: BTreeMap::from([(TaskId::new("unknown"), key_k())]),
            ..Rebuild::default()
        })
        .unwrap();
    new.scheduler.set_leadership_grant(Some(grant_of(OFFICE)));

    assert!(new.scheduler.request_claim(&worker("w1"), &plain_task).is_ok());
}

#[test]
fn a_report_held_for_an_uncertain_task_is_applied_when_its_record_arrives() {
    let mut old = Fixture::leading();
    let task = old.scheduler.submit(plain(b"t")).unwrap();
    let claim = old.scheduler.request_claim(&worker("w1"), &task).unwrap();
    let records = newest_records(&old);
    let mut new = reconciling_after(&old);
    new.scheduler
        .reconcile(Rebuild {
            uncertain: BTreeMap::from([(task.clone(), BTreeSet::from([claim.task_run_id.clone()]))]),
            reports: report("w1", vec![reported(&claim, ReportedState::Running)]),
            ..Rebuild::default()
        })
        .unwrap();
    new.scheduler.set_leadership_grant(Some(grant_of(OFFICE)));

    new.scheduler
        .adopt(Rebuild {
            records,
            ..Rebuild::default()
        })
        .unwrap();

    assert_eq!(last_states(&new, &task), [TaskRunState::Running]);
}

#[test]
fn a_late_answer_without_a_run_this_leader_just_decided_does_not_lose_it() {
    let mut fixture = reconciling();
    fixture.scheduler.reconcile(Rebuild::default()).unwrap();
    fixture
        .scheduler
        .set_leadership_grant(Some(grant_of(OFFICE)));
    let task = fixture.scheduler.submit(plain(b"t")).unwrap();
    let claim = fixture
        .scheduler
        .request_claim(&worker("w1"), &task)
        .unwrap();
    let asked_at = fixture.clock.now();

    let adopted = fixture
        .scheduler
        .adopt(Rebuild {
            reports: BTreeMap::from([(
                worker("w1"),
                WorkerRuns {
                    runs: Vec::new(),
                    asked_at: Some(asked_at),
                },
            )]),
            ..Rebuild::default()
        })
        .unwrap();
    assert!(
        adopted.lost.is_empty(),
        "its claim answer may still be on its way"
    );

    fixture.clock.advance(ANSWER_IN_FLIGHT);
    let later = fixture
        .scheduler
        .adopt(Rebuild {
            reports: BTreeMap::from([(
                worker("w1"),
                WorkerRuns {
                    runs: Vec::new(),
                    asked_at: Some(fixture.clock.now()),
                },
            )]),
            ..Rebuild::default()
        })
        .unwrap();
    assert_eq!(
        later
            .lost
            .iter()
            .map(|lost| lost.task_run_id.clone())
            .collect::<Vec<_>>(),
        [claim.task_run_id]
    );
}

#[test]
fn a_submission_for_a_key_with_an_unknown_generation_is_refused_until_the_key_is_known() {
    let mut old = Fixture::leading();
    let unknown = old
        .scheduler
        .submit(plain(b"u").with_coalescing_key("k"))
        .unwrap();
    let records = newest_records(&old);
    let mut new = reconciling_after(&old);
    new.scheduler
        .reconcile(Rebuild {
            uncertain: BTreeMap::from([(unknown.clone(), BTreeSet::new())]),
            uncertain_keys: BTreeMap::from([(unknown.clone(), key_k())]),
            ..Rebuild::default()
        })
        .unwrap();
    new.scheduler.set_leadership_grant(Some(grant_of(OFFICE)));

    assert_eq!(
        new.scheduler.submit(plain(b"n").with_coalescing_key("k")),
        Err(SubmitRejection::KeyNotReady),
        "the unknown generation may still run, so no newer one may start"
    );
    assert!(
        new.scheduler.submit(plain(b"other").with_coalescing_key("j")).is_ok(),
        "another key is not held back"
    );

    new.scheduler
        .adopt(Rebuild {
            records,
            ..Rebuild::default()
        })
        .unwrap();

    let newer = new
        .scheduler
        .submit(plain(b"n").with_coalescing_key("k"))
        .unwrap();
    assert_eq!(
        new.scheduler.request_claim(&worker("w1"), &unknown),
        Err(ClaimRejection::Superseded)
    );
    assert!(new.scheduler.request_claim(&worker("w1"), &newer).is_ok());
}

#[test]
fn knowledge_offered_to_a_scheduler_that_does_not_lead_is_handed_back_and_adopted_once_it_leads() {
    let mut old = Fixture::leading();
    let task = old.scheduler.submit(plain(b"t")).unwrap();
    let records = newest_records(&old);
    let mut new = reconciling_after(&old);
    new.scheduler.reconcile(Rebuild::default()).unwrap();
    let learnt = Rebuild {
        records,
        ..Rebuild::default()
    };

    let refused = new.scheduler.adopt(learnt.clone());

    assert_eq!(refused.unwrap_err(), learnt, "a scheduler without a grant takes nothing");
    new.scheduler.set_leadership_grant(Some(grant_of(OFFICE)));
    assert_eq!(new.scheduler.adopt(learnt).unwrap().installed, 1);
    assert!(new.scheduler.request_claim(&worker("w1"), &task).is_ok());
}

#[test]
fn a_late_generation_of_a_key_leaves_what_the_running_generation_absorbed_intact() {
    let mut old = Fixture::leading();
    old.scheduler
        .submit(plain(b"first").with_coalescing_key("k"))
        .unwrap();
    let running = old
        .scheduler
        .submit(plain(b"second").with_coalescing_key("k").with_retries(1))
        .unwrap();
    let claim = old.scheduler.request_claim(&worker("w1"), &running).unwrap();
    old.scheduler
        .report_started(&worker("w1"), &claim.task_run_id)
        .unwrap();
    let late = old
        .scheduler
        .submit(plain(b"third").with_coalescing_key("k"))
        .unwrap();
    let (late_record, known): (Vec<_>, Vec<_>) = newest_records(&old)
        .into_iter()
        .partition(|record| record.task.as_ref().unwrap().task_id() == late);
    let mut new = reconciling_after(&old);
    new.scheduler
        .reconcile(Rebuild {
            records: known,
            reports: report("w1", vec![reported(&claim, ReportedState::Running)]),
            ..Rebuild::default()
        })
        .unwrap();
    new.scheduler.set_leadership_grant(Some(grant_of(OFFICE)));

    new.scheduler
        .adopt(Rebuild {
            records: late_record,
            ..Rebuild::default()
        })
        .unwrap();
    let failure = new
        .scheduler
        .fail(&worker("w1"), &claim.task_run_id, "ValueError")
        .unwrap();

    assert!(failure.retry.is_some());
    assert_eq!(
        new.scheduler.request_claim(&worker("w2"), &running).unwrap().chain,
        [b"first".to_vec()],
        "the retry still folds the generation it replaced"
    );
}

#[test]
fn a_worker_is_believed_to_hold_the_runs_it_reported_until_a_newer_answer_leaves_them_out() {
    let mut old = Fixture::leading();
    let task = old.scheduler.submit(plain(b"t")).unwrap();
    let claim = old.scheduler.request_claim(&worker("w1"), &task).unwrap();
    let finished = old.scheduler.submit(plain(b"f")).unwrap();
    let done = old.scheduler.request_claim(&worker("w1"), &finished).unwrap();
    let mut new = reconciling_after(&old);
    new.scheduler
        .reconcile(Rebuild {
            uncertain: BTreeMap::from([
                (task.clone(), BTreeSet::from([claim.task_run_id.clone()])),
                (finished.clone(), BTreeSet::from([done.task_run_id.clone()])),
            ]),
            reports: report(
                "w1",
                vec![
                    reported(&claim, ReportedState::Running),
                    reported(
                        &done,
                        ReportedState::Succeeded {
                            result_digest: Digest::blake3(b"out"),
                        },
                    ),
                ],
            ),
            ..Rebuild::default()
        })
        .unwrap();
    new.scheduler.set_leadership_grant(Some(grant_of(OFFICE)));

    assert_eq!(
        new.scheduler.active_runs_of(&worker("w1")),
        [claim.task_run_id],
        "its heartbeat digest names the active run only, so it is not asked again for it"
    );
    assert!(new.scheduler.active_runs_of(&worker("w2")).is_empty());

    new.scheduler
        .adopt(Rebuild {
            reports: report("w1", Vec::new()),
            ..Rebuild::default()
        })
        .unwrap();

    assert!(new.scheduler.active_runs_of(&worker("w1")).is_empty());
}

#[test]
fn a_claimed_compaction_still_holds_its_key_after_a_rebuild_and_its_fold_is_accepted() {
    let limits = Some(MemoryLimits { soft: 300, hard: 10_000 });
    let generation = |payload: &[u8]| plain(payload).with_coalescing_key("k");
    let runner = worker("runner");
    let mut old = Fixture::leading();
    old.scheduler.set_memory_limits(limits);
    old.scheduler.set_compaction_runners(BTreeSet::from([runner.clone()]));
    let holder = old.scheduler.submit(generation(b"h")).unwrap();
    let held = old.scheduler.request_claim(&worker("w1"), &holder).unwrap();
    old.scheduler.report_started(&worker("w1"), &held.task_run_id).unwrap();
    let mut newest = holder;
    for letter in b'a'..b'g' {
        newest = old.scheduler.submit(generation(&[letter; 80])).unwrap();
    }
    let compaction = old
        .scheduler
        .claim_oldest(&runner, 10)
        .unwrap()
        .into_iter()
        .find(|claim| claim.task.compacts.is_some())
        .expect("a compaction was made");
    let mut new = reconciling_after(&old);
    new.scheduler.set_memory_limits(limits);
    new.scheduler.set_compaction_runners(BTreeSet::from([runner.clone()]));

    new.scheduler
        .reconcile(Rebuild {
            records: newest_records(&old),
            reports: BTreeMap::from([
                (
                    runner.clone(),
                    WorkerRuns {
                        runs: vec![reported(&compaction, ReportedState::Claimed)],
                        asked_at: None,
                    },
                ),
                (
                    worker("w1"),
                    WorkerRuns {
                        runs: vec![reported(&held, ReportedState::Running)],
                        asked_at: None,
                    },
                ),
            ]),
            ..Rebuild::default()
        })
        .unwrap();
    new.scheduler.set_leadership_grant(Some(grant_of(OFFICE)));
    new.scheduler
        .complete(&worker("w1"), &held.task_run_id, Digest::blake3(b"out"), Completion::Final)
        .unwrap();

    assert_eq!(
        new.scheduler.request_claim(&worker("w2"), &newest),
        Err(ClaimRejection::KeyBusy),
        "the compaction a worker still holds keeps the newest generation back"
    );
    let folded = compaction.chain.concat();
    let done = new
        .scheduler
        .complete_compaction(&runner, &compaction.task_run_id, folded)
        .unwrap();
    assert!(done.applied);
    assert!(new.scheduler.request_claim(&worker("w2"), &newest).is_ok());
}

#[test]
fn only_the_grant_of_the_office_rebuilt_for_ends_a_reconciliation_and_a_rebuild_is_taken_once() {
    let mut fixture = Fixture::not_leading();
    let refused = fixture.scheduler.reconcile(Rebuild::default()).unwrap_err();
    assert_eq!(refused.rejection, ReconcileRejection::NotReconciling);

    fixture.scheduler.begin_reconcile(OFFICE);
    fixture.scheduler.set_leadership_grant(Some(grant_of(OFFICE)));
    assert!(!fixture.scheduler.is_leader(), "not before the rebuild");

    fixture.scheduler.reconcile(Rebuild::default()).unwrap();
    let again = fixture.scheduler.reconcile(Rebuild::default()).unwrap_err();
    assert_eq!(again.rejection, ReconcileRejection::AlreadyRebuilt);

    let later = ReconcileTerm {
        term: OFFICE.term + 1,
        ..OFFICE
    };
    fixture.scheduler.set_leadership_grant(Some(grant_of(later)));
    assert!(!fixture.scheduler.is_leader(), "not another office's grant");
    assert_eq!(fixture.scheduler.reconciling(), Some(OFFICE));

    fixture.scheduler.set_leadership_grant(Some(grant_of(OFFICE)));
    assert!(fixture.scheduler.is_leader());
    assert_eq!(fixture.scheduler.reconciling(), None);
}

#[test]
fn a_worker_its_office_counted_silent_that_answered_the_rebuild_keeps_its_runs_at_the_grant() {
    let mut old = Fixture::leading();
    let task = old.scheduler.submit(plain(b"t")).unwrap();
    let claim = old.scheduler.request_claim(&worker("w1"), &task).unwrap();
    let mut new = reconciling_after(&old);

    // Its office counted w1 silent, and w1's reconnect timeout has passed, but
    // w1 answered the rebuild: it was heard, so its run is its own.
    new.scheduler.note_silence(&worker("w1"), Some(new.clock.now()));
    new.clock.advance(ElectionTimings::DEFAULT_RECONNECT_TIMEOUT);
    let rebuilt = new
        .scheduler
        .reconcile(Rebuild {
            records: newest_records(&old),
            reports: report("w1", vec![reported(&claim, ReportedState::Claimed)]),
            ..Rebuild::default()
        })
        .unwrap();
    assert!(rebuilt.answered.contains(&worker("w1")), "its node is told w1 was heard");
    new.scheduler.set_leadership_grant(Some(grant_of(OFFICE)));
    new.scheduler.catch_up();

    assert_eq!(
        last_states(&new, &task),
        [TaskRunState::Claimed],
        "w1 answered, so its run is not replayed"
    );
}

#[test]
fn a_successor_learns_a_worker_still_holds_a_run_stored_cancelled_and_names_it_once_it_leads() {
    let mut old = Fixture::leading();
    let cancelled = old.scheduler.submit(plain(b"cancelled")).unwrap();
    let kept = old.scheduler.submit(plain(b"kept")).unwrap();
    let cancelled_claim = old.scheduler.request_claim(&worker("w1"), &cancelled).unwrap();
    old.scheduler
        .report_started(&worker("w1"), &cancelled_claim.task_run_id)
        .unwrap();
    let kept_claim = old.scheduler.request_claim(&worker("w1"), &kept).unwrap();
    // The old leader stored the cancel, and stepped down before its ack told
    // w1: w1 still runs the body.
    old.scheduler.cancel(&cancelled).unwrap();
    let mut new = reconciling_after(&old);
    let held = || {
        vec![
            reported(&cancelled_claim, ReportedState::Running),
            reported(&kept_claim, ReportedState::Claimed),
        ]
    };

    new.scheduler
        .reconcile(Rebuild {
            records: newest_records(&old),
            reports: report("w1", held()),
            ..Rebuild::default()
        })
        .unwrap();
    assert!(
        new.scheduler.take_held_cancels().is_empty(),
        "no worker is told before the successor leads"
    );
    new.scheduler.set_leadership_grant(Some(grant_of(OFFICE)));

    let told = HeldCancel {
        task_id: cancelled.clone(),
        task_run_id: cancelled_claim.task_run_id.clone(),
        worker: worker("w1"),
    };
    assert_eq!(new.scheduler.take_held_cancels(), [told.clone()]);
    assert!(new.scheduler.take_held_cancels().is_empty());

    // Its heartbeats still disagree, so it is asked again, and still holds it.
    new.scheduler
        .adopt(Rebuild {
            reports: report("w1", held()),
            ..Rebuild::default()
        })
        .unwrap();
    assert_eq!(new.scheduler.take_held_cancels(), [told]);
}
