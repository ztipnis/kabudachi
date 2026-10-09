//! What a new leader may conclude from the answers it has, and when it may
//! stop asking.

use std::collections::{BTreeMap, BTreeSet};

use kabudachi_core::coordination_authority::RecoveryEpoch;
use kabudachi_core::protocol::generated::{Task, TaskRecord};
use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, TaskRunId, WorkerId};
use kabudachi_core::reconcile::{
    Answered, CoalescingKey, Cursor, HeldKey, ReconcileRound, ReconcileTerm, ReportPage, ReportedRun,
    ReportedState,
};
use kabudachi_core::scheduler::Claim;
use kabudachi_core::task_record::RecordVersion;
use kabudachi_core::time::{Duration, Instant};

const TERM: ReconcileTerm = ReconcileTerm {
    recovery_epoch: RecoveryEpoch::new(0, 0),
    term: 5,
};
const GRACE: Duration = Duration::from_millis(1_000);

fn w(id: &str) -> WorkerId {
    WorkerId::new(id)
}

fn version(term: u64, revision: u64) -> RecordVersion {
    RecordVersion {
        recovery_epoch: RecoveryEpoch::new(0, 0),
        leader_term: term,
        revision,
    }
}

fn held(task: &str, version: RecordVersion, placement: &[&str]) -> HeldKey {
    HeldKey {
        task_id: TaskId::new(task),
        version,
        input_digest: None,
        latest_run: Some(TaskRunId::new(format!("{task}-run"))),
        placement: placement.iter().map(|id| w(id)).collect(),
        finished: false,
        coalescing: None,
    }
}

fn record(task: &str, version: RecordVersion, placement: &[&str]) -> TaskRecord {
    TaskRecord {
        version: Some(version.into()),
        task: Some(Task {
            task_id: Some(TaskId::new(task).into()),
            ..Task::default()
        }),
        placement: placement.iter().map(|id| w(id).into()).collect(),
        ..TaskRecord::default()
    }
}

fn running(task: &str, run: &str) -> ReportedRun {
    ReportedRun {
        claim: Claim {
            task: Task {
                task_id: Some(TaskId::new(task).into()),
                ..Task::default()
            },
            task_run_id: TaskRunId::new(run),
            attempt_number: 1,
            reconnect_timeout: kabudachi_core::election::ElectionTimings::DEFAULT_RECONNECT_TIMEOUT,
            chain: Vec::new(),
        },
        state: ReportedState::Running,
    }
}

fn answer(round: &mut ReconcileRound, from: &str, keys: Vec<HeldKey>) {
    let next = round.page(
        &w(from),
        ReportPage {
            keys,
            last: true,
            ..ReportPage::default()
        },
        Instant::at(0),
    );
    assert_eq!(next, None);
}

fn seven() -> Vec<WorkerId> {
    ["a", "b", "c", "d", "e", "f", "g"].map(w).to_vec()
}

fn versions(records: &[TaskRecord]) -> Vec<RecordVersion> {
    records
        .iter()
        .map(|r| RecordVersion::from(r.version.as_ref().unwrap()))
        .collect()
}

#[test]
fn a_task_is_known_once_every_holder_still_in_the_configuration_answered() {
    let mut round = ReconcileRound::new(TERM, seven(), Instant::at(0), GRACE);
    answer(
        &mut round,
        "c",
        vec![held("t", version(3, 9), &["a", "b", "c"])],
    );
    round.fetched(record("t", version(3, 9), &["a", "b", "c"]));
    assert!(round.take_settled(|_| true).records.is_empty());

    let gone: BTreeSet<WorkerId> = [w("a"), w("b")].into();
    let settled = round.take_settled(|worker| !gone.contains(worker));

    assert_eq!(
        settled.records.len(),
        1,
        "a and b left: c's revision is the newest anyone still holds"
    );
}

#[test]
fn a_version_whose_every_holder_left_and_whose_record_is_not_found_falls_back_to_the_newest_found() {
    let mut round = ReconcileRound::new(TERM, seven(), Instant::at(0), GRACE);
    answer(&mut round, "a", vec![held("t", version(4, 0), &["a", "b", "c"])]);
    answer(&mut round, "d", vec![held("t", version(3, 9), &["d"])]);
    round.fetched(record("t", version(3, 9), &["d"]));
    let gone: BTreeSet<WorkerId> = [w("a"), w("b"), w("c")].into();
    let present = |worker: &WorkerId| !gone.contains(worker);

    let uncertain = round.take_settled(|_| true);
    assert!(uncertain.records.is_empty(), "while a holder of the newest is a member");
    assert!(uncertain.uncertain.contains_key(&TaskId::new("t")));

    assert!(round.missing_records(present).is_empty());
    let settled = round.take_settled(present);

    assert_eq!(versions(&settled.records), [version(3, 9)]);
    assert!(settled.uncertain.is_empty());
}

#[test]
fn a_reported_version_without_a_full_record_as_new_is_uncertain() {
    let mut round = ReconcileRound::new(TERM, [w("a"), w("b"), w("c")], Instant::at(0), GRACE);
    for holder in ["a", "b", "c"] {
        answer(
            &mut round,
            holder,
            vec![held("t", version(4, 2), &["a", "b", "c"])],
        );
    }
    assert_eq!(round.missing_records(|_| true), [TaskId::new("t")]);
    round.fetched(record("t", version(4, 1), &["a", "b", "c"]));
    assert_eq!(
        round.missing_records(|_| true),
        [TaskId::new("t")],
        "an older record does not stand in for the newest"
    );

    assert!(
        round
            .take_settled(|_| true)
            .uncertain
            .contains_key(&TaskId::new("t"))
    );
}

#[test]
fn an_uncertain_task_names_its_coalescing_key_and_a_keyless_one_names_none() {
    let mut round = ReconcileRound::new(TERM, seven(), Instant::at(0), GRACE);
    let key = CoalescingKey {
        definition: TaskDefinitionId::new("d"),
        key: "k".into(),
    };
    let mut keyed = held("t", version(3, 9), &["a", "b", "c"]);
    keyed.coalescing = Some(key.clone());
    let keyless = held("u", version(3, 9), &["a", "b", "c"]);
    // Only c answers, so neither newest record can be known yet.
    answer(&mut round, "c", vec![keyed, keyless]);

    let settled = round.take_settled(|_| true);

    assert!(settled.uncertain.contains_key(&TaskId::new("u")));
    assert_eq!(
        settled.uncertain_keys,
        BTreeMap::from([(TaskId::new("t"), key)])
    );
}

#[test]
fn an_uncertain_task_names_every_run_reported_for_it() {
    let mut round = ReconcileRound::new(TERM, seven(), Instant::at(0), GRACE);
    let page = ReportPage {
        runs: vec![running("t", "r-claimed")],
        keys: vec![held("t", version(3, 9), &["a", "b", "c"])],
        last: true,
    };
    round.page(&w("c"), page, Instant::at(0));
    round.fetched(record("t", version(3, 9), &["a", "b", "c"]));

    let settled = round.take_settled(|_| true);

    assert_eq!(
        settled.uncertain[&TaskId::new("t")],
        BTreeSet::from([TaskRunId::new("t-run"), TaskRunId::new("r-claimed")])
    );
}

#[test]
fn the_round_may_stop_when_every_voter_answered_or_a_quorum_did_after_the_grace() {
    let round = ReconcileRound::new(TERM, [w("a"), w("b"), w("c")], Instant::at(0), GRACE);
    assert!(round.may_finish(Answered::All, Instant::at(0)));
    assert!(!round.may_finish(Answered::Quorum, Instant::at(999)));
    assert!(round.may_finish(Answered::Quorum, Instant::at(1_000)));
    assert!(
        !round.may_finish(Answered::Short, Instant::at(1_000_000)),
        "never without a quorum"
    );
}

#[test]
fn pages_continue_after_the_last_run_then_the_last_key() {
    let mut round = ReconcileRound::new(TERM, [w("a")], Instant::at(0), GRACE);
    let run_page = ReportPage {
        runs: vec![running("t", "r9")],
        ..ReportPage::default()
    };
    assert_eq!(
        round.page(&w("a"), run_page, Instant::at(0)),
        Some(Cursor::AfterRun(TaskRunId::new("r9")))
    );
    let key_page = ReportPage {
        keys: vec![held("t7", version(1, 0), &["a"])],
        ..ReportPage::default()
    };
    assert_eq!(
        round.page(&w("a"), key_page, Instant::at(0)),
        Some(Cursor::AfterKey(TaskId::new("t7")))
    );
    assert_eq!(
        round.page(
            &w("a"),
            ReportPage {
                last: true,
                ..ReportPage::default()
            },
            Instant::at(0)
        ),
        None
    );
    assert_eq!(round.answered(), BTreeSet::from([w("a")]));
}

#[test]
fn a_late_answer_is_handed_over_once_with_when_it_was_asked() {
    let mut round = ReconcileRound::new(TERM, [w("a"), w("b")], Instant::at(0), GRACE);
    let runs = |run: &str| ReportPage {
        runs: vec![running("t", run)],
        last: true,
        ..ReportPage::default()
    };
    round.page(&w("a"), runs("r1"), Instant::at(0));

    let first = round.take_settled(|_| true);
    assert_eq!(first.reports.keys().cloned().collect::<Vec<_>>(), [w("a")]);
    assert_eq!(first.reports[&w("a")].asked_at, None);
    assert_eq!(round.unanswered(), [w("b")]);

    round.page(&w("b"), runs("r2"), Instant::at(40));
    let later = round.take_settled(|_| true);
    let expected: BTreeMap<WorkerId, Option<Instant>> =
        BTreeMap::from([(w("b"), Some(Instant::at(40)))]);
    assert_eq!(
        later
            .reports
            .iter()
            .map(|(worker, runs)| (worker.clone(), runs.asked_at))
            .collect::<BTreeMap<_, _>>(),
        expected,
        "a's answer was handed over already"
    );
}

#[test]
fn a_page_from_a_worker_not_asked_is_ignored_until_it_is_asked() {
    let mut round = ReconcileRound::new(TERM, [w("a")], Instant::at(0), GRACE);
    answer(
        &mut round,
        "joiner",
        vec![held("t", version(1, 0), &["joiner"])],
    );
    assert!(round.answered().is_empty());
    assert!(round.missing_records(|_| true).is_empty());

    round.ask_also(w("joiner"));
    assert_eq!(round.unanswered(), [w("a"), w("joiner")]);
}

#[test]
fn the_round_is_complete_when_every_worker_asked_answered_or_left_and_no_task_is_uncertain() {
    let mut round = ReconcileRound::new(TERM, [w("a"), w("b"), w("c")], Instant::at(0), GRACE);
    answer(
        &mut round,
        "a",
        vec![held("t", version(3, 9), &["a", "b", "c"])],
    );
    round.fetched(record("t", version(3, 9), &["a", "b", "c"]));
    assert!(!round.is_complete(|_| true));

    answer(&mut round, "b", Vec::new());
    assert!(!round.is_complete(|_| true), "c has not answered");
    assert!(
        round.is_complete(|worker| *worker != w("c")),
        "c left, and a and b are the holders left"
    );
    answer(&mut round, "c", Vec::new());
    assert!(round.is_complete(|_| true));
}

#[test]
fn a_newer_version_reported_after_an_older_fetch_is_not_covered_by_it() {
    let mut round = ReconcileRound::new(TERM, seven(), Instant::at(0), GRACE);
    answer(
        &mut round,
        "c",
        vec![held("t", version(3, 9), &["a", "b", "c"])],
    );
    round.fetched(record("t", version(3, 9), &["a", "b", "c"]));
    assert!(round.missing_records(|_| true).is_empty());

    answer(
        &mut round,
        "a",
        vec![held("t", version(4, 0), &["a", "b", "c"])],
    );

    assert_eq!(round.missing_records(|_| true), [TaskId::new("t")]);
    let settled = round.take_settled(|_| true);
    assert!(settled.records.is_empty(), "the term 3 record is not current");
    assert!(settled.uncertain.contains_key(&TaskId::new("t")));
}

#[test]
fn a_truncated_page_is_not_an_answer() {
    let mut round = ReconcileRound::new(TERM, [w("a")], Instant::at(0), GRACE);
    let more = ReportPage {
        runs: vec![running("t", "r-stale")],
        ..ReportPage::default()
    };
    assert!(round.page(&w("a"), more, Instant::at(0)).is_some());

    let truncated = round.page(&w("a"), ReportPage::default(), Instant::at(0));

    assert_eq!(truncated, None);
    assert_eq!(round.unanswered(), [w("a")]);
    assert!(round.take_settled(|_| true).reports.is_empty());

    let again = ReportPage {
        runs: vec![running("t", "r-fresh")],
        last: true,
        ..ReportPage::default()
    };
    round.page(&w("a"), again, Instant::at(5));
    let settled = round.take_settled(|_| true);
    let runs: Vec<_> = settled.reports[&w("a")]
        .runs
        .iter()
        .map(|run| run.claim.task_run_id.clone())
        .collect();
    assert_eq!(runs, [TaskRunId::new("r-fresh")]);
}

#[test]
fn a_repeated_answer_replaces_the_runs_not_yet_handed_over_with_the_newer_complete_one() {
    let mut round = ReconcileRound::new(TERM, [w("a")], Instant::at(0), GRACE);
    let last = |run: &str| ReportPage {
        runs: vec![running("t", run)],
        last: true,
        ..ReportPage::default()
    };
    round.page(&w("a"), last("r1"), Instant::at(0));
    round.page(&w("a"), last("r2"), Instant::at(7));

    let settled = round.take_settled(|_| true);

    let runs: Vec<_> = settled.reports[&w("a")]
        .runs
        .iter()
        .map(|run| run.claim.task_run_id.clone())
        .collect();
    assert_eq!(
        runs,
        [TaskRunId::new("r2")],
        "a run the newer answer lacks is not handed over"
    );
}

#[test]
fn a_first_page_sent_again_does_not_duplicate_its_runs() {
    let mut round = ReconcileRound::new(TERM, [w("a")], Instant::at(0), GRACE);
    let first = || ReportPage {
        runs: vec![running("t", "r1")],
        ..ReportPage::default()
    };
    round.page(&w("a"), first(), Instant::at(0));
    round.page(&w("a"), first(), Instant::at(0));
    round.page(
        &w("a"),
        ReportPage {
            last: true,
            ..ReportPage::default()
        },
        Instant::at(0),
    );

    let settled = round.take_settled(|_| true);

    assert_eq!(settled.reports[&w("a")].runs.len(), 1);
}

#[test]
fn a_re_asked_workers_newer_run_state_replaces_the_earlier_one() {
    let mut round = ReconcileRound::new(TERM, [w("a")], Instant::at(0), GRACE);
    let last = |state: ReportedState| {
        let mut run = running("t", "r1");
        run.state = state;
        ReportPage {
            runs: vec![run],
            last: true,
            ..ReportPage::default()
        }
    };
    round.page(&w("a"), last(ReportedState::Running), Instant::at(0));
    let failed = ReportedState::Failed {
        failure_kind: "crash".into(),
    };
    round.page(&w("a"), last(failed.clone()), Instant::at(7));

    let settled = round.take_settled(|_| true);

    let runs = &settled.reports[&w("a")].runs;
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].state, failed);
}
