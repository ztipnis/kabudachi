//! A leading scheduler hands its observer the whole record of every task a
//! call changed, versioned by its epoch, term and revision counter, and
//! publishes nothing once its lease has ended.

use kabudachi_core::coordination_authority::RecoveryEpoch;
use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::generated::TaskRecord;
use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::records::TaskRunRecord;
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::scheduler::{
    Completion, Event, LeadershipGrant, LeaseEnd, MAX_SUBMISSION_BYTES, Submission, SubmitRejection,
};
use kabudachi_core::task_record::{HISTORY_TOO_LARGE_FAILURE_KIND, MAX_RECORD_BYTES, RecordVersion};
use kabudachi_core::time::Instant;
use prost::Message;

use crate::support::grant::unbounded_grant;
use crate::support::scheduler::{Fixture, ticks};

fn plain(input: &[u8]) -> Submission {
    Submission::new(TaskDefinitionId::new("billing.charge"), 0, input.to_vec(), "default")
}

fn worker() -> WorkerId {
    WorkerId::new("w1")
}

fn version_of(record: &TaskRecord) -> RecordVersion {
    RecordVersion::from(record.version.as_ref().expect("every revision is versioned"))
}

fn task_of(record: &TaskRecord) -> TaskId {
    record.task.as_ref().expect("every revision carries its task").task_id()
}

#[test]
fn a_submission_publishes_one_whole_record_as_the_first_revision_of_the_term() {
    let mut fixture = Fixture::leading();
    fixture.clock.set_wall_clock_millis(1_700_000_000_000);

    let task = fixture.scheduler.submit(plain(b"in").with_delay(ticks(100))).unwrap();

    let [record] = fixture.spy.revisions().try_into().expect("exactly one revision");
    assert_eq!(
        version_of(&record),
        RecordVersion { recovery_epoch: RecoveryEpoch::new(0, 0), leader_term: 1, revision: 0 }
    );
    assert_eq!(task_of(&record), task);
    assert_eq!(record.runs.len(), 1);
    assert_eq!(record.runs[0].current_state(), TaskRunState::Scheduled);
    assert_eq!(
        record.input_digest.as_ref().map(|digest| Digest::try_from(digest).unwrap()),
        Some(Digest::blake3(b"in"))
    );
    assert_eq!(record.published_at.map(|at| at.unix_millis), Some(1_700_000_000_000));
    assert!(!record.finished);
    assert!(record.placement.is_empty(), "the scheduler does not choose where a record is written");
}

#[test]
fn a_failure_with_retries_left_publishes_the_failed_and_the_new_run_in_one_revision() {
    let mut fixture = Fixture::leading();
    let task = fixture.scheduler.submit(plain(b"in").with_retries(1)).unwrap();
    let claim = fixture.scheduler.request_claim(&worker(), &task).unwrap();
    fixture.scheduler.report_started(&worker(), &claim.task_run_id).unwrap();
    let before = fixture.spy.revisions().len();

    fixture.scheduler.fail(&worker(), &claim.task_run_id, "ValueError").unwrap();

    let revisions = fixture.spy.revisions();
    let [record] = &revisions[before..] else {
        panic!("one revision for the failure, got {}", revisions.len() - before);
    };
    let states: Vec<TaskRunState> = record.runs.iter().map(TaskRunRecord::current_state).collect();
    assert_eq!(states, [TaskRunState::Failed, TaskRunState::Queued]);
    assert_eq!(version_of(record).revision, 3, "submit, claim and start came first");
}

#[test]
fn revisions_count_up_within_a_term_and_start_again_in_the_next() {
    let mut fixture = Fixture::leading();
    let first = fixture.scheduler.submit(plain(b"a")).unwrap();
    fixture.scheduler.set_leadership_grant(None);
    fixture.scheduler.set_leadership_grant(Some(unbounded_grant()));
    fixture.scheduler.request_claim(&worker(), &first).unwrap();
    fixture.scheduler.set_leadership_grant(Some(LeadershipGrant { term: 2, ..unbounded_grant() }));

    fixture.scheduler.submit(plain(b"b")).unwrap();

    let versions: Vec<(u64, u64)> = fixture
        .spy
        .revisions()
        .iter()
        .map(|record| (version_of(record).leader_term, version_of(record).revision))
        .collect();
    assert_eq!(versions, [(1, 0), (1, 1), (2, 0)], "a grant of the same term handed back continues its count");
}

#[test]
fn a_grant_of_an_older_term_than_one_published_in_does_not_let_the_scheduler_act() {
    let mut fixture = Fixture::not_leading();
    let term = |term| Some(LeadershipGrant { term, ..unbounded_grant() });
    fixture.scheduler.set_leadership_grant(term(2));
    let kept = fixture.scheduler.submit(plain(b"a")).unwrap();
    fixture.scheduler.set_leadership_grant(None);

    fixture.scheduler.set_leadership_grant(term(1));

    assert!(!fixture.scheduler.is_leader(), "a version under term 1 would repeat one published under term 2");
    let minted = fixture.scheduler.mint(plain(b"b"));
    assert_eq!(fixture.scheduler.submit_minted(minted.clone()), Err(SubmitRejection::NotLeader));
    assert!(fixture.scheduler.runs_of(&minted.task_id).is_empty(), "nothing was changed in memory");
    assert!(fixture.scheduler.request_claim(&worker(), &kept).is_err());
    assert_eq!(fixture.spy.revisions().len(), 1);

    fixture.scheduler.set_leadership_grant(term(2));
    assert!(fixture.scheduler.is_leader());
    fixture.scheduler.request_claim(&worker(), &kept).unwrap();
    let versions: Vec<(u64, u64)> = fixture
        .spy
        .revisions()
        .iter()
        .map(|record| (version_of(record).leader_term, version_of(record).revision))
        .collect();
    assert_eq!(versions, [(2, 0), (2, 1)], "the same term carries on counting where it stopped");
}

#[test]
fn a_task_id_asked_for_again_answers_the_id_and_records_nothing_more() {
    let mut fixture = Fixture::leading();
    let minted = fixture.scheduler.mint(plain(b"a"));
    fixture.scheduler.submit_minted(minted.clone()).unwrap();
    let revisions = fixture.spy.revisions().len();

    let mut again = fixture.scheduler.mint(plain(b"other"));
    again.task_id = minted.task_id.clone();

    assert_eq!(fixture.scheduler.submit_minted(again), Ok(minted.task_id.clone()));
    assert_eq!(fixture.scheduler.runs_of(&minted.task_id).len(), 1, "the first recording stands untouched");
    assert_eq!(fixture.spy.revisions().len(), revisions, "nothing changed, nothing published");
}

#[test]
fn nothing_is_recorded_or_published_once_the_lease_has_ended() {
    let mut fixture = Fixture::not_leading();
    fixture.scheduler.set_leadership_grant(Some(LeadershipGrant {
        valid_until: LeaseEnd::At(Instant::at(10)),
        ..unbounded_grant()
    }));
    let first = fixture.scheduler.submit(plain(b"a")).unwrap();
    fixture.clock.advance(ticks(10));

    let minted = fixture.scheduler.mint(plain(b"b"));
    assert_eq!(fixture.scheduler.submit_minted(minted.clone()), Err(SubmitRejection::NotLeader));
    assert!(fixture.scheduler.request_claim(&worker(), &first).is_err());
    fixture.scheduler.catch_up();
    assert_eq!(fixture.spy.revisions().len(), 1, "only the submission made while the lease ran");

    fixture.scheduler.set_leadership_grant(Some(unbounded_grant()));
    assert_eq!(fixture.spy.revisions().len(), 1, "a refused submission leaves nothing to publish later");
    assert_eq!(fixture.scheduler.submit_minted(minted.clone()), Ok(minted.task_id));
    assert_eq!(fixture.spy.revisions().len(), 2);
}

#[test]
fn a_task_that_finishes_publishes_the_generations_it_absorbed_as_finished_too() {
    let mut fixture = Fixture::leading();
    fixture.scheduler.set_result_ttl(Some(ticks(100)));
    let older = fixture.scheduler.submit(plain(b"old").with_coalescing_key("k")).unwrap();
    let newer = fixture.scheduler.submit(plain(b"new").with_coalescing_key("k")).unwrap();
    let claim = fixture.scheduler.request_claim(&worker(), &newer).unwrap();
    fixture.scheduler.report_started(&worker(), &claim.task_run_id).unwrap();

    fixture
        .scheduler
        .complete(&worker(), &claim.task_run_id, Digest::blake3(b"done"), Completion::Final)
        .unwrap();

    let revisions = fixture.spy.revisions();
    let last_of = |task: &TaskId| {
        revisions.iter().rfind(|record| task_of(record) == *task).unwrap_or_else(|| panic!("no revision of {task:?}"))
    };
    assert!(last_of(&newer).finished);
    assert!(last_of(&older).finished, "the absorbed generation ends with the generation that finished");

    fixture.clock.advance(ticks(100));
    assert_eq!(fixture.scheduler.catch_up().forgotten, 2);
    assert!(fixture.scheduler.runs_of(&older).is_empty());
    assert!(fixture.scheduler.runs_of(&newer).is_empty());
    let published = fixture.spy.revisions().len();
    fixture.scheduler.catch_up();
    assert_eq!(fixture.spy.revisions().len(), published, "forgetting publishes nothing more");
}

#[test]
fn the_revision_that_ends_a_task_says_so_and_a_continuation_ends_it_later() {
    let mut fixture = Fixture::leading();
    let task = fixture.scheduler.submit(plain(b"a")).unwrap();
    let claim = fixture.scheduler.request_claim(&worker(), &task).unwrap();
    fixture.scheduler.report_started(&worker(), &claim.task_run_id).unwrap();

    fixture
        .scheduler
        .complete(&worker(), &claim.task_run_id, Digest::blake3(b"step"), Completion::Continues)
        .unwrap();
    let certified = fixture.spy.revisions().last().cloned().unwrap();
    assert!(!certified.finished, "a continuing task is not over");

    assert_eq!(fixture.scheduler.end_continuation(&task), Ok(true));
    let ended = fixture.spy.revisions().last().cloned().unwrap();
    assert!(ended.finished);
    assert_eq!(version_of(&ended).revision, version_of(&certified).revision + 1);
}

#[test]
fn a_minted_submission_keeps_its_id_and_counts_its_delay_from_when_it_was_minted() {
    let mut fixture = Fixture::not_leading();
    let submitted = fixture.scheduler.mint(plain(b"a").with_delay(ticks(100)));
    assert!(fixture.scheduler.submit_minted(submitted.clone()).is_err());
    assert!(fixture.spy.revisions().is_empty());

    fixture.clock.advance(ticks(30));
    fixture.scheduler.set_leadership_grant(Some(unbounded_grant()));
    let task = fixture.scheduler.submit_minted(submitted.clone()).unwrap();

    assert_eq!(task, submitted.task_id);
    fixture.clock.advance(ticks(69));
    assert_eq!(fixture.scheduler.catch_up().queued, 0);
    fixture.clock.advance(ticks(1));
    assert_eq!(fixture.scheduler.catch_up().queued, 1, "due 100 ms after minting, 30 of which passed before the grant");
}

#[test]
fn a_forward_wall_clock_jump_never_releases_a_delayed_submission_early() {
    let mut fixture = Fixture::leading();
    fixture.clock.set_wall_clock_millis(10_000);
    let submitted = fixture.scheduler.mint(plain(b"a").with_delay(ticks(100)));

    fixture.clock.set_wall_clock_millis(10_000 + 5_000);
    fixture.scheduler.submit_minted(submitted).unwrap();

    assert_eq!(fixture.scheduler.catch_up().queued, 0, "the wall clock does not shorten the wait");
    fixture.clock.advance(ticks(99));
    assert_eq!(fixture.scheduler.catch_up().queued, 0);
    fixture.clock.advance(ticks(1));
    assert_eq!(fixture.scheduler.catch_up().queued, 1);
}

#[test]
fn a_submission_whose_record_would_pass_the_limit_is_refused_before_anything_is_recorded() {
    let mut fixture = Fixture::leading();
    let half = (MAX_RECORD_BYTES / 2) as usize;
    fixture.scheduler.submit(plain(&vec![1; half]).with_coalescing_key("k")).unwrap();
    let before = fixture.spy.revisions().len();

    let refused = fixture.scheduler.submit(plain(&vec![2; half]).with_coalescing_key("k"));

    assert!(matches!(refused, Err(SubmitRejection::RecordTooLarge { .. })), "{refused:?}");
    assert_eq!(fixture.spy.revisions().len(), before, "nothing changed, so nothing was published");
}

/// Submits a task whose record has room for only a few more runs, and
/// returns it. The record is brought close to its limit through the
/// coalescing chain, which carries the superseded generation's input: a first
/// generation with a near-largest input, then a second one of the same key
/// sized from the first's published record. The few runs that still fit then
/// fill the rest.
fn a_task_with_room_for_a_few_more_runs(fixture: &mut Fixture, retries: u32) -> TaskId {
    let first = fixture
        .scheduler
        .submit(plain(&vec![0; MAX_SUBMISSION_BYTES as usize - 64]).with_coalescing_key("k"))
        .unwrap();
    let first_len = fixture.spy.revisions_of(&first).last().unwrap().encoded_len();
    // Room left beside the first generation's record, less what the chain
    // entry, the run and the record's reserve take, and a few runs' margin.
    let mut input_len = (MAX_RECORD_BYTES as usize).saturating_sub(first_len + 4 * 1024);
    loop {
        let second = plain(&vec![0; input_len]).with_coalescing_key("k").with_retries(retries);
        match fixture.scheduler.submit(second) {
            Ok(task) => return task,
            Err(SubmitRejection::RecordTooLarge { size, limit }) => {
                input_len -= (size - limit) as usize + 512;
            }
            Err(other) => panic!("the second generation was refused: {other:?}"),
        }
    }
}

#[test]
fn a_replay_that_would_outgrow_the_record_is_not_created_and_the_task_is_over() {
    let mut fixture = Fixture::leading();
    let task = a_task_with_room_for_a_few_more_runs(&mut fixture, 0);
    let mut replays = 0;
    while let Ok(_claim) = fixture.scheduler.request_claim(&worker(), &task) {
        let lost = fixture.scheduler.lose_worker(&worker()).unwrap();
        if lost[0].replayed.is_none() {
            break;
        }
        replays += 1;
        assert!(replays < 100, "replays never stopped");
    }

    assert!(replays > 0, "some replays fit before the record filled");
    assert!(fixture.scheduler.take_events().contains(&Event::RecordFull { task_id: task.clone() }));
    let last = fixture.spy.revisions().into_iter().rfind(|r| task_of(r) == task).unwrap();
    assert!(last.finished);
    assert!(last.encoded_len() as u64 <= MAX_RECORD_BYTES);
    assert_eq!(last.runs.last().unwrap().failure_kind, HISTORY_TOO_LARGE_FAILURE_KIND);
}

#[test]
fn a_retry_that_would_outgrow_the_record_is_not_created_and_the_failure_stands() {
    let mut fixture = Fixture::leading();
    let task = a_task_with_room_for_a_few_more_runs(&mut fixture, 100_000);
    let mut retries = 0;
    loop {
        let claim = fixture.scheduler.request_claim(&worker(), &task).unwrap();
        fixture.scheduler.report_started(&worker(), &claim.task_run_id).unwrap();
        let failure = fixture.scheduler.fail(&worker(), &claim.task_run_id, "Boom").unwrap();
        if failure.retry.is_none() {
            break;
        }
        retries += 1;
        assert!(retries < 100, "retries never stopped");
    }

    assert!(retries > 0, "some retries fit before the record filled");
    assert!(fixture.scheduler.take_events().contains(&Event::RecordFull { task_id: task.clone() }));
    let last = fixture.spy.revisions().into_iter().rfind(|r| task_of(r) == task).unwrap();
    assert!(last.finished);
    assert!(last.encoded_len() as u64 <= MAX_RECORD_BYTES);
    assert_eq!(last.runs.last().unwrap().failure_kind, "Boom");
}

#[test]
fn a_failure_kind_too_long_for_the_record_is_cut_so_the_record_stays_within_the_limit() {
    let mut fixture = Fixture::leading();
    let task = fixture.scheduler.submit(plain(b"in")).unwrap();
    let claim = fixture.scheduler.request_claim(&worker(), &task).unwrap();
    fixture.scheduler.report_started(&worker(), &claim.task_run_id).unwrap();
    let kind = "K".repeat(MAX_RECORD_BYTES as usize);

    fixture.scheduler.fail(&worker(), &claim.task_run_id, kind.as_str()).unwrap();

    let last = fixture.spy.revisions().into_iter().rfind(|r| task_of(r) == task).unwrap();
    assert!(last.encoded_len() as u64 <= MAX_RECORD_BYTES);
    let stored = &last.runs.last().unwrap().failure_kind;
    assert!(!stored.is_empty() && kind.starts_with(stored.as_str()), "the kind is cut, not replaced");
}
