//! The one-node scheduler's happy path: submit, claim, start, complete and
//! certify, and who is allowed to do each. The leader is the only authority,
//! and only the worker that claimed a run can report on it.

mod support;

use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, TaskRunId, WorkerId};
use kabudachi_core::protocol::messages::Task;
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::records::TaskRunRecord;
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::{
    Certification, ClaimRejection, ReportRejection, Scheduler, Submission,
};
use kabudachi_core::time::{Duration, Instant};
use support::clock::FakeClock;
use support::ids::SequentialIds;

const DIGEST: &[u8] = b"digest-of-the-result";

fn worker(name: &str) -> WorkerId {
    WorkerId::new(name)
}

struct Fixture {
    clock: FakeClock,
    scheduler: Scheduler<FakeClock, SequentialIds>,
}

/// A scheduler that is already `Leader`, as the runtime leaves it once its
/// election is won.
fn leading() -> Fixture {
    let mut fixture = not_leading();
    fixture.scheduler.set_worker_state(WorkerState::Leader);
    fixture
}

fn not_leading() -> Fixture {
    let clock = FakeClock::new();
    let scheduler = Scheduler::new(clock.clone(), SequentialIds::new());
    Fixture { clock, scheduler }
}

fn submit(scheduler: &mut Scheduler<FakeClock, SequentialIds>) -> TaskId {
    scheduler
        .submit(Submission::new(
            TaskDefinitionId::new("billing.charge"),
            3,
            b"input-bytes".to_vec(),
            "default",
        ))
        .unwrap()
}

fn state_of(scheduler: &Scheduler<FakeClock, SequentialIds>, run: &TaskRunId) -> TaskRunState {
    scheduler.task_run(run).unwrap().current_state()
}

/// Submits one task and claims and starts it as `worker("w1")`.
fn running_task(fixture: &mut Fixture) -> (TaskId, TaskRunId) {
    let task_id = submit(&mut fixture.scheduler);
    let claim = fixture
        .scheduler
        .request_claim(&worker("w1"), &task_id)
        .unwrap();
    fixture
        .scheduler
        .report_started(&worker("w1"), &claim.task_run_id)
        .unwrap();
    (task_id, claim.task_run_id)
}

#[test]
fn a_submitted_task_waits_in_the_queue() {
    let mut fixture = leading();

    let task_id = submit(&mut fixture.scheduler);

    assert_eq!(fixture.scheduler.pending_tasks(), vec![task_id.clone()]);
    let run = fixture.scheduler.run_of(&task_id).unwrap();
    assert_eq!(run.current_state(), TaskRunState::Queued);
    assert_eq!(run.attempt_number(), 1);
}

#[test]
fn every_submission_is_a_distinct_task() {
    let mut fixture = leading();

    let first = submit(&mut fixture.scheduler);
    let second = submit(&mut fixture.scheduler);

    assert_ne!(first, second);
}

#[test]
fn pending_tasks_come_out_in_submission_order() {
    let mut fixture = leading();

    let ids: Vec<TaskId> = (0..5).map(|_| submit(&mut fixture.scheduler)).collect();

    assert_eq!(fixture.scheduler.pending_tasks(), ids);
}

#[test]
fn a_submitted_task_records_the_submission_and_the_time() {
    let mut fixture = leading();
    fixture.clock.advance(Duration::from_ticks(40));

    let task_id = submit(&mut fixture.scheduler);

    let task: &Task = fixture.scheduler.task(&task_id).unwrap();
    assert_eq!(task.source_version, 3);
    assert_eq!(task.serialized_input, b"input-bytes".to_vec());
    assert_eq!(task.queue, "default");
    assert_eq!(task.created_at_ticks, 40);
    assert_eq!(
        fixture.scheduler.run_of(&task_id).unwrap().created_at_ticks,
        40
    );
}

#[test]
fn a_new_scheduler_refuses_claims_until_told_it_leads() {
    let mut fixture = not_leading();
    let task_id = submit(&mut fixture.scheduler);

    let result = fixture.scheduler.request_claim(&worker("w1"), &task_id);

    assert_eq!(result.unwrap_err(), ClaimRejection::NotLeader);
}

#[test]
fn a_claim_is_refused_in_every_state_but_leader() {
    for state in WorkerState::ALL
        .into_iter()
        .filter(|s| *s != WorkerState::Leader)
    {
        let mut fixture = not_leading();
        fixture.scheduler.set_worker_state(state);
        let task_id = submit(&mut fixture.scheduler);

        let result = fixture.scheduler.request_claim(&worker("w1"), &task_id);

        assert_eq!(result.unwrap_err(), ClaimRejection::NotLeader, "{state:?}");
        assert_eq!(fixture.scheduler.pending_tasks(), vec![task_id]);
    }
}

#[test]
fn a_leader_accepts_claims() {
    let mut fixture = leading();
    let task_id = submit(&mut fixture.scheduler);

    let result = fixture.scheduler.request_claim(&worker("w1"), &task_id);

    assert!(result.is_ok());
}

#[test]
fn a_non_leader_does_not_reveal_whether_a_task_exists() {
    let mut fixture = not_leading();

    let result = fixture
        .scheduler
        .request_claim(&worker("w1"), &TaskId::new("no-such-task"));

    assert_eq!(result.unwrap_err(), ClaimRejection::NotLeader);
}

#[test]
fn a_claim_hands_the_task_to_the_worker_and_takes_it_off_the_queue() {
    let mut fixture = leading();
    let task_id = submit(&mut fixture.scheduler);

    let claim = fixture
        .scheduler
        .request_claim(&worker("w1"), &task_id)
        .unwrap();

    assert_eq!(claim.task, *fixture.scheduler.task(&task_id).unwrap());
    assert_eq!(
        state_of(&fixture.scheduler, &claim.task_run_id),
        TaskRunState::Claimed
    );
    let run = fixture.scheduler.task_run(&claim.task_run_id).unwrap();
    assert_eq!(run.selected_worker(), Some(worker("w1")));
    assert!(fixture.scheduler.pending_tasks().is_empty());
}

#[test]
fn only_the_first_of_two_racing_claims_wins() {
    let mut fixture = leading();
    let task_id = submit(&mut fixture.scheduler);

    let winner = fixture.scheduler.request_claim(&worker("w1"), &task_id);
    let loser = fixture.scheduler.request_claim(&worker("w2"), &task_id);

    let winning_run = winner.unwrap().task_run_id;
    assert_eq!(loser.unwrap_err(), ClaimRejection::AlreadySelected);
    let run = fixture.scheduler.task_run(&winning_run).unwrap();
    assert_eq!(run.selected_worker(), Some(worker("w1")));
}

#[test]
fn claiming_an_unknown_task_is_refused() {
    let mut fixture = leading();

    let result = fixture
        .scheduler
        .request_claim(&worker("w1"), &TaskId::new("no-such-task"));

    assert_eq!(result.unwrap_err(), ClaimRejection::TaskUnknown);
}

#[test]
fn the_claiming_worker_can_start_its_run() {
    let mut fixture = leading();
    let (_, run_id) = running_task(&mut fixture);

    assert_eq!(state_of(&fixture.scheduler, &run_id), TaskRunState::Running);
}

#[test]
fn another_worker_cannot_start_a_run_it_did_not_claim() {
    let mut fixture = leading();
    let task_id = submit(&mut fixture.scheduler);
    let claim = fixture
        .scheduler
        .request_claim(&worker("w1"), &task_id)
        .unwrap();

    let result = fixture
        .scheduler
        .report_started(&worker("w2"), &claim.task_run_id);

    assert_eq!(result.unwrap_err(), ReportRejection::NotAuthoritative);
    assert_eq!(
        state_of(&fixture.scheduler, &claim.task_run_id),
        TaskRunState::Claimed
    );
}

#[test]
fn completing_a_run_certifies_its_result() {
    let mut fixture = leading();
    let (task_id, run_id) = running_task(&mut fixture);

    let certification = fixture
        .scheduler
        .complete(&worker("w1"), &run_id, DIGEST.to_vec())
        .unwrap();

    assert_eq!(
        certification,
        Certification {
            task_id,
            task_run_id: run_id.clone(),
            result_digest: DIGEST.to_vec(),
        }
    );
    assert_eq!(
        state_of(&fixture.scheduler, &run_id),
        TaskRunState::Succeeded
    );
    assert_eq!(
        fixture.scheduler.task_run(&run_id).unwrap().result_digest,
        DIGEST.to_vec()
    );
}

#[test]
fn a_run_is_stamped_with_the_time_of_each_step() {
    let mut fixture = leading();
    let (_, run_id) = running_task(&mut fixture);
    fixture.clock.advance(Duration::from_ticks(25));

    fixture
        .scheduler
        .complete(&worker("w1"), &run_id, DIGEST.to_vec())
        .unwrap();

    let run = fixture.scheduler.task_run(&run_id).unwrap();
    assert_eq!(run.created_at_ticks, 0);
    assert_eq!(run.updated_at_ticks, Instant::at(25).as_ticks());
}

#[test]
fn a_second_completion_of_the_same_run_is_refused() {
    let mut fixture = leading();
    let (_, run_id) = running_task(&mut fixture);
    fixture
        .scheduler
        .complete(&worker("w1"), &run_id, DIGEST.to_vec())
        .unwrap();

    let again = fixture
        .scheduler
        .complete(&worker("w1"), &run_id, b"different".to_vec());

    assert_eq!(again.unwrap_err(), ReportRejection::NotAuthoritative);
    assert_eq!(
        fixture.scheduler.task_run(&run_id).unwrap().result_digest,
        DIGEST.to_vec()
    );
}

#[test]
fn a_worker_cannot_complete_a_run_it_did_not_claim() {
    let mut fixture = leading();
    let (_, run_id) = running_task(&mut fixture);

    let result = fixture
        .scheduler
        .complete(&worker("w2"), &run_id, DIGEST.to_vec());

    assert_eq!(result.unwrap_err(), ReportRejection::NotAuthoritative);
    assert_eq!(state_of(&fixture.scheduler, &run_id), TaskRunState::Running);
}

#[test]
fn a_run_that_never_started_cannot_be_completed() {
    let mut fixture = leading();
    let task_id = submit(&mut fixture.scheduler);
    let claim = fixture
        .scheduler
        .request_claim(&worker("w1"), &task_id)
        .unwrap();

    let result = fixture
        .scheduler
        .complete(&worker("w1"), &claim.task_run_id, DIGEST.to_vec());

    assert_eq!(result.unwrap_err(), ReportRejection::NotAuthoritative);
    assert_eq!(
        state_of(&fixture.scheduler, &claim.task_run_id),
        TaskRunState::Claimed
    );
}

#[test]
fn reporting_on_an_unknown_run_is_refused() {
    let mut fixture = leading();

    let started = fixture
        .scheduler
        .report_started(&worker("w1"), &TaskRunId::new("no-such-run"));
    let completed = fixture.scheduler.complete(
        &worker("w1"),
        &TaskRunId::new("no-such-run"),
        DIGEST.to_vec(),
    );

    assert_eq!(started.unwrap_err(), ReportRejection::UnknownRun);
    assert_eq!(completed.unwrap_err(), ReportRejection::UnknownRun);
}

#[test]
fn a_scheduler_that_lost_leadership_certifies_nothing() {
    let mut fixture = leading();
    let (_, run_id) = running_task(&mut fixture);

    fixture.scheduler.set_worker_state(WorkerState::Fenced);
    let result = fixture
        .scheduler
        .complete(&worker("w1"), &run_id, DIGEST.to_vec());

    assert_eq!(result.unwrap_err(), ReportRejection::NotLeader);
    assert_eq!(state_of(&fixture.scheduler, &run_id), TaskRunState::Running);
}

#[test]
fn a_submitted_task_never_changes_as_its_run_progresses() {
    let mut fixture = leading();
    let task_id = submit(&mut fixture.scheduler);
    let before = fixture.scheduler.task(&task_id).unwrap().clone();

    let claim = fixture
        .scheduler
        .request_claim(&worker("w1"), &task_id)
        .unwrap();
    fixture
        .scheduler
        .report_started(&worker("w1"), &claim.task_run_id)
        .unwrap();
    fixture
        .scheduler
        .complete(&worker("w1"), &claim.task_run_id, DIGEST.to_vec())
        .unwrap();

    assert_eq!(*fixture.scheduler.task(&task_id).unwrap(), before);
}

#[test]
fn a_scheduler_that_lost_leadership_starts_nothing() {
    let mut fixture = leading();
    let task_id = submit(&mut fixture.scheduler);
    let claim = fixture
        .scheduler
        .request_claim(&worker("w1"), &task_id)
        .unwrap();

    fixture.scheduler.set_worker_state(WorkerState::Fenced);
    let result = fixture
        .scheduler
        .report_started(&worker("w1"), &claim.task_run_id);

    assert_eq!(result.unwrap_err(), ReportRejection::NotLeader);
    assert_eq!(
        state_of(&fixture.scheduler, &claim.task_run_id),
        TaskRunState::Claimed
    );
}

#[test]
fn claiming_a_task_from_the_middle_leaves_the_others_queued_in_order() {
    let mut fixture = leading();
    let first = submit(&mut fixture.scheduler);
    let middle = submit(&mut fixture.scheduler);
    let last = submit(&mut fixture.scheduler);

    let claim = fixture
        .scheduler
        .request_claim(&worker("w1"), &middle)
        .unwrap();

    assert_eq!(claim.task, *fixture.scheduler.task(&middle).unwrap());
    assert_eq!(fixture.scheduler.pending_tasks(), vec![first, last]);
}

#[test]
fn each_step_of_a_run_stamps_its_own_time() {
    let mut fixture = leading();
    let task_id = submit(&mut fixture.scheduler);
    let updated_at = |fixture: &Fixture, run: &TaskRunId| {
        fixture.scheduler.task_run(run).unwrap().updated_at_ticks
    };

    fixture.clock.advance(Duration::from_ticks(10));
    let claim = fixture
        .scheduler
        .request_claim(&worker("w1"), &task_id)
        .unwrap();
    assert_eq!(updated_at(&fixture, &claim.task_run_id), 10);

    fixture.clock.advance(Duration::from_ticks(10));
    fixture
        .scheduler
        .report_started(&worker("w1"), &claim.task_run_id)
        .unwrap();
    assert_eq!(updated_at(&fixture, &claim.task_run_id), 20);

    fixture.clock.advance(Duration::from_ticks(10));
    fixture
        .scheduler
        .complete(&worker("w1"), &claim.task_run_id, DIGEST.to_vec())
        .unwrap();
    assert_eq!(updated_at(&fixture, &claim.task_run_id), 30);
}

#[test]
fn claiming_the_oldest_takes_them_in_order_up_to_the_limit() {
    let mut fixture = leading();
    let ids: Vec<TaskId> = (0..5).map(|_| submit(&mut fixture.scheduler)).collect();

    let claims = fixture.scheduler.claim_oldest(&worker("w1"), 2).unwrap();

    let claimed: Vec<TaskId> = claims.iter().map(|claim| claim.task.task_id()).collect();
    assert_eq!(claimed, ids[..2].to_vec());
    assert_eq!(fixture.scheduler.pending_tasks(), ids[2..].to_vec());
}

#[test]
fn claiming_the_oldest_gives_each_run_to_the_claiming_worker() {
    let mut fixture = leading();
    submit(&mut fixture.scheduler);
    submit(&mut fixture.scheduler);

    let claims = fixture.scheduler.claim_oldest(&worker("w1"), 10).unwrap();

    assert_eq!(claims.len(), 2);
    for claim in claims {
        let run = fixture.scheduler.task_run(&claim.task_run_id).unwrap();
        assert_eq!(run.current_state(), TaskRunState::Claimed);
        assert_eq!(run.selected_worker(), Some(worker("w1")));
    }
}

#[test]
fn claiming_more_than_is_pending_takes_everything_and_zero_takes_nothing() {
    let mut fixture = leading();
    let ids: Vec<TaskId> = (0..3).map(|_| submit(&mut fixture.scheduler)).collect();

    assert!(
        fixture
            .scheduler
            .claim_oldest(&worker("w1"), 0)
            .unwrap()
            .is_empty()
    );
    assert_eq!(fixture.scheduler.pending_tasks(), ids);
    assert_eq!(
        fixture
            .scheduler
            .claim_oldest(&worker("w1"), 50)
            .unwrap()
            .len(),
        3
    );
    assert!(fixture.scheduler.pending_tasks().is_empty());
}

#[test]
fn claiming_with_nothing_pending_takes_nothing() {
    let mut fixture = leading();

    assert!(
        fixture
            .scheduler
            .claim_oldest(&worker("w1"), 10)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn a_scheduler_that_does_not_lead_claims_nothing() {
    let mut fixture = not_leading();
    let id = submit(&mut fixture.scheduler);

    let result = fixture.scheduler.claim_oldest(&worker("w1"), 10);

    assert_eq!(result.unwrap_err(), ClaimRejection::NotLeader);
    assert_eq!(fixture.scheduler.pending_tasks(), vec![id]);
}
