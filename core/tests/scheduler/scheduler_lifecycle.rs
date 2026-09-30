//! The one-node scheduler's happy path: submit, claim, start, complete and
//! certify, and who is allowed to do each. The leader is the only authority,
//! and only the worker that claimed a run can report on it.

use crate::support::grant::unbounded_grant;
use crate::support::scheduler::{Fixture, TestScheduler};
use crate::support::spy::Noted;
use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, TaskRunId, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::messages::Task;
use kabudachi_core::protocol::records::TaskRunRecord;
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::scheduler::{
    Certification, ClaimRejection, Completion, LeadershipGrant, LeaseEnd, ReportRejection,
    Submission,
};
use kabudachi_core::time::{Clock, Duration};

const DIGEST: &[u8] = b"digest-of-the-result";

fn worker(name: &str) -> WorkerId {
    WorkerId::new(name)
}

fn submit(scheduler: &mut TestScheduler) -> TaskId {
    scheduler
        .submit(Submission::new(
            TaskDefinitionId::new("billing.charge"),
            3,
            b"input-bytes".to_vec(),
            "default",
        ))
        .unwrap()
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
    let mut fixture = Fixture::leading();

    let task_id = submit(&mut fixture.scheduler);

    assert_eq!(fixture.spy.pending(), 1);
    let run = fixture.spy.run_of(&task_id);
    assert_eq!(run.current_state(), TaskRunState::Queued);
    assert_eq!(run.attempt_number(), 1);
    assert_eq!(run.parent_task_run_id(), None);
}

#[test]
fn a_submitted_task_records_the_submission_and_the_time() {
    let mut fixture = Fixture::leading();
    fixture.clock.advance(Duration::from_ticks(40));

    let task_id = submit(&mut fixture.scheduler);

    let task: Task = fixture.spy.task(&task_id);
    assert_eq!(
        task.task_definition_id(),
        TaskDefinitionId::new("billing.charge")
    );
    assert_eq!(task.source_version, 3);
    assert_eq!(task.serialized_input, b"input-bytes".to_vec());
    assert_eq!(task.queue, "default");
    assert_eq!(task.created_at_ticks, 40);
    let run = fixture.spy.run_of(&task_id);
    assert_eq!(run.created_at_ticks, 40);
    assert_eq!(run.source_version, 3);
    assert_eq!(run.execution_version, 3);
}

#[test]
fn a_new_scheduler_refuses_claims_until_told_it_leads() {
    let mut fixture = Fixture::not_leading();
    let task_id = submit(&mut fixture.scheduler);

    let result = fixture.scheduler.request_claim(&worker("w1"), &task_id);

    assert_eq!(result.unwrap_err(), ClaimRejection::NotLeader);
    // The leader check comes before any lookup, so it reveals nothing about
    // whether a task exists.
    let unknown = fixture
        .scheduler
        .request_claim(&worker("w1"), &TaskId::new("no-such-task"));
    assert_eq!(unknown.unwrap_err(), ClaimRejection::NotLeader);
}

#[test]
fn a_withdrawn_grant_refuses_claims() {
    let mut fixture = Fixture::leading();
    let task_id = submit(&mut fixture.scheduler);
    fixture.scheduler.set_leadership_grant(None);

    let result = fixture.scheduler.request_claim(&worker("w1"), &task_id);

    assert_eq!(result.unwrap_err(), ClaimRejection::NotLeader);
    assert_eq!(fixture.spy.pending(), 1);
    assert_eq!(fixture.state(&task_id), TaskRunState::Queued);
}

#[test]
fn a_grant_lets_claims_through_until_the_schedulers_clock_reaches_its_end() {
    let mut fixture = Fixture::not_leading();
    let end = fixture.clock.now() + Duration::from_ticks(10);
    let grant = LeadershipGrant {
        valid_until: LeaseEnd::At(end),
        ..unbounded_grant()
    };
    fixture.scheduler.set_leadership_grant(Some(grant));
    let (before, at) = (
        submit(&mut fixture.scheduler),
        submit(&mut fixture.scheduler),
    );

    fixture.clock.advance(Duration::from_ticks(9));
    let just_before_the_end = fixture.scheduler.request_claim(&worker("w1"), &before);
    fixture.clock.advance(Duration::from_ticks(1));
    let at_the_end = fixture.scheduler.request_claim(&worker("w1"), &at);

    assert!(just_before_the_end.is_ok(), "{just_before_the_end:?}");
    assert_eq!(at_the_end.unwrap_err(), ClaimRejection::NotLeader);
    assert!(
        !fixture.spy.leading(),
        "the refused claim noticed the lapse and told the observer"
    );
    assert_eq!(fixture.spy.pending(), 1);
    assert_eq!(fixture.state(&at), TaskRunState::Queued);
}

#[test]
fn an_unbounded_grant_never_runs_out() {
    let mut fixture = Fixture::leading();
    let task_id = submit(&mut fixture.scheduler);
    fixture.clock.advance(Duration::from_ticks(u64::MAX / 2));

    let result = fixture.scheduler.request_claim(&worker("w1"), &task_id);

    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn a_claim_hands_the_task_to_the_worker_and_takes_it_off_the_queue() {
    let mut fixture = Fixture::leading();
    let task_id = submit(&mut fixture.scheduler);

    let claim = fixture
        .scheduler
        .request_claim(&worker("w1"), &task_id)
        .unwrap();

    assert_eq!(claim.task, fixture.spy.task(&task_id));
    assert_eq!(fixture.run_state(&claim.task_run_id), TaskRunState::Claimed);
    let run = fixture.scheduler.task_run(&claim.task_run_id).unwrap();
    assert_eq!(run.selected_worker(), Some(worker("w1")));
    assert_eq!(fixture.spy.pending(), 0);
}

#[test]
fn only_the_first_of_two_racing_claims_wins() {
    let mut fixture = Fixture::leading();
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
    let mut fixture = Fixture::leading();

    let result = fixture
        .scheduler
        .request_claim(&worker("w1"), &TaskId::new("no-such-task"));

    assert_eq!(result.unwrap_err(), ClaimRejection::TaskUnknown);
}

#[test]
fn completing_a_run_certifies_its_result() {
    let mut fixture = Fixture::leading();
    let (task_id, run_id) = running_task(&mut fixture);

    let certification = fixture
        .scheduler
        .complete(&worker("w1"), &run_id, DIGEST.to_vec(), Completion::Final)
        .unwrap();

    assert_eq!(
        certification,
        Certification {
            task_id,
            task_run_id: run_id.clone(),
            result_digest: DIGEST.to_vec(),
        }
    );
    assert_eq!(fixture.run_state(&run_id), TaskRunState::Succeeded);
    assert_eq!(
        fixture.scheduler.task_run(&run_id).unwrap().result_digest,
        DIGEST.to_vec()
    );
}

#[test]
fn a_second_completion_of_the_same_run_is_refused() {
    let mut fixture = Fixture::leading();
    let (_, run_id) = running_task(&mut fixture);
    fixture
        .scheduler
        .complete(&worker("w1"), &run_id, DIGEST.to_vec(), Completion::Final)
        .unwrap();

    let again = fixture.scheduler.complete(
        &worker("w1"),
        &run_id,
        b"different".to_vec(),
        Completion::Final,
    );

    assert_eq!(again.unwrap_err(), ReportRejection::NotAuthoritative);
    assert_eq!(
        fixture.scheduler.task_run(&run_id).unwrap().result_digest,
        DIGEST.to_vec()
    );
    // A succeeded run cannot be failed afterwards either.
    let failed = fixture.scheduler.fail(&worker("w1"), &run_id, "ValueError");
    assert_eq!(failed.unwrap_err(), ReportRejection::NotAuthoritative);
    assert_eq!(fixture.run_state(&run_id), TaskRunState::Succeeded);
}

#[test]
fn a_worker_cannot_complete_a_run_it_did_not_claim() {
    let mut fixture = Fixture::leading();
    let (_, run_id) = running_task(&mut fixture);

    let result =
        fixture
            .scheduler
            .complete(&worker("w2"), &run_id, DIGEST.to_vec(), Completion::Final);

    assert_eq!(result.unwrap_err(), ReportRejection::NotAuthoritative);
    assert_eq!(fixture.run_state(&run_id), TaskRunState::Running);
    // Nor can it fail the run it did not claim, or start one claimed by another.
    let failed = fixture.scheduler.fail(&worker("w2"), &run_id, "ValueError");
    assert_eq!(failed.unwrap_err(), ReportRejection::NotAuthoritative);
    assert_eq!(fixture.run_state(&run_id), TaskRunState::Running);
    let task_id = submit(&mut fixture.scheduler);
    let claim = fixture
        .scheduler
        .request_claim(&worker("w1"), &task_id)
        .unwrap();
    let started = fixture
        .scheduler
        .report_started(&worker("w2"), &claim.task_run_id);
    assert_eq!(started.unwrap_err(), ReportRejection::NotAuthoritative);
    assert_eq!(fixture.run_state(&claim.task_run_id), TaskRunState::Claimed);
}

#[test]
fn a_run_that_never_started_cannot_be_completed() {
    let mut fixture = Fixture::leading();
    let task_id = submit(&mut fixture.scheduler);
    let claim = fixture
        .scheduler
        .request_claim(&worker("w1"), &task_id)
        .unwrap();

    let result = fixture.scheduler.complete(
        &worker("w1"),
        &claim.task_run_id,
        DIGEST.to_vec(),
        Completion::Final,
    );

    assert_eq!(result.unwrap_err(), ReportRejection::NotAuthoritative);
    assert_eq!(fixture.run_state(&claim.task_run_id), TaskRunState::Claimed);
}

#[test]
fn reporting_on_an_unknown_run_is_refused() {
    let mut fixture = Fixture::leading();

    let started = fixture
        .scheduler
        .report_started(&worker("w1"), &TaskRunId::new("no-such-run"));
    let completed = fixture.scheduler.complete(
        &worker("w1"),
        &TaskRunId::new("no-such-run"),
        DIGEST.to_vec(),
        Completion::Final,
    );

    assert_eq!(started.unwrap_err(), ReportRejection::UnknownRun);
    assert_eq!(completed.unwrap_err(), ReportRejection::UnknownRun);
}

#[test]
fn a_scheduler_that_lost_leadership_certifies_nothing() {
    let mut fixture = Fixture::leading();
    let (_, run_id) = running_task(&mut fixture);
    let claimed_task = submit(&mut fixture.scheduler);
    let claim = fixture
        .scheduler
        .request_claim(&worker("w1"), &claimed_task)
        .unwrap();

    fixture.scheduler.set_leadership_grant(None);
    let result =
        fixture
            .scheduler
            .complete(&worker("w1"), &run_id, DIGEST.to_vec(), Completion::Final);

    assert_eq!(result.unwrap_err(), ReportRejection::NotLeader);
    assert_eq!(fixture.run_state(&run_id), TaskRunState::Running);
    // A failure starts no retry either, and a claimed run is not started.
    let failed = fixture.scheduler.fail(&worker("w1"), &run_id, "ValueError");
    assert_eq!(failed.unwrap_err(), ReportRejection::NotLeader);
    assert_eq!(fixture.run_state(&run_id), TaskRunState::Running);
    assert_eq!(fixture.spy.pending(), 0);
    let started = fixture
        .scheduler
        .report_started(&worker("w1"), &claim.task_run_id);
    assert_eq!(started.unwrap_err(), ReportRejection::NotLeader);
    assert_eq!(fixture.run_state(&claim.task_run_id), TaskRunState::Claimed);
}

#[test]
fn claiming_a_task_from_the_middle_leaves_the_others_queued_in_order() {
    let mut fixture = Fixture::leading();
    let first = submit(&mut fixture.scheduler);
    let middle = submit(&mut fixture.scheduler);
    let last = submit(&mut fixture.scheduler);

    let mark = fixture.spy.mark();
    let claim = fixture
        .scheduler
        .request_claim(&worker("w1"), &middle)
        .unwrap();

    assert_eq!(claim.task, fixture.spy.task(&middle));
    assert_eq!(fixture.spy.pending(), 2);
    let changed: Vec<Noted> = fixture
        .spy
        .since(mark)
        .into_iter()
        .map(|note| note.change)
        .collect();
    assert_eq!(
        changed,
        vec![Noted::Run {
            task: middle,
            run: claim.task_run_id.clone(),
            state: TaskRunState::Claimed,
        }],
        "nothing but the claimed run moved"
    );
    // Claiming the rest hands out what is left in submission order.
    let rest = fixture.scheduler.claim_oldest(&worker("w2"), 10).unwrap();
    let rest: Vec<TaskId> = rest.iter().map(|claim| claim.task.task_id()).collect();
    assert_eq!(rest, vec![first, last]);
}

#[test]
fn each_step_of_a_run_stamps_its_own_time() {
    let mut fixture = Fixture::leading();
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
        .complete(
            &worker("w1"),
            &claim.task_run_id,
            DIGEST.to_vec(),
            Completion::Final,
        )
        .unwrap();
    assert_eq!(updated_at(&fixture, &claim.task_run_id), 30);
}

#[test]
fn claiming_the_oldest_takes_them_in_order_up_to_the_limit() {
    let mut fixture = Fixture::leading();
    let ids: Vec<TaskId> = (0..5).map(|_| submit(&mut fixture.scheduler)).collect();

    let claims = fixture.scheduler.claim_oldest(&worker("w1"), 2).unwrap();

    let claimed: Vec<TaskId> = claims.iter().map(|claim| claim.task.task_id()).collect();
    assert_eq!(claimed, ids[..2].to_vec());
    assert_eq!(fixture.spy.pending(), 3);
    for claim in claims {
        let run = fixture.scheduler.task_run(&claim.task_run_id).unwrap();
        assert_eq!(run.current_state(), TaskRunState::Claimed);
        assert_eq!(run.selected_worker(), Some(worker("w1")));
    }
    let rest = fixture.scheduler.claim_oldest(&worker("w1"), 10).unwrap();
    let rest: Vec<TaskId> = rest.iter().map(|claim| claim.task.task_id()).collect();
    assert_eq!(
        rest,
        ids[2..].to_vec(),
        "the rest come out in submission order"
    );
}

#[test]
fn claiming_more_than_is_pending_takes_everything_and_zero_takes_nothing() {
    let mut fixture = Fixture::leading();
    for _ in 0..3 {
        submit(&mut fixture.scheduler);
    }

    let mark = fixture.spy.mark();
    assert!(fixture
        .scheduler
        .claim_oldest(&worker("w1"), 0)
        .unwrap()
        .is_empty());
    assert!(fixture.spy.since(mark).is_empty());
    assert_eq!(fixture.spy.pending(), 3);
    assert_eq!(
        fixture
            .scheduler
            .claim_oldest(&worker("w1"), 50)
            .unwrap()
            .len(),
        3
    );
    assert_eq!(fixture.spy.pending(), 0);
    assert!(fixture
        .scheduler
        .claim_oldest(&worker("w1"), 10)
        .unwrap()
        .is_empty());
}

#[test]
fn a_scheduler_that_does_not_lead_claims_nothing() {
    let mut fixture = Fixture::not_leading();
    let id = submit(&mut fixture.scheduler);
    let mark = fixture.spy.mark();

    let result = fixture.scheduler.claim_oldest(&worker("w1"), 10);

    assert_eq!(result.unwrap_err(), ClaimRejection::NotLeader);
    assert!(fixture.spy.since(mark).is_empty());
    assert_eq!(fixture.spy.pending(), 1);
    assert_eq!(fixture.state(&id), TaskRunState::Queued);
}
