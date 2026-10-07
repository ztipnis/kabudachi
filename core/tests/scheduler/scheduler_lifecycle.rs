//! The one-node scheduler's happy path: submit, claim, start, complete and
//! certify, and who is allowed to do each. The leader is the only authority,
//! and only the worker that claimed a run can report on it.

use crate::support::grant::unbounded_grant;
use crate::support::scheduler::{Fixture, TestScheduler};
use crate::support::spy::Noted;
use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, TaskRunId, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::records::TaskRunRecord;
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::scheduler::{
    CancelRejection, ClaimRejection, Completion, LeadershipGrant, LeaseEnd, LoseRejection,
    ReportRejection, Submission,
};
use kabudachi_core::time::{Clock, Duration};

const RESULT: &[u8] = b"the-result";

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

/// The digest the scheduler holds on `run`, read back from its record.
fn stored_digest(fixture: &Fixture, run: &TaskRunId) -> Option<Digest> {
    let run = fixture.scheduler.task_run(run).unwrap();
    run.result_digest
        .as_ref()
        .map(|digest| Digest::try_from(digest).unwrap())
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
    assert_eq!(fixture.spy.pending(), 1);
    assert_eq!(fixture.state(&at), TaskRunState::Queued);
}

#[test]
fn every_decision_needs_a_live_grant_and_changes_nothing_without_one() {
    let mut fixture = Fixture::leading();
    let queued = submit(&mut fixture.scheduler);
    let (_, running) = running_task(&mut fixture);
    let claimed_task = submit(&mut fixture.scheduler);
    let claimed = fixture
        .scheduler
        .request_claim(&worker("w1"), &claimed_task)
        .unwrap()
        .task_run_id;
    fixture.scheduler.set_leadership_grant(None);
    let mark = fixture.spy.mark();

    let s = &mut fixture.scheduler;
    let not_leader = Some(ReportRejection::NotLeader);
    let refused = [
        (
            "claim",
            s.request_claim(&worker("w2"), &queued).err() == Some(ClaimRejection::NotLeader),
        ),
        (
            "claim of an unknown task, which the leader check precedes",
            s.request_claim(&worker("w2"), &TaskId::new("no-such-task"))
                .err()
                == Some(ClaimRejection::NotLeader),
        ),
        (
            "claim_oldest",
            s.claim_oldest(&worker("w2"), 10).err() == Some(ClaimRejection::NotLeader),
        ),
        (
            "start",
            s.report_started(&worker("w1"), &claimed).err() == not_leader,
        ),
        (
            "complete",
            s.complete(&worker("w1"), &running, Digest::blake3(RESULT), Completion::Final)
                .err()
                == not_leader,
        ),
        (
            "fail",
            s.fail(&worker("w1"), &running, "ValueError").err() == not_leader,
        ),
        (
            "cancel",
            s.cancel(&queued).err() == Some(CancelRejection::NotLeader),
        ),
        (
            "lose_worker",
            s.lose_worker(&worker("w1")).err() == Some(LoseRejection::NotLeader),
        ),
    ];

    for (decision, was_refused) in refused {
        assert!(was_refused, "{decision} was not refused as NotLeader");
    }
    assert_eq!(fixture.run_state(&running), TaskRunState::Running);
    assert_eq!(fixture.run_state(&claimed), TaskRunState::Claimed);
    assert_eq!(fixture.state(&queued), TaskRunState::Queued);
    assert_eq!(fixture.spy.pending(), 1);
    assert!(
        fixture.spy.since(mark).is_empty(),
        "a refused decision changes nothing"
    );
}

#[test]
fn only_the_claiming_worker_may_report_and_only_in_order() {
    let mut fixture = Fixture::leading();
    let (_, running) = running_task(&mut fixture);
    let claimed_task = submit(&mut fixture.scheduler);
    let claimed = fixture
        .scheduler
        .request_claim(&worker("w1"), &claimed_task)
        .unwrap()
        .task_run_id;
    let (_, succeeded) = running_task(&mut fixture);
    fixture
        .scheduler
        .complete(&worker("w1"), &succeeded, Digest::blake3(RESULT), Completion::Final)
        .unwrap();
    let (_, failed) = running_task(&mut fixture);
    fixture
        .scheduler
        .fail(&worker("w1"), &failed, "ValueError")
        .unwrap();
    let lost_task = submit(&mut fixture.scheduler);
    let lost = fixture
        .scheduler
        .request_claim(&worker("w3"), &lost_task)
        .unwrap()
        .task_run_id;
    fixture
        .scheduler
        .report_started(&worker("w3"), &lost)
        .unwrap();
    fixture.scheduler.lose_worker(&worker("w3")).unwrap();
    let other = Digest::blake3(b"different");

    let s = &mut fixture.scheduler;
    let refused = [
        (
            "a second completion",
            s.complete(&worker("w1"), &succeeded, other.clone(), Completion::Final).err(),
        ),
        (
            "a failure after a completion",
            s.fail(&worker("w1"), &succeeded, "ValueError").err(),
        ),
        (
            "a completion by a worker that did not claim the run",
            s.complete(&worker("w2"), &running, other.clone(), Completion::Final).err(),
        ),
        (
            "a failure by a worker that did not claim the run",
            s.fail(&worker("w2"), &running, "ValueError").err(),
        ),
        (
            "a start by a worker that did not claim the run",
            s.report_started(&worker("w2"), &claimed).err(),
        ),
        (
            "a completion of a run that never started",
            s.complete(&worker("w1"), &claimed, other.clone(), Completion::Final).err(),
        ),
        (
            "a failure of a run that never started",
            s.fail(&worker("w1"), &claimed, "ValueError").err(),
        ),
        (
            "a completion after a failure",
            s.complete(&worker("w1"), &failed, other.clone(), Completion::Final).err(),
        ),
        (
            "a second failure",
            s.fail(&worker("w1"), &failed, "KeyError").err(),
        ),
        (
            "a completion by the worker that was lost",
            s.complete(&worker("w3"), &lost, other.clone(), Completion::Final).err(),
        ),
        (
            "a failure by the worker that was lost",
            s.fail(&worker("w3"), &lost, "ValueError").err(),
        ),
    ];

    for (report, rejection) in refused {
        assert_eq!(
            rejection,
            Some(ReportRejection::NotAuthoritative),
            "{report}"
        );
    }
    assert_eq!(fixture.run_state(&running), TaskRunState::Running);
    assert_eq!(fixture.run_state(&claimed), TaskRunState::Claimed);
    assert_eq!(fixture.run_state(&succeeded), TaskRunState::Succeeded);
    assert_eq!(
        stored_digest(&fixture, &succeeded),
        Some(Digest::blake3(RESULT))
    );
    assert_eq!(fixture.run_state(&failed), TaskRunState::Failed);
    assert_eq!(
        fixture.scheduler.task_run(&failed).unwrap().failure_kind,
        "ValueError"
    );
    assert_eq!(fixture.run_state(&lost), TaskRunState::Lost);
}

#[test]
fn the_queue_hands_out_in_order_and_up_to_the_limit() {
    let mut fixture = Fixture::leading();
    let ids: Vec<TaskId> = (0..6).map(|_| submit(&mut fixture.scheduler)).collect();

    let mark = fixture.spy.mark();
    let claim = fixture
        .scheduler
        .request_claim(&worker("w1"), &ids[2])
        .unwrap();
    assert_eq!(claim.task, fixture.spy.task(&ids[2]));
    let changed: Vec<Noted> = fixture
        .spy
        .since(mark)
        .into_iter()
        .map(|note| note.change)
        .collect();
    assert_eq!(
        changed,
        vec![Noted::Run {
            task: ids[2].clone(),
            run: claim.task_run_id.clone(),
            state: TaskRunState::Claimed,
        }],
        "nothing but the claimed run moved"
    );

    let mark = fixture.spy.mark();
    assert!(
        fixture
            .scheduler
            .claim_oldest(&worker("w1"), 0)
            .unwrap()
            .is_empty()
    );
    assert!(fixture.spy.since(mark).is_empty());

    let claims = fixture.scheduler.claim_oldest(&worker("w2"), 2).unwrap();
    let claimed: Vec<TaskId> = claims.iter().map(|claim| claim.task.task_id()).collect();
    assert_eq!(claimed, vec![ids[0].clone(), ids[1].clone()]);
    assert_eq!(fixture.spy.pending(), 3);
    for claim in claims {
        let run = fixture.scheduler.task_run(&claim.task_run_id).unwrap();
        assert_eq!(run.current_state(), TaskRunState::Claimed);
        assert_eq!(run.selected_worker(), Some(worker("w2")));
    }

    let rest = fixture.scheduler.claim_oldest(&worker("w3"), 50).unwrap();
    let rest: Vec<TaskId> = rest.iter().map(|claim| claim.task.task_id()).collect();
    assert_eq!(
        rest,
        ids[3..].to_vec(),
        "the rest come out in submission order, the middle one skipped"
    );
    assert_eq!(fixture.spy.pending(), 0);
    assert!(
        fixture
            .scheduler
            .claim_oldest(&worker("w3"), 10)
            .unwrap()
            .is_empty()
    );
}
