//! What a scheduler cannot take yet waits in its backlog, in order, and is
//! handed over once the scheduler leads.

use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::scheduler::composite::{CompositeState, OnError};
use kabudachi_core::scheduler::{
    Backlog, Cancellation, Completion, MemoryLimits, Submission, SubmitRejection,
};

use crate::support::composites::{finished, flow_of, group_of, task_stage};
use crate::support::grant::unbounded_grant;
use crate::support::scheduler::Fixture;

fn worker() -> WorkerId {
    WorkerId::new("w1")
}

fn plain(input: &[u8]) -> Submission {
    Submission::new(TaskDefinitionId::new("billing.charge"), 3, input.to_vec(), "default")
}

fn generation(input: &[u8], key: &str) -> Submission {
    plain(input).with_coalescing_key(key)
}

fn claimed_tasks(fixture: &mut Fixture) -> Vec<TaskId> {
    let claims = fixture.scheduler.claim_oldest(&worker(), 10).unwrap();
    claims.iter().map(|claim| claim.task.task_id()).collect()
}

#[test]
fn submissions_made_before_the_scheduler_leads_are_recorded_in_order_once_it_leads() {
    let mut fixture = Fixture::not_leading();
    fixture.scheduler.set_memory_limits(Some(MemoryLimits { soft: 50, hard: 100 }));
    let mut backlog = Backlog::default();

    let dropped = backlog.submit(&mut fixture.scheduler, plain(&[b'a'; 60])).unwrap();
    assert_eq!(
        backlog.cancel(&mut fixture.scheduler, &dropped),
        Ok(Cancellation::Cancelled { was_running: false })
    );
    let first = backlog.submit(&mut fixture.scheduler, plain(&[b'b'; 30])).unwrap();
    let second = backlog.submit(&mut fixture.scheduler, plain(&[b'c'; 30])).unwrap();
    assert!(
        matches!(
            backlog.submit(&mut fixture.scheduler, plain(&[b'd'; 50])),
            Err(SubmitRejection::Backpressure { .. })
        ),
        "what waits counts against the hard limit; the cancelled one no longer does"
    );
    backlog.hand_over(&mut fixture.scheduler);
    assert!(fixture.forgotten(&first), "nothing is recorded before the scheduler leads");

    fixture.scheduler.set_leadership_grant(Some(unbounded_grant()));
    backlog.hand_over(&mut fixture.scheduler);

    assert_eq!(claimed_tasks(&mut fixture), [first, second]);
    assert!(fixture.forgotten(&dropped));
}

#[test]
fn a_submission_its_record_cannot_hold_yet_waits_with_only_its_own_key() {
    // Each payload fits a record alone, but a record that also carries the
    // superseded generation's input does not.
    let half = vec![b'x'; 557_056];
    let mut fixture = Fixture::not_leading();
    fixture
        .scheduler
        .set_memory_limits(Some(MemoryLimits { soft: 1_800_000, hard: 1_900_000 }));
    let mut backlog = Backlog::default();
    let first = backlog.submit(&mut fixture.scheduler, generation(&half, "k")).unwrap();
    let second = backlog.submit(&mut fixture.scheduler, generation(&half, "k")).unwrap();
    let other_key = backlog.submit(&mut fixture.scheduler, generation(b"small", "other")).unwrap();
    let no_key = backlog.submit(&mut fixture.scheduler, plain(b"input")).unwrap();

    fixture.scheduler.set_leadership_grant(Some(unbounded_grant()));
    backlog.hand_over(&mut fixture.scheduler);
    // `second` still waits: what it holds counts for a submission recorded
    // at once, another key is recorded at once, and a later submission of
    // its key waits behind it.
    assert!(matches!(
        backlog.submit(&mut fixture.scheduler, generation(&vec![b'y'; 1_000_000], "late-other")),
        Err(SubmitRejection::Backpressure { .. })
    ));
    let late_other = backlog
        .submit(&mut fixture.scheduler, generation(b"small", "late-other"))
        .unwrap();
    let third = backlog.submit(&mut fixture.scheduler, generation(b"third", "k")).unwrap();
    backlog.hand_over(&mut fixture.scheduler);
    assert!(
        fixture.forgotten(&second) && fixture.forgotten(&third),
        "`third` is not recorded ahead of `second`, even by a hand-over that finds room for others"
    );

    let mut claimed = claimed_tasks(&mut fixture);
    claimed.sort();
    let mut expected = vec![first.clone(), other_key, no_key, late_other];
    expected.sort();
    assert_eq!(claimed, expected);

    let run = fixture.scheduler.runs_of(&first).pop().unwrap();
    fixture.scheduler.report_started(&worker(), &run, None).unwrap();
    fixture
        .scheduler
        .complete(&worker(), &run, Digest::blake3(b"done"), Completion::Final)
        .unwrap();
    backlog.hand_over(&mut fixture.scheduler);

    let claims = fixture.scheduler.claim_oldest(&worker(), 10).unwrap();
    let [next] = claims.as_slice() else { panic!("one claim, got {claims:?}") };
    assert_eq!(next.task.task_id(), third, "`second` was recorded first and `third` absorbed it");
    assert_eq!(next.chain, [half]);
    assert_eq!(fixture.state(&second), TaskRunState::Superseded);
}

#[test]
fn composites_submitted_before_the_scheduler_leads_start_once_it_leads_and_a_cancel_drops_one() {
    let mut fixture = Fixture::not_leading();
    fixture.scheduler.set_memory_limits(Some(MemoryLimits { soft: 400, hard: 800 }));
    let mut backlog = Backlog::default();
    let mint = |fixture: &Fixture, step| fixture.scheduler.mint_composite(step, Some(worker()));

    let dropped = mint(&fixture, group_of(OnError::FailFast, vec![task_stage("dropped", Some(b"d"))]));
    let dropped = backlog.submit_composite(&mut fixture.scheduler, dropped).unwrap();
    let kept = mint(&fixture, flow_of(vec![task_stage("first", Some(b"x")), task_stage("second", None)]));
    let kept = backlog.submit_composite(&mut fixture.scheduler, kept).unwrap();
    let task = backlog.submit(&mut fixture.scheduler, plain(b"input")).unwrap();
    let takes_input = mint(&fixture, flow_of(vec![task_stage("takes", None)]));
    assert!(matches!(
        backlog.submit_composite(&mut fixture.scheduler, takes_input),
        Err(SubmitRejection::StepTakesInput)
    ));
    let status = backlog.composite_status(&fixture.scheduler, &kept).unwrap();
    assert_eq!((status.state, status.counts.pending), (CompositeState::Running, 2), "it waits as pending");

    assert_eq!(
        backlog.cancel_composite(&mut fixture.scheduler, &dropped),
        Ok(Cancellation::Cancelled { was_running: false })
    );
    assert_eq!(backlog.composite_status(&fixture.scheduler, &dropped), None);

    assert!(finished(&mut fixture).is_empty(), "nothing is said before the scheduler leads");

    fixture.scheduler.set_leadership_grant(Some(unbounded_grant()));
    backlog.hand_over(&mut fixture.scheduler);

    let events = finished(&mut fixture);
    assert_eq!(
        events.iter().map(|event| (event.composite_id.clone(), event.state)).collect::<Vec<_>>(),
        [(dropped.clone(), CompositeState::Cancelled)],
        "its client is told, as it is of a composite cancelled while it runs"
    );
    let mut claimed = claimed_tasks(&mut fixture);
    claimed.sort();
    let mut expected = vec![task, kept.member_task(0)];
    expected.sort();
    assert_eq!(claimed, expected, "the task and the flow's first stage, and nothing of the dropped one");
    assert_eq!(
        backlog.cancel_composite(&mut fixture.scheduler, &dropped),
        Ok(Cancellation::UnknownTask),
        "the scheduler never learnt of the dropped composite"
    );
    assert_eq!(
        backlog.composite_status(&fixture.scheduler, &kept).unwrap().state,
        CompositeState::Running
    );
}

#[test]
fn a_composite_that_would_outgrow_the_hard_limit_with_what_waits_is_refused() {
    let mut fixture = Fixture::not_leading();
    let mut backlog = Backlog::default();
    let member = |index: usize| task_stage(&format!("member-{index}-{}", "x".repeat(300)), Some(b"v"));
    let wide = fixture.scheduler.mint_composite(group_of(OnError::FailFast, (0..3).map(member).collect()), None);
    let weight = wide.record_bytes();
    fixture.scheduler.set_memory_limits(Some(MemoryLimits { soft: weight, hard: weight + weight / 2 }));

    backlog.submit_composite(&mut fixture.scheduler, wide).unwrap();
    let another = fixture.scheduler.mint_composite(group_of(OnError::FailFast, (0..3).map(member).collect()), None);
    assert!(matches!(
        backlog.submit_composite(&mut fixture.scheduler, another),
        Err(SubmitRejection::Backpressure { .. })
    ));
}
