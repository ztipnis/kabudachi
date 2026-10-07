//! What a scheduler cannot take yet waits in its backlog, in order, and is
//! handed over once the scheduler leads.

use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::scheduler::{
    Backlog, Cancellation, ClaimRejection, Completion, ContinuationRejection, MemoryLimits,
    Submission, SubmitRejection,
};

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
    fixture.scheduler.report_started(&worker(), &run).unwrap();
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
fn a_continuation_end_refused_for_want_of_leadership_is_ended_once_the_scheduler_leads() {
    let mut fixture = Fixture::leading();
    let mut backlog = Backlog::default();
    let flow = backlog.submit(&mut fixture.scheduler, generation(b"step", "k")).unwrap();
    let run = fixture.scheduler.request_claim(&worker(), &flow).unwrap().task_run_id;
    fixture.scheduler.report_started(&worker(), &run).unwrap();
    fixture
        .scheduler
        .complete(&worker(), &run, Digest::blake3(b"step"), Completion::Continues)
        .unwrap();
    let waiting = backlog.submit(&mut fixture.scheduler, generation(b"next", "k")).unwrap();
    fixture.scheduler.set_leadership_grant(None);

    assert_eq!(
        backlog.end_continuation(&mut fixture.scheduler, &flow),
        Err(ContinuationRejection::NotLeader)
    );
    backlog.hand_over(&mut fixture.scheduler);
    fixture.scheduler.set_leadership_grant(Some(unbounded_grant()));
    assert_eq!(
        fixture.scheduler.request_claim(&worker(), &waiting),
        Err(ClaimRejection::KeyBusy),
        "the flow holds its key until the hand-over ends it"
    );
    backlog.hand_over(&mut fixture.scheduler);

    assert!(fixture.scheduler.request_claim(&worker(), &waiting).is_ok());
}
