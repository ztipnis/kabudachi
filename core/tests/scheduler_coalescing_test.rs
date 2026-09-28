//! Coalescing: a newer pending generation of a key supersedes the older one,
//! a running generation is never touched, and only one generation of a key
//! runs at a time (README §3.2.1, §25.4.1-3).

mod support;

use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::records::TaskRunRecord;
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::scheduler::{ClaimRejection, Event, Scheduler, Submission};
use kabudachi_core::time::Duration;
use support::clock::FakeClock;
use support::grant::unbounded_grant;
use support::ids::SequentialIds;

fn worker() -> WorkerId {
    WorkerId::new("w1")
}

fn ticks(n: u64) -> Duration {
    Duration::from_ticks(n)
}

struct Fixture {
    clock: FakeClock,
    scheduler: Scheduler<FakeClock, SequentialIds>,
}

fn leading() -> Fixture {
    let clock = FakeClock::new();
    let mut scheduler = Scheduler::new(clock.clone(), SequentialIds::new());
    scheduler.set_leadership_grant(Some(unbounded_grant()));
    Fixture { clock, scheduler }
}

fn generation(definition: &str, key: &str, payload: &str) -> Submission {
    Submission::new(
        TaskDefinitionId::new(definition),
        0,
        payload.as_bytes().to_vec(),
        "default",
    )
    .with_coalescing_key(key)
}

fn refresh(payload: &str) -> Submission {
    generation("index.refresh", "", payload)
}

fn state(fixture: &Fixture, task: &TaskId) -> TaskRunState {
    fixture.scheduler.run_of(task).unwrap().current_state()
}

fn claim_one(fixture: &mut Fixture) -> kabudachi_core::scheduler::Claim {
    fixture
        .scheduler
        .claim_oldest(&worker(), 1)
        .unwrap()
        .remove(0)
}

fn start_and_complete(fixture: &mut Fixture, claim: &kabudachi_core::scheduler::Claim) {
    fixture
        .scheduler
        .report_started(&worker(), &claim.task_run_id)
        .unwrap();
    fixture
        .scheduler
        .complete(&worker(), &claim.task_run_id, b"d".to_vec())
        .unwrap();
}

fn superseded_events(fixture: &mut Fixture) -> Vec<(TaskId, TaskId)> {
    fixture
        .scheduler
        .take_events()
        .into_iter()
        .filter_map(|event| match event {
            Event::Superseded { task_id, by, .. } => Some((task_id, by)),
            _ => None,
        })
        .collect()
}

#[test]
fn a_newer_pending_generation_supersedes_the_older_one() {
    let mut fixture = leading();
    let older = fixture.scheduler.submit(refresh("a")).unwrap();

    let newer = fixture.scheduler.submit(refresh("b")).unwrap();

    assert_eq!(state(&fixture, &older), TaskRunState::Superseded);
    assert_eq!(state(&fixture, &newer), TaskRunState::Queued);
    assert_eq!(fixture.scheduler.pending_tasks(), vec![newer.clone()]);
    assert_eq!(superseded_events(&mut fixture), vec![(older, newer)]);
}

#[test]
fn a_superseded_generation_is_never_handed_out() {
    let mut fixture = leading();
    let older = fixture.scheduler.submit(refresh("a")).unwrap();
    fixture.scheduler.submit(refresh("b")).unwrap();

    let result = fixture.scheduler.request_claim(&worker(), &older);

    assert_eq!(result.unwrap_err(), ClaimRejection::Superseded);
}

#[test]
fn different_keys_and_different_tasks_do_not_supersede_each_other() {
    let mut fixture = leading();
    let tenant_a = fixture
        .scheduler
        .submit(generation("index.refresh", "a", "1"))
        .unwrap();
    let tenant_b = fixture
        .scheduler
        .submit(generation("index.refresh", "b", "2"))
        .unwrap();
    let other_task = fixture
        .scheduler
        .submit(generation("cache.warm", "a", "3"))
        .unwrap();

    assert_eq!(
        fixture.scheduler.pending_tasks(),
        vec![tenant_a, tenant_b, other_task]
    );
    assert!(!fixture.scheduler.has_events());
}

#[test]
fn a_task_without_a_key_is_never_superseded() {
    let mut fixture = leading();
    let plain = || {
        Submission::new(
            TaskDefinitionId::new("billing.charge"),
            0,
            b"x".to_vec(),
            "default",
        )
    };
    let first = fixture.scheduler.submit(plain()).unwrap();
    let second = fixture.scheduler.submit(plain()).unwrap();

    assert_eq!(fixture.scheduler.pending_tasks(), vec![first, second]);
}

#[test]
fn a_delayed_pending_generation_is_superseded_too() {
    let mut fixture = leading();
    let older = fixture
        .scheduler
        .submit(refresh("a").with_delay(ticks(100)))
        .unwrap();

    let newer = fixture.scheduler.submit(refresh("b")).unwrap();

    assert_eq!(state(&fixture, &older), TaskRunState::Superseded);
    fixture.clock.advance(ticks(500));
    let advanced = fixture.scheduler.advance();
    assert_eq!(advanced.queued, 0);
    assert_eq!(fixture.scheduler.pending_tasks(), vec![newer]);
    assert_eq!(fixture.scheduler.next_deadline(), None);
}

#[test]
fn a_superseded_generation_no_longer_expires() {
    let mut fixture = leading();
    fixture
        .scheduler
        .submit(refresh("a").with_expiry(ticks(50)))
        .unwrap();
    fixture.scheduler.submit(refresh("b")).unwrap();
    fixture.scheduler.take_events();

    fixture.clock.advance(ticks(500));

    assert_eq!(fixture.scheduler.advance().expired, 0);
    assert!(!fixture.scheduler.has_events());
}

#[test]
fn a_claimed_generation_is_never_superseded_and_the_newer_one_waits_for_it() {
    let mut fixture = leading();
    let running = fixture.scheduler.submit(refresh("a")).unwrap();
    let claim = claim_one(&mut fixture);
    fixture
        .scheduler
        .report_started(&worker(), &claim.task_run_id)
        .unwrap();

    let newer = fixture.scheduler.submit(refresh("b")).unwrap();

    assert_eq!(state(&fixture, &running), TaskRunState::Running);
    assert_eq!(state(&fixture, &newer), TaskRunState::Queued);
    assert!(!fixture.scheduler.has_events());
    // Only one generation of a key runs at a time.
    assert!(
        fixture
            .scheduler
            .claim_oldest(&worker(), 10)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        fixture
            .scheduler
            .request_claim(&worker(), &newer)
            .unwrap_err(),
        ClaimRejection::KeyBusy
    );
    // Its result is certified as usual, and then the key is free.
    fixture
        .scheduler
        .complete(&worker(), &claim.task_run_id, b"d".to_vec())
        .unwrap();
    assert_eq!(
        fixture.scheduler.claim_oldest(&worker(), 10).unwrap().len(),
        1
    );
}

#[test]
fn while_a_generation_runs_only_the_newest_pending_one_is_kept() {
    let mut fixture = leading();
    fixture.scheduler.submit(refresh("running")).unwrap();
    let claim = claim_one(&mut fixture);
    fixture
        .scheduler
        .report_started(&worker(), &claim.task_run_id)
        .unwrap();

    let first = fixture.scheduler.submit(refresh("b")).unwrap();
    let second = fixture.scheduler.submit(refresh("c")).unwrap();
    let third = fixture.scheduler.submit(refresh("d")).unwrap();

    assert_eq!(state(&fixture, &first), TaskRunState::Superseded);
    assert_eq!(state(&fixture, &second), TaskRunState::Superseded);
    assert_eq!(state(&fixture, &third), TaskRunState::Queued);
    assert_eq!(fixture.scheduler.pending_tasks(), vec![third]);
}

#[test]
fn the_claim_of_the_newest_generation_carries_the_superseded_payloads_oldest_first() {
    let mut fixture = leading();
    for payload in ["a", "b", "c"] {
        fixture.scheduler.submit(refresh(payload)).unwrap();
    }
    fixture.scheduler.submit(refresh("d")).unwrap();

    let claim = claim_one(&mut fixture);

    assert_eq!(claim.task.serialized_input, b"d".to_vec());
    assert_eq!(
        claim.chain,
        vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
    );
}

#[test]
fn a_generation_nobody_superseded_has_an_empty_chain() {
    let mut fixture = leading();
    fixture.scheduler.submit(refresh("only")).unwrap();

    assert!(claim_one(&mut fixture).chain.is_empty());
}

#[test]
fn the_chain_keeps_growing_across_a_running_generation() {
    let mut fixture = leading();
    fixture.scheduler.submit(refresh("running")).unwrap();
    let running = claim_one(&mut fixture);
    fixture
        .scheduler
        .report_started(&worker(), &running.task_run_id)
        .unwrap();
    fixture.scheduler.submit(refresh("b")).unwrap();
    fixture.scheduler.submit(refresh("c")).unwrap();
    start_and_complete_later(&mut fixture, &running);

    let claim = claim_one(&mut fixture);

    assert_eq!(claim.task.serialized_input, b"c".to_vec());
    assert_eq!(claim.chain, vec![b"b".to_vec()]);
}

fn start_and_complete_later(fixture: &mut Fixture, claim: &kabudachi_core::scheduler::Claim) {
    fixture
        .scheduler
        .complete(&worker(), &claim.task_run_id, b"d".to_vec())
        .unwrap();
}

#[test]
fn superseded_tasks_are_kept_until_the_generation_that_absorbed_them_finishes() {
    let mut fixture = leading();
    fixture.scheduler.set_result_ttl(Some(ticks(100)));
    let older = fixture.scheduler.submit(refresh("a")).unwrap();
    let newest = fixture.scheduler.submit(refresh("b")).unwrap();

    // While it waits, and while it runs (a retry would fold the chain again),
    // the payload is still needed.
    fixture.clock.advance(ticks(10_000));
    assert_eq!(fixture.scheduler.sweep(), 0);
    let claim = claim_one(&mut fixture);
    fixture
        .scheduler
        .report_started(&worker(), &claim.task_run_id)
        .unwrap();
    fixture.clock.advance(ticks(10_000));
    assert_eq!(fixture.scheduler.sweep(), 0);
    assert!(fixture.scheduler.task(&older).is_some());

    fixture
        .scheduler
        .complete(&worker(), &claim.task_run_id, b"d".to_vec())
        .unwrap();
    fixture.clock.advance(ticks(100));
    assert_eq!(fixture.scheduler.sweep(), 2);
    assert!(fixture.scheduler.task(&older).is_none());
    assert!(fixture.scheduler.task(&newest).is_none());
}

#[test]
fn a_failed_generation_holds_its_key_while_it_waits_for_a_retry() {
    let mut fixture = leading();
    fixture
        .scheduler
        .submit(refresh("a").with_retries(1))
        .unwrap();
    let first = claim_one(&mut fixture);
    fixture
        .scheduler
        .report_started(&worker(), &first.task_run_id)
        .unwrap();
    let newer = fixture.scheduler.submit(refresh("b")).unwrap();
    fixture
        .scheduler
        .fail(&worker(), &first.task_run_id, "ValueError")
        .unwrap();

    // The retry of the running generation goes first; the newer one waits.
    let retry = claim_one(&mut fixture);
    assert_eq!(retry.attempt_number, 2);
    assert_eq!(retry.task.serialized_input, b"a".to_vec());
    assert!(
        fixture
            .scheduler
            .claim_oldest(&worker(), 10)
            .unwrap()
            .is_empty()
    );
    fixture
        .scheduler
        .report_started(&worker(), &retry.task_run_id)
        .unwrap();
    fixture
        .scheduler
        .fail(&worker(), &retry.task_run_id, "ValueError")
        .unwrap();

    // Out of retries: the key is free.
    let next = claim_one(&mut fixture);
    assert_eq!(next.task.task_id(), newer);
}

#[test]
fn a_timeout_style_failure_is_not_requeued_and_frees_the_key() {
    let mut fixture = leading();
    fixture.scheduler.submit(refresh("a")).unwrap();
    let claim = claim_one(&mut fixture);
    fixture
        .scheduler
        .report_started(&worker(), &claim.task_run_id)
        .unwrap();
    let newer = fixture.scheduler.submit(refresh("b")).unwrap();

    let failure = fixture
        .scheduler
        .fail(&worker(), &claim.task_run_id, "TaskTimeoutError")
        .unwrap();

    assert_eq!(failure.retry, None);
    assert_eq!(claim_one(&mut fixture).task.task_id(), newer);
}

#[test]
fn cancelling_a_running_generation_frees_its_key() {
    let mut fixture = leading();
    let running = fixture.scheduler.submit(refresh("a")).unwrap();
    let claim = claim_one(&mut fixture);
    fixture
        .scheduler
        .report_started(&worker(), &claim.task_run_id)
        .unwrap();
    let newer = fixture.scheduler.submit(refresh("b")).unwrap();

    fixture.scheduler.cancel(&running).unwrap();

    assert_eq!(claim_one(&mut fixture).task.task_id(), newer);
}

#[test]
fn a_blocked_generation_does_not_hold_up_other_keys() {
    let mut fixture = leading();
    fixture
        .scheduler
        .submit(generation("index.refresh", "a", "running"))
        .unwrap();
    let running = claim_one(&mut fixture);
    fixture
        .scheduler
        .report_started(&worker(), &running.task_run_id)
        .unwrap();
    let blocked = fixture
        .scheduler
        .submit(generation("index.refresh", "a", "blocked"))
        .unwrap();
    let free = fixture
        .scheduler
        .submit(generation("index.refresh", "b", "free"))
        .unwrap();

    let claims = fixture.scheduler.claim_oldest(&worker(), 10).unwrap();

    assert_eq!(
        claims.iter().map(|c| c.task.task_id()).collect::<Vec<_>>(),
        vec![free]
    );
    assert_eq!(state(&fixture, &blocked), TaskRunState::Queued);
}

#[test]
fn cancelling_the_newest_pending_generation_releases_the_chain() {
    let mut fixture = leading();
    fixture.scheduler.set_result_ttl(Some(ticks(100)));
    let older = fixture.scheduler.submit(refresh("a")).unwrap();
    let newest = fixture.scheduler.submit(refresh("b")).unwrap();

    fixture.scheduler.cancel(&newest).unwrap();
    fixture.clock.advance(ticks(100));

    assert_eq!(fixture.scheduler.sweep(), 2);
    assert!(fixture.scheduler.task(&older).is_none());
    // A later submission starts a fresh generation with nothing chained.
    fixture.scheduler.submit(refresh("c")).unwrap();
    assert!(claim_one(&mut fixture).chain.is_empty());
}

#[test]
fn an_expired_newest_generation_releases_the_chain() {
    let mut fixture = leading();
    fixture.scheduler.set_result_ttl(Some(ticks(100)));
    fixture.scheduler.submit(refresh("a")).unwrap();
    fixture
        .scheduler
        .submit(refresh("b").with_expiry(ticks(10)))
        .unwrap();

    fixture.clock.advance(ticks(10));
    fixture.scheduler.advance();
    fixture.clock.advance(ticks(100));

    assert_eq!(fixture.scheduler.sweep(), 2);
}

#[test]
fn the_completed_generation_certifies_its_own_result() {
    let mut fixture = leading();
    fixture.scheduler.submit(refresh("a")).unwrap();
    let newest = fixture.scheduler.submit(refresh("b")).unwrap();
    let claim = claim_one(&mut fixture);

    start_and_complete(&mut fixture, &claim);

    assert_eq!(state(&fixture, &newest), TaskRunState::Succeeded);
}
