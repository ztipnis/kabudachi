//! Composites one leader drives: flows, groups, maps, nesting, cancels and
//! returned steps. Members are ordinary Tasks with derived ids, so each
//! test claims and reports them as a worker would.

use kabudachi_core::blob::BlobKey;
use kabudachi_core::manifest::{Manifest, ManifestEntry};
use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::generated::composite_entry;
use kabudachi_core::protocol::ids::TaskDefinitionId;
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::scheduler::composite::CompositeState;
use kabudachi_core::scheduler::{ClaimRejection, Completion, SubmitRejection};
use kabudachi_core::task_record::{RecordKey, ShardRecord};

use crate::support::composites::*;
use crate::support::scheduler::{Fixture, ticks};

#[test]
fn a_flow_starts_each_stage_on_the_result_of_the_one_before_and_finishes_with_every_result() {
    let mut fixture = Fixture::leading();
    let flow = submitted(
        &mut fixture,
        flow_of(vec![task_stage("load", Some(b"x")), task_stage("shout", None)]),
    );
    assert_eq!(
        fixture.scheduler.request_claim(&worker(), &flow.member_task(1)),
        Err(ClaimRejection::TaskUnknown),
        "the second stage does not exist before the first certifies"
    );
    let first = run_member(&mut fixture, &flow, 0, b"loaded");
    assert_eq!(first.task.serialized_input, b"x");
    assert_eq!(first.task.composite_id(), Some(flow.clone()));
    let second = run_member(&mut fixture, &flow, 1, b"shouted");
    assert_eq!(
        second.input_manifest().unwrap(),
        Some(Manifest::new(vec![ManifestEntry::Blob(BlobKey::of(b"loaded"))], 0)),
        "the second stage takes the first stage's result blob"
    );
    assert_eq!(second.task.composite_index, 1);
    let finished = finished(&mut fixture);
    assert_eq!(finished.len(), 1);
    assert_eq!(finished[0].state, CompositeState::Succeeded);
    assert_eq!(finished[0].client, Some(worker()));
    assert_eq!(
        finished[0].results(),
        Manifest::new(vec![ManifestEntry::Page { composite: flow.clone(), page: 0 }], 0)
    );
    for (index, result, producer) in [(0, &b"loaded"[..], "load"), (1, b"shouted", "shout")] {
        let entry = entry(&fixture, &flow, index).unwrap();
        assert_eq!(entry.outcome, Some(composite_entry::Outcome::Value(Digest::blake3(result).into())));
        assert_eq!(entry.producer, Some(TaskDefinitionId::new(producer).into()));
    }
    let record = fixture.spy.newest_composite(&flow).unwrap();
    assert!(record.finished);
    assert_eq!(fixture.scheduler.composite_status(&flow).unwrap().state, CompositeState::Succeeded);
}

#[test]
fn a_failed_stage_fails_the_flow_and_no_later_stage_is_ever_made() {
    let mut fixture = Fixture::leading();
    let flow = submitted(
        &mut fixture,
        flow_of(vec![task_stage("explode", Some(b"x")), task_stage("after", None)]),
    );
    fail_member(&mut fixture, &flow, 0, "ValueError");
    let finished = finished(&mut fixture);
    assert_eq!(finished[0].state, CompositeState::Failed);
    assert_eq!(finished[0].failure.as_ref().unwrap().kind, "ValueError");
    assert_eq!(
        fixture.scheduler.request_claim(&worker(), &flow.member_task(1)),
        Err(ClaimRejection::TaskUnknown)
    );
}

#[test]
fn a_composite_submitted_twice_under_one_id_is_one_composite_and_one_that_takes_an_input_is_refused() {
    let mut fixture = Fixture::leading();
    let minted = fixture
        .scheduler
        .mint_composite(flow_of(vec![task_stage("load", Some(b"x"))]), Some(worker()));
    let id = fixture.scheduler.submit_composite(minted.clone()).unwrap();
    assert_eq!(fixture.scheduler.submit_composite(minted), Ok(id.clone()));
    assert_eq!(fixture.scheduler.pending_len(), 1, "one first stage, not two");
    let open = fixture.scheduler.mint_composite(flow_of(vec![task_stage("shout", None)]), None);
    assert_eq!(fixture.scheduler.submit_composite(open), Err(SubmitRejection::StepTakesInput));
}

#[test]
fn a_retried_stage_keeps_its_place_and_the_flow_waits_for_its_last_attempt() {
    let mut fixture = Fixture::leading();
    let mut flaky = task_stage("flaky", Some(b"x"));
    flaky.options_mut().retries = 1;
    let flow = submitted(&mut fixture, flow_of(vec![flaky, task_stage("after", None)]));
    fail_member(&mut fixture, &flow, 0, "ValueError");
    assert!(finished(&mut fixture).is_empty(), "a failure with retries left settles nothing");
    assert_eq!(fixture.state(&flow.member_task(0)), TaskRunState::Queued);
    run_member(&mut fixture, &flow, 0, b"ok");
    assert!(fixture.scheduler.request_claim(&worker(), &flow.member_task(1)).is_ok());
}

#[test]
fn a_call_publishes_the_task_then_the_pages_then_the_composite_it_changed() {
    let mut fixture = Fixture::leading();
    let flow = submitted(&mut fixture, flow_of(vec![task_stage("load", Some(b"x"))]));
    let claim = start_member(&mut fixture, &flow, 0);
    fixture.spy.take_revisions();
    fixture
        .scheduler
        .complete(&worker(), &claim.task_run_id, Digest::blake3(b"loaded"), Completion::Final)
        .unwrap();
    let kinds: Vec<&str> = fixture
        .spy
        .take_revisions()
        .iter()
        .map(|record| match record {
            ShardRecord::Task(_) => "task",
            ShardRecord::Page(_) => "page",
            ShardRecord::Composite(_) => "composite",
        })
        .collect();
    assert_eq!(kinds, ["task", "page", "composite"]);
}

#[test]
fn a_finished_composite_and_its_pages_are_forgotten_with_the_result_ttl_and_a_leader_republishes_what_it_holds() {
    let mut fixture = Fixture::leading();
    fixture.scheduler.set_result_ttl(Some(ticks(100)));
    let flow = submitted(&mut fixture, flow_of(vec![task_stage("load", Some(b"x"))]));
    let (composite, page) = (RecordKey::Composite(flow.clone()), RecordKey::Page(flow.clone(), 0));
    assert!(fixture.scheduler.holds(&composite));
    run_member(&mut fixture, &flow, 0, b"loaded");
    assert!(fixture.scheduler.holds(&page));
    let mark = fixture.spy.mark();
    assert_eq!(fixture.scheduler.republish(&[composite.clone(), page.clone()]), 2);
    assert_eq!(fixture.spy.mark() - mark, 2, "each held record is written again");
    fixture.clock.advance(ticks(99));
    fixture.scheduler.catch_up();
    assert!(fixture.scheduler.composite_record(&flow).is_some(), "kept for the whole ttl");
    fixture.clock.advance(ticks(1));
    fixture.scheduler.catch_up();
    assert!(!fixture.scheduler.holds(&composite));
    assert!(!fixture.scheduler.holds(&page));
    assert!(fixture.scheduler.composite_status(&flow).is_none());
    assert!(fixture.forgotten(&flow.member_task(0)));
}
