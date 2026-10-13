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
use kabudachi_core::scheduler::composite::{CompositeState, MemberCounts, OnError, PAGE_ENTRIES, Stage};
use kabudachi_core::scheduler::{Cancellation, ClaimRejection, Completion, Event, SubmitRejection};
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

#[test]
fn fail_fast_fails_the_group_at_its_first_failure_and_leaves_running_members_alone() {
    let mut fixture = Fixture::leading();
    let group = submitted(
        &mut fixture,
        group_of(
            OnError::FailFast,
            vec![task_stage("explode", Some(b"a")), task_stage("slow", Some(b"b"))],
        ),
    );
    let slow = start_member(&mut fixture, &group, 1);
    fail_member(&mut fixture, &group, 0, "ValueError");
    let finished_events = finished(&mut fixture);
    assert_eq!(finished_events.len(), 1);
    assert_eq!(finished_events[0].state, CompositeState::Failed);
    assert_eq!(
        fixture.run_state(&slow.task_run_id),
        TaskRunState::Running,
        "like asyncio.gather, the other member is left to finish"
    );
    fixture
        .scheduler
        .complete(&worker(), &slow.task_run_id, Digest::blake3(b"late"), Completion::Final)
        .unwrap();
    assert!(finished(&mut fixture).is_empty(), "the group finished once");
    assert!(entry(&fixture, &group, 1).is_some(), "the late result is still recorded");
}

#[test]
fn collect_all_waits_for_every_member_and_records_each_failure_as_its_entry() {
    let mut fixture = Fixture::leading();
    let group = submitted(
        &mut fixture,
        group_of(
            OnError::CollectAll,
            vec![
                task_stage("fine", Some(b"a")),
                task_stage("explode", Some(b"b")),
                task_stage("pending", Some(b"c")),
            ],
        ),
    );
    run_member(&mut fixture, &group, 0, b"ok");
    fail_member(&mut fixture, &group, 1, "ValueError");
    assert!(finished(&mut fixture).is_empty(), "one member is still pending");
    fixture.scheduler.cancel(&group.member_task(2)).unwrap();
    let finished_events = finished(&mut fixture);
    assert_eq!(finished_events[0].state, CompositeState::Succeeded);
    let kinds: Vec<Option<String>> = (0..3)
        .map(|index| match entry(&fixture, &group, index).unwrap().outcome {
            Some(composite_entry::Outcome::Failure(failure)) => Some(failure.kind),
            _ => None,
        })
        .collect();
    assert_eq!(kinds, [None, Some("ValueError".into()), Some("TaskCancelledError".into())]);
}

#[test]
fn a_member_lost_with_its_worker_is_replayed_and_settles_only_with_its_replay() {
    let mut fixture = Fixture::leading();
    let group = submitted(&mut fixture, group_of(OnError::FailFast, vec![task_stage("a", Some(b"a"))]));
    start_member(&mut fixture, &group, 0);
    let lost = fixture.lose_silent(&worker());
    assert!(lost[0].replayed.is_some());
    assert!(finished(&mut fixture).is_empty());
    run_member(&mut fixture, &group, 0, b"replayed");
    assert_eq!(finished(&mut fixture)[0].state, CompositeState::Succeeded);
}

#[test]
fn a_map_of_more_members_than_a_page_holds_spreads_its_outcomes_over_pages_each_published_before_the_record_naming_it() {
    let mut fixture = Fixture::leading();
    let members = (0..=PAGE_ENTRIES).map(|item| task_stage("each", Some(&item.to_be_bytes()))).collect();
    let map = submitted(&mut fixture, map_of(members));
    let mark = fixture.spy.mark();
    run_member(&mut fixture, &map, PAGE_ENTRIES, &PAGE_ENTRIES.to_be_bytes());
    let published = fixture.spy.published_since(mark);
    let position_of = |wanted: &dyn Fn(&ShardRecord) -> bool| published.iter().position(wanted).unwrap();
    let page_zero = position_of(&|record| matches!(record, ShardRecord::Page(page) if page.index == 0));
    let page_one = position_of(&|record| matches!(record, ShardRecord::Page(page) if page.index == 1));
    let composite = position_of(&|record| matches!(record, ShardRecord::Composite(_)));
    assert!(
        page_zero < composite && page_one < composite,
        "the record now names two pages, so the lower one is published too, before the record"
    );
    for index in 0..PAGE_ENTRIES {
        run_member(&mut fixture, &map, index, &index.to_be_bytes());
    }
    let finished_events = finished(&mut fixture);
    assert_eq!(finished_events[0].pages, 2);
    assert_eq!(fixture.spy.newest_page(&map, 0).unwrap().entries.len() as u64, PAGE_ENTRIES);
    assert_eq!(fixture.spy.newest_page(&map, 1).unwrap().entries[0].index, PAGE_ENTRIES);
}

#[test]
fn a_map_of_no_members_finishes_at_once_with_no_pages() {
    let mut fixture = Fixture::leading();
    submitted(&mut fixture, map_of(Vec::new()));
    let finished_events = finished(&mut fixture);
    assert_eq!(finished_events.len(), 1);
    assert_eq!(finished_events[0].state, CompositeState::Succeeded);
    assert_eq!(finished_events[0].pages, 0);
}

#[test]
fn republishing_a_key_named_twice_writes_it_once() {
    let mut fixture = Fixture::leading();
    let flow = submitted(&mut fixture, flow_of(vec![task_stage("load", Some(b"x"))]));
    run_member(&mut fixture, &flow, 0, b"loaded");
    let page = RecordKey::Page(flow.clone(), 0);
    let mark = fixture.spy.mark();
    assert_eq!(fixture.scheduler.republish(&[page.clone(), page]), 1);
    assert_eq!(fixture.spy.mark() - mark, 1);
}

#[test]
fn a_group_stage_is_a_child_composite_whose_results_the_next_stage_takes_as_its_pages() {
    let mut fixture = Fixture::leading();
    let flow = submitted(
        &mut fixture,
        flow_of(vec![
            task_stage("load", Some(b"x")),
            Stage::Composite(group_of(
                OnError::FailFast,
                vec![task_stage("shout", None), task_stage("wrap", None)],
            )),
            task_stage("report", None),
        ]),
    );
    run_member(&mut fixture, &flow, 0, b"loaded");
    let group = flow.child(1);
    // Both records name the flow as their tree's root: the owner of every
    // large value bound in it, which a member's worker reads from the record.
    for id in [&flow, &group] {
        let root = fixture.scheduler.composite_record(id).unwrap().root;
        assert_eq!(root, Some(flow.clone().into()), "{id:?} names its root");
    }
    for index in 0..2 {
        let member = run_member(&mut fixture, &group, index, &[index as u8]);
        assert_eq!(
            member.input_manifest().unwrap(),
            Some(Manifest::new(vec![ManifestEntry::Blob(BlobKey::of(b"loaded"))], 0)),
            "each member takes the flow's previous result"
        );
    }
    let report = run_member(&mut fixture, &flow, 2, b"reported");
    assert_eq!(
        report.input_manifest().unwrap(),
        Some(Manifest::new(vec![ManifestEntry::Page { composite: group.clone(), page: 0 }], 0))
    );
    assert_eq!(
        entry(&fixture, &flow, 1).unwrap().outcome,
        Some(composite_entry::Outcome::Child(group.clone().into()))
    );
    let finished = finished(&mut fixture);
    assert_eq!(finished.len(), 1, "only the submitted composite has a client");
    assert_eq!(finished[0].composite_id, flow);
}

#[test]
fn cancelling_a_flow_cancels_its_running_group_member_by_member_and_nothing_after_starts() {
    let mut fixture = Fixture::leading();
    let flow = submitted(
        &mut fixture,
        flow_of(vec![
            Stage::Composite(group_of(
                OnError::FailFast,
                vec![task_stage("slow", Some(b"a")), task_stage("queued", Some(b"b"))],
            )),
            task_stage("after", None),
        ]),
    );
    let group = flow.child(0);
    let slow = start_member(&mut fixture, &group, 0);
    assert_eq!(
        fixture.scheduler.cancel_composite(&flow),
        Ok(Cancellation::Cancelled { was_running: true })
    );
    assert_eq!(fixture.run_state(&slow.task_run_id), TaskRunState::Cancelled);
    assert_eq!(fixture.state(&group.member_task(1)), TaskRunState::Cancelled);
    let events = fixture.scheduler.take_events();
    assert!(
        events.contains(&Event::Cancelled {
            task_id: group.member_task(0),
            task_run_id: slow.task_run_id.clone(),
            was_running: true,
        }),
        "the running member's worker is told to stop"
    );
    let finished: Vec<_> = events
        .into_iter()
        .filter_map(|event| match event {
            Event::CompositeFinished(finished) => Some(finished),
            _ => None,
        })
        .collect();
    assert_eq!(finished.len(), 1);
    assert_eq!(finished[0].state, CompositeState::Cancelled);
    assert_eq!(
        fixture.scheduler.composite_status(&group).unwrap().state,
        CompositeState::Cancelled
    );
    assert_eq!(fixture.scheduler.cancel_composite(&flow), Ok(Cancellation::AlreadyFinished));
    assert_eq!(
        fixture.scheduler.request_claim(&worker(), &flow.member_task(1)),
        Err(ClaimRejection::TaskUnknown)
    );
}

#[test]
fn a_composites_status_counts_its_members_by_state() {
    let mut fixture = Fixture::leading();
    let group = submitted(
        &mut fixture,
        group_of(
            OnError::CollectAll,
            vec![task_stage("a", Some(b"a")), task_stage("b", Some(b"b")), task_stage("c", Some(b"c"))],
        ),
    );
    run_member(&mut fixture, &group, 0, b"ok");
    fail_member(&mut fixture, &group, 1, "ValueError");
    let status = fixture.scheduler.composite_status(&group).unwrap();
    assert_eq!(status.state, CompositeState::Running);
    assert_eq!(status.counts, MemberCounts { pending: 1, succeeded: 1, failed: 1, cancelled: 0 });
}
