//! Composites one leader drives: flows, groups, maps, nesting, cancels and
//! returned steps. Members are ordinary Tasks with derived ids, so each
//! test claims and reports them as a worker would.

use kabudachi_core::blob::BlobKey;
use kabudachi_core::manifest::{Manifest, ManifestEntry};
use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::generated::{Expansion, composite_entry};
use kabudachi_core::protocol::ids::{CompositeId, TaskDefinitionId, TaskId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::records::TaskRunRecord;
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::scheduler::composite::{
    BoundInput, CompositeFinished, CompositeKind, CompositeState, EXPANSION_DEFINITION, MemberCounts, OnError,
    PAGE_ENTRIES, Stage, Step,
};
use kabudachi_core::scheduler::{
    Cancellation, ClaimRejection, Completion, Event, ReportRejection, Submission, SubmitRejection,
};
use kabudachi_core::task_record::{MAX_RECORD_BYTES, RecordKey, ShardRecord};

use prost::Message as _;

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
    let failure = finished[0].failure.as_ref().unwrap();
    assert_eq!((failure.kind.as_str(), failure.message.as_str()), ("ValueError", "member 0 failed"));
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
    assert_eq!(
        fixture.scheduler.republish(&[composite.clone(), page.clone(), page.clone()]),
        2,
        "a key named twice is written once"
    );
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
fn a_member_is_kept_with_its_results_until_its_group_is_forgotten_however_long_ago_it_finished() {
    let mut fixture = Fixture::leading();
    fixture.scheduler.set_result_ttl(Some(ticks(100)));
    let group = submitted(
        &mut fixture,
        group_of(OnError::CollectAll, vec![task_stage("early", Some(b"a")), task_stage("late", Some(b"b"))]),
    );
    let (early, late) = (group.member_task(0), group.member_task(1));
    run_member(&mut fixture, &group, 0, b"first");

    fixture.clock.advance(ticks(250));
    fixture.scheduler.catch_up();
    assert!(!fixture.forgotten(&early), "its group still waits for the other member");
    let record = fixture.spy.newest_revision_of(&early).unwrap();
    assert!(
        record.finished && record.retained_by_composite,
        "holders are told to keep it while the group is held"
    );

    run_member(&mut fixture, &group, 1, b"second");
    fixture.clock.advance(ticks(99));
    fixture.scheduler.catch_up();
    assert!(!fixture.forgotten(&early), "kept as long as the group, which is kept a ttl after it finishes");

    fixture.clock.advance(ticks(1));
    fixture.scheduler.catch_up();
    assert!(fixture.scheduler.composite_status(&group).is_none());
    assert!(fixture.forgotten(&early) && fixture.forgotten(&late));
    let record = fixture.spy.newest_revision_of(&early).unwrap();
    assert!(
        record.finished && !record.retained_by_composite,
        "the last revision the holders see lets their retention begin"
    );
}

#[test]
fn a_composites_end_names_the_tasks_whose_ends_led_to_it() {
    let mut fixture = Fixture::leading();
    let group = submitted(&mut fixture, group_of(OnError::CollectAll, vec![task_stage("plan", Some(b"a"))]));
    let planner = start_member(&mut fixture, &group, 0);
    let step = flow_of(vec![task_stage("stage", Some(b"s"))]);
    fixture
        .scheduler
        .complete(&worker(), &planner.task_run_id, step.digest(), Completion::Continues(step))
        .unwrap();
    let continuation = CompositeId::continuation_of(&planner.task_run_id);

    run_member(&mut fixture, &continuation, 0, b"staged");

    let ended = finished(&mut fixture);
    let settled_by = |id: &CompositeId| ended.iter().find(|end| end.composite_id == *id).unwrap().settled_by.clone();
    assert_eq!(settled_by(&continuation), [continuation.member_task(0)]);
    assert_eq!(
        settled_by(&group),
        [continuation.member_task(0), group.member_task(0)],
        "the stage that ended the continuation, then the planner whose end that was"
    );
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
fn a_composites_failure_names_the_task_whose_run_failed_through_every_composite_it_ended() {
    let mut fixture = Fixture::leading();
    let flow = submitted(
        &mut fixture,
        flow_of(vec![Stage::Composite(group_of(
            OnError::FailFast,
            vec![task_stage("fine", Some(b"a")), task_stage("explode", Some(b"b"))],
        ))]),
    );
    let group = flow.child(0);

    fail_member(&mut fixture, &group, 1, "ValueError");

    let failed = &finished(&mut fixture)[0];
    assert_eq!(failed.composite_id, flow);
    assert_eq!(failed.failure.as_ref().and_then(|failure| failure.task.clone()), Some(group.member_task(1)));
    let Some(composite_entry::Outcome::Failure(recorded)) = entry(&fixture, &flow, 0).unwrap().outcome else {
        panic!("the flow records its group's failure");
    };
    assert_eq!(recorded.task.map(TaskId::from), Some(group.member_task(1)));
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
    let failures: Vec<Option<(String, String)>> = (0..3)
        .map(|index| match entry(&fixture, &group, index).unwrap().outcome {
            Some(composite_entry::Outcome::Failure(failure)) => Some((failure.kind, failure.message)),
            _ => None,
        })
        .collect();
    assert_eq!(
        failures,
        [
            None,
            Some(("ValueError".into(), "member 1 failed".into())),
            Some(("TaskCancelledError".into(), String::new()))
        ]
    );
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
    run_member(&mut fixture, &map, PAGE_ENTRIES, &PAGE_ENTRIES.to_be_bytes());
    for index in 0..PAGE_ENTRIES - 1 {
        run_member(&mut fixture, &map, index, &index.to_be_bytes());
    }
    assert!(fixture.spy.newest_page(&map, 0).is_none(), "page 0 is not whole yet");
    assert!(fixture.spy.newest_page(&map, 1).is_some(), "page 1 is, with its one member");
    let mark = fixture.spy.mark();
    run_member(&mut fixture, &map, PAGE_ENTRIES - 1, &(PAGE_ENTRIES - 1).to_be_bytes());
    let published = fixture.spy.published_since(mark);
    let position_of = |wanted: &dyn Fn(&ShardRecord) -> bool| published.iter().position(wanted).unwrap();
    let page_zero = position_of(&|record| matches!(record, ShardRecord::Page(page) if page.index == 0));
    let composite = position_of(&|record| matches!(record, ShardRecord::Composite(_)));
    assert!(page_zero < composite, "a page is published before the record that ends the composite");
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
            vec![
                task_stage("a", Some(b"a")),
                task_stage("b", Some(b"b")),
                task_stage("c", Some(b"c")),
                task_stage("d", Some(b"d")),
                task_stage("e", Some(b"e")),
            ],
        ),
    );
    run_member(&mut fixture, &group, 0, b"ok");
    fail_member(&mut fixture, &group, 1, "ValueError");
    start_member(&mut fixture, &group, 2);
    fixture.scheduler.request_claim(&worker(), &group.member_task(3)).unwrap();
    let status = fixture.scheduler.composite_status(&group).unwrap();
    assert_eq!(status.state, CompositeState::Running);
    assert_eq!(
        status.counts,
        MemberCounts { pending: 1, running: 2, succeeded: 1, failed: 1, cancelled: 0 },
        "a member that is claimed or started is running, not pending"
    );
}

#[test]
fn a_returned_step_runs_as_a_composite_and_its_task_finishes_with_it() {
    let mut fixture = Fixture::leading();
    let task = fixture
        .scheduler
        .submit(Submission::new(TaskDefinitionId::new("plan"), 0, b"p".to_vec(), "default"))
        .unwrap();
    let claim = fixture.scheduler.request_claim(&worker(), &task).unwrap();
    fixture.scheduler.report_started(&worker(), &claim.task_run_id).unwrap();
    let step = flow_of(vec![task_stage("stage", Some(b"s"))]);
    assert_eq!(
        fixture.scheduler.complete(
            &worker(),
            &claim.task_run_id,
            Digest::blake3(b"not the step"),
            Completion::Continues(step.clone())
        ),
        Err(ReportRejection::NotAuthoritative),
        "a continuation is certified only under its own digest"
    );
    fixture
        .scheduler
        .complete(&worker(), &claim.task_run_id, step.digest(), Completion::Continues(step.clone()))
        .unwrap();
    let continuation = CompositeId::continuation_of(&claim.task_run_id);
    assert_eq!(fixture.state(&task), TaskRunState::Succeeded);
    assert_eq!(fixture.scheduler.continuation_of_run(&claim.task_run_id), Some(step.clone()));
    let certifying = fixture.spy.newest_revision_of(&task).unwrap();
    assert!(!certifying.finished, "the task is not over yet");
    let run = certifying.runs.last().unwrap();
    assert_eq!(run.current_state(), TaskRunState::Succeeded);
    assert_eq!(
        run.continuation.as_ref().map(|stored| Step::try_from(stored).unwrap()),
        Some(step),
        "the revision that certifies the run carries the step it returned"
    );
    run_member(&mut fixture, &continuation, 0, b"staged");
    let finished = finished(&mut fixture);
    assert_eq!(finished[0].parent_task, Some(task.clone()));
    assert_eq!(finished[0].state, CompositeState::Succeeded);
    assert!(fixture.spy.newest_revision_of(&task).unwrap().finished);
}

#[test]
fn a_client_hears_that_its_task_that_returned_a_step_is_over_from_the_step_alone() {
    let mut fixture = Fixture::leading();
    let client = kabudachi_core::protocol::ids::WorkerId::new("client");
    let task = fixture
        .scheduler
        .submit(Submission::new(TaskDefinitionId::new("plan"), 0, b"p".to_vec(), "default").with_client(client))
        .unwrap();
    let claim = fixture.scheduler.request_claim(&worker(), &task).unwrap();
    fixture.scheduler.report_started(&worker(), &claim.task_run_id).unwrap();
    let step = flow_of(vec![task_stage("stage", Some(b"s"))]);
    fixture
        .scheduler
        .complete(&worker(), &claim.task_run_id, step.digest(), Completion::Continues(step))
        .unwrap();
    assert!(fixture.scheduler.take_conclusions().is_empty(), "the task is not over");
    run_member(&mut fixture, &CompositeId::continuation_of(&claim.task_run_id), 0, b"staged");
    let told = finished(&mut fixture);
    assert_eq!(told[0].parent_task, Some(task));
    assert!(
        fixture.scheduler.take_conclusions().is_empty(),
        "the client takes its end from the step's: a certification of the step's digest would match no result it holds"
    );
}

#[test]
fn a_step_too_large_for_the_task_record_fails_its_run_for_good() {
    let mut fixture = Fixture::leading();
    let task = fixture
        .scheduler
        .submit(Submission::new(TaskDefinitionId::new("plan"), 0, b"p".to_vec(), "default").with_retries(3))
        .unwrap();
    let claim = fixture.scheduler.request_claim(&worker(), &task).unwrap();
    fixture.scheduler.report_started(&worker(), &claim.task_run_id).unwrap();
    let step = flow_of(vec![task_stage("huge", Some(&vec![0; MAX_RECORD_BYTES as usize]))]);
    assert_eq!(
        fixture.scheduler.complete(
            &worker(),
            &claim.task_run_id,
            step.digest(),
            Completion::Continues(step)
        ),
        Err(ReportRejection::ContinuationTooLarge)
    );
    let run = fixture.scheduler.task_run(&claim.task_run_id).unwrap();
    assert_eq!(run.current_state(), TaskRunState::Failed);
    assert_eq!(run.failure_kind, "ContinuationTooLarge");
    assert_eq!(fixture.scheduler.runs_of(&task).len(), 1, "the same body would return the same step");
    assert!(fixture.spy.newest_revision_of(&task).unwrap().finished);
}

#[test]
fn the_largest_step_a_record_can_carry_is_taken_and_one_byte_more_fails_its_run() {
    // Whether a step of this many payload bytes was taken, and the size of the record then published
    // with the empty version the limit is measured at.
    let returns_step_of = |payload: usize| {
        let mut fixture = Fixture::leading();
        let task = fixture
            .scheduler
            .submit(Submission::new(TaskDefinitionId::new("plan"), 0, b"p".to_vec(), "default"))
            .unwrap();
        let claim = fixture.scheduler.request_claim(&worker(), &task).unwrap();
        fixture.scheduler.report_started(&worker(), &claim.task_run_id).unwrap();
        // The first stage keeps the step's task running, so the revision that
        // certifies the run is the newest the task has.
        let step = flow_of(vec![task_stage("first", Some(b"f")), task_stage("big", Some(&vec![0; payload]))]);
        let taken = fixture
            .scheduler
            .complete(&worker(), &claim.task_run_id, step.digest(), Completion::Continues(step))
            .is_ok();
        let mut published = fixture.spy.newest_revision_of(&task).unwrap();
        published.version = Some(Default::default());
        (taken, published.encoded_len() as u64)
    };
    let (mut taken, mut refused) = (0, MAX_RECORD_BYTES as usize);
    assert!(returns_step_of(taken).0 && !returns_step_of(refused).0);
    while refused - taken > 1 {
        let middle = (taken + refused) / 2;
        if returns_step_of(middle).0 {
            taken = middle;
        } else {
            refused = middle;
        }
    }
    let (_, measured) = returns_step_of(taken);
    // A record keeps this much free for what a measurement leaves out.
    let limit = MAX_RECORD_BYTES - 1024;
    assert_eq!(
        measured, limit,
        "the largest step taken fills the limit to the byte, so nothing of the record goes unmeasured"
    );
}

#[test]
fn cancelling_a_task_whose_step_is_running_cancels_the_step_and_the_task_never_runs_again() {
    let mut fixture = Fixture::leading();
    let task = fixture
        .scheduler
        .submit(Submission::new(TaskDefinitionId::new("plan"), 0, b"p".to_vec(), "default").with_retries(3))
        .unwrap();
    let claim = fixture.scheduler.request_claim(&worker(), &task).unwrap();
    fixture.scheduler.report_started(&worker(), &claim.task_run_id).unwrap();
    let step = flow_of(vec![task_stage("slow", Some(b"s")), task_stage("after", None)]);
    fixture
        .scheduler
        .complete(&worker(), &claim.task_run_id, step.digest(), Completion::Continues(step))
        .unwrap();
    let continuation = CompositeId::continuation_of(&claim.task_run_id);
    let slow = start_member(&mut fixture, &continuation, 0);
    assert_eq!(fixture.scheduler.cancel(&task), Ok(Cancellation::Cancelled { was_running: true }));
    assert_eq!(fixture.run_state(&slow.task_run_id), TaskRunState::Cancelled);
    let finished = finished(&mut fixture);
    assert_eq!(finished[0].state, CompositeState::Cancelled);
    assert_eq!(finished[0].parent_task, Some(task.clone()));
    assert_eq!(fixture.scheduler.runs_of(&task).len(), 1, "a continuation's end never re-runs its task");
}

#[test]
fn cancelling_a_group_member_that_already_returned_a_step_cancels_the_step_and_counts_the_member_cancelled() {
    let mut fixture = Fixture::leading();
    let group = submitted(
        &mut fixture,
        group_of(OnError::CollectAll, vec![task_stage("plan", Some(b"a")), task_stage("other", Some(b"b"))]),
    );
    let planner = start_member(&mut fixture, &group, 0);
    let step = flow_of(vec![task_stage("slow", Some(b"s"))]);
    fixture
        .scheduler
        .complete(&worker(), &planner.task_run_id, step.digest(), Completion::Continues(step))
        .unwrap();
    let slow = start_member(&mut fixture, &CompositeId::continuation_of(&planner.task_run_id), 0);
    assert_eq!(
        fixture.scheduler.cancel(&group.member_task(0)),
        Ok(Cancellation::Cancelled { was_running: true }),
        "a member that succeeded with a step is still not over, so it can be cancelled"
    );
    assert_eq!(fixture.run_state(&slow.task_run_id), TaskRunState::Cancelled);
    let status = fixture.scheduler.composite_status(&group).unwrap();
    assert_eq!(status.state, CompositeState::Running, "collect_all waits for the other member");
    assert_eq!(status.counts, MemberCounts { pending: 1, running: 0, succeeded: 0, failed: 0, cancelled: 1 });
}

#[test]
fn stages_that_never_started_count_pending_after_their_flow_is_cancelled() {
    let mut fixture = Fixture::leading();
    let flow = submitted(
        &mut fixture,
        flow_of(vec![task_stage("slow", Some(b"s")), task_stage("second", None), task_stage("third", None)]),
    );
    start_member(&mut fixture, &flow, 0);
    assert_eq!(
        fixture.scheduler.cancel_composite(&flow),
        Ok(Cancellation::Cancelled { was_running: true })
    );
    let status = fixture.scheduler.composite_status(&flow).unwrap();
    assert_eq!(status.state, CompositeState::Cancelled);
    assert_eq!(status.counts, MemberCounts { pending: 2, running: 0, succeeded: 0, failed: 0, cancelled: 1 });
}

/// The map member a worker builds for one item it stored.
fn item_member(item: &[u8]) -> Stage {
    let Stage::Task(mut member) = task_stage("each", None) else { unreachable!("a task stage") };
    member.input = BoundInput::Chunked(Manifest::new(vec![ManifestEntry::Blob(BlobKey::of(item))], 0));
    Stage::Task(member)
}

fn map_over_previous() -> Stage {
    Stage::Composite(Step {
        kind: CompositeKind::Map,
        on_error: OnError::FailFast,
        stages: vec![task_stage("each", None)],
        streamed: false,
    })
}

#[test]
fn a_map_after_a_stage_is_split_by_an_expansion_run_and_the_next_stage_takes_the_items_results() {
    let mut fixture = Fixture::leading();
    let flow = submitted(
        &mut fixture,
        flow_of(vec![task_stage("load", Some(b"x")), map_over_previous(), task_stage("report", None)]),
    );
    run_member(&mut fixture, &flow, 0, b"[1, 2]");
    let map = flow.child(1);
    let expansion = start_member(&mut fixture, &map, 0);
    assert_eq!(expansion.task.task_definition_id().as_str(), EXPANSION_DEFINITION);
    let asked = Expansion::decode(&expansion.task.serialized_input[..]).unwrap();
    assert_eq!(
        Manifest::try_from(asked.source.as_ref().unwrap()).unwrap(),
        Manifest::new(vec![ManifestEntry::Blob(BlobKey::of(b"[1, 2]"))], 0)
    );
    assert_eq!(asked.producer, Some(TaskDefinitionId::new("load").into()));
    let items = Step {
        kind: CompositeKind::Map,
        on_error: OnError::FailFast,
        stages: vec![item_member(b"1"), item_member(b"2")],
        streamed: false,
    };
    fixture
        .scheduler
        .complete(&worker(), &expansion.task_run_id, items.digest(), Completion::Continues(items))
        .unwrap();
    let members = CompositeId::continuation_of(&expansion.task_run_id);
    for index in 0..2 {
        run_member(&mut fixture, &members, index, &[index as u8]);
    }
    let report = run_member(&mut fixture, &flow, 2, b"reported");
    assert_eq!(
        report.input_manifest().unwrap(),
        Some(Manifest::new(vec![ManifestEntry::Page { composite: members, page: 0 }], 0)),
        "the stage after the map takes the items' results, not the expansion's"
    );
    assert_eq!(finished(&mut fixture)[0].state, CompositeState::Succeeded);
}

#[test]
fn a_list_that_splits_into_no_items_gives_the_next_stage_an_empty_list() {
    let mut fixture = Fixture::leading();
    let flow = submitted(
        &mut fixture,
        flow_of(vec![task_stage("load", Some(b"x")), map_over_previous(), task_stage("report", None)]),
    );
    run_member(&mut fixture, &flow, 0, b"[]");
    let expansion = start_member(&mut fixture, &flow.child(1), 0);
    let items = Step { kind: CompositeKind::Map, on_error: OnError::FailFast, stages: Vec::new(), streamed: false };
    fixture
        .scheduler
        .complete(&worker(), &expansion.task_run_id, items.digest(), Completion::Continues(items))
        .unwrap();
    let report = run_member(&mut fixture, &flow, 2, b"reported");
    assert_eq!(report.input_manifest().unwrap(), Some(Manifest::new(Vec::new(), 0)));
    assert_eq!(finished(&mut fixture)[0].state, CompositeState::Succeeded);
}

#[test]
fn an_expansion_that_fails_fails_its_map_and_the_flow_whatever_the_maps_error_policy() {
    for on_error in [OnError::FailFast, OnError::CollectAll] {
        let mut fixture = Fixture::leading();
        let map = Stage::Composite(Step {
            kind: CompositeKind::Map,
            on_error,
            stages: vec![task_stage("each", None)],
            streamed: false,
        });
        let flow = submitted(&mut fixture, flow_of(vec![task_stage("load", Some(b"x")), map]));
        run_member(&mut fixture, &flow, 0, b"not a list");
        fail_member(&mut fixture, &flow.child(1), 0, "SerializationError");
        let finished = finished(&mut fixture);
        assert_eq!(finished[0].state, CompositeState::Failed);
        assert_eq!(finished[0].failure.as_ref().unwrap().kind, "SerializationError");
    }
}

#[test]
fn a_finished_composites_record_gives_the_finish_its_client_was_told_and_a_running_one_gives_none() {
    let mut fixture = Fixture::leading();
    let flow = submitted(
        &mut fixture,
        flow_of(vec![task_stage("explode", Some(b"x")), task_stage("after", None)]),
    );
    assert_eq!(CompositeFinished::of_record(&fixture.spy.newest_composite(&flow).unwrap()), None);
    fail_member(&mut fixture, &flow, 0, "ValueError");
    let told = finished(&mut fixture);
    let record = fixture.spy.newest_composite(&flow).unwrap();
    // The record cannot say which Tasks' ends led to it.
    let told_by_record = CompositeFinished { settled_by: Vec::new(), ..told[0].clone() };
    assert_eq!(CompositeFinished::of_record(&record), Some(told_by_record));

    // A task's continuation is told with the task that returned it.
    let task = fixture
        .scheduler
        .submit(Submission::new(TaskDefinitionId::new("plan"), 0, b"p".to_vec(), "default"))
        .unwrap();
    let claim = fixture.scheduler.request_claim(&worker(), &task).unwrap();
    fixture.scheduler.report_started(&worker(), &claim.task_run_id).unwrap();
    let step = flow_of(vec![task_stage("stage", Some(b"s"))]);
    fixture
        .scheduler
        .complete(&worker(), &claim.task_run_id, step.digest(), Completion::Continues(step))
        .unwrap();
    let continuation = CompositeId::continuation_of(&claim.task_run_id);
    run_member(&mut fixture, &continuation, 0, b"staged");
    let told = finished(&mut fixture);
    let record = fixture.spy.newest_composite(&continuation).unwrap();
    let told_by_record = CompositeFinished { settled_by: Vec::new(), ..told[0].clone() };
    assert_eq!(CompositeFinished::of_record(&record), Some(told_by_record));
    assert_eq!(told[0].parent_task, Some(task));
}
