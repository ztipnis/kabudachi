//! A leader dies part-way through storing a decision about a composite;
//! what its successor does with what reached the holders. Each case keeps
//! the newest revision of every record except those the old leader's last
//! call wrote to the records it names.

use std::collections::BTreeMap;

use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::generated::{CompositeEntry, CompositePage, composite_entry};
use kabudachi_core::protocol::ids::{CompositeId, TaskDefinitionId, TaskId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::reconcile::{Rebuild, ReportedRun, ReportedState, WorkerRuns};
use kabudachi_core::scheduler::composite::{
    CompositeKind, CompositeState, OnError, Stage, Step,
};
use kabudachi_core::scheduler::{CancelRejection, ClaimRejection, Completion, Submission};
use kabudachi_core::task_record::RecordKey;

use crate::support::composites::*;
use crate::support::scheduler::{Fixture, ticks};

/// The stage after a group runs as one certified run per group, whatever
/// part of the group's end reached the holders: the last member's
/// certification is stored either way, and either the next stage's Task
/// record or the composites' records were lost.
#[test]
fn the_stage_after_a_group_runs_once_whichever_record_of_the_groups_end_was_lost() {
    for lose_the_next_stage in [true, false] {
        let mut old = Fixture::leading();
        let flow = submitted(
            &mut old,
            flow_of(vec![
                Stage::Composite(group_of(
                    OnError::FailFast,
                    vec![task_stage("a", Some(b"a")), task_stage("b", Some(b"b"))],
                )),
                task_stage("after", None),
            ]),
        );
        let group = flow.child(0);
        run_member(&mut old, &group, 0, b"a");
        let mark = old.spy.mark();
        run_member(&mut old, &group, 1, b"b");
        let after = flow.member_task(1);
        let rebuild = rebuild_losing(&old, mark, |key| {
            if lose_the_next_stage {
                *key == RecordKey::Task(after.clone())
            } else {
                is_composite_or_page(key)
            }
        });
        let mut new = took_over(&old, rebuild);
        let claim = run_member(&mut new, &flow, 1, b"after");
        assert_eq!(claim.attempt_number, 1);
        assert_eq!(new.scheduler.runs_of(&after).len(), 1, "one Task, one run");
        let finished = finished(&mut new);
        assert_eq!(finished.len(), 1);
        assert_eq!(finished[0].state, CompositeState::Succeeded);
        assert_eq!(
            entry(&new, &flow, 1).unwrap().outcome,
            Some(composite_entry::Outcome::Value(Digest::blake3(b"after").into()))
        );
    }
}

/// A member kept for its group by the old leader is kept by the successor
/// too, and let go with the group; a member whose group record was lost has
/// nothing to wait for.
#[test]
fn a_successor_keeps_a_finished_member_for_its_group_and_lets_it_go_with_the_group() {
    let mut old = Fixture::leading();
    old.scheduler.set_result_ttl(Some(ticks(100)));
    let group = submitted(
        &mut old,
        group_of(OnError::CollectAll, vec![task_stage("a", Some(b"a")), task_stage("b", Some(b"b"))]),
    );
    run_member(&mut old, &group, 0, b"a");
    let (first, second) = (group.member_task(0), group.member_task(1));

    let mut lost_group = took_over(&old, rebuild_losing(&old, 0, |_| false));
    lost_group.scheduler.set_result_ttl(Some(ticks(100)));
    let mut orphaned = took_over(&old, rebuild_losing(&old, 0, is_composite_or_page));
    orphaned.scheduler.set_result_ttl(Some(ticks(100)));

    for fixture in [&mut lost_group, &mut orphaned] {
        fixture.clock.advance(ticks(250));
        fixture.scheduler.catch_up();
    }
    assert!(!lost_group.forgotten(&first), "its group is still held and unfinished");
    assert!(orphaned.forgotten(&first), "its group's record is gone: nothing to keep it for");

    run_member(&mut lost_group, &group, 1, b"b");
    lost_group.clock.advance(ticks(100));
    lost_group.scheduler.catch_up();
    assert!(lost_group.forgotten(&first) && lost_group.forgotten(&second));
}

/// A continuation is committed by the revision that certifies its run: the
/// step is on the run. The composite's record and pages were lost with the
/// leader and no worker reports anything, yet the successor derives the
/// same composite from the run alone. Its failure ends the task's
/// continuation and never re-runs the task, and the continuation holds the
/// task's coalescing key until it is over.
#[test]
fn a_continuation_committed_by_its_certifying_revision_is_rebuilt_from_the_run_when_its_composite_record_was_lost() {
    let mut old = Fixture::leading();
    let generation = || Submission::new(TaskDefinitionId::new("plan"), 0, b"p".to_vec(), "default").with_coalescing_key("k");
    let task = old.scheduler.submit(generation()).unwrap();
    let claim = old.scheduler.request_claim(&worker(), &task).unwrap();
    old.scheduler.report_started(&worker(), &claim.task_run_id).unwrap();
    let step = flow_of(vec![task_stage("explode", Some(b"x"))]);
    let mark = old.spy.mark();
    old.scheduler
        .complete(&worker(), &claim.task_run_id, step.digest(), Completion::Continues(step))
        .unwrap();
    let waiting = old.scheduler.submit(generation()).unwrap();
    // No report: the run's revision alone must be enough.
    let mut new = took_over(&old, rebuild_losing(&old, mark, is_composite_or_page));
    let continuation = CompositeId::continuation_of(&claim.task_run_id);
    assert!(new.scheduler.composite_status(&continuation).is_some(), "derived under the same id");
    assert_eq!(
        new.scheduler.request_claim(&worker(), &waiting),
        Err(ClaimRejection::KeyBusy),
        "the continuation still holds its key"
    );
    fail_member(&mut new, &continuation, 0, "ValueError");
    let finished = finished(&mut new);
    assert_eq!(finished.len(), 1);
    assert_eq!(finished[0].parent_task, Some(task.clone()));
    assert_eq!(finished[0].state, CompositeState::Failed);
    assert_eq!(new.scheduler.runs_of(&task).len(), 1, "the returning task never runs again");
    assert!(new.scheduler.request_claim(&worker(), &waiting).is_ok(), "and its key is free");
}

/// A client asks a new leader about a task whose returned step finished under
/// the old one. The answer says the run returned a step, so the client waits
/// for the step's end, not a certification of the step's digest, which would
/// match no result it holds. A task that returned a result says no such thing.
#[test]
fn a_new_leader_answers_a_finished_continuing_task_as_continued() {
    let client = kabudachi_core::protocol::ids::WorkerId::new("client");
    let mut old = Fixture::leading();
    let plain = old
        .scheduler
        .submit(Submission::new(TaskDefinitionId::new("plain"), 0, b"p".to_vec(), "default").with_client(client.clone()))
        .unwrap();
    let plain_claim = old.scheduler.request_claim(&worker(), &plain).unwrap();
    old.scheduler.report_started(&worker(), &plain_claim.task_run_id).unwrap();
    old.scheduler
        .complete(&worker(), &plain_claim.task_run_id, Digest::blake3(b"result"), Completion::Final)
        .unwrap();
    let task = old
        .scheduler
        .submit(Submission::new(TaskDefinitionId::new("plan"), 0, b"p".to_vec(), "default").with_client(client))
        .unwrap();
    let claim = old.scheduler.request_claim(&worker(), &task).unwrap();
    old.scheduler.report_started(&worker(), &claim.task_run_id).unwrap();
    let step = flow_of(vec![task_stage("stage", Some(b"s"))]);
    old.scheduler
        .complete(&worker(), &claim.task_run_id, step.digest(), Completion::Continues(step.clone()))
        .unwrap();
    let continuation = CompositeId::continuation_of(&claim.task_run_id);
    run_member(&mut old, &continuation, 0, b"staged");
    assert_eq!(finished(&mut old)[0].parent_task, Some(task.clone()));

    let mark = old.spy.mark();
    let mut new = took_over(&old, rebuild_losing(&old, mark, |_| false));
    let continued = new.scheduler.outcome(&task).unwrap().expect("the new leader holds it");
    assert_eq!(
        (continued.state, continued.finished, continued.continued, continued.result_digest),
        (TaskRunState::Succeeded, true, true, Some(step.digest()))
    );
    let plain = new.scheduler.outcome(&plain).unwrap().expect("the new leader holds it");
    assert_eq!((plain.finished, plain.continued), (true, false));
}

/// The certifying revision itself was not stored: the run is still
/// running in the records, and only the worker's report says it returned
/// a step. The report certifies it with that step, as any report certifies
/// a run.
#[test]
fn a_run_whose_certifying_revision_was_lost_is_certified_with_its_step_from_the_workers_report() {
    let mut old = Fixture::leading();
    let task = old
        .scheduler
        .submit(Submission::new(TaskDefinitionId::new("plan"), 0, b"p".to_vec(), "default"))
        .unwrap();
    let claim = old.scheduler.request_claim(&worker(), &task).unwrap();
    old.scheduler.report_started(&worker(), &claim.task_run_id).unwrap();
    let step = flow_of(vec![task_stage("stage", Some(b"x"))]);
    let mark = old.spy.mark();
    old.scheduler
        .complete(&worker(), &claim.task_run_id, step.digest(), Completion::Continues(step.clone()))
        .unwrap();
    let mut rebuild = rebuild_losing(&old, mark, |_| true);
    rebuild.reports = BTreeMap::from([(
        worker(),
        WorkerRuns {
            runs: vec![ReportedRun {
                claim: claim.clone(),
                state: ReportedState::Succeeded { result_digest: step.digest(), delivered: false },
                continuation: Some(step),
            }],
            asked_at: None,
        },
    )]);
    let mut new = took_over(&old, rebuild);
    let continuation = CompositeId::continuation_of(&claim.task_run_id);
    run_member(&mut new, &continuation, 0, b"staged");
    assert_eq!(finished(&mut new)[0].parent_task, Some(task.clone()));
    assert_eq!(new.scheduler.runs_of(&task).len(), 1);
}

#[test]
fn a_fail_fast_group_whose_member_failed_as_its_leader_died_fails_once_and_the_next_stage_never_starts() {
    let mut old = Fixture::leading();
    let flow = submitted(
        &mut old,
        flow_of(vec![
            Stage::Composite(group_of(
                OnError::FailFast,
                vec![task_stage("explode", Some(b"a")), task_stage("slow", Some(b"b"))],
            )),
            task_stage("after", None),
        ]),
    );
    let group = flow.child(0);
    let slow = start_member(&mut old, &group, 1);
    let mark = old.spy.mark();
    fail_member(&mut old, &group, 0, "ValueError");
    let mut new = took_over(&old, rebuild_losing(&old, mark, is_composite_or_page));
    let finished = finished(&mut new);
    assert_eq!(finished.len(), 1, "the flow fails once");
    assert_eq!(finished[0].composite_id, flow);
    assert_eq!(finished[0].state, CompositeState::Failed);
    assert_eq!(
        new.scheduler.request_claim(&worker(), &flow.member_task(1)),
        Err(ClaimRejection::TaskUnknown),
        "the stage after the group is never made"
    );
    assert_eq!(new.run_state(&slow.task_run_id), TaskRunState::Running, "the running member is left alone");
}

#[test]
fn a_composite_whose_member_record_is_not_known_yet_waits_for_it_before_it_moves_on() {
    let mut old = Fixture::leading();
    let flow = submitted(&mut old, flow_of(vec![task_stage("a", Some(b"a")), task_stage("b", None)]));
    let mark = old.spy.mark();
    run_member(&mut old, &flow, 0, b"a");
    let first: TaskId = flow.member_task(0);
    // The second stage's Task was made in the same call, and is lost too.
    let second = RecordKey::Task(flow.member_task(1));
    let mut rebuild = rebuild_losing(&old, mark, |key| is_composite_or_page(key) || *key == second);
    let known = rebuild
        .records
        .iter()
        .position(|record| record.task.as_ref().unwrap().task_id() == first)
        .unwrap();
    let record = rebuild.records.remove(known);
    rebuild.uncertain.insert(first.clone(), Default::default());
    let mut new = took_over(&old, rebuild);
    assert_eq!(
        new.scheduler.request_claim(&worker(), &flow.member_task(1)),
        Err(ClaimRejection::TaskUnknown),
        "nothing moves on while the first stage's end is unknown"
    );
    new.scheduler
        .adopt(Rebuild { records: vec![record], ..Rebuild::default() })
        .unwrap();
    assert!(new.scheduler.request_claim(&worker(), &flow.member_task(1)).is_ok());
}

/// A group member returned a step; the step ended and its task finished,
/// but neither the continuation's final revision nor the group's page for
/// the member was stored. The members' own records say all of it.
#[test]
fn a_group_member_whose_continuation_ended_as_its_leader_died_settles_with_that_continuation() {
    let mut old = Fixture::leading();
    let group = submitted(
        &mut old,
        group_of(OnError::CollectAll, vec![task_stage("plan", Some(b"p")), task_stage("other", Some(b"o"))]),
    );
    let plan = start_member(&mut old, &group, 0);
    let continuation = continue_with_a_stage(&mut old, &plan.task_run_id);
    let mark = old.spy.mark();
    end_continuation(&mut old, &continuation);
    let mut new = took_over(&old, rebuild_losing(&old, mark, is_composite_or_page));
    run_member(&mut new, &group, 1, b"o");
    let finished = finished(&mut new);
    let ended = finished.iter().find(|ended| ended.composite_id == group).expect("the group ends");
    assert_eq!(ended.state, CompositeState::Succeeded);
    assert_eq!(
        entry(&new, &group, 0).unwrap().outcome,
        Some(composite_entry::Outcome::Child(continuation.clone().into())),
        "the member's entry names its continuation, not the step it returned"
    );
    assert_eq!(new.scheduler.composite_status(&continuation).unwrap().state, CompositeState::Succeeded);
}

/// A flow's map stage was split by an expansion run: the items are a
/// composite of the expansion's task, which the map's results name. They
/// are kept as long as the flow that holds the map is, though the map's
/// own members long since finished, and a successor keeps them the same.
#[test]
fn the_items_an_expansion_split_are_kept_as_long_as_the_flow_that_names_them() {
    let ttl = ticks(100);
    for takeover in [false, true] {
        let mut fixture = Fixture::leading();
        fixture.scheduler.set_result_ttl(Some(ttl));
        let map = Stage::Composite(Step {
            kind: CompositeKind::Map,
            on_error: OnError::FailFast,
            stages: vec![task_stage("each", None)],
            streamed: false,
        });
        let flow = submitted(
            &mut fixture,
            flow_of(vec![task_stage("load", Some(b"x")), map, task_stage("report", Some(b"r"))]),
        );
        run_member(&mut fixture, &flow, 0, b"[1]");
        let split = flow.child(1);
        let expansion = fixture.scheduler.request_claim(&worker(), &split.expansion_task()).unwrap();
        fixture.scheduler.report_started(&worker(), &expansion.task_run_id).unwrap();
        fixture.scheduler.submit_members(item_batch(&split, 0, 1, false)).unwrap();
        fixture
            .scheduler
            .complete(&worker(), &expansion.task_run_id, Digest::blake3(b""), Completion::Final)
            .unwrap();
        run_member(&mut fixture, &split, 0, b"1!");
        assert_eq!(
            fixture.scheduler.composite_status(&flow.child(1)).unwrap().state,
            CompositeState::Succeeded
        );
        // The flow's last stage is still to run; the map and its items are over.
        let mut leading = if takeover {
            let rebuild = rebuild_losing(&fixture, usize::MAX, |_| false);
            took_over(&fixture, rebuild)
        } else {
            fixture
        };
        leading.scheduler.set_result_ttl(Some(ttl));
        leading.clock.advance(ttl);
        leading.clock.advance(ttl);
        leading.scheduler.catch_up();
        assert!(
            leading.scheduler.composite_page(&split, 0).is_some(),
            "the items the map's results name outlive their own ttl while the flow runs (takeover: {takeover})"
        );
        assert!(leading.scheduler.composite_record(&flow.child(1)).is_some());
        run_member(&mut leading, &flow, 2, b"reported");
        leading.clock.advance(ttl);
        leading.scheduler.catch_up();
        assert!(leading.scheduler.composite_record(&flow).is_none(), "the flow goes (takeover: {takeover})");
        assert!(
            leading.scheduler.composite_record(&flow.child(1)).is_none(),
            "and the map it held (takeover: {takeover})"
        );
        assert!(
            leading.scheduler.composite_page(&split, 0).is_none(),
            "and the items' results (takeover: {takeover})"
        );
    }
}

#[test]
fn a_composite_record_learnt_after_the_rebuild_is_written_again_at_the_new_term() {
    let mut old = Fixture::leading();
    let flow = submitted(&mut old, flow_of(vec![task_stage("a", Some(b"a")), task_stage("b", None)]));
    let mut rebuild = rebuild_losing(&old, 0, |_| false);
    let composites = std::mem::take(&mut rebuild.composites);
    let pages = std::mem::take(&mut rebuild.pages);
    let mut new = took_over(&old, rebuild);
    assert_eq!(new.spy.newest_composite(&flow), None, "the rebuild did not know the composite");
    new.scheduler
        .adopt(Rebuild { composites, pages, ..Rebuild::default() })
        .unwrap();
    assert!(new.scheduler.composite_status(&flow).is_some());
    let written = new.spy.newest_composite(&flow).expect("the composite learnt late is published");
    let version = written.version.unwrap();
    assert_eq!(version.leader_term, 2, "it is written at the new leader's term, fencing the old leader's writes");
}

/// A composite's record is certain at the rebuild but a page of its member
/// outcomes is not. The composite waits, and once the page is learnt late it
/// holds the outcome it names and moves on as if it had been known.
#[test]
fn a_composite_whose_page_is_learnt_after_the_rebuild_moves_on_once_it_is_learnt() {
    let mut old = Fixture::leading();
    let group = submitted(
        &mut old,
        group_of(OnError::CollectAll, vec![task_stage("a", Some(b"a")), task_stage("b", Some(b"b"))]),
    );
    run_member(&mut old, &group, 0, b"a");
    let mut rebuild = rebuild_losing(&old, 0, |_| false);
    assert!(rebuild.pages.is_empty(), "a page is not written before it is whole");
    let page = CompositePage {
        version: rebuild.composites[0].version.clone(),
        entries: vec![CompositeEntry {
            index: 0,
            outcome: Some(composite_entry::Outcome::Value(Digest::blake3(b"a").into())),
            producer: Some(TaskDefinitionId::new("a").into()),
        }],
        ..page(&group, 0, 0, 0)
    };
    let pages = vec![page];
    rebuild.uncertain_composites.insert(group.clone());
    let mut new = took_over(&old, rebuild);
    new.scheduler
        .adopt(Rebuild { pages, ..Rebuild::default() })
        .unwrap();
    run_member(&mut new, &group, 1, b"b");
    let finished = finished(&mut new);
    assert_eq!(finished.len(), 1, "the group ends once the page is known");
    assert_eq!(finished[0].state, CompositeState::Succeeded);
    assert_eq!(
        entry(&new, &group, 0).unwrap().outcome,
        Some(composite_entry::Outcome::Value(Digest::blake3(b"a").into()))
    );
}

/// A worker reports a run that returned a step while the record of the
/// step's composite is not known yet. The composite is not made over the
/// record that may exist: it is held when the record is learnt, and the
/// step's stage then runs it to its end.
#[test]
fn a_continuation_certified_while_its_composite_record_is_unknown_is_taken_from_the_record_learnt_later() {
    let mut old = Fixture::leading();
    let task = old
        .scheduler
        .submit(Submission::new(TaskDefinitionId::new("plan"), 0, b"p".to_vec(), "default"))
        .unwrap();
    let claim = old.scheduler.request_claim(&worker(), &task).unwrap();
    old.scheduler.report_started(&worker(), &claim.task_run_id).unwrap();
    let step = flow_of(vec![task_stage("stage", Some(b"x"))]);
    let mark = old.spy.mark();
    old.scheduler
        .complete(&worker(), &claim.task_run_id, step.digest(), Completion::Continues(step.clone()))
        .unwrap();
    let continuation = CompositeId::continuation_of(&claim.task_run_id);
    let known_later = rebuild_losing(&old, 0, |_| false);

    let mut rebuild = rebuild_losing(&old, mark, |_| true);
    rebuild.uncertain_composites.insert(continuation.clone());
    rebuild.reports = BTreeMap::from([(
        worker(),
        WorkerRuns {
            runs: vec![ReportedRun {
                claim: claim.clone(),
                state: ReportedState::Succeeded { result_digest: step.digest(), delivered: false },
                continuation: Some(step),
            }],
            asked_at: None,
        },
    )]);
    let mut new = took_over(&old, rebuild);
    assert!(new.scheduler.composite_status(&continuation).is_none(), "the record may exist: nothing is made over it");
    assert_eq!(
        new.scheduler.cancel(&task),
        Err(CancelRejection::NotReady),
        "a cancel cannot reach a step whose record is unknown, and says to ask again"
    );
    new.scheduler
        .adopt(Rebuild { composites: known_later.composites, pages: known_later.pages, records: known_later.records, ..Rebuild::default() })
        .unwrap();
    run_member(&mut new, &continuation, 0, b"staged");
    let finished = finished(&mut new);
    assert_eq!(finished.len(), 1);
    assert_eq!(finished[0].parent_task, Some(task));
    assert_eq!(finished[0].state, CompositeState::Succeeded);
}

/// The old leader stored a batch's member records but not the map's record.
/// Its successor counts the members it finds: the submitter, whether it carries
/// on with the next batch or asks for the lost one again, is taken once, and
/// the map ends when its last batch comes.
#[test]
fn a_streamed_maps_batch_whose_record_was_lost_with_its_leader_is_counted_from_its_members() {
    let mut old = Fixture::leading();
    let map = submitted(&mut old, streamed_map(OnError::FailFast));
    old.scheduler.submit_members(item_batch(&map, 0, 2, false)).unwrap();
    let mark = old.spy.mark();
    old.scheduler.submit_members(item_batch(&map, 2, 3, false)).unwrap();
    let lose_the_record = |key: &RecordKey| matches!(key, RecordKey::Composite(_));

    let mut carries_on = took_over(&old, rebuild_losing(&old, mark, lose_the_record));
    assert_eq!(
        carries_on.scheduler.submit_members(item_batch(&map, 5, 0, true)).unwrap().members,
        5,
        "the members found count, so the next batch is no gap"
    );
    let mut asks_again = took_over(&old, rebuild_losing(&old, mark, lose_the_record));
    assert_eq!(
        asks_again.scheduler.submit_members(item_batch(&map, 2, 3, false)).unwrap().members,
        5,
        "the batch asked again creates nothing"
    );
    asks_again.scheduler.submit_members(item_batch(&map, 5, 0, true)).unwrap();
    for new in [&mut carries_on, &mut asks_again] {
        for index in 0..5 {
            let claim = run_member(new, &map, index, b"r");
            assert_eq!(new.scheduler.runs_of(&map.member_task(index)).len(), 1);
            assert_eq!(claim.task.serialized_input, item(index));
        }
        assert_eq!(finished(new)[0].state, CompositeState::Succeeded);
        let record = new.spy.newest_composite(&map).unwrap();
        assert_eq!((record.members, record.sealed), (5, true));
    }
}
