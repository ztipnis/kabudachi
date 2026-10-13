//! A leader dies part-way through storing a decision about a composite;
//! what its successor does with what reached the holders. Each case keeps
//! the newest revision of every record except those the old leader's last
//! call wrote to the records it names.

use std::collections::BTreeMap;

use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::generated::composite_entry;
use kabudachi_core::protocol::ids::{CompositeId, TaskDefinitionId, TaskId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::reconcile::{Rebuild, ReportedRun, ReportedState, WorkerRuns};
use kabudachi_core::scheduler::composite::{
    BoundInput, CompositeKind, CompositeState, OnError, Stage, Step,
};
use kabudachi_core::scheduler::{ClaimRejection, Completion, Submission};
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
        });
        let flow = submitted(
            &mut fixture,
            flow_of(vec![task_stage("load", Some(b"x")), map, task_stage("report", Some(b"r"))]),
        );
        run_member(&mut fixture, &flow, 0, b"[1]");
        let expansion = start_member(&mut fixture, &flow.child(1), 0);
        let Stage::Task(mut item) = task_stage("each", None) else { unreachable!("a task stage") };
        item.input = BoundInput::Value(b"1".to_vec());
        let items = Step { kind: CompositeKind::Map, on_error: OnError::FailFast, stages: vec![Stage::Task(item)] };
        fixture
            .scheduler
            .complete(&worker(), &expansion.task_run_id, items.digest(), Completion::Continues(items))
            .unwrap();
        let split = CompositeId::continuation_of(&expansion.task_run_id);
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
        assert!(leading.scheduler.composite_record(&split).is_none(), "and the items (takeover: {takeover})");
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
