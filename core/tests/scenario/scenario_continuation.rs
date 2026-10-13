//! A run that returned a step, whose leader and worker are both lost once
//! the revision certifying it is stored: the step was committed by that
//! revision, so the shard runs it.

use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::ids::{CompositeId, TaskDefinitionId};
use kabudachi_core::protocol::records::TaskRunRecord;
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::scheduler::Submission;
use kabudachi_core::scheduler::composite::CompositeState;
use kabudachi_core::task_record::RecordKey;

use super::scenario_reconcile::{await_new_leader, cut_off, newest_record, some_other};
use super::scenario_records::{elected_among, run_until_settled, running, submitted_with};
use crate::support::composites::{flow_of, task_stage};
use crate::support::harness::Answer;

#[test]
fn a_continuation_runs_once_and_frees_its_key_when_its_leader_and_worker_die_before_its_composite_is_stored() {
    let (mut cluster, leader) = elected_among(5);
    let generation =
        || Submission::new(TaskDefinitionId::new("plan"), 0, b"p".to_vec(), "default").with_coalescing_key("k");
    let task = submitted_with(&mut cluster, &leader, generation());
    let worker = some_other(&cluster, &[&leader]);
    let run = running(&mut cluster, &leader, &worker, &task);
    // Only Task revisions reach their holders from here on.
    cluster
        .records()
        .refuse_writes_of(|key| matches!(key, RecordKey::Composite(_) | RecordKey::Page(..)));
    let step = flow_of(vec![task_stage("stage", Some(b"s"))]);
    let certified = cluster.complete_continuing(&leader, &worker, &run, step);
    run_until_settled(&mut cluster);
    assert_eq!(
        cluster.answer(certified),
        Some(&Answer::NotLeader),
        "the worker never hears its certification: the composite's writes never stored"
    );
    assert_eq!(
        newest_record(&cluster, &task).unwrap().runs.last().unwrap().current_state(),
        TaskRunState::Succeeded,
        "the certifying revision is stored"
    );
    let continuation = CompositeId::continuation_of(&run);
    assert!(cluster.newest_composite(&continuation).is_none());
    let waiting = submitted_with(&mut cluster, &leader, generation());
    // Both are lost: the worker, with whatever it would report, and the
    // leader, cut off before any repair of its refused writes can land.
    cluster.set_down(&worker);
    cut_off(&mut cluster, &leader);
    cluster.records().refuse_writes_of(|_| false);
    let successor = await_new_leader(&mut cluster, &leader);
    let member = continuation.member_task(0);
    let claimant = some_other(&cluster, &[&leader, &worker, &successor]);
    let member_run = running(&mut cluster, &successor, &claimant, &member);
    cluster.complete(&successor, &claimant, &member_run, Digest::blake3(b"staged"));
    run_until_settled(&mut cluster);
    let scheduler = cluster.scheduler_mut(&successor);
    assert_eq!(scheduler.runs_of(&member).len(), 1, "the continuation's stage ran once");
    assert_eq!(scheduler.runs_of(&task).len(), 1, "the returning task never ran again");
    assert_eq!(scheduler.composite_status(&continuation).unwrap().state, CompositeState::Succeeded);
    assert!(newest_record(&cluster, &task).unwrap().finished, "the task finished with its continuation");
    // `running` panics unless the claim is accepted: the key is free.
    running(&mut cluster, &successor, &claimant, &waiting);
}
