use std::collections::BTreeMap;

use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::generated::{
    self, CompositeEntry, CompositePage, CompositeRecord, RecordVersion,
};
use kabudachi_core::protocol::ids::{CompositeId, TaskDefinitionId, TaskRunId, WorkerId};
use kabudachi_core::scheduler::composite::{
    BoundInput, BoundStep, CompositeFinished, CompositeKind, OnError, PAGE_ENTRIES, Stage, Step,
    TaskOptions,
};
use kabudachi_core::reconcile::Rebuild;
use kabudachi_core::scheduler::{Claim, Completion, Event};
use kabudachi_core::task_record::{RecordKey, ShardRecord};

use crate::support::scheduler::{Fixture, OFFICE, grant_of, reconciling_after};

fn version(term: u64, revision: u64) -> Option<RecordVersion> {
    Some(RecordVersion {
        recovery_epoch: 0,
        recovery_lineage: 0,
        leader_term: term,
        revision,
    })
}

/// A flow of one task, the step every record below describes.
pub fn one_stage_flow() -> Step {
    Step {
        kind: CompositeKind::Flow,
        on_error: OnError::FailFast,
        stages: vec![Stage::Task(BoundStep {
            definition: TaskDefinitionId::new("work"),
            version: 0,
            options: TaskOptions::on_queue("default"),
            input: BoundInput::Value(b"x".to_vec()),
            fields: Vec::new(),
        })],
    }
}

/// A composite's record at epoch (0, 0), with no placement.
pub fn composite_record(id: &CompositeId, term: u64, revision: u64) -> CompositeRecord {
    CompositeRecord {
        version: version(term, revision),
        composite_id: Some(id.clone().into()),
        description: Some(generated::StepDescription::from(&one_stage_flow())),
        ..Default::default()
    }
}

/// Page `index` of a composite's outcomes at epoch (0, 0), with no placement.
pub fn page(id: &CompositeId, index: u32, term: u64, revision: u64) -> CompositePage {
    CompositePage {
        version: version(term, revision),
        composite_id: Some(id.clone().into()),
        index,
        ..Default::default()
    }
}

pub fn worker() -> WorkerId {
    WorkerId::new("w1")
}

/// A step running `name` on its input, or on `value` when bound.
pub fn task_stage(name: &str, value: Option<&[u8]>) -> Stage {
    Stage::Task(BoundStep {
        definition: TaskDefinitionId::new(name),
        version: 0,
        options: TaskOptions::on_queue("default"),
        input: value.map_or(BoundInput::Previous, |value| BoundInput::Value(value.to_vec())),
        fields: Vec::new(),
    })
}

pub fn flow_of(stages: Vec<Stage>) -> Step {
    Step { kind: CompositeKind::Flow, on_error: OnError::FailFast, stages }
}

/// A group of `members` run side by side under `on_error`.
pub fn group_of(on_error: OnError, members: Vec<Stage>) -> Step {
    Step { kind: CompositeKind::Group, on_error, stages: members }
}

/// A map whose members are bound to their items, failing fast.
pub fn map_of(members: Vec<Stage>) -> Step {
    Step { kind: CompositeKind::Map, on_error: OnError::FailFast, stages: members }
}

/// Settings of a task stage, which the builders above leave at their defaults.
pub trait StageExt {
    fn options_mut(&mut self) -> &mut TaskOptions;
}

impl StageExt for Stage {
    fn options_mut(&mut self) -> &mut TaskOptions {
        match self {
            Stage::Task(step) => &mut step.options,
            Stage::Composite(_) => panic!("a composite stage has no task options"),
        }
    }
}

/// Submits `step` for `worker()` as its client, and returns its id.
pub fn submitted(fixture: &mut Fixture, step: Step) -> CompositeId {
    let minted = fixture.scheduler.mint_composite(step, Some(worker()));
    fixture.scheduler.submit_composite(minted).unwrap()
}

/// Claims, starts and completes member `index` of `composite` as `worker()`,
/// with a result whose digest is that of `result`; returns the claim.
pub fn run_member(fixture: &mut Fixture, composite: &CompositeId, index: u64, result: &[u8]) -> Claim {
    let claim = start_member(fixture, composite, index);
    fixture
        .scheduler
        .complete(&worker(), &claim.task_run_id, Digest::blake3(result), Completion::Final)
        .unwrap();
    claim
}

/// Claims and starts member `index` of `composite`; returns the claim.
pub fn start_member(fixture: &mut Fixture, composite: &CompositeId, index: u64) -> Claim {
    let claim = fixture
        .scheduler
        .request_claim(&worker(), &composite.member_task(index))
        .unwrap();
    fixture.scheduler.report_started(&worker(), &claim.task_run_id).unwrap();
    claim
}

/// Fails member `index` for good (it has no retries).
pub fn fail_member(fixture: &mut Fixture, composite: &CompositeId, index: u64, kind: &str) {
    let claim = start_member(fixture, composite, index);
    fixture.scheduler.fail(&worker(), &claim.task_run_id, kind).unwrap();
}

/// The `CompositeFinished` events taken from the scheduler so far.
pub fn finished(fixture: &mut Fixture) -> Vec<CompositeFinished> {
    fixture
        .scheduler
        .take_events()
        .into_iter()
        .filter_map(|event| match event {
            Event::CompositeFinished(finished) => Some(finished),
            _ => None,
        })
        .collect()
}

/// Entry `index` of `composite`'s pages, as last published.
pub fn entry(fixture: &Fixture, composite: &CompositeId, index: u64) -> Option<CompositeEntry> {
    let page = fixture.spy.newest_page(composite, (index / PAGE_ENTRIES) as u32)?;
    page.entries.into_iter().find(|entry| entry.index == index)
}

/// What a new leader rebuilds from when the old one's last writes to the
/// records `lost` names did not reach their holders: of those, the newest
/// revision published before `mark`; of every other record, the newest.
pub fn rebuild_losing(old: &Fixture, mark: usize, lost: impl Fn(&RecordKey) -> bool) -> Rebuild {
    let mut newest: BTreeMap<RecordKey, ShardRecord> = BTreeMap::new();
    for (at, record) in old.spy.shard_revisions().into_iter().enumerate() {
        let key = record.identify().unwrap().0;
        if at >= mark && lost(&key) {
            continue;
        }
        newest.insert(key, record);
    }
    let mut rebuild = Rebuild::default();
    for record in newest.into_values() {
        match record {
            ShardRecord::Task(task) => rebuild.records.push(task),
            ShardRecord::Composite(composite) => rebuild.composites.push(composite),
            ShardRecord::Page(page) => rebuild.pages.push(page),
        }
    }
    rebuild
}

/// Whether `key` names a composite's record or one of its pages.
pub fn is_composite_or_page(key: &RecordKey) -> bool {
    matches!(key, RecordKey::Composite(_) | RecordKey::Page(..))
}

/// The next leader of what `old` led, rebuilt from `rebuild` and leading.
pub fn took_over(old: &Fixture, rebuild: Rebuild) -> Fixture {
    let mut new = reconciling_after(old);
    new.scheduler.reconcile(rebuild).unwrap();
    new.scheduler.set_leadership_grant(Some(grant_of(OFFICE)));
    new
}

/// Certifies `run`, a running run of `worker()`, as having returned a flow of
/// one stage; returns the composite that stage runs in.
pub fn continue_with_a_stage(fixture: &mut Fixture, run: &TaskRunId) -> CompositeId {
    let step = flow_of(vec![task_stage("stage", Some(b"s"))]);
    fixture
        .scheduler
        .complete(&worker(), run, step.digest(), Completion::Continues(step))
        .unwrap();
    CompositeId::continuation_of(run)
}

/// Runs the stage of a continuation made by [`continue_with_a_stage`] to its
/// end, which ends the continuation and so its task.
pub fn end_continuation(fixture: &mut Fixture, continuation: &CompositeId) {
    run_member(fixture, continuation, 0, b"staged");
}
