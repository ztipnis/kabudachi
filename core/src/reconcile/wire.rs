//! The wire form of a worker's reconciliation report.

use crate::protocol::digest::Digest;
use crate::protocol::generated;
use crate::protocol::messages::prelude::*;
use crate::protocol::ids::{CompositeId, TaskDefinitionId, TaskId, TaskRunId, WorkerId};
use crate::reconcile::{CoalescingKey, HeldKey, ReportPage, ReportedRun, ReportedState};
use crate::scheduler::Claim;
use crate::scheduler::composite::Step;
use crate::task_record::{RecordKey, RecordVersion, ShardRecord};
use crate::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("a reconciliation report this worker cannot read")]
pub struct MalformedReport;

impl From<Claim> for generated::Claim {
    fn from(claim: Claim) -> Self {
        generated::Claim {
            task: Some(claim.task),
            task_run_id: Some(claim.task_run_id.into()),
            attempt_number: claim.attempt_number,
            chain: claim.chain,
            reconnect_timeout_ms: claim.reconnect_timeout.as_ticks(),
        }
    }
}

impl TryFrom<&generated::Claim> for Claim {
    type Error = MalformedReport;

    fn try_from(claim: &generated::Claim) -> Result<Self, MalformedReport> {
        Ok(Claim {
            task: claim.task.clone().ok_or(MalformedReport)?,
            task_run_id: claim
                .task_run_id
                .clone()
                .map(TaskRunId::from)
                .ok_or(MalformedReport)?,
            attempt_number: claim.attempt_number,
            chain: claim.chain.clone(),
            reconnect_timeout: Duration::from_millis(claim.reconnect_timeout_ms),
        })
    }
}

impl From<&ReportedRun> for generated::ReportedRun {
    fn from(run: &ReportedRun) -> Self {
        let claim = generated::Claim {
            chain: Vec::new(),
            ..generated::Claim::from(run.claim.clone())
        };
        let (state, result_digest, failure_kind, failure_message) = match &run.state {
            ReportedState::Claimed => (
                generated::ReportedRunState::ReportedRunClaimed,
                None,
                String::new(),
                String::new(),
            ),
            ReportedState::Running => (
                generated::ReportedRunState::ReportedRunRunning,
                None,
                String::new(),
                String::new(),
            ),
            ReportedState::Succeeded { result_digest, .. } => (
                generated::ReportedRunState::ReportedRunSucceeded,
                Some(result_digest.clone().into()),
                String::new(),
                String::new(),
            ),
            ReportedState::Failed {
                failure_kind,
                failure_message,
            } => (
                generated::ReportedRunState::ReportedRunFailed,
                None,
                failure_kind.clone(),
                failure_message.clone(),
            ),
        };
        generated::ReportedRun {
            claim: Some(claim),
            state: state as i32,
            result_digest,
            failure_kind,
            delivered: matches!(
                &run.state,
                ReportedState::Succeeded {
                    delivered: true,
                    ..
                }
            ),
            failure_message,
            continuation: run.continuation.as_ref().map(generated::StepDescription::from),
        }
    }
}

impl TryFrom<&generated::ReportedRun> for ReportedRun {
    type Error = MalformedReport;

    fn try_from(run: &generated::ReportedRun) -> Result<Self, MalformedReport> {
        let claim = Claim::try_from(run.claim.as_ref().ok_or(MalformedReport)?)?;
        let state = match generated::ReportedRunState::try_from(run.state) {
            Ok(generated::ReportedRunState::ReportedRunClaimed) => ReportedState::Claimed,
            Ok(generated::ReportedRunState::ReportedRunRunning) => ReportedState::Running,
            Ok(generated::ReportedRunState::ReportedRunSucceeded) => ReportedState::Succeeded {
                result_digest: Digest::try_from(run.result_digest.as_ref().ok_or(MalformedReport)?)
                    .map_err(|_| MalformedReport)?,
                delivered: run.delivered,
            },
            Ok(generated::ReportedRunState::ReportedRunFailed) => ReportedState::Failed {
                failure_kind: run.failure_kind.clone(),
                failure_message: run.failure_message.clone(),
            },
            Ok(generated::ReportedRunState::Unspecified) | Err(_) => return Err(MalformedReport),
        };
        let continuation = run
            .continuation
            .as_ref()
            .map(|description| Step::try_from(description).map_err(|_| MalformedReport))
            .transpose()?;
        Ok(ReportedRun {
            claim,
            state,
            continuation,
        })
    }
}

impl From<&HeldKey> for generated::HeldKey {
    fn from(key: &HeldKey) -> Self {
        let (task_id, composite_id, page) = match &key.key {
            RecordKey::Task(task) => (Some(task.clone().into()), None, None),
            RecordKey::Composite(id) => (None, Some(id.clone().into()), None),
            RecordKey::Page(id, page) => (None, Some(id.clone().into()), Some(*page)),
        };
        generated::HeldKey {
            task_id,
            version: Some(key.version.into()),
            input_digest: key.input_digest.clone().map(Into::into),
            latest_run: key.latest_run.clone().map(Into::into),
            placement: key.placement.iter().cloned().map(Into::into).collect(),
            finished: key.finished,
            task_definition_id: key
                .coalescing
                .as_ref()
                .map(|coalescing| coalescing.definition.clone().into()),
            coalescing_key: key.coalescing.as_ref().map(|coalescing| coalescing.key.clone()),
            composite_id,
            page,
        }
    }
}

impl TryFrom<&generated::HeldKey> for HeldKey {
    type Error = MalformedReport;

    fn try_from(key: &generated::HeldKey) -> Result<Self, MalformedReport> {
        let record = match (key.task_id.clone(), key.composite_id.clone(), key.page) {
            (Some(task), None, None) => RecordKey::Task(TaskId::from(task)),
            (None, Some(id), None) => RecordKey::Composite(CompositeId::from(id)),
            (None, Some(id), Some(page)) => RecordKey::Page(CompositeId::from(id), page),
            _ => return Err(MalformedReport),
        };
        Ok(HeldKey {
            key: record,
            version: key
                .version
                .as_ref()
                .map(RecordVersion::from)
                .ok_or(MalformedReport)?,
            input_digest: key
                .input_digest
                .as_ref()
                .map(|digest| Digest::try_from(digest).map_err(|_| MalformedReport))
                .transpose()?,
            latest_run: key.latest_run.clone().map(TaskRunId::from),
            placement: key.placement.iter().cloned().map(WorkerId::from).collect(),
            finished: key.finished,
            coalescing: key
                .coalescing_key
                .clone()
                .map(|flat| {
                    let definition = key
                        .task_definition_id
                        .clone()
                        .map(TaskDefinitionId::from)
                        .ok_or(MalformedReport)?;
                    Ok(CoalescingKey {
                        definition,
                        key: flat,
                    })
                })
                .transpose()?,
        })
    }
}

/// Reads a page a worker sent.
pub fn page(report: &generated::ReconcileReport) -> Result<ReportPage, MalformedReport> {
    Ok(ReportPage {
        runs: report
            .runs
            .iter()
            .map(ReportedRun::try_from)
            .collect::<Result<_, _>>()?,
        keys: report
            .keys
            .iter()
            .map(HeldKey::try_from)
            .collect::<Result<_, _>>()?,
        last: report.last,
    })
}

/// The message for a page a worker sends.
pub fn report(page: &ReportPage) -> generated::ReconcileReport {
    generated::ReconcileReport {
        runs: page.runs.iter().map(generated::ReportedRun::from).collect(),
        keys: page.keys.iter().map(generated::HeldKey::from).collect(),
        last: page.last,
    }
}

/// The summary of `record` a worker reports for it.
pub fn held_key(record: &ShardRecord) -> Result<HeldKey, MalformedReport> {
    let (key, version) = record.identify().map_err(|_| MalformedReport)?;
    let placement = record.placement().iter().cloned().map(WorkerId::from).collect();
    let ShardRecord::Task(task_record) = record else {
        return Ok(HeldKey {
            key,
            version,
            input_digest: None,
            latest_run: None,
            placement,
            finished: record.is_finished(),
            coalescing: None,
        });
    };
    Ok(HeldKey {
        key,
        version,
        input_digest: task_record
            .input_digest
            .as_ref()
            .map(|digest| Digest::try_from(digest).map_err(|_| MalformedReport))
            .transpose()?,
        latest_run: task_record
            .runs
            .last()
            .and_then(|run| run.identity.as_ref())
            .and_then(|identity| identity.task_run_id.clone())
            .map(TaskRunId::from),
        placement,
        finished: task_record.finished,
        coalescing: task_record.task.as_ref().and_then(|task| {
            Some(CoalescingKey {
                definition: task.task_definition_id(),
                key: task.coalescing_key.clone()?,
            })
        }),
    })
}
