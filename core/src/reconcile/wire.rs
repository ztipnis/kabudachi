//! The wire form of a worker's reconciliation report.

use crate::protocol::digest::Digest;
use crate::protocol::generated;
use crate::protocol::messages::prelude::*;
use crate::protocol::ids::{TaskDefinitionId, TaskId, TaskRunId, WorkerId};
use crate::reconcile::{CoalescingKey, HeldKey, ReportPage, ReportedRun, ReportedState};
use crate::scheduler::Claim;
use crate::task_record::{RecordVersion, identify};

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
        })
    }
}

impl From<&ReportedRun> for generated::ReportedRun {
    fn from(run: &ReportedRun) -> Self {
        let claim = generated::Claim {
            chain: Vec::new(),
            ..generated::Claim::from(run.claim.clone())
        };
        let (state, result_digest, failure_kind) = match &run.state {
            ReportedState::Claimed => (
                generated::ReportedRunState::ReportedRunClaimed,
                None,
                String::new(),
            ),
            ReportedState::Running => (
                generated::ReportedRunState::ReportedRunRunning,
                None,
                String::new(),
            ),
            ReportedState::Succeeded { result_digest } => (
                generated::ReportedRunState::ReportedRunSucceeded,
                Some(result_digest.clone().into()),
                String::new(),
            ),
            ReportedState::Failed { failure_kind } => (
                generated::ReportedRunState::ReportedRunFailed,
                None,
                failure_kind.clone(),
            ),
        };
        generated::ReportedRun {
            claim: Some(claim),
            state: state as i32,
            result_digest,
            failure_kind,
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
            },
            Ok(generated::ReportedRunState::ReportedRunFailed) => ReportedState::Failed {
                failure_kind: run.failure_kind.clone(),
            },
            Ok(generated::ReportedRunState::Unspecified) | Err(_) => return Err(MalformedReport),
        };
        Ok(ReportedRun { claim, state })
    }
}

impl From<&HeldKey> for generated::HeldKey {
    fn from(key: &HeldKey) -> Self {
        generated::HeldKey {
            task_id: Some(key.task_id.clone().into()),
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
        }
    }
}

impl TryFrom<&generated::HeldKey> for HeldKey {
    type Error = MalformedReport;

    fn try_from(key: &generated::HeldKey) -> Result<Self, MalformedReport> {
        Ok(HeldKey {
            task_id: key
                .task_id
                .clone()
                .map(TaskId::from)
                .ok_or(MalformedReport)?,
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
pub fn held_key(record: &generated::TaskRecord) -> Result<HeldKey, MalformedReport> {
    let (task_id, version) = identify(record).map_err(|_| MalformedReport)?;
    Ok(HeldKey {
        task_id,
        version,
        input_digest: record
            .input_digest
            .as_ref()
            .map(|digest| Digest::try_from(digest).map_err(|_| MalformedReport))
            .transpose()?,
        latest_run: record
            .runs
            .last()
            .and_then(|run| run.identity.as_ref())
            .and_then(|identity| identity.task_run_id.clone())
            .map(TaskRunId::from),
        placement: record
            .placement
            .iter()
            .cloned()
            .map(WorkerId::from)
            .collect(),
        finished: record.finished,
        coalescing: record.task.as_ref().and_then(|task| {
            Some(CoalescingKey {
                definition: task.task_definition_id(),
                key: task.coalescing_key.clone()?,
            })
        }),
    })
}
