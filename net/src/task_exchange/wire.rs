//! Mapping between the task exchange's wire messages and the scheduler's
//! types. A message that lacks something the scheduler needs maps to
//! [`Malformed`], which the leader answers as a refusal instead of panicking
//! on a peer's bad message.

use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::generated;
use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId};
use kabudachi_core::protocol::messages::{
    CancelAnswer, CancelOutcome, RunCertified, RunFailed, RunLost, SubmitTask, TaskReject, TaskRejectReason,
    TaskResponse, task_response,
};
use kabudachi_core::scheduler::{
    CancelRejection, Cancellation, Certification, Failure, LostRun, ReportRejection, Submission, Submitted,
    SubmitRejection,
};
use kabudachi_core::time::{Clock, Duration, WallTime};

/// Why a request could not be read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Malformed;

/// `submitted` as a request: everything its client fixed, so that asking
/// again names the same task.
pub(crate) fn submit_task(submitted: &Submitted) -> SubmitTask {
    let submission = &submitted.submission;
    SubmitTask {
        task_id: Some(submitted.task_id.clone().into()),
        submitted_at: Some(submitted.submitted_at.into()),
        task_definition_id: Some(submission.definition_id.clone().into()),
        source_version: submission.source_version,
        serialized_input: submission.serialized_input.clone(),
        queue: submission.queue.clone(),
        max_retries: submission.retries,
        delay_millis: submission.delay.map(|delay| delay.as_ticks()),
        expiry_millis: submission.expiry.map(|expiry| expiry.as_ticks()),
        coalescing_key: submission.coalescing_key.clone(),
        drop_oldest: submission.drop_oldest,
        ephemeral: submission.ephemeral,
        non_retriable: submission.non_retriable,
        reconnect_timeout_ms: submission.reconnect_timeout.map_or(0, |timeout| timeout.as_ticks()),
    }
}

/// The submission `task` asks for, as it arrives on this node: its delay and
/// expiry count from `clock`'s reading now.
pub(crate) fn submitted(task: &SubmitTask, clock: &impl Clock) -> Result<Submitted, Malformed> {
    let task_id = task.task_id.clone().ok_or(Malformed)?;
    let submitted_at = task.submitted_at.ok_or(Malformed)?;
    let definition_id = task.task_definition_id.clone().ok_or(Malformed)?;
    Ok(Submitted::received(
        TaskId::from(task_id),
        WallTime::from(submitted_at),
        Submission {
            definition_id: TaskDefinitionId::from(definition_id),
            source_version: task.source_version,
            serialized_input: task.serialized_input.clone(),
            queue: task.queue.clone(),
            retries: task.max_retries,
            delay: task.delay_millis.map(Duration::from_millis),
            expiry: task.expiry_millis.map(Duration::from_millis),
            coalescing_key: task.coalescing_key.clone(),
            drop_oldest: task.drop_oldest,
            ephemeral: task.ephemeral,
            non_retriable: task.non_retriable,
            reconnect_timeout: (task.reconnect_timeout_ms > 0)
                .then(|| Duration::from_millis(task.reconnect_timeout_ms)),
        },
        clock,
    ))
}

/// The digest a request carries, if it carries a usable one.
pub(crate) fn digest(digest: Option<&generated::Digest>) -> Result<Digest, Malformed> {
    digest
        .ok_or(Malformed)
        .and_then(|digest| Digest::try_from(digest).map_err(|_| Malformed))
}

pub(crate) fn submit_reject(rejection: SubmitRejection) -> TaskRejectReason {
    match rejection {
        SubmitRejection::TooLarge { .. } => TaskRejectReason::TaskRejectTooLarge,
        SubmitRejection::Backpressure { .. } => TaskRejectReason::TaskRejectBackpressure,
        SubmitRejection::NotLeader => TaskRejectReason::TaskRejectNotLeader,
        SubmitRejection::RecordTooLarge { .. } => TaskRejectReason::TaskRejectRecordTooLarge,
        SubmitRejection::KeyNotReady => TaskRejectReason::TaskRejectNotReady,
        SubmitRejection::KeyBackpressure { .. } => TaskRejectReason::TaskRejectBackpressure,
    }
}

pub(crate) fn report_reject(rejection: ReportRejection) -> TaskRejectReason {
    match rejection {
        ReportRejection::NotLeader => TaskRejectReason::TaskRejectNotLeader,
        ReportRejection::UnknownRun => TaskRejectReason::TaskRejectUnknownRun,
        ReportRejection::NotAuthoritative => TaskRejectReason::TaskRejectNotAuthoritative,
        ReportRejection::NotReady => TaskRejectReason::TaskRejectNotReady,
    }
}

pub(crate) fn cancel_reject(rejection: CancelRejection) -> TaskRejectReason {
    match rejection {
        CancelRejection::NotLeader => TaskRejectReason::TaskRejectNotLeader,
        CancelRejection::NotReady => TaskRejectReason::TaskRejectNotReady,
    }
}

pub(crate) fn certified(certification: Certification) -> RunCertified {
    RunCertified {
        task_id: Some(certification.task_id.into()),
        task_run_id: Some(certification.task_run_id.into()),
        result_digest: Some(certification.result_digest.into()),
    }
}

pub(crate) fn failed(failure: Failure) -> RunFailed {
    RunFailed {
        task_id: Some(failure.task_id.into()),
        task_run_id: Some(failure.task_run_id.into()),
        retry: failure.retry.map(Into::into),
    }
}

pub(crate) fn lost(lost: LostRun) -> RunLost {
    RunLost {
        task_id: Some(lost.task_id.into()),
        task_run_id: Some(lost.task_run_id.into()),
        state: generated::TaskRunState::from(lost.state) as i32,
        replay: lost.replayed.map(Into::into),
    }
}

pub(crate) fn cancel_answer(cancellation: Cancellation) -> CancelAnswer {
    let (outcome, was_running) = match cancellation {
        Cancellation::Cancelled { was_running } => (CancelOutcome::Cancelled, was_running),
        Cancellation::AlreadyFinished => (CancelOutcome::AlreadyFinished, false),
        Cancellation::UnknownTask => (CancelOutcome::UnknownTask, false),
    };
    CancelAnswer {
        outcome: outcome as i32,
        was_running,
    }
}

pub(crate) fn reject(reason: TaskRejectReason) -> TaskResponse {
    TaskResponse {
        result: Some(task_response::Result::Reject(TaskReject {
            reason: reason as i32,
        })),
    }
}
