//! Mapping between the task exchange's wire messages and the scheduler's
//! types. A message that lacks something the scheduler needs maps to
//! [`Malformed`], which the leader answers as a refusal instead of panicking
//! on a peer's bad message.

use kabudachi_core::manifest::{InvalidManifest, Manifest};
use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::generated;
use kabudachi_core::protocol::ids::{CompositeId, TaskDefinitionId, TaskId, WorkerId};
use kabudachi_core::protocol::messages::{
    CancelAnswer, CancelOutcome, CompositeAccepted, OutcomeAnswer, RunCertified, RunFailed, RunLost, SubmitComposite,
    SubmitTask, TaskReject, TaskRejectReason, TaskResponse, task_response,
};
use kabudachi_core::scheduler::composite::{Step, SubmittedComposite};
use kabudachi_core::scheduler::{
    CancelRejection, Cancellation, Certification, Failure, LostRun, Outcome, OutcomeRejection, ReportRejection,
    Submission, Submitted, SubmitRejection, TraceContext,
};
use kabudachi_core::task_record::RecordKey;
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
        client: submission.client.clone().map(Into::into),
        input_manifest: submission.input_manifest.clone().map(Into::into),
        trace_parent: submission.trace.trace_parent.clone(),
        trace_state: submission.trace.trace_state.clone(),
        baggage: submission.trace.baggage.clone(),
    }
}

/// The submission `task` asks for, as it arrives on this node: its delay and
/// expiry count from `clock`'s reading now.
pub(crate) fn submitted(task: &SubmitTask, clock: &impl Clock) -> Result<Submitted, Malformed> {
    let task_id = TaskId::from(task.task_id.clone().ok_or(Malformed)?);
    if !RecordKey::may_name_a_task(&task_id) {
        return Err(Malformed);
    }
    let submitted_at = task.submitted_at.ok_or(Malformed)?;
    let definition_id = task.task_definition_id.clone().ok_or(Malformed)?;
    let input_manifest = task
        .input_manifest
        .as_ref()
        .map(|wire| {
            let manifest = Manifest::try_from(wire)?;
            manifest.check_cut_payload()?;
            Ok::<_, InvalidManifest>(manifest)
        })
        .transpose()
        .map_err(|_| Malformed)?;
    if input_manifest.is_some() && !task.serialized_input.is_empty() {
        return Err(Malformed);
    }
    Ok(Submitted::received(
        task_id,
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
            client: task.client.clone().map(WorkerId::from),
            input_manifest,
            trace: TraceContext {
                trace_parent: task.trace_parent.clone(),
                trace_state: task.trace_state.clone(),
                baggage: task.baggage.clone(),
            },
            member_of: None,
        },
        clock,
    ))
}

/// `submitted` as a request: everything its client fixed, so that asking
/// again names the same composite.
pub(crate) fn submit_composite(submitted: &SubmittedComposite) -> SubmitComposite {
    SubmitComposite {
        composite_id: Some(submitted.composite_id.clone().into()),
        submitted_at: Some(submitted.submitted_at.into()),
        description: Some(generated::StepDescription::from(&submitted.step)),
        client: submitted.client.clone().map(Into::into),
    }
}

/// The composite `submit` asks for.
pub(crate) fn submitted_composite(submit: &SubmitComposite) -> Result<SubmittedComposite, Malformed> {
    let step = submit.description.as_ref().ok_or(Malformed)?;
    let composite_id = CompositeId::from(submit.composite_id.clone().ok_or(Malformed)?);
    if !RecordKey::may_name_a_composite(&composite_id) {
        return Err(Malformed);
    }
    Ok(SubmittedComposite {
        composite_id,
        submitted_at: WallTime::from(submit.submitted_at.ok_or(Malformed)?),
        step: Step::try_from(step).map_err(|_| Malformed)?,
        client: submit.client.clone().map(WorkerId::from),
    })
}

/// The answer that a composite is recorded.
pub(crate) fn composite_accepted(id: CompositeId) -> CompositeAccepted {
    CompositeAccepted {
        composite_id: Some(id.into()),
        slow_down: false,
    }
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
        SubmitRejection::PayloadTooLarge { .. } => TaskRejectReason::TaskRejectPayloadTooLarge,
        SubmitRejection::StepTakesInput => TaskRejectReason::TaskRejectMalformed,
    }
}

pub(crate) fn report_reject(rejection: ReportRejection) -> TaskRejectReason {
    match rejection {
        ReportRejection::NotLeader => TaskRejectReason::TaskRejectNotLeader,
        ReportRejection::UnknownRun => TaskRejectReason::TaskRejectUnknownRun,
        ReportRejection::NotAuthoritative => TaskRejectReason::TaskRejectNotAuthoritative,
        ReportRejection::NotReady => TaskRejectReason::TaskRejectNotReady,
        ReportRejection::ContinuationTooLarge => TaskRejectReason::TaskRejectRecordTooLarge,
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

/// How `task` stands, as a client is told: `known` false and nothing else
/// set when the leader does not hold it.
pub(crate) fn outcome_answer(task: &TaskId, outcome: Option<Outcome>) -> OutcomeAnswer {
    let Some(outcome) = outcome else {
        return OutcomeAnswer {
            task_id: Some(task.clone().into()),
            ..OutcomeAnswer::default()
        };
    };
    OutcomeAnswer {
        task_id: Some(outcome.task_id.into()),
        known: true,
        task_run_id: Some(outcome.task_run_id.into()),
        state: generated::TaskRunState::from(outcome.state) as i32,
        finished: outcome.finished,
        result_digest: outcome.result_digest.map(Into::into),
        failure_kind: outcome.failure_kind,
        superseded_by: outcome.superseded_by.map(Into::into),
        continued: outcome.continued,
    }
}

pub(crate) fn outcome_reject(rejection: OutcomeRejection) -> TaskRejectReason {
    match rejection {
        OutcomeRejection::NotLeader => TaskRejectReason::TaskRejectNotLeader,
        OutcomeRejection::NotReady => TaskRejectReason::TaskRejectNotReady,
    }
}

pub(crate) fn reject(reason: TaskRejectReason) -> TaskResponse {
    TaskResponse {
        result: Some(task_response::Result::Reject(TaskReject {
            reason: reason as i32,
        })),
    }
}
