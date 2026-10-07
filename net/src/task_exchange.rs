//! The task exchange: how a worker asks its shard's leader to record a task,
//! and tells it what became of a run the worker claimed.
//!
//! [`codec`] frames the `/kabudachi/task/1` messages; `wire` maps them to and
//! from `core::scheduler`'s types. The asking side is [`Net::submit`],
//! [`Net::report_started`], [`Net::complete`], [`Net::fail`] and
//! [`Net::cancel`], each sent to the leader the caller names: the transport
//! keeps no leader of its own. The calls about a run also keep this worker's
//! [`ClaimedRuns`](crate::claimed_runs::ClaimedRuns) ledger, from the answers
//! they get.
//!
//! The answering side is [`answer`], which the driver applies to each inbound
//! request from a voter or pending member of the leader's shard (as with
//! claims, any other worker is answered [`not_member`] without asking the
//! scheduler): whether to record a submission, a started, completed or failed
//! report, or a cancel is `core::scheduler::Scheduler`'s decision alone, and
//! a request that lacks an id or digest it needs is refused as malformed
//! without asking it. As with claims, the decision is made at once, but the
//! driver sends the answer only once the revisions it wrote are acknowledged
//! by a quorum of the task's placement and the leader still leads;
//! otherwise the asker is answered [`not_leader`] and keeps the run in its
//! ledger for the next leader. Only the digest of a result travels.

use std::str::FromStr;

use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::generated;
use kabudachi_core::protocol::ids::{TaskId, TaskRunId, WorkerId};
use kabudachi_core::protocol::messages::{
    CancelTask, ReportCompleted, ReportFailed, ReportStarted, StartAccepted, SubmitAccepted,
    TaskReject, TaskRejectReason, TaskRequest, TaskResponse, task_request, task_response,
};
use kabudachi_core::protocol::ids::IdGenerator;
use kabudachi_core::scheduler::{Completion, Observer, Scheduler, Submitted};
use kabudachi_core::time::Clock;
use libp2p::PeerId;

use crate::claimed_runs::{ClaimedRuns, HeldRun};
use crate::exchange::Asked;
use crate::messenger::Net;
use crate::peers::worker_id_of;
use crate::task_exchange::codec::TaskCodec;
use crate::task_exchange::wire::Malformed;

pub mod codec;
mod wire;

/// Why a task-exchange call got no answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskFailure {
    /// The leader named is this worker: its scheduler decides in process,
    /// and nothing was sent.
    ThisWorkerLeads,
    /// No answer came: the request failed outright (such as a leader that
    /// cannot be dialed), the leader disconnected before answering, or
    /// nothing could be sent (this `Net` has stopped, or the leader's id
    /// names no libp2p peer).
    Unanswered,
}

/// An unanswered inbound `/kabudachi/task/1` request, returned by
/// [`Net::poll_task_requests`]. Answer it with [`Net::respond_task`];
/// dropping it unanswered just lets the requester's substream eventually fail
/// with `OutboundFailure` on their side, the same contract as
/// `ClaimRequestHandle`.
pub struct TaskRequestHandle(Asked<TaskCodec>);

impl TaskRequestHandle {
    /// The `WorkerId` of whoever sent this request.
    pub fn from(&self) -> WorkerId {
        worker_id_of(&self.0.from)
    }

    /// What this request asks for.
    pub fn request(&self) -> &task_request::Request {
        self.0
            .request
            .request
            .as_ref()
            .expect("the task codec only accepts a request that asks for something")
    }
}

impl Net {
    /// Asks `leader` to record `submitted`. Answered `submitted` once the
    /// leader has stored the task's first record where it must be, or a
    /// reason: `TASK_REJECT_NOT_LEADER` means ask again (with the same
    /// `submitted`, so the task id holds).
    pub async fn submit(
        &self,
        leader: WorkerId,
        submitted: Submitted,
    ) -> Result<TaskResponse, TaskFailure> {
        let request = task_request::Request::Submit(wire::submit_task(&submitted));
        self.ask_task(leader, request).await
    }

    /// Tells `leader` this worker began running `run`.
    pub async fn report_started(
        &self,
        leader: WorkerId,
        run: TaskRunId,
    ) -> Result<TaskResponse, TaskFailure> {
        let request = task_request::Request::Started(ReportStarted {
            task_run_id: Some(run.clone().into()),
        });
        let response = self.ask_task(leader, request).await?;
        self.note_report(
            &run,
            &response,
            |result| matches!(result, task_response::Result::Started(_)),
            Some(HeldRun::Running),
        );
        Ok(response)
    }

    /// Tells `leader` `run` succeeded with a result of digest
    /// `result_digest`. The result itself is not sent.
    pub async fn complete(
        &self,
        leader: WorkerId,
        run: TaskRunId,
        result_digest: Digest,
    ) -> Result<TaskResponse, TaskFailure> {
        self.claimed.set(
            &run,
            HeldRun::Completed {
                result_digest: result_digest.clone(),
            },
        );
        let request = task_request::Request::Completed(ReportCompleted {
            task_run_id: Some(run.clone().into()),
            result_digest: Some(result_digest.into()),
        });
        let response = self.ask_task(leader, request).await?;
        self.note_report(
            &run,
            &response,
            |result| matches!(result, task_response::Result::Certified(_)),
            None,
        );
        Ok(response)
    }

    /// Tells `leader` `run` failed with an error of type `failure_kind`.
    pub async fn fail(
        &self,
        leader: WorkerId,
        run: TaskRunId,
        failure_kind: String,
    ) -> Result<TaskResponse, TaskFailure> {
        self.claimed.set(
            &run,
            HeldRun::Failed {
                failure_kind: failure_kind.clone(),
            },
        );
        let request = task_request::Request::Failed(ReportFailed {
            task_run_id: Some(run.clone().into()),
            failure_kind,
        });
        let response = self.ask_task(leader, request).await?;
        self.note_report(
            &run,
            &response,
            |result| matches!(result, task_response::Result::Failed(_)),
            None,
        );
        Ok(response)
    }

    /// Asks `leader` to cancel `task`.
    pub async fn cancel(
        &self,
        leader: WorkerId,
        task: TaskId,
    ) -> Result<TaskResponse, TaskFailure> {
        let request = task_request::Request::Cancel(CancelTask {
            task_id: Some(task.into()),
        });
        self.ask_task(leader, request).await
    }

    /// Drains every inbound `/kabudachi/task/1` request not yet answered.
    /// Answer each with [`Self::respond_task`].
    pub fn poll_task_requests(&self) -> Vec<TaskRequestHandle> {
        self.take_asked::<TaskCodec>()
            .into_iter()
            .map(TaskRequestHandle)
            .collect()
    }

    /// Answers a request obtained from [`Self::poll_task_requests`].
    /// Fire-and-forget like `respond_claim`: if the driver task has already
    /// stopped, there's nowhere for the answer to go, and that's fine to drop.
    pub fn respond_task(&self, handle: TaskRequestHandle, response: TaskResponse) {
        self.answer::<TaskCodec>(handle.0.channel, response);
    }

    /// The runs this worker claimed and has not heard the end of.
    pub fn claimed_runs(&self) -> ClaimedRuns {
        self.claimed.clone()
    }

    async fn ask_task(
        &self,
        leader: WorkerId,
        request: task_request::Request,
    ) -> Result<TaskResponse, TaskFailure> {
        if leader == self.local_worker_id() {
            return Err(TaskFailure::ThisWorkerLeads);
        }
        let to = PeerId::from_str(leader.as_str()).map_err(|_| TaskFailure::Unanswered)?;
        self.ask::<TaskCodec>(
            to,
            TaskRequest {
                request: Some(request),
            },
        )
        .await
        .ok_or(TaskFailure::Unanswered)
    }

    /// Updates the ledger for `run` from `response` to a report about it.
    /// An answer for which `took` holds means the leader took the report:
    /// the run then moves to `then`, or is forgotten if that is `None`. A
    /// refusal that says the run is not this worker's forgets it; any other
    /// answer, a retryable one included, changes nothing.
    fn note_report(
        &self,
        run: &TaskRunId,
        response: &TaskResponse,
        took: impl Fn(&task_response::Result) -> bool,
        then: Option<HeldRun>,
    ) {
        match response.result.as_ref() {
            Some(task_response::Result::Reject(TaskReject { reason })) => {
                if matches!(
                    TaskRejectReason::try_from(*reason),
                    Ok(TaskRejectReason::TaskRejectUnknownRun
                        | TaskRejectReason::TaskRejectNotAuthoritative)
                ) {
                    self.claimed.forget(run);
                }
            }
            Some(result) if took(result) => match then {
                Some(state) => self.claimed.set(run, state),
                None => self.claimed.forget(run),
            },
            _ => {}
        }
    }
}

/// The leader's answer to `request` from `from`, as `scheduler` decides it.
/// The caller holds it until the writes the decision made are stored, and
/// answers [`not_leader`] instead if they are not, or the lease ends first.
/// A scheduler holding no live grant refuses with `TASK_REJECT_NOT_LEADER`.
/// A submission's delay and expiry count from `clock`'s reading now.
pub(crate) fn answer<C: Clock, I: IdGenerator, O: Observer>(
    scheduler: &mut Scheduler<C, I, O>,
    from: &WorkerId,
    request: &task_request::Request,
    clock: &impl Clock,
) -> TaskResponse {
    use task_request::Request;
    let result = match request {
        Request::Submit(task) => match wire::submitted(task, clock) {
            Ok(submitted) => scheduler
                .submit_minted(submitted)
                .map(|task_id| {
                    task_response::Result::Submitted(SubmitAccepted {
                        task_id: Some(task_id.into()),
                    })
                })
                .map_err(wire::submit_reject),
            Err(Malformed) => Err(TaskRejectReason::TaskRejectMalformed),
        },
        Request::Started(report) => run_id(report.task_run_id.as_ref()).and_then(|run| {
            scheduler
                .report_started(from, &run)
                .map(|()| task_response::Result::Started(StartAccepted {}))
                .map_err(wire::report_reject)
        }),
        Request::Completed(report) => run_id(report.task_run_id.as_ref()).and_then(|run| {
            let digest = wire::digest(report.result_digest.as_ref())
                .map_err(|Malformed| TaskRejectReason::TaskRejectMalformed)?;
            scheduler
                .complete(from, &run, digest, Completion::Final)
                .map(|certification| task_response::Result::Certified(wire::certified(certification)))
                .map_err(wire::report_reject)
        }),
        Request::Failed(report) => run_id(report.task_run_id.as_ref()).and_then(|run| {
            scheduler
                .fail(from, &run, report.failure_kind.clone())
                .map(|failure| task_response::Result::Failed(wire::failed(failure)))
                .map_err(wire::report_reject)
        }),
        Request::Cancel(cancel) => match cancel.task_id.clone() {
            Some(task) => scheduler
                .cancel(&TaskId::from(task))
                .map(|cancellation| task_response::Result::Cancelled(wire::cancel_answer(cancellation)))
                .map_err(wire::cancel_reject),
            None => Err(TaskRejectReason::TaskRejectMalformed),
        },
    };
    match result {
        Ok(result) => TaskResponse {
            result: Some(result),
        },
        Err(reason) => wire::reject(reason),
    }
}

/// The run a report names.
fn run_id(run: Option<&generated::TaskRunId>) -> Result<TaskRunId, TaskRejectReason> {
    run.cloned()
        .map(TaskRunId::from)
        .ok_or(TaskRejectReason::TaskRejectMalformed)
}

/// The answer to a request from a worker the leader's roster holds neither as
/// a voter nor as a pending member: it must join the shard first, and nothing
/// of the request took effect.
pub(crate) fn not_member() -> TaskResponse {
    wire::reject(TaskRejectReason::TaskRejectNotMember)
}

/// The answer to a request the leader could not decide, or whose writes
/// were not acknowledged while it still led: a retryable `NotLeader`.
pub(crate) fn not_leader() -> TaskResponse {
    wire::reject(TaskRejectReason::TaskRejectNotLeader)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use kabudachi_core::coordination_authority::RecoveryEpoch;
    use kabudachi_core::protocol::ids::{TaskDefinitionId, Uuid7Ids};
    use kabudachi_core::protocol::messages::{CancelOutcome, CancelTask, ReportCompleted, SubmitTask};
    use kabudachi_core::reconcile::{Rebuild, ReconcileTerm};
    use kabudachi_core::scheduler::{LeadershipGrant, LeaseEnd, Scheduler, Submission};
    use kabudachi_core::time::RealClock;

    use super::*;

    fn leading() -> Scheduler<RealClock, Uuid7Ids> {
        let mut scheduler = Scheduler::new(RealClock::new(), Uuid7Ids);
        scheduler.set_leadership_grant(Some(LeadershipGrant {
            term: 1,
            recovery_epoch: RecoveryEpoch::new(0, 0),
            valid_until: LeaseEnd::Unbounded,
        }));
        scheduler
    }

    fn reason(response: &TaskResponse) -> Option<TaskRejectReason> {
        match &response.result {
            Some(task_response::Result::Reject(reject)) => {
                TaskRejectReason::try_from(reject.reason).ok()
            }
            _ => None,
        }
    }

    /// A peer's request that lacks something the leader needs is refused as
    /// unreadable, and records nothing: the codec lets a request with a
    /// missing id through.
    #[test]
    fn a_request_missing_its_id_or_digest_is_refused_as_malformed() {
        let mut scheduler = leading();
        let someone = WorkerId::new("w1");
        let run = Some(TaskRunId::new("run-1").into());
        let requests = [
            task_request::Request::Submit(SubmitTask::default()),
            task_request::Request::Cancel(CancelTask { task_id: None }),
            task_request::Request::Started(ReportStarted { task_run_id: None }),
            task_request::Request::Completed(ReportCompleted {
                task_run_id: run,
                result_digest: None,
            }),
        ];

        for request in &requests {
            let response = answer(&mut scheduler, &someone, request, &RealClock::new());
            assert_eq!(
                reason(&response),
                Some(TaskRejectReason::TaskRejectMalformed),
                "{request:?}"
            );
        }
    }

    fn held_by(net: &Net, run: &TaskRunId) {
        net.claimed.claimed(kabudachi_core::protocol::messages::Claim {
            task_run_id: Some(run.clone().into()),
            ..Default::default()
        });
    }

    /// The leader's refusal that says the run is not this worker's makes the
    /// worker forget the run; the leader's "ask again" does not, or a report
    /// no leader certified would be lost before the next leader hears it.
    #[tokio::test]
    async fn a_refusal_that_the_run_is_not_ours_forgets_it_and_a_retryable_one_keeps_it() {
        let net = Net::new();
        let run = TaskRunId::new("run-1");
        let ended = HeldRun::Failed { failure_kind: "ValueError".into() };
        let took = |_: &task_response::Result| false;

        held_by(&net, &run);
        net.claimed.set(&run, ended.clone());
        net.note_report(&run, &not_leader(), took, None);
        assert_eq!(net.claimed_runs().get(&run).map(|held| held.state), Some(ended));

        net.note_report(&run, &wire::reject(TaskRejectReason::TaskRejectNotAuthoritative), took, None);
        assert_eq!(net.claimed_runs().get(&run), None);

        held_by(&net, &run);
        net.note_report(&run, &wire::reject(TaskRejectReason::TaskRejectUnknownRun), took, None);
        assert_eq!(net.claimed_runs().get(&run), None);
    }

    /// What the scheduler decides about a failure and a cancel reaches the
    /// worker: a failed run names the queued run that replaces it, and a
    /// cancel says whether the task was in a worker's hands.
    #[test]
    fn a_failure_answer_names_its_retry_and_a_cancel_answer_its_outcome() {
        let mut scheduler = leading();
        let clock = RealClock::new();
        let worker = WorkerId::new("w1");
        let submission =
            Submission::new(TaskDefinitionId::new("billing.charge"), 0, b"in".to_vec(), "default")
                .with_retries(1);
        let task = scheduler
            .submit_minted(scheduler.mint(submission))
            .expect("the leader records the submission");
        let run = scheduler
            .request_claim(&worker, &task)
            .expect("the task is queued")
            .task_run_id;
        scheduler.report_started(&worker, &run).expect("the claimant starts it");

        let failed = answer(
            &mut scheduler,
            &worker,
            &task_request::Request::Failed(ReportFailed {
                task_run_id: Some(run.clone().into()),
                failure_kind: "ValueError".into(),
            }),
            &clock,
        );
        let Some(task_response::Result::Failed(failed)) = failed.result else {
            panic!("expected the failure accepted, got {failed:?}");
        };
        let retry = failed.retry.map(TaskRunId::from).expect("a retry is queued");
        assert_ne!(retry, run);
        assert!(scheduler.runs_of(&task).contains(&retry));

        scheduler.request_claim(&worker, &task).expect("the retry is queued");
        let cancelled = answer(
            &mut scheduler,
            &worker,
            &task_request::Request::Cancel(CancelTask { task_id: Some(task.into()) }),
            &clock,
        );
        let Some(task_response::Result::Cancelled(cancelled)) = cancelled.result else {
            panic!("expected the cancel answered, got {cancelled:?}");
        };
        assert_eq!(cancelled.outcome, CancelOutcome::Cancelled as i32);
        assert!(cancelled.was_running, "the retry was claimed");
    }

    /// A report or cancel about a task the new leader has not been able to
    /// pin down is answered retryably, so its claimant keeps the run in its
    /// ledger.
    #[test]
    fn a_task_the_leader_cannot_know_yet_is_answered_not_ready() {
        let mut scheduler = Scheduler::new(RealClock::new(), Uuid7Ids);
        let office = ReconcileTerm {
            recovery_epoch: RecoveryEpoch::new(0, 0),
            term: 2,
        };
        let task = TaskId::new("task-1");
        let run = TaskRunId::new("run-1");
        scheduler.begin_reconcile(office);
        scheduler
            .reconcile(Rebuild {
                uncertain: BTreeMap::from([(task.clone(), BTreeSet::from([run.clone()]))]),
                ..Rebuild::default()
            })
            .unwrap();
        scheduler.set_leadership_grant(Some(LeadershipGrant {
            term: office.term,
            recovery_epoch: office.recovery_epoch,
            valid_until: LeaseEnd::Unbounded,
        }));
        let someone = WorkerId::new("w1");

        let started = task_request::Request::Started(ReportStarted {
            task_run_id: Some(run.into()),
        });
        let cancel = task_request::Request::Cancel(CancelTask {
            task_id: Some(task.into()),
        });

        for request in [started, cancel] {
            let response = answer(&mut scheduler, &someone, &request, &RealClock::new());
            assert_eq!(
                reason(&response),
                Some(TaskRejectReason::TaskRejectNotReady),
                "{request:?}"
            );
        }
    }
}
