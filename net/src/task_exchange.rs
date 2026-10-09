//! The task exchange: how a worker asks its shard's leader to record a task,
//! and tells it what became of a run the worker claimed.
//!
//! [`codec`] frames the `/kabudachi/task/1` messages; `wire` maps them to and
//! from `core::scheduler`'s types. The asking side is [`Net::submit`],
//! [`Net::report_started`], [`Net::complete`], [`Net::fail`],
//! [`Net::report_lost`] and [`Net::cancel`], each sent to the leader the
//! caller names: the transport keeps no leader of its own. A call addressed
//! to this worker itself, while it leads, is answered by its own driver the
//! same way. The calls about a
//! run also keep this worker's
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
//! ledger for the next leader. Only the digest of a result travels, except
//! for a compaction run, whose result is the folded payload itself
//! ([`Net::complete_compaction`]): the leader's answer says whether it was
//! applied.

use std::str::FromStr;

use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::generated;
use kabudachi_core::protocol::ids::{TaskId, TaskRunId, WorkerId};
use kabudachi_core::protocol::messages::{
    CancelTask, CompactionApplied, PlaceRecords, PlacedKey, RecordPlacements, ReportCompacted, ReportCompleted,
    ReportFailed, ReportLost, ReportStarted, StartAccepted, SubmitAccepted, TaskReject, TaskRejectReason,
    TaskRequest, TaskResponse, task_request, task_response,
};
use kabudachi_core::protocol::ids::IdGenerator;
use kabudachi_core::scheduler::{Completion, Observer, Scheduler, Submitted};
use kabudachi_core::time::Clock;
use libp2p::PeerId;
use tokio::sync::oneshot;

use crate::claimed_runs::{ClaimedRuns, HeldRun};
use crate::exchange::Asked;
use crate::messenger::Net;
use crate::peers::worker_id_of;
use crate::task_exchange::codec::TaskCodec;
use crate::task_exchange::wire::Malformed;
use crate::task_store::placement::{Placement, ReplicationFactor, placement};

pub mod codec;
mod wire;

/// The most task ids one placement request carries (see
/// [`Net::place_records`]): a worker with more asks in pages, so that an
/// answer, which names every holder, stays far below a message's size limit.
pub(crate) const MAX_PLACE_IDS: usize = 256;

/// Why a task-exchange call got no answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskFailure {
    /// No answer came: the request failed outright (such as a leader that
    /// cannot be dialed), the leader disconnected before answering, or
    /// nothing could be sent (this `Net` has stopped, or the leader's id
    /// names no libp2p peer); or, for a request this worker made of itself,
    /// its driver stopped before answering.
    Unanswered,
}

/// A task-exchange request this worker made of itself while its node names
/// it leader: its own driver answers it, as it answers a peer's.
pub(crate) struct OwnTask {
    pub(crate) request: TaskRequest,
    pub(crate) reply: oneshot::Sender<TaskResponse>,
}

/// Who asked a request, and where its answer goes.
enum Asker {
    Peer(Asked<TaskCodec>),
    Own { from: WorkerId, task: OwnTask },
}

/// An unanswered `/kabudachi/task/1` request, returned by
/// [`Net::poll_task_requests`]: a peer's, or one this worker made of itself
/// while it leads. Answer it with [`Net::respond_task`]; dropping it
/// unanswered lets a peer's substream fail with `OutboundFailure` on their
/// side, and this worker's own ask end `Unanswered`.
pub struct TaskRequestHandle(Asker);

impl TaskRequestHandle {
    /// The `WorkerId` of whoever sent this request.
    pub fn from(&self) -> WorkerId {
        match &self.0 {
            Asker::Peer(asked) => worker_id_of(&asked.from),
            Asker::Own { from, .. } => from.clone(),
        }
    }

    /// What this request asks for.
    pub fn request(&self) -> &task_request::Request {
        let request = match &self.0 {
            Asker::Peer(asked) => &asked.request,
            Asker::Own { task, .. } => &task.request,
        };
        request
            .request
            .as_ref()
            .expect("every task request this worker reads asks for something")
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
        self.report(leader, task_request::Request::Started(ReportStarted {
            task_run_id: Some(run.into()),
        }))
        .await
    }

    /// Tells `leader` `run` succeeded with a result of digest
    /// `result_digest`. The result itself is not sent.
    pub async fn complete(
        &self,
        leader: WorkerId,
        run: TaskRunId,
        result_digest: Digest,
    ) -> Result<TaskResponse, TaskFailure> {
        self.report(leader, task_request::Request::Completed(ReportCompleted {
            task_run_id: Some(run.into()),
            result_digest: Some(result_digest.into()),
        }))
        .await
    }

    /// Tells `leader` `run` failed with an error of type `failure_kind`.
    pub async fn fail(
        &self,
        leader: WorkerId,
        run: TaskRunId,
        failure_kind: String,
    ) -> Result<TaskResponse, TaskFailure> {
        self.report(leader, task_request::Request::Failed(ReportFailed {
            task_run_id: Some(run.into()),
            failure_kind,
        }))
        .await
    }

    /// Tells `leader` the compaction run `run` folded the entries it was
    /// given into `folded`. The answer says whether the leader applied the
    /// fold (`compaction`), or the run was refused. The run stays in this
    /// worker's ledger until the leader has taken the fold, so that a leader
    /// that has changed still finds it held. A fold larger than a message
    /// cannot be sent: report the run failed instead.
    pub async fn complete_compaction(
        &self,
        leader: WorkerId,
        run: TaskRunId,
        folded: Vec<u8>,
    ) -> Result<TaskResponse, TaskFailure> {
        self.report(leader, task_request::Request::Compacted(ReportCompacted {
            task_run_id: Some(run.into()),
            folded_payload: folded,
        }))
        .await
    }

    /// Tells `leader` the run `run` ended with no outcome known: the process
    /// running its body died. The leader decides it as a run lost with its
    /// worker: lost and replayed, or orphaned. The run stays in this worker's
    /// ledger until a leader has taken the report.
    pub async fn report_lost(
        &self,
        leader: WorkerId,
        run: TaskRunId,
    ) -> Result<TaskResponse, TaskFailure> {
        self.report(leader, task_request::Request::Lost(ReportLost {
            task_run_id: Some(run.into()),
        }))
        .await
    }

    /// Sends `request`, a report on one of this worker's runs, to `leader`,
    /// and keeps the ledger by it: by what the report says before it is sent
    /// (see [`Self::note_sending`]), then by the answer (see
    /// [`Self::note_answer`]).
    pub(crate) async fn report(
        &self,
        leader: WorkerId,
        request: task_request::Request,
    ) -> Result<TaskResponse, TaskFailure> {
        self.note_sending(&request);
        let response = self.ask_task(leader, request.clone()).await?;
        self.note_answer(&request, &response);
        Ok(response)
    }

    /// Records in the ledger what a report says before any leader takes it:
    /// a run reported completed or failed stays so, whoever leads next.
    pub(crate) fn note_sending(&self, request: &task_request::Request) {
        let Some(run) = reported_run(request) else {
            return;
        };
        match request {
            task_request::Request::Completed(report) => {
                if let Ok(result_digest) = wire::digest(report.result_digest.as_ref()) {
                    self.claimed.set(&run, HeldRun::Completed { result_digest });
                }
            }
            task_request::Request::Failed(report) => self.claimed.set(
                &run,
                HeldRun::Failed {
                    failure_kind: report.failure_kind.clone(),
                },
            ),
            _ => {}
        }
    }

    /// Updates the ledger from `response`, a leader's answer to `request`,
    /// a report on one of this worker's runs (see [`Self::note_report`]).
    pub(crate) fn note_answer(&self, request: &task_request::Request, response: &TaskResponse) {
        use task_request::Request;
        use task_response::Result as Answer;
        let Some(run) = reported_run(request) else {
            return;
        };
        match request {
            Request::Started(_) => self.note_report(
                &run,
                response,
                |answer| matches!(answer, Answer::Started(_)),
                Some(HeldRun::Running),
            ),
            Request::Completed(_) => {
                self.note_report(&run, response, |answer| matches!(answer, Answer::Certified(_)), None)
            }
            Request::Failed(_) => {
                self.note_report(&run, response, |answer| matches!(answer, Answer::Failed(_)), None)
            }
            Request::Compacted(_) => {
                self.note_report(&run, response, |answer| matches!(answer, Answer::Compaction(_)), None)
            }
            Request::Lost(_) => {
                self.note_report(&run, response, |answer| matches!(answer, Answer::Lost(_)), None)
            }
            Request::Submit(_) | Request::Cancel(_) | Request::Place(_) => {}
        }
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

    /// Asks `leader` where it would place each of `tasks` now, this worker
    /// left out: a draining worker, which knows no voters, asks before it
    /// hands its copies over. No more than [`MAX_PLACE_IDS`] ids at once;
    /// the answer names the holders and the quorum of each task the leader
    /// could place.
    pub(crate) async fn place_records(
        &self,
        leader: WorkerId,
        tasks: Vec<TaskId>,
    ) -> Result<TaskResponse, TaskFailure> {
        let request = task_request::Request::Place(PlaceRecords {
            task_ids: tasks.into_iter().map(Into::into).collect(),
        });
        self.ask_task(leader, request).await
    }

    /// Drains every `/kabudachi/task/1` request not yet answered: peers', and
    /// this worker's own. Answer each with [`Self::respond_task`].
    pub fn poll_task_requests(&self) -> Vec<TaskRequestHandle> {
        let me = self.local_worker_id();
        let mut handles: Vec<TaskRequestHandle> = self
            .take_asked::<TaskCodec>()
            .into_iter()
            .map(|asked| TaskRequestHandle(Asker::Peer(asked)))
            .collect();
        handles.extend(self.inbound.take_own_tasks().into_iter().map(|task| {
            TaskRequestHandle(Asker::Own {
                from: me.clone(),
                task,
            })
        }));
        handles
    }

    /// Answers a request obtained from [`Self::poll_task_requests`].
    /// Fire-and-forget: an asker that stopped waiting, or a swarm task that
    /// stopped, drops the answer, and that is fine.
    pub fn respond_task(&self, handle: TaskRequestHandle, response: TaskResponse) {
        match handle.0 {
            Asker::Peer(asked) => self.answer::<TaskCodec>(asked.channel, response),
            Asker::Own { task, .. } => {
                let _ = task.reply.send(response);
            }
        }
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
            // This worker leads: its own driver decides, from its own
            // scheduler, and answers once the writes it made are stored,
            // as it answers a peer.
            let (reply, answer) = oneshot::channel();
            self.inbound.queue_own_task(OwnTask {
                request: TaskRequest {
                    request: Some(request),
                },
                reply,
            });
            return answer.await.map_err(|_| TaskFailure::Unanswered);
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
        Request::Compacted(report) => run_id(report.task_run_id.as_ref()).and_then(|run| {
            scheduler
                .complete_compaction(from, &run, report.folded_payload.clone())
                .map(|compacted| {
                    task_response::Result::Compaction(CompactionApplied {
                        applied: compacted.applied,
                    })
                })
                .map_err(wire::report_reject)
        }),
        Request::Failed(report) => run_id(report.task_run_id.as_ref()).and_then(|run| {
            scheduler
                .fail(from, &run, report.failure_kind.clone())
                .map(|failure| task_response::Result::Failed(wire::failed(failure)))
                .map_err(wire::report_reject)
        }),
        Request::Lost(report) => run_id(report.task_run_id.as_ref()).and_then(|run| {
            scheduler
                .report_lost(from, &run)
                .map(|lost| task_response::Result::Lost(wire::lost(lost)))
                .map_err(wire::report_reject)
        }),
        Request::Cancel(cancel) => match cancel.task_id.clone() {
            Some(task) => scheduler
                .cancel(&TaskId::from(task))
                .map(|cancellation| task_response::Result::Cancelled(wire::cancel_answer(cancellation)))
                .map_err(wire::cancel_reject),
            None => Err(TaskRejectReason::TaskRejectMalformed),
        },
        // Where records go is the driver's to say: it alone knows the voters.
        Request::Place(_) => Err(TaskRejectReason::TaskRejectMalformed),
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

/// The run a report names; `None` for a request that reports on no run, or a
/// report that names none.
pub(crate) fn reported_run(request: &task_request::Request) -> Option<TaskRunId> {
    use task_request::Request;
    let run = match request {
        Request::Started(report) => &report.task_run_id,
        Request::Completed(report) => &report.task_run_id,
        Request::Failed(report) => &report.task_run_id,
        Request::Compacted(report) => &report.task_run_id,
        Request::Lost(report) => &report.task_run_id,
        Request::Submit(_) | Request::Cancel(_) | Request::Place(_) => return None,
    };
    run.clone().map(TaskRunId::from)
}

/// The leader's answer to `request`: where each task it names would be placed
/// among `voters`, the voters it can place records on other than the asker,
/// `factor` of them for each. A task that could not be placed (no voter, or a
/// voter that is no peer) is left out. Not gated on the writes of any
/// decision: it decides nothing. A request that names too many tasks, or one
/// without its id, is malformed.
pub(crate) fn placements(
    request: &PlaceRecords,
    voters: &[WorkerId],
    factor: ReplicationFactor,
) -> TaskResponse {
    if request.task_ids.len() > MAX_PLACE_IDS {
        return wire::reject(TaskRejectReason::TaskRejectMalformed);
    }
    let placed = request
        .task_ids
        .iter()
        .filter_map(|task| {
            let task = TaskId::from(task.clone());
            let Placement { holders, quorum } = placement(&task, voters, factor)?;
            Some(PlacedKey {
                task_id: Some(task.into()),
                holders: holders.into_iter().map(Into::into).collect(),
                quorum: u32::try_from(quorum).unwrap_or(u32::MAX),
            })
        })
        .collect();
    TaskResponse {
        result: Some(task_response::Result::Placements(RecordPlacements {
            placements: placed,
        })),
    }
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
