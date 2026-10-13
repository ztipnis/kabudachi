//! The task exchange: how a worker asks its shard's leader to record a task,
//! and tells it what became of a run the worker claimed.
//!
//! [`codec`] frames the `/kabudachi/task/1` messages; `wire` maps them to and
//! from `core::scheduler`'s types. The asking side is [`Net::submit`],
//! [`Net::report_started`], [`Net::complete`], [`Net::fail`],
//! [`Net::report_lost`], [`Net::cancel`] and [`Net::query_outcome`], each
//! sent to the leader the caller names. [`Net::submit_to_leader`],
//! [`Net::cancel_at_leader`] and [`Net::query_outcome_at_leader`] instead
//! find the leader themselves, the one the worker's node names, and
//! ask again until a leader decides, as the executor's reports are asked
//! again (see `crate::executor`). A call addressed to this worker itself,
//! while it leads, is answered by its own driver the same way. The calls
//! about a run also keep this worker's
//! [`ClaimedRuns`](crate::claimed_runs::ClaimedRuns) ledger, from the answers
//! they get.
//!
//! The answering side is [`answer`], which the driver applies to each inbound
//! request from a voter or pending member of the leader's shard (as with
//! claims, any other worker is answered [`not_member`] without asking the
//! scheduler): whether to record a submission, a started, completed or failed
//! report, or a cancel is `core::scheduler::Scheduler`'s decision alone, as is
//! how a task stands, which decides nothing and is answered for any asker, and
//! a request that lacks an id or digest it needs is refused as malformed
//! without asking it. As with claims, the decision is made at once, but the
//! driver sends the answer only once the revisions it wrote are acknowledged
//! by a quorum of the task's placement and the leader still leads;
//! otherwise the asker is answered [`not_leader`] and keeps the run in its
//! ledger for the next leader. Only the digest of a result travels, except
//! for a compaction run, whose result is the folded payload itself
//! ([`Net::complete_compaction`]): the leader's answer says whether it was
//! applied.
//!
//! [`Net::submit_members`] sends a streamed composite's members to the leader
//! in batches, each taken whole or refused whole, and [`Net::query_slow_down`]
//! asks whether the leader still asks submitters to wait; every accepted
//! submission's answer carries the same flag.
//!
//! [`Net::store_payload_at_leader`] stores a payload too large for one
//! message where the leader would place its owner's record (a task's or a
//! composite's), asking again as `submit_to_leader` does.

use std::future::Future;
use std::str::FromStr;
use std::time::Duration;

use kabudachi_core::blob::BlobOwner;
use kabudachi_core::manifest::Manifest;
use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::generated::{self, StepDescription};
use kabudachi_core::protocol::ids::{CompositeId, TaskId, TaskRunId, WorkerId};
use kabudachi_core::protocol::messages::{
    BlockNoted, CancelTask, CompactionApplied, PlaceRecords, PlacedKey, PlacedWithAsker, QueryOutcome, QuerySlowDown,
    RecordPlacements, ReportBlocked, ReportCompacted, ReportCompleted, ReportFailed, ReportLost, ReportStarted,
    SlowDownState, StartAccepted, SubmitAccepted, SubmitComposite, SubmitMembers, SubmitTask, TaskReject,
    TaskRejectReason, TaskRequest, TaskResponse, task_request, task_response,
};
use kabudachi_core::protocol::ids::IdGenerator;
use kabudachi_core::scheduler::composite::{MemberBatch, Step, SubmittedComposite};
use kabudachi_core::scheduler::{BlockRejection, Completion, Observer, Scheduler, Submitted};
use kabudachi_core::task_record::RecordKey;
use kabudachi_core::time::Clock;
use libp2p::PeerId;
use tokio::sync::oneshot;

use crate::blobs::BlobRole;
use crate::claimed_runs::{ClaimedRuns, HeldRun};
use crate::exchange::Asked;
use crate::messenger::{Net, PutBlobError};
use crate::peers::worker_id_of;
use crate::task_exchange::codec::TaskCodec;
use crate::task_exchange::wire::Malformed;
use crate::task_store::placement::{Placement, ReplicationFactor, placement};

pub mod codec;
mod wire;

pub(crate) use wire::outcome_answer;

/// The most record keys (task ids and composite record keys together) one
/// placement request carries (see [`Net::place_records`]): a worker with more
/// asks in pages, so that an answer, which names every holder, stays far below
/// a message's size limit. An answer names a record's holders twice when the
/// asker is among the nearest voters (the plain placement and the real one),
/// so the page is half what one list of holders would allow.
pub(crate) const MAX_PLACE_IDS: usize = 128;

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

    /// Asks `leader` to run `submitted`. Answered `composite` once the leader
    /// has stored the composite's first record where it must be, or a reason:
    /// `TASK_REJECT_NOT_LEADER` means ask again (with the same `submitted`,
    /// so the composite id holds).
    pub async fn submit_composite(
        &self,
        leader: WorkerId,
        submitted: &SubmittedComposite,
    ) -> Result<TaskResponse, TaskFailure> {
        let request = task_request::Request::SubmitComposite(wire::submit_composite(submitted));
        self.ask_task(leader, request).await
    }

    /// Asks `leader` to cancel the composite `id` and everything it runs.
    pub async fn cancel_composite(&self, leader: WorkerId, id: CompositeId) -> Result<TaskResponse, TaskFailure> {
        let request = task_request::Request::Cancel(CancelTask {
            task_id: None,
            composite_id: Some(id.into()),
        });
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
            execution_version: 0,
        }))
        .await
    }

    /// Tells `leader` `run` succeeded with a result of digest
    /// `result_digest`, for a run whose result went to no client. The
    /// result itself is not sent.
    pub async fn complete(
        &self,
        leader: WorkerId,
        run: TaskRunId,
        result_digest: Digest,
    ) -> Result<TaskResponse, TaskFailure> {
        self.report(leader, task_request::Request::Completed(ReportCompleted {
            task_run_id: Some(run.into()),
            result_digest: Some(result_digest.into()),
            delivered: false,
            continuation: None,
        }))
        .await
    }

    /// Tells `leader` `run` succeeded by returning `step`, which runs in its
    /// place. The result is the step's digest.
    pub async fn complete_continuing(
        &self,
        leader: WorkerId,
        run: TaskRunId,
        step: &Step,
    ) -> Result<TaskResponse, TaskFailure> {
        self.report(leader, task_request::Request::Completed(ReportCompleted {
            task_run_id: Some(run.into()),
            result_digest: Some(step.digest().into()),
            delivered: false,
            continuation: Some(StepDescription::from(step)),
        }))
        .await
    }

    /// Tells `leader` `run` failed with an error of type `failure_kind` and
    /// message `failure_message`.
    pub async fn fail(
        &self,
        leader: WorkerId,
        run: TaskRunId,
        failure_kind: String,
        failure_message: String,
    ) -> Result<TaskResponse, TaskFailure> {
        self.report(leader, task_request::Request::Failed(ReportFailed {
            task_run_id: Some(run.into()),
            failure_kind,
            failure_message,
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
                let continuation = report.continuation.as_ref().map(Step::try_from).transpose();
                if let (Ok(result_digest), Ok(continuation)) =
                    (wire::digest(report.result_digest.as_ref()), continuation)
                {
                    self.claimed.set(
                        &run,
                        HeldRun::Completed {
                            result_digest,
                            delivered: report.delivered,
                            continuation,
                        },
                    );
                }
            }
            task_request::Request::Failed(report) => self.claimed.set(
                &run,
                HeldRun::Failed {
                    failure_kind: report.failure_kind.clone(),
                    failure_message: report.failure_message.clone(),
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
            Request::Submit(_)
            | Request::Cancel(_)
            | Request::Place(_)
            | Request::QueryOutcome(_)
            | Request::SubmitComposite(_)
            | Request::SubmitMembers(_)
            | Request::QuerySlowDown(_)
            | Request::Blocked(_) => {}
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
            composite_id: None,
        });
        self.ask_task(leader, request).await
    }

    /// Asks `leader` how `task` stands. Decides nothing, so any worker may ask.
    pub async fn query_outcome(
        &self,
        leader: WorkerId,
        task: TaskId,
    ) -> Result<TaskResponse, TaskFailure> {
        let request = task_request::Request::QueryOutcome(QueryOutcome {
            task_id: Some(task.into()),
        });
        self.ask_task(leader, request).await
    }

    /// Asks `leader` where it would place each of `keys` now, this worker
    /// left out: a draining worker, which knows no voters, asks before it
    /// hands its copies over. No more than [`MAX_PLACE_IDS`] keys at once;
    /// the answer names the holders and the quorum of each record the leader
    /// could place.
    pub async fn place_records(
        &self,
        leader: WorkerId,
        keys: Vec<RecordKey>,
    ) -> Result<TaskResponse, TaskFailure> {
        let mut task_ids = Vec::new();
        let mut record_keys = Vec::new();
        for key in keys {
            match key {
                RecordKey::Task(task) => task_ids.push(task.into()),
                other => record_keys.push(other.kad_bytes()),
            }
        }
        let request = task_request::Request::Place(PlaceRecords { task_ids, record_keys });
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

/// How [`Net::submit_to_leader`], [`Net::cancel_at_leader`] and
/// [`Net::query_outcome_at_leader`] keep asking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaderRetry {
    /// How long a leader may take to answer before it is asked again.
    pub answer_within: Duration,
    /// How long to wait before asking again, after an answer that came too
    /// late, no answer, or a refusal that says to ask again.
    pub ask_again_after: Duration,
}

/// The placements [`Net::store_payload_at_leader`] writes to before it gives
/// up, each asked of the leader afresh.
pub const STORE_ATTEMPTS: u32 = 3;

/// Why [`Net::store_payload_at_leader`] gave up.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum NotStored {
    /// This worker left its shard: no leader will answer.
    #[error("this worker left its shard")]
    LeftShard,
    /// The shard did not store it in [`STORE_ATTEMPTS`] placements in a row
    /// (holders' stores full or unreachable): refused for now, as
    /// backpressure.
    #[error("the shard could not store the payload now")]
    Refused,
}

/// The leader this worker's node last named, and whether the worker has left
/// its shard, for the requests [`Net`] routes to its leader.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct NamedLeader {
    leader: Option<WorkerId>,
    left: bool,
}

impl NamedLeader {
    /// The leader named, unless the worker has left its shard.
    pub(crate) fn leader(&self) -> Option<WorkerId> {
        self.leader.clone().filter(|_| !self.left)
    }
}

impl Net {
    /// Asks the shard's leader to record `submitted` until a leader decides
    /// it, and returns that decision: stored, or refused for good. The
    /// leader asked is the one this worker's node names; while it names none
    /// the ask waits for one. An ask that is not answered within
    /// `retry.answer_within`, fails, or is refused for now
    /// (`TASK_REJECT_NOT_LEADER` or `TASK_REJECT_NOT_READY`, which the
    /// executor's reports are asked again on too, or
    /// `TASK_REJECT_NOT_MEMBER`: the leader is not yet sure of this worker)
    /// is asked again, of whichever leader the
    /// node names then, `retry.ask_again_after` later, with the same
    /// `submitted`, so the task id holds. `None` once the worker has left
    /// its shard (see [`Self::has_left_shard`]): no leader will answer.
    ///
    /// Any driver (`run_driver` included) names a leader here, but only
    /// [`crate::worker::Worker::run`] marks the worker out of its shard when
    /// it ends, however it does. A `Net` driven by `run_driver` alone waits
    /// for ever in this call and `cancel_at_leader` once its driver stops.
    pub async fn submit_to_leader(&self, submitted: Submitted, retry: LeaderRetry) -> Option<TaskResponse> {
        self.ask_until_decided(task_request::Request::Submit(wire::submit_task(&submitted)), retry)
            .await
    }

    /// Asks the shard's leader to cancel `task` until a leader decides it,
    /// and returns that decision (cancelled, already finished, unknown, or
    /// refused for good), asking again as [`Self::submit_to_leader`] does;
    /// `None` once the worker has left its shard.
    pub async fn cancel_at_leader(&self, task: TaskId, retry: LeaderRetry) -> Option<TaskResponse> {
        let request = task_request::Request::Cancel(CancelTask {
            task_id: Some(task.into()),
            composite_id: None,
        });
        self.ask_until_decided(request, retry).await
    }

    /// Asks the shard's leader how `task` stands until a leader answers,
    /// asking again as [`Self::submit_to_leader`] does; `None` once the
    /// worker has left its shard.
    pub async fn query_outcome_at_leader(&self, task: TaskId, retry: LeaderRetry) -> Option<TaskResponse> {
        let request = task_request::Request::QueryOutcome(QueryOutcome {
            task_id: Some(task.into()),
        });
        self.ask_until_decided(request, retry).await
    }

    /// Stores `payload` as blobs in `role`, owned by `owner`, where the shard's
    /// leader would place the owner's record (a task's or a composite's), and
    /// returns their manifest. The leader is asked as [`Self::submit_to_leader`] asks it.
    /// The placement is the record's real one (see
    /// [`placement_with_asker_of`]), so this worker keeps a copy of the blobs
    /// only if it is one of the holders, where its own copy counts towards
    /// their quorum and its record arrives to keep them. A write that
    /// fails is made again, to a placement asked afresh, after
    /// `retry.ask_again_after`. `LeftShard` once the worker has left its
    /// shard; `Refused` once [`STORE_ATTEMPTS`] placements in a row answered
    /// but the write failed (a store full, or holders unreachable), which
    /// its caller treats as backpressure.
    pub async fn store_payload_at_leader(
        &self,
        role: BlobRole,
        owner: &BlobOwner,
        payload: &[u8],
        retry: LeaderRetry,
    ) -> Result<Manifest, NotStored> {
        self.stored_where_placed(owner, retry, |placement| self.store_payload(role, owner, payload, placement))
            .await
    }

    /// Stores each of `blobs` in `role`, owned by `owner`, as
    /// [`Self::store_payload_at_leader`] stores a payload's: for blobs a
    /// caller has cut itself, such as a result's and the manifest naming it.
    pub async fn store_blobs_at_leader(
        &self,
        role: BlobRole,
        owner: &BlobOwner,
        blobs: Vec<Vec<u8>>,
        retry: LeaderRetry,
    ) -> Result<(), NotStored> {
        self.stored_where_placed(owner, retry, |placement| {
            self.store_blobs(role, owner, blobs.clone(), placement)
        })
        .await
    }

    /// Runs `store` against the placement the leader gives `owner`'s record,
    /// asking for a fresh one after a failed write, up to [`STORE_ATTEMPTS`]
    /// writes.
    async fn stored_where_placed<T, Fut>(
        &self,
        owner: &BlobOwner,
        retry: LeaderRetry,
        store: impl Fn(Placement) -> Fut,
    ) -> Result<T, NotStored>
    where
        Fut: Future<Output = Result<T, PutBlobError>>,
    {
        let key = record_key_of(owner);
        let mut failed = 0;
        loop {
            if let Some(placement) = self.placement_from_leader(&key, retry).await? {
                if let Ok(stored) = store(placement).await {
                    return Ok(stored);
                }
                failed += 1;
                if failed >= STORE_ATTEMPTS {
                    return Err(NotStored::Refused);
                }
            }
            tokio::time::sleep(retry.ask_again_after).await;
        }
    }

    /// Asks the shard's leader to run `submitted` until a leader decides it:
    /// recorded (`composite`), or refused for good. Asks again as
    /// [`Self::submit_to_leader`] does, with the same `submitted`, so the
    /// composite id holds; `None` once the worker has left its shard.
    pub async fn submit_composite_to_leader(
        &self,
        submitted: &SubmittedComposite,
        retry: LeaderRetry,
    ) -> Option<TaskResponse> {
        self.ask_until_decided(
            task_request::Request::SubmitComposite(wire::submit_composite(submitted)),
            retry,
        )
        .await
    }

    /// Asks the shard's leader to take `batch`, the next members of a
    /// streamed composite, until a leader decides it: accepted whole, or
    /// refused for good. Asks again as [`Self::submit_to_leader`] does, with
    /// the same `batch`, so a batch whose answer was lost creates nothing
    /// twice; `None` once the worker has left its shard.
    pub async fn submit_members(&self, batch: &MemberBatch, retry: LeaderRetry) -> Option<TaskResponse> {
        self.ask_until_decided(task_request::Request::SubmitMembers(SubmitMembers::from(batch)), retry)
            .await
    }

    /// Asks the shard's leader whether it still asks submitters to wait,
    /// asking again as [`Self::submit_to_leader`] does; `None` once the
    /// worker has left its shard. An answer that is not the leader's flag
    /// (a refusal for good) counts as no wait.
    pub async fn query_slow_down(&self, retry: LeaderRetry) -> Option<bool> {
        let response = self
            .ask_until_decided(task_request::Request::QuerySlowDown(QuerySlowDown {}), retry)
            .await?;
        Some(matches!(
            response.result,
            Some(task_response::Result::SlowDown(SlowDownState { slow_down: true }))
        ))
    }

    /// Asks the shard's leader to cancel the composite `id` until a leader
    /// decides it, as [`Self::cancel_at_leader`] does for a task.
    pub async fn cancel_composite_at_leader(&self, id: CompositeId, retry: LeaderRetry) -> Option<TaskResponse> {
        let request = task_request::Request::Cancel(CancelTask {
            task_id: None,
            composite_id: Some(id.into()),
        });
        self.ask_until_decided(request, retry).await
    }

    /// Where the leader the node names would place the record `key` names, if
    /// it answered in time.
    async fn placement_from_leader(
        &self,
        key: &RecordKey,
        retry: LeaderRetry,
    ) -> Result<Option<Placement>, NotStored> {
        let leader = self.routed_leader().await.ok_or(NotStored::LeftShard)?;
        let asked = tokio::time::timeout(
            retry.answer_within,
            self.place_records(leader, vec![key.clone()]),
        )
        .await;
        Ok(match asked {
            Ok(Ok(response)) => placement_with_asker_of(&response, key),
            _ => None,
        })
    }

    /// Whether this worker has left its shard: [`crate::worker::Worker::run`]
    /// ended, however it did. No request routed to the leader is answered
    /// from then on.
    pub fn has_left_shard(&self) -> bool {
        self.named_leader.borrow().left
    }

    /// Records the leader the node driving this `Net` names now, if any.
    pub(crate) fn name_leader(&self, leader: Option<WorkerId>) {
        self.named_leader.send_if_modified(|named| {
            let changed = named.leader != leader;
            if changed {
                named.leader = leader;
            }
            changed
        });
    }

    /// Records that this worker has left its shard, for good.
    pub(crate) fn leave_shard(&self) {
        self.named_leader.send_modify(|named| named.left = true);
    }

    /// Sends `request` to the leader the node names until one decides it
    /// (see [`Self::submit_to_leader`]); `None` once the worker has left
    /// its shard.
    async fn ask_until_decided(&self, request: task_request::Request, retry: LeaderRetry) -> Option<TaskResponse> {
        loop {
            let leader = self.routed_leader().await?;
            let asked = self.ask_task(leader, request.clone());
            if let Ok(Ok(response)) = tokio::time::timeout(retry.answer_within, asked).await {
                let ask_again = rejection(&response)
                    .is_some_and(|reason| refused_for_now(reason) || reason == TaskRejectReason::TaskRejectNotMember);
                if !ask_again {
                    return Some(response);
                }
            }
            tokio::time::sleep(retry.ask_again_after).await;
        }
    }

    /// The leader the node names, once it names one; `None` once the worker
    /// has left its shard.
    async fn routed_leader(&self) -> Option<WorkerId> {
        let mut named = self.named_leader.subscribe();
        let named = named
            .wait_for(|named| named.left || named.leader.is_some())
            .await
            .ok()?;
        if named.left {
            return None;
        }
        named.leader.clone()
    }
}

/// The record key whose records a blob of `owner` is kept with.
pub(crate) fn record_key_of(owner: &BlobOwner) -> RecordKey {
    match owner {
        BlobOwner::Task(task) => RecordKey::Task(task.clone()),
        BlobOwner::Composite(id) => RecordKey::Composite(id.clone()),
    }
}

/// The record `placed`, an entry of a leader's answer to `PlaceRecords`,
/// places; `None` for an entry that names none.
pub(crate) fn placed_record(placed: &PlacedKey) -> Option<RecordKey> {
    match &placed.task_id {
        Some(task) => Some(RecordKey::Task(task.clone().into())),
        None => RecordKey::from_kad_bytes(&placed.record_key).filter(|_| !placed.record_key.is_empty()),
    }
}

/// The answer `response`, a leader's answer to `PlaceRecords`, gives for `key`.
fn placed_key<'a>(response: &'a TaskResponse, key: &RecordKey) -> Option<&'a PlacedKey> {
    let Some(task_response::Result::Placements(answer)) = &response.result else {
        return None;
    };
    answer.placements.iter().find(|placed| placed_record(placed).as_ref() == Some(key))
}

/// Where `response`, a leader's answer to `PlaceRecords`, places the record
/// `key` names, the asker left out.
pub fn placement_of(response: &TaskResponse, key: &RecordKey) -> Option<Placement> {
    placed_key(response, key).map(|placed| Placement {
        holders: placed.holders.iter().cloned().map(WorkerId::from).collect(),
        quorum: usize::try_from(placed.quorum).unwrap_or(usize::MAX),
    })
}

/// Where `response` places the record `key` names, for a writer of its blobs: the real
/// placement, which names the asker itself when it is among the nearest
/// voters, and otherwise is [`placement_of`]. The asker then stores its own
/// copy, as one of the holders, and no other writer keeps one.
pub fn placement_with_asker_of(response: &TaskResponse, key: &RecordKey) -> Option<Placement> {
    let placed = placed_key(response, key)?;
    Some(match &placed.with_asker {
        Some(real) => Placement {
            holders: real.holders.iter().cloned().map(WorkerId::from).collect(),
            quorum: usize::try_from(real.quorum).unwrap_or(usize::MAX),
        },
        None => placement_of(response, key)?,
    })
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
        Request::Submit(task) => submit(scheduler, task, clock),
        Request::Started(report) => started(scheduler, from, report),
        Request::Completed(report) => completed(scheduler, from, report),
        Request::Compacted(report) => compacted(scheduler, from, report),
        Request::Failed(report) => failed(scheduler, from, report),
        Request::Lost(report) => lost(scheduler, from, report),
        Request::Blocked(report) => blocked(scheduler, from, report),
        Request::Cancel(request) => cancel(scheduler, request),
        // Where records go is the driver's to say: it alone knows the voters.
        Request::Place(_) => Err(TaskRejectReason::TaskRejectMalformed),
        Request::SubmitComposite(submit) => submit_composite(scheduler, submit),
        Request::QueryOutcome(query) => query_outcome(scheduler, query),
        Request::SubmitMembers(batch) => submit_members(scheduler, batch),
        // Whether the leader asks submitters to wait is the driver's to say, ungated.
        Request::QuerySlowDown(_) => Err(TaskRejectReason::TaskRejectMalformed),
    };
    match result {
        Ok(result) => TaskResponse {
            result: Some(result),
        },
        Err(reason) => wire::reject(reason),
    }
}

/// What the leader decided of one request: the answer, or why it refuses.
type Answered = Result<task_response::Result, TaskRejectReason>;

fn submit<C: Clock, I: IdGenerator, O: Observer>(
    scheduler: &mut Scheduler<C, I, O>,
    task: &SubmitTask,
    clock: &impl Clock,
) -> Answered {
    let Ok(submitted) = wire::submitted(task, clock) else {
        return Err(TaskRejectReason::TaskRejectMalformed);
    };
    let outcome = scheduler.submit_minted(submitted);
    let slow_down = scheduler.slow_down();
    outcome
        .map(|task_id| {
            task_response::Result::Submitted(SubmitAccepted {
                task_id: Some(task_id.into()),
                slow_down,
            })
        })
        .map_err(wire::submit_reject)
}

fn submit_composite<C: Clock, I: IdGenerator, O: Observer>(
    scheduler: &mut Scheduler<C, I, O>,
    submit: &SubmitComposite,
) -> Answered {
    let Ok(submitted) = wire::submitted_composite(submit) else {
        return Err(TaskRejectReason::TaskRejectMalformed);
    };
    let outcome = scheduler.submit_composite(submitted);
    let slow_down = scheduler.slow_down();
    outcome
        .map(|id| task_response::Result::Composite(wire::composite_accepted(id, slow_down)))
        .map_err(wire::submit_reject)
}

fn submit_members<C: Clock, I: IdGenerator, O: Observer>(
    scheduler: &mut Scheduler<C, I, O>,
    submit: &SubmitMembers,
) -> Answered {
    let Ok(batch) = MemberBatch::try_from(submit) else {
        return Err(TaskRejectReason::TaskRejectMalformed);
    };
    let composite = batch.composite_id.clone();
    scheduler
        .submit_members(batch)
        .map(|accepted| task_response::Result::Members(wire::members_accepted(composite, accepted)))
        .map_err(wire::submit_reject)
}

fn started<C: Clock, I: IdGenerator, O: Observer>(
    scheduler: &mut Scheduler<C, I, O>,
    from: &WorkerId,
    report: &ReportStarted,
) -> Answered {
    let run = run_id(report.task_run_id.as_ref())?;
    let execution_version = (report.execution_version != 0).then_some(report.execution_version);
    scheduler
        .report_started(from, &run, execution_version)
        .map(|()| task_response::Result::Started(StartAccepted {}))
        .map_err(wire::report_reject)
}

fn blocked<C: Clock, I: IdGenerator, O: Observer>(
    scheduler: &mut Scheduler<C, I, O>,
    from: &WorkerId,
    report: &ReportBlocked,
) -> Answered {
    let task = report
        .task_id
        .clone()
        .map(TaskId::from)
        .ok_or(TaskRejectReason::TaskRejectMalformed)?;
    scheduler
        .report_blocked(from, &task, report.reason.clone(), report.incompatible)
        .map(|_| task_response::Result::Blocked(BlockNoted {}))
        .map_err(|rejection| match rejection {
            BlockRejection::NotLeader => TaskRejectReason::TaskRejectNotLeader,
            BlockRejection::UnknownTask => TaskRejectReason::TaskRejectUnknownRun,
            BlockRejection::NotReady => TaskRejectReason::TaskRejectNotReady,
        })
}

fn completed<C: Clock, I: IdGenerator, O: Observer>(
    scheduler: &mut Scheduler<C, I, O>,
    from: &WorkerId,
    report: &ReportCompleted,
) -> Answered {
    let run = run_id(report.task_run_id.as_ref())?;
    let digest = wire::digest(report.result_digest.as_ref())
        .map_err(|Malformed| TaskRejectReason::TaskRejectMalformed)?;
    let completion = match &report.continuation {
        None => Completion::Final,
        // A step the run returns is not given an input beyond the run's own.
        Some(description) => match Step::try_from(description) {
            Ok(step) if !step.takes_input() => Completion::Continues(step),
            _ => return Err(TaskRejectReason::TaskRejectMalformed),
        },
    };
    scheduler
        .complete_delivered(from, &run, digest, completion, report.delivered)
        .map(|certification| task_response::Result::Certified(wire::certified(certification)))
        .map_err(wire::report_reject)
}

fn compacted<C: Clock, I: IdGenerator, O: Observer>(
    scheduler: &mut Scheduler<C, I, O>,
    from: &WorkerId,
    report: &ReportCompacted,
) -> Answered {
    let run = run_id(report.task_run_id.as_ref())?;
    scheduler
        .complete_compaction(from, &run, report.folded_payload.clone())
        .map(|compacted| {
            task_response::Result::Compaction(CompactionApplied {
                applied: compacted.applied,
            })
        })
        .map_err(wire::report_reject)
}

fn failed<C: Clock, I: IdGenerator, O: Observer>(
    scheduler: &mut Scheduler<C, I, O>,
    from: &WorkerId,
    report: &ReportFailed,
) -> Answered {
    let run = run_id(report.task_run_id.as_ref())?;
    scheduler
        .fail_with_message(
            from,
            &run,
            report.failure_kind.clone(),
            report.failure_message.clone(),
        )
        .map(|failure| task_response::Result::Failed(wire::failed(failure)))
        .map_err(wire::report_reject)
}

fn lost<C: Clock, I: IdGenerator, O: Observer>(
    scheduler: &mut Scheduler<C, I, O>,
    from: &WorkerId,
    report: &ReportLost,
) -> Answered {
    let run = run_id(report.task_run_id.as_ref())?;
    scheduler
        .report_lost(from, &run)
        .map(|lost| task_response::Result::Lost(wire::lost(lost)))
        .map_err(wire::report_reject)
}

fn cancel<C: Clock, I: IdGenerator, O: Observer>(
    scheduler: &mut Scheduler<C, I, O>,
    cancel: &CancelTask,
) -> Answered {
    let cancelled = match (cancel.task_id.clone(), cancel.composite_id.clone()) {
        (Some(task), None) => scheduler.cancel(&TaskId::from(task)),
        (None, Some(composite)) => scheduler.cancel_composite(&CompositeId::from(composite)),
        _ => return Err(TaskRejectReason::TaskRejectMalformed),
    };
    cancelled
        .map(|cancellation| task_response::Result::Cancelled(wire::cancel_answer(cancellation)))
        .map_err(wire::cancel_reject)
}

fn query_outcome<C: Clock, I: IdGenerator, O: Observer>(
    scheduler: &mut Scheduler<C, I, O>,
    query: &QueryOutcome,
) -> Answered {
    match query.task_id.clone().map(TaskId::from) {
        Some(task) => scheduler
            .outcome(&task)
            .map(|outcome| task_response::Result::Outcome(wire::outcome_answer(&task, outcome)))
            .map_err(wire::outcome_reject),
        None => Err(TaskRejectReason::TaskRejectMalformed),
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
        Request::Submit(_)
        | Request::Cancel(_)
        | Request::Place(_)
        | Request::QueryOutcome(_)
        | Request::SubmitComposite(_)
        | Request::SubmitMembers(_)
        | Request::QuerySlowDown(_)
        | Request::Blocked(_) => {
            return None;
        }
    };
    run.clone().map(TaskRunId::from)
}

/// The leader's answer to `request` from `asker`: where each record it names
/// (by task id, or by the key of a composite record or page) would be placed
/// among the voters it can place records on other than the asker, `factor` of
/// them for each. `voters` are all the voters it can place
/// records on, the asker among them if it is one; when the asker would be
/// among the nearest to a task, the answer also names the placement with it in
/// (see [`placement_with_asker_of`]); when the asker is the only voter, that
/// is the whole answer and the plain placement is empty. A record that could
/// not be placed (no voter, or a voter that is no peer) is left out. Not gated
/// on the writes of any decision: it decides nothing. A request that names too
/// many keys, ids and record keys together, is malformed.
pub(crate) fn placements(
    request: &PlaceRecords,
    asker: &WorkerId,
    voters: &[WorkerId],
    factor: ReplicationFactor,
) -> TaskResponse {
    if request.task_ids.len() + request.record_keys.len() > MAX_PLACE_IDS {
        return wire::reject(TaskRejectReason::TaskRejectMalformed);
    }
    let others: Vec<WorkerId> = voters.iter().filter(|voter| *voter != asker).cloned().collect();
    let tasks = request.task_ids.iter().map(|task| RecordKey::Task(task.clone().into()));
    let composites = request.record_keys.iter().filter_map(|bytes| RecordKey::from_kad_bytes(bytes));
    let placed = tasks
        .chain(composites)
        .filter_map(|key| {
            let with_asker = (others.len() < voters.len())
                .then(|| placement(&key, voters, factor))
                .flatten()
                .filter(|real| real.holders.contains(asker))
                .map(|real| PlacedWithAsker {
                    holders: real.holders.into_iter().map(Into::into).collect(),
                    quorum: u32::try_from(real.quorum).unwrap_or(u32::MAX),
                });
            // With no voter but the asker the plain placement is empty, and
            // the real one, the asker alone, is the answer.
            let Placement { holders, quorum } = match placement(&key, &others, factor) {
                Some(plain) => plain,
                None if with_asker.is_some() => Placement { holders: Vec::new(), quorum: 0 },
                None => return None,
            };
            let (task_id, record_key) = match key {
                RecordKey::Task(task) => (Some(task.into()), Vec::new()),
                other => (None, other.kad_bytes()),
            };
            Some(PlacedKey {
                task_id,
                holders: holders.into_iter().map(Into::into).collect(),
                quorum: u32::try_from(quorum).unwrap_or(u32::MAX),
                with_asker,
                record_key,
            })
        })
        .collect();
    TaskResponse {
        result: Some(task_response::Result::Placements(RecordPlacements {
            placements: placed,
        })),
    }
}

/// The answer to a SlowDown query: whether the leader asks submitters to
/// wait. Not gated on any write: it decides nothing.
pub(crate) fn slow_down_state(slow_down: bool) -> TaskResponse {
    TaskResponse {
        result: Some(task_response::Result::SlowDown(SlowDownState { slow_down })),
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

/// The reason `response` refuses its request, if it does and the reason can
/// be read.
pub(crate) fn rejection(response: &TaskResponse) -> Option<TaskRejectReason> {
    match &response.result {
        Some(task_response::Result::Reject(reject)) => TaskRejectReason::try_from(reject.reason).ok(),
        _ => None,
    }
}

/// Whether `reason` refuses a request only for now: the worker asked does
/// not lead, or leads but cannot decide yet. The same request may be taken
/// when asked again, of whichever leader the asker's node names then.
pub(crate) fn refused_for_now(reason: TaskRejectReason) -> bool {
    matches!(
        reason,
        TaskRejectReason::TaskRejectNotLeader | TaskRejectReason::TaskRejectNotReady
    )
}
