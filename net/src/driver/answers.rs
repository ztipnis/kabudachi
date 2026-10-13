use kabudachi_core::election::WorkerNode;
use kabudachi_core::protocol::generated;
use kabudachi_core::protocol::ids::{CompositeId, IdGenerator, TaskId, TaskRunId, WorkerId};
use kabudachi_core::protocol::messages::{
    ClaimResponse, PlaceRecords, ResultCertified, TaskResponse, claim_request, task_request,
};
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::queues::QueuePolicy;
use kabudachi_core::scheduler::{Conclusion, Scheduler};
use kabudachi_core::task_record::{LeaderRecords, RecordKey, RecordOutbox, Settled, Write};
use kabudachi_core::time::{Clock, WallTime};

use super::stepper::NetRecords;
use crate::claim::{self, ClaimRequestHandle};
use crate::executor::OwnAnswer;
use crate::join::pointer_for;
use crate::messenger::Net;
use crate::reconcile::report::page_of;
use crate::steal::candidates_for_steal;
use crate::task_exchange::{self, TaskRequestHandle};
use crate::task_store::placement::ReplicationFactor;

/// Answers every inbound `/kabudachi/join/1` request queued on `net` with a
/// pointer to the shard's leader (see `crate::join::pointer_for`).
pub(super) async fn respond_to_join_requests<C>(node: &WorkerNode<C>, net: &Net)
where
    C: Clock,
{
    let pending = net.poll_join_requests();
    // A node back in `Bootstrapping` answers no one, as the bootstrap
    // cascade does not (see `crate::bootstrap`): "no leader known" would
    // tell another bootstrapper a shard exists. Dropping the requests
    // closes their streams.
    if pending.is_empty() || node.state() == WorkerState::Bootstrapping {
        return;
    }

    let response = pointer_for(node, net).await;
    for handle in pending {
        net.respond_join(handle, response.clone());
    }
}

/// An answer the leader has decided and holds until the writes its decision
/// made settle.
pub(super) enum HeldAnswer {
    Claim {
        handle: ClaimRequestHandle,
        response: ClaimResponse,
    },
    Task {
        handle: TaskRequestHandle,
        response: TaskResponse,
    },
    /// A claim or report of this worker's own, for its executor.
    Own(OwnAnswer),
    /// A cancel of a run `worker` holds, told to it once the cancel is stored.
    CancelledRun { worker: WorkerId, run: TaskRunId },
    /// How a task with a client ended, told to the client once the
    /// revision that ended it is stored.
    Concluded(Conclusion),
    /// That a composite is over, told to its client once its terminal
    /// records are stored.
    Finished { client: WorkerId, finished: generated::CompositeFinished },
}

/// What settled answers leave this worker itself to act on.
#[derive(Default)]
pub(super) struct Local {
    /// Its own claims and reports, for its executor.
    pub(super) own: Vec<Settled<OwnAnswer>>,
    /// Stored cancels of running runs, with the worker holding each.
    pub(super) cancelled: Vec<(WorkerId, TaskRunId)>,
}

/// What deciding a request needs this batch: the node and its scheduler,
/// the transport, and the office's record path, which writes what a
/// decision changed and holds its answer until those writes are stored
/// (see [`LeaderRecords::hold`]).
pub(super) struct Deciding<'b, C: Clock, I: IdGenerator> {
    pub(super) node: &'b WorkerNode<C>,
    pub(super) scheduler: &'b mut Scheduler<C, I, RecordOutbox>,
    pub(super) net: &'b Net,
    pub(super) records: &'b mut LeaderRecords<HeldAnswer>,
    pub(super) factor: ReplicationFactor,
}

impl<C: Clock, I: IdGenerator> Deciding<'_, C, I> {
    /// Places and writes every revision the scheduler published since the
    /// last write; returns the writes made.
    pub(super) fn write(&mut self) -> Vec<Write> {
        self.records.write(
            self.node,
            self.scheduler,
            &mut NetRecords {
                net: self.net,
                factor: self.factor,
            },
        )
    }

    /// Writes what the decision just made changed, then holds `held` until
    /// those writes settle, and sends it at once if nothing holds it. One
    /// that made no write of its own waits for the writes of the record
    /// `named` names still unsettled instead (see [`LeaderRecords::hold`]).
    pub(super) fn hold(&mut self, held: HeldAnswer, named: Option<RecordKey>, local: &mut Local) {
        let made = self.write();
        if let Some(settled) = self.records.hold(held, made, named.as_ref()) {
            send_answer(self.net, settled, local);
        }
    }

    /// Holds `held`, which made no write of its own, until the writes of the
    /// record `named` names still unsettled are stored.
    pub(super) fn hold_unwritten(
        &mut self,
        held: HeldAnswer,
        named: Option<RecordKey>,
        local: &mut Local,
    ) {
        self.hold_unwritten_on(held, named.as_slice(), local);
    }

    /// Like [`Self::hold_unwritten`] for an answer about several records:
    /// held until the writes of all of `named` still unsettled are stored.
    pub(super) fn hold_unwritten_on(
        &mut self,
        held: HeldAnswer,
        named: &[RecordKey],
        local: &mut Local,
    ) {
        if let Some(settled) = self.records.hold_on(held, Vec::new(), named) {
            send_answer(self.net, settled, local);
        }
    }
}

/// Decides every inbound `/kabudachi/claim/1` request queued on `net` with
/// `scheduler`'s decision (see `claim::answer`) and writes the revisions that
/// decision made. The answer goes out once those writes are acknowledged,
/// while the scheduler still leads (see [`LeaderRecords::hold`]). One that made no
/// write of its own, because the task it names was already decided, waits
/// for that task's writes still unsettled instead (or, once one was refused,
/// is answered `NotLeader` at once): it tells the asker of
/// what an earlier answer decided, which a leader elected next may never
/// see until those writes land.
pub(super) fn respond_to_claim_requests<C: Clock, I: IdGenerator>(
    deciding: &mut Deciding<'_, C, I>,
    local: &mut Local,
) {
    for handle in deciding.net.poll_claim_requests() {
        if deciding.scheduler.is_leader() && !deciding.node.is_voter_or_pending(&handle.from()) {
            deciding.net.respond_claim(handle, claim::not_member());
            continue;
        }
        let response = claim::answer(deciding.scheduler, &handle.from(), handle.request());
        let task = claimed_task(handle.request()).map(RecordKey::Task);
        deciding.hold(HeldAnswer::Claim { handle, response }, task, local);
    }
}

/// The task a claim request names, if it names one.
fn claimed_task(request: &claim_request::Request) -> Option<TaskId> {
    match request {
        claim_request::Request::TaskId(named) => Some(TaskId::from(named.clone())),
        _ => None,
    }
}

/// Decides every inbound `/kabudachi/task/1` request queued on `net` with
/// `scheduler`'s decision (see `task_exchange::answer`) and writes the
/// revisions that decision made, then holds the answer as
/// [`respond_to_claim_requests`] does. As claims are, task requests are
/// answered only for the voters and pending members of the shard the leader
/// leads: any other worker is answered `NotMember` at once, and its request
/// never reaches the scheduler, unless it asks how a task stands, which
/// decides nothing and is answered for any asker. A submission, a cancel, a
/// report that made no write of its own (its task was already decided, or the
/// report repeats one) or a question of how a task stands waits for that
/// task's writes still unsettled, for the same reason.
pub(super) fn respond_to_task_requests<C: Clock, I: IdGenerator>(
    deciding: &mut Deciding<'_, C, I>,
    clock: &C,
    local: &mut Local,
) {
    for handle in deciding.net.poll_task_requests() {
        if let Some(response) = ungated_answer(deciding, &handle) {
            deciding.net.respond_task(handle, response);
            continue;
        }
        if refused_as_no_member(deciding, &handle) {
            deciding
                .net
                .respond_task(handle, task_exchange::not_member());
            continue;
        }
        let named = record_named(deciding.scheduler, handle.request());
        let response =
            task_exchange::answer(deciding.scheduler, &handle.from(), handle.request(), clock);
        deciding.hold(HeldAnswer::Task { handle, response }, named, local);
    }
}

/// Whether the leader refuses `handle` because its sender is neither a voter
/// nor a pending member. How a task stands decides nothing, so it is
/// answered for any asker, member or not, once that task's writes have
/// settled.
fn refused_as_no_member<C: Clock, I: IdGenerator>(
    deciding: &Deciding<'_, C, I>,
    handle: &TaskRequestHandle,
) -> bool {
    if matches!(handle.request(), task_request::Request::QueryOutcome(_)) {
        return false;
    }
    deciding.scheduler.is_leader() && !deciding.node.is_voter_or_pending(&handle.from())
}

/// The answer to a request any asker gets at once, gated on no write and
/// no membership; `None` for every request the leader decides. Where
/// records go is one: a worker that drained has been taken out of the
/// roster by the time it asks, and the answer decides nothing and names
/// only voters. Whether the leader asks submitters to wait is another: it
/// decides nothing either, and any worker may poll it.
fn ungated_answer<C: Clock, I: IdGenerator>(
    deciding: &Deciding<'_, C, I>,
    handle: &TaskRequestHandle,
) -> Option<TaskResponse> {
    match handle.request() {
        task_request::Request::Place(place) => Some(place_answer(deciding, &handle.from(), place)),
        task_request::Request::QuerySlowDown(_) => Some(slow_down_answer(deciding)),
        _ => None,
    }
}

fn place_answer<C: Clock, I: IdGenerator>(
    deciding: &Deciding<'_, C, I>,
    asker: &WorkerId,
    place: &PlaceRecords,
) -> TaskResponse {
    if !deciding.scheduler.is_leader() {
        return task_exchange::not_leader();
    }
    let voters = deciding.node.placeable_voters();
    task_exchange::placements(place, asker, &voters, deciding.factor)
}

/// Whether this leader asks submitters to wait; a node that does not lead
/// says so.
fn slow_down_answer<C: Clock, I: IdGenerator>(deciding: &Deciding<'_, C, I>) -> TaskResponse {
    if !deciding.scheduler.is_leader() {
        return task_exchange::not_leader();
    }
    task_exchange::slow_down_state(deciding.scheduler.slow_down())
}

/// Answers every inbound `/kabudachi/reconcile/1` request queued on `net`
/// that `node` may answer (see `WorkerNode::may_answer_reconcile`) with a
/// page of what this worker holds, from its own runs and records alone. A
/// request that proves neither that its sender is the leader this worker
/// follows nor an office no earlier than the highest term it has seen is
/// logged and left unanswered, which the asker reads as no answer.
pub(super) fn respond_to_reconcile_requests<C: Clock>(node: &WorkerNode<C>, net: &Net) {
    for handle in net.poll_reconcile_requests() {
        let request = handle.request();
        if !node.may_answer_reconcile(&handle.from(), request.proof.as_ref()) {
            tracing::warn!(
                from = handle.from().as_str(),
                recovery_epoch = request.recovery_epoch,
                term = request.term,
                highest_term_seen = node.highest_term_seen(),
                "refusing a reconciliation request: its sender is not the leader this worker \
                 follows and proves no office for a term this worker could still honour"
            );
            continue;
        }
        tracing::debug!(
            from = handle.from().as_str(),
            recovery_epoch = request.recovery_epoch,
            term = request.term,
            "answering a reconciliation request"
        );
        let Some(page) = page_of(handle.request(), &net.claimed_runs(), &net.held_records()) else {
            tracing::warn!(
                from = handle.from().as_str(),
                "refusing a reconciliation request: its cursor names a record this worker cannot read"
            );
            continue;
        };
        net.respond_reconcile(handle, page);
    }
}

/// Answers every inbound `/kabudachi/steal/1` request queued on `net` with
/// the tasks this worker holds records of that look claimable in the asker's
/// queues, most urgent first by `policy`. Every worker answers: it needs no
/// leadership and decides nothing, since a task it names must still be
/// claimed from the leader.
pub(super) fn respond_to_steal_requests<C: Clock>(net: &Net, clock: &C, policy: &QueuePolicy) {
    let now = WallTime::now(clock);
    for handle in net.poll_steal_requests() {
        let task_ids = candidates_for_steal(
            &net.held_records(),
            now,
            handle.limit(),
            handle.runs_compaction(),
            &handle.queues(),
            policy,
        );
        net.respond_steal(handle, task_ids);
    }
}

/// The record a request names, for the requests that name one (a task, or a
/// composite), or whose run `scheduler` holds (a report names its run).
pub(super) fn record_named<C: Clock, I: IdGenerator>(
    scheduler: &Scheduler<C, I, RecordOutbox>,
    request: &task_request::Request,
) -> Option<RecordKey> {
    let of_run = |run: &Option<_>| {
        let run = TaskRunId::from(Clone::clone(run.as_ref()?));
        scheduler
            .task_run(&run)
            .and_then(|held| held.identity.as_ref()?.task_id.clone())
            .map(|task| RecordKey::Task(TaskId::from(task)))
    };
    let task = |task: &Option<_>| Some(RecordKey::Task(TaskId::from(Clone::clone(task.as_ref()?))));
    let composite = |id: &Option<_>| Some(RecordKey::Composite(CompositeId::from(Clone::clone(id.as_ref()?))));
    match request {
        task_request::Request::Submit(submit) => task(&submit.task_id),
        task_request::Request::Cancel(cancel) => task(&cancel.task_id).or_else(|| composite(&cancel.composite_id)),
        task_request::Request::SubmitComposite(submit) => composite(&submit.composite_id),
        task_request::Request::SubmitMembers(submit) => composite(&submit.composite_id),
        task_request::Request::QuerySlowDown(_) => None,
        task_request::Request::Started(report) => of_run(&report.task_run_id),
        task_request::Request::Completed(report) => of_run(&report.task_run_id),
        task_request::Request::Failed(report) => of_run(&report.task_run_id),
        task_request::Request::Compacted(report) => of_run(&report.task_run_id),
        task_request::Request::Lost(report) => of_run(&report.task_run_id),
        task_request::Request::QueryOutcome(query) => task(&query.task_id),
        task_request::Request::Place(_) => None,
    }
}

pub(super) fn send_answer(net: &Net, settled: Settled<HeldAnswer>, local: &mut Local) {
    match settled {
        Settled::Released(HeldAnswer::Claim { handle, response }) => {
            net.respond_claim(handle, response);
        }
        Settled::NotLeader(HeldAnswer::Claim { handle, .. }) => {
            net.respond_claim(handle, claim::not_leader());
        }
        Settled::Released(HeldAnswer::Task { handle, response }) => {
            net.respond_task(handle, response);
        }
        Settled::NotLeader(HeldAnswer::Task { handle, .. }) => {
            net.respond_task(handle, task_exchange::not_leader());
        }
        Settled::Released(HeldAnswer::Own(answer)) => local.own.push(Settled::Released(answer)),
        Settled::NotLeader(HeldAnswer::Own(answer)) => local.own.push(Settled::NotLeader(answer)),
        Settled::Released(HeldAnswer::CancelledRun { worker, run }) => {
            local.cancelled.push((worker, run));
        }
        // The cancel may not have been stored: the worker learns what the
        // next leader decided when it next reports on the run.
        Settled::NotLeader(HeldAnswer::CancelledRun { .. }) => {}
        Settled::Released(HeldAnswer::Concluded(conclusion)) => tell_client(net, conclusion),
        // The ending may not have been stored: the client asks the next
        // leader how the task stands.
        Settled::NotLeader(HeldAnswer::Concluded(_)) => {}
        Settled::Released(HeldAnswer::Finished { client, finished }) => tell_finished(net, client, finished),
        // The ending may not have been stored: the client reads the
        // composite's record when it next looks.
        Settled::NotLeader(HeldAnswer::Finished { .. }) => {}
    }
}

/// Tells a task's client how its task ended, in the background: its run
/// certified, or the task over without a result. A client that never
/// takes it asks the next leader.
fn tell_client(net: &Net, conclusion: Conclusion) {
    let Conclusion { client, outcome } = conclusion;
    match (outcome.state, outcome.result_digest.clone()) {
        (TaskRunState::Succeeded, Some(digest)) => {
            tokio::spawn(net.send_certified(
                client,
                ResultCertified {
                    task_id: Some(outcome.task_id.into()),
                    task_run_id: Some(outcome.task_run_id.into()),
                    result_digest: Some(digest.into()),
                },
            ));
        }
        _ => {
            let task = outcome.task_id.clone();
            tokio::spawn(net.send_concluded(
                client,
                task_exchange::outcome_answer(&task, Some(outcome)),
            ));
        }
    }
}

/// Tells a composite's client it is over, in the background. The composite is
/// durable either way: a client that never takes it reads the record.
fn tell_finished(net: &Net, client: WorkerId, finished: generated::CompositeFinished) {
    let composite = finished.composite_id.clone().map(CompositeId::from);
    let told = net.send_composite_finished(client, finished);
    tokio::spawn(async move {
        if !told.await {
            tracing::debug!(?composite, "a composite's client did not take that it is over");
        }
    });
}
