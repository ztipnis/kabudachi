use kabudachi_core::election::WorkerNode;
use kabudachi_core::protocol::ids::{IdGenerator, TaskId, TaskRunId, WorkerId};
use kabudachi_core::protocol::messages::{ClaimResponse, TaskResponse, claim_request, task_request};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::Scheduler;
use kabudachi_core::task_record::{LeaderRecords, RecordOutbox, Settled};
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
}

/// What settled answers leave this worker itself to act on.
#[derive(Default)]
pub(super) struct Local {
    /// Its own claims and reports, for its executor.
    pub(super) own: Vec<Settled<OwnAnswer>>,
    /// Stored cancels of running runs, with the worker holding each.
    pub(super) cancelled: Vec<(WorkerId, TaskRunId)>,
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
    node: &WorkerNode<C>,
    scheduler: &mut Scheduler<C, I, RecordOutbox>,
    net: &Net,
    records: &mut LeaderRecords<HeldAnswer>,
    factor: ReplicationFactor,
    local: &mut Local,
) {
    for handle in net.poll_claim_requests() {
        if scheduler.is_leader() && !node.is_voter_or_pending(&handle.from()) {
            net.respond_claim(handle, claim::not_member());
            continue;
        }
        let response = claim::answer(scheduler, &handle.from(), handle.request());
        let made = records.write(node, scheduler, &mut NetRecords { net, factor });
        let task = match handle.request() {
            claim_request::Request::TaskId(named) => Some(TaskId::from(named.clone())),
            _ => None,
        };
        if let Some(settled) =
            records.hold(HeldAnswer::Claim { handle, response }, made, task.as_ref())
        {
            send_answer(net, settled, local);
        }
    }
}

/// Decides every inbound `/kabudachi/task/1` request queued on `net` with
/// `scheduler`'s decision (see `task_exchange::answer`) and writes the
/// revisions that decision made, then holds the answer as
/// [`respond_to_claim_requests`] does. As claims are, task requests are
/// answered only for the voters and pending members of the shard the leader
/// leads: any other worker is answered `NotMember` at once, and its request
/// never reaches the scheduler. A submission, a cancel or a report
/// that made no write of its own (its task was already decided, or the report
/// repeats one) waits for that task's writes still unsettled, for the same
/// reason.
pub(super) fn respond_to_task_requests<C: Clock, I: IdGenerator>(
    node: &WorkerNode<C>,
    scheduler: &mut Scheduler<C, I, RecordOutbox>,
    net: &Net,
    clock: &C,
    records: &mut LeaderRecords<HeldAnswer>,
    factor: ReplicationFactor,
    local: &mut Local,
) {
    for handle in net.poll_task_requests() {
        // A worker that drained has been taken out of the roster by the time
        // it asks where its records go, so this answers any asker; it
        // decides nothing and names only voters.
        if let task_request::Request::Place(place) = handle.request() {
            let response = if scheduler.is_leader() {
                let asker = handle.from();
                let voters: Vec<WorkerId> = node
                    .placeable_voters()
                    .into_iter()
                    .filter(|voter| *voter != asker)
                    .collect();
                task_exchange::placements(place, &voters, factor)
            } else {
                task_exchange::not_leader()
            };
            net.respond_task(handle, response);
            continue;
        }
        if scheduler.is_leader() && !node.is_voter_or_pending(&handle.from()) {
            net.respond_task(handle, task_exchange::not_member());
            continue;
        }
        let named = task_named(scheduler, handle.request());
        let response = task_exchange::answer(scheduler, &handle.from(), handle.request(), clock);
        let made = records.write(node, scheduler, &mut NetRecords { net, factor });
        if let Some(settled) =
            records.hold(HeldAnswer::Task { handle, response }, made, named.as_ref())
        {
            send_answer(net, settled, local);
        }
    }
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
        let page = page_of(
            handle.request(),
            &net.claimed_runs(),
            &net.held_records(),
        );
        net.respond_reconcile(handle, page);
    }
}

/// Answers every inbound `/kabudachi/steal/1` request queued on `net` with
/// the tasks this worker holds records of that look claimable. Every worker
/// answers: it needs no leadership and decides nothing, since a task it names
/// must still be claimed from the leader.
pub(super) fn respond_to_steal_requests<C: Clock>(net: &Net, clock: &C) {
    let now = WallTime::now(clock);
    for handle in net.poll_steal_requests() {
        let task_ids = candidates_for_steal(
            &net.held_records(),
            now,
            handle.limit(),
            handle.runs_compaction(),
        );
        net.respond_steal(handle, task_ids);
    }
}

/// The task a request names, for the requests that name one, or whose run
/// `scheduler` holds (a report names its run).
pub(super) fn task_named<C: Clock, I: IdGenerator>(
    scheduler: &Scheduler<C, I, RecordOutbox>,
    request: &task_request::Request,
) -> Option<TaskId> {
    let of_run = |run: &Option<_>| {
        let run = TaskRunId::from(Clone::clone(run.as_ref()?));
        scheduler
            .task_run(&run)
            .and_then(|held| held.identity.as_ref()?.task_id.clone())
            .map(TaskId::from)
    };
    match request {
        task_request::Request::Submit(task) => task.task_id.clone().map(TaskId::from),
        task_request::Request::Cancel(cancel) => cancel.task_id.clone().map(TaskId::from),
        task_request::Request::Started(report) => of_run(&report.task_run_id),
        task_request::Request::Completed(report) => of_run(&report.task_run_id),
        task_request::Request::Failed(report) => of_run(&report.task_run_id),
        task_request::Request::Compacted(report) => of_run(&report.task_run_id),
        task_request::Request::Lost(report) => of_run(&report.task_run_id),
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
    }
}
