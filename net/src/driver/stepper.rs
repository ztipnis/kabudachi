use kabudachi_core::election::{
    AbortBy, AuthorityCall, AuthorityPerformer, AuthorityReply, HandOffTo, Input, MessageSink,
    Output, Step, WorkerNode, carry_out,
};
use kabudachi_core::protocol::ids::{IdGenerator, TaskId, TaskRunId, WorkerId};
use kabudachi_core::protocol::messages::ElectionMessage;
use kabudachi_core::scheduler::Scheduler;
use kabudachi_core::task_record::{LeaderRecords, RecordOutbox, RecordPorts, Write};
use kabudachi_core::time::{Clock, Instant};

use super::answers::{Deciding, HeldAnswer};
use crate::authority::AuthorityClient;
use crate::messenger::{Net, PlacedWrite};
use crate::task_store::placement::{Placement, ReplicationFactor, placement};

/// The driver run's stepper: what it steps its node with in each batch,
/// kept between batches.
pub(super) struct Stepper<'a, C: Clock, I: IdGenerator, O> {
    pub(super) node: &'a mut WorkerNode<C>,
    pub(super) scheduler: &'a mut Scheduler<C, I, RecordOutbox>,
    pub(super) net: &'a Net,
    pub(super) replication_factor: ReplicationFactor,
    /// The office's record path.
    pub(super) records: LeaderRecords<HeldAnswer>,
    /// The client to perform the node's calls with; `None` answers each at
    /// once as `Unavailable`.
    pub(super) authority: Option<AuthorityClient>,
    /// [`super::run_driver`]'s `observe`.
    pub(super) observe: O,
    /// What the node's steps reported that the batch acts on once they are
    /// all taken.
    pub(super) collected: Collected,
}

/// What steps of the node reported that [`super::run_driver`] acts on after them.
#[derive(Default)]
pub(super) struct Collected {
    /// The run digests that heartbeats heard in office reported, with the
    /// workers that sent them, for the batch to compare (see
    /// [`crate::reconcile::leader::LeaderReconciliation::runs_heard`]).
    pub(super) runs_heard: Vec<(WorkerId, Vec<u8>)>,
    /// Where the node, once it drained, said to hand the records it holds.
    pub(super) hand_off: Option<HandOffTo>,
    /// The latest abort deadline a step reported, if one did this batch.
    pub(super) abort_deadline: Option<Option<AbortBy>>,
    /// The runs the leader's acks said were cancelled, for the executor.
    pub(super) cancelled_runs: Vec<TaskRunId>,
}

impl<C, I, O> Stepper<'_, C, I, O>
where
    C: Clock,
    I: IdGenerator,
    O: FnMut(&WorkerNode<C>, Option<&Input>, &Step),
{
    /// Steps the node with `input`, carries that step out (see
    /// [`Self::carry`]), and returns the node's next deadline.
    pub(super) fn step(&mut self, input: Input) -> Option<Instant> {
        let stepped = self.node.step(input.clone());
        self.carry(stepped, Some(&input))
    }

    /// Carries out `stepped`, a step the node has just taken on `input`,
    /// through `carry_out`: its messages go out through the `Net` and its
    /// authority calls to the [`AuthorityClient`]. With no authority, each
    /// call's `Unavailable` reply is fed back at once instead, carrying out
    /// that step too, until a step asks for no more. Logs every step's
    /// alerts (see [`log_alerts`]), hands every step to `observe`, and
    /// returns the node's next deadline.
    pub(super) fn carry(&mut self, stepped: Step, input: Option<&Input>) -> Option<Instant> {
        let mut performer = Calls(self.authority.as_mut());
        let observe = &mut self.observe;
        let collected = &mut self.collected;
        let net = self.net;
        let next_deadline = carry_out(
            &mut *self.node,
            stepped,
            &mut *self.scheduler,
            &mut &*self.net,
            &mut performer,
            |node, _, reply, step| {
                log_alerts(node, &step.outputs);
                for output in &step.outputs {
                    match output {
                        Output::RunsHeard { worker, digest } => {
                            collected.runs_heard.push((worker.clone(), digest.clone()));
                        }
                        Output::HandOff(to) => collected.hand_off = Some(to.clone()),
                        Output::AbortDeadline(by) => collected.abort_deadline = Some(*by),
                        Output::RunsCancelled(runs) => {
                            collected.cancelled_runs.extend(runs.iter().cloned());
                        }
                        _ => {}
                    }
                }
                net.name_leader(node.known_leader().map(|(leader, _)| leader));
                // Only the first step has no reply for its input.
                observe(node, reply.or(input), step);
            },
        );
        self.write_revisions();
        next_deadline
    }

    /// Places and writes every revision the scheduler published since the
    /// last call.
    pub(super) fn write_revisions(&mut self) {
        let mut ports = self.ports();
        self.records.write(self.node, self.scheduler, &mut ports);
    }
}

impl<'a, C: Clock, I: IdGenerator, O> Stepper<'a, C, I, O> {
    /// The leader-side view of this batch for deciding requests (see
    /// [`Deciding`]).
    pub(super) fn deciding(&mut self) -> Deciding<'_, C, I> {
        Deciding {
            node: &*self.node,
            scheduler: &mut *self.scheduler,
            net: self.net,
            records: &mut self.records,
            factor: self.replication_factor,
        }
    }

    /// The record path's ports over this batch's `Net`.
    pub(super) fn ports(&self) -> NetRecords<'a> {
        NetRecords {
            net: self.net,
            factor: self.replication_factor,
        }
    }
}

/// A leader's record path over the network: kad's placement among the
/// voters, and `Net`'s writes.
pub(super) struct NetRecords<'a> {
    pub(super) net: &'a Net,
    pub(super) factor: ReplicationFactor,
}

impl RecordPorts for NetRecords<'_> {
    fn place(&self, task: &TaskId, voters: &[WorkerId]) -> Option<Placement> {
        placement(task, voters, self.factor)
    }

    fn write(&mut self, writes: Vec<PlacedWrite>) {
        self.net.write_records(writes);
    }

    fn refuse(&mut self, write: Write) {
        self.net.refuse_write(write);
    }
}

/// Performs the node's authority calls through its client, or, with none,
/// answers each at once as `Unavailable`.
struct Calls<'a>(Option<&'a mut AuthorityClient>);

impl AuthorityPerformer for Calls<'_> {
    fn perform(&mut self, call: AuthorityCall) -> Option<AuthorityReply> {
        match self.0.as_deref_mut() {
            Some(client) => client.perform(call),
            None => Some(call.unavailable()),
        }
    }
}

impl MessageSink for &Net {
    fn send(&mut self, to: WorkerId, message: ElectionMessage) {
        Net::send(self, to, message);
    }

    fn publish(&mut self, message: ElectionMessage) {
        Net::publish(self, message);
    }
}

/// Logs what `outputs`, one step of `node`'s, report that an operator must
/// know of: the deadline by which the worker must
/// abort its TaskRuns, or its lifting, and the shard's recovery epoch gone
/// from the authority.
fn log_alerts<C: Clock>(node: &WorkerNode<C>, outputs: &[Output]) {
    for output in outputs {
        match output {
            Output::AbortDeadline(Some(by)) => tracing::warn!(
                shard = node.shard_id().as_str(),
                by = ?by,
                "this worker cannot show that its leader still hears it, or has fenced itself, \
                 and must abort each TaskRun it is running by that run's deadline unless that \
                 changes"
            ),
            Output::AbortDeadline(None) => tracing::info!(
                shard = node.shard_id().as_str(),
                "this worker's leader hears it again: it keeps its TaskRuns"
            ),
            Output::ShardAbandoned => tracing::error!(
                shard = node.shard_id().as_str(),
                recovery_epoch = node.recovery_epoch(),
                "the shard's recovery epoch is gone from the coordination authority; this \
                 worker has stopped"
            ),
            Output::Send { .. }
            | Output::Publish { .. }
            | Output::StateChanged(_)
            | Output::Grant(_)
            | Output::Reconcile(_)
            | Output::Authority(_)
            | Output::RunsHeard { .. }
            | Output::RunsCancelled(_)
            | Output::HandOff(_)
            | Output::WorkerLost(_)
            | Output::WorkerSilence { .. } => {}
        }
    }
}
