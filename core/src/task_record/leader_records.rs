//! What a leader office does with its scheduler's revisions.

mod reconciliation;

pub use reconciliation::OfficeReconciliation;

use crate::election::{Input, WorkerNode};
use crate::protocol::generated::TaskRecord;
use crate::protocol::ids::{IdGenerator, TaskId, WorkerId};
use crate::protocol::worker_state::WorkerState;
use crate::reconcile::ReconcileTerm;
use crate::scheduler::{Observer, ReconcileRefused, Scheduler};
use crate::time::{Clock, Duration, Instant};

use super::{
    EffectGate, PlacedWrite, Repair, Settled, Settlement, Waits, Write, WriteLedger, WriteOrder,
    WriteOutcome,
};
use reconciliation::{Progress, Stuck};

/// Where one revision of a record is written and how many must store it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placement {
    /// The voters that hold the record, nearest the record's key first.
    pub holders: Vec<WorkerId>,
    /// How many of them must store it before the write counts: a majority,
    /// so any later read of `holders.len() - quorum + 1` of them meets it.
    pub quorum: usize,
}

/// How a leader's records reach its shard: where a record of a task is
/// held, and the writes that carry a revision there. Every worker of a shard
/// must place records the same way.
pub trait RecordPorts {
    /// The holders of a record of `task` among `voters`, and how many of
    /// them must store a revision; `None` when it cannot be placed there.
    fn place(&self, task: &TaskId, voters: &[WorkerId]) -> Option<Placement>;
    /// Starts each write. Each outcome comes back later, as a
    /// [`WriteOutcome`] for [`LeaderRecords::settle`].
    fn write(&mut self, writes: Vec<PlacedWrite>);
    /// `write` could not be placed: its outcome is to come back as not
    /// stored, like a write no holder stored.
    fn refuse(&mut self, write: Write);
}

/// A scheduler observer that keeps every revision it is handed until a
/// leader's record path takes them to write.
pub trait PublishedRevisions {
    /// Every revision published since the last call, oldest first.
    fn take_published(&mut self) -> Vec<TaskRecord>;
}

/// What a leader office does with its scheduler's revisions, without I/O:
/// it places each on the voters and writes it through its ports, holding a
/// superseded generation's revision back until its successor's is stored;
/// it keeps the writes whose outcome still bears on what the leader may
/// tell; it holds each answer (`E`) until the writes its decision made are
/// stored while the scheduler leads, and answers it `NotLeader` otherwise;
/// it writes records again where they belong when the voters change or a
/// write was refused; and, while the office reconciles, it rebuilds the
/// scheduler, republishes at the office's term and retries stuck work. It
/// keeps nothing past an office: refusals of one office answer no question
/// of the next.
pub struct LeaderRecords<E> {
    /// The office it last saw, to tell one office from the next even when one
    /// was lost and another won between two looks.
    office: Option<ReconcileTerm>,
    /// The writes whose outcome still bears on what the leader may tell.
    ledger: WriteLedger,
    /// Holds a superseded generation's revision behind its successor's.
    order: WriteOrder,
    /// Where each record went, for putting it where it belongs once the
    /// voters change or a write is refused.
    repair: Repair,
    /// The answers decided and not yet told: each waits for the writes its
    /// decision made.
    held: EffectGate<E>,
    /// How long after a refusal a record is published again.
    retry_after: Duration,
}

/// What a reconciliation asks of its driver next (see
/// [`LeaderRecords::reconcile`]).
// Each turn is matched at once, never kept, so it is not boxed.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum Turn {
    /// Nothing more until something arrives or the reconciliation's
    /// [`OfficeReconciliation::wake_at`] comes.
    Wait,
    /// Step the node with this input and carry the step out, writing what the
    /// scheduler published as [`LeaderRecords::write`] does; then call again.
    Step(Input),
    /// The node no longer holds the reconciliation's office: drop it.
    Ended,
}

/// The office whose reconciliation is to start now: the node holds it, is
/// still reconciling it, and its scheduler awaits the rebuild for it.
pub fn office_to_reconcile<C: Clock, I: IdGenerator, O: Observer>(
    node: &WorkerNode<C>,
    scheduler: &Scheduler<C, I, O>,
) -> Option<ReconcileTerm> {
    let office = node.office_term()?;
    (node.state() == WorkerState::LeaderReconciling && scheduler.reconciling() == Some(office))
        .then_some(office)
}

impl<E> LeaderRecords<E> {
    /// `office` is the node's office now; a refused write is published again
    /// after `retry_after`.
    pub fn new(office: Option<ReconcileTerm>, retry_after: Duration) -> Self {
        LeaderRecords {
            office,
            ledger: WriteLedger::default(),
            order: WriteOrder::default(),
            repair: Repair::new(retry_after),
            held: EffectGate::new(),
            retry_after,
        }
    }

    /// Notes the node's office. When it changed, forgets the last office's
    /// writes and repair, and returns every answer still held, to be answered
    /// `NotLeader`: the next office rebuilt from what was stored, which need
    /// not hold the decision.
    #[must_use]
    pub fn follow_office(&mut self, office: Option<ReconcileTerm>) -> Vec<E> {
        if office == self.office {
            return Vec::new();
        }
        self.office = office;
        self.ledger.clear();
        self.order.clear();
        self.repair = Repair::new(self.retry_after);
        self.held.lease_ended()
    }

    /// Places and writes every revision the scheduler published since the
    /// last call, and returns their writes for an answer to wait on.
    pub fn write<C: Clock, I: IdGenerator, O: Observer + PublishedRevisions>(
        &mut self,
        node: &WorkerNode<C>,
        scheduler: &mut Scheduler<C, I, O>,
        ports: &mut impl RecordPorts,
    ) -> Vec<Write> {
        let revisions = scheduler.observer_mut().take_published();
        let writes: Vec<Write> = revisions.iter().map(Write::of).collect();
        let admitted = self.order.admit(revisions);
        self.place_and_write(node, admitted, ports);
        self.ledger.made(&writes);
        writes
    }

    /// Holds `answer` until `made`, the writes its call made, are stored while
    /// the scheduler leads. A call about `task` that made no write waits for
    /// that task's writes still unsettled instead, since it tells of what an
    /// earlier answer decided. Returns it settled at once when nothing need
    /// wait: released when there is nothing to wait for, `NotLeader` when one
    /// of the task's writes was refused.
    #[must_use]
    pub fn hold(
        &mut self,
        answer: E,
        made: Vec<Write>,
        task: Option<&TaskId>,
    ) -> Option<Settled<E>> {
        let mut writes = made;
        if writes.is_empty()
            && let Some(task) = task
        {
            match self.ledger.waits_on(task) {
                Waits::Refused => return Some(Settled::NotLeader(answer)),
                Waits::Writes(pending) => writes.extend(pending),
            }
        }
        self.held.hold(answer, writes)
    }

    /// Settles `outcomes`, the write outcomes that arrived: notes each for
    /// repair, lets `republished` take those of a republish, releases the
    /// revisions held behind a stored one, and returns the answers settled,
    /// in order. Ends with [`Self::end_unless_leading`].
    #[must_use]
    pub fn settle<C: Clock, I: IdGenerator, O: Observer>(
        &mut self,
        node: &WorkerNode<C>,
        scheduler: &Scheduler<C, I, O>,
        mut outcomes: Vec<WriteOutcome>,
        mut republished: impl FnMut(&WriteOutcome) -> bool,
        now: Instant,
        ports: &mut impl RecordPorts,
    ) -> Vec<Settled<E>> {
        for outcome in &outcomes {
            self.repair.settled(outcome, now);
        }
        // The republish's own writes first; every other outcome is the gate's.
        outcomes.retain(|outcome| !republished(outcome));
        // Read once per call. A loss and regain of leadership inside one call
        // would keep the old term's refused entries, but the scheduler
        // exposes no term identity to tell the terms apart (a lease's end moves
        // with every renewal), and the leftover only errs safe: it answers
        // `NotLeader` for a task until a newer revision of it is stored.
        let leading = scheduler.is_leader();
        let mut settled = Vec::new();
        for outcome in outcomes {
            self.ledger.settled(&outcome.write, outcome.stored, leading);
            // A superseded generation's revision is written only once its
            // successor's is stored while the leader still leads.
            let mut refused = Vec::new();
            match self
                .order
                .settled(&outcome.write, outcome.stored && leading)
            {
                Settlement::Release(records) => self.place_and_write(node, records, ports),
                Settlement::Refuse(writes) => {
                    for write in writes {
                        self.ledger.settled(&write, false, leading);
                        refused.push(write);
                    }
                }
            }
            if outcome.stored {
                settled.extend(self.held.acknowledged(&outcome.write, leading));
            } else {
                tracing::debug!(
                    task = outcome.write.task_id.as_str(),
                    "a record revision was not stored at its quorum"
                );
                refused.push(outcome.write);
            }
            for write in refused {
                let newer = self.ledger.pending_newer_than(&write);
                settled.extend(
                    self.held
                        .refused(&write, &newer)
                        .into_iter()
                        .map(Settled::NotLeader),
                );
            }
        }
        settled.extend(self.end_unless_leading(leading));
        settled
    }

    /// Unless the scheduler leads: every held answer, settled `NotLeader`, and
    /// the ledger and write order forgotten.
    #[must_use]
    pub fn end_unless_leading(&mut self, leading: bool) -> Vec<Settled<E>> {
        if leading {
            return Vec::new();
        }
        self.ledger.clear();
        self.order.clear();
        self.held
            .lease_ended()
            .into_iter()
            .map(Settled::NotLeader)
            .collect()
    }

    /// Has the scheduler publish again the records the voters' changes, or
    /// refused writes, call for, and writes them where they belong now.
    pub fn repair<C: Clock, I: IdGenerator, O: Observer + PublishedRevisions>(
        &mut self,
        node: &WorkerNode<C>,
        scheduler: &mut Scheduler<C, I, O>,
        now: Instant,
        ports: &mut impl RecordPorts,
    ) {
        let in_office = node.office_term().is_some();
        let placeable = if in_office {
            node.placeable_voters()
        } else {
            Vec::new()
        };
        let tasks = {
            let (scheduler, ports) = (&*scheduler, &*ports);
            self.repair.check(
                in_office,
                scheduler.is_leader(),
                &placeable,
                |task| scheduler.holds(task),
                |task| ports.place(task, &placeable).map(|placed| placed.holders),
                now,
            )
        };
        if !tasks.is_empty() && scheduler.republish(&tasks) > 0 {
            self.write(node, scheduler, ports);
        }
    }

    /// Takes `reconciliation` one turn on: rebuilds the scheduler when the
    /// round may stop, places and republishes its records, retries stuck
    /// work, and once the node leads hands the scheduler what late answers
    /// teach. A turn that needs the node stepped returns [`Turn::Step`].
    pub fn reconcile<C: Clock, I: IdGenerator, O: Observer + PublishedRevisions>(
        &mut self,
        reconciliation: &mut OfficeReconciliation,
        node: &WorkerNode<C>,
        scheduler: &mut Scheduler<C, I, O>,
        lookups_done: bool,
        now: Instant,
        ports: &mut impl RecordPorts,
    ) -> Turn {
        if let Some(learnt) = reconciliation.take_learnt() {
            // The node was stepped with a `Tick` first, which checks its
            // lease: one that ended took it out of office, and with it the
            // reconciliation, before the scheduler is handed anything.
            if node.office_term() != Some(reconciliation.office()) {
                return Turn::Ended;
            }
            let found = learnt.records.clone();
            match scheduler.adopt(learnt) {
                Ok(adopted) => {
                    self.repair.found(&found);
                    if !adopted.silent_holders.is_empty() {
                        return Turn::Step(Input::WatchWorkers(adopted.silent_holders));
                    }
                }
                // Holding office does not mean leading: the grant also ends
                // with the recovery fence, which the node can renew, and has
                // not arrived before a quorum confirms the office. The round
                // has given the knowledge up, so it takes it back and offers
                // it again once the scheduler leads.
                Err(learnt) => {
                    tracing::debug!("late reconciliation answers were not adopted: not leading");
                    reconciliation.give_back(learnt);
                    return Turn::Wait;
                }
            }
            self.write(node, scheduler, ports);
        }
        loop {
            match reconciliation.progress(node, scheduler.is_leader(), lookups_done, now, ports) {
                Progress::Waiting => return Turn::Wait,
                Progress::Rebuild(rebuild) => {
                    // Where the rebuild's records were held is noted first, so
                    // a revision that moves one is written to the holders it
                    // left too.
                    self.repair.found(&rebuild.records);
                    match scheduler.reconcile(rebuild) {
                        Ok(rebuilt) => {
                            tracing::info!(
                                republished = rebuilt.republished,
                                uncertain = rebuilt.uncertain,
                                "a new leader rebuilt its scheduler from its shard"
                            );
                            let revisions = scheduler.observer_mut().take_published();
                            self.place_republish(reconciliation, node, revisions, &*ports);
                            if !rebuilt.silent_holders.is_empty() {
                                return Turn::Step(Input::WatchWorkers(rebuilt.silent_holders));
                            }
                        }
                        Err(ReconcileRefused { rejection, rebuild }) => {
                            // Nothing was installed, so the same rebuild is
                            // offered again; leading without it would serve an
                            // empty shard.
                            tracing::error!(%rejection, "the scheduler did not take the rebuild: not leading");
                            reconciliation.stuck(
                                Stuck::Rebuild(rebuild),
                                node.placeable_voters(),
                                now,
                            );
                        }
                    }
                }
                Progress::Place(records) => {
                    self.place_republish(reconciliation, node, records, &*ports);
                }
                Progress::RePlace => {
                    let voters = node.placeable_voters();
                    let (repair, ports) = (&mut self.repair, &*ports);
                    reconciliation.re_place(
                        |write| match ports.place(&Write::of(&write.record).task_id, &voters) {
                            Some(Placement { holders, quorum }) => {
                                write.record.placement =
                                    holders.into_iter().map(Into::into).collect();
                                write.quorum = quorum;
                                repair.written(write, |holder| node.is_member(holder));
                            }
                            None => tracing::error!(
                                task = Write::of(&write.record).task_id.as_str(),
                                "a republished record could not be placed on the new voters"
                            ),
                        },
                        now,
                    );
                }
                // The node leads once stepped: answers that came while it was
                // republishing are taken on the next turn.
                Progress::Republished(office) => return Turn::Step(Input::Reconciled(office)),
                Progress::Learnt(learnt) => {
                    reconciliation.hold_learnt(learnt);
                    return Turn::Step(Input::Tick);
                }
            }
        }
    }

    /// When the driver must next wake for the record path: a refused write
    /// due again, or, while answers are held, one tick past `lease_end`, so
    /// they are answered `NotLeader` even if nothing else arrives.
    pub fn wake_at(&self, lease_end: Option<Instant>) -> Option<Instant> {
        // A held answer is released `NotLeader` when the scheduler's lease
        // ends, and nothing else guarantees a wake then: the election's own
        // deadlines are not shown to coincide with the grant's end (the
        // earlier of its quorum and fence ends), and no write outcome or
        // arrival need come. One tick past the end, so a sleep that counts
        // whole ticks never fires before the lease has ended.
        let lease_wake = if self.held.is_empty() {
            None
        } else {
            lease_end.map(|end| end + Duration::from_ticks(1))
        };
        [lease_wake, self.repair.wake_at()]
            .into_iter()
            .flatten()
            .min()
    }

    /// Places the republished `records` on the voters and starts writing them. If
    /// any cannot be placed none is written, and all are kept to be placed again
    /// when the voters change or after a suspicion timeout: leading without every
    /// record written again would leave a late write of the last leader unfenced.
    fn place_republish<C: Clock>(
        &mut self,
        reconciliation: &mut OfficeReconciliation,
        node: &WorkerNode<C>,
        records: Vec<TaskRecord>,
        ports: &impl RecordPorts,
    ) {
        let (mut writes, unplaced) = place(node, records, ports);
        if unplaced.is_empty() {
            for write in &mut writes {
                self.repair.written(write, |holder| node.is_member(holder));
            }
            reconciliation.republishing(
                writes,
                node.timings().heartbeat_interval,
                node.placeable_voters(),
            );
        } else {
            tracing::error!(
                unplaced = unplaced.len(),
                "records could not be placed on the voters: not leading"
            );
            let all = writes
                .into_iter()
                .map(|write| write.record)
                .chain(unplaced)
                .collect();
            reconciliation.stuck(Stuck::Place(all), node.placeable_voters(), node.now());
        }
    }

    /// Places `records` on the node's voters and writes them; each write's
    /// outcome comes back through the ports' owner.
    fn place_and_write<C: Clock>(
        &mut self,
        node: &WorkerNode<C>,
        records: Vec<TaskRecord>,
        ports: &mut impl RecordPorts,
    ) {
        let (mut placed, unplaced) = place(node, records, &*ports);
        for write in &mut placed {
            self.repair.written(write, |holder| node.is_member(holder));
        }
        // No placement means either the leader has just stopped leading (a
        // scheduler publishes only while it leads, so its own roster no longer
        // holds it), or a voter id is not a peer id, so it cannot be placed:
        // either way the write counts as refused.
        for record in unplaced {
            ports.refuse(Write::of(&record));
        }
        ports.write(placed);
    }
}

/// `records` placed on the node's voters, and those that could not be.
fn place<C: Clock>(
    node: &WorkerNode<C>,
    records: Vec<TaskRecord>,
    ports: &impl RecordPorts,
) -> (Vec<PlacedWrite>, Vec<TaskRecord>) {
    let voters = node.placeable_voters();
    let (mut placed, mut unplaced) = (Vec::new(), Vec::new());
    for mut record in records {
        match ports.place(&Write::of(&record).task_id, &voters) {
            Some(Placement { holders, quorum }) => {
                record.placement = holders.into_iter().map(Into::into).collect();
                placed.push(PlacedWrite::new(record, quorum));
            }
            None => unplaced.push(record),
        }
    }
    (placed, unplaced)
}
