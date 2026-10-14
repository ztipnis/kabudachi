//! The scheduler: it holds every Task and TaskRun and is the single authority
//! on who may run what.
//!
//! It only reacts to calls and never waits, sleeps or does I/O, so the runtime
//! drives it and a test can drive it with a fake clock. It accepts claims and
//! reports only while it holds a leadership grant whose lease has not ended
//! by its own clock: a node that has not won an election, has lost
//! leadership, or has outlived its lease must not decide anything.
//!
//! Only the leader accepts a submission, and only while it leads does it
//! publish records: at the end of every call that changed a task, the
//! observer is handed that task's whole record, versioned by the leader's
//! term and a revision counter, so the next leader can rebuild from them.
//!
//! It owns what time does to its tasks. [`Scheduler::catch_up`] is the one
//! time-driven entry: it forgets finished tasks whose result TTL has passed
//! and, only while the scheduler leads, releases delayed tasks that are due
//! and expires pending ones. [`Scheduler::next_deadline`] says when to call
//! it next. Every call that changes the scheduler also forgets what has
//! outlived its TTL, so a caller never sweeps.
//!
//! A scheduler whose node took office does not lead at once: it first
//! rebuilds from what its shard holds ([`Scheduler::begin_reconcile`],
//! [`Scheduler::reconcile`]), replacing whatever it held and never merging,
//! and only the grant of that office lets it decide anything. The rebuild
//! installs every record known for certain, applies what the workers that
//! answered report about their runs (a run its worker still holds is adopted,
//! one it answered without is lost and replayed unless a newer generation of
//! its key waits, a result no leader certified is certified, a failure no
//! leader recorded is applied, and a run whose task has no known record is
//! rebuilt from its own claim), and writes every installed record again at the
//! office's term, so that a late write of the leader before it loses.
//!
//! A task whose newest record is not yet known is uncertain. It is not
//! scheduled, written or cancelled, and a claim, cancel or report naming it
//! gets a retryable not-ready answer rather than an unknown-run one. Every
//! other generation of its coalescing key is held back with it, since no
//! generation of a key may run while one of them might already be running;
//! tasks without a key are never held back. What a worker reported for an
//! uncertain task is kept and applied once its record arrives, and
//! [`Scheduler::adopt`] takes that late knowledge, and answers that come after
//! the grant, through the same rules. A run decided shortly before a worker
//! was asked is not lost for being missing from its answer, since the
//! worker's claim answer may still be on its way.
//!
//! Silences reported while the scheduler reconciles are kept and acted on
//! once the grant arrives. A worker that holds a run the records name
//! but never answered is returned as a silent holder, for the election to
//! watch as a lost worker. A worker the election reports silent has each run
//! it holds lost once that run's own reconnect timeout has passed (see
//! [`Scheduler::note_silence`]).
//!
//! A coalescing key's waiting chain is kept under a bound. Once it holds more
//! than [`COMPACTION_SOFT_BYTES`] of payload (or memory is past its soft
//! limit), and some worker has said it runs compaction
//! ([`Scheduler::set_compaction_runners`]), the scheduler makes an internal
//! compaction task naming the oldest entries of the chain; a worker folds
//! them and [`Scheduler::complete_compaction`] swaps in the result only if the
//! chain still starts with those entries (see `compaction`). With no such
//! worker the newest generation folds its whole chain itself. No claim ever
//! outgrows one message: a submission that would make its key's waiting claim
//! too large is refused with `KeyBackpressure`, and a fold that does so fails
//! the newest generation (`CoalescedPayloadTooLarge`).
//!
//! A run may return a step instead of a result ([`Completion::Continues`]).
//! The run is certified under the step's digest and carries the step, so the
//! revision that certifies it commits the step with it; the scheduler runs
//! the step as a composite derived from that run. The task keeps its key and
//! its memory until that composite is over, and then finishes; cancelling the
//! task cancels the composite.
//!
//! Tasks and runs are stored privately and handed out only as shared
//! references or clones, so nothing outside can edit a submitted Task or move
//! a run without going through the transition table.

use std::collections::{BTreeMap, BTreeSet};

use crate::coalescing::{self, Key, Occupancy};
use crate::coordination_authority::RecoveryEpoch;
use crate::protocol::digest::Digest;
use crate::protocol::generated::CoalescingLink;
use crate::protocol::ids::{IdGenerator, TaskId, TaskRunId, WorkerId};
use crate::protocol::messages::prelude::*;
use crate::protocol::messages::{Task, TaskRun};
use crate::protocol::records::TaskRunRecord;
use crate::queues::{QueuePolicy, Subscription};
use crate::task_record::{RecordKey, RecordVersion};
use crate::time::{Clock, Duration, Instant};

mod backlog;
mod blocked;
mod cancel;
mod claims;
mod compaction;
pub mod composite;
mod deadlines;
mod failures;
mod fairness;
mod finish;
mod leadership;
mod loss;
mod memory;
mod memory_budget;
mod observer;
mod outcome;
mod publication;
mod rebuild;
mod reconciliation;
mod record_size;
mod reports;
mod retention;
mod submission;
mod waiting_room;

pub use backlog::Backlog;
use blocked::BlockedTask;
pub use blocked::{BlockRejection, Blocked, DEFAULT_VERSION_BLOCK_TTL};
pub use cancel::{CancelRejection, Cancellation, HeldCancel};
pub use claims::{Claim, ClaimRejection};
use composite::Composites;
pub use compaction::Compacted;
pub use deadlines::CaughtUp;
pub use failures::{Failure, MAX_FAILURE_MESSAGE_BYTES};
use fairness::Turns;
use leadership::Leadership;
pub use leadership::{LeadershipGrant, LeaseEnd};
pub use loss::LostRun;
pub use memory::MemoryLimits;
use memory_budget::MemoryBudget;
pub use observer::{NoObserver, Observer};
pub use outcome::{Conclusion, Outcome, OutcomeRejection};
pub use rebuild::{Adopted, ReconcileRefused, ReconcileRejection, Reconciled};
use reconciliation::Reconciliation;
pub use reports::{Certification, Completion, ReportRejection, Reported};
use retention::Retention;
pub use submission::{Submission, SubmitRejection, Submitted, TraceContext, mint};
use waiting_room::WaitingRoom;

/// The largest message a worker accepts, which bounds what one claim can
/// carry. `net`'s framing limit is this size, and it checks that at compile
/// time.
pub const MAX_CLAIM_FRAME_BYTES: u64 = 1024 * 1024;

/// What a claim needs besides what a submission chooses: the task's IDs,
/// times and flags, the run's ID and the message wrapping. A generous bound
/// (generated IDs are capped at `MAX_ID_BYTES`, which the scheduler checks),
/// so a task within [`MAX_SUBMISSION_BYTES`] always fits a claim response.
const CLAIM_OVERHEAD_BYTES: u64 = 4 * 1024;

/// The most a task may weigh at submission: its input, queue, definition ID
/// and coalescing key together. A task any bigger could never be handed to a
/// worker, so it is refused at once instead of sitting pending for ever.
///
/// A coalescing task's claim also carries its retained chain, and the same
/// bound covers the two together: a submission that would make its key's
/// waiting claim larger is refused with `KeyBackpressure`, and a fold that
/// does so fails the newest generation, so no claim ever outgrows a frame.
/// Compaction folds a chain long before that.
pub const MAX_SUBMISSION_BYTES: u64 = MAX_CLAIM_FRAME_BYTES - CLAIM_OVERHEAD_BYTES;

/// A waiting generation whose retained chain holds more payload than this
/// gets a compaction run: half of what one claim may carry, so a chain is
/// folded long before its newest generation could no longer be handed out.
pub const COMPACTION_SOFT_BYTES: u64 = MAX_SUBMISSION_BYTES / 2;

/// What a size measurement of a record leaves out and so keeps free: the
/// version and publication time, whose encoding grows with their values, and
/// the placement.
const RECORD_RESERVE_BYTES: u64 = 1024;

/// What a claim spends on each payload of a chain besides its bytes: the
/// field's tag and its length.
const CHAIN_ENTRY_FRAMING_BYTES: u64 = 6;

/// The failure kind of a coalescing generation whose folded chain grew too
/// large to hand to any worker.
pub const COALESCED_PAYLOAD_TOO_LARGE: &str = "CoalescedPayloadTooLarge";

/// The failure kind of a run that returned a step too large for its task's
/// record.
pub const CONTINUATION_TOO_LARGE: &str = "ContinuationTooLarge";

/// The failure kind of a task that cannot run at its input version.
pub const MIGRATION_ERROR: &str = "MigrationError";

/// A version for measuring a record's size, which does not depend on it.
const UNVERSIONED: RecordVersion = RecordVersion {
    recovery_epoch: RecoveryEpoch::new(0, 0),
    leader_term: 0,
    revision: 0,
};

/// Something the scheduler decided on its own, because time passed, that
/// whoever drives it has to act on. Drained with [`Scheduler::take_events`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// A task was still pending at its expiry, so it will never run.
    Expired {
        task_id: TaskId,
        task_run_id: TaskRunId,
    },
    /// A pending generation was replaced by a newer one of the same
    /// coalescing key, `by`, which absorbed its payload.
    Superseded {
        task_id: TaskId,
        task_run_id: TaskRunId,
        by: TaskId,
    },
    /// Memory use crossed the soft limit (`active`), or fell far enough below
    /// it again (not `active`). Raised only when that changes.
    SlowDown { active: bool },
    /// A task was cancelled. If `was_running`, a worker had claimed it and
    /// has to be told to stop the body; nothing that worker reports about the
    /// run counts any more.
    Cancelled {
        task_id: TaskId,
        task_run_id: TaskRunId,
        was_running: bool,
    },
    /// A run of this task ended and the attempt that would follow it was not
    /// created: its record would have grown past the largest a record may be.
    /// The task is over.
    RecordFull { task_id: TaskId },
    /// A compaction folded a key's chain into a payload too large to hand
    /// out with its newest generation, which therefore failed: its client
    /// should learn its task will never run.
    CoalescedPayloadTooLarge {
        task_id: TaskId,
        task_run_id: TaskRunId,
    },
    /// A composite is over: its client, or the handle of the task it
    /// continues, is to be told.
    CompositeFinished(composite::CompositeFinished),
    /// A task ended FAILED (`MigrationError`) because no worker could run its
    /// input version: one reported it incompatible, or it waited past the
    /// block TTL. `reason` is what the worker wrote.
    MigrationFailed {
        task_id: TaskId,
        task_run_id: TaskRunId,
        reason: String,
    },
}

/// Owns all Tasks and TaskRuns and decides every claim and report. Drive it
/// with calls; it never blocks or does I/O.
pub struct Scheduler<C: Clock, I: IdGenerator, O: Observer = NoObserver> {
    clock: C,
    ids: I,
    /// Handed every revision the scheduler publishes.
    observer: O,
    /// Its grant, lease and publishing versions.
    leadership: Leadership,
    tasks: BTreeMap<TaskId, Task>,
    runs: BTreeMap<TaskRunId, TaskRun>,
    /// The run that is currently authoritative for each task.
    current_run: BTreeMap<TaskId, TaskRunId>,
    /// Every run of each task, oldest attempt first.
    runs_of_task: BTreeMap<TaskId, Vec<TaskRunId>>,
    /// The tasks waiting to start: queued, delayed, or with an expiry.
    waiting: WaitingRoom,
    /// When finished tasks are forgotten.
    retention: Retention,
    /// What the scheduler decided on its own, waiting to be taken.
    events: Vec<Event>,
    /// Runs stored as cancelled that their workers reported they still hold,
    /// waiting to be taken once the scheduler leads.
    held_cancels: Vec<HeldCancel>,
    /// Which generation of each coalescing key waits, and which one runs.
    occupancy: Occupancy,
    /// The bytes tasks hold, the limits on them and `SlowDown`.
    budget: MemoryBudget,
    /// The bytes of results a worker read as the input of a member's run,
    /// counted in `budget` until the run is decided.
    member_inputs: BTreeMap<TaskRunId, u64>,
    /// Tasks whose run is certified but whose continuation composite is not over.
    continuing: BTreeSet<TaskId>,
    /// How many runs of each task were lost with their worker. A replay of a
    /// lost run is not a retry, so these attempts do not count against the
    /// task's retries.
    losses: BTreeMap<TaskId, u32>,
    /// Tasks changed since their last published revision.
    unpublished: BTreeSet<TaskId>,
    /// Each task's input digest, computed once at submission.
    input_digests: BTreeMap<TaskId, Digest>,
    /// Each coalescing generation's links: who superseded it, whom it absorbed.
    links: BTreeMap<TaskId, CoalescingLink>,
    /// What a rebuild left it: see [`Reconciliation`].
    reconciliation: Reconciliation,
    /// When this scheduler last changed each run. A worker asked what it
    /// holds may not have received a run decided shortly before the question.
    run_decided_at: BTreeMap<TaskRunId, Instant>,
    /// The workers that run compaction, as their node's heartbeats say.
    compaction_runners: BTreeSet<WorkerId>,
    /// The workers its node's office counts silent, each with the instant
    /// from which the reconnect timeouts of the runs it holds count (see
    /// [`Scheduler::note_silence`]).
    silent: BTreeMap<WorkerId, Instant>,
    /// The waiting generation of each key whose last compaction failed: a
    /// merge that failed once would fail again on the same chain, so no new
    /// compaction is made for that generation.
    failed_compactions: BTreeMap<Key, TaskId>,
    /// Conclusions not yet taken, oldest first.
    conclusions: Vec<Conclusion>,
    /// How the leader orders and limits each queue (see `crate::queues`).
    queue_policy: QueuePolicy,
    /// Each queue's turns in its tier's round (see `fairness::Turns`).
    turns: Turns,
    /// The queues each worker takes, as its heartbeats say. A worker not here
    /// may take any queue.
    subscriptions: BTreeMap<WorkerId, Subscription>,
    /// Per queue, the runs that were claimed or running when added. Finished
    /// runs are ignored when counted and dropped as runs are added.
    running: BTreeMap<String, BTreeSet<TaskRunId>>,
    /// The composites it drives, and keeps until their retention ends.
    composites: Composites,
    /// Tasks some worker could not run at their input version and left for
    /// another: since when, why, and (not stored; a successor starts it
    /// empty) which workers this leader must not hand them to.
    blocked: BTreeMap<TaskId, BlockedTask>,
    /// Why each task that ended for want of a runnable input version did.
    ended_reasons: BTreeMap<TaskId, String>,
    /// How long a task may wait blocked before it fails.
    version_block_ttl: Duration,
}

impl<C: Clock, I: IdGenerator> Scheduler<C, I, NoObserver> {
    /// A scheduler that refuses every claim and report until it is given a
    /// leadership grant ([`Self::set_leadership_grant`]). `clock` must be the
    /// clock its worker's election runs on, or a copy sharing its readings:
    /// a grant's lease ends at an instant of that clock.
    pub fn new(clock: C, ids: I) -> Self {
        Self::with_observer(clock, ids, NoObserver)
    }
}

impl<C: Clock, I: IdGenerator, O: Observer> Scheduler<C, I, O> {
    /// Like [`Self::new`], handing `observer` every revision published.
    pub fn with_observer(clock: C, ids: I, observer: O) -> Self {
        Scheduler {
            clock,
            ids,
            observer,
            leadership: Leadership::default(),
            tasks: BTreeMap::new(),
            runs: BTreeMap::new(),
            current_run: BTreeMap::new(),
            runs_of_task: BTreeMap::new(),
            waiting: WaitingRoom::default(),
            retention: Retention::default(),
            events: Vec::new(),
            held_cancels: Vec::new(),
            occupancy: Occupancy::default(),
            budget: MemoryBudget::default(),
            member_inputs: BTreeMap::new(),
            continuing: BTreeSet::new(),
            losses: BTreeMap::new(),
            unpublished: BTreeSet::new(),
            input_digests: BTreeMap::new(),
            links: BTreeMap::new(),
            reconciliation: Reconciliation::default(),
            run_decided_at: BTreeMap::new(),
            compaction_runners: BTreeSet::new(),
            silent: BTreeMap::new(),
            failed_compactions: BTreeMap::new(),
            conclusions: Vec::new(),
            queue_policy: QueuePolicy::default(),
            turns: Turns::default(),
            subscriptions: BTreeMap::new(),
            running: BTreeMap::new(),
            composites: Composites::default(),
            blocked: BTreeMap::new(),
            ended_reasons: BTreeMap::new(),
            version_block_ttl: DEFAULT_VERSION_BLOCK_TTL,
        }
    }

    /// What every call ends with: publishes the tasks it changed, then
    /// forgets what has outlived its TTL (so a task finished and forgotten
    /// in one call is still published first).
    fn end_call(&mut self) -> usize {
        self.publish_changed();
        self.forget_due()
    }

    /// Whether [`Self::take_events`] has anything to return.
    pub fn has_events(&self) -> bool {
        !self.events.is_empty()
    }

    /// What the scheduler decided on its own since the last call, in the
    /// order it happened.
    pub fn take_events(&mut self) -> Vec<Event> {
        let events = std::mem::take(&mut self.events);
        self.end_call();
        events
    }

    pub fn task_run(&self, run_id: &TaskRunId) -> Option<&TaskRun> {
        self.runs.get(run_id)
    }

    /// The Task `run_id` is a run of; `None` if the run is unknown or its
    /// task has been forgotten.
    pub fn task_of_run(&self, run_id: &TaskRunId) -> Option<&Task> {
        self.tasks.get(&self.runs.get(run_id)?.task_id())
    }

    /// Every run of `task_id`, oldest attempt first; empty if the task is
    /// unknown or has been forgotten.
    pub fn runs_of(&self, task_id: &TaskId) -> Vec<TaskRunId> {
        self.runs_of_task.get(task_id).cloned().unwrap_or_default()
    }

    fn coalescing_key_of(&self, task_id: &TaskId) -> Option<Key> {
        let task = self.tasks.get(task_id)?;
        Some(coalescing::key(
            &task.task_definition_id(),
            task.coalescing_key.as_deref()?,
        ))
    }

    /// Whether a task has to wait because a generation of its coalescing key
    /// is held back (see [`Reconciliation::holds_key_back`]): none of them may run while
    /// one might already be running, however late that was learnt.
    fn is_held_back(&self, task_id: &TaskId) -> bool {
        self.coalescing_key_of(task_id)
            .is_some_and(|key| self.reconciliation.holds_key_back(&key))
    }

    /// Whether a task has to wait because another generation of its
    /// coalescing key holds the key.
    fn is_blocked(&self, task_id: &TaskId) -> bool {
        self.coalescing_key_of(task_id)
            .is_some_and(|key| self.occupancy.is_blocked(&key, task_id))
    }

    /// Whether `task`'s newest record is not yet known to it: held back until
    /// a holder answers or leaves.
    pub fn is_uncertain(&self, task: &TaskId) -> bool {
        self.reconciliation.is_uncertain(task)
    }

    /// Whether it holds the record `key` names: it decides it, and a driver
    /// that kept where it wrote the record can forget it once this is false.
    pub fn holds(&self, key: &RecordKey) -> bool {
        match key {
            RecordKey::Task(task) => self.tasks.contains_key(task),
            RecordKey::Composite(id) => self.composites.held.contains_key(id),
            RecordKey::Page(id, page) => self
                .composites
                .held
                .get(id)
                .is_some_and(|composite| {
                    *page < composite.pages()
                        || composite.holds_item_page(*page)
                        || (self.composites.is_finished(id) && composite.is_item_page_of_items(*page))
                }),
        }
    }

    /// Whether repairing `key` needs an item page the leader released after
    /// its write was stored. The page remains part of the retained reduce.
    pub(crate) fn needs_item_page(&self, key: &RecordKey) -> bool {
        let RecordKey::Page(id, page) = key else {
            return false;
        };
        self.composites.held.get(id).is_some_and(|composite| {
            composite.is_item_page_of_items(*page)
                && !composite.holds_item_page(*page)
                && !self.composites.is_finished(id)
        })
    }

    pub(crate) fn retains_record(&self, key: &RecordKey) -> bool {
        self.holds(key) || self.needs_item_page(key)
    }

    /// How many tasks are queued to be claimed, including generations their
    /// coalescing key holds back. A delayed task counts once it is due.
    pub fn pending_len(&self) -> usize {
        self.waiting.queued_len()
    }

    /// Its observer, for a driver that takes what the scheduler published.
    pub fn observer_mut(&mut self) -> &mut O {
        &mut self.observer
    }

    /// The observer every revision is published to.
    pub fn observer(&self) -> &O {
        &self.observer
    }

    /// Records that `run_id` was decided now, and that its task has a change
    /// to publish.
    fn mark_decided(&mut self, run_id: &TaskRunId) {
        self.run_decided_at
            .insert(run_id.clone(), self.clock.now());
        self.unpublished.insert(self.runs[run_id].task_id());
        self.release_decided_member_input(run_id);
    }

    fn mark_current_decided(&mut self, task_id: &TaskId) {
        let run_id = self.current_run[task_id].clone();
        self.mark_decided(&run_id);
    }
}
