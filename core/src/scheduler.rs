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
//! Tasks and runs are stored privately and handed out only as shared
//! references or clones, so nothing outside can edit a submitted Task or move
//! a run without going through the transition table.

use std::collections::{BTreeMap, BTreeSet};

use prost::Message;

use crate::coalescing::{self, ChainItem, Folded, Key, Occupancy};
use crate::coordination_authority::RecoveryEpoch;
use crate::protocol::digest::Digest;
use crate::protocol::generated::{ChainEntry, CoalescingLink, TaskRecord, chain_entry};
use crate::protocol::ids::{
    IdGenerator, TaskId, TaskRunId, WorkerId,
};
use crate::protocol::messages::prelude::*;
use crate::protocol::generated;
use crate::protocol::messages::{Task, TaskRun, TaskRunIdentity};
use crate::protocol::records::{TaskRunRecord, retry_of};
use crate::protocol::task::TaskRunState;
use crate::reconcile::{
    ANSWER_IN_FLIGHT, RECONCILE_SKEW_MARGIN, Rebuild, ReconcileTerm, ReportedRun, ReportedState,
    WorkerRuns,
};
use crate::task_record::{HISTORY_TOO_LARGE_FAILURE_KIND, RecordVersion};
use crate::time::{Clock, Duration, Instant, WallTime};

mod backlog;
mod compaction;
mod leadership;
mod memory_budget;
mod observer;
mod publication;
mod reconciliation;
mod record_size;
mod retention;
mod submission;
mod waiting_room;

pub use backlog::Backlog;
pub use compaction::Compacted;
use leadership::Leadership;
pub use leadership::{LeadershipGrant, LeaseEnd};
use memory_budget::MemoryBudget;
pub use observer::{NoObserver, Observer};
use reconciliation::Reconciliation;
use retention::Retention;
pub use submission::{Submission, SubmitRejection, Submitted, mint};
use waiting_room::WaitingRoom;

/// When the scheduler asks for less work, and when it refuses it, counted in
/// the serialized bytes of every task that has not finished.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryLimits {
    /// Past this, `SlowDown` is raised: whoever submits in bulk should pause.
    pub soft: u64,
    /// A submission that would take usage past this is refused.
    pub hard: u64,
}

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

/// The longest failure kind a record keeps, in bytes. A kind is an error
/// type's name, so any real one fits; a longer one is cut rather than refused
/// (the failure is reported either way), so that what a worker supplies can
/// never push a record past the limit the network refuses to store.
const MAX_FAILURE_KIND_BYTES: usize = 128;

/// What a claim spends on each payload of a chain besides its bytes: the
/// field's tag and its length.
const CHAIN_ENTRY_FRAMING_BYTES: u64 = 6;

/// The failure kind of a coalescing generation whose folded chain grew too
/// large to hand to any worker.
pub const COALESCED_PAYLOAD_TOO_LARGE: &str = "CoalescedPayloadTooLarge";

/// A version for measuring a record's size, which does not depend on it.
const UNVERSIONED: RecordVersion = RecordVersion {
    recovery_epoch: RecoveryEpoch::new(0, 0),
    leader_term: 0,
    revision: 0,
};

/// A worker's permission to run a task: the Task itself and the run it now
/// owns.
#[derive(Debug, Clone, PartialEq)]
pub struct Claim {
    pub task: Task,
    pub task_run_id: TaskRunId,
    /// Which attempt this run is: 1 for the first, then one more per retry.
    pub attempt_number: u32,
    /// The run's reconnect timeout: its task's own, or else the shard's
    /// default from this leader's grant.
    pub reconnect_timeout: Duration,
    /// The serialized inputs of the generations this one superseded, oldest
    /// first, for the worker to fold before running the task.
    /// Empty unless the task is a coalescing one that absorbed others.
    pub chain: Vec<Vec<u8>>,
}

/// The leader's word that a run's result is the authoritative one. Until a
/// client holds this, result bytes it received are only provisional.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Certification {
    pub task_id: TaskId,
    pub task_run_id: TaskRunId,
    pub result_digest: Digest,
}

/// The leader's record that a run failed, so its client can be told.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    pub task_id: TaskId,
    pub task_run_id: TaskRunId,
    /// The run that replaces the failed one, queued, if the task has retries
    /// left. `None` means the task has failed for good.
    pub retry: Option<TaskRunId>,
}

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
}

/// A run stored as cancelled that its worker reported it still holds: the
/// worker has to be told to stop the body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeldCancel {
    pub task_id: TaskId,
    pub task_run_id: TaskRunId,
    pub worker: WorkerId,
}

/// A run that was lost with its worker, and the run that replaces it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LostRun {
    pub task_id: TaskId,
    pub task_run_id: TaskRunId,
    /// What the run became: `Lost`, or `Orphaned` for a running run of a
    /// non-retriable task.
    pub state: TaskRunState,
    /// The queued next attempt, or `None` when the run is not replayed: an
    /// ephemeral task's, an orphaned one, and a coalescing generation that a
    /// newer one had superseded in the meantime.
    pub replayed: Option<TaskRunId>,
}

/// What cancelling a task did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cancellation {
    /// The task's current run is now `Cancelled`.
    Cancelled {
        was_running: bool,
    },
    /// The task had already finished, so there was nothing to cancel.
    AlreadyFinished,
    UnknownTask,
}

/// Why a cancellation was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CancelRejection {
    #[error("this node is not the leader")]
    NotLeader,
    /// The task's newest record cannot be known yet: ask again.
    #[error("the task's newest record is not known yet")]
    NotReady,
}

/// Why `Scheduler::reconcile` refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ReconcileRejection {
    #[error("this scheduler is not reconciling")]
    NotReconciling,
    #[error("this scheduler has already rebuilt for its office")]
    AlreadyRebuilt,
}

/// A rebuild the scheduler did not take, handed back with the reason.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[error("{rejection}")]
pub struct ReconcileRefused {
    pub rejection: ReconcileRejection,
    pub rebuild: Rebuild,
}

/// What adopting late knowledge did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Adopted {
    /// Tasks whose records were installed.
    pub installed: usize,
    pub lost: Vec<LostRun>,
    pub certified: Vec<Certification>,
    pub failed: Vec<Failure>,
    /// Workers holding a claimed or running run among the installed records
    /// that have not answered: the election must watch them as lost workers.
    pub silent_holders: BTreeSet<WorkerId>,
    /// Workers that answered: they have been heard, so the silence their
    /// node reported for any of them is over (its node must count them heard
    /// now, see `Input::WatchWorkers`).
    pub answered: BTreeSet<WorkerId>,
}

/// What a rebuild did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reconciled {
    /// Records written again at the new term.
    pub republished: usize,
    /// Tasks held back until their newest record is known.
    pub uncertain: usize,
    /// Workers holding a claimed or running run among the installed records
    /// that did not answer. They may be dead with the old leader and may
    /// never be heard from, so the election must watch them as lost workers.
    pub silent_holders: BTreeSet<WorkerId>,
    /// Workers that answered: they have been heard, so the silence their
    /// node reported for any of them is over (its node must count them heard
    /// now, see `Input::WatchWorkers`).
    pub answered: BTreeSet<WorkerId>,
}

/// What one call to [`Scheduler::catch_up`] did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CaughtUp {
    /// Delayed tasks that became due and are now pending (only while leading).
    pub queued: usize,
    /// Pending tasks that expired (only while leading).
    pub expired: usize,
    /// Finished tasks forgotten because their result TTL had passed.
    pub forgotten: usize,
}

/// Whether a certified run ends its task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Completion {
    /// The task is over: its coalescing key is freed, its payload stops
    /// counting, and it is forgotten `result_ttl` later.
    Final,
    /// The result is a continuation (an implicit flow): the run
    /// is certified, but the task holds its key and its memory until
    /// [`Scheduler::end_continuation`].
    Continues,
}

/// Why a claim request was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ClaimRejection {
    #[error("this node is not the leader")]
    NotLeader,
    #[error("no such task")]
    TaskUnknown,
    #[error("the task is not due yet")]
    NotReady,
    #[error("the task's run was already claimed")]
    AlreadySelected,
    #[error("the task's run has finished")]
    Finished,
    #[error("a newer generation of the task's coalescing key replaced it")]
    Superseded,
    #[error("another generation of the task's coalescing key is running")]
    KeyBusy,
    /// The task is a compaction run and the asking worker has not said it
    /// runs them.
    #[error("the worker does not run compaction")]
    CannotRun,
}

/// Why a worker's report about a run was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ReportRejection {
    #[error("this node is not the leader")]
    NotLeader,
    #[error("no such run")]
    UnknownRun,
    #[error("the run does not belong to this worker or is not in the expected state")]
    NotAuthoritative,
    /// The run's task has a newest record this leader cannot know yet: ask again.
    #[error("the run's task is not known for certain yet")]
    NotReady,
}

/// Why ending a continuation was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ContinuationRejection {
    #[error("this node is not the leader")]
    NotLeader,
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
    /// Tasks whose run is certified but whose continuation has not ended.
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
}

/// What `Scheduler::settle` made of the records it was given.
struct Settled {
    install: Vec<TaskRecord>,
    held_back: Vec<TaskRecord>,
    /// Supersessions to finish once the records are installed: the older
    /// generation is already installed, pending, and its record does not name
    /// the newer one (older, newer).
    finish_live: Vec<(TaskId, TaskId)>,
}

/// Whether a generation that absorbed another agrees with that one's record.
enum Supersession {
    /// The older record names the newer one, or the generation absorbed
    /// nothing.
    Holds,
    /// The newer generation's first revision was stored but the older one's
    /// superseded revision was not, and the older one is still pending (the
    /// index of its record).
    Unfinished(usize),
    /// The same, but the older generation is already installed and pending.
    UnfinishedInstalled(TaskId),
    /// The older record is not known, or cannot be reconciled with the newer
    /// one: the key is not known yet.
    Broken,
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
        }
    }

    /// What every call ends with: publishes the tasks it changed, then
    /// forgets what has outlived its TTL (so a task finished and forgotten
    /// in one call is still published first).
    fn end_call(&mut self) -> usize {
        self.publish_changed();
        self.forget_due()
    }

    /// Sets the memory limits, or removes them with `None`. Set them before
    /// anything is recorded: they are not checked against what is already in
    /// use, and no `SlowDown` event is raised for them.
    ///
    /// # Panics
    ///
    /// If the soft limit is above the hard one.
    pub fn set_memory_limits(&mut self, limits: Option<MemoryLimits>) {
        self.budget.set_limits(limits);
    }

    /// Its node took office for `term` and reconciles before it leads.
    /// Until the grant of that office arrives, the silences its node reports
    /// are kept, not acted on, and the scheduler,
    /// holding no grant, refuses every claim and report as `NotLeader`. A
    /// grant of `None` meanwhile (the node left office) drops the
    /// reconciliation. It forgets every silence its node reported in an
    /// earlier office: a worker silent then may have been heard by another
    /// leader since, and the new office counts every worker afresh.
    pub fn begin_reconcile(&mut self, term: ReconcileTerm) {
        self.silent.clear();
        self.leadership.hold(None);
        self.check_leader();
        self.reconciliation.begin(term);
    }

    /// The office it reconciles for, until that office's grant arrives.
    pub fn reconciling(&self) -> Option<ReconcileTerm> {
        self.reconciliation.office()
    }

    /// Replaces everything this scheduler holds with what `rebuild` says,
    /// never merging with what it held before: every certain record is
    /// installed as its newest revision says (runs, waiting room with
    /// deadlines from its wall-clock times, retention, coalescing occupancy
    /// and chains, memory use, losses), every uncertain task is held back,
    /// and then every installed record is republished at the office's term,
    /// which fences any late write of an earlier leader.
    ///
    /// When the office is no longer the one the scheduler may publish under
    /// (a stale office, whose grant was superseded), the rebuilt state is
    /// still installed but nothing is republished and `republished` is 0:
    /// there is no live term left to fence a late write at.
    pub fn reconcile(&mut self, rebuild: Rebuild) -> Result<Reconciled, ReconcileRefused> {
        if let Some(rejection) = self.reconciliation.refusal() {
            return Err(ReconcileRefused { rejection, rebuild });
        }
        let Rebuild {
            records,
            uncertain,
            uncertain_keys,
            reports,
        } = rebuild;
        self.clear_tasks();
        let office = self.reconciliation.rebuilt(
            reports.keys().cloned().collect(),
            uncertain,
            uncertain_keys,
        );
        self.install_settled(records, false);
        // What applying the reports did (runs lost, certified or failed) is
        // not reported back: the caller learns of it from the revisions the
        // scheduler publishes and from the events it queues.
        let mut applied = Adopted::default();
        self.apply_reports(reports, &mut applied);
        let republished = if self
            .leadership
            .may_publish_under(office.recovery_epoch, office.term)
        {
            self.publish_unpublished(office.recovery_epoch, office.term)
        } else {
            0
        };
        let silent_holders = self
            .holders_of(self.current_run.keys())
            .difference(self.reconciliation.answered())
            .cloned()
            .collect();
        let answered = self.reconciliation.answered().clone();
        for worker in &answered {
            self.silent.remove(worker);
        }
        Ok(Reconciled {
            republished,
            uncertain: self.reconciliation.uncertain_count(),
            silent_holders,
            answered,
        })
    }

    /// While leading: takes what reconciliation learnt after the rebuild
    /// (records that became certain, answers that came late, a worker's
    /// re-report) through the same table as the rebuild. A record is installed
    /// only for a task this leader does not hold; what it already holds it
    /// decided itself. Changes publish as any call's do.
    ///
    /// A scheduler that does not lead (its grant has not arrived, or ended:
    /// the lease may run out, or the recovery fence lapse, while the node
    /// still holds office) takes nothing and hands `learnt` back. The round
    /// has already given that knowledge up, so the caller gives it back to the
    /// round, which offers it again once the scheduler leads.
    pub fn adopt(&mut self, learnt: Rebuild) -> Result<Adopted, Rebuild> {
        if !self.check_leader() {
            return Err(learnt);
        }
        let Rebuild {
            records,
            uncertain,
            uncertain_keys,
            reports,
        } = learnt;
        let tasks = &self.tasks;
        let candidates = self
            .reconciliation
            .learn(records, uncertain, uncertain_keys, |task| {
                tasks.contains_key(task)
            });
        let mut adopted = Adopted::default();
        let installed = self.install_settled(candidates, true);
        adopted.installed = installed.len();
        let reporters: BTreeSet<WorkerId> = reports.keys().cloned().collect();
        for worker in &reporters {
            self.silent.remove(worker);
        }
        let mut answered = reporters.clone();
        answered.extend(self.reconciliation.answered().iter().cloned());
        for task_id in &installed {
            for (worker, run) in self.reconciliation.take_reports_for(task_id) {
                answered.insert(worker.clone());
                self.apply_reported(&worker, run, &mut adopted);
            }
        }
        self.apply_reports(reports, &mut adopted);
        adopted.silent_holders = self
            .holders_of(installed.iter())
            .difference(&answered)
            .cloned()
            .collect();
        adopted.answered = reporters;
        self.end_call();
        Ok(adopted)
    }

    /// The runs this leader believes `worker` holds: claimed or running, and
    /// selected for it, and those of tasks held back as uncertain that it
    /// reported as claimed or running (a heartbeat digest covers no others).
    /// What `worker`'s heartbeat digest is compared with.
    pub fn active_runs_of(&self, worker: &WorkerId) -> Vec<TaskRunId> {
        self.held_by(worker)
            .iter()
            .map(|task_id| self.current_run[task_id].clone())
            .chain(self.reconciliation.active_runs_reported_by(worker))
            .collect()
    }

    /// Installs the records of `records` that may be installed (see
    /// [`Self::settle`]) and holds the others back, with their tasks marked
    /// uncertain. Returns the tasks installed. `late` says this is not the
    /// rebuild: only the coalescing keys of what is installed are restored.
    fn install_settled(&mut self, records: Vec<TaskRecord>, late: bool) -> Vec<TaskId> {
        let Settled {
            mut install,
            held_back,
            finish_live,
        } = self.settle(records);
        let now = self.clock.now();
        let wall_now = WallTime::now(&self.clock);
        install.sort_by_cached_key(submission_order);
        let mut kept_chains = BTreeMap::new();
        let mut installed = Vec::new();
        for record in install {
            if let Some(task) = record.task.as_ref() {
                kept_chains.insert(task.task_id(), chain_of(&record));
                installed.push(task.task_id());
            }
            self.install(record, now, wall_now);
        }
        self.reconciliation.installed(&installed);
        for record in held_back {
            self.reconciliation.hold_back(record);
        }
        for (older, newer) in finish_live {
            self.supersede(&older, &newer);
        }
        let touched = late.then(|| {
            installed
                .iter()
                .filter_map(|task_id| self.coalescing_key_of(task_id))
                .collect()
        });
        self.restore_keys(&kept_chains, touched.as_ref());
        self.restore_compactions();
        self.update_pressure();
        installed
    }

    /// Decides which of `records`, each the newest known of its task, may be
    /// installed. A coalescing generation that absorbed another is live only
    /// if the absorbed one's record is known and agrees (see
    /// [`Supersession`]); a key where that fails is held back whole, since
    /// the generations of a key decide each other's occupancy. A pending
    /// generation that a newer one absorbed, whose own superseded revision
    /// was never written, is finished here: the older record becomes
    /// superseded, naming its successor, and is republished like any other.
    fn settle(&self, mut records: Vec<TaskRecord>) -> Settled {
        let index: BTreeMap<TaskId, usize> = records
            .iter()
            .enumerate()
            .filter_map(|(at, record)| Some((record.task.as_ref()?.task_id(), at)))
            .collect();
        let key_of = |record: &TaskRecord| {
            let task = record.task.as_ref()?;
            Some(coalescing::key(
                &task.task_definition_id(),
                task.coalescing_key.as_deref()?,
            ))
        };
        // A key with a generation whose record is unknown is held whole, like
        // one found broken: the generations decide each other's fate, and one
        // not yet known may have absorbed a generation known already.
        let mut broken: BTreeSet<Key> = self.reconciliation.unknown_keys();
        let mut unfinished: Vec<(usize, TaskId)> = Vec::new();
        let mut finish_live: Vec<(TaskId, TaskId)> = Vec::new();
        for record in &records {
            let (Some(key), Some(newer)) = (key_of(record), record.task.as_ref()) else {
                continue;
            };
            match self.supersession_of(record, &records, &index) {
                Supersession::Holds => {}
                Supersession::Unfinished(older) => unfinished.push((older, newer.task_id())),
                Supersession::UnfinishedInstalled(older) => {
                    finish_live.push((older, newer.task_id()));
                }
                Supersession::Broken => {
                    broken.insert(key);
                }
            }
        }
        let stamped_at = WallTime::now(&self.clock);
        for (older, newer) in unfinished {
            if key_of(&records[older]).is_some_and(|key| broken.contains(&key)) {
                continue;
            }
            let record = &mut records[older];
            if let Some(run) = record.runs.last_mut() {
                run.transition_to(TaskRunState::Superseded, stamped_at)
                    .expect("a pending run can always be superseded");
            }
            record.link.get_or_insert_default().superseded_by = Some(newer.into());
        }
        finish_live.retain(|(_, newer)| {
            index
                .get(newer)
                .and_then(|&at| key_of(&records[at]))
                .is_some_and(|key| !broken.contains(&key))
        });
        let (held_back, install) = records
            .into_iter()
            .partition(|record| key_of(record).is_some_and(|key| broken.contains(&key)));
        Settled {
            install,
            held_back,
            finish_live,
        }
    }

    /// Whether `newer`, a generation that absorbed others, agrees with the
    /// record of the generation it replaced.
    fn supersession_of(
        &self,
        newer: &TaskRecord,
        records: &[TaskRecord],
        index: &BTreeMap<TaskId, usize>,
    ) -> Supersession {
        let Some(link) = newer.link.as_ref().filter(|link| !link.absorbed.is_empty()) else {
            return Supersession::Holds;
        };
        if newer.finished {
            return Supersession::Holds;
        }
        let Some(newer_id) = newer.task.as_ref().map(Task::task_id) else {
            return Supersession::Holds;
        };
        let predecessor = TaskId::from(link.absorbed.last().expect("not empty").clone());
        // The generations a compaction folded were released with it, and their
        // records may be forgotten by now.
        let folded = folded_ids(newer);
        // A chain entry the leader would fold must have its record: the
        // chain's size, release and fold look it up as a held task.
        let known = |task_id: &TaskId| {
            index.contains_key(task_id) || self.tasks.contains_key(task_id) || folded.contains(task_id)
        };
        if !retained_ids(newer)
            .iter()
            .chain([&predecessor])
            .all(known)
        {
            return Supersession::Broken;
        }
        let names_newer = |link: Option<&CoalescingLink>| {
            link.and_then(|link| link.superseded_by.clone()).map(TaskId::from)
                == Some(newer_id.clone())
        };
        let Some(&at) = index.get(&predecessor) else {
            if names_newer(self.links.get(&predecessor)) {
                return Supersession::Holds;
            }
            if folded.contains(&predecessor) && !self.tasks.contains_key(&predecessor) {
                return Supersession::Holds;
            }
            // The predecessor was installed before this generation's record
            // was known: if nothing claimed it, the supersession is finished
            // live; a claimed one cannot have been absorbed.
            let unclaimed = self.current_run.get(&predecessor).is_some_and(|run_id| {
                matches!(
                    self.runs[run_id].current_state(),
                    TaskRunState::Scheduled | TaskRunState::Queued
                ) && self.runs[run_id].selected_worker.is_none()
            });
            let named_other = self
                .links
                .get(&predecessor)
                .is_some_and(|link| link.superseded_by.is_some());
            if unclaimed && !named_other && !self.retention.holds(&predecessor) {
                return Supersession::UnfinishedInstalled(predecessor);
            }
            tracing::error!(
                newer = newer_id.as_str(),
                older = predecessor.as_str(),
                "a generation absorbed an installed one that is claimed or finished"
            );
            return Supersession::Broken;
        };
        let older = &records[at];
        if names_newer(older.link.as_ref()) {
            return Supersession::Holds;
        }
        let pending = older.runs.last().is_some_and(|run| {
            matches!(
                run.current_state(),
                TaskRunState::Scheduled | TaskRunState::Queued
            ) && run.selected_worker.is_none()
        });
        let named_other = older
            .link
            .as_ref()
            .is_some_and(|link| link.superseded_by.is_some());
        if pending && !named_other && !older.finished {
            return Supersession::Unfinished(at);
        }
        // A supersession only ever absorbs a generation that had not been
        // claimed, and writes its revision 0 before touching the older one.
        tracing::error!(
            newer = newer_id.as_str(),
            older = predecessor.as_str(),
            "a generation absorbed one whose record shows it claimed or finished"
        );
        Supersession::Broken
    }

    /// Applies what the workers that answered reported, run by run: a run
    /// its worker reports is adopted at the state it reports (a success is
    /// certified, a failure applied), one the leader holds for a worker that
    /// answered without it is lost, and one whose task has no record is
    /// rebuilt from its claim. A run decided shortly before the worker was
    /// asked may not have reached it, so it is not lost for being missing.
    /// What a worker's earlier answer left held for tasks still uncertain is
    /// dropped when its newer answer leaves those runs out.
    fn apply_reports(&mut self, reports: BTreeMap<WorkerId, WorkerRuns>, out: &mut Adopted) {
        for (worker, answer) in reports {
            let now = self.clock.now();
            let stamped_at = WallTime::now(&self.clock);
            let reported: BTreeSet<TaskRunId> = answer
                .runs
                .iter()
                .map(|run| run.claim.task_run_id.clone())
                .collect();
            self.reconciliation.drop_unreported(&worker, &reported);
            for task_id in self.held_by(&worker) {
                let run_id = &self.current_run[&task_id];
                let in_flight = answer.asked_at.is_some_and(|asked_at| {
                    self.run_decided_at
                        .get(run_id)
                        .is_some_and(|decided| *decided + ANSWER_IN_FLIGHT > asked_at)
                });
                if !reported.contains(run_id) && !in_flight {
                    out.lost.push(self.lose_run(&task_id, now, stamped_at));
                }
            }
            for run in answer.runs {
                self.apply_reported(&worker, run, out);
            }
        }
    }

    /// Applies one run `worker` reported (see [`Self::apply_reports`]).
    fn apply_reported(&mut self, worker: &WorkerId, reported: ReportedRun, out: &mut Adopted) {
        let Some(reported) = self.reconciliation.hold_report(worker, reported) else {
            return;
        };
        let task_id = reported.claim.task.task_id();
        let run_id = reported.claim.task_run_id.clone();
        if !self.tasks.contains_key(&task_id) && !self.rebuild_from_claim(worker, &reported.claim) {
            return;
        }
        if self.current_run.get(&task_id) != Some(&run_id) {
            return;
        }
        let recorded = self.runs[&run_id].current_state();
        if self.runs[&run_id].selected_worker().as_ref() != Some(worker) {
            return;
        }
        if recorded == TaskRunState::Cancelled
            && matches!(reported.state, ReportedState::Claimed | ReportedState::Running)
        {
            self.held_cancels.push(HeldCancel {
                task_id,
                task_run_id: run_id,
                worker: worker.clone(),
            });
            return;
        }
        if !matches!(recorded, TaskRunState::Claimed | TaskRunState::Running) {
            return;
        }
        let beyond_claimed = !matches!(reported.state, ReportedState::Claimed);
        if recorded == TaskRunState::Claimed && beyond_claimed {
            let _ = self.start_owned(worker, &run_id);
        }
        match reported.state {
            ReportedState::Claimed | ReportedState::Running => {}
            ReportedState::Succeeded { result_digest } => {
                if let Ok(certified) =
                    self.certify_owned(worker, &run_id, result_digest, Completion::Final)
                {
                    out.certified.push(certified);
                }
            }
            ReportedState::Failed { failure_kind } => {
                if let Ok(failed) = self.fail_owned(worker, &run_id, failure_kind) {
                    out.failed.push(failed);
                }
            }
        }
    }

    /// Holds the task `claim` was granted for, which no record is known of, as
    /// its worker's claimed run: the key it holds is occupied with an empty
    /// chain (the worker folded the chain it was given). Says whether it did;
    /// it does not when another generation already holds the key.
    fn rebuild_from_claim(&mut self, worker: &WorkerId, claim: &Claim) -> bool {
        let task = claim.task.clone();
        let task_id = task.task_id();
        let key = task
            .coalescing_key
            .as_deref()
            .map(|key| coalescing::key(&task.task_definition_id(), key));
        if key
            .as_ref()
            .is_some_and(|key| self.occupancy.is_blocked(key, &task_id))
        {
            return false;
        }
        let stamped_at = WallTime::now(&self.clock);
        let mut run = TaskRun {
            identity: Some(TaskRunIdentity {
                task_run_id: Some(claim.task_run_id.clone().into()),
                task_id: Some(task_id.clone().into()),
                attempt_number: claim.attempt_number,
                parent_task_run_id: None,
            }),
            source_version: task.source_version,
            execution_version: task.source_version,
            created_at: Some(stamped_at.into()),
            state: generated::TaskRunState::Queued as i32,
            updated_at: Some(stamped_at.into()),
            selected_worker: None,
            result_digest: None,
            failure_kind: String::new(),
        };
        run.transition_to(TaskRunState::Claimed, stamped_at)
            .expect("a queued run can be claimed");
        run.selected_worker = Some(worker.clone().into());
        let needed = task.serialized_input.len() as u64;
        let compaction_key = task.compacts.as_ref().map(|prefix| {
            coalescing::key(&task.task_definition_id(), &prefix.coalescing_key)
        });
        if compaction_key.as_ref().is_some_and(|key| {
            self.occupancy
                .compaction_of(key)
                .is_some_and(|other| *other != task_id)
        }) {
            return false;
        }
        self.input_digests
            .insert(task_id.clone(), Digest::blake3(&task.serialized_input));
        self.current_run
            .insert(task_id.clone(), claim.task_run_id.clone());
        self.runs_of_task
            .insert(task_id.clone(), vec![claim.task_run_id.clone()]);
        self.runs.insert(claim.task_run_id.clone(), run);
        self.tasks.insert(task_id.clone(), task);
        self.mark_current_decided(&task_id);
        self.budget.take(needed);
        if let Some(key) = key {
            self.occupancy.start(&key, &task_id);
        }
        if let Some(key) = compaction_key {
            self.occupancy.restore_compaction(&key, &task_id, true);
        }
        self.update_pressure();
        true
    }

    /// Forgets every task, run and what hangs on them, keeping the result
    /// TTL and the memory limits.
    fn clear_tasks(&mut self) {
        self.tasks.clear();
        self.runs.clear();
        self.current_run.clear();
        self.runs_of_task.clear();
        self.waiting.clear();
        self.retention.clear();
        self.occupancy.clear();
        self.budget.reset_usage();
        self.continuing.clear();
        self.losses.clear();
        self.input_digests.clear();
        self.links.clear();
        self.unpublished.clear();
        self.events.clear();
        self.held_cancels.clear();
        self.run_decided_at.clear();
        self.failed_compactions.clear();
    }

    /// Holds the task `record` describes, as its newest revision says.
    fn install(&mut self, record: TaskRecord, now: Instant, wall_now: WallTime) {
        let TaskRecord {
            task,
            runs,
            input_digest,
            link,
            finished,
            retained_chain,
            ..
        } = record;
        let (Some(task), Some(current)) = (task, runs.last().map(TaskRunRecord::task_run_id))
        else {
            return;
        };
        let task_id = task.task_id();
        let state = runs[runs.len() - 1].current_state();
        // Only a task's first attempt waits out a delay or an expiry; a
        // later one follows a run that was claimed.
        let first_attempt = runs.len() == 1;
        let submitted_at = task.submitted_at.map_or(wall_now, WallTime::from);
        let deadline = |after_submission: u64| {
            submitted_at.deadline(
                Duration::from_millis(
                    after_submission.saturating_add(RECONCILE_SKEW_MARGIN.as_ticks()),
                ),
                now,
                wall_now,
            )
        };
        if matches!(state, TaskRunState::Scheduled | TaskRunState::Queued) {
            let not_before = (state == TaskRunState::Scheduled && first_attempt)
                .then_some(task.delay_millis)
                .flatten()
                .map(deadline);
            let expires_at = first_attempt
                .then_some(task.expiry_millis)
                .flatten()
                .map(deadline);
            self.waiting.admit(&task_id, not_before, expires_at);
        }
        if finished {
            self.retention.record(&task_id, now);
        } else {
            self.budget.take(task.serialized_input.len() as u64);
            self.budget.take(
                chain_items(&retained_chain)
                    .iter()
                    .map(|item| match item {
                        ChainItem::Folded(folded) => folded.payload.len() as u64,
                        ChainItem::Absorbed(_) => 0,
                    })
                    .sum(),
            );
            if state == TaskRunState::Succeeded {
                self.continuing.insert(task_id.clone());
            }
        }
        let lost = runs
            .iter()
            .filter(|run| run.current_state() == TaskRunState::Lost)
            .count();
        if lost > 0 {
            self.losses.insert(task_id.clone(), lost as u32);
        }
        let digest = input_digest
            .as_ref()
            .and_then(|digest| Digest::try_from(digest).ok())
            .unwrap_or_else(|| Digest::blake3(&task.serialized_input));
        self.input_digests.insert(task_id.clone(), digest);
        if let Some(link) = link {
            self.links.insert(task_id.clone(), link);
        }
        self.runs_of_task.insert(
            task_id.clone(),
            runs.iter().map(TaskRunRecord::task_run_id).collect(),
        );
        for run in runs {
            self.runs.insert(run.task_run_id(), run);
        }
        self.current_run.insert(task_id.clone(), current);
        self.tasks.insert(task_id.clone(), task);
        self.mark_current_decided(&task_id);
        // Installing a record decides nothing: this leader did not make it.
        for run_id in self.runs_of_task[&task_id].clone() {
            self.run_decided_at.remove(&run_id);
        }
    }

    /// Sets what each coalescing key holds from the tasks installed:
    /// `kept_chains` is the chain each one's record still carries. A
    /// generation held before and not among them keeps the chain it had.
    ///
    /// An absorbed generation whose own record is not installed is left out
    /// of the chain, even when the winner's record still holds its payload:
    /// the chain's size, release and fold all look the generation up as an
    /// installed task. This loses nothing for a certain key, because the
    /// classification of the rebuilt records makes a key uncertain
    /// when an absorbed generation's record is unknown, so such a key is
    /// held back and never reaches this function.
    fn restore_keys(
        &mut self,
        kept_chains: &BTreeMap<TaskId, Vec<ChainItem>>,
        only: Option<&BTreeSet<Key>>,
    ) {
        let mut generations: BTreeMap<Key, Vec<TaskId>> = BTreeMap::new();
        for task_id in self.tasks.keys() {
            if let Some(key) = self.coalescing_key_of(task_id) {
                generations.entry(key).or_default().push(task_id.clone());
            }
        }
        for (key, mut tasks) in generations {
            if only.is_some_and(|only| !only.contains(&key)) {
                continue;
            }
            tasks.retain(|task_id| !self.retention.holds(task_id));
            tasks.sort_by_cached_key(|task_id| {
                let task = &self.tasks[task_id];
                (
                    task.submitted_at.as_ref().map(|at| at.unix_millis),
                    task_id.clone(),
                )
            });
            let claimed = |scheduler: &Self, task_id: &TaskId| {
                scheduler.runs_of_task[task_id]
                    .iter()
                    .any(|run| scheduler.runs[run].selected_worker.is_some())
            };
            let waiting = tasks
                .iter()
                .rev()
                .find(|task_id| {
                    !claimed(self, task_id)
                        && matches!(
                            self.runs[&self.current_run[*task_id]].current_state(),
                            TaskRunState::Scheduled | TaskRunState::Queued
                        )
                })
                .cloned();
            let holder = tasks
                .iter()
                .rev()
                .find(|task_id| claimed(self, task_id))
                .cloned();
            let chains = [waiting.as_ref(), holder.as_ref()]
                .into_iter()
                .flatten()
                .map(|task_id| {
                    // An absorbed generation whose record is not installed
                    // cannot be folded either, so it is not chained. A task
                    // not installed in this batch was held before: what its
                    // chain kept stays kept.
                    let chain = match kept_chains.get(task_id) {
                        Some(chain) => chain.clone(),
                        None => self.occupancy.chain(task_id).to_vec(),
                    };
                    let chain = chain
                        .into_iter()
                        .filter(|item| match item {
                            ChainItem::Absorbed(absorbed) => self.tasks.contains_key(absorbed),
                            ChainItem::Folded(_) => true,
                        })
                        .collect();
                    (task_id.clone(), chain)
                })
                .collect();
            self.occupancy.restore(&key, waiting, holder, chains);
        }
    }

    /// Lets time take effect, and is the only call a runtime makes for time's
    /// sake: it forgets every task that finished at least `result_ttl` ago,
    /// and, only while this scheduler leads, makes due delayed tasks pending
    /// and expires pending tasks past their expiry (reported through
    /// [`Self::take_events`]), and loses the runs of silent workers whose
    /// reconnect timeouts have passed (see [`Self::note_silence`]). Call it
    /// when [`Self::next_deadline`] comes.
    pub fn catch_up(&mut self) -> CaughtUp {
        let mut caught_up = if self.check_leader() {
            let released = self.release_due();
            self.lose_overdue();
            released
        } else {
            CaughtUp::default()
        };
        caught_up.forgotten = self.end_call();
        caught_up
    }

    /// Delayed tasks that are due become pending, and pending tasks past
    /// their expiry expire. Only a leader decides this, so its callers check.
    /// Claims do this themselves first, so a task that ran out of time is
    /// never handed out.
    fn release_due(&mut self) -> CaughtUp {
        let mut advanced = CaughtUp::default();
        let now = self.clock.now();
        let stamped_at = WallTime::now(&self.clock);
        for task_id in self.waiting.take_expired(now) {
            self.expire(&task_id, now);
            advanced.expired += 1;
        }
        for task_id in self.waiting.release_due(now) {
            let run_id = self.current_run[&task_id].clone();
            self.runs
                .get_mut(&run_id)
                .expect("every current run is stored")
                .transition_to(TaskRunState::Queued, stamped_at)
                .expect("a Scheduled run can always be queued");
            self.mark_decided(&run_id);
            advanced.queued += 1;
        }
        advanced
    }

    /// When [`Self::catch_up`] next has something to do, or `None`: the next
    /// forgetting time always, and the next delay or expiry only while this
    /// scheduler leads. Also the end of a bounded lease while this scheduler
    /// has found itself leading: that end stays in the answer until a call
    /// finds it passed, since finding the lapse is then `catch_up`'s to do.
    /// An unbounded lease adds nothing. Also the next replay of a silent
    /// worker's run, only while this scheduler leads.
    pub fn next_deadline(&self) -> Option<Instant> {
        let forgetting = self.retention.next_due();
        let lease_end = self.leadership.noticed_lease_end();
        let waiting = if self.is_leader() {
            self.waiting.next_deadline()
        } else {
            None
        };
        let replays = if self.is_leader() {
            self.replays_due().into_iter().map(|(at, _)| at).min()
        } else {
            None
        };
        [waiting, forgetting, lease_end, replays].into_iter().flatten().min()
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

    /// The runs stored as cancelled that their workers reported they still
    /// hold, each time one reported it: whichever leader stored the cancel,
    /// the worker must still be told to stop the body. Nothing while the
    /// scheduler does not lead: what a rebuild installed is told of only once
    /// it is stored again at this leader's term. Kept until then.
    pub fn take_held_cancels(&mut self) -> Vec<HeldCancel> {
        if !self.is_leader() {
            return Vec::new();
        }
        std::mem::take(&mut self.held_cancels)
    }

    /// A worker asks for up to `limit` of the oldest pending tasks at once.
    /// Fewer, or none, are returned if fewer are pending.
    pub fn claim_oldest(
        &mut self,
        worker: &WorkerId,
        limit: usize,
    ) -> Result<Vec<Claim>, ClaimRejection> {
        self.claim_oldest_fitting(worker, limit, |_| true)
    }

    /// Like [`Self::claim_oldest`], but each candidate, oldest first, is
    /// shown to `fits` before it is claimed. The first one `fits` refuses
    /// ends the batch and stays pending, first in line for the next call;
    /// except that one refused while nothing is claimed yet, which would
    /// never fit, is passed over so the tasks behind it still go out. No
    /// claim outgrows a frame, so that last rule only guards a caller whose
    /// limit is smaller. It is how a caller that must deliver the claims
    /// keeps them within what it can deliver.
    pub fn claim_oldest_fitting(
        &mut self,
        worker: &WorkerId,
        limit: usize,
        fits: impl FnMut(&Claim) -> bool,
    ) -> Result<Vec<Claim>, ClaimRejection> {
        let outcome = self.claim_while_fitting(worker, limit, fits);
        self.end_call();
        outcome
    }

    fn claim_while_fitting(
        &mut self,
        worker: &WorkerId,
        limit: usize,
        mut fits: impl FnMut(&Claim) -> bool,
    ) -> Result<Vec<Claim>, ClaimRejection> {
        if !self.check_leader() {
            return Err(ClaimRejection::NotLeader);
        }
        self.release_due();
        let mut claims = Vec::new();
        let mut after = None;
        // Time is only allowed to take effect once per call: each candidate
        // is claimed as it is, whatever the clock has done since.
        while claims.len() < limit {
            let Some((position, task_id)) = self.next_unblocked_after(after) else {
                break;
            };
            after = Some(position);
            if self.is_compaction(&task_id) && !self.may_claim_compaction(worker, &task_id) {
                continue;
            }
            let claim = self.claim_to_be(&task_id);
            if fits(&claim) {
                self.mark_claimed(worker, &task_id);
                claims.push(claim);
            } else if !claims.is_empty() {
                break;
            }
        }
        Ok(claims)
    }

    /// The oldest queued task, past queue position `after` if given, that
    /// no other generation of its coalescing key holds back, and whose key is
    /// not held back for want of knowing one of its generations.
    fn next_unblocked_after(&self, after: Option<u64>) -> Option<(u64, TaskId)> {
        self.waiting
            .queued_after(after)
            .find(|(_, task_id)| !self.is_blocked(task_id) && !self.is_held_back(task_id))
            .map(|(position, task_id)| (position, task_id.clone()))
    }

    /// A worker asks for `task_id`. Of several workers asking for the same
    /// task, the first is accepted and the rest are refused.
    pub fn request_claim(
        &mut self,
        worker: &WorkerId,
        task_id: &TaskId,
    ) -> Result<Claim, ClaimRejection> {
        let outcome = self.claim_task(worker, task_id);
        self.end_call();
        outcome
    }

    fn claim_task(&mut self, worker: &WorkerId, task_id: &TaskId) -> Result<Claim, ClaimRejection> {
        if !self.check_leader() {
            return Err(ClaimRejection::NotLeader);
        }
        self.release_due();
        if self.reconciliation.is_uncertain(task_id) {
            return Err(ClaimRejection::NotReady);
        }
        let run_id = self
            .current_run
            .get(task_id)
            .ok_or(ClaimRejection::TaskUnknown)?
            .clone();
        match self.runs[&run_id].current_state() {
            TaskRunState::Queued if self.is_held_back(task_id) => Err(ClaimRejection::NotReady),
            TaskRunState::Queued if self.is_blocked(task_id) => Err(ClaimRejection::KeyBusy),
            TaskRunState::Queued if self.is_compaction(task_id) => {
                self.claim_compaction(worker, task_id)
            }
            TaskRunState::Queued => Ok(self.claim_queued(worker, task_id)),
            TaskRunState::Scheduled => Err(ClaimRejection::NotReady),
            TaskRunState::Claimed | TaskRunState::Running => Err(ClaimRejection::AlreadySelected),
            TaskRunState::Superseded => Err(ClaimRejection::Superseded),
            _ => Err(ClaimRejection::Finished),
        }
    }

    /// Claims `task_id`, whose current run must be `Queued`, for `worker`.
    fn claim_queued(&mut self, worker: &WorkerId, task_id: &TaskId) -> Claim {
        let claim = self.claim_to_be(task_id);
        self.mark_claimed(worker, task_id);
        claim
    }

    /// The claim that claiming `task_id`, whose current run must be
    /// `Queued`, would hand out. Changes nothing.
    fn claim_to_be(&self, task_id: &TaskId) -> Claim {
        let run_id = self.current_run[task_id].clone();
        let chain = match self.coalescing_key_of(task_id) {
            None if self.is_compaction(task_id) => {
                self.compaction_claim_chain(task_id).unwrap_or_default()
            }
            Some(_) => self
                .occupancy
                .chain(task_id)
                .iter()
                .map(|item| self.item_payload(item).to_vec())
                .collect(),
            None => Vec::new(),
        };
        Claim {
            task: self.tasks[task_id].clone(),
            attempt_number: self.runs[&run_id].attempt_number(),
            reconnect_timeout: self.reconnect_timeout_of(task_id),
            task_run_id: run_id,
            chain,
        }
    }

    /// How long after its worker falls silent `task_id`'s current run may be
    /// replayed: the task's own reconnect timeout, or else the shard's, from
    /// the grant. Asked only while leading, so a grant is held.
    fn reconnect_timeout_of(&self, task_id: &TaskId) -> Duration {
        match self.tasks[task_id].reconnect_timeout_ms {
            0 => self.leadership.shard_reconnect_timeout(),
            own => Duration::from_millis(own),
        }
    }

    /// Records that `worker` claimed `task_id`, whose current run must be
    /// `Queued`. Starting is what expiry is about, so the task's expiry is
    /// dropped.
    fn mark_claimed(&mut self, worker: &WorkerId, task_id: &TaskId) {
        let stamped_at = WallTime::now(&self.clock);
        let run_id = self.current_run[task_id].clone();
        let run = self
            .runs
            .get_mut(&run_id)
            .expect("every current run is stored");
        run.transition_to(TaskRunState::Claimed, stamped_at)
            .expect("a Queued run can always be claimed");
        run.selected_worker = Some(worker.clone().into());
        self.waiting.claimed(task_id);
        if let Some(key) = self.coalescing_key_of(task_id) {
            self.occupancy.start(&key, task_id);
        } else if let Some(key) = self.compaction_key_of(task_id) {
            self.occupancy.compaction_claimed(&key, task_id);
        }
        self.mark_decided(&run_id);
    }

    /// The worker that claimed `run_id` reports that it began executing. The
    /// same report again, while the run still runs, is taken and changes
    /// nothing.
    pub fn report_started(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
    ) -> Result<(), ReportRejection> {
        let outcome = self.start_run(worker, run_id);
        self.end_call();
        outcome
    }

    fn start_run(&mut self, worker: &WorkerId, run_id: &TaskRunId) -> Result<(), ReportRejection> {
        self.require_leader()?;
        self.start_owned(worker, run_id)
    }

    /// Starts `worker`'s claimed run. A start repeated by the worker of a run
    /// already running (its first went unanswered, or a new leader rebuilt the
    /// run as running) is taken again and changes nothing, so a report sent
    /// again never stops a healthy run.
    fn start_owned(&mut self, worker: &WorkerId, run_id: &TaskRunId) -> Result<(), ReportRejection> {
        let already_running = self.runs.get(run_id).is_some_and(|run| {
            run.current_state() == TaskRunState::Running && run.selected_worker().as_ref() == Some(worker)
        });
        if already_running {
            return Ok(());
        }
        let stamped_at = WallTime::now(&self.clock);
        let run = self.owned_run(worker, run_id, TaskRunState::Claimed)?;
        run.transition_to(TaskRunState::Running, stamped_at)
            .expect("a Claimed run can always start");
        self.mark_decided(run_id);
        Ok(())
    }

    /// The worker running `run_id` reports success with the digest of its
    /// result. Only a `Running` run owned by that worker can complete, so a
    /// stale or repeated report is refused and certifies nothing. `completion`
    /// says whether that ends the task.
    pub fn complete(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
        result_digest: Digest,
        completion: Completion,
    ) -> Result<Certification, ReportRejection> {
        let outcome = self.certify(worker, run_id, result_digest, completion);
        self.end_call();
        outcome
    }

    /// Certifies the result of a running run, and finishes its task unless it continues.
    fn certify(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
        result_digest: Digest,
        completion: Completion,
    ) -> Result<Certification, ReportRejection> {
        self.require_leader()?;
        // A compaction run's result is its fold, reported by `complete_compaction`.
        if self
            .runs
            .get(run_id)
            .is_some_and(|run| self.is_compaction(&run.task_id()))
        {
            return Err(ReportRejection::NotAuthoritative);
        }
        self.certify_owned(worker, run_id, result_digest, completion)
    }

    fn certify_owned(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
        result_digest: Digest,
        completion: Completion,
    ) -> Result<Certification, ReportRejection> {
        let now = self.clock.now();
        let stamped_at = WallTime::now(&self.clock);
        let run = self.owned_run(worker, run_id, TaskRunState::Running)?;
        run.transition_to(TaskRunState::Succeeded, stamped_at)
            .expect("a Running run can always succeed");
        run.result_digest = Some(result_digest.clone().into());
        let task_id = run.task_id();
        self.mark_decided(run_id);
        match completion {
            Completion::Continues => {
                self.continuing.insert(task_id.clone());
            }
            Completion::Final => self.record_finished(&task_id, now),
        }
        Ok(Certification {
            task_id,
            task_run_id: run_id.clone(),
            result_digest,
        })
    }

    /// The continuation of `task_id` is over, however it ended: the task is
    /// finished after all. Says whether it had a continuation to end, so
    /// ending twice, or a task with none, changes nothing.
    pub fn end_continuation(&mut self, task_id: &TaskId) -> Result<bool, ContinuationRejection> {
        if !self.check_leader() {
            return Err(ContinuationRejection::NotLeader);
        }
        let had_one = self.finish_continuation(task_id);
        self.end_call();
        Ok(had_one)
    }

    fn finish_continuation(&mut self, task_id: &TaskId) -> bool {
        if !self.continuing.remove(task_id) {
            return false;
        }
        let now = self.clock.now();
        self.record_finished(task_id, now);
        true
    }

    /// The worker running `run_id` reports that it failed with an error of
    /// type `failure_kind` (its name, never its message; one longer than
    /// `MAX_FAILURE_KIND_BYTES` is cut). Like completing,
    /// only a `Running` run owned by that worker can fail.
    pub fn fail(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
        failure_kind: impl Into<String>,
    ) -> Result<Failure, ReportRejection> {
        let outcome = self.fail_run(worker, run_id, failure_kind.into());
        self.end_call();
        outcome
    }

    fn fail_run(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
        failure_kind: String,
    ) -> Result<Failure, ReportRejection> {
        self.require_leader()?;
        self.fail_owned(worker, run_id, failure_kind)
    }

    fn fail_owned(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
        failure_kind: String,
    ) -> Result<Failure, ReportRejection> {
        self.start_if_claimed_compaction(worker, run_id);
        let now = self.clock.now();
        let stamped_at = WallTime::now(&self.clock);
        let run = self.owned_run(worker, run_id, TaskRunState::Running)?;
        run.transition_to(TaskRunState::Failed, stamped_at)
            .expect("a Running run can always fail");
        run.failure_kind = cut_to_fit(failure_kind, MAX_FAILURE_KIND_BYTES);
        let task_id = run.task_id();
        let attempt = run.attempt_number();
        self.mark_decided(run_id);
        self.note_failed_compaction(&task_id);
        let retry = self.replace_failed_run(&task_id, run_id, attempt);
        if retry.is_none() {
            self.record_finished(&task_id, now);
        }
        Ok(Failure {
            task_id,
            task_run_id: run_id.clone(),
            retry,
        })
    }

    pub fn task_run(&self, run_id: &TaskRunId) -> Option<&TaskRun> {
        self.runs.get(run_id)
    }

    /// Cancels `task_id` whatever state its current run is in: a pending or
    /// scheduled run never starts, and a claimed or running one is taken from
    /// its worker, whose later reports are refused. Only a leader decides.
    /// A cancelled task is not retried, and does not expire.
    pub fn cancel(&mut self, task_id: &TaskId) -> Result<Cancellation, CancelRejection> {
        let outcome = self.cancel_task(task_id);
        self.end_call();
        outcome
    }

    fn cancel_task(&mut self, task_id: &TaskId) -> Result<Cancellation, CancelRejection> {
        if !self.check_leader() {
            return Err(CancelRejection::NotLeader);
        }
        if self.reconciliation.is_uncertain(task_id) {
            return Err(CancelRejection::NotReady);
        }
        let Some(run_id) = self.current_run.get(task_id).cloned() else {
            return Ok(Cancellation::UnknownTask);
        };
        // Compaction runs are internal: no client holds their ids.
        if self.is_compaction(task_id) {
            return Ok(Cancellation::UnknownTask);
        }
        let now = self.clock.now();
        let stamped_at = WallTime::now(&self.clock);
        let run = self
            .runs
            .get_mut(&run_id)
            .expect("every current run is stored");
        let was_running = match run.current_state() {
            TaskRunState::Scheduled | TaskRunState::Queued => false,
            TaskRunState::Claimed | TaskRunState::Running => true,
            _ => return Ok(Cancellation::AlreadyFinished),
        };
        run.transition_to(TaskRunState::Cancelled, stamped_at)
            .expect("an unfinished run can always be cancelled");
        self.waiting.leave(task_id);
        self.record_finished(task_id, now);
        self.mark_decided(&run_id);
        self.events.push(Event::Cancelled {
            task_id: task_id.clone(),
            task_run_id: run_id,
            was_running,
        });
        Ok(Cancellation::Cancelled { was_running })
    }

    /// Its node's office counts `worker` silent from `reconnect_from` on:
    /// each claimed or running run the worker holds is lost once that run's
    /// own reconnect timeout has passed since then (a run of a task
    /// submitted without one at the shard's, which the grant carries). A
    /// lost run is replayed at once and uses up no retry, except that an
    /// ephemeral task's stays `Lost`, a running run of a non-retriable task
    /// is `Orphaned`, and a coalescing generation with a newer one waiting
    /// stays lost. `None`: the worker was heard again, and the runs it holds
    /// are kept. Kept while this scheduler does not lead, and acted on while
    /// it does, here and by [`Self::catch_up`] at [`Self::next_deadline`].
    pub fn note_silence(&mut self, worker: &WorkerId, reconnect_from: Option<Instant>) {
        match reconnect_from {
            Some(from) => {
                self.silent.insert(worker.clone(), from);
            }
            None => {
                self.silent.remove(worker);
            }
        }
        if self.check_leader() {
            self.lose_overdue();
        }
        self.end_call();
    }

    /// Each claimed or running current run of a silent worker, by its task,
    /// with the instant from which it may be replayed. One pass over the
    /// current runs, and none while no worker is silent.
    fn replays_due(&self) -> Vec<(Instant, TaskId)> {
        if self.silent.is_empty() {
            return Vec::new();
        }
        self.current_run
            .iter()
            .filter_map(|(task_id, run_id)| {
                let run = &self.runs[run_id];
                if !matches!(
                    run.current_state(),
                    TaskRunState::Claimed | TaskRunState::Running
                ) {
                    return None;
                }
                let from = self.silent.get(run.selected_worker().as_ref()?)?;
                Some((*from + self.reconnect_timeout_of(task_id), task_id.clone()))
            })
            .collect()
    }

    /// Loses every run of a silent worker whose reconnect timeout has
    /// passed (see [`Self::note_silence`]). Only a leader decides this, so
    /// its callers check.
    fn lose_overdue(&mut self) {
        let now = self.clock.now();
        let stamped_at = WallTime::now(&self.clock);
        for (at, task_id) in self.replays_due() {
            if at <= now {
                self.lose_run(&task_id, now, stamped_at);
            }
        }
    }

    /// One run `worker` holds is gone while the worker is not: the process
    /// that ran its body died. The run becomes lost or orphaned, and is
    /// replayed or not, exactly as if its whole worker had been lost.
    /// Refused, as a failure report is, when this node does not lead, the
    /// run is unknown, or it is not a claimed or running run of `worker`
    /// (it already ended, or a retry or replay replaced it).
    pub fn report_lost(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
    ) -> Result<LostRun, ReportRejection> {
        let outcome = self.lose_held_run(worker, run_id);
        self.end_call();
        outcome
    }

    fn lose_held_run(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
    ) -> Result<LostRun, ReportRejection> {
        self.require_leader()?;
        let expected = match self.runs.get(run_id).map(TaskRun::current_state) {
            Some(TaskRunState::Claimed) => TaskRunState::Claimed,
            _ => TaskRunState::Running,
        };
        // A replaced run is `Failed` or `Lost`, so a run still held in one of
        // these states is its task's current run.
        let task_id = self.owned_run(worker, run_id, expected)?.task_id();
        let now = self.clock.now();
        let stamped_at = WallTime::now(&self.clock);
        Ok(self.lose_run(&task_id, now, stamped_at))
    }

    /// The workers a claimed or running current run of one of `tasks` is
    /// selected for.
    fn holders_of<'t>(&self, tasks: impl Iterator<Item = &'t TaskId>) -> BTreeSet<WorkerId> {
        tasks
            .filter_map(|task_id| {
                let run = &self.runs[self.current_run.get(task_id)?];
                matches!(
                    run.current_state(),
                    TaskRunState::Claimed | TaskRunState::Running
                )
                .then(|| run.selected_worker())
                .flatten()
            })
            .collect()
    }

    /// The tasks whose current run is claimed or running for `worker`.
    fn held_by(&self, worker: &WorkerId) -> Vec<TaskId> {
        self.current_run
            .iter()
            .filter(|(_, run_id)| {
                let run = &self.runs[*run_id];
                matches!(
                    run.current_state(),
                    TaskRunState::Claimed | TaskRunState::Running
                ) && run.selected_worker().as_ref() == Some(worker)
            })
            .map(|(task_id, _)| task_id.clone())
            .collect()
    }

    /// `task_id`'s current run, claimed or running, is gone with its worker:
    /// it becomes lost, or orphaned if it was a running run of a non-retriable
    /// task, and is replayed unless the task is ephemeral, orphaned, or a
    /// newer generation of its coalescing key waits behind it.
    fn lose_run(&mut self, task_id: &TaskId, now: Instant, stamped_at: WallTime) -> LostRun {
        let run_id = self.current_run[task_id].clone();
        let was_running = self.runs[&run_id].current_state() == TaskRunState::Running;
        let task = &self.tasks[task_id];
        let orphaned = task.non_retriable && !task.ephemeral && was_running;
        let state = if orphaned {
            TaskRunState::Orphaned
        } else {
            TaskRunState::Lost
        };
        self.runs
            .get_mut(&run_id)
            .expect("every current run is stored")
            .transition_to(state, stamped_at)
            .expect("a claimed run can be lost, and a running one lost or orphaned");
        self.mark_decided(&run_id);
        let newer_waits = self
            .coalescing_key_of(task_id)
            .is_some_and(|key| self.occupancy.has_waiting(&key));
        let replayed = if orphaned || self.tasks[task_id].ephemeral || newer_waits {
            self.record_finished(task_id, now);
            None
        } else {
            *self.losses.entry(task_id.clone()).or_default() += 1;
            let next = self.queue_next_attempt(task_id, &run_id);
            if next.is_none() {
                self.record_finished(task_id, now);
            }
            next
        };
        if let Some(key) = self.compaction_key_of(task_id) {
            self.compact_if_due(&key);
        }
        LostRun {
            task_id: task_id.clone(),
            task_run_id: run_id,
            state,
            replayed,
        }
    }

    /// Every run of `task_id`, oldest attempt first; empty if the task is
    /// unknown or has been forgotten.
    pub fn runs_of(&self, task_id: &TaskId) -> Vec<TaskRunId> {
        self.runs_of_task.get(task_id).cloned().unwrap_or_default()
    }

    /// Queues the next attempt of `task_id` if it has retries left, and makes
    /// it the one authoritative run.
    fn replace_failed_run(
        &mut self,
        task_id: &TaskId,
        failed: &TaskRunId,
        attempt: u32,
    ) -> Option<TaskRunId> {
        let retries = self.tasks.get(task_id)?.max_retries;
        // Attempt 1 is the first try, so retries left = retries - (attempt - 1),
        // not counting the attempts that were lost rather than failed.
        let lost = self.losses.get(task_id).copied().unwrap_or(0);
        if attempt - lost > retries {
            return None;
        }
        self.queue_next_attempt(task_id, failed)
    }

    /// Queues the attempt that follows `previous`, a terminal run of
    /// `task_id`, and makes it the one authoritative run. If the record would
    /// then be past the limit, no attempt is created: the task is over (the
    /// caller finishes it), a `RecordFull` event is raised, and a run that
    /// has no failure kind of its own gets one saying why.
    fn queue_next_attempt(
        &mut self,
        task_id: &TaskId,
        previous: &TaskRunId,
    ) -> Option<TaskRunId> {
        let stamped_at = WallTime::now(&self.clock);
        let next = retry_of(&self.runs[previous], &self.ids, stamped_at);
        let record = self.record_of(task_id, UNVERSIONED, stamped_at);
        // Adding the run to the record also adds its field tag and length prefix.
        let next_len = next.encoded_len();
        let run_framing = 1 + prost::encoding::encoded_len_varint(next_len as u64);
        let grown = (record.encoded_len() + run_framing + next_len) as u64;
        if grown > Self::record_limit() {
            let run = self
                .runs
                .get_mut(previous)
                .expect("a previous run is stored");
            if run.failure_kind.is_empty() {
                run.failure_kind = HISTORY_TOO_LARGE_FAILURE_KIND.to_owned();
                self.mark_decided(previous);
            }
            self.events.push(Event::RecordFull {
                task_id: task_id.clone(),
            });
            return None;
        }
        let next_id = next.task_run_id();
        self.current_run.insert(task_id.clone(), next_id.clone());
        self.runs_of_task
            .entry(task_id.clone())
            .or_default()
            .push(next_id.clone());
        self.runs.insert(next_id.clone(), next);
        self.waiting.requeue(task_id);
        self.mark_decided(&next_id);
        Some(next_id)
    }

    /// Expires `task_id`, which `take_expired` found still waiting, so its
    /// current run is pending.
    fn expire(&mut self, task_id: &TaskId, now: Instant) {
        let stamped_at = WallTime::now(&self.clock);
        let run_id = self.current_run[task_id].clone();
        let run = self
            .runs
            .get_mut(&run_id)
            .expect("every current run is stored");
        // The waiting room drops a task's expiry when the task is claimed or
        // leaves, so a task it reports as expired has not started: its run is
        // still `Scheduled` or `Queued`. Without this, a broken invariant
        // would expire a claimed run, which the transition table allows.
        assert!(
            matches!(
                run.current_state(),
                TaskRunState::Scheduled | TaskRunState::Queued
            ),
            "a waiting task's run is pending"
        );
        run.transition_to(TaskRunState::Expired, stamped_at)
            .expect("a pending run can always expire");
        self.record_finished(task_id, now);
        self.mark_decided(&run_id);
        self.events.push(Event::Expired {
            task_id: task_id.clone(),
            task_run_id: run_id,
        });
    }

    /// Takes a task that is over out of every place it was waiting in.
    fn record_finished(&mut self, task_id: &TaskId, now: Instant) {
        self.release(task_id, now);
        if let Some(key) = self.compaction_key_of(task_id) {
            self.occupancy.end_compaction(&key, task_id);
        }
        if let Some(key) = self.coalescing_key_of(task_id) {
            // Whatever it absorbed is no longer needed, so it can be forgotten
            // in its turn.
            for item in self.occupancy.finish(&key, task_id) {
                self.release_item(&item, now);
            }
        }
        self.update_pressure();
    }

    /// `task_id`'s payload is no longer needed: it stops counting against
    /// memory, and the task is forgotten `result_ttl` from now.
    fn release(&mut self, task_id: &TaskId, now: Instant) {
        let payload = self.payload_len(task_id);
        self.budget.give_back(payload);
        self.forget_later(task_id, now);
    }

    /// `task_id` is over and holds no payload any more: the task is forgotten
    /// `result_ttl` from now.
    pub(super) fn forget_later(&mut self, task_id: &TaskId, now: Instant) {
        self.retention.record(task_id, now);
        self.unpublished.insert(task_id.clone());
    }

    /// An entry of a chain is no longer needed. An absorbed generation is
    /// released; a folded payload stops counting against memory (the
    /// generations it replaced were released when it replaced them).
    fn release_item(&mut self, item: &ChainItem, now: Instant) {
        match item {
            ChainItem::Absorbed(task_id) => self.release(task_id, now),
            ChainItem::Folded(folded) => self.budget.give_back(folded.payload.len() as u64),
        }
    }

    /// The serialized bytes of the task's input, which is what memory use counts.
    fn payload_len(&self, task_id: &TaskId) -> u64 {
        self.tasks[task_id].serialized_input.len() as u64
    }

    /// Refuses `submission` if it would take memory past the hard limit and
    /// cannot be made to fit by dropping what it is allowed to drop.
    fn check_room(&self, submission: &Submission, needed: u64) -> Result<(), SubmitRejection> {
        self.budget.check_room(needed, || {
            match (&submission.coalescing_key, submission.drop_oldest) {
                (Some(key), true) => {
                    let key = coalescing::key(&submission.definition_id, key);
                    self.occupancy
                        .retained(&key)
                        .iter()
                        .map(|item| self.item_len(item))
                        .sum()
                }
                _ => 0,
            }
        })
    }

    /// If `task_id` took memory past the hard limit, which `check_room` only
    /// allows for a task that may drop its key's retained payloads, drops
    /// them oldest first until it fits.
    fn drop_oldest_until_it_fits(&mut self, task_id: &TaskId, now: Instant) {
        while self.budget.over_hard_limit() {
            let Some(dropped) = self.occupancy.drop_oldest(task_id) else {
                break;
            };
            self.release_item(&dropped, now);
        }
    }

    /// Raises or clears `SlowDown` if memory use has crossed the relevant
    /// limit.
    fn update_pressure(&mut self) {
        let event = self.budget.update_pressure();
        let crossed_soft_limit = matches!(event, Some(Event::SlowDown { active: true }));
        self.announce(event);
        if crossed_soft_limit {
            self.compact_every_due_key();
        }
    }

    /// Records for the caller the event the budget returned, if
    /// any.
    fn announce(&mut self, event: Option<Event>) {
        let Some(event) = event else {
            return;
        };
        self.events.push(event);
    }

    /// `older`, a pending generation, is replaced by `newer`, which has
    /// absorbed its payload: it is over, but not forgotten until `newer` is.
    fn supersede(&mut self, older: &TaskId, newer: &TaskId) {
        let stamped_at = WallTime::now(&self.clock);
        let run_id = self.current_run[older].clone();
        self.runs
            .get_mut(&run_id)
            .expect("every current run is stored")
            .transition_to(TaskRunState::Superseded, stamped_at)
            .expect("a pending run can always be superseded");
        self.waiting.leave(older);
        self.links.entry(older.clone()).or_default().superseded_by = Some(newer.clone().into());
        self.mark_decided(&run_id);
        self.events.push(Event::Superseded {
            task_id: older.clone(),
            task_run_id: run_id,
            by: newer.clone(),
        });
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

    /// Whether it holds `task`: it decides the task, and a driver that kept
    /// where it wrote its record can forget the task once this is false.
    pub fn holds(&self, task: &TaskId) -> bool {
        self.tasks.contains_key(task)
    }

    /// How many tasks are queued to be claimed, including generations their
    /// coalescing key holds back. A delayed task counts once it is due.
    pub fn pending_len(&self) -> usize {
        self.waiting.queued_len()
    }

    /// Serialized bytes of every task that has not finished, and of every
    /// superseded one still needed by the generation that absorbed it.
    pub fn memory_in_use(&self) -> u64 {
        self.budget.in_use()
    }

    /// Its observer, for a driver that takes what the scheduler published.
    pub fn observer_mut(&mut self) -> &mut O {
        &mut self.observer
    }

    /// Records that `run_id` was decided now, and that its task has a change
    /// to publish.
    fn mark_decided(&mut self, run_id: &TaskRunId) {
        self.run_decided_at
            .insert(run_id.clone(), self.clock.now());
        self.unpublished.insert(self.runs[run_id].task_id());
    }

    fn mark_current_decided(&mut self, task_id: &TaskId) {
        let run_id = self.current_run[task_id].clone();
        self.mark_decided(&run_id);
    }

    /// `run_id`'s run, if the run exists, `worker` claimed it and it is in
    /// `expected` state. Does not ask whether this node leads: a rebuild
    /// applies what workers report before it holds a grant.
    fn owned_run(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
        expected: TaskRunState,
    ) -> Result<&mut TaskRun, ReportRejection> {
        if !self.runs.contains_key(run_id) {
            let uncertain = self.reconciliation.holds_uncertain_run(run_id);
            return Err(if uncertain {
                ReportRejection::NotReady
            } else {
                ReportRejection::UnknownRun
            });
        }
        let run = self
            .runs
            .get_mut(run_id)
            .expect("just seen");
        let owned = run.selected_worker().as_ref() == Some(worker);
        // A run replaced by a retry or a replay is `Failed` or `Lost`, which no
        // report expects, so ownership and state are enough.
        if !owned || run.current_state() != expected {
            return Err(ReportRejection::NotAuthoritative);
        }
        Ok(run)
    }
}

/// The order records are installed in, oldest submission first, so the
/// waiting room's queue follows submission order.
fn submission_order(record: &TaskRecord) -> (Option<u64>, Option<TaskId>) {
    let task = record.task.as_ref();
    (
        task.and_then(|task| task.submitted_at.as_ref().map(|at| at.unix_millis)),
        task.map(|task| task.task_id()),
    )
}

/// The chain `record` still carries, oldest entry first. An absorbed entry
/// that names no generation is left out; a folded one whose digest is missing
/// or unreadable gets the digest of its payload.
fn chain_of(record: &TaskRecord) -> Vec<ChainItem> {
    chain_items(&record.retained_chain)
}

fn chain_items(entries: &[ChainEntry]) -> Vec<ChainItem> {
    entries
        .iter()
        .filter_map(|entry| match entry.entry.as_ref()? {
            chain_entry::Entry::Absorbed(absorbed) => {
                absorbed.task_id.clone().map(|id| ChainItem::Absorbed(TaskId::from(id)))
            }
            chain_entry::Entry::Folded(folded) => Some(ChainItem::Folded(Folded {
                generations: folded.generations.iter().cloned().map(TaskId::from).collect(),
                digest: folded
                    .input_digest
                    .as_ref()
                    .and_then(|digest| Digest::try_from(digest).ok())
                    .unwrap_or_else(|| Digest::blake3(&folded.serialized_input)),
                payload: folded.serialized_input.clone(),
            })),
        })
        .collect()
}

/// The generations a compaction folded into the chain `record` carries.
fn folded_ids(record: &TaskRecord) -> BTreeSet<TaskId> {
    chain_of(record)
        .iter()
        .filter_map(|item| match item {
            ChainItem::Folded(folded) => Some(folded.generations.iter().cloned()),
            ChainItem::Absorbed(_) => None,
        })
        .flatten()
        .collect()
}

/// The generations whose payloads `record` still carries.
fn retained_ids(record: &TaskRecord) -> BTreeSet<TaskId> {
    record
        .retained_chain
        .iter()
        .filter_map(|entry| match entry.entry.as_ref()? {
            chain_entry::Entry::Absorbed(absorbed) => {
                absorbed.task_id.clone().map(TaskId::from)
            }
            chain_entry::Entry::Folded(_) => None,
        })
        .collect()
}

/// `text` cut to at most `max` bytes, at a character boundary.
fn cut_to_fit(mut text: String, max: usize) -> String {
    if text.len() > max {
        let mut end = max;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
    }
    text
}
