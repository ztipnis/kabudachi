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
use crate::protocol::records::TaskRunRecord;
use crate::protocol::task::TaskRunState;
use crate::reconcile::{
    ANSWER_IN_FLIGHT, RECONCILE_SKEW_MARGIN, Rebuild, ReconcileTerm, ReportedRun, ReportedState,
    WorkerRuns,
};
use crate::task_record::RecordVersion;
use crate::time::{Clock, Duration, Instant, WallTime};

mod backlog;
mod cancel;
mod claims;
mod compaction;
mod deadlines;
mod failures;
mod finish;
mod leadership;
mod loss;
mod memory;
mod memory_budget;
mod observer;
mod publication;
mod reconciliation;
mod record_size;
mod reports;
mod retention;
mod submission;
mod waiting_room;

pub use backlog::Backlog;
pub use cancel::{CancelRejection, Cancellation, HeldCancel};
pub use claims::{Claim, ClaimRejection};
pub use compaction::Compacted;
pub use deadlines::CaughtUp;
pub use failures::Failure;
use leadership::Leadership;
pub use leadership::{LeadershipGrant, LeaseEnd};
pub use loss::LostRun;
pub use memory::MemoryLimits;
use memory_budget::MemoryBudget;
pub use observer::{NoObserver, Observer};
use reconciliation::Reconciliation;
pub use reports::{Certification, Completion, ContinuationRejection, ReportRejection};
use retention::Retention;
pub use submission::{Submission, SubmitRejection, Submitted, mint};
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
