//! The scheduler: it holds every Task and TaskRun and is the single authority
//! on who may run what.
//!
//! It only reacts to calls and never waits, sleeps or does I/O, so the runtime
//! drives it and a test can drive it with a fake clock. It accepts claims and
//! reports only while it holds a leadership grant whose lease has not ended
//! by its own clock: a node that has not won an election, has lost
//! leadership, or has outlived its lease must not decide anything.
//!
//! It owns what time does to its tasks. [`Scheduler::catch_up`] is the one
//! time-driven entry: it forgets finished tasks whose result TTL has passed
//! and, only while the scheduler leads, releases delayed tasks that are due
//! and expires pending ones. [`Scheduler::next_deadline`] says when to call
//! it next. Every call that changes the scheduler also forgets what has
//! outlived its TTL, so a caller never sweeps.
//!
//! Tasks and runs are stored privately and handed out only as shared
//! references or clones, so nothing outside can edit a submitted Task or move
//! a run without going through the transition table.

use std::collections::{BTreeMap, BTreeSet};

use crate::coalescing::{self, Key, Occupancy};
use crate::protocol::ids::{IdGenerator, TaskDefinitionId, TaskId, TaskRunId, WorkerId};
use crate::protocol::messages::prelude::*;
use crate::protocol::messages::{Task, TaskRun};
use crate::protocol::records::{NewTask, TaskRunRecord, first_attempt, new_task, retry_of};
use crate::protocol::task::TaskRunState;
use crate::time::{Clock, Duration, Instant};

mod memory_budget;
mod observer;
mod retention;
mod waiting_room;

pub use observer::{Change, Counts, NoObserver, Observer};
use memory_budget::MemoryBudget;
use retention::Retention;
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
/// It does not bound a coalescing task's retained chain, which a claim also
/// carries: many small superseded payloads can still add up past a frame.
/// Bounding that is chain compaction, which is not implemented yet; a
/// claim that has outgrown a frame is passed over by the leader's batch
/// claim until then.
pub const MAX_SUBMISSION_BYTES: u64 = MAX_CLAIM_FRAME_BYTES - CLAIM_OVERHEAD_BYTES;

/// Why a submission was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SubmitRejection {
    #[error("a task of {size} bytes could never be claimed: at most {limit} fit in one message")]
    TooLarge { size: u64, limit: u64 },
    #[error(
        "{needed} bytes would take memory past the hard limit of {hard_limit} ({in_use} in use)"
    )]
    Backpressure {
        hard_limit: u64,
        in_use: u64,
        needed: u64,
    },
}

/// What a task is submitted with. Start from [`Submission::new`] and add the
/// options that differ from the defaults.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Submission {
    pub definition_id: TaskDefinitionId,
    pub source_version: u32,
    pub serialized_input: Vec<u8>,
    pub queue: String,
    /// How many more attempts a failed run is replaced by.
    pub retries: u32,
    /// How long after submission the task may first start.
    pub delay: Option<Duration>,
    /// How long after submission the task may wait to start before it
    /// expires instead.
    pub expiry: Option<Duration>,
    /// Makes this a coalescing generation of that key: a newer pending one
    /// supersedes an older one with the same key.
    pub coalescing_key: Option<String>,
    /// For a coalescing task: past the hard limit, drop the key's oldest
    /// retained payloads to make room instead of refusing the submission.
    pub drop_oldest: bool,
    /// An ephemeral task's run lost with its worker is not replayed.
    pub ephemeral: bool,
    /// A non-retriable task's run that was running when its worker was lost
    /// is orphaned, not replayed.
    pub non_retriable: bool,
}

impl Submission {
    /// A submission with no retries.
    pub fn new(
        definition_id: TaskDefinitionId,
        source_version: u32,
        serialized_input: Vec<u8>,
        queue: impl Into<String>,
    ) -> Self {
        Submission {
            definition_id,
            source_version,
            serialized_input,
            queue: queue.into(),
            retries: 0,
            delay: None,
            expiry: None,
            coalescing_key: None,
            drop_oldest: false,
            ephemeral: false,
            non_retriable: false,
        }
    }

    /// The task is ephemeral: a run lost with its worker
    /// becomes `Lost` and the task is over.
    pub fn ephemeral(mut self) -> Self {
        self.ephemeral = true;
        self
    }

    /// The task is not safe to run twice: a run that was running when its
    /// worker was lost becomes `Orphaned` and the task is over.
    pub fn non_retriable(mut self) -> Self {
        self.non_retriable = true;
        self
    }

    /// Past the hard memory limit, make room by dropping this coalescing
    /// key's oldest retained payloads rather than refusing (never the
    /// default: dropped payloads are never folded).
    pub fn with_drop_oldest(mut self) -> Self {
        self.drop_oldest = true;
        self
    }

    /// The task is a generation of the coalescing key `key`.
    pub fn with_coalescing_key(mut self, key: impl Into<String>) -> Self {
        self.coalescing_key = Some(key.into());
        self
    }

    pub fn with_retries(mut self, retries: u32) -> Self {
        self.retries = retries;
        self
    }

    /// The task waits `Scheduled` for `delay` and only then becomes pending.
    pub fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = Some(delay);
        self
    }

    /// The task expires if it has not been claimed within `expiry`.
    pub fn with_expiry(mut self, expiry: Duration) -> Self {
        self.expiry = Some(expiry);
        self
    }
}

/// A worker's permission to run a task: the Task itself and the run it now
/// owns.
#[derive(Debug, Clone, PartialEq)]
pub struct Claim {
    pub task: Task,
    pub task_run_id: TaskRunId,
    /// Which attempt this run is: 1 for the first, then one more per retry.
    pub attempt_number: u32,
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
    pub result_digest: Vec<u8>,
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

/// Why losing a worker was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum LoseRejection {
    #[error("this node is not the leader")]
    NotLeader,
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
}

/// What lets a scheduler act as its shard's leader: its worker's election
/// says the worker leads `term` at `recovery_epoch`, and may act until
/// `valid_until`.
///
/// The election hands it over (see `election::carry_out`), and the
/// scheduler holds it until the election reports a change. The scheduler
/// checks `valid_until` against its own clock on every leader-only call, so
/// a grant that has run out stops it acting even before the election says
/// anything more.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeadershipGrant {
    pub term: u64,
    pub recovery_epoch: u64,
    pub valid_until: LeaseEnd,
}

/// When a leader's lease ends, and with it the time it may act as leader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseEnd {
    /// Nothing ends it, as for a leader that alone is a majority of its
    /// electorate.
    Unbounded,
    /// It ends at this instant of the election's clock, which the scheduler
    /// must read too: the lease is over from this instant on.
    At(Instant),
}

/// Owns all Tasks and TaskRuns and decides every claim and report. Drive it
/// with calls; it never blocks or does I/O.
pub struct Scheduler<C: Clock, I: IdGenerator, O: Observer = NoObserver> {
    clock: C,
    ids: I,
    /// Told about every change the scheduler makes.
    observer: O,
    /// What the scheduler last told its observer about leadership: it tells
    /// changes, not checks, and `next_deadline` reads it for the lease end.
    noticed_leading: bool,
    /// The leadership grant its worker's election last gave it, if any.
    grant: Option<LeadershipGrant>,
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
    /// Like [`Self::new`], telling `observer` about every change.
    pub fn with_observer(clock: C, ids: I, observer: O) -> Self {
        Scheduler {
            clock,
            ids,
            observer,
            noticed_leading: false,
            grant: None,
            tasks: BTreeMap::new(),
            runs: BTreeMap::new(),
            current_run: BTreeMap::new(),
            runs_of_task: BTreeMap::new(),
            waiting: WaitingRoom::default(),
            retention: Retention::default(),
            events: Vec::new(),
            occupancy: Occupancy::default(),
            budget: MemoryBudget::default(),
            continuing: BTreeSet::new(),
            losses: BTreeMap::new(),
        }
    }

    /// How long a task is kept after it finishes, or `None` to keep tasks
    /// forever. Takes effect at once.
    pub fn set_result_ttl(&mut self, ttl: Option<Duration>) {
        self.retention.set_ttl(ttl);
        self.forget_due();
    }

    /// Forgets every task that finished at least `result_ttl` ago, with its
    /// run, and returns how many. Cheap when nothing is due, so every
    /// mutating call ends with it.
    fn forget_due(&mut self) -> usize {
        let Some(due) = self.retention.next_due() else {
            return 0;
        };
        let now = self.clock.now();
        if due > now {
            return 0;
        }
        let mut forgotten = 0;
        for task_id in self.retention.take_due(now) {
            self.current_run.remove(&task_id);
            self.losses.remove(&task_id);
            for run_id in self.runs_of_task.remove(&task_id).unwrap_or_default() {
                self.runs.remove(&run_id);
            }
            self.tasks.remove(&task_id);
            let counts = self.counts();
            self.observer
                .notify(Change::TaskForgotten(&task_id), counts);
            forgotten += 1;
        }
        forgotten
    }

    /// Sets the memory limits, or removes them with `None`.
    ///
    /// # Panics
    ///
    /// If the soft limit is above the hard one.
    pub fn set_memory_limits(&mut self, limits: Option<MemoryLimits>) {
        let event = self.budget.set_limits(limits);
        self.announce(event);
        self.forget_due();
    }

    /// Takes the leadership grant its worker's election reports, or `None`
    /// once the worker does not lead. The scheduler keeps it until the next
    /// call, but acts on it only until its lease ends.
    pub fn set_leadership_grant(&mut self, grant: Option<LeadershipGrant>) {
        self.grant = grant;
        self.check_leader();
        self.forget_due();
    }

    /// Records a new Task and queues its first run. Every call is a new Task
    /// (submission is not idempotent). Submitting decides
    /// nothing, so it does not require leadership; only claims and reports do.
    pub fn submit(&mut self, submission: Submission) -> Result<TaskId, SubmitRejection> {
        let outcome = self.record_submission(submission);
        self.forget_due();
        outcome
    }

    fn record_submission(&mut self, submission: Submission) -> Result<TaskId, SubmitRejection> {
        let needed = submission.serialized_input.len() as u64;
        let size = needed
            + (submission.queue.len()
                + submission.definition_id.as_str().len()
                + submission.coalescing_key.as_deref().map_or(0, str::len)) as u64;
        if size > MAX_SUBMISSION_BYTES {
            return Err(SubmitRejection::TooLarge {
                size,
                limit: MAX_SUBMISSION_BYTES,
            });
        }
        self.check_room(&submission, needed)?;
        let now = self.clock.now();
        let not_before = submission
            .delay
            .filter(|delay| delay.as_ticks() > 0)
            .map(|delay| now + delay);
        let expires_at = submission.expiry.map(|expiry| now + expiry);
        let task = new_task(
            &self.ids,
            now,
            NewTask {
                definition_id: submission.definition_id,
                source_version: submission.source_version,
                serialized_input: submission.serialized_input,
                queue: submission.queue,
                max_retries: submission.retries,
                not_before,
                expires_at,
                coalescing_key: submission.coalescing_key,
                ephemeral: submission.ephemeral,
                non_retriable: submission.non_retriable,
            },
        );
        let first_state = if not_before.is_some() {
            TaskRunState::Scheduled
        } else {
            TaskRunState::Queued
        };
        let run = first_attempt(&task, &self.ids, now, first_state);

        let task_id = task.task_id();
        self.current_run.insert(task_id.clone(), run.task_run_id());
        self.runs_of_task
            .insert(task_id.clone(), vec![run.task_run_id()]);
        self.runs.insert(run.task_run_id(), run);
        self.tasks.insert(task_id.clone(), task);
        self.waiting.admit(&task_id, not_before, expires_at);
        let counts = self.counts();
        self.observer
            .notify(Change::TaskRecorded(&self.tasks[&task_id]), counts);
        self.notify_current_run(&task_id);
        self.budget.take(needed);
        if needed > 0 {
            self.notify_memory();
        }
        let older = self
            .coalescing_key_of(&task_id)
            .and_then(|key| self.occupancy.submit(&key, &task_id));
        if let Some(older) = older {
            self.supersede(&older, &task_id, now);
        }
        self.drop_oldest_until_it_fits(&task_id, now);
        self.update_pressure();
        Ok(task_id)
    }

    /// Lets time take effect, and is the only call a runtime makes for time's
    /// sake: it forgets every task that finished at least `result_ttl` ago,
    /// and, only while this scheduler leads, makes due delayed tasks pending
    /// and expires pending tasks past their expiry (reported through
    /// [`Self::take_events`]). Call it when [`Self::next_deadline`] comes.
    pub fn catch_up(&mut self) -> CaughtUp {
        let mut caught_up = if self.check_leader() {
            self.release_due()
        } else {
            CaughtUp::default()
        };
        caught_up.forgotten = self.forget_due();
        caught_up
    }

    /// Delayed tasks that are due become pending, and pending tasks past
    /// their expiry expire. Only a leader decides this, so its callers check.
    /// Claims do this themselves first, so a task that ran out of time is
    /// never handed out.
    fn release_due(&mut self) -> CaughtUp {
        let mut advanced = CaughtUp::default();
        let now = self.clock.now();
        for task_id in self.waiting.take_expired(now) {
            self.expire(&task_id, now);
            advanced.expired += 1;
        }
        for task_id in self.waiting.release_due(now) {
            let run_id = self.current_run[&task_id].clone();
            self.runs
                .get_mut(&run_id)
                .expect("every current run is stored")
                .transition_to(TaskRunState::Queued, now)
                .expect("a Scheduled run can always be queued");
            self.notify_run(&run_id);
            advanced.queued += 1;
        }
        advanced
    }

    /// When [`Self::catch_up`] next has something to do, or `None`: the next
    /// forgetting time always, and the next delay or expiry only while this
    /// scheduler leads. Also the end of a bounded lease while this scheduler
    /// has found itself leading: that end stays in the answer until a call
    /// finds it passed, since finding the lapse is then `catch_up`'s to do.
    /// An unbounded lease adds nothing.
    pub fn next_deadline(&self) -> Option<Instant> {
        let forgetting = self.retention.next_due();
        // A lapse is found by a call that reads the clock, so the lease end
        // stays in the answer until one has: the caller that asks "is it due
        // yet?" must still get to `catch_up`. That is why it reads what was
        // noticed, not `is_leader`, which is false from the end's instant on.
        let lease_end = match self.grant.map(|grant| grant.valid_until) {
            Some(LeaseEnd::At(end)) if self.noticed_leading => Some(end),
            _ => None,
        };
        let waiting = if self.is_leader() {
            self.waiting.next_deadline()
        } else {
            None
        };
        [waiting, forgetting, lease_end].into_iter().flatten().min()
    }

    /// Whether [`Self::take_events`] has anything to return.
    pub fn has_events(&self) -> bool {
        !self.events.is_empty()
    }

    /// What the scheduler decided on its own since the last call, in the
    /// order it happened.
    pub fn take_events(&mut self) -> Vec<Event> {
        let events = std::mem::take(&mut self.events);
        self.forget_due();
        events
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
    /// never fit, is passed over so the tasks behind it still go out. It is
    /// how a caller that must deliver the claims keeps them within what it
    /// can deliver.
    pub fn claim_oldest_fitting(
        &mut self,
        worker: &WorkerId,
        limit: usize,
        fits: impl FnMut(&Claim) -> bool,
    ) -> Result<Vec<Claim>, ClaimRejection> {
        let outcome = self.claim_while_fitting(worker, limit, fits);
        self.forget_due();
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
    /// no other generation of its coalescing key holds back.
    fn next_unblocked_after(&self, after: Option<u64>) -> Option<(u64, TaskId)> {
        self.waiting
            .queued_after(after)
            .find(|(_, task_id)| !self.is_blocked(task_id))
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
        self.forget_due();
        outcome
    }

    fn claim_task(&mut self, worker: &WorkerId, task_id: &TaskId) -> Result<Claim, ClaimRejection> {
        if !self.check_leader() {
            return Err(ClaimRejection::NotLeader);
        }
        self.release_due();
        let run_id = self
            .current_run
            .get(task_id)
            .ok_or(ClaimRejection::TaskUnknown)?
            .clone();
        match self.runs[&run_id].current_state() {
            TaskRunState::Queued if self.is_blocked(task_id) => Err(ClaimRejection::KeyBusy),
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
            Some(_) => self
                .occupancy
                .chain(task_id)
                .iter()
                .map(|absorbed| self.tasks[absorbed].serialized_input.clone())
                .collect(),
            None => Vec::new(),
        };
        Claim {
            task: self.tasks[task_id].clone(),
            attempt_number: self.runs[&run_id].attempt_number(),
            task_run_id: run_id,
            chain,
        }
    }

    /// Records that `worker` claimed `task_id`, whose current run must be
    /// `Queued`. Starting is what expiry is about, so the task's expiry is
    /// dropped.
    fn mark_claimed(&mut self, worker: &WorkerId, task_id: &TaskId) {
        let now = self.clock.now();
        let run_id = self.current_run[task_id].clone();
        let run = self
            .runs
            .get_mut(&run_id)
            .expect("every current run is stored");
        run.transition_to(TaskRunState::Claimed, now)
            .expect("a Queued run can always be claimed");
        run.selected_worker = Some(worker.clone().into());
        self.waiting.claimed(task_id);
        if let Some(key) = self.coalescing_key_of(task_id) {
            self.occupancy.start(&key, task_id);
        }
        self.notify_run(&run_id);
    }

    /// The worker that claimed `run_id` reports that it began executing.
    pub fn report_started(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
    ) -> Result<(), ReportRejection> {
        let outcome = self.start_run(worker, run_id);
        self.forget_due();
        outcome
    }

    fn start_run(&mut self, worker: &WorkerId, run_id: &TaskRunId) -> Result<(), ReportRejection> {
        let now = self.clock.now();
        let run = self.run_owned_by(worker, run_id, TaskRunState::Claimed)?;
        run.transition_to(TaskRunState::Running, now)
            .expect("a Claimed run can always start");
        self.notify_run(run_id);
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
        result_digest: Vec<u8>,
        completion: Completion,
    ) -> Result<Certification, ReportRejection> {
        let outcome = self.certify(worker, run_id, result_digest, completion);
        self.forget_due();
        outcome
    }

    /// Certifies the result of a running run, and finishes its task unless it continues.
    fn certify(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
        result_digest: Vec<u8>,
        completion: Completion,
    ) -> Result<Certification, ReportRejection> {
        let now = self.clock.now();
        let run = self.run_owned_by(worker, run_id, TaskRunState::Running)?;
        run.transition_to(TaskRunState::Succeeded, now)
            .expect("a Running run can always succeed");
        run.result_digest = result_digest.clone();
        let task_id = run.task_id();
        self.notify_run(run_id);
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
    pub fn end_continuation(&mut self, task_id: &TaskId) -> bool {
        let had_one = self.finish_continuation(task_id);
        self.forget_due();
        had_one
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
    /// type `failure_kind` (its name, never its message). Like completing,
    /// only a `Running` run owned by that worker can fail.
    pub fn fail(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
        failure_kind: impl Into<String>,
    ) -> Result<Failure, ReportRejection> {
        let outcome = self.fail_run(worker, run_id, failure_kind.into());
        self.forget_due();
        outcome
    }

    fn fail_run(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
        failure_kind: String,
    ) -> Result<Failure, ReportRejection> {
        let now = self.clock.now();
        let run = self.run_owned_by(worker, run_id, TaskRunState::Running)?;
        run.transition_to(TaskRunState::Failed, now)
            .expect("a Running run can always fail");
        run.failure_kind = failure_kind;
        let task_id = run.task_id();
        let attempt = run.attempt_number();
        self.notify_run(run_id);
        let retry = self.replace_failed_run(&task_id, run_id, attempt, now);
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
        self.forget_due();
        outcome
    }

    fn cancel_task(&mut self, task_id: &TaskId) -> Result<Cancellation, CancelRejection> {
        if !self.check_leader() {
            return Err(CancelRejection::NotLeader);
        }
        let Some(run_id) = self.current_run.get(task_id).cloned() else {
            return Ok(Cancellation::UnknownTask);
        };
        let now = self.clock.now();
        let run = self
            .runs
            .get_mut(&run_id)
            .expect("every current run is stored");
        let was_running = match run.current_state() {
            TaskRunState::Scheduled | TaskRunState::Queued => false,
            TaskRunState::Claimed | TaskRunState::Running => true,
            _ => return Ok(Cancellation::AlreadyFinished),
        };
        run.transition_to(TaskRunState::Cancelled, now)
            .expect("an unfinished run can always be cancelled");
        self.waiting.leave(task_id);
        self.record_finished(task_id, now);
        self.notify_run(&run_id);
        self.events.push(Event::Cancelled {
            task_id: task_id.clone(),
            task_run_id: run_id,
            was_running,
        });
        Ok(Cancellation::Cancelled { was_running })
    }

    /// `worker` is lost, its reconnect timeout having passed: every
    /// run it held, claimed or running, becomes `Lost` and nothing it reports
    /// afterwards counts. Each is replayed by a new queued attempt, which is
    /// at-least-once and does not use up a retry (a loss is not a failure), except a coalescing
    /// generation that a newer one is waiting behind: that one stays lost and
    /// its payload is not folded into the newer one. Two kinds
    /// are not replayed: an ephemeral task's run stays `Lost`,
    /// and the running run of a non-retriable task becomes `Orphaned`, since
    /// its effects may have happened. A non-retriable task's claimed run never
    /// started, so it is replayed. Either way the task is over. Only a leader
    /// decides.
    pub fn lose_worker(&mut self, worker: &WorkerId) -> Result<Vec<LostRun>, LoseRejection> {
        let outcome = self.lose_runs_of(worker);
        self.forget_due();
        outcome
    }

    fn lose_runs_of(&mut self, worker: &WorkerId) -> Result<Vec<LostRun>, LoseRejection> {
        if !self.check_leader() {
            return Err(LoseRejection::NotLeader);
        }
        let now = self.clock.now();
        let held: Vec<TaskId> = self
            .current_run
            .iter()
            .filter(|(_, run_id)| {
                let run = &self.runs[*run_id];
                matches!(
                    run.current_state(),
                    TaskRunState::Claimed | TaskRunState::Running
                ) && run.selected_worker().as_ref() == Some(worker)
            })
            .map(|(task_id, _)| task_id.clone())
            .collect();
        let mut lost = Vec::new();
        for task_id in held {
            let run_id = self.current_run[&task_id].clone();
            let was_running = self.runs[&run_id].current_state() == TaskRunState::Running;
            let task = &self.tasks[&task_id];
            let orphaned = task.non_retriable && !task.ephemeral && was_running;
            let state = if orphaned {
                TaskRunState::Orphaned
            } else {
                TaskRunState::Lost
            };
            self.runs
                .get_mut(&run_id)
                .expect("every current run is stored")
                .transition_to(state, now)
                .expect("a claimed run can be lost, and a running one lost or orphaned");
            self.notify_run(&run_id);
            let newer_waits = self
                .coalescing_key_of(&task_id)
                .is_some_and(|key| self.occupancy.has_waiting(&key));
            let replayed = if orphaned || self.tasks[&task_id].ephemeral || newer_waits {
                self.record_finished(&task_id, now);
                None
            } else {
                *self.losses.entry(task_id.clone()).or_default() += 1;
                Some(self.queue_next_attempt(&task_id, &run_id, now))
            };
            lost.push(LostRun {
                task_id,
                task_run_id: run_id,
                state,
                replayed,
            });
        }
        Ok(lost)
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
        now: Instant,
    ) -> Option<TaskRunId> {
        let retries = self.tasks.get(task_id)?.max_retries;
        // Attempt 1 is the first try, so retries left = retries - (attempt - 1),
        // not counting the attempts that were lost rather than failed.
        let lost = self.losses.get(task_id).copied().unwrap_or(0);
        if attempt - lost > retries {
            return None;
        }
        Some(self.queue_next_attempt(task_id, failed, now))
    }

    /// Queues the attempt that follows `previous`, a terminal run of
    /// `task_id`, and makes it the one authoritative run.
    fn queue_next_attempt(
        &mut self,
        task_id: &TaskId,
        previous: &TaskRunId,
        now: Instant,
    ) -> TaskRunId {
        let next = retry_of(&self.runs[previous], &self.ids, now);
        let next_id = next.task_run_id();
        self.current_run.insert(task_id.clone(), next_id.clone());
        self.runs_of_task
            .entry(task_id.clone())
            .or_default()
            .push(next_id.clone());
        self.runs.insert(next_id.clone(), next);
        self.waiting.requeue(task_id);
        self.notify_run(&next_id);
        next_id
    }

    /// Expires `task_id`, which `take_expired` found still waiting, so its
    /// current run is pending.
    fn expire(&mut self, task_id: &TaskId, now: Instant) {
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
        run.transition_to(TaskRunState::Expired, now)
            .expect("a pending run can always expire");
        self.record_finished(task_id, now);
        self.notify_run(&run_id);
        self.events.push(Event::Expired {
            task_id: task_id.clone(),
            task_run_id: run_id,
        });
    }

    /// Takes a task that is over out of every place it was waiting in.
    fn record_finished(&mut self, task_id: &TaskId, now: Instant) {
        self.release(task_id, now);
        if let Some(key) = self.coalescing_key_of(task_id) {
            // Whatever it absorbed is no longer needed, so it can be forgotten
            // in its turn.
            for absorbed in self.occupancy.finish(&key, task_id) {
                self.release(&absorbed, now);
            }
        }
        self.update_pressure();
    }

    /// `task_id`'s payload is no longer needed: it stops counting against
    /// memory, and the task is forgotten `result_ttl` from now.
    fn release(&mut self, task_id: &TaskId, now: Instant) {
        let payload = self.payload_len(task_id);
        self.budget.give_back(payload);
        self.retention.record(task_id, now);
        if payload > 0 {
            self.notify_memory();
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
                        .map(|task_id| self.payload_len(task_id))
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
            self.release(&dropped, now);
        }
    }

    /// Raises or clears `SlowDown` if memory use has crossed the relevant
    /// limit.
    fn update_pressure(&mut self) {
        let event = self.budget.update_pressure();
        self.announce(event);
    }

    /// Tells the observer, then records for the caller, the `SlowDown` change
    /// the budget returned, if any.
    fn announce(&mut self, event: Option<Event>) {
        let Some(event) = event else {
            return;
        };
        if let Event::SlowDown { active } = event {
            self.notify_slow_down(active);
        }
        self.events.push(event);
    }

    /// `older`, a pending generation, is replaced by `newer`, which has
    /// absorbed its payload: it is over, but not forgotten until `newer` is.
    fn supersede(&mut self, older: &TaskId, newer: &TaskId, now: Instant) {
        let run_id = self.current_run[older].clone();
        self.runs
            .get_mut(&run_id)
            .expect("every current run is stored")
            .transition_to(TaskRunState::Superseded, now)
            .expect("a pending run can always be superseded");
        self.waiting.leave(older);
        self.notify_run(&run_id);
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

    /// Whether a task has to wait because another generation of its
    /// coalescing key holds the key.
    fn is_blocked(&self, task_id: &TaskId) -> bool {
        self.coalescing_key_of(task_id)
            .is_some_and(|key| self.occupancy.is_blocked(&key, task_id))
    }

    /// Whether this worker leads, the only one that decides anything: it
    /// holds a grant whose lease has not ended by this scheduler's clock.
    fn is_leader(&self) -> bool {
        match self.grant.map(|grant| grant.valid_until) {
            None => false,
            Some(LeaseEnd::Unbounded) => true,
            Some(LeaseEnd::At(end)) => self.clock.now() < end,
        }
    }

    /// Like `is_leader`, and tells the observer if the answer differs from
    /// what it was last told, so the observer hears changes, not checks.
    fn check_leader(&mut self) -> bool {
        let leading = self.is_leader();
        if leading != self.noticed_leading {
            self.noticed_leading = leading;
            let counts = self.counts();
            self.observer.notify(Change::Leadership(leading), counts);
        }
        leading
    }

    /// The counts every notification carries, read after the change.
    fn counts(&self) -> Counts {
        Counts {
            pending: self.waiting.queued_len(),
            memory_in_use: self.budget.in_use(),
        }
    }

    fn notify_run(&mut self, run_id: &TaskRunId) {
        let counts = self.counts();
        self.observer.notify(Change::Run(&self.runs[run_id]), counts);
    }

    fn notify_current_run(&mut self, task_id: &TaskId) {
        let run_id = self.current_run[task_id].clone();
        self.notify_run(&run_id);
    }

    fn notify_memory(&mut self) {
        let counts = self.counts();
        self.observer.notify(Change::Memory, counts);
    }

    fn notify_slow_down(&mut self, active: bool) {
        let counts = self.counts();
        self.observer.notify(Change::SlowDown(active), counts);
    }

    /// `run_id`'s run, if this node is leader, the run exists, `worker`
    /// claimed it and it is in `expected` state.
    fn run_owned_by(
        &mut self,
        worker: &WorkerId,
        run_id: &TaskRunId,
        expected: TaskRunState,
    ) -> Result<&mut TaskRun, ReportRejection> {
        if !self.check_leader() {
            return Err(ReportRejection::NotLeader);
        }
        let run = self
            .runs
            .get_mut(run_id)
            .ok_or(ReportRejection::UnknownRun)?;
        let owned = run.selected_worker().as_ref() == Some(worker);
        // A run replaced by a retry or a replay is `Failed` or `Lost`, which no
        // report expects, so ownership and state are enough.
        if !owned || run.current_state() != expected {
            return Err(ReportRejection::NotAuthoritative);
        }
        Ok(run)
    }
}
