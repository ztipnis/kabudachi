//! How a task is submitted: the submission and its options, the checks that
//! refuse one, and how the leader records a new task and its first run.

use prost::Message as _;

use crate::coalescing;
use crate::manifest::{self, Manifest};
use crate::protocol::digest::Digest;
use crate::protocol::generated;
use crate::protocol::ids::{IdGenerator, TaskDefinitionId, TaskId, WorkerId, mint_task_id};
use crate::protocol::messages::prelude::*;
use crate::protocol::messages::{Task, TaskRun};
use crate::protocol::records::{NewTask, TaskRunRecord, first_attempt, new_task};
use crate::protocol::task::TaskRunState;
use crate::time::{Clock, Duration, Instant, WallTime};

use super::record_size::claim_weight_sum;
use super::{MAX_SUBMISSION_BYTES, Observer, Scheduler};

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
    #[error("this node is not the leader")]
    NotLeader,
    #[error("the task's record would be {size} bytes, past the {limit} a record may have")]
    RecordTooLarge { size: u64, limit: u64 },
    /// A generation of the submission's coalescing key has no record this
    /// leader can rely on yet, and may still run: submit again shortly.
    #[error("a generation of the task's coalescing key is not known yet")]
    KeyNotReady,
    /// The key's waiting generation would carry more than one claim can
    /// hold: further submissions of the key are refused until its chain is
    /// folded or claimed.
    #[error("the coalescing key's claim would be {size} bytes, past the {limit} one claim carries")]
    KeyBackpressure { size: u64, limit: u64 },
    /// The whole input is larger than the hard memory limit, so it could
    /// never fit (see [`Scheduler::set_memory_limits`]). Unlike
    /// `Backpressure`, asking again is refused again.
    #[error("a task input of {size} bytes is past the hard memory limit of {limit}")]
    PayloadTooLarge { size: u64, limit: u64 },
}

/// A submission given its task id and its submission time: what its client
/// holds from the moment it submits, before any leader has recorded it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Submitted {
    pub task_id: TaskId,
    pub submitted_at: WallTime,
    pub submission: Submission,
    /// The monotonic reading when it was minted. Its delay and expiry count
    /// from here, so a wall clock that jumps never shortens the wait. It means
    /// something only to the scheduler that minted it.
    minted_at: Instant,
}

impl Submitted {
    /// A submission whose id and submission time were fixed elsewhere, as
    /// it arrives here: its delay and expiry count from `clock`'s monotonic
    /// reading now, because the client's reading means nothing on this node.
    pub fn received(
        task_id: TaskId,
        submitted_at: WallTime,
        submission: Submission,
        clock: &impl Clock,
    ) -> Self {
        Submitted {
            task_id,
            submitted_at,
            submission,
            minted_at: clock.now(),
        }
    }
}

/// Gives `submission` a fresh task id from `ids` and stamps it with
/// `clock`'s wall-clock time now. Records nothing anywhere, so a client
/// mints its submission before it knows who leads, and asking again after a
/// refusal resubmits the same task.
pub fn mint(submission: Submission, ids: &impl IdGenerator, clock: &impl Clock) -> Submitted {
    Submitted {
        task_id: mint_task_id(ids),
        submitted_at: WallTime::now(clock),
        submission,
        minted_at: clock.now(),
    }
}

/// The W3C trace context a task carries from its submission to its record and
/// every claim of it, unchanged. Filled by tracing; empty until then.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TraceContext {
    pub trace_parent: String,
    pub trace_state: String,
    pub baggage: String,
}

impl TraceContext {
    /// The bytes the three fields take.
    pub fn len(&self) -> usize {
        self.trace_parent.len() + self.trace_state.len() + self.baggage.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
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
    /// How long after its worker falls silent a run of the task may be
    /// replayed, and so how long, less drift, a worker cut off from its
    /// leader has to abort it. `None`, or zero, leaves it to the shard's
    /// default (`ElectionTimings::reconnect_timeout`).
    pub reconnect_timeout: Option<Duration>,
    /// The worker that submitted the task and awaits its result: the
    /// worker that runs it delivers the result there, and the leader
    /// tells it how the task ended. `None` for a task nobody awaits
    /// across workers, as in the one-node runtime.
    pub client: Option<WorkerId>,
    /// The input as blobs, for one too large to travel inline:
    /// `serialized_input` is then empty. The blobs are stored before the
    /// submission is made.
    pub input_manifest: Option<Manifest>,
    /// The trace context to carry; empty for none.
    pub trace: TraceContext,
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
            reconnect_timeout: None,
            client: None,
            input_manifest: None,
            trace: TraceContext::default(),
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

    /// A run of the task may be replayed `timeout` after its worker falls
    /// silent, instead of after the shard's reconnect timeout.
    pub fn with_reconnect_timeout(mut self, timeout: Duration) -> Self {
        self.reconnect_timeout = Some(timeout);
        self
    }

    /// The task is awaited by `client`, which hears how it ends.
    pub fn with_client(mut self, client: WorkerId) -> Self {
        self.client = Some(client);
        self
    }

    /// The input travels as the blobs `manifest` names, already stored.
    pub fn with_input_manifest(mut self, manifest: Manifest) -> Self {
        self.serialized_input = Vec::new();
        self.input_manifest = Some(manifest);
        self
    }

    /// Carries `trace` unchanged to the task's record and every claim of it.
    #[must_use]
    pub fn with_trace_context(mut self, trace: TraceContext) -> Self {
        self.trace = trace;
        self
    }

    /// What the input weighs: its whole length, inline or as blobs.
    pub fn payload_bytes(&self) -> u64 {
        self.input_manifest
            .as_ref()
            .map_or(self.serialized_input.len() as u64, Manifest::total_bytes)
    }
}

impl Submission {
    /// What a claim of the submitted task weighs before its chain: its
    /// input, queue, definition id, coalescing key and trace context.
    pub(super) fn claim_weight(&self) -> u64 {
        claim_weight_sum(
            self.carried_bytes(),
            &self.queue,
            self.definition_id.as_str(),
            self.coalescing_key.as_deref(),
            self.trace.len() as u64,
        )
    }

    /// What a claim of this task carries for its input: the bytes, or the
    /// encoded manifest.
    fn carried_bytes(&self) -> u64 {
        self.serialized_input.len() as u64
            + self.input_manifest.as_ref().map_or(0, |manifest| {
                generated::Manifest::from(manifest.clone()).encoded_len() as u64
            })
    }
}

impl<C: Clock, I: IdGenerator, O: Observer> Scheduler<C, I, O> {
    /// Gives `submission` a fresh task id and stamps it with the wall-clock
    /// time now, noting the monotonic time too. Records nothing, so it needs no leadership.
    pub fn mint(&self, submission: Submission) -> Submitted {
        mint(submission, &self.ids, &self.clock)
    }

    /// Whether `submission` could be recorded now with `queued_bytes` more
    /// input already promised to submissions not yet recorded: it is not too
    /// large to claim, and it fits under the hard limit with them. Records
    /// nothing, so it needs no leadership.
    pub fn check_submission(
        &self,
        submission: &Submission,
        queued_bytes: u64,
    ) -> Result<(), SubmitRejection> {
        let weight = submission.payload_bytes();
        if let Some(limits) = self.budget.limits()
            && weight > limits.hard
        {
            // It could never fit, however much is released: refused for good,
            // where `check_room` below refuses one that does not fit now.
            return Err(SubmitRejection::PayloadTooLarge {
                size: weight,
                limit: limits.hard,
            });
        }
        if submission.input_manifest.is_some() && submission.coalescing_key.is_some() {
            // A coalescing generation's payload travels in one claim with its
            // chain, so it can never be carried as blobs.
            return Err(SubmitRejection::TooLarge {
                size: weight,
                limit: MAX_SUBMISSION_BYTES,
            });
        }
        // The carried input (bytes or encoded manifest), queue, definition id,
        // key and trace context: the shared claim-weight sum.
        let size = submission.claim_weight();
        if size > MAX_SUBMISSION_BYTES {
            return Err(SubmitRejection::TooLarge {
                size,
                limit: MAX_SUBMISSION_BYTES,
            });
        }
        if let Some(key) = submission.coalescing_key.as_deref() {
            // The submission would wait with the key's retained payloads as
            // its chain, and a claim carries all of it.
            let key = coalescing::key(&submission.definition_id, key);
            let claim_size = self.claim_bytes(size, self.occupancy.retained(&key).iter());
            if claim_size > MAX_SUBMISSION_BYTES {
                return Err(SubmitRejection::KeyBackpressure {
                    size: claim_size,
                    limit: MAX_SUBMISSION_BYTES,
                });
            }
        }
        self.check_room(submission, weight + queued_bytes)
    }

    /// Records a new Task and queues its first run, under the id and
    /// submission time `submitted` carries. Its delay and expiry count from
    /// when it was minted, by the monotonic clock: time passed since then,
    /// including time spent waiting for a leader, is taken off them, and the
    /// wall clock never shortens them. Only a leader records a submission.
    ///
    /// A task id it already holds is a client asking again after a refusal:
    /// it records nothing and answers the id, so a retry never makes a second
    /// task. A task it has forgotten (past its result's time to live) that is
    /// submitted again is recorded afresh.
    pub fn submit_minted(&mut self, submitted: Submitted) -> Result<TaskId, SubmitRejection> {
        let outcome = self.record_submission(submitted);
        self.end_call();
        outcome
    }

    /// [`Self::mint`] then [`Self::submit_minted`]: a submission made where
    /// the leader is. Every call is a new Task (submission is not idempotent).
    pub fn submit(&mut self, submission: Submission) -> Result<TaskId, SubmitRejection> {
        let submitted = self.mint(submission);
        self.submit_minted(submitted)
    }

    /// Records `submitted` as a new task with its first run, unless it is
    /// already held (a retry after a refusal) or is refused.
    fn record_submission(&mut self, submitted: Submitted) -> Result<TaskId, SubmitRejection> {
        if !self.check_leader() {
            return Err(SubmitRejection::NotLeader);
        }
        if self.tasks.contains_key(&submitted.task_id) {
            return Ok(submitted.task_id);
        }
        self.check_submission(&submitted.submission, 0)?;
        self.check_key_ready(&submitted.submission)?;
        let now = self.clock.now();
        let Submitted {
            task_id,
            submitted_at,
            submission,
            minted_at,
        } = submitted;
        let (not_before, expires_at) = wait_bounds(&submission, minted_at, now);
        let input_digest = submission
            .input_manifest
            .as_ref()
            .map_or_else(|| Digest::blake3(&submission.serialized_input), Manifest::digest);
        let task = task_of(task_id, submitted_at, submission);
        let run = self.first_run_within_limit(&task, not_before.is_some(), &input_digest)?;
        let task_id = self.hold_submitted(task, run, input_digest);
        self.waiting
            .admit(&task_id, &self.tasks[&task_id].queue, now, not_before, expires_at);
        self.absorb_waiting_generation(&task_id);
        self.drop_oldest_until_it_fits(&task_id, now);
        self.update_pressure();
        if let Some(key) = self.coalescing_key_of(&task_id) {
            self.compact_if_due(&key);
        }
        Ok(task_id)
    }

    /// Refuses a submission to a coalescing key one of whose generations is
    /// held back: no generation of a key may run while one might already be.
    fn check_key_ready(&self, submission: &Submission) -> Result<(), SubmitRejection> {
        let Some(key) = submission.coalescing_key.as_deref() else {
            return Ok(());
        };
        if self
            .reconciliation
            .holds_key_back(&coalescing::key(&submission.definition_id, key))
        {
            return Err(SubmitRejection::KeyNotReady);
        }
        Ok(())
    }

    /// The first run of `task`, scheduled if it waits out a delay and queued
    /// otherwise, unless the task's record would then be larger than a record
    /// may be.
    fn first_run_within_limit(
        &self,
        task: &Task,
        delayed: bool,
        input_digest: &Digest,
    ) -> Result<TaskRun, SubmitRejection> {
        let stamped_at = WallTime::now(&self.clock);
        let first_state = if delayed {
            TaskRunState::Scheduled
        } else {
            TaskRunState::Queued
        };
        let run = first_attempt(task, &self.ids, stamped_at, first_state);
        let size = self.submitted_record_len(task, &run, input_digest);
        if size > Self::record_limit() {
            return Err(SubmitRejection::RecordTooLarge {
                size,
                limit: Self::record_limit(),
            });
        }
        Ok(run)
    }

    /// Holds a newly submitted `task` with `run`, its first, and counts its
    /// input against memory. Returns its id.
    fn hold_submitted(&mut self, task: Task, run: TaskRun, input_digest: Digest) -> TaskId {
        let task_id = task.task_id();
        let needed = manifest::payload_bytes(&task);
        self.current_run.insert(task_id.clone(), run.task_run_id());
        self.runs_of_task
            .insert(task_id.clone(), vec![run.task_run_id()]);
        self.runs.insert(run.task_run_id(), run);
        self.tasks.insert(task_id.clone(), task);
        self.input_digests.insert(task_id.clone(), input_digest);
        self.unpublished.insert(task_id.clone());
        self.mark_current_decided(&task_id);
        self.budget.take(needed);
        task_id
    }

    /// The generation `task_id` supersedes the one of its coalescing key that
    /// was waiting, if any, and absorbs it with everything it had absorbed.
    fn absorb_waiting_generation(&mut self, task_id: &TaskId) {
        let Some(older) = self
            .coalescing_key_of(task_id)
            .and_then(|key| self.occupancy.submit(&key, task_id))
        else {
            return;
        };
        self.supersede(&older, task_id);
        let mut absorbed = self
            .links
            .get(&older)
            .map(|link| link.absorbed.clone())
            .unwrap_or_default();
        absorbed.push(older.into());
        self.links.entry(task_id.clone()).or_default().absorbed = absorbed;
    }
}

/// When a task minted at `minted_at` may first start, if not at once, and
/// when it expires, both by the monotonic clock. Time spent waiting for a
/// leader since it was minted is taken off both.
fn wait_bounds(
    submission: &Submission,
    minted_at: Instant,
    now: Instant,
) -> (Option<Instant>, Option<Instant>) {
    let not_before = submission
        .delay
        .filter(|delay| delay.as_ticks() > 0)
        .map(|delay| minted_at + delay)
        .filter(|at| *at > now);
    let expires_at = submission.expiry.map(|expiry| minted_at + expiry);
    (not_before, expires_at)
}

/// The task `submission` makes, under `task_id` and submitted at
/// `submitted_at`.
fn task_of(task_id: TaskId, submitted_at: WallTime, submission: Submission) -> Task {
    new_task(NewTask {
        task_id,
        submitted_at,
        definition_id: submission.definition_id,
        source_version: submission.source_version,
        serialized_input: submission.serialized_input,
        queue: submission.queue,
        max_retries: submission.retries,
        delay: submission.delay,
        expiry: submission.expiry,
        coalescing_key: submission.coalescing_key,
        ephemeral: submission.ephemeral,
        non_retriable: submission.non_retriable,
        reconnect_timeout: submission.reconnect_timeout,
        client: submission.client,
        input_manifest: submission.input_manifest,
        trace: submission.trace,
    })
}
