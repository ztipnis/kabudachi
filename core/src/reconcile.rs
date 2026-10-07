//! What a new leader learns from its shard before it schedules anything:
//! every worker reports the runs it holds, each with the claim it was granted
//! under, and a summary of every Task record it holds, page by page (see
//! [`wire`] for the messages and their limits).
//!
//! A [`ReconcileRound`] collects the answers, asks for the full records the
//! summaries show the leader lacks, and decides two things. A task's newest
//! record is known once enough of its holders have answered that a silent one
//! cannot be hiding a newer revision, or once every holder still in the shard's
//! configuration has; a task short of that is uncertain and the scheduler
//! holds it back. And the leader may stop collecting once every voter has
//! answered, or a quorum has and a suspicion timeout has passed. What the
//! round hands over, a [`Rebuild`], is what the scheduler rebuilds from, and
//! what it adopts as answers come late.
//!
//! Once the leader leads, a worker's heartbeats carry a digest of the runs it
//! holds ([`active_runs_digest`]). A [`DriftWatch`] tells the leader when its
//! own view of a worker has disagreed with that digest for long enough to ask
//! the worker what it holds again.

mod alone;
mod drift;
mod round;
pub mod wire;

pub use alone::reconcile_alone;
pub use drift::DriftWatch;
pub use round::{Cursor, ReconcileRound};

use std::collections::{BTreeMap, BTreeSet};

use crate::coordination_authority::RecoveryEpoch;
use crate::protocol::digest::Digest;
use crate::protocol::generated::TaskRecord;
use crate::protocol::ids::{TaskDefinitionId, TaskId, TaskRunId, WorkerId};
use crate::scheduler::Claim;
use crate::task_record::RecordVersion;
use crate::time::{Duration, Instant};

/// The office a leader reconciles for: its recovery epoch and term.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReconcileTerm {
    pub recovery_epoch: RecoveryEpoch,
    pub term: u64,
}

/// What a worker last did to a run it holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReportedState {
    Claimed,
    Running,
    /// Succeeded with this digest; no leader has certified it.
    Succeeded {
        result_digest: Digest,
    },
    /// Failed; no leader has recorded the failure.
    Failed {
        failure_kind: String,
    },
}

/// A run a worker reports, with the claim it was granted under (its chain
/// left out).
#[derive(Debug, Clone, PartialEq)]
pub struct ReportedRun {
    pub claim: Claim,
    pub state: ReportedState,
}

/// The coalescing key a task belongs to: the generations of one key replace
/// each other, so their fates are decided together.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoalescingKey {
    pub definition: TaskDefinitionId,
    pub key: String,
}

/// One record a worker holds, summarised.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeldKey {
    pub task_id: TaskId,
    pub version: RecordVersion,
    pub input_digest: Option<Digest>,
    pub latest_run: Option<TaskRunId>,
    pub placement: Vec<WorkerId>,
    pub finished: bool,
    /// The coalescing key of the task, unset when it has none.
    pub coalescing: Option<CoalescingKey>,
}

/// One page of a worker's report.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ReportPage {
    pub runs: Vec<ReportedRun>,
    pub keys: Vec<HeldKey>,
    pub last: bool,
}

/// The runs one worker reported, and, for a report asked for while this
/// leader already leads, when it was asked (see [`ANSWER_IN_FLIGHT`]).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct WorkerRuns {
    pub runs: Vec<ReportedRun>,
    pub asked_at: Option<Instant>,
}

/// What a leader rebuilds its scheduler from, or, once it leads, what it
/// has learnt since.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Rebuild {
    /// The newest record of every task known for certain.
    pub records: Vec<TaskRecord>,
    /// Tasks some worker holds whose newest record cannot be known yet, with
    /// the run ids known to be theirs.
    pub uncertain: BTreeMap<TaskId, BTreeSet<TaskRunId>>,
    /// The coalescing key of each uncertain task that has one, so the leader
    /// can hold the key's other generations back with it.
    pub uncertain_keys: BTreeMap<TaskId, CoalescingKey>,
    /// The runs each worker that answered reported. A worker missing here
    /// did not answer.
    pub reports: BTreeMap<WorkerId, WorkerRuns>,
}

/// How many of a leader's voters have answered its reconciliation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Answered {
    /// Every voter of its configuration (of both sides, for a joint one).
    All,
    /// A quorum, not all.
    Quorum,
    /// Short of a quorum.
    Short,
}

/// The longest an answer a leader released can take to reach a worker
/// that asked for it: a worker gives up on its request after this. A run
/// decided more recently than this before a worker was asked what it holds
/// may simply not have reached it yet, so it is not lost for being missing
/// from the answer.
pub const ANSWER_IN_FLIGHT: Duration = Duration::from_millis(10_000);

/// Added to a task's delay and expiry when a leader turns the wall-clock
/// times in its record into deadlines of its own clock: workers' clocks
/// disagree, and a delay or expiry may come late but never early.
pub const RECONCILE_SKEW_MARGIN: Duration = Duration::from_millis(1_000);

/// The digest of the runs a worker holds, as its heartbeats carry it and as
/// its leader computes it from its own view: BLAKE3 over the run ids,
/// sorted, each prefixed by its length. Equal sets give equal digests,
/// whoever computes them.
pub fn active_runs_digest<'a>(runs: impl IntoIterator<Item = &'a TaskRunId>) -> Digest {
    let mut sorted: Vec<&str> = runs.into_iter().map(TaskRunId::as_str).collect();
    sorted.sort_unstable();
    let mut bytes = Vec::new();
    for run in sorted {
        bytes.extend_from_slice(&(run.len() as u64).to_be_bytes());
        bytes.extend_from_slice(run.as_bytes());
    }
    Digest::blake3(&bytes)
}
