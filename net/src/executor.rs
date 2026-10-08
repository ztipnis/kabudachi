//! The seam between a worker's driver and the executor that runs its
//! TaskRuns: two channels, made by [`executor_channel`]. The driver holds the
//! [`ExecutorEndpoint`] (see `crate::worker::WorkerConfig::with_executor`); the
//! executor holds the [`ExecutorHandle`].
//!
//! ## What the executor sends: [`Report`]
//!
//! `Capacity(n)` offers `n` more places. The driver claims work while places
//! it was offered are not taken, one per run it hands over, and the executor
//! offers a place again once a run has ended. An offer is a credit, never a
//! count of free places, so an offer and a run crossing on the two channels
//! never make the driver claim more than the executor can hold.
//!
//! Per run, in order: `Started` once the body runs (a compaction run sends
//! none), then exactly one of `Completed` (with the digest of its result),
//! `Failed` (with the error's type name), `Lost` (the process running it died
//! or was killed at an abort deadline, so no outcome is known), or, for a
//! compaction run, `Compacted` (the fold). The driver takes each report to
//! the leader in that order, asking again as leaders change, until one takes
//! it.
//!
//! ## What the driver sends: [`Work`]
//!
//! `Run(claim)` hands over a run the leader granted: run the task's body.
//! `Compact(claim)` hands over a compaction run: fold the claim's chain,
//! oldest first, with the task's merge. `Cancel(run)`: the run is no longer
//! this worker's to finish; stop its body cooperatively and report however it
//! ends.
//!
//! `Abort { run, deadline }`: the worker cannot show that its leader still
//! hears it, so another leader may replay the run once `deadline` has passed,
//! on this host's monotonic clock. The executor cancels the body
//! cooperatively at `deadline` less its cancel grace (at once if that has
//! passed), and, if the body still runs at `deadline`, kills the process
//! running it. Runs sharing that process die with it, which is safe: they
//! share the lost contact, and so the deadline. A run killed this way is
//! reported `Lost`. A later `Abort` for the same run replaces its deadline.
//! `AbortWithdrawn(run)`: the leader hears the worker again before the
//! deadline; drop the pending abort and let the run go on.
//!
//! The driver claims nothing until some leader has vouched for hearing this
//! worker (see `WorkerNode::has_contact_floor`), so every run it hands over
//! has an abort deadline it could name, and nothing while an abort deadline
//! stands. While this worker leads, it claims from its own scheduler and
//! decides its own reports there, each held, like any other decision, until a
//! quorum of the task's placement has stored it.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::Instant;

use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::ids::TaskRunId;
use kabudachi_core::protocol::messages::{Claim, task_request};
use tokio::sync::mpsc;

/// What the driver hands the executor (see the module doc).
#[derive(Debug, Clone, PartialEq)]
pub enum Work {
    Run(Claim),
    Compact(Claim),
    Cancel(TaskRunId),
    Abort { run: TaskRunId, deadline: Instant },
    AbortWithdrawn(TaskRunId),
}

/// What the executor tells the driver (see the module doc).
#[derive(Debug, Clone, PartialEq)]
pub enum Report {
    Capacity(u32),
    Started(TaskRunId),
    Completed { run: TaskRunId, digest: Digest },
    Failed { run: TaskRunId, reason: String },
    Lost(TaskRunId),
    Compacted { run: TaskRunId, payload: Vec<u8> },
}

/// The driver's end of the seam, with what the driver knows of its executor
/// between runs of the driver.
pub struct ExecutorEndpoint {
    pub(crate) work: mpsc::UnboundedSender<Work>,
    pub(crate) reports: mpsc::UnboundedReceiver<Report>,
    pub(crate) held: Held,
}

/// The executor's end of the seam.
pub struct ExecutorHandle {
    work: mpsc::UnboundedReceiver<Work>,
    reports: mpsc::UnboundedSender<Report>,
}

/// The driver holding the other end has stopped, and dropped it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DriverGone;

/// A new seam: the endpoint for the worker's driver, the handle for its
/// executor. Neither channel is bounded: the driver hands over no more runs
/// than the executor offered places for, and the executor reports on no more.
pub fn executor_channel() -> (ExecutorEndpoint, ExecutorHandle) {
    let (work_sender, work) = mpsc::unbounded_channel();
    let (reports_sender, reports) = mpsc::unbounded_channel();
    (
        ExecutorEndpoint {
            work: work_sender,
            reports,
            held: Held::default(),
        },
        ExecutorHandle {
            work,
            reports: reports_sender,
        },
    )
}

impl ExecutorHandle {
    /// The next work the driver hands over, in the order it was handed;
    /// `None` once the driver has stopped and nothing is left.
    pub async fn next_work(&mut self) -> Option<Work> {
        self.work.recv().await
    }

    /// Tells the driver `report`.
    pub fn report(&self, report: Report) -> Result<(), DriverGone> {
        self.reports.send(report).map_err(|_| DriverGone)
    }
}

/// A report on one of this worker's runs, as the leader is asked to take it.
pub(crate) type RunReport = task_request::Request;

/// What the driver keeps of its executor across runs of the driver.
#[derive(Default)]
pub(crate) struct Held {
    /// Places the executor offered that no run has taken.
    pub(crate) credits: u32,
    /// The runs handed over whose end the executor has not reported.
    pub(crate) handed: BTreeSet<TaskRunId>,
    /// The runs of `handed` the executor was already told to stop.
    pub(crate) cancelled: BTreeSet<TaskRunId>,
    /// Per run, the reports no leader has taken yet, oldest first.
    pub(crate) waiting: BTreeMap<TaskRunId, VecDeque<RunReport>>,
    /// The executor dropped its handle: nothing more is claimed for it.
    pub(crate) gone: bool,
}
