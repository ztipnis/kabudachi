//! The seam between the driver and Python: the runs, events and waits that
//! pass between them, behind one lock and one wake-up.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration as StdDuration;

use kabudachi_core::protocol::ids::{CompositeId, TaskId, TaskRunId, WorkerId};
use kabudachi_core::protocol::messages::Task;
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::result_backend::ResultBackend;
use kabudachi_core::scheduler::Claim;
use kabudachi_net::executor::{Report, ReportSink};
use kabudachi_net::messenger::Net;
use kabudachi_net::payload_memory::Taken;
use kabudachi_net::task_exchange::LeaderRetry;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use tokio::runtime::Handle;
use tokio::sync::{Notify, watch};
use tokio::task::AbortHandle;

use super::observed::Observed;
use super::slow_down::SlowDownWatch;
use crate::result_backend::Mirror;
use crate::work::{PyClaim, PyEvent};

pub(super) const GONE_MESSAGE: &str = "the worker left its shard";

/// What passes between the driver and Python.
#[derive(Default)]
pub(super) struct Seam {
    /// Places offered to the driver that no run has used yet.
    pub(super) offered: u32,
    /// Runs handed over that Python has not taken, oldest first.
    pub(super) handed: VecDeque<Claim>,
    /// Runs handed over whose input's blobs are still being fetched: handed
    /// over as far as a cancel or an abort deadline is concerned, but not
    /// yet takeable. Each holds the handle that stops its fetch.
    pub(super) resolving: BTreeMap<TaskRunId, AbortHandle>,
    /// The inputs of runs handed over that travelled as blobs, and the results
    /// a member run read as its input, counted in flight until each run ends.
    pub(super) inputs_in_flight: BTreeMap<TaskRunId, Vec<Taken>>,
    /// Every run Python took and has not ended.
    pub(super) held: BTreeMap<TaskRunId, HeldRun>,
    pub(super) events: VecDeque<PyEvent>,
    /// Tasks submitted here whose submission no leader has answered yet: a
    /// cancel of one waits, so it never reaches a leader before the task.
    pub(super) delivering: BTreeSet<TaskId>,
    /// Tasks submitted here whose outcome has not come back.
    pub(super) awaiting: BTreeSet<TaskId>,
    /// Composites submitted here whose submission no leader has answered
    /// yet: a cancel of one waits, as for a task.
    pub(super) delivering_composites: BTreeSet<CompositeId>,
    /// Composites submitted here, and continuations of tasks submitted here,
    /// that have not been told finished.
    pub(super) awaiting_composites: BTreeSet<CompositeId>,
    /// Composites submitted here whose submission a leader refused for good:
    /// members sent for one are answered as for a composite that is over.
    pub(super) refused_composites: BTreeSet<CompositeId>,
    /// Nothing more is taken: a run handed over is reported lost at once.
    pub(super) closed: bool,
    /// Runs reported completed whose result is mirrored once the leader
    /// certifies them, until the leader answers the report.
    /// Each holds a copy of the run's result until the leader answers, the
    /// report is abandoned, or the run ends another way.
    pub(super) mirroring: BTreeMap<TaskRunId, Mirror>,
    /// Mirrors being stored now, after their report was answered.
    pub(super) mirrors_running: usize,
}

/// A run Python took and has not ended: its task, and where its result goes.
pub(super) struct HeldRun {
    pub(super) task: TaskId,
    /// The task's own result retention in milliseconds (0: keep nothing).
    pub(super) result_ttl_ms: Option<u64>,
    /// The worker awaiting the result; `None` for a task nobody awaits.
    pub(super) client: Option<WorkerId>,
    /// How long the result may take to reach the client: the run's reconnect timeout.
    pub(super) reconnect_timeout: StdDuration,
    /// The run as the leader gave it, kept for a member of a composite or an
    /// expansion, whose input is read from the composite's records.
    pub(super) member: Option<Task>,
}

pub(super) struct Shared {
    seam: Mutex<Seam>,
    /// Woken whenever a claim or an event may be waiting.
    changed: Notify,
    reports: ReportSink,
    pub(super) net: Arc<Net>,
    pub(super) observed: watch::Receiver<Observed>,
    /// How submissions and cancels keep asking the leader.
    pub(super) retry: LeaderRetry,
    /// What the leader last said about SlowDown, and the poll that waits for
    /// it to clear.
    pub(super) slow_down: Mutex<SlowDownWatch>,
    /// Where certified results are mirrored; `None` mirrors nothing.
    pub(super) backend: Option<Arc<dyn ResultBackend>>,
    /// How long the shard keeps a finished task, which is how long a mirrored
    /// result is kept unless its task chose its own.
    pub(super) result_ttl_ms: u64,
    /// The worker's runtime, for the mirrors it starts from the pump.
    pub(super) handle: Handle,
}

impl Shared {
    pub(super) fn new(
        reports: ReportSink,
        net: Arc<Net>,
        observed: watch::Receiver<Observed>,
        retry: LeaderRetry,
        backend: Option<Arc<dyn ResultBackend>>,
        result_ttl_ms: u64,
        handle: Handle,
    ) -> Self {
        Shared {
            seam: Mutex::default(),
            changed: Notify::new(),
            reports,
            net,
            observed,
            retry,
            slow_down: Mutex::default(),
            backend,
            result_ttl_ms,
            handle,
        }
    }

    pub(super) fn seam(&self) -> MutexGuard<'_, Seam> {
        self.seam.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Hands `report` to the driver. A driver that has stopped shows as the
    /// worker leaving its shard, which every wait reports. A report that
    /// ends a run gives back the room its input held.
    pub(super) fn report(&self, report: Report) {
        if let Report::Completed { run, .. }
        | Report::Failed { run, .. }
        | Report::Lost(run)
        | Report::Compacted { run, .. } = &report
        {
            // Dropped after the lock is released, which wakes the claims
            // waiting for room.
            let freed = {
                let mut seam = self.seam();
                // A run that ends any way but certified has nothing to mirror.
                if !matches!(&report, Report::Completed { .. }) {
                    seam.mirroring.remove(run);
                }
                seam.inputs_in_flight.remove(run)
            };
            drop(freed);
        }
        let completed = match &report {
            Report::Completed { run, .. } => Some(run.clone()),
            _ => None,
        };
        // A report the driver cannot take is never answered: nothing to mirror.
        if self.reports.report(report).is_err()
            && let Some(run) = completed
        {
            self.seam().mirroring.remove(&run);
        }
    }

    /// Hands the driver a report that this worker cannot run `task` at its
    /// input version. The run Python held for it, if any, is over: it ends
    /// here and gives back the room its input held.
    pub(super) fn report_blocked(&self, task: TaskId, reason: String, incompatible: bool) {
        let freed = {
            let mut seam = self.seam();
            // The leader has one current run of a task, and this worker holds
            // at most that one, so the first match is the only one.
            let run = seam
                .held
                .iter()
                .find_map(|(run, held)| (held.task == task).then(|| run.clone()));
            run.and_then(|run| {
                seam.held.remove(&run);
                seam.inputs_in_flight.remove(&run)
            })
        };
        drop(freed);
        let _ = self.reports.report(Report::Blocked { task, reason, incompatible });
    }

    /// Counts `taken`, the payloads a member run `run` read as its input, in
    /// flight until the run ends. A run that ended meanwhile counts nothing.
    pub(super) fn hold_inputs(&self, run: &TaskRunId, taken: Vec<Taken>) {
        let mut seam = self.seam();
        if seam.held.contains_key(run) {
            seam.inputs_in_flight.entry(run.clone()).or_default().extend(taken);
        }
    }

    /// Whether a result is waiting for its report's answer to be mirrored, or
    /// is being mirrored now.
    pub(super) fn mirrors_pending(&self) -> bool {
        let seam = self.seam();
        !seam.mirroring.is_empty() || seam.mirrors_running > 0
    }

    pub(super) fn wake(&self) {
        self.changed.notify_waiters();
    }

    /// Offers the driver the places `free` has that are neither offered nor
    /// held by a run Python has not taken.
    pub(super) fn offer(&self, free: usize) {
        let more = {
            let mut seam = self.seam();
            if seam.closed {
                return;
            }
            let free = u32::try_from(free).unwrap_or(u32::MAX);
            let waiting = u32::try_from(seam.handed.len() + seam.resolving.len()).unwrap_or(u32::MAX);
            let more = free.saturating_sub(seam.offered.saturating_add(waiting));
            seam.offered += more;
            more
        };
        if more > 0 {
            self.report(Report::Capacity(more));
        }
    }

    /// Up to `limit` runs handed over, now held by Python; `None` while
    /// there are none, and none at all once nothing more is taken.
    pub(super) fn take_claims(&self, limit: usize) -> Option<Vec<PyClaim>> {
        let mut seam = self.seam();
        if seam.closed {
            return Some(Vec::new());
        }
        if seam.handed.is_empty() {
            return None;
        }
        let count = limit.min(seam.handed.len());
        let claims: Vec<Claim> = seam.handed.drain(..count).collect();
        for claim in &claims {
            let held = HeldRun {
                task: claim.task.task_id(),
                result_ttl_ms: claim.task.result_ttl_ms,
                client: claim.task.client(),
                reconnect_timeout: StdDuration::from_millis(claim.reconnect_timeout.as_ticks()),
                member: claim.task.composite_id().is_some().then(|| claim.task.clone()),
            };
            seam.held.insert(claim.task_run_id.clone(), held);
        }
        Some(claims.into_iter().map(PyClaim::from).collect())
    }

    pub(super) fn take_events(&self) -> Option<Vec<PyEvent>> {
        let mut seam = self.seam();
        (!seam.events.is_empty()).then(|| seam.events.drain(..).collect())
    }

    pub(super) fn holds(&self, run: &TaskRunId) -> bool {
        self.seam().held.contains_key(run)
    }

    /// The task a composite's member run is, and how long its reads may be
    /// retried; `None` for a run Python does not hold, or one that is no
    /// member.
    pub(super) fn member_of(&self, run: &TaskRunId) -> Option<(Task, StdDuration)> {
        let seam = self.seam();
        let held = seam.held.get(run)?;
        Some((held.member.clone()?, held.reconnect_timeout))
    }

    /// Ends a run Python held, and returns it; `None` if it held none (the
    /// driver took it back), and the report is then dropped.
    pub(super) fn end(&self, run: &TaskRunId) -> Option<HeldRun> {
        self.seam().held.remove(run)
    }

    /// Ends a run Python held and hands the driver `report` on it; nothing
    /// if Python held none (the driver took it back).
    pub(super) fn finish(&self, run: TaskRunId, report: impl FnOnce(TaskRunId) -> Report) {
        if self.end(&run).is_some() {
            self.report(report(run));
        }
    }

    /// Takes no more runs: those handed over and not taken, or still having
    /// their blobs fetched, are reported lost, later ones too, and a claim
    /// waiting resolves empty.
    pub(super) fn stop_taking(&self) {
        let unstarted: Vec<TaskRunId> = {
            let mut seam = self.seam();
            seam.closed = true;
            let mut unstarted: Vec<TaskRunId> = seam
                .handed
                .drain(..)
                .map(|claim| claim.task_run_id)
                .collect();
            for (run, fetch) in std::mem::take(&mut seam.resolving) {
                fetch.abort();
                unstarted.push(run);
            }
            unstarted
        };
        for run in unstarted {
            self.report(Report::Lost(run));
        }
        self.wake();
    }

    /// Waits until `take` finds something, or the worker leaves its shard.
    pub(super) async fn until<T>(&self, take: impl Fn(&Shared) -> Option<T>) -> PyResult<T> {
        let mut observed = self.observed.clone();
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(found) = take(self) {
                return Ok(found);
            }
            if observed.borrow_and_update().gone {
                return Err(PyRuntimeError::new_err(GONE_MESSAGE));
            }
            tokio::select! {
                () = &mut notified => {}
                changed = observed.changed() => {
                    if changed.is_err() {
                        return Err(PyRuntimeError::new_err(GONE_MESSAGE));
                    }
                }
            }
        }
    }
}
