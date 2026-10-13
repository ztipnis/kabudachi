//! The seam between the driver and Python: the runs, events and waits that
//! pass between them, behind one lock and one wake-up.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration as StdDuration;

use kabudachi_core::protocol::ids::{TaskId, TaskRunId, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::scheduler::Claim;
use kabudachi_net::executor::{Report, ReportSink};
use kabudachi_net::messenger::Net;
use kabudachi_net::payload_memory::Taken;
use kabudachi_net::task_exchange::LeaderRetry;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use tokio::sync::{Notify, watch};

use super::observed::Observed;
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
    /// yet takeable.
    pub(super) resolving: BTreeSet<TaskRunId>,
    /// The inputs of runs handed over that travelled as blobs, counted in
    /// flight until each run ends.
    pub(super) inputs_in_flight: BTreeMap<TaskRunId, Taken>,
    /// Every run Python took and has not ended.
    pub(super) held: BTreeMap<TaskRunId, HeldRun>,
    pub(super) events: VecDeque<PyEvent>,
    /// Tasks submitted here whose submission no leader has answered yet: a
    /// cancel of one waits, so it never reaches a leader before the task.
    pub(super) delivering: BTreeSet<TaskId>,
    /// Tasks submitted here whose outcome has not come back.
    pub(super) awaiting: BTreeSet<TaskId>,
    /// Nothing more is taken: a run handed over is reported lost at once.
    pub(super) closed: bool,
}

/// A run Python took and has not ended: its task, and where its result goes.
pub(super) struct HeldRun {
    pub(super) task: TaskId,
    /// The worker awaiting the result; `None` for a task nobody awaits.
    pub(super) client: Option<WorkerId>,
    /// How long the result may take to reach the client: the run's reconnect timeout.
    pub(super) reconnect_timeout: StdDuration,
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
}

impl Shared {
    pub(super) fn new(
        reports: ReportSink,
        net: Arc<Net>,
        observed: watch::Receiver<Observed>,
        retry: LeaderRetry,
    ) -> Self {
        Shared {
            seam: Mutex::default(),
            changed: Notify::new(),
            reports,
            net,
            observed,
            retry,
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
            let freed = self.seam().inputs_in_flight.remove(run);
            drop(freed);
        }
        let _ = self.reports.report(report);
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
            let waiting = u32::try_from(seam.handed.len()).unwrap_or(u32::MAX);
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
                client: claim.task.client(),
                reconnect_timeout: StdDuration::from_millis(claim.reconnect_timeout.as_ticks()),
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
            unstarted.extend(std::mem::take(&mut seam.resolving));
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
