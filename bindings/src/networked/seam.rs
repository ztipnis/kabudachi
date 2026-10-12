use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use kabudachi_core::protocol::ids::{TaskId, TaskRunId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::scheduler::Claim;
use kabudachi_net::executor::{Report, ReportSink};
use kabudachi_net::messenger::Net;
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
    /// The task of every run Python took and has not ended.
    pub(super) held: BTreeMap<TaskRunId, TaskId>,
    pub(super) events: VecDeque<PyEvent>,
    /// Tasks submitted here whose submission no leader has answered yet: a
    /// cancel of one waits, so it never reaches a leader before the task.
    pub(super) delivering: BTreeSet<TaskId>,
    /// Nothing more is taken: a run handed over is reported lost at once.
    pub(super) closed: bool,
}

pub(super) struct Shared {
    pub(super) seam: Mutex<Seam>,
    /// Woken whenever a claim or an event may be waiting.
    pub(super) changed: Notify,
    pub(super) reports: ReportSink,
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
    /// worker leaving its shard, which every wait reports.
    pub(super) fn report(&self, report: Report) {
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
            seam.held
                .insert(claim.task_run_id.clone(), claim.task.task_id());
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

    /// Ends a run Python held; `false` if it held none (the driver took it
    /// back), and the report is then dropped.
    pub(super) fn end(&self, run: &TaskRunId) -> bool {
        self.seam().held.remove(run).is_some()
    }

    /// Ends a run Python held and hands the driver `report` on it; nothing
    /// if Python held none (the driver took it back).
    pub(super) fn finish(&self, run: TaskRunId, report: impl FnOnce(TaskRunId) -> Report) {
        if self.end(&run) {
            self.report(report(run));
        }
    }

    /// Takes no more runs: those handed over and not taken are reported
    /// lost, later ones too, and a claim waiting resolves empty.
    pub(super) fn stop_taking(&self) {
        let unstarted: Vec<TaskRunId> = {
            let mut seam = self.seam();
            seam.closed = true;
            seam.handed
                .drain(..)
                .map(|claim| claim.task_run_id)
                .collect()
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
