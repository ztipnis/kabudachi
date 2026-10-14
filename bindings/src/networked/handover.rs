//! How each piece of work the driver hands over changes the seam: a run to
//! hold for Python, a run taken back, an abort deadline, or one withdrawn.

use std::time::Instant as StdInstant;

use kabudachi_core::protocol::ids::TaskRunId;
use kabudachi_core::protocol::messages::Claim as WireClaim;
use kabudachi_core::scheduler::Claim;
use kabudachi_net::executor::{Report, Work};

use super::seam::{Seam, Shared};
use crate::outcomes::PyEventKind;
use crate::work::PyEvent;

/// How a run the driver took back is reported: its body is being stopped,
/// and nothing it ends with counts.
const CANCELLED_KIND: &str = "TaskCancelledError";
const MALFORMED_KIND: &str = "MalformedClaim";

/// What one piece of work leaves to do once the seam is unlocked: a report
/// for the driver, and whether a waiting claim or event wait should look
/// again.
#[derive(Default)]
struct Handled {
    report: Option<Report>,
    wake: bool,
}

impl Handled {
    fn report(report: Report) -> Self {
        Handled {
            report: Some(report),
            wake: false,
        }
    }

    fn wake() -> Self {
        Handled {
            report: None,
            wake: true,
        }
    }
}

impl Shared {
    /// Applies `work` to the seam under its lock, then reports and wakes
    /// outside it.
    pub(super) fn on_work(&self, work: Work) {
        let handled = {
            let mut seam = self.seam();
            match work {
                Work::Run(wire) | Work::Compact(wire) => seam.hand_over(wire),
                Work::Cancel(run) => seam.take_back(run),
                Work::Abort { run, deadline } => seam.abort(run, deadline),
                Work::AbortWithdrawn(run) => seam.withdraw_abort(&run),
            }
        };
        if let Some(report) = handled.report {
            self.report(report);
        }
        if handled.wake {
            self.wake();
        }
    }
}

impl Seam {
    /// A run the driver handed over, which uses one offered place. It waits
    /// for Python to take it, or is lost at once if nothing more is taken.
    /// A claim this build cannot read fails.
    fn hand_over(&mut self, wire: WireClaim) -> Handled {
        self.offered = self.offered.saturating_sub(1);
        match Claim::try_from(&wire).ok() {
            Some(claim) if self.closed => Handled::report(Report::Lost(claim.task_run_id)),
            Some(claim) => {
                self.handed.push_back(claim);
                Handled::wake()
            }
            None => Handled {
                report: wire.task_run_id.map(|run| Report::Failed {
                    run: run.into(),
                    reason: MALFORMED_KIND.into(),
                }),
                wake: false,
            },
        }
    }

    /// The driver took a run back. One Python holds is reported failed and
    /// Python is told to stop its body; one it has not taken is reported
    /// failed; one already ended needs nothing.
    fn take_back(&mut self, run: TaskRunId) -> Handled {
        let unstarted = self.take_handed(&run);
        match self.held.remove(&run) {
            Some(task) => {
                self.events.push_back(PyEvent::networked(
                    PyEventKind::Cancelled,
                    task.as_str(),
                    run.as_str(),
                ));
                Handled {
                    report: Some(cancelled(run)),
                    wake: true,
                }
            }
            None if unstarted => Handled::report(cancelled(run)),
            None => Handled::default(),
        }
    }

    /// Another leader may replay the run once `deadline` passes. One not
    /// yet taken never starts, so it is lost now; Python is told of one it
    /// holds.
    fn abort(&mut self, run: TaskRunId, deadline: StdInstant) -> Handled {
        if self.take_handed(&run) {
            return Handled::report(Report::Lost(run));
        }
        let Some(task) = self.held.get(&run) else {
            return Handled::default();
        };
        let left = deadline
            .saturating_duration_since(StdInstant::now())
            .as_secs_f64();
        self.events
            .push_back(PyEvent::abort(task.as_str(), run.as_str(), left));
        Handled::wake()
    }

    /// The leader hears this worker again: Python is told, for a run it
    /// holds, that the run's abort is lifted.
    fn withdraw_abort(&mut self, run: &TaskRunId) -> Handled {
        let Some(task) = self.held.get(run) else {
            return Handled::default();
        };
        self.events.push_back(PyEvent::networked(
            PyEventKind::AbortWithdrawn,
            task.as_str(),
            run.as_str(),
        ));
        Handled::wake()
    }

    /// Removes `run` from the runs not taken yet; whether it was there.
    fn take_handed(&mut self, run: &TaskRunId) -> bool {
        let before = self.handed.len();
        self.handed.retain(|claim| &claim.task_run_id != run);
        self.handed.len() != before
    }
}

fn cancelled(run: TaskRunId) -> Report {
    Report::Failed {
        run,
        reason: CANCELLED_KIND.into(),
    }
}
