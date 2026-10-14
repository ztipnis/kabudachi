//! How each piece of work the driver hands over changes the seam: a run to
//! hold for Python, a run taken back, an abort deadline, or one withdrawn.

use std::sync::Arc;
use std::time::Instant as StdInstant;

use kabudachi_core::blob::BlobOwner;
use kabudachi_core::manifest::PAYLOAD_UNAVAILABLE_KIND;
use kabudachi_core::protocol::ids::TaskRunId;
use kabudachi_core::protocol::messages::Claim as WireClaim;
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::scheduler::Claim;
use kabudachi_net::executor::{Report, Work};
use kabudachi_net::payload_memory::{PAYLOAD_TOO_LARGE_KIND, Taken};

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

/// Why a run whose input travels as blobs cannot be handed to Python.
enum Resolving {
    Unresolved,
    TooLarge,
}

impl Shared {
    /// A run whose input travels as blobs: once this process has room for
    /// the input, they are fetched and checked before Python can take the
    /// run, so a body always starts with its whole input. The input counts
    /// in flight until the run ends. A run whose blobs cannot all be had
    /// fails as `PayloadUnavailable`, and one whose input alone is past this
    /// worker's hard limit as `PayloadTooLarge` (the driver reports either
    /// started first); its task's retries apply.
    pub(super) fn resolve_then_hand(self: Arc<Self>, wire: WireClaim) {
        let Ok(mut claim) = Claim::try_from(&wire) else {
            // The plain hand-over fails a claim this build cannot read.
            return self.on_work(Work::Run(wire));
        };
        {
            let mut seam = self.seam();
            seam.offered = seam.offered.saturating_sub(1);
            if seam.closed {
                drop(seam);
                return self.report(Report::Lost(claim.task_run_id));
            }
            let run = claim.task_run_id.clone();
            let shared = Arc::clone(&self);
            // Spawned under the lock, so the fetch cannot end before its
            // handle is kept.
            let fetch = tokio::spawn(async move {
                let outcome = shared.fetch_input(&mut claim).await;
                shared.finish_resolving(claim, outcome);
            });
            seam.resolving.insert(run, fetch.abort_handle());
        }
    }

    /// Waits, still resolving, while other inputs fill this process's room:
    /// a cancel or abort deadline meanwhile ends the run as before and
    /// stops this, dropping the room it holds. Puts the whole input in `claim`
    /// and returns the room it takes, if its input travelled as blobs.
    async fn fetch_input(&self, claim: &mut Claim) -> Result<Option<Taken>, Resolving> {
        let manifest = match claim.input_manifest() {
            Ok(Some(manifest)) => manifest,
            Ok(None) => return Ok(None),
            Err(_) => return Err(Resolving::Unresolved),
        };
        let taken = self
            .net
            .payload_memory()
            .take_when_room(manifest.total_bytes())
            .await
            .map_err(|_| Resolving::TooLarge)?;
        let owner = BlobOwner::Task(claim.task.task_id());
        let fetched = self.net.fetch_blobs(&owner, &manifest).await;
        claim
            .resolve_input(|key| fetched.get(key).cloned())
            .map(|()| Some(taken))
            .map_err(|_| Resolving::Unresolved)
    }

    /// Hands a run whose blobs were fetched to Python, with its input
    /// counted until the run ends, unless it ended meanwhile.
    fn finish_resolving(&self, claim: Claim, outcome: Result<Option<Taken>, Resolving>) {
        let run = claim.task_run_id.clone();
        let report = {
            let mut seam = self.seam();
            if seam.resolving.remove(&run).is_none() {
                // Cancelled, aborted or stopped meanwhile: already reported
                // (the fetch was stopped, unless it was already here).
                return;
            }
            match outcome {
                Ok(taken) => {
                    if let Some(taken) = taken {
                        seam.inputs_in_flight.insert(run, taken);
                    }
                    seam.handed.push_back(claim);
                    None
                }
                Err(Resolving::Unresolved) => Some(Report::Failed {
                    run,
                    reason: PAYLOAD_UNAVAILABLE_KIND.into(),
                    message: String::new(),
                }),
                Err(Resolving::TooLarge) => Some(Report::Failed {
                    run,
                    reason: PAYLOAD_TOO_LARGE_KIND.into(),
                    message: String::new(),
                }),
            }
        };
        match report {
            Some(report) => self.report(report),
            None => self.wake(),
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
                    message: String::new(),
                }),
                wake: false,
            },
        }
    }

    /// The driver took a run back. One Python holds is reported failed and
    /// Python is told to stop its body; one it has not taken, or whose blobs
    /// are still being fetched, is reported failed; one already ended needs
    /// nothing.
    fn take_back(&mut self, run: TaskRunId) -> Handled {
        let unstarted = self.take_handed(&run) || self.stop_resolving(&run);
        match self.held.remove(&run) {
            Some(held) => {
                self.events.push_back(PyEvent::networked(
                    PyEventKind::Cancelled,
                    held.task.as_str(),
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
    /// yet taken, or still having its blobs fetched, never starts, so it is
    /// lost now; Python is told of one it holds.
    fn abort(&mut self, run: TaskRunId, deadline: StdInstant) -> Handled {
        if self.take_handed(&run) || self.stop_resolving(&run) {
            return Handled::report(Report::Lost(run));
        }
        let Some(held) = self.held.get(&run) else {
            return Handled::default();
        };
        let left = deadline
            .saturating_duration_since(StdInstant::now())
            .as_secs_f64();
        self.events
            .push_back(PyEvent::abort(held.task.as_str(), run.as_str(), left));
        Handled::wake()
    }

    /// The leader hears this worker again: Python is told, for a run it
    /// holds, that the run's abort is lifted.
    fn withdraw_abort(&mut self, run: &TaskRunId) -> Handled {
        let Some(held) = self.held.get(run) else {
            return Handled::default();
        };
        self.events.push_back(PyEvent::networked(
            PyEventKind::AbortWithdrawn,
            held.task.as_str(),
            run.as_str(),
        ));
        Handled::wake()
    }

    /// Stops the fetch of `run`'s blobs, which drops the room it holds;
    /// whether it was being fetched.
    fn stop_resolving(&mut self, run: &TaskRunId) -> bool {
        self.resolving.remove(run).is_some_and(|fetch| {
            fetch.abort();
            true
        })
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
        message: String::new(),
    }
}
