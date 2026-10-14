//! The Tokio runtime a networked worker runs on, with the tasks it keeps
//! there: the worker itself, the pump that hands the driver's work to the
//! seam, the reader that hands Python the result messages arriving for the
//! tasks submitted here, and the task that asks a new leader about those
//! still awaited.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration as StdDuration;

use kabudachi_core::election::{Input, Step, WorkerNode};
use kabudachi_core::protocol::messages::{Claim as WireClaim, JoinResponseIds};
use kabudachi_core::time::RealClock;
use kabudachi_net::client::Client;
use kabudachi_net::executor::{Work, WorkSource};
use kabudachi_net::handoff::HandedOff;
use kabudachi_net::messenger::Net;
use kabudachi_net::worker::Worker;
use pyo3::prelude::*;
use tokio::runtime::{Handle, Runtime};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::observed::{LeftOnDrop, Observed, publish};
use super::results::{ask_after_leader_change, read_results};
use super::seam::Shared;
use crate::bridge::Bridge;

const SHUTDOWN_GRACE: StdDuration = StdDuration::from_secs(1);

pub(super) enum Peer {
    Worker(Worker),
    Client(Client),
}

impl Peer {
    pub(super) fn net(&self) -> Arc<Net> {
        match self {
            Self::Worker(worker) => worker.net(),
            Self::Client(client) => client.net(),
        }
    }
}

/// Dropped without `shutdown`, it abandons its tasks rather than block its
/// owner's thread, as `NativeRuntime` does.
pub(super) struct Host {
    tokio: Mutex<Option<Runtime>>,
    handle: Handle,
    bridge: Bridge,
    observed: Arc<watch::Sender<Observed>>,
    /// True while the pump task runs.
    pumping: Arc<AtomicBool>,
    worker: Mutex<Option<JoinHandle<()>>>,
    pump: Mutex<Option<JoinHandle<()>>>,
    /// The tasks that read result messages and ask a new leader.
    background: Mutex<Vec<JoinHandle<()>>>,
}

impl Host {
    /// Runs `worker` on `tokio`, publishing what its node shows to
    /// `observed` and leaving what it handed off in `drained` once it ends,
    /// and pumps the work its driver hands over from `source` into `shared`.
    pub(super) fn start(
        tokio: Runtime,
        peer: Peer,
        source: WorkSource,
        shared: Arc<Shared>,
        observed: Arc<watch::Sender<Observed>>,
        drained: Arc<Mutex<Option<HandedOff>>>,
    ) -> Host {
        let handle = tokio.handle().clone();
        let seen = Arc::clone(&observed);
        let left = LeftOnDrop(Arc::clone(&observed));
        let running = tokio.spawn(async move {
            // Dropped however this ends, a panic or an abort included.
            let _left = left;
            let handed = match peer {
                Peer::Worker(worker) => {
                    worker
                        .run(
                            move |node: &WorkerNode<RealClock>, _: Option<&Input>, _: &Step| {
                                publish(&seen, node);
                            },
                        )
                        .await
                }
                Peer::Client(client) => {
                    client
                        .run(move |pointer| {
                            seen.send_if_modified(|old| {
                                let leader = pointer
                                    .as_ref()
                                    .and_then(|p| p.leader_id().map(|id| (id, p.term)));
                                let changed = old.leader != leader;
                                if changed {
                                    old.leader_changes += 1;
                                    old.leader = leader;
                                }
                                if let Some(pointer) = pointer
                                    && let Some(id) = pointer.shard_id()
                                {
                                    old.shard_id = id.as_str().to_owned();
                                }
                                changed
                            });
                        })
                        .await;
                    return;
                }
            };
            *drained.lock().unwrap_or_else(PoisonError::into_inner) = Some(handed);
        });
        let results = tokio.spawn(read_results(Arc::clone(&shared)));
        let outcomes = tokio.spawn(ask_after_leader_change(Arc::clone(&shared)));
        let pumping = Arc::new(AtomicBool::new(true));
        let pump_guard = StoppedOnDrop(Arc::clone(&pumping));
        let pump = tokio.spawn(async move {
            // Dropped however this ends, a panic or an abort included.
            let _pumping = pump_guard;
            let mut source = source;
            while let Some(work) = source.next_work().await {
                match work {
                    // Its input is fetched first, apart from the pump.
                    Work::Run { claim: wire, stage } if input_is_blobs(&wire) => {
                        Arc::clone(&shared).resolve_then_hand(wire, stage);
                    }
                    work => shared.on_work(work),
                }
            }
        });
        Host {
            tokio: Mutex::new(Some(tokio)),
            bridge: Bridge::new(handle.clone()),
            handle,
            observed,
            pumping,
            worker: Mutex::new(Some(running)),
            pump: Mutex::new(Some(pump)),
            background: Mutex::new(vec![results, outcomes]),
        }
    }

    pub(super) fn bridge(&self) -> &Bridge {
        &self.bridge
    }

    pub(super) fn handle(&self) -> &Handle {
        &self.handle
    }

    /// Whether the pump task still runs; it ends with the seam.
    pub(super) fn pumping(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.pumping)
    }

    /// Stops the worker at once and shuts the runtime down; awaitables
    /// still pending fail. Does nothing if already shut down.
    pub(super) fn shutdown(&self, py: Python<'_>, shared: &Shared) {
        // As for `NativeRuntime`: everything here blocks on Tokio work, and
        // the tasks handing results to Python need the GIL.
        py.detach(|| {
            let mut slot = self.tokio.lock().unwrap_or_else(PoisonError::into_inner);
            let Some(runtime) = slot.take() else {
                return;
            };
            self.bridge.close();
            // Every wait gives up, as it does when the worker leaves.
            self.observed.send_modify(|seen| seen.gone = true);
            shared.wake();
            for task in [&self.worker, &self.pump] {
                if let Some(task) = task.lock().unwrap_or_else(PoisonError::into_inner).take() {
                    task.abort();
                }
            }
            let mut background = self
                .background
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            for task in background.drain(..) {
                task.abort();
            }
            drop(background);
            runtime.block_on(async {
                let _ = tokio::time::timeout(SHUTDOWN_GRACE, self.bridge.drained()).await;
            });
            runtime.shutdown_timeout(SHUTDOWN_GRACE);
        });
    }
}

/// Marks the pump stopped when dropped.
struct StoppedOnDrop(Arc<AtomicBool>);

impl Drop for StoppedOnDrop {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// Whether a run's input travels as blobs this process fetches before the run
/// is handed over. A composite member's does not: its manifest names results
/// of the composite, which its body reads through the composite's record.
fn input_is_blobs(wire: &WireClaim) -> bool {
    wire.task
        .as_ref()
        .is_some_and(|task| task.input_manifest.is_some() && task.composite_id.is_none())
}

impl Drop for Host {
    fn drop(&mut self) {
        let runtime = self
            .tokio
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(runtime) = runtime {
            runtime.shutdown_background();
        }
    }
}
