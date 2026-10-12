//! The Tokio runtime a networked worker runs on, with the two tasks it
//! keeps there: the worker itself, and the pump that hands the driver's
//! work to the seam.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration as StdDuration;

use kabudachi_core::election::{Input, Step, WorkerNode};
use kabudachi_core::time::RealClock;
use kabudachi_net::executor::WorkSource;
use kabudachi_net::handoff::HandedOff;
use kabudachi_net::worker::Worker;
use pyo3::prelude::*;
use tokio::runtime::{Handle, Runtime};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::observed::{LeftOnDrop, Observed, publish};
use super::seam::Shared;
use crate::bridge::Bridge;

const SHUTDOWN_GRACE: StdDuration = StdDuration::from_secs(1);

/// Dropped without `shutdown`, it abandons its tasks rather than block its
/// owner's thread, as `NativeRuntime` does.
pub(super) struct Host {
    tokio: Mutex<Option<Runtime>>,
    handle: Handle,
    bridge: Bridge,
    observed: Arc<watch::Sender<Observed>>,
    worker: Mutex<Option<JoinHandle<()>>>,
    pump: Mutex<Option<JoinHandle<()>>>,
}

impl Host {
    /// Runs `worker` on `tokio`, publishing what its node shows to
    /// `observed` and leaving what it handed off in `drained` once it ends,
    /// and pumps the work its driver hands over from `source` into `shared`.
    pub(super) fn start(
        tokio: Runtime,
        worker: Worker,
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
            let handed = worker
                .run(
                    move |node: &WorkerNode<RealClock>, _: Option<&Input>, _: &Step| {
                        publish(&seen, node);
                    },
                )
                .await;
            *drained.lock().unwrap_or_else(PoisonError::into_inner) = Some(handed);
        });
        let pump = tokio.spawn(async move {
            let mut source = source;
            while let Some(work) = source.next_work().await {
                shared.on_work(work);
            }
        });
        Host {
            tokio: Mutex::new(Some(tokio)),
            bridge: Bridge::new(handle.clone()),
            handle,
            observed,
            worker: Mutex::new(Some(running)),
            pump: Mutex::new(Some(pump)),
        }
    }

    pub(super) fn bridge(&self) -> &Bridge {
        &self.bridge
    }

    pub(super) fn handle(&self) -> &Handle {
        &self.handle
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
            runtime.block_on(async {
                let _ = tokio::time::timeout(SHUTDOWN_GRACE, self.bridge.drained()).await;
            });
            runtime.shutdown_timeout(SHUTDOWN_GRACE);
        });
    }
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
