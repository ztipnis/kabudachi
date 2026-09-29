//! Awaiting Rust futures from Python's asyncio.
//!
//! A future runs on the Tokio runtime and Python gets an `asyncio.Future` on
//! the running event loop. When the Rust side finishes it tells the loop, from
//! whatever thread it is on, to resolve that future; cancelling the Python
//! future aborts the Rust task. Python keeps the event loop, so nothing here
//! ever runs Python code on a Tokio thread except to hand a result to the loop.
//!
//! Every future admitted through the bridge is counted until it has handed its
//! result to the loop. Closing the bridge stops admitting new ones, and
//! waiting for it to drain lets the runtime be destroyed without stranding a
//! Python future that was about to be resolved.
//!
//! Two things are not covered: a future that panics never settles its Python
//! future, and one still running when the runtime is dropped without a drain
//! is abandoned. Futures passed in are expected to return an error, not panic.

use std::future::Future;
use std::sync::Arc;

use pyo3::BoundObject;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyCFunction, PyDict, PyTuple};
use tokio::runtime::Handle;
use tokio::sync::watch;

pub const CLOSED_MESSAGE: &str = "the native runtime has shut down";

#[derive(Debug, Clone, Copy)]
/// Lets awaitables in only while the runtime is open, and counts those still pending.
struct Gate {
    open: bool,
    in_flight: usize,
}

/// Counts an admitted future until it is dropped, wherever that happens: after
/// delivering its result, when aborted, or when the runtime discards it.
struct InFlight(Arc<watch::Sender<Gate>>);

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.send_modify(|gate| gate.in_flight -= 1);
    }
}

/// Runs futures on a Tokio runtime for Python to await.
#[derive(Debug, Clone)]
pub struct Bridge {
    runtime: Handle,
    gate: Arc<watch::Sender<Gate>>,
}

impl Bridge {
    pub fn new(runtime: Handle) -> Self {
        let gate = Gate {
            open: true,
            in_flight: 0,
        };
        Bridge {
            runtime,
            gate: Arc::new(watch::channel(gate).0),
        }
    }

    /// Stops admitting futures. Those already admitted still finish.
    pub fn close(&self) {
        self.gate.send_modify(|gate| gate.open = false);
    }

    pub fn is_open(&self) -> bool {
        self.gate.borrow().open
    }

    /// The number of admitted futures that have not yet handed over a result.
    pub fn in_flight(&self) -> usize {
        self.gate.borrow().in_flight
    }

    /// Resolves once no admitted future is left. Call [`Self::close`] first,
    /// or new ones may keep arriving.
    pub async fn drained(&self) {
        let mut gate = self.gate.subscribe();
        // The sender lives in `self`, so the channel cannot be closed here.
        let _ = gate.wait_for(|gate| gate.in_flight == 0).await;
    }

    /// Counts a new future in, or returns `None` if the bridge is closed.
    /// Checking and counting are one step, so a future is either admitted
    /// before a `close` and awaited by `drained`, or refused.
    fn admit(&self) -> Option<InFlight> {
        let mut admitted = false;
        self.gate.send_modify(|gate| {
            if gate.open {
                gate.in_flight += 1;
                admitted = true;
            }
        });
        admitted.then(|| InFlight(Arc::clone(&self.gate)))
    }

    /// Runs `future` on the runtime and returns an awaitable that resolves on
    /// the asyncio event loop that is running the caller.
    ///
    /// Raises `RuntimeError` if the bridge is closed or no event loop is
    /// running. If the loop has closed by the time `future` finishes, its
    /// result is dropped: nobody is waiting.
    pub fn spawn_into_py<'py, F, T>(
        &self,
        py: Python<'py>,
        future: F,
    ) -> PyResult<Bound<'py, PyAny>>
    where
        F: Future<Output = PyResult<T>> + Send + 'static,
        T: for<'a> IntoPyObject<'a> + Send + 'static,
    {
        let counted = self
            .admit()
            .ok_or_else(|| PyRuntimeError::new_err(CLOSED_MESSAGE))?;
        let event_loop = py.import("asyncio")?.call_method0("get_running_loop")?;
        let awaitable = event_loop.call_method0("create_future")?;
        let event_loop = event_loop.unbind();
        let target = awaitable.clone().unbind();

        let task = self.runtime.spawn(async move {
            let _counted = counted;
            let outcome = future.await;
            Python::attach(|py| {
                if let Err(error) = hand_to_loop(py, &event_loop, &target, outcome) {
                    report_undeliverable(py, &event_loop, error);
                }
            });
        });

        let abort = task.abort_handle();
        let watch_for_cancel = |py: Python<'py>| -> PyResult<()> {
            let on_done = PyCFunction::new_closure(
                py,
                None,
                None,
                move |args: &Bound<'_, PyTuple>,
                      _kwargs: Option<&Bound<'_, PyDict>>|
                      -> PyResult<()> {
                    if args.get_item(0)?.call_method0("cancelled")?.is_truthy()? {
                        abort.abort();
                    }
                    Ok(())
                },
            )?;
            awaitable.call_method1("add_done_callback", (on_done,))?;
            Ok(())
        };
        if let Err(error) = watch_for_cancel(py) {
            task.abort();
            return Err(error);
        }
        Ok(awaitable)
    }
}

/// Resolves `future` with `value`, unless it was cancelled or already done.
#[pyfunction]
fn resolve(future: &Bound<'_, PyAny>, value: &Bound<'_, PyAny>) -> PyResult<()> {
    if !future.call_method0("done")?.is_truthy()? {
        future.call_method1("set_result", (value,))?;
    }
    Ok(())
}

/// Fails `future` with `exception`, unless it was cancelled or already done.
#[pyfunction]
fn fail(future: &Bound<'_, PyAny>, exception: &Bound<'_, PyAny>) -> PyResult<()> {
    if !future.call_method0("done")?.is_truthy()? {
        future.call_method1("set_exception", (exception,))?;
    }
    Ok(())
}

/// Schedules the resolution of `target` on `event_loop`, which may be running
/// on another thread.
fn hand_to_loop<T>(
    py: Python<'_>,
    event_loop: &Py<PyAny>,
    target: &Py<PyAny>,
    outcome: PyResult<T>,
) -> PyResult<()>
where
    T: for<'a> IntoPyObject<'a>,
{
    let (settle, payload) = match outcome {
        Ok(value) => (
            wrap_pyfunction!(resolve, py)?,
            value
                .into_pyobject(py)
                .map_err(Into::into)?
                .into_any()
                .unbind(),
        ),
        Err(error) => (wrap_pyfunction!(fail, py)?, error.into_value(py).into_any()),
    };
    event_loop.bind(py).call_method1(
        "call_soon_threadsafe",
        (settle, target.clone_ref(py), payload),
    )?;
    Ok(())
}

/// A result that could not be handed to the loop is only expected when the
/// loop has closed, and then nobody is waiting. Anything else would leave a
/// Python future pending forever, so it is reported instead of hidden.
fn report_undeliverable(py: Python<'_>, event_loop: &Py<PyAny>, error: PyErr) {
    let loop_closed = event_loop
        .bind(py)
        .call_method0("is_closed")
        .and_then(|closed| closed.is_truthy())
        .unwrap_or(false);
    if !loop_closed {
        error.write_unraisable(py, None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::runtime::Builder;

    fn runtime() -> tokio::runtime::Runtime {
        Builder::new_current_thread().enable_time().build().unwrap()
    }

    #[test]
    fn draining_waits_for_every_admitted_future() {
        let runtime = runtime();
        let bridge = Bridge::new(runtime.handle().clone());
        let counted = bridge.admit().unwrap();
        bridge.close();

        runtime.block_on(async {
            let drained = tokio::spawn({
                let bridge = bridge.clone();
                async move { bridge.drained().await }
            });
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            assert!(
                !drained.is_finished(),
                "drained while a future was in flight"
            );

            drop(counted);
            tokio::time::timeout(std::time::Duration::from_secs(5), drained)
                .await
                .expect("drains once the last future is gone")
                .unwrap();

            // After the last drop, a fresh `drained()` resolves at once.
            tokio::time::timeout(std::time::Duration::from_secs(5), bridge.drained())
                .await
                .expect("a bridge with nothing in flight drains immediately");
        });
    }
}
