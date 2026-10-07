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
//! A future that is dropped before it hands over a result, because the runtime
//! gave up on it, was aborted, or panicked, fails its Python future with the
//! closed error as it goes, so nothing awaiting it waits forever. The one
//! exception is a Python interpreter that is already finalizing: nothing is
//! left to settle then.

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

/// Owes a Python future its result. Dropped before delivering, it fails the
/// future with the closed error. It is a separate type from [`InFlight`] so
/// that counting stays free of Python, and it owns the count so the future is
/// settled before it stops counting: a drain implies every Python future has
/// been handed its result.
struct Owed {
    event_loop: Py<PyAny>,
    target: Py<PyAny>,
    delivered: bool,
    _counted: InFlight,
}

impl Owed {
    fn deliver<T>(&mut self, py: Python<'_>, outcome: PyResult<T>)
    where
        T: for<'a> IntoPyObject<'a>,
    {
        self.delivered = true;
        if let Err(error) = hand_to_loop(py, &self.event_loop, &self.target, outcome) {
            report_undeliverable(py, &self.event_loop, error);
        }
    }
}

impl Drop for Owed {
    fn drop(&mut self) {
        if !self.delivered {
            // `None` means the interpreter is finalizing, and nobody awaits.
            Python::try_attach(|py| {
                self.deliver::<()>(py, Err(PyRuntimeError::new_err(CLOSED_MESSAGE)));
            });
        }
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

        // Built before spawning, so a task dropped without ever running
        // still settles the future.
        let mut owed = Owed {
            event_loop,
            target,
            delivered: false,
            _counted: counted,
        };
        let task = self.runtime.spawn(async move {
            let outcome = future.await;
            Python::attach(|py| owed.deliver(py, outcome));
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
