use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use kabudachi_core::protocol::ids::{
    IncarnationId, ShardId, TaskDefinitionId, TaskId, TaskRunId, Uuid7Ids, WorkerId,
};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::{
    Completion, MemoryLimits, ReportRejection, Scheduler, Submission,
};
use kabudachi_core::time::{Duration as CoreDuration, RealClock};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;

use crate::bridge::{Bridge, CLOSED_MESSAGE};
use crate::door::{Closed, Refusal, SchedulerDoor};
use crate::election::{Publisher, run_election};
use crate::errors;
use crate::local_node::local_node;
use crate::outcomes::{PyCancelOutcome, PyRunState, coalescing};
use crate::timers::run_timers;
use crate::work::{PyCertification, PyClaim, PyEvent};

const DEFAULT_WORKER_THREADS: usize = 2;
/// A lone worker has no peer to wait for, so it starts electing itself at
/// once, and leads once its one-voter roll call closes a millisecond later.
const DEFAULT_SUSPECT_TIMEOUT_MS: u64 = 0;
/// The shard of a process that runs no cluster: just this worker.
const LOCAL_SHARD_ID: &str = "local";
/// Finished tasks are kept this long unless the caller says otherwise.
const DEFAULT_RESULT_TTL_MS: u64 = 60 * 60 * 1000;
const SHUTDOWN_GRACE: Duration = Duration::from_secs(1);

const ELECTION_STOPPED_MESSAGE: &str = "the election task stopped unexpectedly";

/// The native side of kabudachi for one worker: a Tokio runtime that keeps
/// the worker's leadership election going, the scheduler that hands out and
/// certifies work while the worker leads, and awaitables that resolve on the
/// asyncio event loop of the caller.
///
/// Call `shutdown()` when finished, which also releases anything still
/// waiting. Tasks are kept in memory only: any that are pending, claimed or
/// running when the runtime shuts down are lost. It is safe to call more than
/// once.
#[pyclass(name = "NativeRuntime", frozen)]
pub struct NativeRuntime {
    /// `None` once shut down. Held for the whole of a shutdown, so a second
    /// shutdown waits for the first to finish.
    tokio: Mutex<Option<Runtime>>,
    bridge: Bridge,
    election: Mutex<Option<JoinHandle<()>>>,
    timers: Mutex<Option<JoinHandle<()>>>,
    stop: Arc<Notify>,
    state: watch::Receiver<WorkerState>,
    /// The one way into the scheduler, which refuses everything once the
    /// runtime has shut down.
    door: Arc<SchedulerDoor<RealClock>>,
}

#[pymethods]
impl NativeRuntime {
    /// Starts the runtime and begins electing this worker leader.
    ///
    /// `worker_id` and `incarnation_id` identify this worker and this start
    /// of it. `worker_threads` sizes the Tokio thread pool.
    /// `suspect_timeout_ms` is how long the worker waits, with no leader to
    /// hear from, before it elects itself; with 0, the default, it leads a
    /// millisecond after it starts, when its one-voter roll call closes.
    /// `result_ttl_ms` is how long a finished task is kept before it is
    /// forgotten. `memory_soft_limit` and `memory_hard_limit` are in bytes of
    /// serialized task input: past the soft one `next_events()` reports
    /// an `EventKind.SLOW_DOWN` event with `active` set,
    /// and past the hard one `submit` raises `kabudachi._native.BackpressureError`
    /// (re-exported by `kabudachi.errors`). Both or neither must be given,
    /// and the soft one must not be above the hard one, which
    /// `kabudachi.configure` checks before the values reach here.
    ///
    /// Raises `ValueError` if `worker_threads` is zero or only one of the two
    /// memory limits is given.
    #[new]
    #[pyo3(signature = (
        worker_id,
        incarnation_id,
        worker_threads = DEFAULT_WORKER_THREADS,
        suspect_timeout_ms = DEFAULT_SUSPECT_TIMEOUT_MS,
        result_ttl_ms = DEFAULT_RESULT_TTL_MS,
        memory_soft_limit = None,
        memory_hard_limit = None,
    ))]
    fn new(
        worker_id: String,
        incarnation_id: String,
        worker_threads: usize,
        suspect_timeout_ms: u64,
        result_ttl_ms: u64,
        memory_soft_limit: Option<u64>,
        memory_hard_limit: Option<u64>,
    ) -> PyResult<Self> {
        if worker_threads == 0 {
            return Err(PyValueError::new_err("worker_threads must be at least 1"));
        }
        let limits = match (memory_soft_limit, memory_hard_limit) {
            (None, None) => None,
            (Some(soft), Some(hard)) => Some(MemoryLimits { soft, hard }),
            _ => {
                return Err(PyValueError::new_err("give both memory limits or neither"));
            }
        };

        let tokio = Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .thread_name("kabudachi-native")
            .enable_time()
            .build()
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        let bridge = Bridge::new(tokio.handle().clone());

        let worker_id = WorkerId::new(worker_id);
        // One clock for everything: the scheduler's and the election node's
        // deadlines are ticks of it, so the timer and election loops have to
        // count the same ticks, and the scheduler checks the end of each
        // leadership grant the node hands it against its own reading of it.
        let clock = RealClock::new();
        let (node, first) = local_node(
            worker_id.clone(),
            IncarnationId::new(incarnation_id),
            ShardId::new(LOCAL_SHARD_ID),
            clock,
            CoreDuration::from_millis(suspect_timeout_ms),
        );
        let mut new_scheduler = Scheduler::new(clock, Uuid7Ids);
        new_scheduler.set_result_ttl(Some(CoreDuration::from_millis(result_ttl_ms)));
        new_scheduler.set_memory_limits(limits);
        let door = Arc::new(SchedulerDoor::new(new_scheduler, worker_id));
        let (state_sender, state) = watch::channel(node.state());
        let stop = Arc::new(Notify::new());
        let publisher = Publisher {
            state: state_sender,
            door: Arc::clone(&door),
        };
        let election = tokio.spawn(run_election(
            node,
            first,
            clock,
            Arc::clone(&stop),
            publisher,
        ));

        let timers = tokio.spawn(run_timers(Arc::clone(&door), clock));

        Ok(NativeRuntime {
            tokio: Mutex::new(Some(tokio)),
            bridge,
            election: Mutex::new(Some(election)),
            timers: Mutex::new(Some(timers)),
            stop,
            state,
            door,
        })
    }

    /// Returns an awaitable that resolves once this worker is the leader.
    ///
    /// Must be called from a running asyncio event loop. Raises
    /// `RuntimeError` if there is none, if the runtime has shut down, or if
    /// it shuts down before leadership arrives.
    fn wait_until_leader<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let mut state = self.state.clone();
        let bridge = self.bridge.clone();
        self.bridge.spawn_into_py(py, async move {
            state
                .wait_for(|current| *current == WorkerState::Leader)
                .await
                .map(|_| ())
                .map_err(|_| {
                    // The election only ends early on a shutdown, unless it panicked.
                    let reason = if bridge.is_open() {
                        ELECTION_STOPPED_MESSAGE
                    } else {
                        CLOSED_MESSAGE
                    };
                    PyRuntimeError::new_err(reason)
                })
        })
    }

    /// Records a new task and returns its ID. The task waits until a worker
    /// claims it. Every call is a new task. `kind` is the task's `TaskKind`
    /// value (`"task"`, `"ephemeral"` or `"coalescing"`); a coalescing task
    /// always has a `key` (its default is `""`), and no other kind has one. A
    /// run that fails is replaced by a new attempt up to `retries` times. With
    /// `delay_ms` the task is not claimed before that long has passed; with
    /// `expires_in_ms` it expires instead of running if it is still unclaimed
    /// after that long. With `drop_oldest`, a coalescing task that would pass
    /// the hard memory limit loses its key's oldest retained payloads to fit
    /// instead of being refused.
    ///
    /// Raises `ValueError` if `kind`, `key` and `drop_oldest` disagree,
    /// `BackpressureError` if the task does not fit under the hard memory
    /// limit, and `RuntimeError` if the runtime has shut down.
    #[pyo3(signature = (
        definition_id,
        source_version,
        serialized_input,
        queue,
        kind,
        key,
        *,
        retries = 0,
        delay_ms = None,
        expires_in_ms = None,
        drop_oldest = false,
    ))]
    #[allow(
        clippy::too_many_arguments,
        reason = "each argument is a Python parameter"
    )]
    fn submit(
        &self,
        definition_id: &str,
        source_version: u32,
        serialized_input: &[u8],
        queue: &str,
        kind: &str,
        key: Option<String>,
        retries: u32,
        delay_ms: Option<u64>,
        expires_in_ms: Option<u64>,
        drop_oldest: bool,
        py: Python<'_>,
    ) -> PyResult<String> {
        let coalescing =
            coalescing(kind, key, drop_oldest).map_err(PyValueError::new_err)?;
        let mut submission = Submission::new(
            TaskDefinitionId::new(definition_id),
            source_version,
            serialized_input.to_vec(),
            queue,
        )
        .with_retries(retries);
        if let Some(delay_ms) = delay_ms {
            submission = submission.with_delay(CoreDuration::from_millis(delay_ms));
        }
        if let Some(expires_in_ms) = expires_in_ms {
            submission = submission.with_expiry(CoreDuration::from_millis(expires_in_ms));
        }
        if let Some((key, drop_oldest)) = coalescing {
            submission = submission.with_coalescing_key(key);
            if drop_oldest {
                submission = submission.with_drop_oldest();
            }
        }
        let task_id = self
            .door
            .submit(submission)
            .map_err(|refusal| {
                refused(refusal, |rejection| {
                    errors::backpressure_error(py, rejection.to_string())
                })
            })?;
        Ok(task_id.as_str().to_owned())
    }

    /// Returns an awaitable that resolves to a list of at most `limit` claims,
    /// oldest task first, as soon as this worker is leader and there is work.
    /// A claimed task is not handed out again.
    ///
    /// Must be called from a running asyncio event loop. Raises `ValueError`
    /// if `limit` is zero, and `RuntimeError` if the runtime has shut down or
    /// shuts down while waiting.
    fn claim_pending<'py>(&self, py: Python<'py>, limit: usize) -> PyResult<Bound<'py, PyAny>> {
        if limit == 0 {
            return Err(PyValueError::new_err("limit must be at least 1"));
        }
        let waiting = Arc::clone(&self.door).claim_when_available(limit);
        self.bridge.spawn_into_py(py, async move {
            waiting
                .await
                .map(|claims| claims.into_iter().map(PyClaim::from).collect::<Vec<_>>())
                .map_err(PyErr::from)
        })
    }

    /// Returns an awaitable that resolves to a list of what the scheduler has
    /// decided on its own since the last call (for example that a task
    /// expired), oldest first, as soon as there is anything.
    ///
    /// Must be called from a running asyncio event loop, and must not be
    /// cancelled while it waits except at shutdown: events it has taken but
    /// not delivered are lost. Raises `RuntimeError` if the runtime has shut
    /// down or shuts down while waiting.
    fn next_events<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let waiting = Arc::clone(&self.door).events_when_available();
        self.bridge.spawn_into_py(py, async move {
            waiting
                .await
                .map(|events| events.into_iter().map(PyEvent::from).collect::<Vec<_>>())
                .map_err(PyErr::from)
        })
    }

    /// Reports that the worker began executing a claimed run.
    ///
    /// Raises `RuntimeError` if the run is unknown or is not a claimed run of
    /// this worker.
    fn report_started(&self, task_run_id: &str) -> PyResult<()> {
        self.door
            .report_started(&TaskRunId::new(task_run_id))
            .map_err(|refusal| refused(refusal, rejected))
    }

    /// Reports that a running run succeeded, with the digest of its result,
    /// and returns the certification of that result.
    ///
    /// Raises `RuntimeError` if the run is unknown, is not running, or is not
    /// this worker's; the result is then not certified. With `continues`, the
    /// result is a continuation: the run is certified, but the task is not
    /// over (its coalescing key stays held) until `end_continuation`.
    #[pyo3(signature = (task_run_id, result_digest, continues = false))]
    fn complete(
        &self,
        task_run_id: &str,
        result_digest: &[u8],
        continues: bool,
    ) -> PyResult<PyCertification> {
        let completion = if continues {
            Completion::Continues
        } else {
            Completion::Final
        };
        self.door
            .complete(&TaskRunId::new(task_run_id), result_digest.to_vec(), completion)
            .map(Into::into)
            .map_err(|refusal| refused(refusal, rejected))
    }

    /// The continuation of a task completed with `continues` is over, however
    /// it ended. Returns whether there was one to end.
    fn end_continuation(&self, task_id: &str) -> PyResult<bool> {
        Ok(self.door.end_continuation(&TaskId::new(task_id))?)
    }

    /// Reports that a claimed run failed with an error of type `failure_kind`
    /// (the type's name, never the error's message), whether or not its body
    /// started. Returns whether the task has retries left, so a new attempt
    /// is now waiting to be claimed; `False` means the task has failed for
    /// good.
    ///
    /// Raises `RuntimeError` if the run is unknown, is not this worker's, or
    /// has already finished; the failure is then not recorded.
    fn report_failure(&self, task_run_id: &str, failure_kind: &str) -> PyResult<bool> {
        let failure = self
            .door
            .report_failure(&TaskRunId::new(task_run_id), failure_kind)
            .map_err(|refusal| refused(refusal, rejected))?;
        Ok(failure.retry.is_some())
    }

    /// Cancels a task, whatever it is doing: `CancelOutcome.CANCELLED` if it
    /// was, and the worker running it (if any) is told through `next_events()`
    /// to stop; `ALREADY_FINISHED` if it had already finished; `UNKNOWN_TASK`
    /// if there is no such task.
    ///
    /// Raises `RuntimeError` if this worker is not the leader.
    fn cancel(&self, task_id: &str) -> PyResult<PyCancelOutcome> {
        let outcome = self.door.cancel(&TaskId::new(task_id)).map_err(|refusal| {
            refused(refusal, |rejection| {
                PyRuntimeError::new_err(rejection.to_string())
            })
        })?;
        Ok(outcome.into())
    }

    /// The state of a run, or `None` if the run is unknown.
    fn task_run_state(&self, task_run_id: &str) -> PyResult<Option<PyRunState>> {
        let state = self.door.run_state(&TaskRunId::new(task_run_id))?;
        Ok(state.map(PyRunState::from))
    }

    /// The IDs of every run of a task, oldest attempt first. Empty if the
    /// task is unknown or has been forgotten.
    fn task_run_ids(&self, task_id: &str) -> PyResult<Vec<String>> {
        let runs = self.door.run_ids(&TaskId::new(task_id))?;
        Ok(runs
            .iter()
            .map(|run_id| run_id.as_str().to_owned())
            .collect())
    }

    /// The worker's election state by name, for example `"Leader"`.
    fn worker_state(&self) -> String {
        format!("{:?}", *self.state.borrow())
    }

    /// How many awaitables handed out by this runtime are still waiting for a
    /// result. A cancelled awaitable stops counting.
    fn in_flight_waits(&self) -> usize {
        self.bridge.in_flight()
    }

    /// Stops the worker gracefully and shuts the runtime down. Awaitables
    /// still pending are failed with `RuntimeError`, and awaiting new ones is
    /// refused. Tasks that have not finished are discarded. If another thread
    /// is already shutting down, this waits for it to finish. Does nothing if
    /// already shut down.
    fn shutdown(&self, py: Python<'_>) {
        // Everything here blocks on Tokio work, and the tasks handing results
        // to Python need the GIL, so it must not be held.
        py.detach(|| {
            let mut slot = self.tokio.lock().unwrap_or_else(PoisonError::into_inner);
            let Some(runtime) = slot.take() else {
                return;
            };
            self.bridge.close();
            // Every later operation is refused, claims waiting for work and
            // waits for events give up, and the timer loop ends.
            self.door.close();
            let election = self
                .election
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            let timers = self
                .timers
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            self.stop.notify_one();

            runtime.block_on(async {
                if let Some(election) = election {
                    let _ = election.await;
                }
                if let Some(timers) = timers {
                    let _ = timers.await;
                }
                // The election ending fails every waiter; let each one hand
                // its error to Python before the runtime discards it.
                let _ = tokio::time::timeout(SHUTDOWN_GRACE, self.bridge.drained()).await;
            });
            runtime.shutdown_timeout(SHUTDOWN_GRACE);
        });
    }
}

impl Drop for NativeRuntime {
    /// A runtime that was never shut down still must not block its owner's
    /// thread, so its tasks are abandoned rather than waited for. Awaitables
    /// still pending are then never settled: call `shutdown()` to release them.
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

fn rejected(rejection: ReportRejection) -> PyErr {
    PyRuntimeError::new_err(rejection.to_string())
}

/// What a refused door operation raises: the closed error, or whatever
/// `reject` makes of the scheduler's own rejection.
fn refused<R>(refusal: Refusal<R>, reject: impl FnOnce(R) -> PyErr) -> PyErr {
    match refusal {
        Refusal::Closed => Closed.into(),
        Refusal::Rejected(rejection) => reject(rejection),
    }
}

impl From<Closed> for PyErr {
    fn from(_: Closed) -> Self {
        PyRuntimeError::new_err(CLOSED_MESSAGE)
    }
}
