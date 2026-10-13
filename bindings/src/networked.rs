//! A worker of a networked shard: the bindings host
//! `kabudachi_net::worker::Worker` on their own Tokio runtime and run the
//! tasks it claims in Python, through its executor seam
//! (`kabudachi_net::executor`).
//!
//! Python drives it as it drives the one-node runtime: it asks for claims as
//! places free, reports on each run, reads events and submits. What differs
//! is who decides. The shard's leader may be another process, so a report is
//! handed to the worker's driver, which takes it to whichever node leads, and
//! returns at once; the leader's decision is not seen here. A submission
//! returns its task id at once and is delivered the same way, until a leader
//! stores it (`ACCEPTED`) or refuses it for good (`REFUSED`).
//!
//! A task submitted here names this worker as its client. The worker that
//! runs it delivers its result here before reporting it, and the leader
//! tells this worker when the run is certified or how the task ended
//! otherwise (`RESULT`, `CERTIFIED` and `OUTCOME` in `next_events()`). After
//! a change of leader, every task still awaited is asked about again. A run
//! held here delivers its result to its own client before it is reported.
//!
//! An input too large for one message travels as blobs. A submission stores
//! them before it is sent. A run whose input is a manifest is held back until
//! this worker has room for it under its memory limits and its blobs are
//! fetched and checked, and a cancel or abort deadline meanwhile ends it as
//! it ends a run Python has not taken. The input counts against the limits
//! until the run ends.
//!
//! Places are offered to the driver as credits. Each claim Python asks for
//! offers the places it has free that are neither offered already nor held
//! by a run it has not taken yet; each run the driver hands over uses one.
//!
//! The driver can take a run back. A cancel ends the run here: it is
//! reported failed, Python is told to stop its body (`CANCELLED`), and
//! nothing Python reports on it later counts. An abort deadline goes to
//! Python (`ABORT`) for a run it holds, and a run not yet taken is reported
//! lost at once.

mod delivery;
mod handover;
mod host;
mod observed;
mod results;
mod seam;
mod start;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration as StdDuration;

use kabudachi_core::manifest::fits_inline;
use kabudachi_core::protocol::digest::{Digest, DigestAlgorithm};
use kabudachi_core::protocol::ids::{TaskId, TaskRunId, Uuid7Ids};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::mint;
use kabudachi_core::time::RealClock;
use kabudachi_net::executor::{Report, executor_channel};
use kabudachi_net::handoff::HandedOff;
use kabudachi_net::worker::Worker;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use tokio::sync::watch;

use crate::arguments::SubmitArguments;
use crate::bridge::CLOSED_MESSAGE;
use crate::errors::backpressure_error;
use crate::health;
use crate::work::PyTaskRecord;

use self::delivery::{cancel_at_leader, send_submission};
use self::host::Host;
use self::observed::Observed;
use self::results::deliver_then_report;
use self::seam::{GONE_MESSAGE, Shared};
use self::start::NetworkedArguments;

const REPORTED_POLL: StdDuration = StdDuration::from_millis(20);

/// A worker of a networked shard, and the Tokio runtime it runs on.
///
/// Call `drain()` to leave the shard, then `shutdown()` when finished: the
/// worker stops at once, without leaving its shard, and every awaitable
/// still pending fails.
#[pyclass(name = "NetworkedRuntime", frozen)]
pub struct NetworkedRuntime {
    host: Host,
    shared: Arc<Shared>,
    /// What the worker handed off, left by its task when it ends.
    drained: Arc<Mutex<Option<HandedOff>>>,
    worker_id: String,
    address: String,
    clock: RealClock,
    reconnect_timeout: StdDuration,
    shard_name: String,
    /// Set once Python has loaded the worker's tasks and can take work.
    registry: Arc<AtomicBool>,
    health: Option<SocketAddr>,
}

#[pymethods]
impl NetworkedRuntime {
    /// Starts a worker of a networked shard as `arguments` say (see
    /// `kabudachi.native_protocol.NetworkedArguments`), and bootstraps it into
    /// the shard: through its seeds, through its authority, or, with neither,
    /// by founding the shard alone.
    ///
    /// Raises `ValueError` for an address that cannot be read, a timing or
    /// authority TTL of zero (`drain_wait_limit_ms`, which bounds each wait of
    /// a leaving worker, included), a replication factor or seed rounds of
    /// zero, only one memory limit, no worker threads, an authority that
    /// cannot be used, no queues, a blank queue name or queue options that are
    /// refused (a zero weight, running limit or starvation age, or a queue
    /// given twice), and `RuntimeError` if it cannot listen on its listen
    /// address. `health_listen` raises `ValueError` for an address that is not
    /// `ip:port` and `RuntimeError` when it cannot be bound.
    #[new]
    fn new(py: Python<'_>, arguments: NetworkedArguments) -> PyResult<Self> {
        arguments.check()?;
        // Bound before the worker bootstraps, so a port in use leaves nothing
        // running.
        let health_listener = arguments
            .health_listen
            .as_deref()
            .map(health::bind)
            .transpose()?;
        let (endpoint, executor) = executor_channel();
        let (source, reports) = executor.split();
        let config = arguments.worker_config(endpoint)?;
        let tokio = arguments.tokio()?;
        let worker = py
            .detach(|| tokio.block_on(Worker::start(config)))
            .map_err(|_| {
                PyRuntimeError::new_err(format!("cannot listen on {:?}", arguments.listen))
            })?;
        let worker_id = worker.id().as_str().to_owned();
        let address = worker
            .address()
            .map(|address| address.to_string())
            .unwrap_or_default();
        let (sender, observed) = watch::channel(Observed::default());
        let shared = Arc::new(Shared::new(
            reports,
            worker.net(),
            observed,
            arguments.retry(),
        ));
        let drained = Arc::default();
        let host = Host::start(
            tokio,
            worker,
            source,
            Arc::clone(&shared),
            Arc::new(sender),
            Arc::clone(&drained),
        );
        let registry = Arc::new(AtomicBool::new(false));
        let health = match health_listener {
            Some(listener) => {
                let seen = shared.observed.clone();
                let ready_seen = shared.observed.clone();
                let pump_running = host.pumping();
                let loaded = Arc::clone(&registry);
                Some(health::serve(
                    host.handle(),
                    listener,
                    health::Probe {
                        alive: Box::new(move || {
                            !seen.borrow().gone && pump_running.load(Ordering::Acquire)
                        }),
                        // A leader is ready once reconciled (`Leader`); a
                        // follower once a leader holding a grant has heard
                        // it, which is when its driver starts claiming.
                        ready: Box::new(move || {
                            let seen = ready_seen.borrow();
                            loaded.load(Ordering::Acquire)
                                && match seen.state {
                                    Some(WorkerState::Leader) => true,
                                    Some(WorkerState::Active) => seen.contact_floor,
                                    _ => false,
                                }
                        }),
                    },
                )?)
            }
            None => None,
        };
        Ok(NetworkedRuntime {
            host,
            shared,
            drained,
            worker_id,
            address,
            clock: RealClock::new(),
            reconnect_timeout: StdDuration::from_millis(arguments.reconnect_timeout_ms),
            shard_name: arguments.shard,
            registry,
            health,
        })
    }

    /// The address the health endpoints are served on, `ip:port`; `None`
    /// unless `health_listen` was given.
    fn health_address(&self) -> Option<String> {
        self.health.map(|address| address.to_string())
    }

    /// Says the worker is set up to take work: `/readyz` answers 200 from
    /// now on once the worker can take it.
    fn registry_loaded(&self) {
        self.registry.store(true, Ordering::Release);
    }

    /// The name of the shard this worker belongs to.
    fn shard_name(&self) -> String {
        self.shard_name.clone()
    }

    /// Whether a report's call returns the leader's decision: never, here;
    /// certifications and outcomes arrive as events.
    #[getter]
    fn certifies_locally(&self) -> bool {
        false
    }

    /// The largest payload this worker holds whole: its share of the hard
    /// memory limit for payloads in flight, or `None` when no limit is set.
    #[getter]
    fn payload_limit(&self) -> Option<u64> {
        self.shared.net.payload_memory().hard_limit()
    }

    /// The incarnation of the shard this worker joined; empty before it has.
    fn shard_id(&self) -> String {
        self.shared.observed.borrow().shard_id.clone()
    }

    /// This worker's id, fresh for this process.
    fn worker_id(&self) -> String {
        self.worker_id.clone()
    }

    /// The address this worker gives other workers to reach it at.
    fn address(&self) -> String {
        self.address.clone()
    }

    /// The leader this worker's node names and its term, or `None`.
    fn leader(&self) -> Option<(String, u64)> {
        let seen = self.shared.observed.borrow();
        seen.leader
            .as_ref()
            .map(|(leader, term)| (leader.as_str().to_owned(), *term))
    }

    /// How many voters this worker's node counts in its shard.
    fn voters(&self) -> usize {
        self.shared.observed.borrow().voters
    }

    /// Returns an awaitable that resolves once this worker has joined its
    /// shard and knows its leader. Raises `RuntimeError` if the worker stops
    /// first.
    fn wait_until_ready<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let mut observed = self.shared.observed.clone();
        self.host.bridge().spawn_into_py(py, async move {
            let gone = observed
                .wait_for(|seen| seen.gone || seen.leader.is_some())
                .await
                .map_err(|_| PyRuntimeError::new_err(GONE_MESSAGE))?
                .gone;
            if gone {
                return Err(PyRuntimeError::new_err(
                    "the worker stopped before it joined its shard",
                ));
            }
            Ok(())
        })
    }

    /// Mints a task and returns its id at once; the task is then delivered
    /// to the shard's leader, which `ACCEPTED` or `REFUSED` in
    /// `next_events()` says the end of. `submission` as for
    /// `NativeRuntime.submit`.
    ///
    /// An input larger than one message is stored in the shard as blobs
    /// before the submission is sent. While it is stored it counts against
    /// this process's memory limits: past the hard limit with others in
    /// flight, `BackpressureError`.
    ///
    /// Raises `RuntimeError` once the runtime has shut down, `ValueError`
    /// if the submission's `kind`, `key` and `drop_oldest` disagree or its
    /// `reconnect_timeout_ms` is zero, and `BackpressureError` if the
    /// input's blobs do not fit this process's room for payloads in flight
    /// now.
    fn submit(&self, py: Python<'_>, submission: SubmitArguments) -> PyResult<String> {
        if !self.host.bridge().is_open() {
            return Err(PyRuntimeError::new_err(CLOSED_MESSAGE));
        }
        let mut submission = submission
            .into_submission(py)?
            .with_client(self.shared.net.local_worker_id());
        // Too large for one message: delivered as blobs, stored before the
        // submission is sent, and held here until they are. The submission
        // goes out with no input of its own.
        let as_blobs =
            submission.coalescing_key.is_none() && !fits_inline(submission.serialized_input.len());
        let payload = if as_blobs {
            let payload = std::mem::take(&mut submission.serialized_input);
            match self.shared.net.payload_memory().try_take(payload.len() as u64) {
                Ok(Some(taken)) => Some((payload, taken)),
                // Too many payloads in flight now: refused for now, like the
                // leader's backpressure. One alone past the hard limit is
                // refused by the session first, so it gets the same.
                Ok(None) | Err(_) => {
                    return Err(backpressure_error(
                        py,
                        "too many large payloads in flight past the hard memory limit".into(),
                    ));
                }
            }
        } else {
            None
        };
        Ok(send_submission(
            &self.shared,
            self.host.handle(),
            mint(submission, &Uuid7Ids, &self.clock),
            payload,
        ))
    }

    /// Offers the driver up to `limit` free places, and returns an
    /// awaitable that resolves to the runs it hands over, at most `limit`,
    /// as soon as there is one. An empty list once `stop_taking()` was
    /// called, which also ends a claim already waiting. Raises `ValueError`
    /// if `limit` is zero, and `RuntimeError` if the worker leaves its shard.
    ///
    /// The awaitable must not be cancelled while it waits, except at
    /// shutdown: runs handed over after the cancel stay held here and are
    /// never run. Call `stop_taking()` to end a waiting claim instead.
    fn claim_pending<'py>(&self, py: Python<'py>, limit: usize) -> PyResult<Bound<'py, PyAny>> {
        if limit == 0 {
            return Err(PyValueError::new_err("limit must be at least 1"));
        }
        self.shared.offer(limit);
        let shared = Arc::clone(&self.shared);
        self.host.bridge().spawn_into_py(py, async move {
            shared.until(|shared| shared.take_claims(limit)).await
        })
    }

    /// Returns an awaitable that resolves to the events raised since the
    /// last call, oldest first, as soon as there is one. Raises
    /// `RuntimeError` if the worker leaves its shard.
    ///
    /// The awaitable must not be cancelled while it waits, except at
    /// shutdown: events it took before the cancel are lost.
    fn next_events<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let shared = Arc::clone(&self.shared);
        self.host
            .bridge()
            .spawn_into_py(py, async move { shared.until(Shared::take_events).await })
    }

    /// Hands the leader a started report for a run this worker holds.
    fn report_started(&self, task_run_id: &str) {
        let run = TaskRunId::new(task_run_id);
        if self.shared.holds(&run) {
            self.shared.report(Report::Started(run));
        }
    }

    /// Hands the leader a run's success, with the digest of its result.
    /// `result`, the result itself, is first delivered to the task's client,
    /// for up to the run's reconnect timeout, and the report says whether it
    /// arrived. Raises `ValueError` for a digest of the wrong length, and for
    /// `continues`: a networked worker certifies no continuation.
    #[pyo3(signature = (task_run_id, result_digest, continues = false, *, result = None))]
    fn complete(
        &self,
        task_run_id: &str,
        result_digest: &[u8],
        continues: bool,
        result: Option<Vec<u8>>,
    ) -> PyResult<()> {
        if continues {
            return Err(PyValueError::new_err(
                "a networked worker certifies no continuation: its stages would be orchestrated \
                 in this process",
            ));
        }
        let digest = Digest::new(DigestAlgorithm::Blake3, result_digest.to_vec())
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        let run = TaskRunId::new(task_run_id);
        let Some(held) = self.shared.end(&run) else {
            return Ok(());
        };
        match (held.client.clone(), result) {
            (Some(client), Some(payload)) => {
                self.host.handle().spawn(deliver_then_report(
                    Arc::clone(&self.shared),
                    client,
                    held,
                    run,
                    digest,
                    payload,
                ));
            }
            _ => self.shared.report(Report::Completed {
                run,
                digest,
                delivered: false,
                continuation: None,
            }),
        }
        Ok(())
    }

    /// Hands the leader a run's failure, by its error's type name and
    /// message (never its traceback). The message is stored in the task's
    /// replicated record, so it can contain task data.
    #[pyo3(signature = (task_run_id, failure_kind, failure_message = ""))]
    fn report_failure(&self, task_run_id: &str, failure_kind: &str, failure_message: &str) {
        self.shared
            .finish(TaskRunId::new(task_run_id), |run| Report::Failed {
                run,
                reason: failure_kind.to_owned(),
                message: failure_message.to_owned(),
            });
    }

    /// Hands the leader a run lost with the process that ran it, or stopped
    /// at its abort deadline.
    fn report_lost(&self, task_run_id: &str) {
        self.shared
            .finish(TaskRunId::new(task_run_id), Report::Lost);
    }

    /// Hands the leader a compaction run's fold.
    fn complete_compaction(&self, task_run_id: &str, folded: &[u8]) {
        self.shared
            .finish(TaskRunId::new(task_run_id), |run| Report::Compacted {
                run,
                payload: folded.to_vec(),
            });
    }

    /// No continuation is ever certified here, so there is none to end.
    fn end_continuation(&self, _task_id: &str) -> bool {
        false
    }

    /// Asks the shard's leader to cancel a task, in the background, and
    /// returns at once: whether it was cancelled is the leader's to say,
    /// and the task's record shows it. Raises `RuntimeError` once the
    /// runtime has shut down.
    fn cancel(&self, task_id: &str) -> PyResult<()> {
        if !self.host.bridge().is_open() {
            return Err(PyRuntimeError::new_err(CLOSED_MESSAGE));
        }
        self.host.handle().spawn(cancel_at_leader(
            Arc::clone(&self.shared),
            TaskId::new(task_id),
        ));
        Ok(())
    }

    /// Returns an awaitable that resolves to the newest record of a task the
    /// shard holds, or `None` if no worker this one reaches holds it.
    fn task_record<'py>(&self, py: Python<'py>, task_id: String) -> PyResult<Bound<'py, PyAny>> {
        let net = Arc::clone(&self.shared.net);
        self.host.bridge().spawn_into_py(py, async move {
            Ok(net
                .get_record(TaskId::new(task_id))
                .await
                .map(PyTaskRecord::from))
        })
    }

    /// Takes no more runs: those handed over and not taken, or still
    /// being fetched, are reported lost, later ones too, and a claim waiting resolves empty. The driver
    /// may still hand over up to the places already offered; each is
    /// reported lost as it arrives.
    fn stop_taking(&self) {
        self.shared.stop_taking();
    }

    /// Returns an awaitable that resolves once the leader has taken the
    /// last report on every run this worker claimed (`True`), or the
    /// reconnect timeout has passed or the worker left its shard (`False`).
    fn wait_until_reported<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let net = Arc::clone(&self.shared.net);
        let observed = self.shared.observed.clone();
        let within = self.reconnect_timeout;
        self.host.bridge().spawn_into_py(py, async move {
            let deadline = tokio::time::Instant::now() + within;
            while !net.claimed_runs().snapshot().is_empty() {
                if observed.borrow().gone || tokio::time::Instant::now() >= deadline {
                    return Ok(false);
                }
                tokio::time::sleep(REPORTED_POLL).await;
            }
            Ok(true)
        })
    }

    /// Asks the worker to leave its shard gracefully, a leader handing its
    /// leadership over first, and returns an awaitable that resolves once it
    /// has left to `(stored, superseded, abandoned)`: how many records it
    /// handed over, how many the shard had already written past, and the keys
    /// of those no other worker took, which leave with it (a Task record's
    /// is its task id). A worker already
    /// gone, or shut down, resolves at once with nothing handed over.
    fn drain<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.shared.net.request_drain();
        let mut observed = self.shared.observed.clone();
        let drained = Arc::clone(&self.drained);
        self.host.bridge().spawn_into_py(py, async move {
            while !observed.borrow_and_update().gone {
                if observed.changed().await.is_err() {
                    break;
                }
            }
            let handed = drained
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take()
                .unwrap_or_default();
            let abandoned: Vec<String> = handed
                .abandoned
                .iter()
                .map(|key| String::from_utf8_lossy(&key.kad_bytes()).into_owned())
                .collect();
            Ok((handed.stored, handed.superseded, abandoned))
        })
    }

    /// Stops the worker at once and shuts the runtime down; awaitables
    /// still pending fail. Does nothing if already shut down.
    fn shutdown(&self, py: Python<'_>) {
        self.host.shutdown(py, &self.shared);
    }
}
