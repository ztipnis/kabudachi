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
//! A composite is submitted and cancelled the same way, and its end comes
//! back as `COMPOSITE_FINISHED`. The worker keeps the composites it awaits
//! (those it submitted, and the continuations of tasks it submitted), and
//! after a change of leader reads their records, so a notice lost with a
//! leader still settles them. The records, pages and blobs of a composite are
//! read from the shard.
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

mod composites;
mod delivery;
mod handover;
mod host;
mod observed;
mod results;
mod seam;
mod slow_down;
mod start;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration as StdDuration;

use kabudachi_core::blob::{BlobKey, BlobOwner, MAX_BLOB_BYTES};
use kabudachi_core::manifest::{Manifest, fits_inline};
use kabudachi_core::protocol::digest::{Digest, DigestAlgorithm};
use kabudachi_core::protocol::ids::{CompositeId, IdGenerator, TaskId, TaskRunId, Uuid7Ids};
use kabudachi_core::protocol::messages::Task;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::composite::{
    MEMBER_BATCH_BYTES, SubmittedComposite, expansion_source, member_input, reduce_operands, results,
};
use kabudachi_core::result_backend::{PersistenceState, ResultBounds, composite_entries};
use kabudachi_core::scheduler::{SubmitRejection, mint};
use kabudachi_core::time::{RealClock, WallTime};
use kabudachi_net::blobs::BlobRole;
use kabudachi_net::composites::{CountingSource, NetSource};
use kabudachi_net::executor::{Report, executor_channel};
use kabudachi_net::handoff::HandedOff;
use kabudachi_net::payload_memory::Taken;
use kabudachi_net::worker::Worker;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use tokio::sync::watch;

use crate::arguments::SubmitArguments;
use crate::bridge::CLOSED_MESSAGE;
use crate::composite::{
    CompositeStatusTuple, PyBytesValue, PyManifest, PyMemberInput, PyReduceOperands, PyResolved, member_batch,
    record_status, refuse_values_past_limit, spill_step, step_from_tree,
};
use crate::errors::backpressure_error;
use crate::health;
use crate::result_backend::{
    Mirror, ResumedValue, backend_of, expiring_in, keep_for, mirror, persistence_for, resume_composite, resume_task,
    tree_from_py,
};
use crate::work::PyTaskRecord;

use self::composites::{
    cancel_composite_at_leader, read_until, send_composite, send_members, stored_until,
};
use self::delivery::{cancel_at_leader, send_submission};
use self::host::Host;
use self::observed::Observed;
use self::results::{deliver_then_report, store_then_report_step};
use self::seam::{GONE_MESSAGE, Shared};
use self::start::NetworkedArguments;

const REPORTED_POLL: StdDuration = StdDuration::from_millis(20);

/// What a write to the shard that gave up raises: the run it belongs to
/// fails, and its task's policy retries it.
const NOT_STORED_MESSAGE: &str = "the shard could not store the payload in time";

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
    /// Raises `ValueError` for an address that cannot be read, a timing,
    /// authority TTL or `version_block_ttl_ms` of zero (`drain_wait_limit_ms`,
    /// which bounds each wait of a leaving worker, included), a replication
    /// factor or seed rounds of zero, only one memory limit, no worker
    /// threads, an authority that cannot be used, no queues, a blank queue
    /// name or queue options that are refused (a zero weight, running limit
    /// or starvation age, or a queue given twice), and `RuntimeError` if it
    /// cannot listen on its listen address. `health_listen` raises
    /// `ValueError` for an address that is not `ip:port` and `RuntimeError`
    /// when it cannot be bound.
    ///
    /// `result_backend`, a `RedisResultBackend` or `PythonResultBackend`,
    /// is where this worker mirrors the results of runs the leader has
    /// certified; `TypeError` for anything else.
    #[new]
    #[pyo3(signature = (arguments, result_backend = None))]
    fn new(
        py: Python<'_>,
        arguments: NetworkedArguments,
        result_backend: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let backend = backend_of(result_backend)?;
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
            backend.clone(),
            arguments.result_ttl_ms,
            tokio.handle().clone(),
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

    /// Hands the leader a started report for a run this worker holds, at
    /// `execution_version` of its input if it migrated the input.
    #[pyo3(signature = (task_run_id, execution_version = None))]
    fn report_started(&self, task_run_id: &str, execution_version: Option<u32>) {
        let run = TaskRunId::new(task_run_id);
        if self.shared.holds(&run) {
            self.shared.report(Report::Started { run, execution_version });
        }
    }

    /// Tells the leader, in the background, that this worker cannot run the
    /// input of `task_id` at its version, for `reason`; it returns at once. The
    /// leader gives the claim back and hands this worker the task no more.
    /// With `incompatible` the input can never be migrated and the task ends.
    /// A run Python held for the task is over.
    #[pyo3(signature = (task_id, reason, incompatible = false))]
    fn report_blocked(&self, task_id: &str, reason: String, incompatible: bool) {
        self.shared.report_blocked(TaskId::new(task_id), reason, incompatible);
    }

    /// Hands the leader a run's success. Give either `result_digest`, the
    /// digest of the result, or `continuation`, the tree of the step the run
    /// returned (see `describe_step`): the step runs as a composite and the
    /// run is certified under the digest of its description. `result`, the
    /// result itself, is first delivered to the task's client, for up to the
    /// run's reconnect timeout, and the report says whether it arrived. The
    /// values of a `continuation` too large to inline are first stored as
    /// blobs the composite owns, nested stages' included; a run whose values
    /// cannot be stored by its reconnect timeout is reported lost.
    ///
    /// Raises `ValueError` if neither or both are given, for a digest of the
    /// wrong length, and for a `continuation` that is malformed or takes an
    /// input, and `BackpressureError` if a value of the step alone is past
    /// the hard memory limit.
    #[pyo3(signature = (task_run_id, result_digest = None, continuation = None, *, result = None))]
    fn complete(
        &self,
        py: Python<'_>,
        task_run_id: &str,
        result_digest: Option<&[u8]>,
        continuation: Option<&Bound<'_, PyAny>>,
        result: Option<Vec<u8>>,
    ) -> PyResult<()> {
        let run = TaskRunId::new(task_run_id);
        let digest = match (result_digest, continuation) {
            (Some(digest), None) => Digest::new(DigestAlgorithm::Blake3, digest.to_vec())
                .map_err(|error| PyValueError::new_err(error.to_string()))?,
            (None, Some(tree)) => return self.complete_with_step(py, run, tree),
            _ => {
                return Err(PyValueError::new_err(
                    "give either result_digest or continuation, not both or neither",
                ));
            }
        };
        let Some(held) = self.shared.end(&run) else {
            return Ok(());
        };
        let persistence = persistence_for(
            self.shared.backend.is_some(),
            result.as_deref().map(|result| (result, &digest)),
            held.member.is_some(),
            held.result_ttl_ms,
        );
        if let (PersistenceState::Pending, Some(payload)) = (persistence, &result) {
            // Kept before the report goes, so the leader's answer finds it.
            self.shared.seam().mirroring.insert(
                run.clone(),
                Mirror {
                    task: held.task.clone(),
                    run: run.clone(),
                    digest: digest.clone(),
                    payload: payload.clone(),
                    keep_for: keep_for(held.result_ttl_ms, self.shared.result_ttl_ms),
                },
            );
        }
        match (held.client.clone(), result) {
            (Some(client), Some(payload)) => {
                self.host.handle().spawn(deliver_then_report(
                    Arc::clone(&self.shared),
                    client,
                    held,
                    run,
                    digest,
                    payload,
                    persistence,
                ));
            }
            _ => self.shared.report(Report::Completed {
                run,
                digest,
                delivered: false,
                continuation: None,
                persistence,
                result_manifest: None,
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

    /// Mints a composite for the tree `tree` describes (see `describe_step`)
    /// and returns its id at once; the composite is then delivered to the
    /// shard's leader, which `ACCEPTED` or `REFUSED` in `next_events()` says
    /// the end of (`task_id` is the composite's id), and `COMPOSITE_FINISHED`
    /// says it is over. Values of the tree too large to inline are first
    /// stored in the shard as blobs the composite owns, nested stages'
    /// included, and count against this process's memory limits until they
    /// are.
    ///
    /// Raises `ValueError` if the tree describes a step the leader would not
    /// accept, or one that takes an input, `BackpressureError` if its values
    /// do not fit this process's room for payloads in flight now or one alone
    /// is past the hard limit, and `RuntimeError` once the runtime has shut
    /// down.
    fn submit_composite(&self, py: Python<'_>, tree: &Bound<'_, PyAny>) -> PyResult<String> {
        if !self.host.bridge().is_open() {
            return Err(PyRuntimeError::new_err(CLOSED_MESSAGE));
        }
        let step = step_from_tree(tree)?;
        if step.takes_input() {
            return Err(PyValueError::new_err(SubmitRejection::StepTakesInput.to_string()));
        }
        let (step, values) = spill_step(step);
        let stored = self.take_in_flight(py, values)?;
        let submitted = SubmittedComposite {
            composite_id: Uuid7Ids.next_composite_id(),
            submitted_at: WallTime::now(&self.clock),
            step,
            client: Some(self.shared.net.local_worker_id()),
        };
        Ok(send_composite(&self.shared, self.host.handle(), submitted, stored))
    }

    /// Returns an awaitable that resolves to `"accepted"` when the shard's
    /// leader holds `members`, the ones from `start_index` on of the streamed
    /// composite `composite_id`, or `"finished"` when the composite is over
    /// and takes no more: send no further batch. Members, `last` and
    /// `inline_bytes` are as for `NativeRuntime.submit_members`. The composite
    /// must have been submitted here, or be one this worker's run streams
    /// into; one submitted here waits for its own submission to be answered
    /// first (resolving `"finished"` if it was refused). A member whose
    /// values are stored as blobs has them stored where the leader places the
    /// composite's record first, and they count against this process's memory
    /// limits until they are. The leader is asked again after a refusal for
    /// now, so an answer lost with a leader creates nothing twice.
    ///
    /// Raises `ValueError` if the batch is malformed or cannot go in one
    /// message even with its values stored as blobs or the leader finds it
    /// malformed, `BackpressureError` if its values do not fit this process's
    /// room for payloads in flight now, one alone is past the hard limit, the
    /// shard cannot store them now, or the leader refuses the batch as
    /// backpressure (nothing of it is taken), and `RuntimeError` once the
    /// runtime has shut down or the worker has left its shard.
    #[pyo3(signature = (composite_id, start_index, members, last, inline_bytes = MEMBER_BATCH_BYTES))]
    fn submit_members<'py>(
        &self,
        py: Python<'py>,
        composite_id: &str,
        start_index: u64,
        members: Vec<Bound<'py, PyAny>>,
        last: bool,
        inline_bytes: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        if !self.host.bridge().is_open() {
            return Err(PyRuntimeError::new_err(CLOSED_MESSAGE));
        }
        let (batch, values) = member_batch(composite_id, start_index, &members, last, inline_bytes)?;
        let stored = self.take_in_flight(py, values)?;
        let shared = Arc::clone(&self.shared);
        self.host
            .bridge()
            .spawn_into_py(py, async move { send_members(&shared, batch, stored).await })
    }

    /// Whether submitters are to wait: the shard's leader last said its memory
    /// is past its soft limit and a poll has not yet heard it clear, or this
    /// process's own payloads in flight are past their soft share and have
    /// not fallen below 80% of it. Raises `RuntimeError` once the runtime has
    /// shut down.
    fn slow_down(&self) -> PyResult<bool> {
        if !self.host.bridge().is_open() {
            return Err(PyRuntimeError::new_err(CLOSED_MESSAGE));
        }
        Ok(self.shared.slow_down())
    }

    /// Asks the shard's leader to cancel a composite, in the background, and
    /// returns at once: whether it was cancelled is the leader's to say, and
    /// `COMPOSITE_FINISHED` tells how it ended. Raises `RuntimeError` once
    /// the runtime has shut down.
    fn cancel_composite(&self, composite_id: &str) -> PyResult<()> {
        if !self.host.bridge().is_open() {
            return Err(PyRuntimeError::new_err(CLOSED_MESSAGE));
        }
        self.host.handle().spawn(cancel_composite_at_leader(
            Arc::clone(&self.shared),
            CompositeId::new(composite_id),
        ));
        Ok(())
    }

    /// Returns an awaitable that resolves to where a composite stands, as
    /// `(state, next_stage, pending, running, succeeded, failed, cancelled)`, by the
    /// newest record of it the shard holds that this worker reaches: not
    /// authoritative, so a change the leader has just made may not show yet.
    /// `None` if no worker this one reaches holds it. Reading the record asks
    /// the shard, so it is awaited rather than blocking the caller's loop.
    ///
    /// Raises `RuntimeError` once the runtime has shut down.
    fn composite_status<'py>(&self, py: Python<'py>, composite_id: String) -> PyResult<Bound<'py, PyAny>> {
        let net = Arc::clone(&self.shared.net);
        self.host.bridge().spawn_into_py(py, async move {
            let record = net.composite_record(&CompositeId::new(composite_id)).await;
            Ok::<Option<CompositeStatusTuple>, PyErr>(record.as_ref().map(record_status))
        })
    }

    /// Returns an awaitable that resolves to a composite's results: `("list",
    /// [...])` of its stages' or members' results in order, each `("value",
    /// producer_definition_id, bytes)`, `("failure", kind, message, task_id)` or a
    /// nested `("list", [...])`. A record, page or blob not found is asked
    /// for again for up to the reconnect timeout.
    ///
    /// Raises `RuntimeError` if the shard no longer holds the composite or a
    /// result it names.
    fn composite_results<'py>(&self, py: Python<'py>, composite_id: String) -> PyResult<Bound<'py, PyAny>> {
        let shared = Arc::clone(&self.shared);
        let within = self.reconnect_timeout;
        self.host.bridge().spawn_into_py(py, async move {
            let source = NetSource::new(Arc::clone(&shared.net));
            let id = CompositeId::new(composite_id);
            read_until(&shared, within, || results(&source, &id))
                .await
                .map(PyResolved)
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))
        })
    }

    /// Returns an awaitable that resolves to what the member run
    /// `task_run_id` is given besides its claim, as `(fields, input)` (see
    /// `NativeRuntime.resolve_input`). A record, page or blob not found is
    /// asked for again for up to the run's reconnect timeout.
    ///
    /// Raises `RuntimeError` if this worker does not hold the run or it is
    /// not a composite's member, or a result it takes is no longer held.
    fn resolve_input<'py>(&self, py: Python<'py>, task_run_id: &str) -> PyResult<Bound<'py, PyAny>> {
        let (task, within) = self.member(task_run_id)?;
        let shared = Arc::clone(&self.shared);
        let run = TaskRunId::new(task_run_id);
        self.host.bridge().spawn_into_py(py, async move {
            let source = CountingSource::new(Arc::clone(&shared.net));
            let input = read_until(&shared, within, || {
                source.begin();
                member_input(&source, &task)
            })
            .await
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            shared.hold_inputs(&run, source.counted());
            Ok(PyMemberInput(input))
        })
    }

    /// Returns an awaitable that resolves to what the reduce step run
    /// `task_run_id` works on (see `NativeRuntime.reduce_operands`). A record,
    /// page or blob not found is asked for again for up to the run's
    /// reconnect timeout.
    ///
    /// Raises `RuntimeError` if this worker does not hold the run or it is not
    /// a reduce step's, or an operand is no longer held.
    fn reduce_operands<'py>(&self, py: Python<'py>, task_run_id: &str) -> PyResult<Bound<'py, PyAny>> {
        let (task, within) = self.member(task_run_id)?;
        let shared = Arc::clone(&self.shared);
        let run = TaskRunId::new(task_run_id);
        self.host.bridge().spawn_into_py(py, async move {
            let source = CountingSource::new(Arc::clone(&shared.net));
            let operands = read_until(&shared, within, || {
                source.begin();
                reduce_operands(&source, &task)
            })
            .await
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            shared.hold_inputs(&run, source.counted());
            Ok(PyReduceOperands(operands))
        })
    }

    /// Returns an awaitable that resolves to the list the expansion run
    /// `task_run_id` is to split (see `NativeRuntime.resolve_expansion`). A
    /// record, page or blob not found is asked for again for up to the run's
    /// reconnect timeout.
    ///
    /// Raises `RuntimeError` if this worker does not hold the run or it is no
    /// expansion, or the list is no longer held.
    fn resolve_expansion<'py>(&self, py: Python<'py>, task_run_id: &str) -> PyResult<Bound<'py, PyAny>> {
        let (task, within) = self.member(task_run_id)?;
        let shared = Arc::clone(&self.shared);
        let run = TaskRunId::new(task_run_id);
        self.host.bridge().spawn_into_py(py, async move {
            let source = CountingSource::new(Arc::clone(&shared.net));
            let list = read_until(&shared, within, || {
                source.begin();
                expansion_source(&source, &task)
            })
            .await
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            shared.hold_inputs(&run, source.counted());
            Ok(PyResolved(list))
        })
    }

    /// Returns an awaitable that resolves to the digest of a result: `data`
    /// stored as blobs the task `task_id` owns, then the manifest naming them
    /// as one more blob, which a composite's page names as the member's
    /// result. Report that digest with `complete`. The blobs are stored where
    /// the shard's leader places the task's record, and asked again for up to
    /// the reconnect timeout.
    ///
    /// Raises `BackpressureError` if `data` is past the hard memory limit.
    /// Resolves to `None` if the shard could not store it in time (it has no
    /// leader, say): the run is then reported lost, so it is replayed.
    fn store_result<'py>(&self, py: Python<'py>, task_id: String, data: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
        refuse_values_past_limit(py, self.shared.net.payload_memory().hard_limit(), std::slice::from_ref(&data))?;
        let cut = Manifest::of_result(&data);
        let manifest = cut.manifest.encode();
        if manifest.len() > MAX_BLOB_BYTES {
            return Err(backpressure_error(
                py,
                "the result is too large to be named by one manifest blob".to_owned(),
            ));
        }
        let digest = BlobKey::of(&manifest).digest().value().to_vec();
        let mut blobs = cut.chunks;
        blobs.push(manifest);
        let shared = Arc::clone(&self.shared);
        let within = self.reconnect_timeout;
        let owner = BlobOwner::Task(TaskId::new(task_id));
        self.host.bridge().spawn_into_py(py, async move {
            // The result is in this process's memory already: counted, not
            // waited for, while it is stored.
            let _in_flight = shared.net.payload_memory().take_anyway(data.len() as u64).ok();
            let bound = tokio::time::Instant::now() + within;
            let stored = stored_until(&shared, bound, || {
                shared
                    .net
                    .store_blobs_at_leader(BlobRole::Result, &owner, blobs.clone(), shared.retry)
            })
            .await;
            Ok(stored.map(|()| PyBytesValue(digest)))
        })
    }

    /// Returns an awaitable that resolves to the opaque manifest of `data`,
    /// stored as blobs the composite `composite_id` owns, where the shard's
    /// leader places its record. The manifest goes into a tree as
    /// `("manifest", manifest)`, in a tree whose root is that composite.
    ///
    /// Raises `BackpressureError` if `data` is past the hard memory limit,
    /// and `RuntimeError` if the shard could not store it within the
    /// reconnect timeout.
    fn store_composite_payload<'py>(
        &self,
        py: Python<'py>,
        composite_id: String,
        data: Vec<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        refuse_values_past_limit(py, self.shared.net.payload_memory().hard_limit(), std::slice::from_ref(&data))?;
        let shared = Arc::clone(&self.shared);
        let within = self.reconnect_timeout;
        let owner = BlobOwner::Composite(CompositeId::new(composite_id));
        self.host.bridge().spawn_into_py(py, async move {
            let _in_flight = shared.net.payload_memory().take_anyway(data.len() as u64).ok();
            let bound = tokio::time::Instant::now() + within;
            stored_until(&shared, bound, || {
                shared
                    .net
                    .store_payload_at_leader(BlobRole::Input, &owner, &data, shared.retry)
            })
            .await
            .map(PyManifest)
            .ok_or_else(|| PyRuntimeError::new_err(NOT_STORED_MESSAGE))
        })
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

    /// Stores a finished composite's resolved `results` (see `Resumed.results`
    /// for the tree's form) in the result backend for `expires_in_ms`, in the
    /// background. Does nothing without a backend; a failure of the backend is
    /// logged, never raised.
    ///
    /// Raises `TypeError` if `results` is not a result tree.
    fn mirror_composite(
        &self,
        composite_id: &str,
        results: &Bound<'_, PyAny>,
        expires_in_ms: u64,
    ) -> PyResult<()> {
        let Some(backend) = self.shared.backend.clone() else {
            return Ok(());
        };
        let tree = tree_from_py(results)?;
        let id = CompositeId::new(composite_id);
        let expires_at = expiring_in(StdDuration::from_millis(expires_in_ms));
        self.host.bridge().spawn_detached(async move {
            mirror(backend, move || {
                composite_entries(&ResultBounds::DEFAULT, &id, &tree, expires_at)
            })
            .await;
        });
        Ok(())
    }

    /// Returns an awaitable that resolves to a `Resumed`: where the task
    /// stands by the newest record of it the shard holds that this worker
    /// reaches, and, once it succeeded, its value from the result backend,
    /// checked against the record. A task that returned a step resumes as its
    /// composite.
    fn resume_task<'py>(&self, py: Python<'py>, task_id: String) -> PyResult<Bound<'py, PyAny>> {
        let net = Arc::clone(&self.shared.net);
        let runtime_ttl_ms = self.shared.result_ttl_ms;
        let backend = self.shared.backend.clone();
        self.host.bridge().spawn_into_py(py, async move {
            let task = TaskId::new(task_id);
            let record = net.get_record(task.clone()).await;
            let (composite, resumption) = resume_task(task, record, &NetSource::new(net), backend, runtime_ttl_ms).await;
            Ok(ResumedValue(composite, resumption))
        })
    }

    /// Returns an awaitable that resolves to a `Resumed` for a composite, as
    /// `resume_task` does for a task: the record if the shard still holds it,
    /// then the results reassembled from the backend, or else from the shard
    /// while it holds them.
    fn resume_composite<'py>(&self, py: Python<'py>, composite_id: String) -> PyResult<Bound<'py, PyAny>> {
        let net = Arc::clone(&self.shared.net);
        let backend = self.shared.backend.clone();
        self.host.bridge().spawn_into_py(py, async move {
            let id = CompositeId::new(composite_id);
            let resumption = resume_composite(id.clone(), &NetSource::new(net), backend).await;
            Ok(ResumedValue(Some(id), resumption))
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
    /// last report on every run this worker claimed and every result it is
    /// mirroring is stored and reported (`True`), or the reconnect timeout
    /// has passed or the worker left its shard (`False`). A mirror still
    /// running then leaves its run's persistence `"pending"`.
    fn wait_until_reported<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let net = Arc::clone(&self.shared.net);
        let shared = Arc::clone(&self.shared);
        let observed = self.shared.observed.clone();
        let within = self.reconnect_timeout;
        self.host.bridge().spawn_into_py(py, async move {
            let deadline = tokio::time::Instant::now() + within;
            while !net.claimed_runs().snapshot().is_empty() || shared.mirrors_pending() {
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

impl NetworkedRuntime {
    /// Counts `values`, bound inputs to be stored as blobs, in flight until
    /// each is stored. Raises `BackpressureError` if one alone is past the
    /// hard memory limit or they do not all fit now; whatever was counted so
    /// far is then dropped, and counts no more.
    fn take_in_flight(&self, py: Python<'_>, values: Vec<Vec<u8>>) -> PyResult<Vec<(Vec<u8>, Taken)>> {
        refuse_values_past_limit(py, self.shared.net.payload_memory().hard_limit(), &values)?;
        let mut taken = Vec::with_capacity(values.len());
        for value in values {
            let Ok(Some(counted)) = self.shared.net.payload_memory().try_take(value.len() as u64) else {
                return Err(backpressure_error(
                    py,
                    "too many large payloads in flight past the hard memory limit".into(),
                ));
            };
            taken.push((value, counted));
        }
        Ok(taken)
    }

    /// Hands the leader a run's success by the step `tree` describes. The
    /// values too large to inline are stored first, under the composite the
    /// step runs as, so the report finds them held.
    fn complete_with_step(&self, py: Python<'_>, run: TaskRunId, tree: &Bound<'_, PyAny>) -> PyResult<()> {
        let step = step_from_tree(tree)?;
        if step.takes_input() {
            return Err(PyValueError::new_err("a returned step cannot take an input"));
        }
        let (step, values) = spill_step(step);
        refuse_values_past_limit(py, self.shared.net.payload_memory().hard_limit(), &values)?;
        let Some(held) = self.shared.end(&run) else {
            return Ok(());
        };
        self.host.handle().spawn(store_then_report_step(
            Arc::clone(&self.shared),
            held,
            run,
            step,
            values,
        ));
        Ok(())
    }

    /// The task of the member run `task_run_id` this worker holds, and how
    /// long a read for it is asked again.
    fn member(&self, task_run_id: &str) -> PyResult<(Task, StdDuration)> {
        self.shared
            .member_of(&TaskRunId::new(task_run_id))
            .ok_or_else(|| PyRuntimeError::new_err("no such run"))
    }
}
