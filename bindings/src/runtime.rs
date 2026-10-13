use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use kabudachi_core::blob::{BlobKey, BlobOwner, MAX_BLOB_BYTES};
use kabudachi_core::manifest::Manifest;
use kabudachi_core::protocol::digest::{Digest, DigestAlgorithm};
use kabudachi_core::protocol::ids::{
    CompositeId, IncarnationId, ShardName, TaskId, TaskRunId, Uuid7Ids, WorkerId,
};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::composite::{
    MEMBER_BATCH_BYTES, ReduceOperand, expansion_source, member_input, reduce_operands, results,
};
use kabudachi_core::scheduler::{Completion, ReportRejection, Scheduler, SubmitRejection};
use kabudachi_core::task_record::LocalRecords;
use kabudachi_core::time::{Duration as CoreDuration, RealClock};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;

use crate::arguments::{SubmitArguments, memory_limits};
use crate::bridge::{Bridge, CLOSED_MESSAGE};
use crate::composite::{
    PyBytesValue, PyManifest, PyMemberInput, PyReduceOperands, PyResolved, member_batch, refuse_values_past_limit, spill_step,
    state_name, step_from_tree,
};
use crate::door::{Closed, DoorSource, Refusal, SchedulerDoor};
use crate::election::{Publisher, run_election};
use crate::errors;
use crate::health;
use crate::local_node::local_node;
use crate::outcomes::{PyCancelOutcome, PyRunState};
use crate::queues::{QueueOptionsArgument, queue_policy, subscription};
use crate::timers::run_timers;
use crate::work::{PyCertification, PyClaim, PyEvent, PyTaskRecord};

/// The shard of a process that runs no cluster: just this worker.
const LOCAL_SHARD_NAME: &str = "local";
const SHUTDOWN_GRACE: Duration = Duration::from_secs(1);

const ELECTION_STOPPED_MESSAGE: &str = "the election task stopped unexpectedly";

/// How a one-node runtime runs, read from the attributes of a
/// `kabudachi.native_protocol.OneNodeArguments`.
#[derive(FromPyObject)]
pub(crate) struct OneNodeArguments {
    worker_threads: usize,
    suspect_timeout_ms: u64,
    result_ttl_ms: u64,
    memory_soft_limit: Option<u64>,
    memory_hard_limit: Option<u64>,
    health_listen: Option<String>,
    queues: Option<Vec<String>>,
    queue_options: Vec<QueueOptionsArgument>,
}

/// Counts the runtime's native loops still running; each loop holds one and
/// gives it back however it ends, a panic included.
struct LoopRunning(Arc<AtomicUsize>);

impl Drop for LoopRunning {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

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
    /// The incarnation of the shard this runtime founded at start.
    shard_id: String,
    /// The one way into the scheduler, which refuses everything once the
    /// runtime has shut down.
    door: Arc<SchedulerDoor<RealClock>>,
    /// The hard memory limit, which is the largest payload this runtime
    /// holds whole.
    payload_limit: Option<u64>,
    worker_id: String,
    /// Set once Python has loaded the worker's tasks and can take work.
    registry: Arc<AtomicBool>,
    health: Option<SocketAddr>,
}

#[pymethods]
impl NativeRuntime {
    /// Starts the runtime and begins electing this worker leader.
    ///
    /// `worker_id` and `incarnation_id` identify this worker and this start
    /// of it. `arguments` is a `kabudachi.native_protocol.OneNodeArguments`,
    /// which says how the runtime runs and what each of its fields means.
    ///
    /// Raises `ValueError` if `worker_threads` is zero, only one of the two
    /// memory limits is given, `queues` is empty or names an empty queue,
    /// `queue_options` gives a weight, running limit or starvation age of
    /// zero, or a queue twice, or if `health_listen` is not an `ip:port`
    /// address; `RuntimeError` if `health_listen` cannot be bound.
    #[new]
    fn new(
        worker_id: String,
        incarnation_id: String,
        arguments: OneNodeArguments,
    ) -> PyResult<Self> {
        if arguments.worker_threads == 0 {
            return Err(PyValueError::new_err("worker_threads must be at least 1"));
        }
        let limits = memory_limits(arguments.memory_soft_limit, arguments.memory_hard_limit)?;
        let subscription = subscription(arguments.queues)?;
        let policy = queue_policy(arguments.queue_options)?;

        let tokio = Builder::new_multi_thread()
            .worker_threads(arguments.worker_threads)
            .thread_name("kabudachi-native")
            .enable_all()
            .build()
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        let bridge = Bridge::new(tokio.handle().clone());

        let worker_name = worker_id.clone();
        let worker_id = WorkerId::new(worker_id);
        // One clock for everything: the scheduler's and the election node's
        // deadlines are ticks of it, so the timer and election loops have to
        // count the same ticks, and the scheduler checks the end of each
        // leadership grant the node hands it against its own reading of it.
        let clock = RealClock::new();
        let (node, first) = local_node(
            worker_id.clone(),
            IncarnationId::new(incarnation_id),
            &ShardName::new(LOCAL_SHARD_NAME),
            clock,
            CoreDuration::from_millis(arguments.suspect_timeout_ms),
        );
        let retention = CoreDuration::from_millis(arguments.result_ttl_ms);
        let mut new_scheduler =
            Scheduler::with_observer(
                clock,
                Uuid7Ids,
                LocalRecords::new(worker_id.clone(), clock, Some(retention)),
            );
        new_scheduler.set_result_ttl(Some(retention));
        new_scheduler.set_memory_limits(limits);
        new_scheduler.set_queue_policy(policy);
        new_scheduler.set_subscriptions(BTreeMap::from([(worker_id.clone(), subscription)]));
        let door = Arc::new(SchedulerDoor::new(new_scheduler, worker_id));
        let shard_id = node.shard_id().as_str().to_owned();
        let (state_sender, state) = watch::channel(node.state());
        let stop = Arc::new(Notify::new());
        let publisher = Publisher {
            state: state_sender,
            door: Arc::clone(&door),
        };
        let running = Arc::new(AtomicUsize::new(2));
        let registry = Arc::new(AtomicBool::new(false));
        // Bound before the loops start, so a port in use leaves nothing running.
        let health = match arguments.health_listen {
            Some(listen) => {
                let listener = health::bind(&listen)?;
                let ready_state = state.clone();
                let loaded = Arc::clone(&registry);
                let alive = Arc::clone(&running);
                Some(health::serve(
                    tokio.handle(),
                    listener,
                    health::Probe {
                        alive: Box::new(move || alive.load(Ordering::Acquire) == 2),
                        ready: Box::new(move || {
                            loaded.load(Ordering::Acquire)
                                && *ready_state.borrow() == WorkerState::Leader
                        }),
                    },
                )?)
            }
            None => None,
        };
        let election = tokio.spawn({
            let guard = LoopRunning(Arc::clone(&running));
            let stop = Arc::clone(&stop);
            async move {
                let _guard = guard;
                run_election(node, first, clock, stop, publisher).await
            }
        });

        let timers = tokio.spawn({
            let guard = LoopRunning(Arc::clone(&running));
            let door = Arc::clone(&door);
            async move {
                let _guard = guard;
                run_timers(door, clock).await
            }
        });

        Ok(NativeRuntime {
            tokio: Mutex::new(Some(tokio)),
            bridge,
            election: Mutex::new(Some(election)),
            timers: Mutex::new(Some(timers)),
            stop,
            state,
            shard_id,
            door,
            payload_limit: limits.map(|limits| limits.hard),
            worker_id: worker_name,
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
    /// now on while the worker leads.
    fn registry_loaded(&self) {
        self.registry.store(true, Ordering::Release);
    }

    /// This worker's id.
    fn worker_id(&self) -> String {
        self.worker_id.clone()
    }

    /// The name of this worker's shard: always `local`.
    fn shard_name(&self) -> String {
        LOCAL_SHARD_NAME.to_owned()
    }

    /// Whether a report's call returns the leader's decision (a
    /// certification, a retry): always, here, where this process leads.
    #[getter]
    fn certifies_locally(&self) -> bool {
        true
    }

    /// The largest payload this runtime holds whole: its hard memory limit,
    /// or `None` when no limit is set.
    #[getter]
    fn payload_limit(&self) -> Option<u64> {
        self.payload_limit
    }

    /// The incarnation of the shard this runtime founded at start: `local/`
    /// and an id unique to this start.
    fn shard_id(&self) -> String {
        self.shard_id.clone()
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
                .map(|_| {
                    tracing::info!(
                        target: "kabudachi_bindings::runtime",
                        "this worker leads its shard of one"
                    );
                })
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

    /// Records a new task and returns its ID; `submission` says what it is
    /// (see `kabudachi.native_protocol.SubmitArguments`). The task waits until
    /// a worker claims it. Every call is a new task.
    ///
    /// Raises `ValueError` if the submission's `kind`, `key` and `drop_oldest`
    /// disagree or its `reconnect_timeout_ms` is zero, `BackpressureError` if
    /// the task does not fit under the hard memory limit, is too large to be
    /// claimed (about 1 MiB with its queue and key), or, transiently, its
    /// coalescing key's newest generation is not yet known to the scheduler
    /// (submit again shortly), `BackpressureError` as well, though submitting
    /// again will not help, if the task's input alone is past the hard memory
    /// limit (the session refuses that first with `PayloadTooLargeError`, so
    /// only a caller of this method directly reaches it), and `RuntimeError`
    /// if the runtime has shut down. Before this worker leads the task is queued and its id
    /// returned at once; it is recorded, in order, when the worker comes to lead. A
    /// task whose record cannot yet fit stays queued, with everything behind
    /// it, until the generation it supersedes ends, and no error is raised
    /// for it.
    fn submit(&self, py: Python<'_>, submission: SubmitArguments) -> PyResult<String> {
        let submission = submission.into_submission(py)?;
        let task_id = self
            .door
            .submit(submission)
            .map_err(|refusal| {
                refused(refusal, |rejection| match rejection {
                    // The member-batch refusals answer a batch, not a task.
                    SubmitRejection::NotLeader
                    | SubmitRejection::NotReady
                    | SubmitRejection::UnknownComposite
                    | SubmitRejection::NotStreamed
                    | SubmitRejection::MemberGap { .. }
                    | SubmitRejection::MemberKind
                    | SubmitRejection::CompositeFinished => {
                        PyRuntimeError::new_err(rejection.to_string())
                    }
                    SubmitRejection::StepTakesInput => {
                        PyValueError::new_err(rejection.to_string())
                    }
                    SubmitRejection::TooLarge { .. }
                    | SubmitRejection::RecordTooLarge { .. }
                    | SubmitRejection::Backpressure { .. }
                    | SubmitRejection::KeyNotReady
                    | SubmitRejection::KeyBackpressure { .. }
                    // The session checks a payload against the hard memory limit
                    // first and raises `PayloadTooLargeError`, so this arm is
                    // reached only by a caller that submits to the runtime
                    // directly. It is not transient, but the native layer has no
                    // such error of its own to raise.
                    | SubmitRejection::PayloadTooLarge { .. } => {
                        errors::backpressure_error(py, rejection.to_string())
                    }
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

    /// Reports that a running run succeeded, and returns the certification of
    /// its result. Give either `result_digest`, the digest of the result, or
    /// `continuation`, the tree of the step the run returned (see
    /// `describe_step`): the step runs as a composite and the run is
    /// certified under the digest of its description, so the digest is not
    /// given. When the encoded step is large, its bound values longer than a
    /// manifest are stored as blobs the composite owns, nested stages'
    /// included, and the digest is that of the step as stored. The task is not over (its
    /// coalescing key stays held) until the composite is.
    ///
    /// Raises `ValueError` if neither or both are given, if `result_digest`
    /// is not a BLAKE3 digest's length, or if `continuation` is malformed or
    /// takes an input, `BackpressureError` if a value of the step alone is
    /// past the hard memory limit, `ContinuationTooLargeError` if the encoded
    /// step is past the largest a task's record may be (the run is then
    /// failed for good), and `RuntimeError` if the run is unknown,
    /// is not running, or is not this worker's; the result is then not
    /// certified.
    #[pyo3(signature = (task_run_id, result_digest = None, continuation = None))]
    fn complete(
        &self,
        py: Python<'_>,
        task_run_id: &str,
        result_digest: Option<&[u8]>,
        continuation: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyCertification> {
        let run = TaskRunId::new(task_run_id);
        let certification = match (result_digest, continuation) {
            (Some(digest), None) => {
                let digest = Digest::new(DigestAlgorithm::Blake3, digest.to_vec())
                    .map_err(|error| PyValueError::new_err(error.to_string()))?;
                self.door.complete(&run, digest, Completion::Final)
            }
            (None, Some(tree)) => {
                let step = step_from_tree(tree)?;
                if step.takes_input() {
                    return Err(PyValueError::new_err("a returned step cannot take an input"));
                }
                let (step, values) = spill_step(step);
                self.refuse_values_past_limit(py, &values)?;
                self.door.complete_with_step(&run, step, values)
            }
            _ => {
                return Err(PyValueError::new_err(
                    "give either result_digest or continuation, not both or neither",
                ));
            }
        };
        certification
            .map(Into::into)
            .map_err(|refusal| refused(refusal, rejected))
    }

    /// Records the composite `tree` describes (see `describe_step`) and
    /// returns its ID. A flow starts its first stage, and a group or map all
    /// its members, once this worker leads; a streamed group or map (see
    /// `submit_members`) starts its members as they arrive. When the encoded
    /// step is large, its bound values longer than a manifest are stored as
    /// blobs the composite owns, nested stages' included. The worker is told
    /// when it is over (`COMPOSITE_FINISHED`).
    ///
    /// Before this worker leads the composite is queued and its id returned
    /// at once, as a task's is; it is recorded, in order, when the worker
    /// comes to lead.
    ///
    /// Raises `ValueError` if the tree describes a step the leader would not
    /// accept, or one that takes an input, `BackpressureError` if the
    /// composite does not fit under the hard memory limit or a value of the
    /// tree alone is past it, and `RuntimeError` if the runtime has shut
    /// down.
    fn submit_composite(&self, py: Python<'_>, tree: &Bound<'_, PyAny>) -> PyResult<String> {
        let (step, values) = spill_step(step_from_tree(tree)?);
        self.refuse_values_past_limit(py, &values)?;
        let composite_id = self.door.submit_composite(step, values).map_err(|refusal| {
            refused(refusal, |rejection| match rejection {
                // The member-batch refusals answer a batch, not a composite.
                SubmitRejection::NotLeader
                | SubmitRejection::NotReady
                | SubmitRejection::UnknownComposite
                | SubmitRejection::NotStreamed
                | SubmitRejection::MemberGap { .. }
                | SubmitRejection::MemberKind
                | SubmitRejection::CompositeFinished => PyRuntimeError::new_err(rejection.to_string()),
                SubmitRejection::StepTakesInput => PyValueError::new_err(rejection.to_string()),
                SubmitRejection::TooLarge { .. }
                | SubmitRejection::RecordTooLarge { .. }
                | SubmitRejection::Backpressure { .. }
                | SubmitRejection::KeyNotReady
                | SubmitRejection::KeyBackpressure { .. }
                | SubmitRejection::PayloadTooLarge { .. } => {
                    errors::backpressure_error(py, rejection.to_string())
                }
            })
        })?;
        Ok(composite_id.as_str().to_owned())
    }

    /// Returns an awaitable that resolves to `"accepted"` when the streamed
    /// composite `composite_id` holds `members`, the ones from `start_index`
    /// on, or `"finished"` when the composite is over and takes no more: send
    /// no further batch. `members` are a streamed map's items, each its
    /// template's whole input as `bytes`, or a streamed group's members, each
    /// a stage tree as `describe_step` takes one. `last` says no member
    /// follows. A batch asked again, or overlapping the members already held,
    /// adds only the members the composite lacks. A member whose inline bytes
    /// pass `inline_bytes` (at most `MEMBER_BATCH_BYTES`; a larger value
    /// counts as that) has its bound values stored as blobs the composite
    /// owns, so a batch always fits one message. The batch is taken under the
    /// scheduler's lock before this returns; it is an awaitable as a
    /// networked runtime's is, which asks its leader.
    ///
    /// Raises `BackpressureError` if the batch would take memory past the
    /// hard limit or one of its values alone is past it (nothing of the batch
    /// is taken), `ValueError` if the batch is malformed, starts past the
    /// members held, adds to a composite whose last batch is in, does not
    /// fit the composite, or cannot go in one message even with its values
    /// stored as blobs, and `RuntimeError` if this worker is not the leader or
    /// the runtime has shut down.
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
        let answer = self.send_members(py, composite_id, start_index, &members, last, inline_bytes);
        self.bridge.spawn_into_py(py, async move { answer })
    }

    /// Whether memory in use is past the soft limit and has not fallen back
    /// below 80% of it: new work is to wait. Raises `RuntimeError` once the
    /// runtime has shut down.
    fn slow_down(&self) -> PyResult<bool> {
        Ok(self.door.slow_down()?)
    }

    /// Returns an awaitable that resolves to the digest of a result: `data`
    /// stored as blobs the task `task_id` owns, then the manifest naming them
    /// as one more blob, which a composite's page names as the member's
    /// result. Report that digest with `complete`.
    ///
    /// Raises `BackpressureError` if `data` is past the hard memory limit.
    fn store_result<'py>(&self, py: Python<'py>, task_id: String, data: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
        self.refuse_values_past_limit(py, std::slice::from_ref(&data))?;
        let cut = Manifest::of_result(&data);
        let manifest = cut.manifest.encode();
        if manifest.len() > MAX_BLOB_BYTES {
            return Err(errors::backpressure_error(
                py,
                "the result is too large to be named by one manifest blob".to_owned(),
            ));
        }
        let digest = BlobKey::of(&manifest).digest().value().to_vec();
        let mut blobs = cut.chunks;
        blobs.push(manifest);
        let owner = BlobOwner::Task(TaskId::new(task_id));
        let door = Arc::clone(&self.door);
        self.bridge.spawn_into_py(py, async move {
            door.store_result_blobs(&owner, blobs)?;
            Ok(PyBytesValue(digest))
        })
    }

    /// Returns an awaitable that resolves to the opaque manifest of `data`,
    /// stored as blobs the composite `composite_id` owns. The manifest goes
    /// into a tree as `("manifest", manifest)`, in a tree whose root is that
    /// composite.
    ///
    /// Raises `BackpressureError` if `data` is past the hard memory limit.
    fn store_composite_payload<'py>(
        &self,
        py: Python<'py>,
        composite_id: String,
        data: Vec<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.refuse_values_past_limit(py, std::slice::from_ref(&data))?;
        let door = Arc::clone(&self.door);
        self.bridge.spawn_into_py(py, async move {
            let cut = Manifest::chunk(&data);
            door.store_input_blobs(&BlobOwner::Composite(CompositeId::new(composite_id)), cut.chunks)?;
            Ok(PyManifest(cut.manifest))
        })
    }

    /// Cancels a composite and every member it has started: `CANCELLED` if it
    /// was, `ALREADY_FINISHED` if it had finished, `UNKNOWN_TASK` if there is
    /// no such composite.
    ///
    /// Raises `RuntimeError` if this worker is not the leader.
    fn cancel_composite(&self, composite_id: &str) -> PyResult<PyCancelOutcome> {
        let outcome = self
            .door
            .cancel_composite(&CompositeId::new(composite_id))
            .map_err(|refusal| refused(refusal, |rejection| PyRuntimeError::new_err(rejection.to_string())))?;
        Ok(outcome.into())
    }

    /// Returns an awaitable that resolves to where a composite stands, as
    /// `(state, next_stage, pending, running, succeeded, failed, cancelled)`: `state`
    /// is `"running"`, `"succeeded"`, `"failed"` or `"cancelled"`,
    /// `next_stage` the stage of a flow that runs or runs next, and the rest
    /// count its members. `None` if the runtime holds no such composite. It
    /// is awaitable as a networked runtime's is, which asks the shard.
    fn composite_status<'py>(&self, py: Python<'py>, composite_id: String) -> PyResult<Bound<'py, PyAny>> {
        let door = Arc::clone(&self.door);
        self.bridge.spawn_into_py(py, async move {
            let status = door.composite_status(&CompositeId::new(composite_id))?;
            Ok(status.map(|status| {
                let counts = status.counts;
                (
                    state_name(status.state),
                    status.next_stage,
                    counts.pending,
                    counts.running,
                    counts.succeeded,
                    counts.failed,
                    counts.cancelled,
                )
            }))
        })
    }

    /// Returns an awaitable that resolves to a composite's results: `("list",
    /// [...])` of its stages' or members' results in order, each `("value",
    /// producer_definition_id, bytes)`, `("failure", kind, message, task_id)` or a
    /// nested `("list", [...])`.
    ///
    /// Raises `RuntimeError` if the runtime no longer holds the composite or a
    /// result it names.
    fn composite_results<'py>(&self, py: Python<'py>, composite_id: String) -> PyResult<Bound<'py, PyAny>> {
        let door = Arc::clone(&self.door);
        self.bridge.spawn_into_py(py, async move {
            results(&DoorSource(&door), &CompositeId::new(composite_id))
                .await
                .map(PyResolved)
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))
        })
    }

    /// Returns an awaitable that resolves to what the member run
    /// `task_run_id` is given besides its claim, as `(fields, input)`: the
    /// `(name, value)` pairs bound into its input, and what it takes from
    /// before it, `("value", producer_definition_id, bytes)`, `("failure",
    /// kind, message, task_id)` or `("list", [...])` as `composite_results` has them,
    /// `("input", bytes)` for its own whole input when that was stored as
    /// blobs, or `None` when the claim's `serialized_input` is the whole
    /// input.
    ///
    /// Raises `RuntimeError` if the run is unknown or not a composite's
    /// member, or a result it takes is no longer held.
    fn resolve_input<'py>(&self, py: Python<'py>, task_run_id: &str) -> PyResult<Bound<'py, PyAny>> {
        let task = self
            .door
            .task_of_run(&TaskRunId::new(task_run_id))?
            .ok_or_else(|| PyRuntimeError::new_err("no such run"))?;
        let door = Arc::clone(&self.door);
        let run = TaskRunId::new(task_run_id);
        self.bridge.spawn_into_py(py, async move {
            let input = member_input(&DoorSource(&door), &task)
                .await
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            door.hold_member_input(&run, input.payload_bytes())?;
            Ok(PyMemberInput(input))
        })
    }

    /// Returns an awaitable that resolves to what the reduce step run
    /// `task_run_id` works on, in the order it takes it: a list of `("item",
    /// bytes)`, `("result", bytes)` or `("resolved", <a result as
    /// `composite_results` has it>)`.
    ///
    /// Raises `RuntimeError` if the run is unknown or not a reduce step's, or
    /// an operand is no longer held.
    fn reduce_operands<'py>(&self, py: Python<'py>, task_run_id: &str) -> PyResult<Bound<'py, PyAny>> {
        let task = self
            .door
            .task_of_run(&TaskRunId::new(task_run_id))?
            .ok_or_else(|| PyRuntimeError::new_err("no such run"))?;
        let door = Arc::clone(&self.door);
        let run = TaskRunId::new(task_run_id);
        self.bridge.spawn_into_py(py, async move {
            let operands = reduce_operands(&DoorSource(&door), &task)
                .await
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            door.hold_member_input(&run, operands.iter().map(ReduceOperand::payload_bytes).sum())?;
            Ok(PyReduceOperands(operands))
        })
    }

    /// Returns an awaitable that resolves to the list the expansion run
    /// `task_run_id` is to split, as `composite_results` has a list:
    /// `("value", producer_definition_id, bytes)` or a nested list.
    ///
    /// Raises `RuntimeError` if the run is unknown or not an expansion, or the
    /// list is no longer held.
    fn resolve_expansion<'py>(&self, py: Python<'py>, task_run_id: &str) -> PyResult<Bound<'py, PyAny>> {
        let task = self
            .door
            .task_of_run(&TaskRunId::new(task_run_id))?
            .ok_or_else(|| PyRuntimeError::new_err("no such run"))?;
        let door = Arc::clone(&self.door);
        let run = TaskRunId::new(task_run_id);
        self.bridge.spawn_into_py(py, async move {
            let list = expansion_source(&DoorSource(&door), &task)
                .await
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            door.hold_member_input(&run, list.payload_bytes())?;
            Ok(PyResolved(list))
        })
    }

    /// Reports the fold of a compaction run: the payload made by merging the
    /// run's `chain`, oldest first. Returns whether the leader swapped it in
    /// for the entries it replaces; `False` means the chain changed under the
    /// run (its oldest payloads were dropped, or its generation ended) and
    /// the fold was discarded.
    ///
    /// Raises `RuntimeError` if the run is unknown, is not a compaction run,
    /// or is not this worker's.
    fn complete_compaction(&self, task_run_id: &str, folded: &[u8]) -> PyResult<bool> {
        self.door
            .complete_compaction(&TaskRunId::new(task_run_id), folded.to_vec())
            .map(|compacted| compacted.applied)
            .map_err(|refusal| refused(refusal, rejected))
    }

    /// Reports that a claimed run failed with an error of type `failure_kind`
    /// (the type's name) and the error's message `failure_message` (never its
    /// traceback; the leader keeps at most `MAX_FAILURE_MESSAGE_BYTES` of it),
    /// whether or not its body started. The message is stored in the task's
    /// replicated record, so it can contain task data. Returns whether the task has retries
    /// left, so a new attempt is now waiting to be claimed; `False` means the
    /// task has failed for good.
    ///
    /// Raises `RuntimeError` if the run is unknown, is not this worker's, or
    /// has already finished; the failure is then not recorded.
    #[pyo3(signature = (task_run_id, failure_kind, failure_message = ""))]
    fn report_failure(
        &self,
        task_run_id: &str,
        failure_kind: &str,
        failure_message: &str,
    ) -> PyResult<bool> {
        let failure = self
            .door
            .report_failure(&TaskRunId::new(task_run_id), failure_kind, failure_message)
            .map_err(|refusal| refused(refusal, rejected))?;
        Ok(failure.retry.is_some())
    }

    /// Reports that a claimed run was lost with the process that ran its
    /// body (it died or was killed), not failed. Returns whether a new
    /// attempt is now waiting to be claimed; `False` means the task is over
    /// (an ephemeral task's run is not replayed, and a non-retriable task's
    /// running run is orphaned).
    ///
    /// Raises `RuntimeError` if the run is unknown, is not this worker's, or
    /// has already ended; the loss is then not recorded.
    fn report_lost(&self, task_run_id: &str) -> PyResult<bool> {
        let lost = self
            .door
            .lose_run(&TaskRunId::new(task_run_id))
            .map_err(|refusal| refused(refusal, rejected))?;
        Ok(lost.replayed.is_some())
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

    /// Returns an awaitable that resolves to the newest record of a task this
    /// runtime holds, or `None` if it holds none (never submitted, or
    /// forgotten after its result's time to live).
    fn task_record<'py>(&self, py: Python<'py>, task_id: String) -> PyResult<Bound<'py, PyAny>> {
        let door = Arc::clone(&self.door);
        self.bridge.spawn_into_py(py, async move {
            let record = door.task_record(&TaskId::new(task_id))?;
            Ok(record.map(PyTaskRecord::from))
        })
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
                // its error to Python before the runtime discards it. Any
                // still undelivered when the grace ends fail as they are dropped.
                let _ = tokio::time::timeout(SHUTDOWN_GRACE, self.bridge.drained()).await;
            });
            runtime.shutdown_timeout(SHUTDOWN_GRACE);
        });
    }
}

impl Drop for NativeRuntime {
    /// A runtime that was never shut down still must not block its owner's
    /// thread, so its tasks are abandoned rather than waited for. Awaitables
    /// still pending fail as their tasks are dropped; `shutdown()` is what
    /// lets each hand over its own error first.
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

impl NativeRuntime {
    /// Takes one batch of members into its streamed composite, and says
    /// whether the composite is still taking more (see `submit_members`).
    fn send_members(
        &self,
        py: Python<'_>,
        composite_id: &str,
        start_index: u64,
        members: &[Bound<'_, PyAny>],
        last: bool,
        inline_bytes: u64,
    ) -> PyResult<&'static str> {
        let (batch, values) = member_batch(composite_id, start_index, members, last, inline_bytes)?;
        self.refuse_values_past_limit(py, &values)?;
        match self.door.submit_members(batch, values) {
            Ok(_) => Ok("accepted"),
            Err(Refusal::Rejected(SubmitRejection::CompositeFinished)) => Ok("finished"),
            Err(refusal) => Err(refused(refusal, |rejection| match rejection {
                SubmitRejection::Backpressure { .. }
                | SubmitRejection::TooLarge { .. }
                | SubmitRejection::KeyBackpressure { .. }
                | SubmitRejection::PayloadTooLarge { .. } => errors::backpressure_error(py, rejection.to_string()),
                SubmitRejection::NotLeader | SubmitRejection::NotReady | SubmitRejection::KeyNotReady => {
                    PyRuntimeError::new_err(rejection.to_string())
                }
                SubmitRejection::UnknownComposite
                | SubmitRejection::NotStreamed
                | SubmitRejection::MemberGap { .. }
                | SubmitRejection::MemberKind
                | SubmitRejection::RecordTooLarge { .. }
                | SubmitRejection::StepTakesInput
                | SubmitRejection::CompositeFinished => PyValueError::new_err(rejection.to_string()),
            })),
        }
    }

    /// Refuses any of `values` alone past the hard memory limit, before
    /// anything is stored.
    fn refuse_values_past_limit(&self, py: Python<'_>, values: &[Vec<u8>]) -> PyResult<()> {
        refuse_values_past_limit(py, self.payload_limit, values)
    }
}

fn rejected(rejection: ReportRejection) -> PyErr {
    match rejection {
        ReportRejection::ContinuationTooLarge => {
            errors::ContinuationTooLargeError::new_err(rejection.to_string())
        }
        _ => PyRuntimeError::new_err(rejection.to_string()),
    }
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
