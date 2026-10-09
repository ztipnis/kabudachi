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
//! stores it (`ACCEPTED`) or refuses it for good (`REFUSED`). A result never
//! goes back to the process that submitted its task, so nothing here
//! certifies a result for a handle.
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

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration as StdDuration, Instant as StdInstant};

use kabudachi_core::election::{ElectionTimings, Input, Step, WorkerNode};
use kabudachi_core::protocol::digest::{Digest, DigestAlgorithm};
use kabudachi_core::protocol::ids::{ShardName, TaskDefinitionId, TaskId, TaskRunId, Uuid7Ids, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::messages::{TaskRejectReason, task_response};
use kabudachi_core::scheduler::{Claim, MemoryLimits, Submission, Submitted, mint};
use kabudachi_core::time::{Duration as CoreDuration, RealClock};
use kabudachi_net::executor::{Report, ReportSink, Work, WorkSource, executor_channel};
use kabudachi_net::messenger::Net;
use kabudachi_net::worker::{AuthorityConfig, Multiaddr, Worker, WorkerConfig};
use kabudachi_redis_authority::{RedisAuthority, RedisAuthorityConfig};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use tokio::runtime::{Builder, Handle, Runtime};
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;

use crate::bridge::{Bridge, CLOSED_MESSAGE};
use crate::outcomes::{PyEventKind, coalescing};
use crate::work::{PyClaim, PyEvent, PyTaskRecord};

const DEFAULT_WORKER_THREADS: usize = 2;
const DEFAULT_RESULT_TTL_MS: u64 = 60 * 60 * 1000;
const SHUTDOWN_GRACE: StdDuration = StdDuration::from_secs(1);
/// The default roll-call deadline, unless a quarter of the suspicion timeout
/// is shorter: a roll call must close well inside it.
const ROLL_CALL_DEADLINE_MS: u64 = 250;
/// How a run the driver took back is reported: its body is being stopped,
/// and nothing it ends with counts.
const CANCELLED_KIND: &str = "TaskCancelledError";
const MALFORMED_KIND: &str = "MalformedClaim";
const GONE_MESSAGE: &str = "the worker left its shard";
const REPORTED_POLL: StdDuration = StdDuration::from_millis(20);

/// What the worker's node last showed.
#[derive(Debug, Clone, Default, PartialEq)]
struct Observed {
    leader: Option<(WorkerId, u64)>,
    voters: usize,
    shard_id: String,
    /// The worker is out of its shard: `Worker::run` ended, however it did.
    gone: bool,
}

/// Marks the worker out of its shard when dropped: when `Worker::run`
/// returns, and also when it panics or is aborted, so no wait outlives it.
struct LeftOnDrop(Arc<watch::Sender<Observed>>);

impl Drop for LeftOnDrop {
    fn drop(&mut self) {
        self.0.send_modify(|seen| seen.gone = true);
    }
}

/// What passes between the driver and Python.
#[derive(Default)]
struct Seam {
    /// Places offered to the driver that no run has used yet.
    offered: u32,
    /// Runs handed over that Python has not taken, oldest first.
    handed: VecDeque<Claim>,
    /// The task of every run Python took and has not ended.
    held: BTreeMap<TaskRunId, TaskId>,
    events: VecDeque<PyEvent>,
    /// Tasks submitted here whose submission no leader has answered yet: a
    /// cancel of one waits, so it never reaches a leader before the task.
    delivering: BTreeSet<TaskId>,
    /// Nothing more is taken: a run handed over is reported lost at once.
    closed: bool,
}

struct Shared {
    seam: Mutex<Seam>,
    /// Woken whenever a claim or an event may be waiting.
    changed: Notify,
    reports: ReportSink,
    net: Arc<Net>,
    observed: watch::Receiver<Observed>,
    /// How long to wait before asking a leader again.
    retry_after: StdDuration,
    /// How long a leader may take to answer.
    answer_within: StdDuration,
}

impl Shared {
    fn seam(&self) -> MutexGuard<'_, Seam> {
        self.seam.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Hands `report` to the driver. A driver that has stopped shows as the
    /// worker leaving its shard, which every wait reports.
    fn report(&self, report: Report) {
        let _ = self.reports.report(report);
    }

    fn wake(&self) {
        self.changed.notify_waiters();
    }

    /// Offers the driver the places `free` has that are neither offered nor
    /// held by a run Python has not taken.
    fn offer(&self, free: usize) {
        let more = {
            let mut seam = self.seam();
            if seam.closed {
                return;
            }
            let free = u32::try_from(free).unwrap_or(u32::MAX);
            let waiting = u32::try_from(seam.handed.len()).unwrap_or(u32::MAX);
            let more = free.saturating_sub(seam.offered.saturating_add(waiting));
            seam.offered += more;
            more
        };
        if more > 0 {
            self.report(Report::Capacity(more));
        }
    }

    fn on_work(&self, work: Work) {
        let (report, wake) = {
            let mut guard = self.seam();
            // Through the guard once, so the arms borrow its fields apart.
            let seam = &mut *guard;
            match work {
                Work::Run(wire) | Work::Compact(wire) => {
                    seam.offered = seam.offered.saturating_sub(1);
                    match Claim::try_from(&wire).ok() {
                        Some(claim) if seam.closed => (Some(Report::Lost(claim.task_run_id)), false),
                        Some(claim) => {
                            seam.handed.push_back(claim);
                            (None, true)
                        }
                        None => (
                            wire.task_run_id.map(|run| Report::Failed {
                                run: run.into(),
                                reason: MALFORMED_KIND.into(),
                            }),
                            false,
                        ),
                    }
                }
                Work::Cancel(run) => {
                    let unstarted = take_handed(&mut seam.handed, &run);
                    match seam.held.remove(&run) {
                        Some(task) => {
                            seam.events.push_back(PyEvent::networked(
                                PyEventKind::Cancelled,
                                task.as_str(),
                                run.as_str(),
                            ));
                            (
                                Some(Report::Failed {
                                    run,
                                    reason: CANCELLED_KIND.into(),
                                }),
                                true,
                            )
                        }
                        None if unstarted => (
                            Some(Report::Failed {
                                run,
                                reason: CANCELLED_KIND.into(),
                            }),
                            false,
                        ),
                        None => (None, false),
                    }
                }
                Work::Abort { run, deadline } => {
                    if take_handed(&mut seam.handed, &run) {
                        // Never started: nothing runs, so it is lost now.
                        (Some(Report::Lost(run)), false)
                    } else if let Some(task) = seam.held.get(&run) {
                        let left = deadline
                            .saturating_duration_since(StdInstant::now())
                            .as_secs_f64();
                        seam.events
                            .push_back(PyEvent::abort(task.as_str(), run.as_str(), left));
                        (None, true)
                    } else {
                        (None, false)
                    }
                }
                Work::AbortWithdrawn(run) => match seam.held.get(&run) {
                    Some(task) => {
                        seam.events.push_back(PyEvent::networked(
                            PyEventKind::AbortWithdrawn,
                            task.as_str(),
                            run.as_str(),
                        ));
                        (None, true)
                    }
                    None => (None, false),
                },
            }
        };
        if let Some(report) = report {
            self.report(report);
        }
        if wake {
            self.wake();
        }
    }

    /// Up to `limit` runs handed over, now held by Python; `None` while
    /// there are none, and none at all once nothing more is taken.
    fn take_claims(&self, limit: usize) -> Option<Vec<PyClaim>> {
        let mut seam = self.seam();
        if seam.closed {
            return Some(Vec::new());
        }
        if seam.handed.is_empty() {
            return None;
        }
        let count = limit.min(seam.handed.len());
        let claims: Vec<Claim> = seam.handed.drain(..count).collect();
        for claim in &claims {
            seam.held
                .insert(claim.task_run_id.clone(), claim.task.task_id());
        }
        Some(claims.into_iter().map(PyClaim::from).collect())
    }

    fn take_events(&self) -> Option<Vec<PyEvent>> {
        let mut seam = self.seam();
        (!seam.events.is_empty()).then(|| seam.events.drain(..).collect())
    }

    fn holds(&self, run: &TaskRunId) -> bool {
        self.seam().held.contains_key(run)
    }

    /// Ends a run Python held; `false` if it held none (the driver took it
    /// back), and the report is then dropped.
    fn end(&self, run: &TaskRunId) -> bool {
        self.seam().held.remove(run).is_some()
    }

    /// Waits until `take` finds something, or the worker leaves its shard.
    async fn until<T>(&self, take: impl Fn(&Shared) -> Option<T>) -> PyResult<T> {
        let mut observed = self.observed.clone();
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(found) = take(self) {
                return Ok(found);
            }
            if observed.borrow_and_update().gone {
                return Err(PyRuntimeError::new_err(GONE_MESSAGE));
            }
            tokio::select! {
                () = &mut notified => {}
                changed = observed.changed() => {
                    if changed.is_err() {
                        return Err(PyRuntimeError::new_err(GONE_MESSAGE));
                    }
                }
            }
        }
    }

    /// The leader the node names, once it names one; `None` once the
    /// worker is out of its shard.
    async fn leader(&self) -> Option<WorkerId> {
        let mut observed = self.observed.clone();
        let seen = observed
            .wait_for(|seen| seen.gone || seen.leader.is_some())
            .await
            .ok()?;
        if seen.gone {
            return None;
        }
        seen.leader.as_ref().map(|(leader, _)| leader.clone())
    }
}

/// Removes `run` from the runs not taken yet; whether it was there.
fn take_handed(handed: &mut VecDeque<Claim>, run: &TaskRunId) -> bool {
    let before = handed.len();
    handed.retain(|claim| &claim.task_run_id != run);
    handed.len() != before
}

/// Asks leaders to store `submitted` until one does or refuses it for
/// good, and says which.
async fn deliver(shared: Arc<Shared>, submitted: Submitted) {
    let task = submitted.task_id.clone();
    let event = loop {
        let Some(leader) = shared.leader().await else {
            // Out of its shard: no leader will answer, and a cancel waiting
            // for this submission must not wait for ever.
            shared.seam().delivering.remove(&task);
            return;
        };
        let asked = shared.net.submit(leader, submitted.clone());
        if let Ok(Ok(response)) = tokio::time::timeout(shared.answer_within, asked).await {
            match &response.result {
                Some(task_response::Result::Submitted(_)) => {
                    break PyEvent::networked(PyEventKind::Accepted, task.as_str(), "");
                }
                Some(task_response::Result::Reject(reject)) => {
                    match TaskRejectReason::try_from(reject.reason) {
                        Ok(reason) if asks_again(reason) => {}
                        Ok(reason) => break PyEvent::refused(task.as_str(), reason.as_str_name()),
                        Err(_) => {
                            break PyEvent::refused(task.as_str(), "a reason this worker cannot read");
                        }
                    }
                }
                _ => {
                    break PyEvent::refused(task.as_str(), "an answer that is not about a submission");
                }
            }
        }
        tokio::time::sleep(shared.retry_after).await;
    };
    {
        let mut seam = shared.seam();
        seam.delivering.remove(&task);
        seam.events.push_back(event);
    }
    shared.wake();
}

/// Whether a refusal says to ask again: the node asked is not leading, not
/// ready, or not yet sure of this worker.
fn asks_again(reason: TaskRejectReason) -> bool {
    matches!(
        reason,
        TaskRejectReason::TaskRejectNotLeader
            | TaskRejectReason::TaskRejectNotReady
            | TaskRejectReason::TaskRejectNotMember
    )
}

/// Asks leaders to cancel `task` until one answers, once its own submission
/// from here (if any) has been answered. How the cancel ended is not
/// reported: a run it stopped is ended by the driver's `Cancel`, wherever
/// it runs, and a task it found finished or unknown needs nothing.
async fn cancel_at_leader(shared: Arc<Shared>, task: TaskId) {
    while shared.seam().delivering.contains(&task) {
        if shared.observed.borrow().gone {
            return;
        }
        tokio::time::sleep(shared.retry_after).await;
    }
    loop {
        let Some(leader) = shared.leader().await else {
            return;
        };
        let asked = shared.net.cancel(leader, task.clone());
        if let Ok(Ok(response)) = tokio::time::timeout(shared.answer_within, asked).await {
            match response.result {
                Some(task_response::Result::Reject(reject))
                    if TaskRejectReason::try_from(reject.reason).is_ok_and(asks_again) => {}
                // Cancelled, already finished, unknown, or refused for good:
                // the leader decided.
                _ => return,
            }
        }
        tokio::time::sleep(shared.retry_after).await;
    }
}

fn publish(sender: &watch::Sender<Observed>, node: &WorkerNode<RealClock>) {
    let now = Observed {
        leader: node.known_leader(),
        voters: node.voters().len(),
        shard_id: node.shard_id().as_str().to_owned(),
        gone: false,
    };
    sender.send_if_modified(|seen| {
        let changed = *seen != now;
        if changed {
            *seen = now.clone();
        }
        changed
    });
}

fn address(text: &str) -> PyResult<Multiaddr> {
    text.parse()
        .map_err(|error| PyValueError::new_err(format!("{text:?} is not an address: {error}")))
}

fn memory_limits(soft: Option<u64>, hard: Option<u64>) -> PyResult<Option<MemoryLimits>> {
    match (soft, hard) {
        (None, None) => Ok(None),
        (Some(soft), Some(hard)) => Ok(Some(MemoryLimits { soft, hard })),
        _ => Err(PyValueError::new_err("give both memory limits or neither")),
    }
}

/// A worker of a networked shard, and the Tokio runtime it runs on.
///
/// Call `shutdown()` when finished: the worker stops at once, without
/// leaving its shard, and every awaitable still pending fails.
#[pyclass(name = "NetworkedRuntime", frozen)]
pub struct NetworkedRuntime {
    tokio: Mutex<Option<Runtime>>,
    handle: Handle,
    bridge: Bridge,
    shared: Arc<Shared>,
    observed: Arc<watch::Sender<Observed>>,
    worker: Mutex<Option<JoinHandle<()>>>,
    pump: Mutex<Option<JoinHandle<()>>>,
    worker_id: String,
    address: String,
    clock: RealClock,
    reconnect_timeout: StdDuration,
}

#[pymethods]
impl NetworkedRuntime {
    /// Starts a worker of the shard `shard` listening on `listen`, and
    /// bootstraps it into the shard through `seeds`: with none, it founds
    /// the shard alone. The heartbeat interval, heartbeat timeout and
    /// reconnect timeout are the shard's election timings, the same on every
    /// worker of it. `memory_soft_limit` and `memory_hard_limit` bound
    /// pending task input while this worker leads, as for `NativeRuntime`.
    /// `authority_url`, a `redis://` or `rediss://` server URL, with
    /// `authority_key_prefix` and `authority_database`, is the shard's
    /// coordination authority: the worker then bootstraps through it, and its
    /// timings come from the authority's TTL.
    ///
    /// Raises `ValueError` for an address that cannot be read, a timing of
    /// zero, only one memory limit, no worker threads or an authority that
    /// cannot be used, and `RuntimeError` if it cannot listen on `listen`.
    #[new]
    #[pyo3(signature = (
        shard,
        listen,
        *,
        heartbeat_interval_ms,
        heartbeat_timeout_ms,
        reconnect_timeout_ms,
        seeds = Vec::new(),
        external_address = None,
        authority_url = None,
        authority_key_prefix = None,
        authority_database = 0,
        result_ttl_ms = DEFAULT_RESULT_TTL_MS,
        memory_soft_limit = None,
        memory_hard_limit = None,
        worker_threads = DEFAULT_WORKER_THREADS,
    ))]
    #[allow(
        clippy::too_many_arguments,
        reason = "each argument is a Python parameter"
    )]
    fn new(
        py: Python<'_>,
        shard: &str,
        listen: &str,
        heartbeat_interval_ms: u64,
        heartbeat_timeout_ms: u64,
        reconnect_timeout_ms: u64,
        seeds: Vec<String>,
        external_address: Option<String>,
        authority_url: Option<String>,
        authority_key_prefix: Option<String>,
        authority_database: u16,
        result_ttl_ms: u64,
        memory_soft_limit: Option<u64>,
        memory_hard_limit: Option<u64>,
        worker_threads: usize,
    ) -> PyResult<Self> {
        if worker_threads == 0 {
            return Err(PyValueError::new_err("worker_threads must be at least 1"));
        }
        for (name, value) in [
            ("heartbeat_interval_ms", heartbeat_interval_ms),
            ("heartbeat_timeout_ms", heartbeat_timeout_ms),
            ("reconnect_timeout_ms", reconnect_timeout_ms),
        ] {
            if value == 0 {
                return Err(PyValueError::new_err(format!("{name} must be positive")));
            }
        }
        let timings = ElectionTimings::new(
            CoreDuration::from_millis(heartbeat_timeout_ms),
            CoreDuration::from_millis(heartbeat_interval_ms),
        )
        .with_roll_call_deadline(CoreDuration::from_millis(
            ROLL_CALL_DEADLINE_MS.min(heartbeat_timeout_ms / 4).max(1),
        ))
        .with_reconnect_timeout(CoreDuration::from_millis(reconnect_timeout_ms));
        let (endpoint, executor) = executor_channel();
        let (source, reports) = executor.split();
        let mut config = WorkerConfig::new(ShardName::new(shard), address(listen)?, timings)
            .with_seeds(
                seeds
                    .iter()
                    .map(|seed| address(seed))
                    .collect::<PyResult<_>>()?,
            )
            .with_result_ttl(StdDuration::from_millis(result_ttl_ms))
            .with_executor(endpoint);
        if let Some(external) = external_address {
            config = config.with_external_address(address(&external)?);
        }
        if let Some(limits) = memory_limits(memory_soft_limit, memory_hard_limit)? {
            config = config.with_memory_limits(limits);
        }
        if let Some(url) = authority_url {
            let mut redis = RedisAuthorityConfig::new(vec![url]);
            if let Some(prefix) = authority_key_prefix {
                redis.key_prefix = prefix;
            }
            redis.database = authority_database;
            let authority = RedisAuthority::connect(redis).map_err(|error| {
                PyValueError::new_err(format!("the authority cannot be used: {error}"))
            })?;
            config = config.with_authority(AuthorityConfig::new(Arc::new(authority)));
        }

        let tokio = Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .thread_name("kabudachi-native")
            .enable_all()
            .build()
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        let handle = tokio.handle().clone();
        let worker = py
            .detach(|| tokio.block_on(Worker::start(config)))
            .map_err(|_| PyRuntimeError::new_err(format!("cannot listen on {listen:?}")))?;
        let worker_id = worker.id().as_str().to_owned();
        let address = worker
            .address()
            .map(|address| address.to_string())
            .unwrap_or_default();
        let (sender, observed) = watch::channel(Observed::default());
        let sender = Arc::new(sender);
        let shared = Arc::new(Shared {
            seam: Mutex::default(),
            changed: Notify::new(),
            reports,
            net: worker.net(),
            observed,
            retry_after: StdDuration::from_millis(heartbeat_interval_ms),
            answer_within: StdDuration::from_millis(heartbeat_timeout_ms),
        });

        let seen = Arc::clone(&sender);
        let left = LeftOnDrop(Arc::clone(&sender));
        let running = tokio.spawn(async move {
            // Dropped however this ends, a panic or an abort included.
            let _left = left;
            let _ = worker
                .run(move |node: &WorkerNode<RealClock>, _: Option<&Input>, _: &Step| {
                    publish(&seen, node);
                })
                .await;
        });
        let pumped = Arc::clone(&shared);
        let pump = tokio.spawn(async move {
            let mut source: WorkSource = source;
            while let Some(work) = source.next_work().await {
                pumped.on_work(work);
            }
        });
        Ok(NetworkedRuntime {
            tokio: Mutex::new(Some(tokio)),
            bridge: Bridge::new(handle.clone()),
            handle,
            shared,
            observed: sender,
            worker: Mutex::new(Some(running)),
            pump: Mutex::new(Some(pump)),
            worker_id,
            address,
            clock: RealClock::new(),
            reconnect_timeout: StdDuration::from_millis(reconnect_timeout_ms),
        })
    }

    /// Whether a run's certification comes back to this process: never,
    /// for a networked worker, whose leader may be another process.
    #[getter]
    fn delivers_results(&self) -> bool {
        false
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
        self.bridge.spawn_into_py(py, async move {
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
    /// `next_events()` says the end of. Arguments as for `NativeRuntime.submit`.
    ///
    /// Raises `ValueError` if `kind`, `key` and `drop_oldest` disagree or
    /// `reconnect_timeout_ms` is zero, and `RuntimeError` once the runtime
    /// has shut down.
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
        reconnect_timeout_ms = None,
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
        reconnect_timeout_ms: Option<u64>,
    ) -> PyResult<String> {
        if !self.bridge.is_open() {
            return Err(PyRuntimeError::new_err(CLOSED_MESSAGE));
        }
        let coalescing = coalescing(kind, key, drop_oldest).map_err(PyValueError::new_err)?;
        let mut submission = Submission::new(
            TaskDefinitionId::new(definition_id),
            source_version,
            serialized_input.to_vec(),
            queue,
        )
        .with_retries(retries);
        if kind == "ephemeral" {
            submission = submission.ephemeral();
        }
        if let Some(delay_ms) = delay_ms {
            submission = submission.with_delay(CoreDuration::from_millis(delay_ms));
        }
        if let Some(expires_in_ms) = expires_in_ms {
            submission = submission.with_expiry(CoreDuration::from_millis(expires_in_ms));
        }
        if let Some(reconnect_timeout_ms) = reconnect_timeout_ms {
            if reconnect_timeout_ms == 0 {
                return Err(PyValueError::new_err("reconnect_timeout_ms must be positive"));
            }
            submission =
                submission.with_reconnect_timeout(CoreDuration::from_millis(reconnect_timeout_ms));
        }
        if let Some((key, drop_oldest)) = coalescing {
            submission = submission.with_coalescing_key(key);
            if drop_oldest {
                submission = submission.with_drop_oldest();
            }
        }
        let submitted = mint(submission, &Uuid7Ids, &self.clock);
        let task_id = submitted.task_id.as_str().to_owned();
        self.shared
            .seam()
            .delivering
            .insert(submitted.task_id.clone());
        self.handle
            .spawn(deliver(Arc::clone(&self.shared), submitted));
        Ok(task_id)
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
        self.bridge.spawn_into_py(py, async move {
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
        self.bridge
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
    /// Raises `ValueError` for a digest of the wrong length, and for
    /// `continues`: a networked worker certifies no continuation.
    #[pyo3(signature = (task_run_id, result_digest, continues = false))]
    fn complete(&self, task_run_id: &str, result_digest: &[u8], continues: bool) -> PyResult<()> {
        if continues {
            return Err(PyValueError::new_err(
                "a networked worker certifies no continuation: the tasks it starts would need \
                 their results delivered here",
            ));
        }
        let digest = Digest::new(DigestAlgorithm::Blake3, result_digest.to_vec())
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        let run = TaskRunId::new(task_run_id);
        if self.shared.end(&run) {
            self.shared.report(Report::Completed { run, digest });
        }
        Ok(())
    }

    /// Hands the leader a run's failure, by its error's type name.
    fn report_failure(&self, task_run_id: &str, failure_kind: &str) {
        let run = TaskRunId::new(task_run_id);
        if self.shared.end(&run) {
            self.shared.report(Report::Failed {
                run,
                reason: failure_kind.to_owned(),
            });
        }
    }

    /// Hands the leader a run lost with the process that ran it, or stopped
    /// at its abort deadline.
    fn report_lost(&self, task_run_id: &str) {
        let run = TaskRunId::new(task_run_id);
        if self.shared.end(&run) {
            self.shared.report(Report::Lost(run));
        }
    }

    /// Hands the leader a compaction run's fold.
    fn complete_compaction(&self, task_run_id: &str, folded: &[u8]) {
        let run = TaskRunId::new(task_run_id);
        if self.shared.end(&run) {
            self.shared.report(Report::Compacted {
                run,
                payload: folded.to_vec(),
            });
        }
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
        if !self.bridge.is_open() {
            return Err(PyRuntimeError::new_err(CLOSED_MESSAGE));
        }
        self.handle.spawn(cancel_at_leader(
            Arc::clone(&self.shared),
            TaskId::new(task_id),
        ));
        Ok(())
    }

    /// Returns an awaitable that resolves to the newest record of a task the
    /// shard holds, or `None` if no worker this one reaches holds it.
    fn task_record<'py>(&self, py: Python<'py>, task_id: String) -> PyResult<Bound<'py, PyAny>> {
        let net = Arc::clone(&self.shared.net);
        self.bridge.spawn_into_py(py, async move {
            Ok(net
                .get_record(TaskId::new(task_id))
                .await
                .map(PyTaskRecord::from))
        })
    }

    /// Takes no more runs: those handed over and not taken are reported
    /// lost, later ones too, and a claim waiting resolves empty. The driver
    /// may still hand over up to the places already offered; each is
    /// reported lost as it arrives.
    fn stop_taking(&self) {
        let unstarted: Vec<TaskRunId> = {
            let mut seam = self.shared.seam();
            seam.closed = true;
            seam.handed
                .drain(..)
                .map(|claim| claim.task_run_id)
                .collect()
        };
        for run in unstarted {
            self.shared.report(Report::Lost(run));
        }
        self.shared.wake();
    }

    /// Returns an awaitable that resolves once the leader has taken the
    /// last report on every run this worker claimed (`True`), or the
    /// reconnect timeout has passed or the worker left its shard (`False`).
    fn wait_until_reported<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let net = Arc::clone(&self.shared.net);
        let observed = self.shared.observed.clone();
        let within = self.reconnect_timeout;
        self.bridge.spawn_into_py(py, async move {
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

    /// Stops the worker at once and shuts the runtime down; awaitables
    /// still pending fail. Does nothing if already shut down.
    fn shutdown(&self, py: Python<'_>) {
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
            self.shared.wake();
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

impl Drop for NetworkedRuntime {
    /// As for `NativeRuntime`: a runtime never shut down abandons its tasks
    /// rather than block its owner's thread.
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
