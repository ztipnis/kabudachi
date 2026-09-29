//! What Python sees of the scheduler: claims, certifications, and the loop
//! that hands pending work to a worker as soon as there is a leader and work.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use kabudachi_core::protocol::ids::{Uuid7Ids, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::scheduler::{Certification, Claim, ClaimRejection, Event, Scheduler};
use kabudachi_core::time::RealClock;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use tokio::sync::watch;

/// The scheduler, shared between the calls into it and the loops that drive it.
pub type SharedScheduler = Arc<Mutex<Scheduler<RealClock, Uuid7Ids>>>;

/// Locks the scheduler, which a panic in another holder does not make unusable.
pub fn lock_scheduler(
    scheduler: &SharedScheduler,
) -> MutexGuard<'_, Scheduler<RealClock, Uuid7Ids>> {
    scheduler.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A task handed to a worker to run, and the run that now belongs to it.
#[pyclass(name = "Claim", frozen)]
pub struct PyClaim {
    #[pyo3(get)]
    task_id: String,
    #[pyo3(get)]
    task_run_id: String,
    #[pyo3(get)]
    definition_id: String,
    #[pyo3(get)]
    source_version: u32,
    serialized_input: Vec<u8>,
    #[pyo3(get)]
    queue: String,
    /// Which attempt this is: 1 for the first, then one more per retry.
    #[pyo3(get)]
    attempt_number: u32,
    chain: Vec<Vec<u8>>,
}

#[pymethods]
impl PyClaim {
    /// The task's input exactly as it was submitted.
    #[getter]
    fn serialized_input<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.serialized_input)
    }

    /// The inputs of the generations this one superseded, oldest first, for
    /// the worker to fold before running the task. Empty for a task that
    /// absorbed none.
    #[getter]
    fn chain<'py>(&self, py: Python<'py>) -> Vec<Bound<'py, PyBytes>> {
        self.chain
            .iter()
            .map(|payload| PyBytes::new(py, payload))
            .collect()
    }
}

impl From<Claim> for PyClaim {
    fn from(claim: Claim) -> Self {
        PyClaim {
            task_id: claim.task.task_id().as_str().to_owned(),
            task_run_id: claim.task_run_id.as_str().to_owned(),
            definition_id: claim.task.task_definition_id().as_str().to_owned(),
            source_version: claim.task.source_version,
            serialized_input: claim.task.serialized_input,
            queue: claim.task.queue,
            attempt_number: claim.attempt_number,
            chain: claim.chain,
        }
    }
}

/// The leader's word that a run's result is the authoritative one.
#[pyclass(name = "Certification", frozen)]
pub struct PyCertification {
    #[pyo3(get)]
    task_id: String,
    #[pyo3(get)]
    task_run_id: String,
    result_digest: Vec<u8>,
}

#[pymethods]
impl PyCertification {
    /// The digest the worker reported for the run's result.
    #[getter]
    fn result_digest<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.result_digest)
    }
}

impl From<Certification> for PyCertification {
    fn from(certification: Certification) -> Self {
        PyCertification {
            task_id: certification.task_id.as_str().to_owned(),
            task_run_id: certification.task_run_id.as_str().to_owned(),
            result_digest: certification.result_digest,
        }
    }
}

/// What waiting claims watch. Any update to it wakes them, changed or not, so
/// it only has to say whether the runtime is shutting down. Shutting down is
/// part of the value, so a waiter that reads it cannot miss it whatever it was
/// doing when the shutdown happened.
#[derive(Debug, Clone, Copy, Default)]
pub struct WakeState {
    closed: bool,
}

/// Wakes claims that are waiting for work.
#[derive(Debug, Clone)]
pub struct Wake(Arc<watch::Sender<WakeState>>);

impl Wake {
    pub fn new() -> Self {
        Wake(Arc::new(watch::channel(WakeState::default()).0))
    }

    /// Something happened that may give a waiting claim work: a submission or
    /// a change of leadership.
    pub fn notify(&self) {
        self.0.send_modify(|_| {});
    }

    /// The runtime is shutting down: waiting claims give up.
    pub fn close(&self) {
        self.0.send_modify(|state| state.closed = true);
    }

    /// A receiver that sees every later wake-up, and the shutdown.
    pub fn subscribe(&self) -> watch::Receiver<WakeState> {
        self.0.subscribe()
    }
}

/// Something the scheduler decided because time passed, for Python to act on.
#[pyclass(name = "Event", frozen)]
pub struct PyEvent {
    /// What happened, by name, for example `"expired"`.
    #[pyo3(get)]
    kind: &'static str,
    #[pyo3(get)]
    task_id: String,
    #[pyo3(get)]
    task_run_id: String,
    /// For `"cancelled"`: whether a worker had already claimed the task.
    #[pyo3(get)]
    was_running: bool,
    /// For `"superseded"`: the generation that replaced it.
    #[pyo3(get)]
    superseded_by: Option<String>,
}

impl From<Event> for PyEvent {
    fn from(event: Event) -> Self {
        match event {
            Event::Expired {
                task_id,
                task_run_id,
            } => PyEvent {
                kind: "expired",
                task_id: task_id.as_str().to_owned(),
                task_run_id: task_run_id.as_str().to_owned(),
                was_running: false,
                superseded_by: None,
            },
            Event::Cancelled {
                task_id,
                task_run_id,
                was_running,
            } => PyEvent {
                kind: "cancelled",
                task_id: task_id.as_str().to_owned(),
                task_run_id: task_run_id.as_str().to_owned(),
                was_running,
                superseded_by: None,
            },
            Event::SlowDown { active } => PyEvent {
                kind: if active {
                    "slow_down"
                } else {
                    "slow_down_cleared"
                },
                task_id: String::new(),
                task_run_id: String::new(),
                was_running: false,
                superseded_by: None,
            },
            Event::Superseded {
                task_id,
                task_run_id,
                by,
            } => PyEvent {
                kind: "superseded",
                task_id: task_id.as_str().to_owned(),
                task_run_id: task_run_id.as_str().to_owned(),
                was_running: false,
                superseded_by: Some(by.as_str().to_owned()),
            },
        }
    }
}

/// The runtime is shutting down, so there will be no more work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Closed;

/// Waits until there is a leader and something to claim, then claims up to
/// `limit` tasks. Fails once the runtime is shutting down.
///
/// The wake state is read and marked as seen in one step, before looking for
/// work: a submission or shutdown after that read makes `changed()` return at
/// once, and one before it is visible in the value that was read.
///
/// Claiming lets time take effect (a task past its expiry is not handed out),
/// which can produce events, so `events` is told when it did.
pub async fn claim_when_available(
    scheduler: SharedScheduler,
    worker: WorkerId,
    mut wake: watch::Receiver<WakeState>,
    limit: usize,
    events: Wake,
) -> Result<Vec<Claim>, Closed> {
    loop {
        if wake.borrow_and_update().closed {
            return Err(Closed);
        }
        let (outcome, has_events) = {
            let mut scheduler = lock_scheduler(&scheduler);
            let outcome = scheduler.claim_oldest(&worker, limit);
            (outcome, scheduler.has_events())
        };
        if has_events {
            events.notify();
        }
        let claims = match outcome {
            Ok(claims) => claims,
            // Not leading yet is not an error: there is just nothing to hand out.
            Err(ClaimRejection::NotLeader) => Vec::new(),
            // A rejection this loop does not know about is not "no work": it must not
            // be silently waited out.
            Err(
                ClaimRejection::TaskUnknown
                | ClaimRejection::NotReady
                | ClaimRejection::AlreadySelected
                | ClaimRejection::Finished
                | ClaimRejection::Superseded
                | ClaimRejection::KeyBusy,
            ) => {
                unreachable!("claim_oldest claims from its own queue as leader")
            }
        };
        if !claims.is_empty() {
            return Ok(claims);
        }
        // The sender lives as long as the runtime, so this only fails if the
        // runtime is gone, which is the same as being closed.
        wake.changed().await.map_err(|_| Closed)?;
    }
}

/// Waits until the scheduler has decided something on its own and returns
/// everything it has, oldest first. Fails once the runtime is shutting down.
/// Reads the wake state before looking, like [`claim_when_available`].
pub async fn events_when_available(
    scheduler: SharedScheduler,
    mut wake: watch::Receiver<WakeState>,
) -> Result<Vec<Event>, Closed> {
    loop {
        if wake.borrow_and_update().closed {
            return Err(Closed);
        }
        let events = lock_scheduler(&scheduler).take_events();
        if !events.is_empty() {
            return Ok(events);
        }
        wake.changed().await.map_err(|_| Closed)?;
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::time::Duration;

    use kabudachi_core::protocol::ids::TaskDefinitionId;
    use kabudachi_core::scheduler::{LeadershipGrant, LeaseEnd, Submission};
    use tokio::runtime::Builder;

    use super::*;

    const LIMIT: Duration = Duration::from_secs(5);
    /// Lets a scheduler lead for as long as a test runs. Shared with the
    /// other test modules of this crate that need a leading scheduler.
    pub(crate) const UNBOUNDED_GRANT: LeadershipGrant = LeadershipGrant {
        term: 1,
        recovery_epoch: 0,
        valid_until: LeaseEnd::Unbounded,
    };

    fn worker() -> WorkerId {
        WorkerId::new("worker-1")
    }

    fn scheduler(grant: Option<LeadershipGrant>) -> SharedScheduler {
        let mut scheduler = Scheduler::new(RealClock::new(), Uuid7Ids);
        scheduler.set_leadership_grant(grant);
        Arc::new(Mutex::new(scheduler))
    }

    fn submit(scheduler: &SharedScheduler) {
        lock_scheduler(scheduler)
            .submit(Submission::new(
                TaskDefinitionId::new("definition"),
                0,
                Vec::new(),
                "default",
            ))
            .unwrap();
    }

    fn block_on<F: std::future::Future>(future: F) -> F::Output {
        Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
            .block_on(async {
                tokio::time::timeout(LIMIT, future)
                    .await
                    .expect("timed out")
            })
    }

    #[test]
    fn a_closed_wake_fails_a_claim_at_once() {
        let scheduler = scheduler(Some(UNBOUNDED_GRANT));
        submit(&scheduler);
        let wake = Wake::new();
        let subscription = wake.subscribe();
        wake.close();

        let result = block_on(claim_when_available(
            scheduler,
            worker(),
            subscription,
            10,
            Wake::new(),
        ));

        assert_eq!(result, Err(Closed));
    }

    #[test]
    fn closing_fails_a_claim_that_is_waiting() {
        let scheduler = scheduler(Some(UNBOUNDED_GRANT));
        let wake = Wake::new();

        let result = block_on(async {
            let waiting = tokio::spawn(claim_when_available(
                scheduler,
                worker(),
                wake.subscribe(),
                10,
                Wake::new(),
            ));
            tokio::time::sleep(Duration::from_millis(20)).await;
            wake.close();
            waiting.await.unwrap()
        });

        assert_eq!(result, Err(Closed));
    }

    #[test]
    fn a_submission_before_the_first_wait_is_not_missed() {
        let scheduler = scheduler(None);
        let wake = Wake::new();
        let subscription = wake.subscribe();

        let claims = block_on(async {
            let waiting = tokio::spawn(claim_when_available(
                Arc::clone(&scheduler),
                worker(),
                subscription,
                10,
                Wake::new(),
            ));
            // Becoming leader with work already queued, before the waiter has run.
            submit(&scheduler);
            lock_scheduler(&scheduler).set_leadership_grant(Some(UNBOUNDED_GRANT));
            wake.notify();
            waiting.await.unwrap().unwrap()
        });

        assert_eq!(claims.len(), 1);
    }

    #[test]
    fn a_claim_waits_while_the_worker_is_not_leader() {
        let scheduler = scheduler(None);
        submit(&scheduler);
        let wake = Wake::new();

        let outcome = block_on(async {
            tokio::time::timeout(
                Duration::from_millis(100),
                claim_when_available(scheduler, worker(), wake.subscribe(), 10, Wake::new()),
            )
            .await
        });

        assert!(outcome.is_err(), "claimed work without leading");
    }
}
