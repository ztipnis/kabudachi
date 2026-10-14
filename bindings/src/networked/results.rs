//! A task's result on its way to the worker that awaits it: delivered from
//! here before its run is reported, and what arrives here for the tasks and
//! composites submitted here, asked of a new leader when the leader changes.
//! A run that returns a step delivers no result: its blobs are stored, and it
//! is reported with the step.

use std::sync::Arc;

use kabudachi_core::blob::{BlobOwner, MAX_BLOB_BYTES};
use kabudachi_core::manifest::{Manifest, fits_inline};
use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::ids::{CompositeId, TaskId, TaskRunId, WorkerId};
use kabudachi_core::protocol::messages::{
    DeliverResult, OutcomeAnswer, TaskResponse, deliver_result, task_response,
};
use kabudachi_core::protocol::task::TaskRunState as DomainRunState;
use kabudachi_core::result_backend::PersistenceState;
use kabudachi_core::scheduler::Event;
use kabudachi_core::scheduler::composite::{CompositeFinished, Step};
use kabudachi_net::blobs::BlobRole;
use kabudachi_net::executor::Report;
use kabudachi_net::messenger::Net;
use kabudachi_net::result_transport::ResultEvent;

use super::composites::{ask_about_composites, stored_until};
use super::seam::{HeldRun, Seam, Shared};
use crate::work::PyEvent;

/// The failure kind a client's handle fails with when the result alone is
/// past this process's hard memory limit.
const RESULT_TOO_LARGE_KIND: &str = "ResultTooLargeError";

/// Delivers a run's result to its client, then reports the run completed,
/// saying whether the result arrived. The run's reconnect timeout bounds the
/// delivery: past it the leader may replay the run anyway. A result too
/// large for one message is first stored as blobs, and a run whose result
/// cannot be stored by then is reported lost, so a task that retries is
/// replayed.
pub(super) async fn deliver_then_report(
    shared: Arc<Shared>,
    client: WorkerId,
    held: HeldRun,
    run: TaskRunId,
    digest: Digest,
    payload: Vec<u8>,
    persistence: PersistenceState,
) {
    let Some((body, result_manifest)) = result_body(&shared, &held, payload).await else {
        // Never reported completed, so nothing will answer it.
        shared.seam().mirroring.remove(&run);
        shared.report(Report::Lost(run));
        return;
    };
    let delivery = DeliverResult {
        task_id: Some(held.task.into()),
        task_run_id: Some(run.clone().into()),
        result_digest: Some(digest.clone().into()),
        body: Some(body),
    };
    let delivered = shared
        .net
        .deliver_result(client, delivery, held.reconnect_timeout)
        .await
        .is_ok();
    shared.report(Report::Completed {
        run,
        digest,
        delivered,
        continuation: None,
        persistence,
        result_manifest,
    });
}

/// What carries `payload` to the client: the payload itself, or, past one
/// message, the manifest of blobs owned by the task, stored where its leader
/// keeps the task's record, so they live, and move, with it. Beside the
/// manifest goes the digest of the blob that holds it, which the run is
/// certified with so a reader who finds the result nowhere else can fetch it
/// from the shard; it is `None` for an inline payload, and for a manifest too
/// large to be a blob itself. `None` if the blobs could not be stored within
/// the run's reconnect timeout.
///
/// The payload is in this process's memory already, so it is counted, not
/// waited for, while it is stored: this worker claims no blob input
/// meanwhile if that leaves no room. The session refused one alone past the
/// limit before reporting, so counting it cannot be refused here. A shard
/// whose stores are full refuses for now, and the store is asked again until
/// the bound.
async fn result_body(
    shared: &Shared,
    held: &HeldRun,
    payload: Vec<u8>,
) -> Option<(deliver_result::Body, Option<Digest>)> {
    if fits_inline(payload.len()) {
        return Some((deliver_result::Body::Payload(payload), None));
    }
    let _in_flight = shared
        .net
        .payload_memory()
        .take_anyway(payload.len() as u64)
        .ok();
    let bound = tokio::time::Instant::now() + held.reconnect_timeout;
    let owner = BlobOwner::Task(held.task.clone());
    let cut = Manifest::chunk(&payload);
    let encoded = cut.manifest.encode();
    // A manifest past one blob is still delivered to the client, but cannot be
    // read back from the shard.
    let stored_manifest = encoded.len() <= MAX_BLOB_BYTES;
    let mut blobs = cut.chunks;
    if stored_manifest {
        blobs.push(encoded);
    }
    // `None` once out of its shard, or past the bound.
    stored_until(shared, bound, || {
        shared
            .net
            .store_blobs_at_leader(BlobRole::Result, &owner, blobs.clone(), shared.retry)
    })
    .await?;
    let digest = stored_manifest.then(|| cut.manifest.digest());
    Some((deliver_result::Body::Manifest(cut.manifest.into()), digest))
}

/// Stores the blobs a returned `step` names under the continuation it
/// becomes, then reports `run` completed with the step. A run whose values
/// cannot be stored by its reconnect timeout is reported lost, so a task that
/// retries is replayed. Each value counts in flight while it is stored: it is
/// in this process's memory already, so it is counted, not waited for.
pub(super) async fn store_then_report_step(
    shared: Arc<Shared>,
    held: HeldRun,
    run: TaskRunId,
    step: Step,
    values: Vec<Vec<u8>>,
) {
    let owner = BlobOwner::Composite(CompositeId::continuation_of(&run));
    let bound = tokio::time::Instant::now() + held.reconnect_timeout;
    for value in &values {
        let _in_flight = shared.net.payload_memory().take_anyway(value.len() as u64).ok();
        let stored = stored_until(&shared, bound, || {
            shared
                .net
                .store_payload_at_leader(BlobRole::Input, &owner, value, shared.retry)
        })
        .await;
        if stored.is_none() {
            return shared.report(Report::Lost(run));
        }
    }
    shared.report(Report::Completed {
        run,
        digest: step.digest(),
        delivered: false,
        continuation: Some(step),
        persistence: PersistenceState::Disabled,
        result_manifest: None,
    });
}

/// What the result reader makes of one result event before the session sees
/// it.
enum Fetched {
    /// `event`, a delivered manifest's blobs joined into its payload.
    Event(ResultEvent),
    /// The result alone is past this process's hard memory limit: its
    /// awaiter's handle fails with `ResultTooLargeError`.
    TooLarge { task: TaskId, run: TaskRunId },
    /// Its blobs cannot be fetched. The result is then not held, as for a
    /// delivery that never arrived, and its certification finds nothing to
    /// match.
    Unavailable,
}

/// `event`, with a delivered manifest's blobs joined into its payload. The
/// payload counts in flight while it is fetched, and stops when it is handed
/// on to the session. It is counted, not waited for: a body of this process
/// that awaits this very result is holding its own input's bytes, which stay
/// counted until it ends, and a reader waiting for room past them would wait
/// for ever. Only a result alone past the hard limit is refused.
async fn with_payload(net: &Net, event: ResultEvent) -> Fetched {
    let ResultEvent::Delivered {
        task,
        run,
        digest,
        body: deliver_result::Body::Manifest(wire),
    } = event
    else {
        return Fetched::Event(event);
    };
    let Ok(manifest) = Manifest::try_from(&wire) else {
        return Fetched::Unavailable;
    };
    if manifest.check_cut_payload().is_err() {
        return Fetched::Unavailable;
    }
    let Ok(_in_flight) = net.payload_memory().take_anyway(manifest.total_bytes()) else {
        return Fetched::TooLarge { task, run };
    };
    let owner = BlobOwner::Task(task.clone());
    let Ok(payload) = net.resolve_manifest(&owner, &manifest).await else {
        return Fetched::Unavailable;
    };
    Fetched::Event(ResultEvent::Delivered {
        task,
        run,
        digest,
        body: deliver_result::Body::Payload(payload),
    })
}

impl Shared {
    /// Tells Python a result is past this process's hard memory limit, so
    /// its awaiter's handle fails; the run stays completed.
    fn on_result_too_large(&self, task: &TaskId, run: &TaskRunId) {
        self.seam().events.push_back(PyEvent::outcome(
            task.as_str(),
            run.as_str(),
            Some(DomainRunState::Failed.into()),
            RESULT_TOO_LARGE_KIND.into(),
            None,
        ));
        self.wake();
    }

    /// Hands Python what arrived for a task submitted here: a result, a
    /// certification, or how the task ended without one.
    fn on_result(&self, event: ResultEvent) {
        let told = {
            let mut seam = self.seam();
            let event = seam.told(event);
            let told = event.is_some();
            seam.events.extend(event);
            told
        };
        if told {
            self.wake();
        }
    }
}

impl Seam {
    /// What Python is told of `event`; a task whose outcome it settles is no
    /// longer awaited.
    fn told(&mut self, event: ResultEvent) -> Option<PyEvent> {
        match event {
            ResultEvent::Delivered {
                task,
                run,
                body: deliver_result::Body::Payload(payload),
                ..
            } => Some(PyEvent::result(task.as_str(), run.as_str(), payload)),
            // The reader joins a manifest's blobs before this.
            ResultEvent::Delivered {
                body: deliver_result::Body::Manifest(_),
                ..
            } => None,
            ResultEvent::Certified { task, run, digest } => {
                self.awaiting.remove(&task);
                Some(PyEvent::certified(
                    task.as_str(),
                    run.as_str(),
                    digest.value().to_vec(),
                ))
            }
            ResultEvent::Concluded { task, outcome } => {
                let event = outcome_event(&task, &outcome)?;
                self.awaiting.remove(&task);
                Some(event)
            }
            ResultEvent::CompositeFinished(wire) => {
                let finished = CompositeFinished::try_from(&wire).ok()?;
                self.awaiting_composites.remove(&finished.composite_id);
                if let Some(task) = &finished.parent_task {
                    self.awaiting.remove(task);
                }
                Some(PyEvent::from(Event::CompositeFinished(finished)))
            }
        }
    }
}

/// What Python is told of `task` from `answer`: a certification, an end
/// without a result, or that the shard does not know it; nothing while it
/// is not over.
fn outcome_event(task: &TaskId, answer: &OutcomeAnswer) -> Option<PyEvent> {
    if !answer.known {
        return Some(PyEvent::outcome(
            task.as_str(),
            "",
            None,
            String::new(),
            None,
        ));
    }
    // A run that returned a step has no result to certify: the task is over
    // when the step's composite is, which is told on its own.
    if !answer.finished || answer.continued {
        return None;
    }
    let run = answer
        .task_run_id
        .clone()
        .map(|run| TaskRunId::from(run).as_str().to_owned())
        .unwrap_or_default();
    let state = DomainRunState::try_from(answer.state()).ok()?;
    if state == DomainRunState::Succeeded {
        let digest = answer.result_digest.as_ref()?.value.clone();
        return Some(PyEvent::certified(task.as_str(), &run, digest));
    }
    Some(PyEvent::outcome(
        task.as_str(),
        &run,
        Some(state.into()),
        answer.failure_kind.clone(),
        answer
            .superseded_by
            .clone()
            .map(|newer| TaskId::from(newer).as_str().to_owned()),
    ))
}

/// Reads the result messages arriving for the tasks submitted here and
/// hands each to Python.
pub(super) async fn read_results(shared: Arc<Shared>) {
    loop {
        shared.net.result_arrived().await;
        for event in shared.net.take_result_events() {
            match with_payload(&shared.net, event).await {
                Fetched::Event(event) => shared.on_result(event),
                Fetched::TooLarge { task, run } => shared.on_result_too_large(&task, &run),
                Fetched::Unavailable => {}
            }
        }
    }
}

/// Whenever the leader this worker's node names changes to another, or the
/// same worker leads in a later term, asks it how each task submitted here,
/// stored and still awaited, stands: a certification or an ending the old
/// leader decided may never have been told. A spell with no leader named
/// counts as a change, even when the same leader is named after it, since
/// what it told meanwhile may not have arrived; so does one that started
/// and ended while asking. The answers come in as any conclusion does.
pub(super) async fn ask_after_leader_change(shared: Arc<Shared>) {
    let mut observed = shared.observed.clone();
    let (mut named, mut seen_changes) = {
        let now = observed.borrow();
        (now.leader.clone(), now.leader_changes)
    };
    while observed.changed().await.is_ok() {
        let (leader, changes) = {
            let now = observed.borrow_and_update();
            (now.leader.clone(), now.leader_changes)
        };
        let Some(now) = leader else {
            continue;
        };
        if changes == seen_changes {
            continue;
        }
        seen_changes = changes;
        if named.replace(now).is_some() {
            ask_about_awaited(&shared).await;
        }
    }
}

/// Asks the leader how each task submitted here, stored and still awaited,
/// stands, and queues each answer behind the result messages that arrived
/// before it, so the result reader hands Python every event in one order.
async fn ask_about_awaited(shared: &Shared) {
    let tasks: Vec<TaskId> = {
        let seam = shared.seam();
        seam.awaiting
            .difference(&seam.delivering)
            .cloned()
            .collect()
    };
    for task in tasks {
        let answer = shared
            .net
            .query_outcome_at_leader(task.clone(), shared.retry)
            .await;
        if let Some(TaskResponse {
            result: Some(task_response::Result::Outcome(outcome)),
        }) = answer
        {
            if let Some(run) = continuing_run(&outcome) {
                // Certified as the step it returned, which runs as a
                // composite: the task is over when that is.
                shared
                    .seam()
                    .awaiting_composites
                    .insert(CompositeId::continuation_of(&run));
            }
            shared
                .net
                .queue_result_event(ResultEvent::Concluded { task, outcome });
        }
    }
    ask_about_composites(shared).await;
}

/// The run that returned a step, if `answer` says a run did, whether the step
/// is still running or has ended: its composite's record says how it ended.
fn continuing_run(answer: &OutcomeAnswer) -> Option<TaskRunId> {
    let continuing = answer.known && answer.continued;
    continuing.then(|| answer.task_run_id.clone().map(TaskRunId::from)).flatten()
}
