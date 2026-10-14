//! Composites on a networked worker: handed to the shard's leader in the
//! background, cancelled the same way, read from the records and blobs the
//! shard holds, and asked about again when the leader changes.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use kabudachi_core::blob::BlobOwner;
use kabudachi_core::protocol::generated;
use kabudachi_core::protocol::ids::CompositeId;
use kabudachi_core::protocol::messages::{TaskRejectReason, task_response};
use kabudachi_core::scheduler::composite::{CompositeFinished, MemberBatch, SubmittedComposite, Unresolved};
use kabudachi_net::blobs::BlobRole;
use kabudachi_net::payload_memory::Taken;
use kabudachi_net::result_transport::ResultEvent;
use kabudachi_net::task_exchange::NotStored;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use tokio::runtime::Handle;
use tokio::time::Instant;

use super::delivery::blob_submission_deadline;
use super::seam::{GONE_MESSAGE, Shared};
use super::slow_down::heard_slow_down;
use crate::errors::backpressure_error;
use crate::outcomes::PyEventKind;
use crate::work::PyEvent;

/// What a `BackpressureError` says when the shard's stores are full for now.
const STORES_FULL_MESSAGE: &str = "the shard's stores cannot take the members' payloads now";

/// Asks leaders, in the background on `handle`, to run `submitted`, and
/// returns its composite id. `values` are the bound inputs too large to
/// inline, each held in flight until it is stored, which happens first. Until
/// a leader answers, a cancel of the composite waits.
pub(super) fn send_composite(
    shared: &Arc<Shared>,
    handle: &Handle,
    submitted: SubmittedComposite,
    values: Vec<(Vec<u8>, Taken)>,
) -> String {
    let id = submitted.composite_id.clone();
    {
        let mut seam = shared.seam();
        seam.delivering_composites.insert(id.clone());
        seam.awaiting_composites.insert(id.clone());
    }
    handle.spawn(deliver_composite(Arc::clone(shared), submitted, values));
    id.as_str().to_owned()
}

impl Shared {
    /// Ends the delivery of the composite `id` with a refusal for `reason`,
    /// which its handle fails with.
    fn refuse_composite(&self, id: &CompositeId, reason: &str) {
        {
            let mut seam = self.seam();
            seam.delivering_composites.remove(id);
            seam.awaiting_composites.remove(id);
            seam.refused_composites.insert(id.clone());
            seam.events.push_back(PyEvent::refused(id.as_str(), reason));
        }
        self.wake();
    }

    /// Ends the delivery of the composite `id` with no word to Python: no
    /// leader will answer, but a cancel waiting for it must not wait for ever.
    fn abandon_composite(&self, id: &CompositeId) {
        self.seam().delivering_composites.remove(id);
        self.wake();
    }
}

/// Asks leaders to run `submitted` until one records it or refuses it for
/// good, and says which. The values it names by manifest are first stored as
/// blobs the composite owns, where the leader will place its record; the
/// storing and the submission together take no longer than
/// [`blob_submission_deadline`], else the handle fails as for backpressure.
async fn deliver_composite(shared: Arc<Shared>, submitted: SubmittedComposite, values: Vec<(Vec<u8>, Taken)>) {
    let id = submitted.composite_id.clone();
    let mut give_up_at = None;
    if !values.is_empty() {
        let at = Instant::now() + blob_submission_deadline();
        give_up_at = Some(at);
        let owner = BlobOwner::Composite(id.clone());
        for (value, _in_flight) in values {
            let stored = tokio::time::timeout_at(
                at,
                shared
                    .net
                    .store_payload_at_leader(BlobRole::Input, &owner, &value, shared.retry),
            )
            .await
            .unwrap_or(Err(NotStored::Refused));
            match stored {
                Ok(_) => {}
                Err(NotStored::LeftShard) => return shared.abandon_composite(&id),
                Err(NotStored::Refused) => {
                    let reason = TaskRejectReason::TaskRejectBackpressure.as_str_name();
                    return shared.refuse_composite(&id, reason);
                }
            }
        }
    }
    let submission = shared.net.submit_composite_to_leader(&submitted, shared.retry);
    let answer = match give_up_at {
        None => submission.await,
        Some(at) => match tokio::time::timeout_at(at, submission).await {
            Ok(answer) => answer,
            Err(_) => {
                let reason = TaskRejectReason::TaskRejectBackpressure.as_str_name();
                shared.refuse_composite(&id, reason);
                // The request may already have reached the leader, which then
                // holds a composite whose handle was refused: cancel it, so a
                // retry by the user cannot run it twice.
                tokio::spawn(cancel_composite_at_leader(Arc::clone(&shared), id));
                return;
            }
        },
    };
    let Some(response) = answer else {
        return shared.abandon_composite(&id);
    };
    match response.result {
        Some(task_response::Result::Composite(accepted)) => {
            heard_slow_down(&shared, accepted.slow_down);
            {
                let mut seam = shared.seam();
                seam.delivering_composites.remove(&id);
                seam.events
                    .push_back(PyEvent::networked(PyEventKind::Accepted, id.as_str(), ""));
            }
            shared.wake();
        }
        Some(task_response::Result::Reject(reject)) => match TaskRejectReason::try_from(reject.reason) {
            Ok(reason) => shared.refuse_composite(&id, reason.as_str_name()),
            Err(_) => shared.refuse_composite(&id, "a reason this worker cannot read"),
        },
        _ => shared.refuse_composite(&id, "an answer that is not about a submission"),
    }
}

/// Takes `batch` to the shard's leader once the composite's own submission
/// from here, if any, has been answered, after storing the `values` it names
/// by manifest as blobs the composite owns (counted in flight until then).
/// Resolves to `"accepted"`, or `"finished"` when the composite is over (or
/// its own submission was refused) and takes no more members. The leader is
/// asked again after a refusal for now, with the same batch, so a batch whose
/// answer was lost creates nothing twice.
///
/// Fails with `BackpressureError` when the shard's stores are full for now or
/// the leader refuses the batch as backpressure, `ValueError` when the leader
/// finds it malformed, and `RuntimeError` once the worker has left its shard.
pub(super) async fn send_members(
    shared: &Arc<Shared>,
    batch: MemberBatch,
    values: Vec<(Vec<u8>, Taken)>,
) -> PyResult<&'static str> {
    let id = batch.composite_id.clone();
    while shared.seam().delivering_composites.contains(&id) {
        if shared.net.has_left_shard() {
            return Err(PyRuntimeError::new_err(GONE_MESSAGE));
        }
        tokio::time::sleep(shared.retry.ask_again_after).await;
    }
    if shared.seam().refused_composites.contains(&id) {
        return Ok("finished");
    }
    let at = Instant::now() + blob_submission_deadline();
    let owner = BlobOwner::Composite(id);
    for (value, _in_flight) in &values {
        let stored = tokio::time::timeout_at(
            at,
            shared
                .net
                .store_payload_at_leader(BlobRole::Input, &owner, value, shared.retry),
        )
        .await
        .unwrap_or(Err(NotStored::Refused));
        match stored {
            Ok(_) => {}
            Err(NotStored::LeftShard) => return Err(PyRuntimeError::new_err(GONE_MESSAGE)),
            Err(NotStored::Refused) => return Err(backpressure(STORES_FULL_MESSAGE)),
        }
    }
    let Some(response) = shared.net.submit_members(&batch, shared.retry).await else {
        return Err(PyRuntimeError::new_err(GONE_MESSAGE));
    };
    match response.result {
        Some(task_response::Result::Members(accepted)) => {
            heard_slow_down(shared, accepted.slow_down);
            Ok("accepted")
        }
        Some(task_response::Result::Reject(reject)) => match TaskRejectReason::try_from(reject.reason) {
            Ok(TaskRejectReason::TaskRejectCompositeFinished) => Ok("finished"),
            Ok(
                TaskRejectReason::TaskRejectBackpressure
                | TaskRejectReason::TaskRejectTooLarge
                | TaskRejectReason::TaskRejectPayloadTooLarge,
            ) => Err(backpressure("the shard's leader cannot take the members now")),
            Ok(reason) => Err(PyValueError::new_err(format!(
                "the shard's leader refused the members: {}",
                reason.as_str_name()
            ))),
            Err(_) => Err(PyValueError::new_err("the leader refused the members for a reason this worker cannot read")),
        },
        _ => Err(PyValueError::new_err("an answer that is not about the members")),
    }
}

/// A `BackpressureError` raised from the worker's own runtime.
fn backpressure(message: &str) -> PyErr {
    Python::attach(|py| backpressure_error(py, message.to_owned()))
}

/// Asks leaders to cancel the composite `id` until one answers, once its own
/// submission from here has been answered. How the cancel ended is not
/// reported: the composite's end is told as any is.
pub(super) async fn cancel_composite_at_leader(shared: Arc<Shared>, id: CompositeId) {
    while shared.seam().delivering_composites.contains(&id) {
        if shared.net.has_left_shard() {
            return;
        }
        tokio::time::sleep(shared.retry.ask_again_after).await;
    }
    let _ = shared.net.cancel_composite_at_leader(id, shared.retry).await;
}

/// Runs `attempt` until it stores, asking again after a refusal for now, up
/// to `bound`; `None` once the worker has left its shard or `bound` has passed.
pub(super) async fn stored_until<T, Fut>(shared: &Shared, bound: Instant, attempt: impl Fn() -> Fut) -> Option<T>
where
    Fut: Future<Output = Result<T, NotStored>>,
{
    loop {
        match tokio::time::timeout_at(bound, attempt()).await {
            Ok(Ok(stored)) => return Some(stored),
            Ok(Err(NotStored::Refused)) => tokio::time::sleep(shared.retry.ask_again_after).await,
            Ok(Err(NotStored::LeftShard)) | Err(_) => return None,
        }
    }
}

/// Runs `read` until it reads, asking again after a record, page or blob is
/// missing, up to `within`: a record just written may not have reached every
/// holder yet.
pub(super) async fn read_until<T, Fut>(
    shared: &Shared,
    within: Duration,
    read: impl Fn() -> Fut,
) -> Result<T, Unresolved>
where
    Fut: Future<Output = Result<T, Unresolved>>,
{
    let bound = Instant::now() + within;
    loop {
        match read().await {
            Err(Unresolved::MissingComposite(_) | Unresolved::MissingPage(..) | Unresolved::MissingBlob(_))
                if Instant::now() < bound =>
            {
                tokio::time::sleep(shared.retry.ask_again_after).await;
            }
            read => return read,
        }
    }
}

/// Reads the record of each composite still awaited here, and queues a finish
/// for each that is over and whose pages are held: a notice lost with a
/// leader still settles its handle. The finish comes in as any does, behind
/// the result messages that arrived before it.
pub(super) async fn ask_about_composites(shared: &Shared) {
    let awaited: Vec<CompositeId> = {
        let seam = shared.seam();
        seam.awaiting_composites
            .difference(&seam.delivering_composites)
            .cloned()
            .collect()
    };
    for id in awaited {
        let Some(record) = shared.net.composite_record(&id).await else {
            continue;
        };
        let Some(finished) = CompositeFinished::of_record(&record) else {
            continue;
        };
        if !pages_held(shared, &finished).await {
            continue;
        }
        shared
            .net
            .queue_result_event(ResultEvent::CompositeFinished(generated::CompositeFinished::from(
                &finished,
            )));
    }
}

/// Whether every page of the finished composite can be read: its results
/// resolve only then.
async fn pages_held(shared: &Shared, finished: &CompositeFinished) -> bool {
    for page in 0..finished.pages {
        if shared.net.composite_page(&finished.composite_id, page).await.is_none() {
            return false;
        }
    }
    true
}
