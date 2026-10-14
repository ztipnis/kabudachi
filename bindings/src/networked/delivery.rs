//! Handing a submitted task to the shard leader in the background, and the
//! event that tells Python how the leader answered.

use std::sync::Arc;
use std::time::Duration;

use kabudachi_core::blob::BlobOwner;
use kabudachi_core::protocol::ids::TaskId;
use kabudachi_core::protocol::messages::{TaskRejectReason, task_response};
use kabudachi_core::scheduler::Submitted;
use kabudachi_core::task_record::ORPHAN_BLOB_GRACE;
use kabudachi_net::blobs::BlobRole;
use kabudachi_net::payload_memory::Taken;
use kabudachi_net::task_exchange::NotStored;
use tokio::runtime::Handle;

use super::seam::Shared;
use super::slow_down::heard_slow_down;
use crate::outcomes::PyEventKind;
use crate::work::PyEvent;

/// Asks leaders, in the background on `handle`, to store `submitted`, and
/// returns its task id. Until a leader answers, a cancel of the task waits.
pub(super) fn send_submission(
    shared: &Arc<Shared>,
    handle: &Handle,
    submitted: Submitted,
    payload: Option<(Vec<u8>, Taken)>,
) -> String {
    let task_id = submitted.task_id.as_str().to_owned();
    {
        let mut seam = shared.seam();
        seam.delivering.insert(submitted.task_id.clone());
        seam.awaiting.insert(submitted.task_id.clone());
    }
    handle.spawn(deliver(Arc::clone(shared), submitted, payload));
    task_id
}

impl Shared {
    /// Ends the delivery of `task` with a refusal for `reason`, which its
    /// handle fails with.
    fn refuse(&self, task: &TaskId, reason: &str) {
        {
            let mut seam = self.seam();
            seam.delivering.remove(task);
            seam.awaiting.remove(task);
            seam.events.push_back(PyEvent::refused(task.as_str(), reason));
        }
        self.wake();
    }
}

/// How long a submission that carries its input as blobs may take, from the
/// start of storing them to the leader's answer, before it is refused as
/// backpressure. Holders drop the blobs of an owner whose record they never
/// saw once the leader has twice, an orphan grace apart, said it knows no such
/// task; the first such look-up comes a grace after the first blob. Half a
/// grace keeps a late submission well clear of that first look-up, so blobs
/// whose task the leader accepted are never dropped for a window an accepted
/// submission could still be in.
pub(super) fn blob_submission_deadline() -> Duration {
    Duration::from_millis(ORPHAN_BLOB_GRACE.as_ticks() / 2)
}

/// Asks leaders to store `submitted` until one does or refuses it for
/// good, and says which. A `payload` too large for one message is first
/// stored as blobs where the leader will place the task's record, and the
/// submission carries their manifest; the storing and the submission together
/// take no longer than [`blob_submission_deadline`], else the handle fails as
/// for backpressure. The payload stops counting in flight once its blobs are
/// stored.
pub(super) async fn deliver(
    shared: Arc<Shared>,
    mut submitted: Submitted,
    payload: Option<(Vec<u8>, Taken)>,
) {
    let task = submitted.task_id.clone();
    let mut give_up_at = None;
    if let Some((payload, _in_flight)) = payload {
        let at = tokio::time::Instant::now() + blob_submission_deadline();
        give_up_at = Some(at);
        let stored = tokio::time::timeout_at(
            at,
            shared
                .net
                .store_payload_at_leader(BlobRole::Input, &BlobOwner::Task(task.clone()), &payload, shared.retry),
        )
        .await
        .unwrap_or(Err(NotStored::Refused));
        match stored {
            Ok(manifest) => {
                submitted.submission = submitted.submission.with_input_manifest(manifest);
            }
            // Out of its shard, no leader will answer: as below, nothing is
            // said, but a cancel waiting for this submission must not wait
            // for ever.
            Err(NotStored::LeftShard) => {
                shared.seam().delivering.remove(&task);
                shared.wake();
                return;
            }
            // The shard's stores are full, or their holders unreachable, for
            // now: the handle fails as for a leader's backpressure refusal,
            // never for good. The blobs stored so far belong to a task no
            // leader has seen, which their holders drop.
            Err(NotStored::Refused) => {
                let reason = TaskRejectReason::TaskRejectBackpressure.as_str_name();
                shared.refuse(&task, reason);
                return;
            }
        }
    }
    let submission = shared.net.submit_to_leader(submitted, shared.retry);
    let answer = match give_up_at {
        None => submission.await,
        Some(at) => match tokio::time::timeout_at(at, submission).await {
            Ok(answer) => answer,
            Err(_) => {
                let reason = TaskRejectReason::TaskRejectBackpressure.as_str_name();
                shared.refuse(&task, reason);
                // The request may already have reached the leader, which then
                // holds a task whose handle was refused: cancel it, so a retry
                // by the user cannot run it twice. A leader that never saw it
                // answers that the task is unknown.
                tokio::spawn(cancel_at_leader(Arc::clone(&shared), task));
                return;
            }
        },
    };
    // Out of its shard, no leader will answer: nothing is said, but a
    // cancel waiting for this submission must not wait for ever.
    let Some(response) = answer else {
        shared.seam().delivering.remove(&task);
        shared.wake();
        return;
    };
    match response.result {
        Some(task_response::Result::Submitted(accepted)) => {
            heard_slow_down(&shared, accepted.slow_down);
            {
                let mut seam = shared.seam();
                seam.delivering.remove(&task);
                seam.events
                    .push_back(PyEvent::networked(PyEventKind::Accepted, task.as_str(), ""));
            }
            shared.wake();
        }
        Some(task_response::Result::Reject(reject)) => match TaskRejectReason::try_from(reject.reason) {
            Ok(reason) => shared.refuse(&task, reason.as_str_name()),
            Err(_) => shared.refuse(&task, "a reason this worker cannot read"),
        },
        _ => shared.refuse(&task, "an answer that is not about a submission"),
    }
}

/// Asks leaders to cancel `task` until one answers, once its own submission
/// from here (if any) has been answered. How the cancel ended is not
/// reported: a run it stopped is ended by the driver's `Cancel`, wherever
/// it runs, and a task it found finished or unknown needs nothing.
pub(super) async fn cancel_at_leader(shared: Arc<Shared>, task: TaskId) {
    while shared.seam().delivering.contains(&task) {
        if shared.net.has_left_shard() {
            return;
        }
        tokio::time::sleep(shared.retry.ask_again_after).await;
    }
    let _ = shared.net.cancel_at_leader(task, shared.retry).await;
}
