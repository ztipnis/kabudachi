//! Handing a submitted task to the shard leader in the background, and the
//! event that tells Python how the leader answered.

use std::sync::Arc;

use kabudachi_core::protocol::ids::TaskId;
use kabudachi_core::protocol::messages::{TaskRejectReason, task_response};
use kabudachi_core::scheduler::Submitted;
use kabudachi_net::payload_memory::Taken;
use kabudachi_net::task_exchange::NotStored;
use tokio::runtime::Handle;

use super::seam::Shared;
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

/// Asks leaders to store `submitted` until one does or refuses it for
/// good, and says which. A `payload` too large for one message is first
/// stored as blobs where the leader will place the task's record, and the
/// submission carries their manifest. The payload stops counting in flight
/// once its blobs are stored.
pub(super) async fn deliver(
    shared: Arc<Shared>,
    mut submitted: Submitted,
    payload: Option<(Vec<u8>, Taken)>,
) {
    let task = submitted.task_id.clone();
    if let Some((payload, _in_flight)) = payload {
        match shared
            .net
            .store_payload_at_leader(&task, &payload, shared.retry)
            .await
        {
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
    let answer = shared.net.submit_to_leader(submitted, shared.retry).await;
    let event = answer.map(|response| match response.result {
        Some(task_response::Result::Submitted(_)) => {
            PyEvent::networked(PyEventKind::Accepted, task.as_str(), "")
        }
        Some(task_response::Result::Reject(reject)) => match TaskRejectReason::try_from(reject.reason) {
            Ok(reason) => PyEvent::refused(task.as_str(), reason.as_str_name()),
            Err(_) => PyEvent::refused(task.as_str(), "a reason this worker cannot read"),
        },
        _ => PyEvent::refused(task.as_str(), "an answer that is not about a submission"),
    });
    let refused = event
        .as_ref()
        .is_some_and(|event| event.kind() == PyEventKind::Refused);
    {
        // Out of its shard, no leader will answer: nothing is said, but a
        // cancel waiting for this submission must not wait for ever.
        let mut seam = shared.seam();
        seam.delivering.remove(&task);
        if refused {
            seam.awaiting.remove(&task);
        }
        seam.events.extend(event);
    }
    shared.wake();
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
