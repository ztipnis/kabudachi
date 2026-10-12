use std::sync::Arc;

use kabudachi_core::protocol::ids::TaskId;
use kabudachi_core::protocol::messages::{TaskRejectReason, task_response};
use kabudachi_core::scheduler::Submitted;
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
) -> String {
    let task_id = submitted.task_id.as_str().to_owned();
    shared.seam().delivering.insert(submitted.task_id.clone());
    handle.spawn(deliver(Arc::clone(shared), submitted));
    task_id
}

/// Asks leaders to store `submitted` until one does or refuses it for
/// good, and says which.
pub(super) async fn deliver(shared: Arc<Shared>, submitted: Submitted) {
    let task = submitted.task_id.clone();
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
    {
        // Out of its shard, no leader will answer: nothing is said, but a
        // cancel waiting for this submission must not wait for ever.
        let mut seam = shared.seam();
        seam.delivering.remove(&task);
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
