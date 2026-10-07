use crate::protocol::generated::TaskRecord;
use crate::protocol::records::TaskRunRecord;
use crate::protocol::task::TaskRunState;
use crate::time::WallTime;

/// Whether `record`, as this worker holds it, shows a task a worker could
/// claim at `now` by its own wall clock: not finished, its latest run waiting
/// (`Queued`, or `Scheduled` with its delay passed), and not past its expiry.
/// Only a hint: the record may be stale and the clocks disagree, so the
/// leader may still refuse the claim.
pub fn looks_claimable(record: &TaskRecord, now: WallTime) -> bool {
    if record.finished {
        return false;
    }
    let (Some(task), Some(latest)) = (record.task.as_ref(), record.runs.last()) else {
        return false;
    };
    let Some(submitted_at) = task.submitted_at.map(WallTime::from) else {
        return false;
    };
    let since = |millis: u64| submitted_at.plus_millis(millis);
    if task.expiry_millis.is_some_and(|expiry| since(expiry) <= now) {
        return false;
    }
    match latest.current_state() {
        TaskRunState::Queued => true,
        TaskRunState::Scheduled => task.delay_millis.is_none_or(|delay| since(delay) <= now),
        _ => false,
    }
}
