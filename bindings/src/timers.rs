//! The timer loop: sleeps until the scheduler's next deadline, then lets time
//! take effect (delayed tasks becoming due, pending tasks expiring, finished
//! tasks being forgotten) and wakes whoever that concerns.

use std::sync::Arc;
use std::time::Duration;

use kabudachi_core::time::Clock;
use tokio::sync::Notify;

use crate::clock::RealClock;
use crate::wakeups::Wakeups;
use crate::work::SharedScheduler;

/// Runs until `stop` is notified. `changed` is notified whenever a deadline
/// may have moved: a submission, a report, or a change of leadership. It keeps
/// a permit, so a change that happens while the loop is working is not lost.
///
/// `wakeups` wakes claims and waits for events, and is the loop's own, so
/// letting time take effect here never notifies `changed` and spins the loop.
pub async fn run_timers(
    scheduler: SharedScheduler,
    clock: RealClock,
    changed: Arc<Notify>,
    stop: Arc<Notify>,
    wakeups: Wakeups,
) {
    loop {
        let wait = wakeups.with_scheduler(&scheduler, |scheduler| {
            scheduler.advance();
            // Only a leader acts on deadlines, so any other worker has
            // nothing to wake for until its leadership changes.
            scheduler
                .is_leading()
                .then(|| scheduler.next_deadline())
                .flatten()
                .map(|deadline| {
                    Duration::from_millis(
                        deadline.as_ticks().saturating_sub(clock.now().as_ticks()),
                    )
                })
        });
        tokio::select! {
            _ = sleep_for(wait) => {}
            _ = changed.notified() => {}
            _ = stop.notified() => return,
        }
    }
}

/// Sleeps for `wait`, or for ever if there is nothing to wait for.
async fn sleep_for(wait: Option<Duration>) {
    match wait {
        Some(wait) => tokio::time::sleep(wait).await,
        None => std::future::pending().await,
    }
}
