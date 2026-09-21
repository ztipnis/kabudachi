//! The timer loop: sleeps until the scheduler's next deadline, then lets time
//! take effect (delayed tasks becoming due, pending tasks expiring, finished
//! tasks being forgotten) and wakes whoever that concerns.

use std::sync::Arc;
use std::time::Duration;

use kabudachi_core::time::Clock;
use tokio::sync::Notify;

use crate::clock::RealClock;
use crate::work::{SharedScheduler, Wake, lock_scheduler};

/// Runs until `stop` is notified. `changed` is notified whenever a deadline
/// may have moved: a submission, a report, or a change of leadership. It keeps
/// a permit, so a change that happens while the loop is working is not lost.
///
/// `claims` is woken when delayed tasks became pending, `events` when the
/// scheduler has something to tell Python.
pub async fn run_timers(
    scheduler: SharedScheduler,
    clock: RealClock,
    changed: Arc<Notify>,
    stop: Arc<Notify>,
    claims: Wake,
    events: Wake,
) {
    loop {
        let (queued, has_events, wait) = {
            let mut scheduler = lock_scheduler(&scheduler);
            let advanced = scheduler.advance();
            scheduler.sweep();
            // Only a leader acts on deadlines, so any other worker has
            // nothing to wake for until its leadership changes.
            let wait = scheduler
                .is_leading()
                .then(|| scheduler.next_deadline())
                .flatten()
                .map(|deadline| {
                    Duration::from_millis(
                        deadline.as_ticks().saturating_sub(clock.now().as_ticks()),
                    )
                });
            (advanced.queued, scheduler.has_events(), wait)
        };
        if queued > 0 {
            claims.notify();
        }
        if has_events {
            events.notify();
        }
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
