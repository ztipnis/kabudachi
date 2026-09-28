//! The timer loop: sleeps until the scheduler's next deadline, then lets time
//! take effect (delayed tasks becoming due, pending tasks expiring, finished
//! tasks being forgotten) and wakes whoever that concerns.

use std::sync::Arc;
use std::time::Duration;

use kabudachi_core::time::{Clock, Instant, RealClock};
use tokio::sync::Notify;

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
            let deadline = scheduler
                .is_leading()
                .then(|| scheduler.next_deadline())
                .flatten();
            time_until(clock, deadline)
        });
        tokio::select! {
            _ = sleep_for(wait) => {}
            _ = changed.notified() => {}
            _ = stop.notified() => return,
        }
    }
}

/// How long from now by `clock` until `deadline`, if there is one.
pub(crate) fn time_until(clock: RealClock, deadline: Option<Instant>) -> Option<Duration> {
    // `core::time`'s `Instant` subtraction already saturates at zero, and its
    // ticks are documented milliseconds, so this needs no separate "1 tick =
    // 1 ms" assumption of its own.
    deadline.map(|deadline| Duration::from_millis((deadline - clock.now()).as_ticks()))
}

/// Sleeps for `wait`, or for ever if there is nothing to wait for.
pub(crate) async fn sleep_for(wait: Option<Duration>) {
    match wait {
        Some(wait) => tokio::time::sleep(wait).await,
        None => std::future::pending().await,
    }
}
