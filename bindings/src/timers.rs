//! The timer loop: sleeps until the scheduler's next deadline, then lets time
//! take effect (delayed tasks becoming due, pending tasks expiring, finished
//! tasks being forgotten) and wakes whoever that concerns.

use std::sync::Arc;
use std::time::Duration;

use kabudachi_core::time::{Clock, Instant};

use crate::door::SchedulerDoor;

/// Runs until the door is closed. Each round lets time take effect through
/// the door's `tick`, then sleeps until the scheduler's next deadline, or
/// until `timers_changed` says a deadline may have moved: a submission, a
/// report, or a change of leadership. That wake-up keeps a permit, so a
/// change that happens while the loop is working is not lost.
///
/// The door's tick never wakes the loop itself, or it would spin.
pub async fn run_timers<C: Clock + Send + Sync + 'static>(door: Arc<SchedulerDoor<C>>, clock: C) {
    loop {
        let Ok(deadline) = door.tick() else {
            return;
        };
        tokio::select! {
            _ = sleep_for(time_until(&clock, deadline)) => {}
            _ = door.timers_changed() => {}
        }
    }
}

/// How long from now by `clock` until `deadline`, if there is one.
pub(crate) fn time_until(clock: &impl Clock, deadline: Option<Instant>) -> Option<Duration> {
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
