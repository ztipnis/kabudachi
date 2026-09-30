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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use kabudachi_core::protocol::ids::TaskDefinitionId;
    use kabudachi_core::protocol::messages::prelude::*;
    use kabudachi_core::protocol::task::TaskRunState;
    use kabudachi_core::scheduler::Submission;
    use kabudachi_core::time::Duration as CoreDuration;
    use tokio::time::timeout;

    use super::*;
    use crate::door::tests::{ManualClock, UNBOUNDED_GRANT, door};

    const WAIT_LIMIT: Duration = Duration::from_secs(5);

    #[tokio::test]
    async fn the_timer_loop_releases_a_delayed_task_once_the_clock_reaches_its_due_time() {
        let clock = ManualClock::default();
        let door = door(clock.clone(), Some(UNBOUNDED_GRANT));
        let task = door
            .submit(
                Submission::new(TaskDefinitionId::new("definition"), 0, Vec::new(), "default")
                    .with_delay(CoreDuration::from_millis(20)),
            )
            .unwrap();
        let timers = tokio::spawn(run_timers(Arc::clone(&door), clock.clone()));
        let claim = tokio::spawn(Arc::clone(&door).claim_when_available(1));
        // Lets the claim look once and start waiting: a claim made after the
        // clock moved would release the task itself, and prove nothing.
        tokio::task::yield_now().await;
        assert!(!claim.is_finished(), "nothing is due yet");
        let run = door.run_ids(&task).unwrap().remove(0);
        // Whatever real time passes, the clock has not moved.
        assert_eq!(door.run_state(&run), Ok(Some(TaskRunState::Scheduled)));

        clock.advance(20);

        let claims = timeout(WAIT_LIMIT, claim)
            .await
            .expect("the timer loop released the task within the limit")
            .expect("the claim did not panic")
            .expect("the door is open");
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].task.task_id(), task);
        door.close();
        timeout(WAIT_LIMIT, timers)
            .await
            .expect("the timer loop ends once the door closes")
            .expect("the timer loop did not panic");
    }

    #[tokio::test]
    async fn closing_the_door_ends_the_timer_loop() {
        let clock = ManualClock::default();
        let door = door(clock.clone(), Some(UNBOUNDED_GRANT));
        let timers = tokio::spawn(run_timers(Arc::clone(&door), clock));

        door.close();

        timeout(WAIT_LIMIT, timers)
            .await
            .expect("the timer loop ends once the door closes")
            .expect("the timer loop did not panic");
    }
}
