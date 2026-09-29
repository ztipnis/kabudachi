//! Who has to be woken when the scheduler changes, and the one place that
//! decides it.

use std::sync::Arc;

use kabudachi_core::protocol::ids::Uuid7Ids;
use kabudachi_core::scheduler::Scheduler;
use kabudachi_core::time::RealClock;
use tokio::sync::Notify;

use crate::work::{SharedScheduler, Wake, lock_scheduler};

/// Everyone who waits on what the scheduler does: claims waiting for work,
/// waits for events, and the timer loop.
///
/// Whoever changes the scheduler goes through [`Wakeups::with_scheduler`]
/// rather than picking which of them to wake, because the two mistakes do not
/// cost the same: waking one that had nothing to do costs it a loop
/// iteration, while missing one that did leaves a caller waiting for ever.
/// Every wake-up here is idempotent, so waking too often is safe.
#[derive(Debug, Clone)]
pub struct Wakeups {
    claims: Wake,
    events: Wake,
    /// `None` inside the timer loop itself, which is what this would notify:
    /// a notification it sent itself would spin it.
    timers: Option<Arc<Notify>>,
}

impl Wakeups {
    pub fn new(claims: Wake, events: Wake, timers: Arc<Notify>) -> Self {
        Wakeups {
            claims,
            events,
            timers: Some(timers),
        }
    }

    /// The same wake-ups as seen from inside the timer loop, which its own
    /// deadlines wake rather than a notification.
    pub fn within_the_timer_loop(claims: Wake, events: Wake) -> Self {
        Wakeups {
            claims,
            events,
            timers: None,
        }
    }

    /// Wakes claims that are waiting for work.
    pub fn claims(&self) -> &Wake {
        &self.claims
    }

    /// Wakes waits for events.
    pub fn events(&self) -> &Wake {
        &self.events
    }

    /// Runs `change` on the scheduler under its lock, lets time take effect,
    /// wakes everyone the change can concern, and returns what `change`
    /// returned.
    ///
    /// Time takes effect after the change, so a task is never forgotten from
    /// under the operation that is working on it, and what the scheduler
    /// decided is read under the same lock, so an event produced here cannot
    /// be missed by the wake-up that follows it.
    pub fn with_scheduler<T>(
        &self,
        scheduler: &SharedScheduler,
        change: impl FnOnce(&mut Scheduler<RealClock, Uuid7Ids>) -> T,
    ) -> T {
        let (outcome, has_events) = {
            let mut scheduler = lock_scheduler(scheduler);
            let outcome = change(&mut scheduler);
            scheduler.sweep();
            (outcome, scheduler.has_events())
        };
        self.notify(has_events);
        outcome
    }

    /// Wakes claims and the timer loop, and the wait for events when the
    /// scheduler has decided something for Python to act on.
    fn notify(&self, has_events: bool) {
        self.claims.notify();
        if let Some(timers) = &self.timers {
            timers.notify_one();
        }
        if has_events {
            self.events.notify();
        }
    }

    /// The runtime is shutting down: claims and waits for events give up.
    pub fn close(&self) {
        self.claims.close();
        self.events.close();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskRunId, Uuid7Ids, WorkerId};
    use kabudachi_core::scheduler::{Event, MemoryLimits, Scheduler, Submission};
    use tokio::runtime::Builder;
    use tokio::sync::Notify;

    use super::*;
    use crate::work::tests::UNBOUNDED_GRANT;
    use crate::work::{SharedScheduler, Wake, lock_scheduler};

    const SOFT_LIMIT: u64 = 100;
    const HARD_LIMIT: u64 = 1_000;
    /// Past the soft limit on its own, so one task raises `SlowDown`.
    const PAYLOAD_BYTES: usize = 150;
    const DIGEST: &[u8] = b"digest-of-the-result";
    /// Long enough that a notification that was sent is never missed, short
    /// enough that one that was not does not hang the suite for ever.
    const NOTIFY_LIMIT: Duration = Duration::from_secs(5);

    fn worker() -> WorkerId {
        WorkerId::new("worker-1")
    }

    fn timers() -> Arc<Notify> {
        Arc::new(Notify::new())
    }

    fn wakeups(timers: &Arc<Notify>) -> Wakeups {
        Wakeups::new(Wake::new(), Wake::new(), Arc::clone(timers))
    }

    fn submission() -> Submission {
        Submission::new(
            TaskDefinitionId::new("bulk.load"),
            0,
            vec![0; PAYLOAD_BYTES],
            "default",
        )
    }

    fn leading_scheduler() -> SharedScheduler {
        let mut scheduler = Scheduler::new(RealClock::new(), Uuid7Ids);
        scheduler.set_leadership_grant(Some(UNBOUNDED_GRANT));
        Arc::new(Mutex::new(scheduler))
    }

    /// A leading scheduler with one running task whose payload is past the
    /// soft limit, so `SlowDown` is raised, and that run's ID. The raised
    /// event is taken, so only what happens next is left to see.
    fn running_over_the_soft_limit() -> (SharedScheduler, TaskRunId) {
        let mut scheduler = Scheduler::new(RealClock::new(), Uuid7Ids);
        scheduler.set_memory_limits(Some(MemoryLimits {
            soft: SOFT_LIMIT,
            hard: HARD_LIMIT,
        }));
        scheduler.set_leadership_grant(Some(UNBOUNDED_GRANT));
        scheduler
            .submit(submission())
            .expect("the payload is under the hard limit");
        let claims = scheduler
            .claim_oldest(&worker(), 1)
            .expect("a leader claims from its own queue");
        let run_id = claims[0].task_run_id.clone();
        scheduler
            .report_started(&worker(), &run_id)
            .expect("a claimed run can start");
        assert!(
            scheduler.slow_down_active(),
            "the payload left SlowDown clear"
        );
        scheduler.take_events();
        (Arc::new(Mutex::new(scheduler)), run_id)
    }

    /// Whether `notify` was told to wake someone.
    fn woken(notify: &Notify) -> bool {
        Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("a current-thread runtime builds")
            .block_on(async {
                tokio::time::timeout(NOTIFY_LIMIT, notify.notified())
                    .await
                    .is_ok()
            })
    }

    #[test]
    fn a_completion_that_clears_slow_down_wakes_the_wait_for_events() {
        let (scheduler, run_id) = running_over_the_soft_limit();
        let wakeups = wakeups(&timers());
        let mut events = wakeups.events().subscribe();
        events.borrow_and_update();

        let certified = wakeups.with_scheduler(&scheduler, |scheduler| {
            scheduler.complete(&worker(), &run_id, DIGEST.to_vec())
        });

        assert_eq!(
            certified.expect("the running run completes").task_run_id,
            run_id
        );
        assert!(
            events.has_changed().expect("the wake outlives the test"),
            "nothing woke the wait for events, so a cleared SlowDown arrives \
             only if the timer loop happens to look"
        );
        assert_eq!(
            lock_scheduler(&scheduler).take_events(),
            vec![Event::SlowDown { active: false }]
        );
    }

    #[test]
    fn every_change_wakes_waiting_claims_and_the_timer_loop() {
        let scheduler = leading_scheduler();
        let timers = timers();
        let wakeups = wakeups(&timers);
        let mut claims = wakeups.claims().subscribe();
        claims.borrow_and_update();

        wakeups.with_scheduler(&scheduler, |scheduler| {
            scheduler.submit(submission()).expect("no limits are set")
        });

        assert!(claims.has_changed().expect("the wake outlives the test"));
        assert!(woken(&timers));
    }
}
