//! The bindings' one way into the shared scheduler: the mutex around it, the
//! wake-ups that follow each change, the flag that says the runtime has shut
//! down, and the worker every claim is made as. Nothing else in the crate
//! locks the scheduler.
//!
//! Every operation runs whole under the scheduler's lock, checks the closed
//! flag under that same lock, and wakes whoever the change concerns before
//! returning. The scheduler here carries no observer (`NoObserver`): the
//! bindings prove leadership through operations, never through a spy.

use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};

use kabudachi_core::election::{DropMessages, NoAuthority, Step, WorkerNode, carry_out};
use kabudachi_core::protocol::ids::{TaskId, TaskRunId, Uuid7Ids, WorkerId};
use kabudachi_core::protocol::records::TaskRunRecord;
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::scheduler::{
    CancelRejection, Cancellation, Certification, Claim, ClaimRejection, Completion, Event,
    Failure, ReportRejection, Scheduler, Submission, SubmitRejection,
};
use kabudachi_core::time::{Clock, Instant};
use tokio::sync::{Notify, watch};

/// The runtime has shut down: the door refuses everything from then on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Closed;

/// Why the door refused an operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal<R> {
    Closed,
    Rejected(R),
}

/// What waiting claims watch. Any update to it wakes them, changed or not, so
/// it only has to say whether the runtime is shutting down. Shutting down is
/// part of the value, so a waiter that reads it cannot miss it whatever it was
/// doing when the shutdown happened.
#[derive(Debug, Clone, Copy, Default)]
struct WakeState {
    closed: bool,
}

/// Wakes claims, or waits for events, that are waiting for something.
#[derive(Debug, Clone)]
struct Wake(Arc<watch::Sender<WakeState>>);

impl Wake {
    fn new() -> Self {
        Wake(Arc::new(watch::channel(WakeState::default()).0))
    }

    /// Something happened that may give a waiter something to do.
    fn notify(&self) {
        self.0.send_modify(|_| {});
    }

    /// The runtime is shutting down: waiters give up.
    fn close(&self) {
        self.0.send_modify(|state| state.closed = true);
    }

    /// A receiver that sees every later wake-up, and the shutdown.
    fn subscribe(&self) -> watch::Receiver<WakeState> {
        self.0.subscribe()
    }
}

/// Everyone who waits on what the scheduler does: claims waiting for work,
/// waits for events, and the timer loop.
///
/// Every change goes through the door rather than picking which of them to
/// wake, because the two mistakes do not cost the same: waking one that had
/// nothing to do costs it a loop iteration, while missing one that did leaves
/// a caller waiting for ever. Every wake-up here is idempotent, so waking too
/// often is safe.
struct Wakeups {
    claims: Wake,
    events: Wake,
    /// Keeps a permit, so a change that happens while the timer loop is
    /// working is not lost.
    timers: Notify,
}

/// Whom a change concerns, besides the wait for events (which is woken
/// whenever the scheduler has something to say).
#[derive(Debug, Clone, Copy)]
enum Concerned {
    /// A change that may give a waiting claim work or move the next deadline.
    ClaimsAndTimers,
    /// The timer loop's own tick: it wakes claims, never itself, or it would spin.
    Claims,
    /// A claim, which only takes work: waking claims would make a waiting
    /// claim spin.
    Nobody,
}

struct Inside<C: Clock> {
    scheduler: Scheduler<C, Uuid7Ids>,
    closed: bool,
}

/// The bindings' one way into the shared scheduler.
pub struct SchedulerDoor<C: Clock> {
    inside: Mutex<Inside<C>>,
    wakeups: Wakeups,
    worker: WorkerId,
}

impl<C: Clock> SchedulerDoor<C> {
    /// Takes only an unobserved scheduler (`Scheduler::new`).
    pub fn new(scheduler: Scheduler<C, Uuid7Ids>, worker: WorkerId) -> Self {
        SchedulerDoor {
            inside: Mutex::new(Inside {
                scheduler,
                closed: false,
            }),
            wakeups: Wakeups {
                claims: Wake::new(),
                events: Wake::new(),
                timers: Notify::new(),
            },
            worker,
        }
    }

    pub fn submit(&self, submission: Submission) -> Result<TaskId, Refusal<SubmitRejection>> {
        refuse(self.change(Concerned::ClaimsAndTimers, |scheduler| {
            scheduler.submit(submission)
        }))
    }

    pub fn report_started(&self, run: &TaskRunId) -> Result<(), Refusal<ReportRejection>> {
        refuse(self.change(Concerned::ClaimsAndTimers, |scheduler| {
            scheduler.report_started(&self.worker, run)
        }))
    }

    pub fn complete(
        &self,
        run: &TaskRunId,
        result_digest: Vec<u8>,
        completion: Completion,
    ) -> Result<Certification, Refusal<ReportRejection>> {
        refuse(self.change(Concerned::ClaimsAndTimers, |scheduler| {
            scheduler.complete(&self.worker, run, result_digest, completion)
        }))
    }

    /// Reports that a run this worker claimed failed with an error of type
    /// `failure_kind` (the type's name, never its message), whether or not its
    /// body started. A run still `Claimed` is stepped through `Running`
    /// first, under the same lock, so no other caller sees it started but not
    /// failed.
    pub fn report_failure(
        &self,
        run: &TaskRunId,
        failure_kind: &str,
    ) -> Result<Failure, Refusal<ReportRejection>> {
        refuse(self.change(Concerned::ClaimsAndTimers, |scheduler| {
            if scheduler.task_run(run).map(TaskRunRecord::current_state)
                == Some(TaskRunState::Claimed)
            {
                scheduler.report_started(&self.worker, run)?;
            }
            scheduler.fail(&self.worker, run, failure_kind)
        }))
    }

    pub fn cancel(&self, task: &TaskId) -> Result<Cancellation, Refusal<CancelRejection>> {
        refuse(self.change(Concerned::ClaimsAndTimers, |scheduler| {
            scheduler.cancel(task)
        }))
    }

    pub fn end_continuation(&self, task: &TaskId) -> Result<bool, Closed> {
        self.change(Concerned::ClaimsAndTimers, |scheduler| {
            scheduler.end_continuation(task)
        })
    }

    pub fn run_state(&self, run: &TaskRunId) -> Result<Option<TaskRunState>, Closed> {
        self.read(|scheduler| {
            scheduler
                .task_run(run)
                .map(|run| run.current_state())
        })
    }

    pub fn run_ids(&self, task: &TaskId) -> Result<Vec<TaskRunId>, Closed> {
        self.read(|scheduler| scheduler.runs_of(task))
    }

    /// The timer loop's tick: `catch_up`, wake claims (never the timer loop
    /// itself), and return `next_deadline`.
    pub fn tick(&self) -> Result<Option<Instant>, Closed> {
        self.change(Concerned::Claims, |scheduler| {
            scheduler.catch_up();
            scheduler.next_deadline()
        })
    }

    /// Applies an election step of the lone local node: `election::carry_out`
    /// with `DropMessages` and `NoAuthority`, with `observe` seeing each step.
    pub fn carry_out(
        &self,
        node: &mut WorkerNode<C>,
        step: Step,
        mut observe: impl FnMut(&Step),
    ) -> Result<Option<Instant>, Closed> {
        self.change(Concerned::ClaimsAndTimers, |scheduler| {
            carry_out(
                node,
                step,
                scheduler,
                &mut DropMessages,
                &mut NoAuthority,
                |_, _, _, step| observe(step),
            )
        })
    }

    /// Resolves when a change may have moved the next deadline (keeps a permit).
    pub fn timers_changed(&self) -> impl Future<Output = ()> + '_ {
        self.wakeups.timers.notified()
    }

    /// Runs `change` on the scheduler under its lock, unless the door is
    /// closed, then wakes whoever `concerned` names, and the wait for events
    /// if the scheduler has something to say. What the scheduler decided is
    /// read under the same lock, so an event produced here cannot be missed
    /// by the wake-up that follows it.
    fn change<T>(
        &self,
        concerned: Concerned,
        change: impl FnOnce(&mut Scheduler<C, Uuid7Ids>) -> T,
    ) -> Result<T, Closed> {
        let (outcome, has_events) = {
            let mut inside = self.lock();
            if inside.closed {
                return Err(Closed);
            }
            let outcome = change(&mut inside.scheduler);
            (outcome, inside.scheduler.has_events())
        };
        match concerned {
            Concerned::ClaimsAndTimers => {
                self.wakeups.claims.notify();
                self.wakeups.timers.notify_one();
            }
            Concerned::Claims => self.wakeups.claims.notify(),
            Concerned::Nobody => {}
        }
        if has_events {
            self.wakeups.events.notify();
        }
        Ok(outcome)
    }

    /// Reads the scheduler under its lock, unless the door is closed.
    fn read<T>(&self, read: impl FnOnce(&Scheduler<C, Uuid7Ids>) -> T) -> Result<T, Closed> {
        let inside = self.lock();
        if inside.closed {
            return Err(Closed);
        }
        Ok(read(&inside.scheduler))
    }

    /// Locks the scheduler, which a panic in another holder does not make
    /// unusable.
    fn lock(&self) -> std::sync::MutexGuard<'_, Inside<C>> {
        self.inside.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Refuses every later operation, then fails waiting claims and event
    /// waits and wakes the timer loop so it ends.
    pub fn close(&self) {
        self.lock().closed = true;
        self.wakeups.claims.close();
        self.wakeups.events.close();
        self.wakeups.timers.notify_one();
    }
}

impl<C: Clock + Send + 'static> SchedulerDoor<C> {
    /// Subscribes to claim wake-ups now, then waits until there is a leader
    /// and work and claims up to `limit` as this door's worker. Fails once
    /// the door is closed.
    ///
    /// The wake state is read and marked as seen in one step, before looking
    /// for work: a submission or shutdown after that read makes `changed()`
    /// return at once, and one before it is visible in the value that was
    /// read.
    ///
    /// Claiming lets time take effect (a task past its expiry is not handed
    /// out), which can produce events, so the wait for events is woken when
    /// it did.
    pub fn claim_when_available(
        self: Arc<Self>,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<Claim>, Closed>> + Send + 'static {
        let mut wake = self.wakeups.claims.subscribe();
        async move {
            loop {
                if wake.borrow_and_update().closed {
                    return Err(Closed);
                }
                let claims = match self.change(Concerned::Nobody, |scheduler| {
                    scheduler.claim_oldest(&self.worker, limit)
                })? {
                    Ok(claims) => claims,
                    // Not leading yet is not an error: there is just nothing
                    // to hand out.
                    Err(ClaimRejection::NotLeader) => Vec::new(),
                    // A rejection this loop does not know about is not "no
                    // work": it must not be silently waited out.
                    Err(
                        ClaimRejection::TaskUnknown
                        | ClaimRejection::NotReady
                        | ClaimRejection::AlreadySelected
                        | ClaimRejection::Finished
                        | ClaimRejection::Superseded
                        | ClaimRejection::KeyBusy,
                    ) => {
                        unreachable!("claim_oldest claims from its own queue as leader")
                    }
                };
                if !claims.is_empty() {
                    return Ok(claims);
                }
                // The sender lives as long as the door, so this only fails if
                // the door is gone, which is the same as being closed.
                wake.changed().await.map_err(|_| Closed)?;
            }
        }
    }

    /// Waits until the scheduler has decided something on its own and returns
    /// everything it has, oldest first. Fails once the door is closed. Reads
    /// the wake state before looking, like [`Self::claim_when_available`].
    pub fn events_when_available(
        self: Arc<Self>,
    ) -> impl Future<Output = Result<Vec<Event>, Closed>> + Send + 'static {
        let mut wake = self.wakeups.events.subscribe();
        async move {
            loop {
                if wake.borrow_and_update().closed {
                    return Err(Closed);
                }
                let events = self.change(Concerned::Nobody, Scheduler::take_events)?;
                if !events.is_empty() {
                    return Ok(events);
                }
                wake.changed().await.map_err(|_| Closed)?;
            }
        }
    }
}

/// Folds the door's two ways of refusing into one error.
fn refuse<T, R>(outcome: Result<Result<T, R>, Closed>) -> Result<T, Refusal<R>> {
    match outcome {
        Err(Closed) => Err(Refusal::Closed),
        Ok(Err(rejection)) => Err(Refusal::Rejected(rejection)),
        Ok(Ok(value)) => Ok(value),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use kabudachi_core::election::Output;
    use kabudachi_core::protocol::ids::{IncarnationId, ShardId, TaskDefinitionId};
    use kabudachi_core::protocol::messages::prelude::*;
    use kabudachi_core::scheduler::{LeadershipGrant, LeaseEnd, MemoryLimits};
    use kabudachi_core::time::Duration as CoreDuration;
    use tokio::runtime::Builder;

    use super::*;
    use crate::local_node::local_node;

    const LIMIT: Duration = Duration::from_secs(5);
    const DIGEST: &[u8] = b"digest-of-the-result";
    /// Lets a scheduler lead for as long as a test runs. Shared with the
    /// other test modules of this crate that need a leading scheduler.
    pub(crate) const UNBOUNDED_GRANT: LeadershipGrant = LeadershipGrant {
        term: 1,
        recovery_epoch: 0,
        valid_until: LeaseEnd::Unbounded,
    };

    /// A clock the test moves by hand. Unlike core's single-threaded
    /// `FakeClock`, it can be shared with the door and the timer loop across
    /// threads.
    #[derive(Debug, Clone, Default)]
    pub(crate) struct ManualClock(Arc<AtomicU64>);

    impl ManualClock {
        pub(crate) fn advance(&self, ticks: u64) {
            self.0.fetch_add(ticks, Ordering::SeqCst);
        }
    }

    impl Clock for ManualClock {
        fn now(&self) -> Instant {
            Instant::at(self.0.load(Ordering::SeqCst))
        }

        fn wall_clock_millis(&self) -> u64 {
            0
        }
    }

    fn worker() -> WorkerId {
        WorkerId::new("worker-1")
    }

    pub(crate) fn door(
        clock: ManualClock,
        grant: Option<LeadershipGrant>,
    ) -> Arc<SchedulerDoor<ManualClock>> {
        let mut scheduler = Scheduler::new(clock, Uuid7Ids);
        scheduler.set_leadership_grant(grant);
        Arc::new(SchedulerDoor::new(scheduler, worker()))
    }

    fn node(clock: ManualClock) -> WorkerNode<ManualClock> {
        local_node(
            worker(),
            IncarnationId::new("incarnation-1"),
            ShardId::new("local"),
            clock,
            CoreDuration::from_millis(0),
        )
        .0
    }

    /// Hands the door the grant an election step of a won election carries.
    fn lead(door: &SchedulerDoor<ManualClock>, node: &mut WorkerNode<ManualClock>) {
        let step = Step {
            outputs: vec![Output::Grant(Some(UNBOUNDED_GRANT))],
            next_deadline: None,
        };
        door.carry_out(node, step, |_| {})
            .expect("the door is open");
    }

    pub(crate) fn submission() -> Submission {
        Submission::new(
            TaskDefinitionId::new("definition"),
            0,
            Vec::new(),
            "default",
        )
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
            .block_on(async {
                tokio::time::timeout(LIMIT, future)
                    .await
                    .expect("timed out")
            })
    }

    #[test]
    fn every_operation_after_close_is_refused() {
        let clock = ManualClock::default();
        let door = door(clock.clone(), Some(UNBOUNDED_GRANT));
        let mut node = node(clock);
        door.close();
        let run = TaskRunId::new("some-run");
        let task = TaskId::new("some-task");

        assert_eq!(door.submit(submission()), Err(Refusal::Closed));
        assert_eq!(door.report_started(&run), Err(Refusal::Closed));
        assert_eq!(
            door.complete(&run, DIGEST.to_vec(), Completion::Final),
            Err(Refusal::Closed)
        );
        assert_eq!(door.report_failure(&run, "ValueError"), Err(Refusal::Closed));
        assert_eq!(door.cancel(&task), Err(Refusal::Closed));
        assert_eq!(door.end_continuation(&task), Err(Closed));
        assert_eq!(door.run_state(&run), Err(Closed));
        assert_eq!(door.run_ids(&task), Err(Closed));
        assert_eq!(door.tick(), Err(Closed));
        let step = Step {
            outputs: Vec::new(),
            next_deadline: None,
        };
        assert_eq!(door.carry_out(&mut node, step, |_| {}), Err(Closed));
        assert_eq!(
            block_on(Arc::clone(&door).claim_when_available(1)),
            Err(Closed)
        );
        assert_eq!(block_on(Arc::clone(&door).events_when_available()), Err(Closed));
    }

    #[test]
    fn closing_fails_a_claim_that_is_waiting() {
        let door = door(ManualClock::default(), Some(UNBOUNDED_GRANT));

        let result = block_on(async {
            let waiting = tokio::spawn(Arc::clone(&door).claim_when_available(10));
            tokio::time::sleep(Duration::from_millis(20)).await;
            door.close();
            waiting.await.unwrap()
        });

        assert_eq!(result, Err(Closed));
    }

    #[test]
    fn a_submission_before_the_first_wait_is_not_missed() {
        let clock = ManualClock::default();
        let door = door(clock.clone(), None);
        let mut node = node(clock);

        let claims = block_on(async {
            let waiting = tokio::spawn(Arc::clone(&door).claim_when_available(10));
            // Becoming leader with work already queued, before the waiter has run.
            door.submit(submission()).unwrap();
            lead(&door, &mut node);
            waiting.await.unwrap().unwrap()
        });

        assert_eq!(claims.len(), 1);
    }

    // Two claims wait while the worker does not lead, with two tasks queued.
    // The grant reaches both in one wake-up, and each must claim one: a
    // second waiter left asleep with work queued would never be woken again.
    #[test]
    fn one_wake_up_hands_queued_work_to_every_waiting_claim() {
        let clock = ManualClock::default();
        let door = door(clock.clone(), None);
        let mut node = node(clock);
        let queued = [
            door.submit(submission()).unwrap(),
            door.submit(submission()).unwrap(),
        ];

        let (first, second) = block_on(async {
            let first = tokio::spawn(Arc::clone(&door).claim_when_available(1));
            let second = tokio::spawn(Arc::clone(&door).claim_when_available(1));
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert!(
                !first.is_finished() && !second.is_finished(),
                "a claim took work before the worker led"
            );
            lead(&door, &mut node);
            (
                first.await.unwrap().unwrap(),
                second.await.unwrap().unwrap(),
            )
        });

        let mut claimed: Vec<TaskId> = first
            .iter()
            .chain(&second)
            .map(|claim| claim.task.task_id())
            .collect();
        claimed.sort();
        let mut queued = queued.to_vec();
        queued.sort();
        assert_eq!(claimed, queued, "each waiting claim took one queued task");
    }

    /// A door over a scheduler that leads and holds one running task whose
    /// payload is past the soft limit, so `SlowDown` is raised, and that
    /// run's ID. The raised event is taken, so only what happens next is left
    /// to see. The setup goes through a bare scheduler because the door has
    /// no synchronous claim.
    fn running_over_the_soft_limit() -> (Arc<SchedulerDoor<ManualClock>>, TaskRunId) {
        let mut scheduler = Scheduler::new(ManualClock::default(), Uuid7Ids);
        scheduler.set_memory_limits(Some(MemoryLimits {
            soft: 100,
            hard: 1_000,
        }));
        scheduler.set_leadership_grant(Some(UNBOUNDED_GRANT));
        scheduler
            .submit(Submission::new(
                TaskDefinitionId::new("bulk.load"),
                0,
                vec![0; 150],
                "default",
            ))
            .expect("the payload is under the hard limit");
        let claims = scheduler
            .claim_oldest(&worker(), 1)
            .expect("a leader claims from its own queue");
        let run_id = claims[0].task_run_id.clone();
        scheduler
            .report_started(&worker(), &run_id)
            .expect("a claimed run can start");
        assert_eq!(
            scheduler.take_events(),
            vec![Event::SlowDown { active: true }],
            "the payload left SlowDown clear"
        );
        (Arc::new(SchedulerDoor::new(scheduler, worker())), run_id)
    }

    #[test]
    fn a_completion_that_clears_slow_down_wakes_the_wait_for_events() {
        let (door, run_id) = running_over_the_soft_limit();
        let mut events = door.wakeups.events.subscribe();
        events.borrow_and_update();

        let certified = door
            .complete(&run_id, DIGEST.to_vec(), Completion::Final)
            .expect("the running run completes");

        assert_eq!(certified.task_run_id, run_id);
        assert!(
            events.has_changed().expect("the wake outlives the test"),
            "nothing woke the wait for events, so a cleared SlowDown arrives \
             only if the timer loop happens to look"
        );
        assert_eq!(
            block_on(Arc::clone(&door).events_when_available()),
            Ok(vec![Event::SlowDown { active: false }])
        );
    }

    #[test]
    fn every_change_wakes_waiting_claims_and_the_timer_loop() {
        let door = door(ManualClock::default(), Some(UNBOUNDED_GRANT));
        let mut claims = door.wakeups.claims.subscribe();
        claims.borrow_and_update();

        door.submit(submission()).expect("no limits are set");

        assert!(claims.has_changed().expect("the wake outlives the test"));
        block_on(door.timers_changed());
    }

    #[test]
    fn a_claim_that_expires_a_task_wakes_the_wait_for_events_but_no_claim() {
        let clock = ManualClock::default();
        let door = door(clock.clone(), Some(UNBOUNDED_GRANT));
        let expiring = door
            .submit(submission().with_expiry(CoreDuration::from_ticks(10)))
            .unwrap();
        let other = door.submit(submission()).unwrap();
        let mut claims = door.wakeups.claims.subscribe();
        let mut events = door.wakeups.events.subscribe();
        claims.borrow_and_update();
        events.borrow_and_update();
        clock.advance(10);

        let claimed = block_on(Arc::clone(&door).claim_when_available(1)).unwrap();

        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].task.task_id(), other);
        assert!(
            events.has_changed().expect("the wake outlives the test"),
            "the claim expired a task but did not wake the wait for events"
        );
        assert!(
            !claims.has_changed().expect("the wake outlives the test"),
            "a claim woke waiting claims, so a waiting claim would spin"
        );
        let events = block_on(Arc::clone(&door).events_when_available()).unwrap();
        assert!(
            matches!(events.as_slice(), [Event::Expired { task_id, .. }] if *task_id == expiring),
            "{events:?}"
        );
    }

    #[test]
    fn a_tick_wakes_claims_but_not_the_timer_loop() {
        let clock = ManualClock::default();
        let door = door(clock.clone(), Some(UNBOUNDED_GRANT));
        door.submit(submission().with_delay(CoreDuration::from_ticks(5)))
            .unwrap();
        // Drain the permit the submission left for the timer loop.
        block_on(async {
            tokio::time::timeout(Duration::ZERO, door.timers_changed())
                .await
                .expect("the submission woke the timer loop");
        });
        let mut claims = door.wakeups.claims.subscribe();
        claims.borrow_and_update();
        clock.advance(5);

        let deadline = door.tick();

        // No TTL is set, nothing else waits, and an unbounded grant adds no
        // lease end: the timer loop has nothing to wake for.
        assert_eq!(deadline, Ok(None));
        assert!(claims.has_changed().expect("the wake outlives the test"));
        let woken = block_on(async {
            tokio::time::timeout(Duration::ZERO, door.timers_changed())
                .await
                .is_ok()
        });
        assert!(!woken, "the tick woke the timer loop, which would spin it");
    }
}
