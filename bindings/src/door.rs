//! The bindings' one way into the shared scheduler: the mutex around it, the
//! wake-ups that follow each change, the flag that says the runtime has shut
//! down, and the worker every claim is made as. Nothing else in the crate
//! locks the scheduler.
//!
//! Every operation runs whole under the scheduler's lock, checks the closed
//! flag under that same lock, and wakes whoever the change concerns before
//! returning. The scheduler here carries the one-node record sink
//! (`LocalRecords`): each revision it publishes is stored in this node's own
//! store before the call returns. An answer that tells a caller something was
//! decided is held in an effect gate until the store has settled every write
//! the call made, and is a retryable `NotLeader` if the store refused one or
//! the lease had ended. A submission made before the node leads gets its task
//! id at once and waits in the door; an end of a continuation the scheduler
//! refused for want of leadership waits there too. Once the node leads, every
//! change except a claim or a read replays the unended continuations, then records the
//! queued submissions in the order they were made.

use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};

use kabudachi_core::election::{DropMessages, NoAuthority, Step, WorkerNode, carry_out};
use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::ids::{TaskId, TaskRunId, Uuid7Ids, WorkerId};
use kabudachi_core::protocol::records::TaskRunRecord;
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::protocol::generated::TaskRecord;
use kabudachi_core::scheduler::{
    CancelRejection, Cancellation, Certification, Claim, ClaimRejection, Completion,
    ContinuationRejection, Event, Failure, ReportRejection, Scheduler, Submission, Submitted,
    SubmitRejection,
};
use kabudachi_core::task_record::{EffectGate, LocalRecords, Settled};
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

/// The scheduler the door guards: it keeps its own records.
pub type DoorScheduler<C> = Scheduler<C, Uuid7Ids, LocalRecords<C>>;

struct Inside<C: Clock> {
    scheduler: DoorScheduler<C>,
    closed: bool,
    /// Submissions made before this node led, in the order they were made.
    queued: Vec<Submitted>,
    /// The input bytes `queued` holds, which each new submission is checked
    /// against the hard limit with.
    queued_bytes: u64,
    /// Continuations whose end the scheduler refused for want of leadership,
    /// in the order they were refused: the grant ends them.
    unended: Vec<TaskId>,
}

/// A rejection that can say "this node is not the leader".
trait NotLeaderRejection {
    const NOT_LEADER: Self;
}

macro_rules! not_leader {
    ($($rejection:ty),*) => {
        $(impl NotLeaderRejection for $rejection {
            const NOT_LEADER: Self = Self::NotLeader;
        })*
    };
}

not_leader!(
    SubmitRejection,
    ClaimRejection,
    ReportRejection,
    CancelRejection,
    ContinuationRejection
);

/// Runs `change`, then holds its outcome in an effect gate until the local
/// store has settled every write the change made. The store settles them
/// before `put` returns, so the outcome is decided before this returns: it
/// stands if every write was kept and the scheduler still leads, and is
/// `NotLeader` otherwise: the answer is `NotLeader` whenever the scheduler no
/// longer leads after the call, whether or not its writes were published.
fn gated<C: Clock, T, R: NotLeaderRejection>(
    scheduler: &mut DoorScheduler<C>,
    change: impl FnOnce(&mut DoorScheduler<C>) -> Result<T, R>,
) -> Result<T, R> {
    // Writes left by an operation nobody waits on are not this call's.
    drop(scheduler.observer_mut().take_settled());
    let outcome = change(scheduler);
    let settled = scheduler.observer_mut().take_settled();
    let outcome = outcome?;
    if !scheduler.is_leader() {
        return Err(R::NOT_LEADER);
    }
    let mut gate = EffectGate::new();
    let mut ended = gate.hold(outcome, settled.iter().map(|(write, _)| write.clone()));
    for (write, kept) in settled {
        let more = if kept {
            gate.acknowledged(&write, scheduler.is_leader())
        } else {
            gate.refused(&write).into_iter().map(Settled::NotLeader).collect()
        };
        ended = ended.or(more.into_iter().next());
    }
    match ended.expect("the local store settles every write before the call returns") {
        Settled::Released(outcome) => Ok(outcome),
        Settled::NotLeader(_) => Err(R::NOT_LEADER),
    }
}

/// Records the submissions queued before the grant, in order. One refused
/// outright (the limits fell since it was queued, or the lease ended) and
/// everything behind it stay queued, in order, rather than being lost. One the
/// scheduler recorded but whose write the store refused stays recorded:
/// recording it again would overwrite the task and its runs.
fn record_queued<C: Clock>(inside: &mut Inside<C>) {
    let queued = std::mem::take(&mut inside.queued);
    let mut waiting = queued.into_iter();
    while let Some(submitted) = waiting.next() {
        let kept = submitted.clone();
        let recorded = gated(&mut inside.scheduler, |scheduler| scheduler.submit_minted(submitted));
        if recorded.is_err() && inside.scheduler.runs_of(&kept.task_id).is_empty() {
            inside.queued.push(kept);
            inside.queued.extend(waiting);
            break;
        }
    }
    inside.queued_bytes = inside
        .queued
        .iter()
        .map(|submitted| submitted.submission.serialized_input.len() as u64)
        .sum();
}

/// Once the scheduler leads, ends the continuations it refused to end earlier
/// (which frees the memory they held), then records the submissions queued
/// before the grant, in order. Run after every change that can free capacity
/// or keys, once the change's own effects have settled, so a refused
/// submission is not left waiting for the next one.
fn settle_pending<C: Clock>(inside: &mut Inside<C>) {
    if !inside.scheduler.is_leader() {
        return;
    }
    let unended = std::mem::take(&mut inside.unended);
    let mut waiting = unended.into_iter();
    while let Some(task) = waiting.next() {
        if gated(&mut inside.scheduler, |scheduler| scheduler.end_continuation(&task)).is_err() {
            inside.unended.push(task);
            inside.unended.extend(waiting);
            break;
        }
    }
    record_queued(inside);
}

/// The bindings' one way into the shared scheduler.
pub struct SchedulerDoor<C: Clock> {
    inside: Mutex<Inside<C>>,
    wakeups: Wakeups,
    worker: WorkerId,
}

impl<C: Clock> SchedulerDoor<C> {
    pub fn new(scheduler: DoorScheduler<C>, worker: WorkerId) -> Self {
        SchedulerDoor {
            inside: Mutex::new(Inside {
                scheduler,
                closed: false,
                queued: Vec::new(),
                queued_bytes: 0,
                unended: Vec::new(),
            }),
            wakeups: Wakeups {
                claims: Wake::new(),
                events: Wake::new(),
                timers: Notify::new(),
            },
            worker,
        }
    }

    /// Gives `submission` its task id at once. A leader records it now; before
    /// the grant, or while earlier submissions are still queued behind a
    /// refusal, it is checked against the hard limit with everything queued
    /// before it, queued, and recorded, in order, by the first change that
    /// finds room once this node leads.
    pub fn submit(&self, submission: Submission) -> Result<TaskId, Refusal<SubmitRejection>> {
        refuse(self.change_inside(Concerned::ClaimsAndTimers, |inside| {
            let submitted = inside.scheduler.mint(submission);
            if inside.scheduler.is_leader() {
                // The lone leader is not woken by an election step again, so
                // capacity freed since a refusal is used by the next submission.
                record_queued(inside);
            }
            if inside.scheduler.is_leader() && inside.queued.is_empty() {
                return gated(&mut inside.scheduler, |scheduler| scheduler.submit_minted(submitted));
            }
            inside
                .scheduler
                .check_submission(&submitted.submission, inside.queued_bytes)?;
            inside.queued_bytes += submitted.submission.serialized_input.len() as u64;
            let task = submitted.task_id.clone();
            inside.queued.push(submitted);
            Ok(task)
        }))
    }

    pub fn report_started(&self, run: &TaskRunId) -> Result<(), Refusal<ReportRejection>> {
        refuse(self.change(Concerned::ClaimsAndTimers, |scheduler| {
            gated(scheduler, |scheduler| scheduler.report_started(&self.worker, run))
        }))
    }

    pub fn complete(
        &self,
        run: &TaskRunId,
        result_digest: Digest,
        completion: Completion,
    ) -> Result<Certification, Refusal<ReportRejection>> {
        refuse(self.change(Concerned::ClaimsAndTimers, |scheduler| {
            gated(scheduler, |scheduler| {
                scheduler.complete(&self.worker, run, result_digest, completion)
            })
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
            gated(scheduler, |scheduler| {
                if scheduler.task_run(run).map(TaskRunRecord::current_state)
                    == Some(TaskRunState::Claimed)
                {
                    scheduler.report_started(&self.worker, run)?;
                }
                scheduler.fail(&self.worker, run, failure_kind)
            })
        }))
    }

    /// Cancels `task`. A submission still queued for the grant is dropped
    /// from the queue, so it never runs.
    pub fn cancel(&self, task: &TaskId) -> Result<Cancellation, Refusal<CancelRejection>> {
        refuse(self.change_inside(Concerned::ClaimsAndTimers, |inside| {
            if let Some(at) = inside.queued.iter().position(|queued| queued.task_id == *task) {
                let dropped = inside.queued.remove(at);
                inside.queued_bytes -= dropped.submission.serialized_input.len() as u64;
                return Ok(Cancellation::Cancelled { was_running: false });
            }
            gated(&mut inside.scheduler, |scheduler| scheduler.cancel(task))
        }))
    }

    /// Ends the continuation of `task`. Refused for want of leadership, it
    /// is kept and ended by the first change once this node leads, so the task
    /// is not left continuing for ever; the caller is still told it was
    /// refused.
    pub fn end_continuation(
        &self,
        task: &TaskId,
    ) -> Result<bool, Refusal<ContinuationRejection>> {
        refuse(self.change_inside(Concerned::ClaimsAndTimers, |inside| {
            let ended = gated(&mut inside.scheduler, |scheduler| scheduler.end_continuation(task));
            if ended.is_err() && !inside.unended.contains(task) {
                inside.unended.push(task.clone());
            }
            ended
        }))
    }

    pub fn run_state(&self, run: &TaskRunId) -> Result<Option<TaskRunState>, Closed> {
        self.read(|scheduler| {
            scheduler
                .task_run(run)
                .map(|run| run.current_state())
        })
    }

    /// The newest record of `task` this node holds.
    #[allow(dead_code, reason = "no Python call reads a record yet; the door tests do")]
    pub fn record(&self, task: &TaskId) -> Result<Option<TaskRecord>, Closed> {
        // `observer_mut` is the scheduler's only way to the store, and
        // reading it wakes nobody.
        self.change(Concerned::Nobody, |scheduler| {
            scheduler.observer_mut().records().get(task).cloned()
        })
    }

    pub fn run_ids(&self, task: &TaskId) -> Result<Vec<TaskRunId>, Closed> {
        self.read(|scheduler| scheduler.runs_of(task))
    }

    /// The timer loop's tick: `catch_up`, wake claims (never the timer loop
    /// itself), sweep the local store, and return the earlier of
    /// `next_deadline` and the store's `next_due`. The deadline is read after what
    /// the catch-up freed has been recorded, since a queued submission
    /// recorded then may carry a delay or expiry the loop must wake for; the
    /// loop is not woken for that, as it learns of it from this result.
    pub fn tick(&self) -> Result<Option<Instant>, Closed> {
        self.change_then(
            Concerned::Claims,
            |inside| {
                inside.scheduler.catch_up();
                // Nothing waits on what time made it publish, so a write the
                // store refuses here is deliberately not retried: no answer
                // depends on it.
                drop(inside.scheduler.observer_mut().take_settled());
                // The store drops a finished record at its retention; nothing
                // else would, since only a write sweeps it.
                inside.scheduler.observer_mut().sweep();
            },
            |inside, ()| {
                let stored = inside.scheduler.observer_mut().records().next_due();
                [inside.scheduler.next_deadline(), stored].into_iter().flatten().min()
            },
        )
    }

    /// Applies an election step of the lone local node: `election::carry_out`
    /// with `DropMessages` and `NoAuthority`, with `observe` seeing each step.
    pub fn carry_out(
        &self,
        node: &mut WorkerNode<C>,
        step: Step,
        mut observe: impl FnMut(&Step),
    ) -> Result<Option<Instant>, Closed> {
        self.change_inside(Concerned::ClaimsAndTimers, |inside| {
            let next = carry_out(
                node,
                step,
                &mut inside.scheduler,
                &mut DropMessages,
                &mut NoAuthority,
                |_, _, _, step| observe(step),
            );
            drop(inside.scheduler.observer_mut().take_settled());
            next
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
    /// by the wake-up that follows it. Any change but a claim or a read is followed,
    /// under the same lock and once this node leads, by ending the
    /// continuations refused earlier and recording the submissions queued for
    /// the grant, as capacity or keys the change freed may let those happen.
    fn change<T>(
        &self,
        concerned: Concerned,
        change: impl FnOnce(&mut DoorScheduler<C>) -> T,
    ) -> Result<T, Closed> {
        self.change_inside(concerned, |inside| change(&mut inside.scheduler))
    }

    /// [`Self::change`] for a change that also needs the submissions queued
    /// before the grant.
    fn change_inside<T>(
        &self,
        concerned: Concerned,
        change: impl FnOnce(&mut Inside<C>) -> T,
    ) -> Result<T, Closed> {
        self.change_then(concerned, change, |_, outcome| outcome)
    }

    /// [`Self::change_inside`] whose result is also read after what the
    /// change queued has settled, so it describes the scheduler as the caller
    /// leaves it.
    fn change_then<T, U>(
        &self,
        concerned: Concerned,
        change: impl FnOnce(&mut Inside<C>) -> T,
        then: impl FnOnce(&mut Inside<C>, T) -> U,
    ) -> Result<U, Closed> {
        let (outcome, has_events) = {
            let mut inside = self.lock();
            if inside.closed {
                return Err(Closed);
            }
            let outcome = change(&mut inside);
            if !matches!(concerned, Concerned::Nobody) {
                settle_pending(&mut inside);
            }
            let outcome = then(&mut inside, outcome);
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
    fn read<T>(&self, read: impl FnOnce(&DoorScheduler<C>) -> T) -> Result<T, Closed> {
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
                    gated(scheduler, |scheduler| scheduler.claim_oldest(&self.worker, limit))
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
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::time::Duration;

    use kabudachi_core::election::Output;
    use kabudachi_core::protocol::ids::{IncarnationId, ShardId, TaskDefinitionId};
    use kabudachi_core::protocol::messages::prelude::*;
    use kabudachi_core::scheduler::{LeadershipGrant, LeaseEnd, MemoryLimits, Observer};
    use kabudachi_core::task_record::LocalRecords;
    use kabudachi_core::time::Duration as CoreDuration;
    use tokio::runtime::Builder;

    use super::*;
    use crate::local_node::local_node;

    const LIMIT: Duration = Duration::from_secs(5);
    fn digest() -> Digest {
        Digest::blake3(b"the-result")
    }
    /// Lets a scheduler lead for as long as a test runs. Shared with the
    /// other test modules of this crate that need a leading scheduler.
    pub(crate) const UNBOUNDED_GRANT: LeadershipGrant = grant(1);

    const fn grant(term: u64) -> LeadershipGrant {
        LeadershipGrant {
            term,
            recovery_epoch: kabudachi_core::coordination_authority::RecoveryEpoch::new(0, 0),
            valid_until: LeaseEnd::Unbounded,
        }
    }

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
        let mut scheduler = Scheduler::with_observer(clock.clone(), Uuid7Ids, LocalRecords::new(worker(), clock, None));
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

    /// Takes the grant away, as an election step that lost leadership does.
    fn withdraw(door: &SchedulerDoor<ManualClock>, node: &mut WorkerNode<ManualClock>) {
        let step = Step {
            outputs: vec![Output::Grant(None)],
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
            door.complete(&run, digest(), Completion::Final),
            Err(Refusal::Closed)
        );
        assert_eq!(door.report_failure(&run, "ValueError"), Err(Refusal::Closed));
        assert_eq!(door.cancel(&task), Err(Refusal::Closed));
        assert_eq!(door.end_continuation(&task), Err(Refusal::Closed));
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
            // Work queued before the waiter has run.
            lead(&door, &mut node);
            door.submit(submission()).unwrap();
            waiting.await.unwrap().unwrap()
        });

        assert_eq!(claims.len(), 1);
    }

    #[test]
    fn a_submission_before_the_grant_keeps_its_id_and_is_recorded_first_when_the_grant_arrives() {
        let clock = ManualClock::default();
        let door = door(clock.clone(), None);
        let mut node = node(clock);

        let task = door.submit(submission()).unwrap();
        assert_eq!(door.record(&task).unwrap(), None, "no leader has recorded it yet");

        lead(&door, &mut node);

        let record = door.record(&task).unwrap().expect("recorded when the grant arrived");
        assert_eq!(
            record.version.map(|version| (version.leader_term, version.revision)),
            Some((1, 0))
        );
        let claims = block_on(Arc::clone(&door).claim_when_available(1)).unwrap();
        assert_eq!(
            claims[0].task.task_id(),
            task,
            "the task claimed is the one the client was told of"
        );
    }

    #[test]
    fn a_submission_before_the_grant_is_refused_when_it_would_pass_the_hard_limit_with_those_queued()
     {
        let mut scheduler =
            {
                let clock = ManualClock::default();
                Scheduler::with_observer(clock.clone(), Uuid7Ids, LocalRecords::new(worker(), clock, None))
            };
        scheduler.set_memory_limits(Some(MemoryLimits { soft: 50, hard: 100 }));
        let door = SchedulerDoor::new(scheduler, worker());
        let sized = |bytes: usize| {
            Submission::new(TaskDefinitionId::new("definition"), 0, vec![0; bytes], "default")
        };

        door.submit(sized(60)).unwrap();

        assert!(matches!(
            door.submit(sized(60)),
            Err(Refusal::Rejected(SubmitRejection::Backpressure { .. }))
        ));
    }

    #[test]
    fn a_queued_submission_the_grant_cannot_record_is_kept_and_recorded_once_it_can() {
        let clock = ManualClock::default();
        let door = door(clock.clone(), None);
        let mut node = node(clock);
        let sized = |bytes: usize| {
            Submission::new(TaskDefinitionId::new("definition"), 0, vec![0; bytes], "default")
        };
        let first = door.submit(sized(60)).unwrap();
        let second = door.submit(sized(60)).unwrap();
        let third = door.submit(sized(1)).unwrap();
        // Limits lowered after the two were queued: only one fits.
        door.lock()
            .scheduler
            .set_memory_limits(Some(MemoryLimits { soft: 50, hard: 100 }));

        lead(&door, &mut node);

        assert!(door.record(&first).unwrap().is_some());
        assert_eq!(door.record(&second).unwrap(), None, "refused for now, not lost");
        assert_eq!(door.record(&third).unwrap(), None, "kept behind the refused one");

        door.lock().scheduler.set_memory_limits(None);
        let step = Step {
            outputs: Vec::new(),
            next_deadline: None,
        };
        door.carry_out(&mut node, step, |_| {}).unwrap();

        let revision = |task: &TaskId| {
            door.record(task).unwrap().and_then(|record| record.version).map(|v| v.revision)
        };
        assert_eq!(
            (revision(&first), revision(&second), revision(&third)),
            (Some(0), Some(1), Some(2))
        );
    }

    #[test]
    fn a_leaders_submission_waits_behind_earlier_ones_still_queued() {
        let clock = ManualClock::default();
        let door = door(clock.clone(), None);
        let mut node = node(clock);
        let sized = |bytes: usize| {
            Submission::new(TaskDefinitionId::new("definition"), 0, vec![0; bytes], "default")
        };
        let first = door.submit(sized(60)).unwrap();
        let second = door.submit(sized(60)).unwrap();
        door.lock()
            .scheduler
            .set_memory_limits(Some(MemoryLimits { soft: 50, hard: 100 }));
        lead(&door, &mut node);
        assert!(door.record(&first).unwrap().is_some());
        assert_eq!(door.record(&second).unwrap(), None, "refused for now, not lost");

        door.lock()
            .scheduler
            .set_memory_limits(Some(MemoryLimits { soft: 50, hard: 130 }));
        let late = door.submit(sized(1)).unwrap();

        let revision = |task: &TaskId| {
            door.record(task).unwrap().and_then(|record| record.version).map(|v| v.revision)
        };
        assert_eq!(
            (revision(&second), revision(&late)),
            (Some(1), Some(2)),
            "the queued one is recorded first, then the new one, without another election step"
        );
    }

    /// A door whose queued second submission the grant refused for want of
    /// room: returns the door, the first task and the refused second one.
    fn door_with_a_submission_refused_at_the_grant() -> (
        Arc<SchedulerDoor<ManualClock>>,
        TaskId,
        TaskId,
    ) {
        let clock = ManualClock::default();
        let door = door(clock.clone(), None);
        let mut node = node(clock);
        let sized = |bytes: usize| {
            Submission::new(TaskDefinitionId::new("definition"), 0, vec![0; bytes], "default")
        };
        let first = door.submit(sized(60)).unwrap();
        let second = door.submit(sized(60)).unwrap();
        door.lock()
            .scheduler
            .set_memory_limits(Some(MemoryLimits { soft: 50, hard: 100 }));
        lead(&door, &mut node);
        assert!(door.record(&first).unwrap().is_some());
        assert_eq!(door.record(&second).unwrap(), None, "refused at the grant");
        (door, first, second)
    }

    #[test]
    fn a_completion_that_frees_room_records_the_queued_submission_without_another_call() {
        let (door, first, second) = door_with_a_submission_refused_at_the_grant();
        let claims = block_on(Arc::clone(&door).claim_when_available(1)).unwrap();
        let run = claims[0].task_run_id.clone();
        assert_eq!(claims[0].task.task_id(), first);
        door.report_started(&run).unwrap();

        door.complete(&run, digest(), Completion::Final).unwrap();

        assert!(door.record(&second).unwrap().is_some());
    }

    #[test]
    fn an_end_of_continuation_replayed_at_the_grant_frees_room_for_a_queued_submission() {
        let clock = ManualClock::default();
        let door = door(clock.clone(), Some(UNBOUNDED_GRANT));
        let mut node = node(clock);
        let sized = |bytes: usize| {
            Submission::new(TaskDefinitionId::new("definition"), 0, vec![0; bytes], "default")
        };
        let first = door.submit(sized(60)).unwrap();
        let claims = block_on(Arc::clone(&door).claim_when_available(1)).unwrap();
        let run = claims[0].task_run_id.clone();
        door.report_started(&run).unwrap();
        door.complete(&run, digest(), Completion::Continues).unwrap();
        withdraw(&door, &mut node);
        assert!(door.end_continuation(&first).is_err());
        let second = door.submit(sized(60)).unwrap();
        door.lock()
            .scheduler
            .set_memory_limits(Some(MemoryLimits { soft: 50, hard: 100 }));

        lead(&door, &mut node);

        assert!(
            door.record(&second).unwrap().is_some(),
            "the continuation ended at the grant freed the room the queued submission needed"
        );
    }

    #[test]
    fn the_timer_loop_releases_a_delayed_submission_recorded_after_an_expiry_freed_room() {
        let clock = ManualClock::default();
        let door = door(clock.clone(), None);
        let mut node = node(clock.clone());
        let sized = |bytes: usize| {
            Submission::new(TaskDefinitionId::new("definition"), 0, vec![0; bytes], "default")
        };
        door.submit(sized(60).with_expiry(CoreDuration::from_ticks(10)))
            .unwrap();
        let delayed = door
            .submit(sized(60).with_delay(CoreDuration::from_ticks(15)))
            .unwrap();
        door.lock()
            .scheduler
            .set_memory_limits(Some(MemoryLimits { soft: 50, hard: 100 }));
        lead(&door, &mut node);
        assert_eq!(door.record(&delayed).unwrap(), None, "no room for it yet");

        block_on(async {
            let timers = tokio::spawn(crate::timers::run_timers(Arc::clone(&door), clock.clone()));
            tokio::task::yield_now().await;
            // The first task expires, which makes room for the delayed one.
            clock.advance(10);
            while door.record(&delayed).unwrap().is_none() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            let claim = tokio::spawn(Arc::clone(&door).claim_when_available(1));
            tokio::task::yield_now().await;
            assert!(!claim.is_finished(), "the delayed task is not due yet");
            clock.advance(5);

            let claims = claim.await.unwrap().unwrap();
            assert_eq!(claims[0].task.task_id(), delayed);
            door.close();
            timers.await.unwrap();
        });
    }

    #[test]
    fn a_leaders_answer_comes_back_only_after_its_record_is_stored() {
        let door = door(ManualClock::default(), Some(UNBOUNDED_GRANT));
        let task = door.submit(submission()).unwrap();

        let record = door.record(&task).unwrap().expect("stored before submit returned");
        assert_eq!(record.runs.len(), 1);
    }

    #[test]
    fn an_answer_whose_record_the_store_refuses_is_not_leader_and_the_stored_record_stands() {
        let door = door(ManualClock::default(), Some(grant(2)));
        let task = door.submit(submission()).unwrap();
        // The store already holds a newer version of the task, as from a
        // leader of a later term, so it refuses what this leader writes next.
        let elsewhere = self::door(ManualClock::default(), Some(grant(5)));
        let other = elsewhere.submit(submission()).unwrap();
        let mut newer = elsewhere.record(&other).unwrap().expect("stored at term 5");
        newer.task.as_mut().expect("a record carries its task").task_id = Some(task.clone().into());
        door.lock().scheduler.observer_mut().revision(newer.clone());

        assert_eq!(
            door.cancel(&task),
            Err(Refusal::Rejected(CancelRejection::NotLeader))
        );
        assert_eq!(door.record(&task).unwrap(), Some(newer));
    }

    /// A clock the test can make end a lease at a defined point: once armed,
    /// the next wall-clock reading (which a leader takes to stamp the records
    /// of its call, after it has checked that it leads) moves time on.
    #[derive(Debug, Clone, Default)]
    struct LapsingClock {
        ticks: Arc<AtomicU64>,
        lapse_at_next_stamp: Arc<AtomicBool>,
    }

    impl Clock for LapsingClock {
        fn now(&self) -> Instant {
            Instant::at(self.ticks.load(Ordering::SeqCst))
        }

        fn wall_clock_millis(&self) -> u64 {
            if self.lapse_at_next_stamp.swap(false, Ordering::SeqCst) {
                self.ticks.fetch_add(100, Ordering::SeqCst);
            }
            0
        }
    }

    #[test]
    fn an_answer_is_not_leader_when_the_lease_ends_during_the_call() {
        let clock = LapsingClock::default();
        let mut scheduler =
            Scheduler::with_observer(clock.clone(), Uuid7Ids, LocalRecords::new(worker(), clock.clone(), None));
        let lease = LeadershipGrant {
            valid_until: LeaseEnd::At(Instant::at(50)),
            ..grant(1)
        };
        scheduler.set_leadership_grant(Some(lease));
        let door = SchedulerDoor::new(scheduler, worker());
        let task = door.submit(submission()).unwrap();
        clock.lapse_at_next_stamp.store(true, Ordering::SeqCst);

        let answer = door.cancel(&task);

        assert_eq!(
            answer,
            Err(Refusal::Rejected(CancelRejection::NotLeader)),
            "the lease ended after the leader check, so the cancellation was not published"
        );
        let stored = door.record(&task).unwrap().expect("stored by the submission");
        assert!(!stored.finished);
        door.lock().scheduler.set_leadership_grant(Some(UNBOUNDED_GRANT));
        let published = door.record(&task).unwrap().expect("still stored");
        assert!(
            published.finished,
            "the cancellation was made before the refusal, and is published once the grant is back"
        );
    }

    #[test]
    fn a_queued_submission_the_scheduler_recorded_is_not_recorded_again_when_its_write_was_refused()
     {
        let clock = ManualClock::default();
        let door = door(clock.clone(), None);
        let mut node = node(clock);
        let older = door.submit(submission().with_coalescing_key("k")).unwrap();
        let newer = door.submit(submission().with_coalescing_key("k")).unwrap();
        // The store already holds `older` from a term above the one that is
        // about to lead, so it refuses what the grant records of it.
        let elsewhere = self::door(ManualClock::default(), Some(grant(5)));
        let other = elsewhere.submit(submission()).unwrap();
        let mut held = elsewhere.record(&other).unwrap().expect("stored at term 5");
        held.task.as_mut().expect("a record carries its task").task_id = Some(older.clone().into());
        door.lock().scheduler.observer_mut().revision(held.clone());

        lead(&door, &mut node);

        assert!(
            door.record(&newer).unwrap().is_some(),
            "the queue went on past the submission the scheduler had recorded"
        );
        assert_eq!(door.record(&older).unwrap(), Some(held), "the stored record stands");
        let runs = door.run_ids(&older).unwrap();
        let step = Step {
            outputs: Vec::new(),
            next_deadline: None,
        };
        door.carry_out(&mut node, step, |_| {}).unwrap();
        assert_eq!(
            door.run_ids(&older).unwrap(),
            runs,
            "the next step did not record it a second time"
        );
    }

    #[test]
    fn an_end_of_continuation_refused_while_not_leading_is_replayed_when_the_leader_returns() {
        let clock = ManualClock::default();
        let door = door(clock.clone(), Some(UNBOUNDED_GRANT));
        let mut node = node(clock);
        let task = door.submit(submission()).unwrap();
        let claims = block_on(Arc::clone(&door).claim_when_available(1)).unwrap();
        let run = claims[0].task_run_id.clone();
        door.report_started(&run).unwrap();
        door.complete(&run, digest(), Completion::Continues).unwrap();
        withdraw(&door, &mut node);

        assert_eq!(
            door.end_continuation(&task),
            Err(Refusal::Rejected(ContinuationRejection::NotLeader))
        );
        lead(&door, &mut node);

        let record = door.record(&task).unwrap().expect("recorded");
        assert!(record.finished, "the continuation the follower could not end was ended on the grant");
    }

    #[test]
    fn cancelling_a_submission_queued_before_the_grant_means_it_never_runs() {
        let clock = ManualClock::default();
        let door = door(clock.clone(), None);
        let mut node = node(clock);
        let task = door.submit(submission()).unwrap();

        assert_eq!(
            door.cancel(&task),
            Ok(Cancellation::Cancelled { was_running: false })
        );
        lead(&door, &mut node);

        assert_eq!(door.record(&task).unwrap(), None, "a cancelled submission is not recorded");
        assert_eq!(door.run_ids(&task).unwrap(), Vec::new());
    }

    // Two claims wait while the worker does not lead, with two tasks queued.
    // The grant reaches both in one wake-up, and each must claim one: a
    // second waiter left asleep with work queued would never be woken again.
    #[test]
    fn one_wake_up_hands_queued_work_to_every_waiting_claim() {
        let clock = ManualClock::default();
        let door = door(clock.clone(), Some(UNBOUNDED_GRANT));
        let mut node = node(clock);
        let queued = [
            door.submit(submission()).unwrap(),
            door.submit(submission()).unwrap(),
        ];
        withdraw(&door, &mut node);

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
        let mut scheduler =
            {
                let clock = ManualClock::default();
                Scheduler::with_observer(clock.clone(), Uuid7Ids, LocalRecords::new(worker(), clock, None))
            };
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
            .complete(&run_id, digest(), Completion::Final)
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

    #[test]
    fn a_finished_tasks_record_is_dropped_from_the_local_store_after_the_result_ttl() {
        let clock = ManualClock::default();
        let ttl = CoreDuration::from_millis(100);
        let mut scheduler = Scheduler::with_observer(
            clock.clone(),
            Uuid7Ids,
            LocalRecords::new(worker(), clock.clone(), Some(ttl)),
        );
        scheduler.set_result_ttl(Some(ttl));
        scheduler.set_leadership_grant(Some(UNBOUNDED_GRANT));
        let door = Arc::new(SchedulerDoor::new(scheduler, worker()));
        let task = door.submit(submission()).unwrap();
        assert_eq!(
            door.cancel(&task).unwrap(),
            Cancellation::Cancelled { was_running: false }
        );
        assert!(door.record(&task).unwrap().is_some_and(|record| record.finished));

        clock.advance(99);
        door.tick().unwrap();
        assert!(door.record(&task).unwrap().is_some(), "kept until the TTL passes");

        clock.advance(1);
        door.tick().unwrap();

        assert_eq!(door.record(&task).unwrap(), None);
    }
}
