//! The bindings' one way into the shared scheduler: the mutex around it, the
//! wake-ups that follow each change, the flag that says the runtime has shut
//! down, and the worker every claim is made as. Nothing else in the crate
//! locks the scheduler.
//!
//! Every operation runs whole under the scheduler's lock, checks the closed
//! flag under that same lock, and wakes whoever the change concerns before
//! returning. The scheduler here carries the one-node record sink
//! (`LocalRecords`): each revision it publishes is stored in this node's own
//! store before the call returns, and keeps every write, so an answer stands
//! once the scheduler has given it. What the scheduler cannot take yet waits
//! in its backlog (`Backlog`), which every change except a claim or a read
//! hands over once the node leads. The one worker this door serves runs
//! compaction, so it claims the compaction runs of its own scheduler like any
//! other claim.

use std::collections::BTreeSet;
use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};

use kabudachi_core::election::{DropMessages, Input, NoAuthority, Step, WorkerNode, carry_out};
use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::ids::{TaskId, TaskRunId, Uuid7Ids, WorkerId};
use kabudachi_core::protocol::records::TaskRunRecord;
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::reconcile::Rebuild;
use kabudachi_core::scheduler::{
    Backlog, CancelRejection, Cancellation, Certification, Claim, ClaimRejection, Compacted,
    Completion, ContinuationRejection, Event, Failure, LostRun, ReportRejection, Scheduler,
    Submission, SubmitRejection,
};
use kabudachi_core::task_record::{LocalRecords, office_to_reconcile};
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
    /// What the scheduler cannot take yet; handed over after every change
    /// but a claim or a read.
    backlog: Backlog,
}

/// Runs `change` and returns its outcome. The local store settles every write
/// the change made before `put` returns, and keeps all of them: it holds only
/// what this one node wrote, each a newer version of the last. So the
/// scheduler's own answer stands, including a `NotLeader` it gave for want of
/// leadership.
fn settled_change<C: Clock, T, R>(
    scheduler: &mut DoorScheduler<C>,
    change: impl FnOnce(&mut DoorScheduler<C>) -> Result<T, R>,
) -> Result<T, R> {
    // Writes left by an operation nobody waits on are not this call's.
    drop(scheduler.observer_mut().take_settled());
    let outcome = change(scheduler);
    let settled = scheduler.observer_mut().take_settled();
    debug_assert!(
        settled.iter().all(|(_, kept)| *kept),
        "the lone node's own store keeps every record it is given"
    );
    outcome
}

/// [`settled_change`] for a change that goes through the backlog.
fn settled_backlog<C: Clock, T, R>(
    inside: &mut Inside<C>,
    change: impl FnOnce(&mut Backlog, &mut DoorScheduler<C>) -> Result<T, R>,
) -> Result<T, R> {
    let Inside { scheduler, backlog, .. } = inside;
    settled_change(scheduler, |scheduler| change(backlog, scheduler))
}

/// Hands the backlog over once the scheduler leads. Run after every change
/// that can free capacity or keys, so a queued submission is not left waiting
/// for the next one.
fn settle_pending<C: Clock>(inside: &mut Inside<C>) {
    let handed_over = settled_backlog(inside, |backlog, scheduler| {
        backlog.hand_over(scheduler);
        Ok::<(), std::convert::Infallible>(())
    });
    let Ok(()) = handed_over;
}

/// The bindings' one way into the shared scheduler.
pub struct SchedulerDoor<C: Clock> {
    inside: Mutex<Inside<C>>,
    wakeups: Wakeups,
    worker: WorkerId,
}

impl<C: Clock> SchedulerDoor<C> {
    /// A door whose own worker runs compaction: the one-node runtime folds the
    /// chains of its coalescing keys itself, on a free place.
    pub fn new(mut scheduler: DoorScheduler<C>, worker: WorkerId) -> Self {
        scheduler.set_compaction_runners(BTreeSet::from([worker.clone()]));
        SchedulerDoor {
            inside: Mutex::new(Inside {
                scheduler,
                closed: false,
                backlog: Backlog::default(),
            }),
            wakeups: Wakeups {
                claims: Wake::new(),
                events: Wake::new(),
                timers: Notify::new(),
            },
            worker,
        }
    }

    /// Gives `submission` its task id at once; the backlog records it now or
    /// holds it until the node leads. Wakes claims and timers.
    pub fn submit(&self, submission: Submission) -> Result<TaskId, Refusal<SubmitRejection>> {
        refuse(self.change_inside(Concerned::ClaimsAndTimers, |inside| {
            settled_backlog(inside, |backlog, scheduler| {
                backlog.submit(scheduler, submission)
            })
        }))
    }

    pub fn report_started(&self, run: &TaskRunId) -> Result<(), Refusal<ReportRejection>> {
        refuse(self.change(Concerned::ClaimsAndTimers, |scheduler| {
            settled_change(scheduler, |scheduler| {
                scheduler.report_started(&self.worker, run)
            })
        }))
    }

    pub fn complete(
        &self,
        run: &TaskRunId,
        result_digest: Digest,
        completion: Completion,
    ) -> Result<Certification, Refusal<ReportRejection>> {
        refuse(self.change(Concerned::ClaimsAndTimers, |scheduler| {
            settled_change(scheduler, |scheduler| {
                scheduler.complete(&self.worker, run, result_digest, completion)
            })
        }))
    }

    /// Reports the fold of a compaction run this worker claimed, and whether
    /// the leader applied it: it does not when the chain it was made for has
    /// changed since. Gated like `complete`.
    pub fn complete_compaction(
        &self,
        run: &TaskRunId,
        folded: Vec<u8>,
    ) -> Result<Compacted, Refusal<ReportRejection>> {
        refuse(self.change(Concerned::ClaimsAndTimers, |scheduler| {
            settled_change(scheduler, |scheduler| {
                scheduler.complete_compaction(&self.worker, run, folded)
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
            settled_change(scheduler, |scheduler| {
                if scheduler.task_run(run).map(TaskRunRecord::current_state)
                    == Some(TaskRunState::Claimed)
                {
                    scheduler.report_started(&self.worker, run)?;
                }
                scheduler.fail(&self.worker, run, failure_kind)
            })
        }))
    }

    /// Reports that one run this worker holds was lost with the process
    /// that ran its body.
    pub fn lose_run(&self, run: &TaskRunId) -> Result<LostRun, Refusal<ReportRejection>> {
        refuse(self.change(Concerned::ClaimsAndTimers, |scheduler| {
            settled_change(scheduler, |scheduler| scheduler.report_lost(&self.worker, run))
        }))
    }

    /// Cancels `task`. A submission still waiting in the backlog is dropped,
    /// so it never runs.
    pub fn cancel(&self, task: &TaskId) -> Result<Cancellation, Refusal<CancelRejection>> {
        refuse(self.change_inside(Concerned::ClaimsAndTimers, |inside| {
            settled_backlog(inside, |backlog, scheduler| backlog.cancel(scheduler, task))
        }))
    }

    /// Ends the continuation of `task`. Refused for want of leadership, it
    /// is kept by the backlog and ended once this node leads; the caller is
    /// still told it was refused.
    pub fn end_continuation(
        &self,
        task: &TaskId,
    ) -> Result<bool, Refusal<ContinuationRejection>> {
        refuse(self.change_inside(Concerned::ClaimsAndTimers, |inside| {
            settled_backlog(inside, |backlog, scheduler| {
                backlog.end_continuation(scheduler, task)
            })
        }))
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
            let mut next = carry_out(
                node,
                step,
                &mut inside.scheduler,
                &mut DropMessages,
                &mut NoAuthority,
                |_, _, _, step| observe(step),
            );
            drop(inside.scheduler.observer_mut().take_settled());
            if let Some(office) = office_to_reconcile(node, &inside.scheduler) {
                // A lone node has nothing stored to rebuild from: a record is
                // only ever written once the node leads, and it is the one
                // that wrote it.
                inside
                    .scheduler
                    // Cannot fail: the caller found the scheduler reconciling this office
                    // (`office_to_reconcile`), and the rebuild runs once, before the node
                    // is told it has reconciled, which is what ends the reconciliation.
                    .reconcile(Rebuild::default())
                    .expect("a lone node reconciles the office its scheduler waits for");
                let watching = node.step(Input::WatchWorkers(BTreeSet::new()));
                carry_out(
                    node,
                    watching,
                    &mut inside.scheduler,
                    &mut DropMessages,
                    &mut NoAuthority,
                    |_, _, _, step| observe(step),
                );
                let reconciled = node.step(Input::Reconciled(office));
                next = carry_out(
                    node,
                    reconciled,
                    &mut inside.scheduler,
                    &mut DropMessages,
                    &mut NoAuthority,
                    |_, _, _, step| observe(step),
                );
                drop(inside.scheduler.observer_mut().take_settled());
            }
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
    /// under the same lock and once this node leads, by handing over the
    /// backlog, as capacity or keys the change freed may let it advance.
    fn change<T>(
        &self,
        concerned: Concerned,
        change: impl FnOnce(&mut DoorScheduler<C>) -> T,
    ) -> Result<T, Closed> {
        self.change_inside(concerned, |inside| change(&mut inside.scheduler))
    }

    /// [`Self::change`] for a change that also needs the backlog.
    fn change_inside<T>(
        &self,
        concerned: Concerned,
        change: impl FnOnce(&mut Inside<C>) -> T,
    ) -> Result<T, Closed> {
        self.change_then(concerned, change, |_, outcome| outcome)
    }

    /// [`Self::change_inside`] whose result is also read after the
    /// backlog has been handed over, so it describes the scheduler as the caller
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
                    settled_change(scheduler, |scheduler| {
                        scheduler.claim_oldest(&self.worker, limit)
                    })
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
                        | ClaimRejection::KeyBusy
                        | ClaimRejection::CannotRun,
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
