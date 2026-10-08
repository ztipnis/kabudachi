//! The scheduler under test, and the one fixture every scheduler test file
//! builds on.

use std::collections::BTreeMap;

use kabudachi_core::coordination_authority::RecoveryEpoch;
use kabudachi_core::protocol::generated::TaskRecord;
use kabudachi_core::protocol::ids::{IdGenerator, TaskId, TaskRunId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::records::TaskRunRecord;
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::reconcile::ReconcileTerm;
use kabudachi_core::scheduler::{LeadershipGrant, LeaseEnd, MemoryLimits, Observer, Scheduler};
use kabudachi_core::time::{Clock, Duration};

use crate::support::clock::FakeClock;
use crate::support::grant::unbounded_grant;
use crate::support::ids::SequentialIds;
use crate::support::spy::Spy;

pub type TestScheduler = Scheduler<FakeClock, SequentialIds, Spy>;

pub struct Fixture {
    ids: SequentialIds,
    pub clock: FakeClock,
    pub scheduler: TestScheduler,
    pub spy: Spy,
}

impl Fixture {
    /// A scheduler that already holds a grant, as the runtime leaves it once
    /// its election is won.
    pub fn leading() -> Self {
        let mut fixture = Self::not_leading();
        fixture
            .scheduler
            .set_leadership_grant(Some(unbounded_grant()));
        fixture
    }

    pub fn not_leading() -> Self {
        Self::not_leading_with(SequentialIds::new())
    }

    /// A scheduler of a shard's next leader: it has not won yet, and its ids
    /// never repeat those `previous` handed out, as no two leaders' ids do.
    pub fn successor_of(previous: &Fixture) -> Self {
        Self::not_leading_with(previous.ids.clone())
    }

    fn not_leading_with(ids: SequentialIds) -> Self {
        let clock = FakeClock::new();
        let spy = Spy::default();
        let scheduler = Scheduler::with_observer(clock.clone(), ids.clone(), spy.clone());
        Self {
            ids,
            clock,
            scheduler,
            spy,
        }
    }

    pub fn leading_with_limits(limits: MemoryLimits) -> Self {
        let mut fixture = Self::leading();
        fixture.scheduler.set_memory_limits(Some(limits));
        fixture
    }

    /// The state of `task`'s current run. Panics on an unknown or forgotten
    /// task.
    pub fn state(&self, task: &TaskId) -> TaskRunState {
        state_of(&self.scheduler, &self.spy, task)
    }

    /// Whether the scheduler has forgotten `task` and every run of it, as it
    /// does a finished task that outlived its retention or was superseded.
    /// A task it never knew counts as forgotten too.
    pub fn forgotten(&self, task: &TaskId) -> bool {
        self.scheduler.runs_of(task).is_empty()
    }

    /// The state of `run`. Panics on an unknown run.
    pub fn run_state(&self, run: &TaskRunId) -> TaskRunState {
        self.scheduler
            .task_run(run)
            .unwrap_or_else(|| panic!("no run {run:?}"))
            .current_state()
    }
}

/// The state of `task`'s current run, its newest. Panics on an unknown or
/// forgotten task.
///
/// While the scheduler leads, it also checks that what the scheduler
/// published says the same: the newest revision `spy` holds of `task` ends in
/// a run with the id and state of the scheduler's newest run. A change the
/// scheduler made after its lease ended is published only by the next call
/// that finds it leading, so nothing is asserted while it does not lead. A
/// task `spy` holds no revision of is skipped, as a driver may have drained
/// the spy.
pub fn state_of<C: Clock, I: IdGenerator, O: Observer>(
    scheduler: &Scheduler<C, I, O>,
    spy: &Spy,
    task: &TaskId,
) -> TaskRunState {
    let newest = scheduler
        .runs_of(task)
        .pop()
        .unwrap_or_else(|| panic!("the scheduler has no run of {task:?}"));
    let state = scheduler
        .task_run(&newest)
        .unwrap_or_else(|| panic!("the scheduler has no run {newest:?}"))
        .current_state();
    if scheduler.is_leader()
        && let Some(record) = spy.newest_revision_of(task)
    {
        let published = record
            .runs
            .last()
            .unwrap_or_else(|| panic!("the newest revision of {task:?} has no run"));
        assert_eq!(
            (published.task_run_id(), published.current_state()),
            (newest, state),
            "the newest revision of {task:?} does not carry its current run as it stands"
        );
    }
    state
}

pub fn ticks(n: u64) -> Duration {
    Duration::from_ticks(n)
}

pub const OFFICE: ReconcileTerm = ReconcileTerm {
    recovery_epoch: RecoveryEpoch::new(0, 0),
    term: 2,
};

pub fn grant_of(office: ReconcileTerm) -> LeadershipGrant {
    LeadershipGrant {
        term: office.term,
        recovery_epoch: office.recovery_epoch,
        valid_until: LeaseEnd::Unbounded,
        reconnect_timeout: kabudachi_core::election::ElectionTimings::DEFAULT_RECONNECT_TIMEOUT,
    }
}

/// The newest record of every task `fixture` published, as holders would
/// keep them.
pub fn newest_records(fixture: &Fixture) -> Vec<TaskRecord> {
    let mut newest: BTreeMap<TaskId, TaskRecord> = BTreeMap::new();
    for record in fixture.spy.revisions() {
        newest.insert(record.task.as_ref().unwrap().task_id(), record);
    }
    newest.into_values().collect()
}

/// The scheduler of the next leader of what `previous` led, which has just
/// taken office for `OFFICE`.
pub fn reconciling_after(previous: &Fixture) -> Fixture {
    let mut fixture = Fixture::successor_of(previous);
    fixture.scheduler.begin_reconcile(OFFICE);
    fixture
}

pub fn reconciling() -> Fixture {
    reconciling_after(&Fixture::not_leading())
}
