//! The scheduler under test, and the one fixture every scheduler test file
//! builds on.

use std::collections::BTreeMap;

use kabudachi_core::coordination_authority::RecoveryEpoch;
use kabudachi_core::protocol::generated::TaskRecord;
use kabudachi_core::protocol::ids::{TaskId, TaskRunId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::records::TaskRunRecord;
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::reconcile::ReconcileTerm;
use kabudachi_core::scheduler::{LeadershipGrant, LeaseEnd, MemoryLimits, Scheduler};
use kabudachi_core::time::Duration;

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

    /// The state of `task`'s current run, from what the spy was told, after
    /// checking it against the scheduler's own public reads. Panics on an
    /// unknown task.
    pub fn state(&self, task: &TaskId) -> TaskRunState {
        self.spy.checked_state_of(&self.scheduler, task)
    }

    /// The state of `run`. Panics on an unknown run.
    pub fn run_state(&self, run: &TaskRunId) -> TaskRunState {
        self.scheduler
            .task_run(run)
            .unwrap_or_else(|| panic!("no run {run:?}"))
            .current_state()
    }
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
