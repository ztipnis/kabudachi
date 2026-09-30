//! The scheduler under test, and the one fixture every scheduler test file
//! builds on.

use kabudachi_core::protocol::ids::{TaskId, TaskRunId};
use kabudachi_core::protocol::records::TaskRunRecord;
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::scheduler::{MemoryLimits, Scheduler};
use kabudachi_core::time::Duration;

use crate::support::clock::FakeClock;
use crate::support::grant::unbounded_grant;
use crate::support::ids::SequentialIds;
use crate::support::spy::Spy;

pub type TestScheduler = Scheduler<FakeClock, SequentialIds, Spy>;

pub struct Fixture {
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
        let clock = FakeClock::new();
        let spy = Spy::default();
        let scheduler = Scheduler::with_observer(clock.clone(), SequentialIds::new(), spy.clone());
        Self {
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
