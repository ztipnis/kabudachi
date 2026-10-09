//! An executor a test plays for a driven worker: it offers the worker's
//! driver room to run tasks, reads the work the driver hands it, and reports
//! what became of each run.

use std::time::Duration;

use kabudachi_core::protocol::ids::TaskRunId;
use kabudachi_core::protocol::messages::Claim;
use kabudachi_net::executor::{ExecutorEndpoint, ExecutorHandle, Report, Work, executor_channel};
use tokio::time::timeout;

/// A backstop for work a test waits on: an election and a quorum write fit
/// well inside it.
const WORK_TIMEOUT: Duration = Duration::from_secs(20);

pub struct FakeExecutor {
    handle: ExecutorHandle,
}

impl FakeExecutor {
    /// A fake executor, and the endpoint to give its worker's driver.
    pub fn new() -> (FakeExecutor, ExecutorEndpoint) {
        let (endpoint, handle) = executor_channel();
        (FakeExecutor { handle }, endpoint)
    }

    /// Offers the driver `places` more places.
    pub fn grant(&self, places: u32) {
        self.report(Report::Capacity(places));
    }

    pub fn report(&self, report: Report) {
        self.handle
            .report(report)
            .expect("the worker's driver holds its executor endpoint");
    }

    /// The next work the driver hands over; panics if none comes in time.
    pub async fn next_work(&mut self) -> Work {
        timeout(WORK_TIMEOUT, self.handle.next_work())
            .await
            .expect("work arrived within the timeout")
            .expect("the worker's driver holds its executor endpoint")
    }

    /// The next run handed over, a task's or a compaction's, with its claim.
    pub async fn next_claim(&mut self) -> (TaskRunId, Claim) {
        match self.next_work().await {
            Work::Run(claim) | Work::Compact(claim) => {
                let run = claim.task_run_id.clone().expect("a granted claim names its run");
                (run.into(), claim)
            }
            other => panic!("expected a run handed over, got {other:?}"),
        }
    }

    /// Panics if the driver hands over any work within `window`.
    pub async fn expect_no_work_for(&mut self, window: Duration) {
        if let Ok(work) = timeout(window, self.handle.next_work()).await {
            panic!("expected no work, got {work:?}");
        }
    }
}
