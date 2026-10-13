//! An executor a test plays for a driven worker: it offers the worker's
//! driver room to run tasks, reads the work the driver hands it, and reports
//! what became of each run. It holds its handle split, as an executor that
//! reads work in one place and reports from another does.

use std::time::Duration;

use kabudachi_core::protocol::ids::TaskRunId;
use kabudachi_core::protocol::messages::Claim;
use kabudachi_net::discovery::Stage;
use kabudachi_net::executor::{ExecutorEndpoint, Report, ReportSink, Work, WorkSource, executor_channel};
use tokio::time::timeout;

/// A backstop for work a test waits on: an election and a quorum write fit
/// well inside it.
const WORK_TIMEOUT: Duration = Duration::from_secs(20);

pub struct FakeExecutor {
    work: Option<WorkSource>,
    reports: ReportSink,
}

impl FakeExecutor {
    /// A fake executor, and the endpoint to give its worker's driver.
    pub fn new() -> (FakeExecutor, ExecutorEndpoint) {
        let (endpoint, handle) = executor_channel();
        let (work, reports) = handle.split();
        (
            FakeExecutor {
                work: Some(work),
                reports,
            },
            endpoint,
        )
    }

    /// Offers the driver `places` more places.
    pub fn grant(&self, places: u32) {
        self.report(Report::Capacity(places));
    }

    pub fn report(&self, report: Report) {
        self.reports
            .report(report)
            .expect("the worker's driver holds its executor endpoint");
    }

    /// The next work the driver hands over; panics if none comes in time.
    pub async fn next_work(&mut self) -> Work {
        timeout(WORK_TIMEOUT, self.work_source().next_work())
            .await
            .expect("work arrived within the timeout")
            .expect("the worker's driver holds its executor endpoint")
    }

    /// The next run handed over, a task's or a compaction's, with the
    /// discovery stage that found its task and its claim. How the leader
    /// answered a completion is not a run, and is passed over.
    pub async fn next_found(&mut self) -> (Stage, TaskRunId, Claim) {
        loop {
            match self.next_work().await {
                Work::Answered { .. } => continue,
                Work::Run { claim, stage } | Work::Compact { claim, stage } => {
                    let run = claim.task_run_id.clone().expect("a granted claim names its run");
                    return (stage, run.into(), claim);
                }
                other => panic!("expected a run handed over, got {other:?}"),
            }
        }
    }

    /// The next run handed over, a task's or a compaction's, with its claim.
    pub async fn next_claim(&mut self) -> (TaskRunId, Claim) {
        let (_, run, claim) = self.next_found().await;
        (run, claim)
    }

    /// Panics if the driver hands over any work within `window`, other than
    /// the leader's answer to a completion.
    pub async fn expect_no_work_for(&mut self, window: Duration) {
        let until = tokio::time::Instant::now() + window;
        while let Ok(work) = tokio::time::timeout_at(until, self.work_source().next_work()).await {
            if !matches!(work, Some(Work::Answered { .. })) {
                panic!("expected no work, got {work:?}");
            }
        }
    }

    /// Stops reading work, as an executor whose work half is dropped while
    /// its report half lives on.
    pub fn stop_taking_work(&mut self) {
        self.work = None;
    }

    fn work_source(&mut self) -> &mut WorkSource {
        self.work.as_mut().expect("this executor still takes work")
    }
}
