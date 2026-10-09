//! A worker started through its entry point that leads asks its own driver,
//! through its own `Net`, as any worker asks its leader: its own scheduler
//! decides, held to the memory limits the worker's config gives; once that
//! driver stops, what it asks itself is heard unanswered.

use std::time::Duration as StdDuration;

use kabudachi_core::election::ElectionTimings;
use kabudachi_core::protocol::ids::{ShardId, Uuid7Ids};
use kabudachi_core::protocol::messages::{TaskRejectReason, task_response};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::{MemoryLimits, mint};
use kabudachi_core::time::{Duration, RealClock};
use kabudachi_net::task_exchange::TaskFailure;

use crate::support::deadline::within_deadline;
use crate::support::records::plain_with;
use crate::support::worker::{spawn_worker, worker_config};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lone_leader_answers_its_own_submission_from_a_scheduler_held_to_its_memory_limits() {
    within_deadline(async {
        let timings = ElectionTimings::new(Duration::from_millis(2_000), Duration::from_millis(100))
            .with_roll_call_deadline(Duration::from_millis(100));
        let config = worker_config(ShardId::new("own-requests"), "/ip4/127.0.0.1/tcp/0", timings, vec![])
            .with_memory_limits(MemoryLimits { soft: 1, hard: 1 });
        let mut worker = spawn_worker(config).await;
        worker
            .wait_until(|seen| seen.state == WorkerState::Leader)
            .await;

        // Its scheduler may not hold its grant yet when the node leads: a
        // `NotLeader` answer is asked again, as any asker would.
        let submitted = mint(plain_with(b"more than a byte"), &Uuid7Ids, &RealClock::new());
        let refused = loop {
            let answer = worker
                .net
                .submit(worker.id.clone(), submitted.clone())
                .await
                .expect("the worker's own driver answered");
            let reason = match answer.result {
                Some(task_response::Result::Reject(reject)) => TaskRejectReason::try_from(reject.reason).ok(),
                other => panic!("a submission past the hard limit was not refused: {other:?}"),
            };
            if reason != Some(TaskRejectReason::TaskRejectNotLeader) {
                break reason;
            }
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        };
        assert_eq!(refused, Some(TaskRejectReason::TaskRejectBackpressure));

        // Once its driver has stopped, its own requests are heard unanswered
        // instead of waiting for an answer that never comes.
        worker.net.request_drain();
        while worker.seen.changed().await.is_ok() {}
        let after = worker.net.submit(worker.id.clone(), submitted).await;
        assert_eq!(after.err(), Some(TaskFailure::Unanswered));
    })
    .await
}
