//! A worker started through its entry point routes its requests to the leader
//! its node names, which, while it leads alone, is its own driver: a routed
//! request waits for a leader, is asked again while refused for now, and gets
//! the scheduler's decision, held to the memory limits the worker's config
//! gives. Once the worker has left its shard, a routed request ends
//! unanswered and what it asks itself is heard unanswered.

use std::time::Duration as StdDuration;

use kabudachi_core::election::ElectionTimings;
use kabudachi_core::protocol::ids::{ShardId, Uuid7Ids};
use kabudachi_core::protocol::messages::{TaskRejectReason, task_response};
use kabudachi_core::scheduler::{MemoryLimits, mint};
use kabudachi_core::time::{Duration, RealClock};
use kabudachi_net::task_exchange::{LeaderRetry, TaskFailure};

use crate::support::deadline::within_deadline;
use crate::support::records::plain_with;
use crate::support::worker::{spawn_worker, worker_config};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_routed_to_the_leader_gets_its_decision_and_ends_once_the_worker_leaves() {
    within_deadline(async {
        let timings = ElectionTimings::new(Duration::from_millis(2_000), Duration::from_millis(100))
            .with_roll_call_deadline(Duration::from_millis(100));
        let config = worker_config(ShardId::new("routed-requests"), "/ip4/127.0.0.1/tcp/0", timings, vec![])
            .with_memory_limits(MemoryLimits { soft: 1, hard: 1 });
        let mut worker = spawn_worker(config).await;
        let retry = LeaderRetry {
            answer_within: StdDuration::from_secs(2),
            ask_again_after: StdDuration::from_millis(10),
        };

        // Asked before the worker names any leader: the ask waits for one,
        // and a refusal that only says "not yet" (its scheduler may not hold
        // its grant when the node first leads) is asked again, so the answer
        // that comes back is the scheduler's own decision.
        let submitted = mint(plain_with(b"more than a byte"), &Uuid7Ids, &RealClock::new());
        let answer = worker
            .net
            .submit_to_leader(submitted.clone(), retry)
            .await
            .expect("the worker is in its shard");
        let reason = match answer.result {
            Some(task_response::Result::Reject(reject)) => TaskRejectReason::try_from(reject.reason).ok(),
            other => panic!("a submission past the hard limit was not refused: {other:?}"),
        };
        assert_eq!(reason, Some(TaskRejectReason::TaskRejectBackpressure));
        assert!(
            worker
                .net
                .cancel_at_leader(submitted.task_id.clone(), retry)
                .await
                .is_some(),
            "the leader answers a cancel of a task it never stored"
        );

        // Once the worker has left its shard, neither waits for a leader.
        worker.net.request_drain();
        while worker.seen.changed().await.is_ok() {}
        assert!(worker.net.has_left_shard());
        assert!(worker.net.submit_to_leader(submitted.clone(), retry).await.is_none());
        assert!(worker.net.cancel_at_leader(submitted.task_id.clone(), retry).await.is_none());
        let after = worker.net.submit(worker.id.clone(), submitted).await;
        assert_eq!(after.err(), Some(TaskFailure::Unanswered));
    })
    .await
}
