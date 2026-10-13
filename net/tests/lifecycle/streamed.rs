//! A streamed map over real sockets: a worker that does not lead sends its
//! members, learns of SlowDown from the leader's answers, polls until it
//! clears, and every member is made once.

use std::time::Duration as StdDuration;

use kabudachi_core::protocol::ids::{IdGenerator, Uuid7Ids};
use kabudachi_core::protocol::messages::{TaskRejectReason, task_response};
use kabudachi_core::scheduler::composite::OnError;
use kabudachi_core::scheduler::MemoryLimits;
use kabudachi_net::task_exchange::LeaderRetry;

use crate::support::deadline::within_deadline;
use crate::support::records::{Voters, composite_accepted_as, item_batch, member_run_through, streamed_map};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_follower_streams_a_map_through_slow_down_and_every_member_is_made_once() {
    within_deadline(async {
        let (mut shard, _) = Voters::start(3).await;
        for voter in 0..3 {
            // 100 items of 8 bytes pass the soft limit; 300 stay far below the hard one.
            shard
                .with(voter, |_, scheduler| {
                    scheduler.set_memory_limits(Some(MemoryLimits { soft: 512, hard: 32 * 1024 }))
                })
                .await;
        }
        let leader = shard.drive_until_a_leader().await;
        let leader_id = shard.id(leader);
        // A voter's net names its leader itself, so it asks the leader as a worker does.
        let client = shard.nets[shard.others(leader)[0]].clone();
        let retry = LeaderRetry {
            answer_within: StdDuration::from_secs(2),
            ask_again_after: StdDuration::from_millis(10),
        };
        let map = Uuid7Ids.next_composite_id();
        let accepted = shard
            .drive_until(composite_accepted_as(&client, &leader_id, &map, streamed_map(OnError::FailFast)))
            .await;
        assert!(!accepted.slow_down, "the head alone weighs a few bytes");
        let mut raised = false;
        for start in (0..300).step_by(100) {
            let batch = item_batch(&map, start, 100, start == 200);
            let answer = shard.drive_until(client.submit_members(&batch, retry)).await.unwrap();
            let Some(task_response::Result::Members(members)) = answer.result else {
                panic!("not accepted: {answer:?}");
            };
            assert_eq!(members.members, start + 100);
            raised |= members.slow_down;
            let again = shard.drive_until(client.submit_members(&batch, retry)).await.unwrap();
            assert!(matches!(again.result, Some(task_response::Result::Members(_))), "asked again, answered");
        }
        assert!(raised, "the follower heard SlowDown in an answer");
        assert_eq!(shard.drive_until(client.query_slow_down(retry)).await, Some(true));
        // Workers run every member and report it, one at a time.
        for index in 0..300 {
            shard.drive_until(member_run_through(&client, &leader_id, &map, index, b"r")).await;
        }
        let counted = map.clone();
        let runs = shard
            .with(leader, move |_, scheduler| {
                (0..300).map(|index| scheduler.runs_of(&counted.member_task(index)).len()).collect::<Vec<_>>()
            })
            .await;
        assert!(runs.iter().all(|count| *count == 1), "each member once: {runs:?}");
        assert_eq!(shard.drive_until(client.query_slow_down(retry)).await, Some(false));
        let refused = shard.drive_until(client.submit_members(&item_batch(&map, 300, 1, true), retry)).await.unwrap();
        assert!(
            matches!(
                refused.result,
                Some(task_response::Result::Reject(ref reject))
                    if reject.reason == TaskRejectReason::TaskRejectCompositeFinished as i32
            ),
            "the map is over: {refused:?}"
        );
    })
    .await;
}
