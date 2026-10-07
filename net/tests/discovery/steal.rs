//! A shard peer tells a worker with nothing to run which tasks it holds
//! records of that look claimable, over real sockets.

use kabudachi_core::protocol::ids::TaskDefinitionId;
use kabudachi_core::scheduler::Submission;
use kabudachi_core::time::{Clock, Duration, RealClock, WallTime};

use crate::support::deadline::within_deadline;
use crate::support::records::{ThreeVoters, plain_with, submitted_with, wait_until_held};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_peer_answers_a_steal_with_the_waiting_tasks_it_holds_oldest_first_up_to_the_limit() {
    within_deadline(async {
        let (mut shard, _client) = ThreeVoters::start().await;
        let leader = shard.drive_until_a_leader().await;
        let [asker, holder] = shard.others(leader)[..] else {
            panic!("three voters have two others");
        };
        let (asker_net, holder_net) = (shard.nets[asker].clone(), shard.nets[holder].clone());
        let (leader_id, holder_id) = (shard.id(leader), shard.id(holder));
        let now = RealClock::new().wall_clock_millis();
        let at = |offset: u64| WallTime::from_unix_millis(now + offset);

        let (answer, one, ids) = shard
            .drive_until(async {
                // The older task's id sorts after the newer one's, so only
                // the order of submission can put it first.
                let first = submitted_with(&asker_net, &leader_id, ("z-older", at(0)), plain_with(b"1")).await;
                let second = submitted_with(&asker_net, &leader_id, ("a-newer", at(10)), plain_with(b"2")).await;
                let later = Submission::new(TaskDefinitionId::new("demo.task"), 1, b"3".to_vec(), "default");
                let later = Submission {
                    delay: Some(Duration::from_secs(3600)),
                    ..later
                };
                let far = submitted_with(&asker_net, &leader_id, ("far", at(0)), later).await;
                wait_until_held(holder_net, vec![first.clone(), second.clone(), far]).await;
                let answer = asker_net.steal(holder_id.clone(), 10, false).await;
                let one = asker_net.steal(holder_id, 1, false).await;
                (answer, one, (first, second))
            })
            .await;

        let (first, second) = ids;
        assert_eq!(
            answer,
            Some(vec![first.clone(), second]),
            "both waiting tasks, oldest submission first, and not the one not due for an hour"
        );
        assert_eq!(one, Some(vec![first]), "no more than the limit");
    })
    .await
}
