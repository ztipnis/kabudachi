//! Where each worker's runs are found. Five workers, each running what its
//! driver claims through an executor, over records skewed onto three of
//! them: a worker, the leader included, takes the records it holds first;
//! an idle worker steals from the nearest distance class first and widens
//! one class at a time; the shard's oldest tasks come only after those; and
//! the work ends up spread over all five.

use std::num::NonZeroUsize;
use std::str::FromStr;
use std::time::Duration;

use kabudachi_core::protocol::ids::{TaskId, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::task_record::RecordKey;
use kabudachi_core::time::{RealClock, WallTime};
use kabudachi_net::discovery::Stage;
use kabudachi_net::task_store::placement::{ReplicationFactor, placement};
use libp2p::PeerId;
use libp2p::kad::KBucketKey;

use crate::support::deadline::within_deadline;
use crate::support::executor::FakeExecutor;
use crate::support::records::{Voters, plain_with, submitted_with, wait_until_held};

/// How many task ids are tried before a shard's identities count as too
/// unlucky to lay out.
const IDS_TRIED: usize = 1 << 16;

/// Each record is written to one voter only, so who holds what is exact.
fn one_holder() -> ReplicationFactor {
    ReplicationFactor::new(NonZeroUsize::MIN)
}

/// The first `count` task ids, of [`IDS_TRIED`], whose one holder among
/// `voters` is `holder`; `None` if fewer land there.
fn held_by(voters: &[WorkerId], holder: &WorkerId, count: usize) -> Option<Vec<TaskId>> {
    let mut held = Vec::new();
    for n in 0..IDS_TRIED {
        if held.len() == count {
            break;
        }
        let task = TaskId::new(format!("task-{n:06}"));
        if placement(&RecordKey::Task(task.clone()), voters, one_holder())?.holders[0] == *holder {
            held.push(task);
        }
    }
    (held.len() == count).then_some(held)
}

/// `tasks`, the ones whose record keys are nearest `worker`'s own key first.
fn nearest_first(worker: &WorkerId, tasks: &[TaskId]) -> Vec<TaskId> {
    let local = KBucketKey::from(PeerId::from_str(worker.as_str()).expect("a worker id is a peer id"));
    let mut tasks = tasks.to_vec();
    tasks.sort_by_key(|task| KBucketKey::new(RecordKey::Task(task.clone()).kad_bytes()).distance(&local));
    tasks
}

/// Drives the shard until `leader` places records among every voter, and
/// returns them.
async fn placing_among_every_voter(shard: &mut Voters, leader: usize) -> Vec<WorkerId> {
    loop {
        let voters = shard.with(leader, |node, _| node.voters()).await;
        if voters.len() == shard.nets.len() {
            return voters;
        }
        shard.drive_until(tokio::time::sleep(Duration::from_millis(10))).await;
    }
}

/// The next `count` runs `executor` is handed, each with its stage and task.
async fn runs(executor: &mut FakeExecutor, count: usize) -> Vec<(Stage, TaskId)> {
    let mut runs = Vec::new();
    for _ in 0..count {
        let (stage, _, claim) = executor.next_found().await;
        runs.push((stage, claim.task.as_ref().expect("a claim carries its task").task_id()));
    }
    runs
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_leader_runs_the_records_it_holds_before_the_shards_oldest_task() {
    within_deadline(async {
        let mut shard = Voters::start_without_client(3, one_holder()).await;
        let mut executors: Vec<FakeExecutor> = (0..3)
            .map(|voter| {
                let (executor, endpoint) = FakeExecutor::new();
                shard.set_executor(voter, endpoint);
                executor
            })
            .collect();
        let leader = shard.drive_until_a_leader().await;
        let voters = placing_among_every_voter(&mut shard, leader).await;
        let follower = shard.others(leader)[0];
        let (leader_id, follower_id) = (shard.id(leader), shard.id(follower));
        let elsewhere = held_by(&voters, &follower_id, 1).expect("a task the follower holds");
        let own = held_by(&voters, &leader_id, 2).expect("tasks the leader holds");
        let nets = shard.nets.clone();
        let start = WallTime::now(&RealClock::new()).as_unix_millis() - 60_000;

        // The follower's record is the shard's oldest task; the leader's two
        // are newer.
        shard
            .drive_until(async {
                for (n, task) in elsewhere.iter().chain(&own).enumerate() {
                    let at = WallTime::from_unix_millis(start + n as u64);
                    submitted_with(&nets[follower], &leader_id, (task.as_str(), at), plain_with(b"input")).await;
                }
                wait_until_held(nets[leader].clone(), own.clone()).await;
                wait_until_held(nets[follower].clone(), elsewhere.clone()).await;
            })
            .await;

        executors[leader].grant(3);
        let ran = shard.drive_until(runs(&mut executors[leader], 3)).await;
        let expected: Vec<(Stage, TaskId)> = nearest_first(&leader_id, &own)
            .into_iter()
            .map(|task| (Stage::Own, task))
            .chain([(Stage::Oldest, elsewhere[0].clone())])
            .collect();
        assert_eq!(ran, expected, "its own records, nearest first, then the shard's oldest task");
    })
    .await
}
