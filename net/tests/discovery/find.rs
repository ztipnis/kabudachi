//! A claim a worker's stale view suggested is refused and discovery moves on,
//! and a worker that knows no peers asks the leader for the oldest tasks.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, WorkerId};
use kabudachi_core::protocol::messages::Claim;
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::queues::Subscription;
use kabudachi_core::task_record::RecordKey;
use kabudachi_core::time::{RealClock, WallTime};
use kabudachi_core::versions::RunnableVersions;
use kabudachi_net::discovery::{Found, Stage, Takes};
use kabudachi_net::messenger::{Net, PlacedWrite};
use kabudachi_net::task_store::placement::{ReplicationFactor, placement};

use crate::support::deadline::within_deadline;
use crate::support::net::wait_until_registered;
use crate::support::records::{
    ThreeVoters, Voters, claimed, claims_in, placing_among_every_voter, plain_with, submitted_through,
    submitted_with, takes, wait_until_held,
};

/// Each record is written to one voter only, so who holds what differs from
/// voter to voter.
fn one_holder() -> ReplicationFactor {
    ReplicationFactor::new(NonZeroUsize::MIN)
}

fn ids(shard: &Voters) -> Vec<WorkerId> {
    (0..shard.nets.len()).map(|voter| shard.id(voter)).collect()
}

fn tasks_of(found: &Found) -> Vec<TaskId> {
    found
        .claims
        .iter()
        .map(|(_, claim): &(Stage, Claim)| claim.task.as_ref().expect("a claim carries its task").task_id())
        .collect()
}

fn sorted(mut tasks: Vec<TaskId>) -> Vec<TaskId> {
    tasks.sort();
    tasks
}

fn now() -> WallTime {
    WallTime::now(&RealClock::new())
}

/// The voter, by index, that holds `task`; one does when each record is
/// written to one voter.
fn holder_of(nets: &[Arc<Net>], task: &TaskId) -> usize {
    (0..nets.len())
        .find(|voter| nets[*voter].held_records().get(task).is_some())
        .expect("one voter holds it")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_claim_the_workers_stale_record_suggested_is_refused_and_discovery_moves_on() {
    within_deadline(async {
        let (mut shard, client) = Voters::start_with(3, one_holder()).await;
        let leader = shard.drive_until_a_leader().await;
        let (nets, ids) = (shard.nets.clone(), ids(&shard));
        let submitter = shard.others(leader)[0];

        let found = shard
            .drive_until(async {
                let task = submitted_through(&nets[submitter], &ids[leader], plain_with(b"input")).await;
                let holder = holder_of(&nets, &task);
                // One voter that is no leader and holds nothing is the
                // worker; the other voter that is no leader is the rival.
                let others: Vec<usize> = (0..nets.len()).filter(|voter| *voter != leader).collect();
                let worker = *others.iter().find(|voter| **voter != holder).expect("two voters are no leader");
                let rival = *others.iter().find(|voter| **voter != worker).expect("two voters are no leader");
                let waiting = nets[holder].held_records().get(&task).expect("held");

                // The rival takes the task, then the worker is handed the
                // record as it was before: a view that lags the leader's.
                claimed(&nets[rival], &ids[leader], &task).await;
                let mut lagging = waiting;
                lagging.placement = vec![ids[worker].clone().into()];
                client.write_records(vec![PlacedWrite::new(lagging.into(), 1)]);
                wait_until_held(nets[worker].clone(), vec![task]).await;

                nets[worker].discover(ids[leader].clone(), 1, now(), &takes(false), None).await
            })
            .await;

        assert_eq!(found.stale, 1, "its record showed the task waiting; the leader said otherwise");
        assert!(found.claims.is_empty(), "{found:?}");
        assert_eq!(found.stopped, None);
    })
    .await
}

/// A task whose input is newer than a worker runs is not claimed from its
/// own records: the worker tells the leader, which records the block and
/// never hands that worker the task, and another worker claims it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_worker_skips_and_reports_a_task_newer_than_it_runs_and_another_claims_it() {
    within_deadline(async {
        let (mut shard, _client) = Voters::start_with(3, one_holder()).await;
        let leader = shard.drive_until_a_leader().await;
        let voters = placing_among_every_voter(&mut shard, leader).await;
        let (nets, ids) = (shard.nets.clone(), ids(&shard));
        // A follower must hold the record: its report crosses the network. (A
        // leader-held record is reported in process, not by `discover`.)
        let (name, holder) = (0..)
            .find_map(|n| {
                let name = format!("task-{n:06}");
                let key = RecordKey::Task(TaskId::new(name.clone()));
                let holder = placement(&key, &voters, one_holder())?.holders.remove(0);
                (holder != ids[leader]).then_some((name, holder))
            })
            .expect("some task id lands on a follower");
        let holder = ids.iter().position(|id| *id == holder).expect("a voter");
        let other = (0..nets.len()).find(|voter| *voter != leader && *voter != holder).expect("three voters");
        let runs_three = Takes {
            runnable: RunnableVersions::default().with(TaskDefinitionId::new("demo.task"), 3),
            ..takes(false)
        };

        let (found, record, oldest, elsewhere) = shard
            .drive_until(async {
                let mut submission = plain_with(b"input");
                submission.source_version = 5;
                let task = submitted_with(&nets[holder], &ids[leader], (name.as_str(), now()), submission).await;
                wait_until_held(nets[holder].clone(), vec![task.clone()]).await;

                let found = nets[holder].discover(ids[leader].clone(), 4, now(), &runs_three, None).await;
                let record = loop {
                    match nets[holder].held_records().get(&task) {
                        Some(record) if record.blocked_since.is_some() => break record,
                        _ => tokio::time::sleep(Duration::from_millis(10)).await,
                    }
                };
                let oldest = nets[holder]
                    .claim_oldest(ids[leader].clone(), 4, &Subscription::Every, None)
                    .await
                    .expect("the leader answers");
                let elsewhere = nets[other].discover(ids[leader].clone(), 4, now(), &takes(false), None).await;
                (found, record, oldest, elsewhere)
            })
            .await;
        let task = TaskId::new(name);

        assert!(!tasks_of(&found).contains(&task), "the holder does not claim a record it cannot run: {found:?}");
        assert!(
            record.blocked_reason.contains("newer than this worker's 3"),
            "the leader kept the worker's reason: {}",
            record.blocked_reason
        );
        assert!(
            !claims_in(&oldest).contains(&task),
            "the leader does not hand the holder the task it blocked"
        );
        assert!(tasks_of(&elsewhere).contains(&task), "a worker that runs version 5 claims it: {elsewhere:?}");
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_worker_that_knows_no_peers_asks_the_leader_for_the_oldest_tasks() {
    within_deadline(async {
        let (mut shard, _client) = ThreeVoters::start().await;
        let leader = shard.drive_until_a_leader().await;
        let (nets, ids) = (shard.nets.clone(), ids(&shard));
        let submitter = shard.others(leader)[0];

        // A net that stores no records has no peers to steal from.
        let bare = Arc::new(Net::new());
        let at = bare.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()).await;
        nets[leader].dial(at);
        wait_until_registered(&bare, &ids[leader]).await;
        shard.join_as_pending(&bare, leader).await;

        let (tasks, found) = shard
            .drive_until(async {
                let first = submitted_through(&nets[submitter], &ids[leader], plain_with(b"first")).await;
                let second = submitted_through(&nets[submitter], &ids[leader], plain_with(b"second")).await;
                let found = bare.discover(ids[leader].clone(), 5, now(), &takes(false), None).await;
                (vec![first, second], found)
            })
            .await;

        assert!(found.claims.iter().all(|(stage, _)| *stage == Stage::Oldest), "{found:?}");
        assert_eq!(sorted(tasks_of(&found)), sorted(tasks));
    })
    .await
}
