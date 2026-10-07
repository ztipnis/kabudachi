//! A drained voter's driver returns only once it has handed the records it
//! holds to voters its leader chose, and the leader then places each of them
//! among the voters left.

use std::num::NonZeroUsize;
use std::sync::Arc;

use kabudachi_core::coordination_authority::RecoveryEpoch;
use kabudachi_core::protocol::generated::{Task, TaskRecord};
use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, WorkerId};
use kabudachi_core::task_record::RecordVersion;
use kabudachi_net::messenger::{Net, PlacedWrite};
use kabudachi_net::task_store::placement::ReplicationFactor;

use crate::support::deadline::within_deadline;
use crate::support::election::wait_until;
use crate::support::records::{ThreeVoters, Voters, plain_with, submitted_through, wait_until_held};

fn two_holders() -> ReplicationFactor {
    ReplicationFactor::new(NonZeroUsize::new(2).expect("two"))
}

/// How many of `nets` other than `except` hold `task`, counting only copies
/// whose placement is accepted by `counts`.
fn holding(nets: &[Arc<Net>], except: usize, task: &TaskId, counts: impl Fn(&[WorkerId]) -> bool) -> usize {
    (0..nets.len())
        .filter(|voter| *voter != except)
        .filter(|voter| {
            nets[*voter].held_records().get(task).is_some_and(|record| {
                let placement: Vec<WorkerId> = record.placement.into_iter().map(WorkerId::from).collect();
                counts(&placement)
            })
        })
        .count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_drained_voter_hands_its_records_over_before_its_driver_returns_and_the_leader_places_them_anew() {
    within_deadline(async {
        let (mut shard, _client) = Voters::start_with(4, two_holders()).await;
        let leader = shard.drive_until_a_leader().await;
        let [drainer, submitter] = [shard.others(leader)[0], shard.others(leader)[1]];
        let nets = shard.nets.clone();
        let (leader_id, drainer_id) = (shard.id(leader), shard.id(drainer));

        let handed_off = shard.handed_off();
        // One call drives the shard throughout: its drivers keep where they
        // wrote each record only while they run.
        shard
            .drive_until(async {
                // The leader's answer to a submission waits for both holders
                // to store its record, so each task the drainer holds is held
                // by its other holder too.
                let mut tasks = Vec::new();
                while tasks.len() < 2 {
                    let task = submitted_through(&nets[submitter], &leader_id, plain_with(b"in")).await;
                    if nets[drainer].held_records().get(&task).is_some() {
                        tasks.push(task);
                    }
                }

                nets[drainer].request_drain();
                let handed = handed_off.returned(drainer).await;

                // The leader may have written a record anew already, past the
                // drainer's copy: then that copy needs no handing over.
                assert_eq!(handed.stored + handed.superseded, tasks.len(), "{handed:?}");
                assert!(handed.abandoned.is_empty(), "{handed:?}");
                for task in &tasks {
                    assert!(
                        holding(&nets, drainer, task, |_| true) >= 2,
                        "{task:?} is held by two voters besides the drainer once its driver has returned"
                    );
                }

                // The leader hears the drainer leave: it publishes each record
                // again, placed on voters that remain.
                wait_until(|| {
                    tasks.iter().all(|task| {
                        holding(&nets, drainer, task, |placement| !placement.contains(&drainer_id)) >= 2
                    })
                })
                .await;
            })
            .await;
    })
    .await;
}

/// `task`'s record at `revision`, placed on `holders`.
fn placed(task: &TaskId, revision: u64, holders: &[WorkerId]) -> TaskRecord {
    TaskRecord {
        version: Some(
            RecordVersion { recovery_epoch: RecoveryEpoch::new(0, 0), leader_term: 1, revision }.into(),
        ),
        task: Some(Task {
            task_id: Some(task.clone().into()),
            task_definition_id: Some(TaskDefinitionId::new("definition").into()),
            ..Task::default()
        }),
        placement: holders.iter().map(|holder| holder.clone().into()).collect(),
        ..TaskRecord::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_revision_placed_elsewhere_makes_a_holder_it_left_drop_its_copy() {
    within_deadline(async {
        let (shard, writer) = ThreeVoters::start().await;
        let (former, other) = (shard.id(0), shard.id(1));
        let task = TaskId::new("task-1");
        writer.write_records(vec![PlacedWrite {
            record: placed(&task, 0, std::slice::from_ref(&former)),
            quorum: 1,
        }]);
        wait_until_held(shard.nets[0].clone(), vec![task.clone()]).await;

        writer.retire_copies(placed(&task, 1, &[other]), vec![former]);

        wait_until(|| shard.nets[0].held_records().get(&task).is_none()).await;
    })
    .await;
}
