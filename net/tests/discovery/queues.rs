//! Workers take work only from the queues they subscribe to, and the leader
//! holds them to it and to each queue's limit of tasks running at once.

use std::collections::BTreeSet;
use std::num::NonZeroU32;
use std::time::Duration;

use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::messages::{ClaimRejectReason, ClaimResponse, claim_response};
use kabudachi_core::queues::{QueueOptions, QueuePolicy, Subscription};
use kabudachi_core::scheduler::Submission;
use kabudachi_net::executor::Report;

use crate::support::deadline::within_deadline;
use crate::support::executor::FakeExecutor;
use crate::support::records::{ThreeVoters, submitted_through};

fn on(queue: &str, input: &[u8]) -> Submission {
    Submission::new(TaskDefinitionId::new("demo.task"), 1, input.to_vec(), queue)
}

fn only(queue: &str) -> Subscription {
    Subscription::Only(BTreeSet::from([queue.to_owned()]))
}

fn refusal(response: ClaimResponse) -> ClaimRejectReason {
    match response.result {
        Some(claim_response::Result::Reject(reject)) => {
            ClaimRejectReason::try_from(reject.reason).expect("a known reason")
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_gpu_worker_takes_only_gpu_tasks_a_cpu_worker_takes_none_and_a_full_queue_waits() {
    within_deadline(async {
        let (mut shard, client) = ThreeVoters::start().await;
        let mut executors: Vec<FakeExecutor> = (0..3)
            .map(|voter| {
                let (executor, endpoint) = FakeExecutor::new();
                shard.set_executor(voter, endpoint);
                executor
            })
            .collect();
        shard.set_queue_policy(QueuePolicy::new().with_queue(
            "gpu",
            QueueOptions {
                max_running: NonZeroU32::new(2),
                ..QueueOptions::DEFAULT
            },
        ));
        let gpu = 0;
        shard.set_queues(0, &["gpu"]);
        shard.set_queues(1, &["default"]);
        shard.set_queues(2, &["default"]);
        let leader = shard.drive_until_a_leader().await;
        let leader_id = shard.id(leader);
        // A CPU worker that is not the leader, so it claims over the network.
        let cpu = *shard
            .others(leader)
            .iter()
            .find(|voter| **voter != gpu)
            .expect("two others");
        let (gpu_id, cpu_id) = (shard.id(gpu), shard.id(cpu));
        shard.join_as_pending(&client, leader).await;

        // Until the leader has heard each worker's queues it would hold it to
        // none, so wait for both.
        loop {
            let known = shard
                .with(leader, |node, _| node.queue_subscriptions())
                .await;
            if known.get(&gpu_id) == Some(&only("gpu"))
                && known.get(&cpu_id) == Some(&only("default"))
            {
                break;
            }
            shard
                .drive_until(tokio::time::sleep(Duration::from_millis(10)))
                .await;
        }

        let mut gpu_tasks = Vec::new();
        let mut cpu_tasks = Vec::new();
        for n in 0..3u8 {
            gpu_tasks.push(
                shard
                    .drive_until(submitted_through(&client, &leader_id, on("gpu", &[n])))
                    .await,
            );
        }
        for n in 0..2u8 {
            cpu_tasks.push(
                shard
                    .drive_until(submitted_through(&client, &leader_id, on("default", &[n])))
                    .await,
            );
        }

        // The leader refuses a CPU worker a GPU task, whatever its records show.
        let cpu_net = shard.nets[cpu].clone();
        let asked = shard
            .drive_until(cpu_net.request_claim(leader_id.clone(), gpu_tasks[0].clone()))
            .await
            .expect("the leader answered");
        assert_eq!(refusal(asked), ClaimRejectReason::ClaimRejectNotSubscribed);

        // Asking for the oldest tasks of every queue, it is handed the oldest of
        // its own: the leader holds it to the queues it reported.
        let handed = shard
            .drive_until(cpu_net.claim_oldest(leader_id.clone(), 1, &Subscription::Every))
            .await
            .expect("the leader answered");
        let Some(claim_response::Result::Batch(batch)) = handed.result else {
            panic!("expected a batch, got {handed:?}");
        };
        let mut cpu_claimed = BTreeSet::new();
        for claim in batch.claims {
            let task = claim.task.expect("a claim carries its task");
            assert_eq!(task.queue, "default", "a CPU worker is handed no GPU task");
            cpu_claimed.insert(task.task_id());
        }
        assert_eq!(cpu_claimed.len(), 1, "the oldest task of its own queue");

        executors[gpu].grant(3);
        executors[cpu].grant(3);
        let mut gpu_runs = Vec::new();
        let mut gpu_claimed = BTreeSet::new();
        for _ in 0..2 {
            let (run, claim) = shard.drive_until(executors[gpu].next_claim()).await;
            let task = claim.task.expect("a claim carries its task");
            assert_eq!(task.queue, "gpu");
            gpu_claimed.insert(task.task_id());
            gpu_runs.push(run);
        }
        // The other CPU task comes through the worker's own discovery.
        let (_, claim) = shard.drive_until(executors[cpu].next_claim()).await;
        let task = claim.task.expect("a claim carries its task");
        assert_eq!(
            task.queue, "default",
            "a CPU worker is never handed a GPU task"
        );
        cpu_claimed.insert(task.task_id());
        assert_eq!(
            cpu_claimed,
            cpu_tasks.iter().cloned().collect::<BTreeSet<TaskId>>()
        );

        // Two GPU tasks are claimed, as many as the queue allows: the third
        // waits, and a claim of it is refused, from anyone.
        let third = gpu_tasks
            .iter()
            .find(|task| !gpu_claimed.contains(*task))
            .expect("one GPU task is left")
            .clone();
        let asked = shard
            .drive_until(client.request_claim(leader_id.clone(), third.clone()))
            .await
            .expect("the leader answered");
        assert_eq!(refusal(asked), ClaimRejectReason::ClaimRejectQueueFull);

        // One GPU run ends: the queue has room, and the GPU worker takes the third.
        let run = gpu_runs.remove(0);
        executors[gpu].report(Report::Started(run.clone()));
        executors[gpu].report(Report::Completed {
            run,
            digest: Digest::blake3(b"done"),
            delivered: false,
        });
        let (_, claim) = shard.drive_until(executors[gpu].next_claim()).await;
        assert_eq!(
            claim.task.expect("a claim carries its task").task_id(),
            third
        );
    })
    .await
}
