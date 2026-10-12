//! A task's result reaches the worker that awaits it, over real sockets.
//! The worker that ran it delivers it, then reports the run delivered, and
//! the leader certifies the run to the client once the report is stored.
//! A client cut off from the runner still hears the run certified, with no
//! result. A task that ends without a result is told to its client, and a
//! new leader answers how a task stands.

use std::time::Duration as StdDuration;

use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::generated::TaskRunState;
use kabudachi_core::protocol::ids::{TaskId, TaskRunId, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::messages::{
    DeliverResult, OutcomeAnswer, TaskRejectReason, task_response,
};
use kabudachi_net::executor::Report;
use kabudachi_net::messenger::Net;
use kabudachi_net::result_transport::{DeliveryError, ResultEvent};

use crate::support::deadline::within_deadline;
use crate::support::election::wait_until;
use crate::support::executor::FakeExecutor;
use crate::support::records::{ThreeVoters, holding, plain_with, submitted_through};
use crate::support::worker::poll_until;

const RESULT: &[u8] = b"the-result";

fn delivery(task: &TaskId, run: &TaskRunId) -> DeliverResult {
    DeliverResult {
        task_id: Some(task.clone().into()),
        task_run_id: Some(run.clone().into()),
        result_digest: Some(Digest::blake3(RESULT).into()),
        payload: RESULT.to_vec(),
    }
}

/// The next `count` result events about `task` that `client` takes.
async fn events_about(client: &Net, task: &TaskId, count: usize) -> Vec<ResultEvent> {
    let mut found = Vec::new();
    while found.len() < count {
        client.result_arrived().await;
        found.extend(
            client
                .take_result_events()
                .into_iter()
                .filter(|event| match event {
                    ResultEvent::Delivered { task: of, .. }
                    | ResultEvent::Certified { task: of, .. }
                    | ResultEvent::Concluded { task: of, .. } => of == task,
                }),
        );
    }
    found
}

/// The leader's answer to how `task` stands, asked until it is not
/// refused for now (a new leader still reconciling).
async fn outcome_of(client: &Net, leader: &WorkerId, task: &TaskId) -> OutcomeAnswer {
    loop {
        if let Ok(response) = client.query_outcome(leader.clone(), task.clone()).await {
            match response.result {
                Some(task_response::Result::Outcome(answer)) => return answer,
                Some(task_response::Result::Reject(reject))
                    if matches!(
                        TaskRejectReason::try_from(reject.reason),
                        Ok(TaskRejectReason::TaskRejectNotLeader
                            | TaskRejectReason::TaskRejectNotReady)
                    ) => {}
                other => panic!("not an outcome: {other:?}"),
            }
        }
        tokio::time::sleep(StdDuration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn results_reach_their_client_and_the_leader_tells_it_how_each_task_ended() {
    within_deadline(async {
        let (mut shard, client) = ThreeVoters::start().await;
        let mut executors: Vec<FakeExecutor> = (0..3)
            .map(|voter| {
                let (executor, endpoint) = FakeExecutor::new();
                shard.set_executor(voter, endpoint);
                executor
            })
            .collect();
        let leader = shard.drive_until_a_leader().await;
        let leader_id = shard.id(leader);
        shard.join_as_pending(&client, leader).await;
        let follower = shard.others(leader)[0];
        let nets = shard.nets.clone();
        let me = client.local_worker_id();
        let digest = Digest::blake3(RESULT);

        // The runner delivers, the client takes it (which acknowledges it),
        // the runner reports the run delivered, and the leader certifies it
        // to the client once a majority stored the report.
        executors[follower].grant(1);
        let delivered = shard
            .drive_until(submitted_through(
                &client,
                &leader_id,
                plain_with(b"delivered").with_client(me.clone()),
            ))
            .await;
        let (run, claim) = shard.drive_until(executors[follower].next_claim()).await;
        assert_eq!(
            claim.task.as_ref().and_then(|task| task.client()),
            Some(me.clone())
        );
        executors[follower].report(Report::Started(run.clone()));
        let (sent, taken) = shard
            .drive_until(async {
                tokio::join!(
                    nets[follower].deliver_result(
                        me.clone(),
                        delivery(&delivered, &run),
                        StdDuration::from_secs(5)
                    ),
                    events_about(&client, &delivered, 1)
                )
            })
            .await;
        assert_eq!(sent, Ok(()));
        assert_eq!(
            taken,
            vec![ResultEvent::Delivered {
                task: delivered.clone(),
                run: run.clone(),
                digest: digest.clone(),
                payload: RESULT.to_vec()
            }]
        );
        executors[follower].report(Report::Completed {
            run: run.clone(),
            digest: digest.clone(),
            delivered: true,
        });
        let certified = shard
            .drive_until(events_about(&client, &delivered, 1))
            .await;
        assert_eq!(
            certified,
            vec![ResultEvent::Certified {
                task: delivered.clone(),
                run: run.clone(),
                digest: digest.clone()
            }]
        );
        let record = shard
            .drive_until(nets[leader].get_record(delivered.clone()))
            .await
            .expect("the leader's shard holds the record");
        assert!(record.runs.last().unwrap().delivered);

        // A client cut off from the runner, not from the leader: the
        // delivery is never acknowledged, the run is reported undelivered,
        // and the client still hears it certified, with no result.
        let follower_id = shard.id(follower);
        client.block_peer(follower_id.clone());
        executors[follower].grant(1);
        let cut_off = shard
            .drive_until(submitted_through(
                &client,
                &leader_id,
                plain_with(b"cut-off").with_client(me.clone()),
            ))
            .await;
        let (run, _) = shard.drive_until(executors[follower].next_claim()).await;
        executors[follower].report(Report::Started(run.clone()));
        let sent = shard
            .drive_until(nets[follower].deliver_result(
                me.clone(),
                delivery(&cut_off, &run),
                StdDuration::from_millis(500),
            ))
            .await;
        assert_eq!(sent, Err(DeliveryError::Unreached));
        executors[follower].report(Report::Completed {
            run: run.clone(),
            digest: digest.clone(),
            delivered: false,
        });
        assert_eq!(
            shard.drive_until(events_about(&client, &cut_off, 1)).await,
            vec![ResultEvent::Certified {
                task: cut_off.clone(),
                run,
                digest: digest.clone()
            }]
        );
        client.unblock_peer(follower_id);

        // A task that ends without a result: an ephemeral task whose run is
        // lost is told to its client as lost.
        executors[follower].grant(1);
        let lost = shard
            .drive_until(submitted_through(
                &client,
                &leader_id,
                plain_with(b"lost").ephemeral().with_client(me.clone()),
            ))
            .await;
        let (run, _) = shard.drive_until(executors[follower].next_claim()).await;
        executors[follower].report(Report::Started(run.clone()));
        executors[follower].report(Report::Lost(run.clone()));
        let [ResultEvent::Concluded { outcome, .. }]: [ResultEvent; 1] = shard
            .drive_until(events_about(&client, &lost, 1))
            .await
            .try_into()
            .unwrap()
        else {
            panic!("the lost task was not concluded");
        };
        assert_eq!(
            (
                outcome.known,
                outcome.finished,
                outcome.state(),
                outcome.task_run_id.map(TaskRunId::from)
            ),
            (true, true, TaskRunState::Lost, Some(run))
        );

        // A run whose result was delivered is running when the leader dies,
        // and is reported completed only then: no leader takes the report, so
        // the next one adopts the run from the follower's reconcile page,
        // with `delivered` as the follower's ledger holds it, and certifies
        // it to the client.
        executors[follower].grant(1);
        let adopted = shard
            .drive_until(submitted_through(
                &client,
                &leader_id,
                plain_with(b"adopted").with_client(me.clone()),
            ))
            .await;
        let (adopted_run, _) = shard.drive_until(executors[follower].next_claim()).await;
        executors[follower].report(Report::Started(adopted_run.clone()));
        shard
            .drive_until(poll_until(
                "the leader stored the adopted run running",
                || holding(&nets, &adopted, &[TaskRunState::Running]) >= 2,
            ))
            .await;
        let (sent, _) = shard
            .drive_until(async {
                tokio::join!(
                    nets[follower].deliver_result(
                        me.clone(),
                        delivery(&adopted, &adopted_run),
                        StdDuration::from_secs(5)
                    ),
                    events_about(&client, &adopted, 1)
                )
            })
            .await;
        assert_eq!(sent, Ok(()));

        // The leader dies: a new one adopts that run, answers how a task
        // stands, idempotently, and says it does not know a task never stored.
        shard.kill(leader);
        executors[follower].report(Report::Completed {
            run: adopted_run.clone(),
            digest: digest.clone(),
            delivered: true,
        });
        // Waits on the voters' states alone: a new leader's reconciliation
        // lives in its driver, which a command run on it would restart.
        let watch = shard.watch();
        let next = shard
            .drive_until(async move {
                wait_until(|| watch.leader().is_some()).await;
                watch.leader().expect("a survivor leads")
            })
            .await;
        let next_id = shard.id(next);
        assert_eq!(
            shard.drive_until(events_about(&client, &adopted, 1)).await,
            vec![ResultEvent::Certified {
                task: adopted.clone(),
                run: adopted_run,
                digest: digest.clone()
            }]
        );
        let record = shard
            .drive_until(nets[next].get_record(adopted.clone()))
            .await
            .expect("the new leader's shard holds the adopted record");
        assert!(
            record.runs.last().unwrap().delivered,
            "adoption dropped `delivered`"
        );
        for _ in 0..2 {
            let answer = shard
                .drive_until(outcome_of(&client, &next_id, &delivered))
                .await;
            assert_eq!(
                (
                    answer.known,
                    answer.finished,
                    answer.state(),
                    answer.result_digest
                ),
                (
                    true,
                    true,
                    TaskRunState::Succeeded,
                    Some(digest.clone().into())
                )
            );
        }
        let unknown = shard
            .drive_until(outcome_of(
                &client,
                &next_id,
                &TaskId::new("never-submitted"),
            ))
            .await;
        assert!(!unknown.known);
    })
    .await;
}
