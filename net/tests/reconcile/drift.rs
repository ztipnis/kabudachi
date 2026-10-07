//! A leader that believes a worker holds a run the worker no longer holds as
//! running asks it again, and certifies what it reports, without the worker
//! resending anything.

use std::sync::Arc;

use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::generated::TaskRunState;
use kabudachi_core::protocol::ids::{TaskId, WorkerId};
use kabudachi_core::protocol::messages::task_response;
use kabudachi_net::messenger::Net;

use crate::support::election::wait_until;
use crate::support::records::{ThreeVoters, claimed_and_started, plain_with, submitted_through};

/// How many voters hold, as the newest run of `task`, one in `state`.
fn holders_at(nets: &[Arc<Net>], task: &TaskId, state: TaskRunState) -> usize {
    nets.iter()
        .filter_map(|net| net.held_records().get(task))
        .filter(|record| record.runs.last().map(|run| run.state()) == Some(state))
        .count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_uncertified_completion_is_certified_after_the_heartbeats_disagree() {
    let (mut shard, _client) = ThreeVoters::start().await;
    let (nets, watch) = (shard.nets.clone(), shard.watch());
    let ids: Vec<WorkerId> = (0..nets.len()).map(|voter| shard.id(voter)).collect();

    // One drive for the whole run: the late answers a leader takes in belong
    // to the driver that reconciled it.
    shard
        .drive_until(async move {
            wait_until(|| watch.leader().is_some()).await;
            let leader = watch.leader().expect("a voter leads");
            let (worker, bystander) = match leader {
                0 => (1, 2),
                1 => (0, 2),
                _ => (0, 1),
            };
            let task = submitted_through(&nets[worker], &ids[leader], plain_with(b"input")).await;
            let run = claimed_and_started(&nets[worker], &ids[leader], &task).await;

            // The completion goes to a voter that does not lead: the worker's
            // ledger says it completed, and its leader never hears.
            let misdirected = nets[worker]
                .complete(ids[bystander].clone(), run, Digest::blake3(b"out"))
                .await
                .expect("the bystander answered");
            assert!(
                !matches!(misdirected.result, Some(task_response::Result::Certified(_))),
                "only the leader certifies: {misdirected:?}"
            );

            wait_until(|| {
                assert_eq!(
                    watch.leader(),
                    Some(leader),
                    "the leader changed while the drift was awaited, so the test proves nothing"
                );
                holders_at(&nets, &task, TaskRunState::Succeeded) >= 2
            })
            .await;
        })
        .await;
}
