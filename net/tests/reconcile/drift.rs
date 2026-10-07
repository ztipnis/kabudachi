//! A leader that believes a worker holds a run the worker no longer holds as
//! running asks it again, and certifies what it reports, without the worker
//! resending anything, even when the worker's start of the run was answered
//! only after the run ended.

use std::sync::Arc;

use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::generated::TaskRunState;
use kabudachi_core::protocol::ids::{TaskId, WorkerId};
use kabudachi_core::protocol::messages::task_response;
use kabudachi_net::claimed_runs::HeldRun;
use kabudachi_net::messenger::Net;

use crate::support::deadline::within_deadline;
use crate::support::election::wait_until;
use crate::support::records::{
    ThreeVoters, claimed, claimed_and_started, plain_with, submitted_through,
};

/// How many voters hold, as the newest run of `task`, one in `state`.
fn holders_at(nets: &[Arc<Net>], task: &TaskId, state: TaskRunState) -> usize {
    nets.iter()
        .filter_map(|net| net.held_records().get(task))
        .filter(|record| record.runs.last().map(|run| run.state()) == Some(state))
        .count()
}

/// Ends a run on `worker` by reporting it to `bystander`, a voter that does
/// not lead, so that the leader never hears; returns the run's task. When
/// `start_while_ending`, the worker's report that the run started is still
/// unanswered when it ends the run.
async fn misdirected_completion(
    nets: &[Arc<Net>],
    ids: &[WorkerId],
    leader: usize,
    (worker, bystander): (usize, usize),
    start_while_ending: bool,
) -> TaskId {
    let task = submitted_through(&nets[worker], &ids[leader], plain_with(b"input")).await;
    let (run, misdirected) = if start_while_ending {
        let run = claimed(&nets[worker], &ids[leader], &task).await;
        // The start is polled first, so it is the first the leader hears;
        // its answer comes once a majority stored the start, after the
        // bystander has answered the completion.
        let (started, misdirected) = tokio::join!(
            nets[worker].report_started(ids[leader].clone(), run.clone()),
            nets[worker].complete(ids[bystander].clone(), run.clone(), Digest::blake3(b"out")),
        );
        assert!(
            matches!(
                started.expect("the leader answered").result,
                Some(task_response::Result::Started(_))
            ),
            "the leader acknowledged the start"
        );
        (run, misdirected)
    } else {
        let run = claimed_and_started(&nets[worker], &ids[leader], &task).await;
        // The completion goes to a voter that does not lead: the worker's
        // ledger says it completed, and its leader never hears.
        let misdirected = nets[worker]
            .complete(ids[bystander].clone(), run.clone(), Digest::blake3(b"out"))
            .await;
        (run, misdirected)
    };
    let misdirected = misdirected.expect("the bystander answered");
    assert!(
        !matches!(misdirected.result, Some(task_response::Result::Certified(_))),
        "only the leader certifies: {misdirected:?}"
    );
    assert_eq!(
        nets[worker].claimed_runs().get(&run).map(|held| held.state),
        Some(HeldRun::Completed { result_digest: Digest::blake3(b"out") }),
        "the worker's ledger holds the run completed, not running"
    );
    task
}

/// A shard whose two non-leading voters each end a run on the other, so that
/// the leader never hears, one of them while its start is unanswered: a
/// majority of the voters must come to hold both runs succeeded, the leader
/// having asked the workers again and certified what they reported.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_uncertified_completion_is_certified_after_the_heartbeats_disagree() {
    within_deadline(async {
        let (mut shard, _client) = ThreeVoters::start().await;
        let (nets, watch) = (shard.nets.clone(), shard.watch());
        let ids: Vec<WorkerId> = (0..nets.len()).map(|voter| shard.id(voter)).collect();

        // One drive for the whole test: the late answers a leader takes in belong
        // to the driver that reconciled it.
        shard
            .drive_until(async move {
                wait_until(|| watch.leader().is_some()).await;
                let leader = watch.leader().expect("a voter leads");
                let (first, second) = match leader {
                    0 => (1, 2),
                    1 => (0, 2),
                    _ => (0, 1),
                };
                let plain =
                    misdirected_completion(&nets, &ids, leader, (first, second), false).await;
                let late_start =
                    misdirected_completion(&nets, &ids, leader, (second, first), true).await;

                wait_until(|| {
                    assert_eq!(
                        watch.leader(),
                        Some(leader),
                        "the leader changed while the drift was awaited, so the test proves nothing"
                    );
                    [&plain, &late_start]
                        .iter()
                        .all(|task| holders_at(&nets, task, TaskRunState::Succeeded) >= 2)
                })
                .await;
            })
            .await;
    })
    .await
}
