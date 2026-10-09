//! Workers that run their claims through an executor, over real sockets.
//! The leader claims for its own executor from its own scheduler and decides
//! its own reports there. A follower claims from the leader and reports to
//! it. Each decision reaches the executor, or leaves the ledger, only once a
//! majority of the task's placement stored it. A run whose executor stops
//! before it ends is reported lost. A follower's cancelled run reaches its
//! executor through the leader's next ack.

use std::time::Duration as StdDuration;

use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::generated::TaskRunState;
use kabudachi_net::executor::{Report, Work};

use crate::support::deadline::within_deadline;
use crate::support::executor::FakeExecutor;
use crate::support::records::{ThreeVoters, holding, plain_with, submitted_through};
use crate::support::worker::poll_until;

const RESULT: &[u8] = b"the-result";
/// The majority of three, which every decision waits for.
const QUORUM: usize = 2;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_leader_runs_its_own_claims_and_a_follower_hears_of_cancels_and_reports_losses() {
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

        // Only the leader's executor offers a place: the leader claims for it
        // from its own scheduler, and hands the run over once a majority
        // stored the claim.
        executors[leader].grant(1);
        let own = shard
            .drive_until(submitted_through(&client, &leader_id, plain_with(b"own")))
            .await;
        let (run, _) = shard.drive_until(executors[leader].next_claim()).await;
        assert!(holding(&nets, &own, &[TaskRunState::Claimed]) >= QUORUM);
        executors[leader].report(Report::Started(run.clone()));
        executors[leader].report(Report::Completed {
            run: run.clone(),
            digest: Digest::blake3(RESULT),
        });
        let leader_net = nets[leader].clone();
        shard
            .drive_until(poll_until("the leader certified its own run", || {
                holding(&nets, &own, &[TaskRunState::Succeeded]) >= QUORUM
                    && leader_net.claimed_runs().get(&run).is_none()
            }))
            .await;

        // A task the leader submits and cancels through its own network,
        // which its own driver answers like any other request, is one its
        // own executor runs: the executor is told to stop the body.
        executors[leader].grant(1);
        let cancelled = shard
            .drive_until(submitted_through(&leader_net, &leader_id, plain_with(b"cancelled")))
            .await;
        let (run, _) = shard.drive_until(executors[leader].next_claim()).await;
        executors[leader].report(Report::Started(run.clone()));
        shard
            .drive_until(poll_until("the leader stored its run running", || {
                holding(&nets, &cancelled, &[TaskRunState::Running]) >= QUORUM
            }))
            .await;
        shard
            .drive_until(leader_net.cancel(leader_id.clone(), cancelled))
            .await
            .expect("the leader's own driver answered");
        assert_eq!(shard.drive_until(executors[leader].next_work()).await, Work::Cancel(run));

        // A run a follower runs is cancelled: the leader's next ack to the
        // follower lists it, and the follower's executor is told to stop the
        // body within a few heartbeat intervals.
        executors[follower].grant(1);
        let remote = shard
            .drive_until(submitted_through(&client, &leader_id, plain_with(b"remote")))
            .await;
        let (run, _) = shard.drive_until(executors[follower].next_claim()).await;
        executors[follower].report(Report::Started(run.clone()));
        shard
            .drive_until(poll_until("the leader stored the follower's run running", || {
                holding(&nets, &remote, &[TaskRunState::Running]) >= QUORUM
            }))
            .await;
        shard
            .drive_until(client.cancel(leader_id.clone(), remote))
            .await
            .expect("the leader answered");
        let told = shard
            .drive_until(tokio::time::timeout(
                StdDuration::from_millis(500),
                executors[follower].next_work(),
            ))
            .await
            .expect("the follower heard of the cancel within five heartbeat intervals");
        assert_eq!(told, Work::Cancel(run));

        // Only a follower's executor offers a place now: it claims from the
        // leader. The process running the body dies: the run is lost and
        // replayed.
        executors[follower].grant(1);
        let lost = shard
            .drive_until(submitted_through(&client, &leader_id, plain_with(b"lost")))
            .await;
        let (run, _) = shard.drive_until(executors[follower].next_claim()).await;
        executors[follower].report(Report::Started(run.clone()));
        executors[follower].report(Report::Lost(run));
        shard
            .drive_until(poll_until("the leader decided the lost run and queued its replay", || {
                holding(&nets, &lost, &[TaskRunState::Lost, TaskRunState::Queued]) >= QUORUM
            }))
            .await;

        // The follower claims the replay, and its executor stops with the run
        // started and not ended: no outcome will come, so the run is reported
        // lost and replayed again.
        executors[follower].grant(1);
        let (run, _) = shard.drive_until(executors[follower].next_claim()).await;
        executors[follower].report(Report::Started(run));
        drop(executors.remove(follower));
        shard
            .drive_until(poll_until("the leader replays the run its executor abandoned", || {
                holding(&nets, &lost, &[TaskRunState::Lost, TaskRunState::Lost, TaskRunState::Queued]) >= QUORUM
            }))
            .await;

        // The leader's executor claims that replay, then stops reading work
        // though it could still report: no run it was handed will be run, so
        // the run is reported lost and replayed once more.
        let own_executor = if follower < leader { leader - 1 } else { leader };
        executors[own_executor].grant(1);
        let (run, _) = shard.drive_until(executors[own_executor].next_claim()).await;
        executors[own_executor].report(Report::Started(run.clone()));
        executors[own_executor].stop_taking_work();
        shard
            .drive_until(poll_until("the leader replays the run its executor no longer reads", || {
                holding(&nets, &lost, &[
                    TaskRunState::Lost,
                    TaskRunState::Lost,
                    TaskRunState::Lost,
                    TaskRunState::Queued,
                ]) >= QUORUM
                    && leader_net.claimed_runs().get(&run).is_none()
            }))
            .await;
    })
    .await
}
