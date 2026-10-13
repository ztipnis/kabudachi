//! Workers that run their claims through an executor, over real sockets.
//! The leader claims for its own executor from its own scheduler and decides
//! its own reports there. A follower claims from the leader and reports to
//! it. Each decision reaches the executor, or leaves the ledger, only once a
//! majority of the task's placement stored it. A run whose executor stops
//! before it ends is reported lost. A follower's cancelled run reaches its
//! executor through the leader's next ack.

use std::collections::BTreeMap;
use std::time::Duration as StdDuration;

use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::generated::TaskRunState;
use kabudachi_core::scheduler::TraceContext;
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
        executors[leader].report(Report::Started { run: run.clone(), execution_version: None });
        executors[leader].report(Report::Completed {
            run: run.clone(),
            digest: Digest::blake3(RESULT),
            delivered: false,
            continuation: None,
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
        executors[leader].report(Report::Started { run: run.clone(), execution_version: None });
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
        executors[follower].report(Report::Started { run: run.clone(), execution_version: None });
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
        executors[follower].report(Report::Started { run: run.clone(), execution_version: None });
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
        executors[follower].report(Report::Started { run, execution_version: None });
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
        executors[own_executor].report(Report::Started { run: run.clone(), execution_version: None });
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_run_that_fails_before_its_body_starts_ends_failed_at_the_leader() {
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

        // A follower is handed a run and fails it without ever starting it,
        // as an executor does whose claim cannot be read or whose input
        // cannot be fetched.
        executors[follower].grant(1);
        let task = shard
            .drive_until(submitted_through(&client, &leader_id, plain_with(b"unstartable")))
            .await;
        let (run, _) = shard.drive_until(executors[follower].next_claim()).await;
        executors[follower].report(Report::Failed {
            run: run.clone(),
            reason: "PayloadUnavailable".into(),
            message: String::new(),
        });
        let follower_net = nets[follower].clone();
        shard
            .drive_until(poll_until("the leader recorded the failure", || {
                holding(&nets, &task, &[TaskRunState::Failed]) >= QUORUM
                    && follower_net.claimed_runs().get(&run).is_none()
            }))
            .await;
    })
    .await
}

/// A follower says how many places it has free in its heartbeats, and the
/// leader keeps the latest per member, its own included. A task's trace
/// context reaches the follower's claim over the wire unchanged, and the
/// follower's failure reaches the record with its message and run times,
/// stored by a quorum.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn members_report_their_free_places_and_a_runs_trace_context_and_failure_message_cross_the_network() {
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
        let follower_id = shard.id(follower);
        let nets = shard.nets.clone();

        executors[follower].grant(2);
        executors[leader].grant(3);
        let mut seen = BTreeMap::new();
        for _ in 0..200 {
            seen = shard.with(leader, |node, _| node.member_capacities()).await;
            if seen.get(&follower_id) == Some(&2) && seen.get(&leader_id) == Some(&3) {
                break;
            }
            shard.drive_until(tokio::time::sleep(StdDuration::from_millis(20))).await;
        }
        assert_eq!((seen.get(&follower_id), seen.get(&leader_id)), (Some(&2), Some(&3)));

        let trace = TraceContext {
            trace_parent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into(),
            trace_state: "vendor=opaque".into(),
            baggage: "tenant=acme".into(),
        };
        // The leader's executor takes no more: only the follower can claim.
        executors[leader].stop_taking_work();
        let failing = shard
            .drive_until(submitted_through(
                &client,
                &leader_id,
                plain_with(b"fails").with_trace_context(trace.clone()),
            ))
            .await;
        let (run, claim) = shard.drive_until(executors[follower].next_claim()).await;
        let task = claim.task.expect("a claim carries its task");
        assert_eq!(
            (task.trace_parent, task.trace_state, task.baggage),
            (trace.trace_parent, trace.trace_state, trace.baggage)
        );
        executors[follower].report(Report::Started { run: run.clone(), execution_version: None });
        executors[follower].report(Report::Failed {
            run,
            reason: "ValueError".into(),
            message: "card declined".into(),
        });
        shard
            .drive_until(poll_until("a quorum stored the failure with its message and times", || {
                nets.iter()
                    .filter_map(|net| net.held_records().get(&failing))
                    .filter(|record| {
                        record.runs.first().is_some_and(|run| {
                            run.failure_message == "card declined"
                                && run.failure_kind == "ValueError"
                                && run.claimed_at.is_some()
                                && run.started_at.is_some_and(|started| {
                                    started.unix_millis >= run.claimed_at.unwrap().unix_millis
                                })
                        })
                    })
                    .count()
                    >= QUORUM
            }))
            .await;
        // The follower's next heartbeats say the claim took one of its two
        // places.
        let mut after = None;
        for _ in 0..200 {
            let seen = shard.with(leader, |node, _| node.member_capacities()).await;
            after = seen.get(&follower_id).copied();
            if after == Some(1) {
                break;
            }
            shard.drive_until(tokio::time::sleep(StdDuration::from_millis(20))).await;
        }
        assert_eq!(after, Some(1), "the claim took one of the follower's two places");
        // It stays at one across several heartbeat periods: the run it holds
        // is counted once, whatever the claim is doing, so the figure never
        // dips while the claim and its report are in flight.
        let mut sampled = Vec::new();
        for _ in 0..30 {
            let seen = shard.with(leader, |node, _| node.member_capacities()).await;
            sampled.push(seen.get(&follower_id).copied());
            shard.drive_until(tokio::time::sleep(StdDuration::from_millis(20))).await;
        }
        assert_eq!(sampled, vec![Some(1); 30], "the follower's free places never dipped");
    })
    .await;
}
