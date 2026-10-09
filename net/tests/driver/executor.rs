//! A worker's driver and its executor over real sockets: the driver claims
//! nothing for its executor until a leader has vouched for hearing the
//! worker, and tells the executor to abort its runs before any other leader
//! can replay them, including a leader that loses its quorum mid-run.

use std::collections::BTreeMap;
use std::time::Duration as StdDuration;
use std::time::Instant as StdInstant;

use kabudachi_core::election::{ElectionTimings, Entry, Identity, Input, WorkerNode};
use kabudachi_core::protocol::ids::{IncarnationId, ShardId, TaskId};
use kabudachi_core::protocol::generated::TaskRunState;
use kabudachi_core::protocol::messages::election_message;
use kabudachi_core::time::{Duration, RealClock};
use kabudachi_net::driver::{DriverConfig, run_driver};
use kabudachi_net::executor::{Report, Work};
use tokio::sync::watch;

use crate::support::deadline::within_deadline;
use crate::support::executor::FakeExecutor;
use crate::support::net::{driven_scheduler, listening_net, pointer_to, wait_until_registered};
use crate::support::records::{
    RECONNECT_TIMEOUT_MS, SUSPECT_TIMEOUT_MS, ThreeVoters, holding, plain_with, submitted_through,
};
use crate::support::worker::poll_until;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_worker_claims_nothing_for_its_executor_before_a_leader_vouches_for_hearing_it() {
    within_deadline(async {
        let clock = RealClock::new();
        // A leader that hears the joiner's heartbeats and never acks them.
        let (leader_net, leader_address) = listening_net().await;
        let leader = leader_net.local_worker_id();
        let (joiner_net, _) = listening_net().await;
        joiner_net.dial(leader_address.clone());
        wait_until_registered(&joiner_net, &leader).await;
        let joiner = joiner_net.local_worker_id();
        let (mut node, first) = WorkerNode::start(
            Identity {
                id: joiner.clone(),
                incarnation: IncarnationId::new("joiner-incarnation-0"),
                shard: ShardId::new("shard-1"),
                timings: ElectionTimings::new(
                    Duration::from_millis(2_000),
                    Duration::from_millis(100),
                )
                .with_roll_call_deadline(Duration::from_millis(100)),
            },
            Entry::Joining(pointer_to(&leader, &leader_address)),
            clock,
            None,
        );
        let mut scheduler = driven_scheduler(clock);
        let (mut executor, mut endpoint) = FakeExecutor::new();
        executor.grant(1);
        let (seen, watched) = watch::channel((None, false));
        let driven = run_driver(
            &mut node,
            first,
            &joiner_net,
            &mut scheduler,
            clock,
            None,
            Some(&mut endpoint),
            DriverConfig::default(),
            move |node, _, _| {
                let leader = node.known_leader().map(|(leader, _)| leader);
                let _ = seen.send((leader, node.has_contact_floor()));
            },
        );
        tokio::select! {
            _ = driven => unreachable!("this test never drains the joiner, so its driver never returns"),
            () = async {
                // Three heartbeats: all the while the joiner follows the
                // leader it was pointed at, with room to run a task.
                let mut heartbeats = 0;
                while heartbeats < 3 {
                    for input in leader_net.take_inputs() {
                        if let Input::Message { from, message } = input
                            && from == joiner
                            && matches!(
                                message.message().payload,
                                Some(election_message::Payload::Heartbeat(_))
                            )
                        {
                            heartbeats += 1;
                        }
                    }
                    leader_net.wait_for_arrival().await;
                }
                assert_eq!(
                    *watched.borrow(),
                    (Some(leader.clone()), false),
                    "the joiner follows a leader that has not vouched for hearing it"
                );
                assert!(
                    leader_net.poll_claim_requests().is_empty(),
                    "the joiner asked its leader for work"
                );
                executor.expect_no_work_for(StdDuration::from_millis(50)).await;
            } => {}
        }
    })
    .await
}

/// A run's reconnect timeout of its own, longer than the shard's.
const OWN_RECONNECT_MS: u64 = 3_000;

// A leader cut off from both its followers mid-run loses its quorum, and its
// own runs with it. Each run is aborted on a deadline of its own reconnect
// timeout, counted from the same lost contact, and the followers' new leader
// replays the run at the shard's timeout only after that run's deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_leader_that_loses_its_quorum_mid_run_aborts_each_run_by_its_own_deadline() {
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
        let nets = shard.nets.clone();
        executors[leader].grant(2);
        let task = shard
            .drive_until(submitted_through(&client, &leader_id, plain_with(b"cut-off")))
            .await;
        let patient = shard
            .drive_until(submitted_through(
                &client,
                &leader_id,
                plain_with(b"patient").with_reconnect_timeout(Duration::from_millis(OWN_RECONNECT_MS)),
            ))
            .await;
        let mut runs = BTreeMap::new();
        for _ in 0..2 {
            let (run, claim) = shard.drive_until(executors[leader].next_claim()).await;
            executors[leader].report(Report::Started(run.clone()));
            let claimed = claim
                .task
                .as_ref()
                .and_then(|task| task.task_id.clone())
                .map(TaskId::from)
                .expect("a granted claim names its task");
            runs.insert(claimed, run);
        }
        shard
            .drive_until(poll_until("a majority stored both runs running", || {
                holding(&nets, &task, &[TaskRunState::Running]) >= 2
                    && holding(&nets, &patient, &[TaskRunState::Running]) >= 2
            }))
            .await;

        for other in shard.others(leader) {
            nets[leader].block_peer(shard.id(other));
            nets[other].block_peer(leader_id.clone());
        }
        let mut deadlines = BTreeMap::new();
        while deadlines.len() < 2 {
            match shard.drive_until(executors[leader].next_work()).await {
                Work::Abort { run, deadline } => {
                    deadlines.entry(run).or_insert(deadline);
                }
                other => panic!("expected the runs aborted, got {other:?}"),
            }
        }
        let deadline = deadlines[&runs[&task]];
        assert!(
            deadline > StdInstant::now(),
            "the executor has time to cancel the body before it kills it"
        );
        // Both deadlines count from the same lost contact, so they differ by
        // exactly the difference of their spans, each less its drift share.
        let less_drift = |ms: u64| ms - ms.div_ceil(ElectionTimings::DEFAULT_CLOCK_DRIFT_DIVISOR);
        assert_eq!(
            deadlines[&runs[&patient]] - deadline,
            StdDuration::from_millis(
                less_drift(SUSPECT_TIMEOUT_MS + OWN_RECONNECT_MS)
                    - less_drift(SUSPECT_TIMEOUT_MS + RECONNECT_TIMEOUT_MS)
            ),
            "each run is aborted by its own reconnect timeout"
        );

        let followers: Vec<_> = shard.others(leader).into_iter().map(|other| nets[other].clone()).collect();
        shard
            .drive_until(poll_until("the followers' new leader replayed the run", || {
                followers.iter().any(|net| {
                    net.held_records()
                        .get(&task)
                        .and_then(|record| record.runs.first().map(|run| run.state()))
                        == Some(TaskRunState::Lost)
                })
            }))
            .await;
        assert!(
            StdInstant::now() >= deadline,
            "the run was replayed before the deadline by which its first worker had to abort it"
        );
    })
    .await
}
