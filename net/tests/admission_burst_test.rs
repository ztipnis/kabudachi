//! An admission burst over real sockets (ADR-0001 decision 9), measured: a
//! burst of workers joins a running shard at once, claims work as pending
//! members, and is admitted in two commit rounds. The test prints how long
//! the commit took and how much traffic went through the leader.
//!
//! The joiners start through their entry point
//! (`kabudachi_net::worker::Worker`). The leader is a node the test drives
//! itself, so that it can hold the leader's driver while the burst joins:
//! the joiners' connections, heartbeats and claims queue on the leader's
//! `Net` and reach its node together, as a burst of workers started at once
//! on separate hosts would. Twenty in-process bootstraps on one loaded host
//! otherwise spread over longer than a commit round takes, and the number of
//! rounds would measure the host's scheduling, not the protocol.

mod support;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration as StdDuration, Instant as StdInstant};

use kabudachi_core::configuration::Configuration;
use kabudachi_core::election::{ElectionTimings, Output, WorkerNode};
use kabudachi_core::protocol::ids::{IncarnationId, TaskDefinitionId, TaskId, Uuid7Ids};
use kabudachi_core::protocol::messages::election_message::Payload;
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::messages::{ClaimResponse, claim_response};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::{Scheduler, Submission};
use kabudachi_core::time::{Duration, RealClock};
use kabudachi_net::driver::run_driver;
use kabudachi_net::messenger::{ClaimFailure, Net};
use kabudachi_net::swarm::build_swarm;
use libp2p::futures::future::join_all;
use libp2p::identity;
use tokio::sync::watch;
use tokio::time::timeout;

use support::membership::{LeaderLog, is_committed_with, shard, spawn_member};
use support::worker::RunningWorker;

/// How many workers join at once.
const BURST: usize = 20;

/// Backstop for each thing the test waits on.
const WAIT: StdDuration = StdDuration::from_secs(30);

/// The leader's driver is held for less than this, well inside the
/// suspicion timeout, so the one voter besides the leader never suspects it.
const MAX_HOLD: StdDuration = StdDuration::from_millis(3_000);

fn timings() -> ElectionTimings {
    ElectionTimings::new(Duration::from_millis(4_000), Duration::from_millis(100))
        .with_roll_call_deadline(Duration::from_millis(400))
}

type TestNode = WorkerNode<RealClock>;

/// The leader: a node the test drives over its own `Net`, and its
/// scheduler.
struct Leader {
    node: TestNode,
    net: Arc<Net>,
    scheduler: Scheduler<RealClock, Uuid7Ids>,
    clock: RealClock,
    /// Where each term the leader leads is recorded, beside the joiners'.
    leaders: LeaderLog,
}

/// What the leader did while driven: each joint configuration it announced
/// on an ack, with when it first did.
#[derive(Default)]
struct LeaderHistory {
    joint_announced: Vec<(StdInstant, Configuration)>,
}

impl Leader {
    /// Drives the leader until its node shows something `done` accepts,
    /// recording what it announces in `history`.
    async fn drive_until(&mut self, history: &mut LeaderHistory, done: impl Fn(&TestNode) -> bool) {
        self.drive_while(history, done, async {}).await;
    }

    /// Drives the leader until its node shows something `done` accepts and
    /// then, still driving it, until `then` completes; returns what `then`
    /// returned. Records what the leader announces in `history`.
    async fn drive_while<T>(
        &mut self,
        history: &mut LeaderHistory,
        done: impl Fn(&TestNode) -> bool,
        then: impl Future<Output = T>,
    ) -> T {
        let (finished, mut is_finished) = watch::channel(false);
        let Leader {
            node,
            net,
            scheduler,
            clock,
            leaders,
        } = self;
        let observe = |node: &TestNode, outputs: &[Output]| {
            let now = StdInstant::now();
            for output in outputs {
                if let Output::Send { message, .. } = output
                    && let Some(Payload::HeartbeatAck(ack)) = &message.payload
                {
                    let configuration = ack.configuration();
                    if configuration.is_joint()
                        && !history.joint_announced.iter().any(|(_, announced)| {
                            announced.generation() == configuration.generation()
                        })
                    {
                        history.joint_announced.push((now, configuration));
                    }
                }
            }
            if node.state() == WorkerState::Leader {
                leaders.record(node.term(), &net.local_worker_id());
            }
            if done(node) {
                let _ = finished.send(true);
            }
        };
        timeout(WAIT, async {
            tokio::select! {
                _ = run_driver(node, net, scheduler, *clock, None, observe) => {
                    unreachable!("run_driver never returns")
                }
                returned = async {
                    let _ = is_finished.wait_for(|finished| *finished).await;
                    then.await
                } => returned,
            }
        })
        .await
        .expect("the leader got there within the timeout")
    }
}

fn leads_committed(node: &TestNode, voter_count: usize) -> bool {
    node.state() == WorkerState::Leader
        && node
            .configuration()
            .is_some_and(|configuration| is_committed_with(configuration, voter_count))
}

fn claimed_tasks(response: Result<ClaimResponse, ClaimFailure>) -> Vec<TaskId> {
    match response.expect("the leader answered").result {
        Some(claim_response::Result::Batch(batch)) => batch
            .claims
            .into_iter()
            .map(|claim| claim.task.expect("a claim carries its Task").task_id())
            .collect(),
        other => panic!("expected a batch of claims, got {other:?}"),
    }
}

// A genesis leader and one voter it admitted; twenty workers then join at
// once through that voter as their seed. Each is a pending member as soon
// as it has joined, and claims a task from the leader it names before any
// admission. The leader admits the burst in two batches (ADR-0001 decision
// 9: one change at a time): the first takes the joiners that had confirmed
// its ack when it started, and every joiner confirming meanwhile waits for
// that commit, which starts the second. Every joiner ends a voter of a
// committed configuration of all twenty-two, and every claim got a
// different task.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_burst_of_twenty_joiners_claims_as_pending_members_and_is_admitted_in_two_rounds() {
    let clock = RealClock::new();
    let net = Arc::new(Net::new(build_swarm(identity::Keypair::generate_ed25519())));
    let leader_net = Arc::clone(&net);
    let leader_address = net.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()).await;
    let leader_id = net.local_worker_id();
    let mut scheduler = Scheduler::new(clock, Uuid7Ids);
    for _ in 0..BURST {
        scheduler
            .submit(Submission::new(
                TaskDefinitionId::new("demo.task"),
                1,
                Vec::new(),
                "default",
            ))
            .expect("submitting with no memory limits configured never fails");
    }
    let leaders = LeaderLog::default();
    let mut leader = Leader {
        node: WorkerNode::genesis(
            leader_id.clone(),
            IncarnationId::new(leader_id.as_str()),
            shard(),
            clock,
            0,
            None,
            timings(),
        ),
        net,
        scheduler,
        clock,
        leaders: leaders.clone(),
    };
    let mut history = LeaderHistory::default();

    let seed = spawn_member(timings(), vec![leader_address], &leaders).await;
    leader
        .drive_until(&mut history, |node| leads_committed(node, 2))
        .await;

    // Held: the burst joins through the seed and queues on the leader's Net.
    let traffic_before = leader_net.traffic();
    let held_at = StdInstant::now();
    let mut joiners: Vec<RunningWorker> =
        join_all((0..BURST).map(|_| spawn_member(timings(), vec![seed.address.clone()], &leaders)))
            .await;
    join_all(joiners.iter_mut().map(|joiner| {
        joiner.wait_until(|seen| seen.pending && seen.leader.as_ref() == Some(&leader_id))
    }))
    .await;
    // Each asks while still pending: nothing can admit it while the leader
    // is held, and the hold lasts until every claim has reached the leader.
    let claims: Vec<_> = joiners
        .iter()
        .map(|joiner| {
            let net = joiner.net.clone();
            tokio::spawn(async move { net.claim_oldest(1).await })
        })
        .collect();
    timeout(WAIT, async {
        while !joiners
            .iter()
            .all(|joiner| leader_net.peer_addresses().contains_key(&joiner.id))
            || (leader_net.traffic() - traffic_before).claim_requests_received < BURST as u64
        {
            tokio::time::sleep(StdDuration::from_millis(5)).await;
        }
    })
    .await
    .expect("every joiner connected to the held leader and claimed within the timeout");
    // One heartbeat interval more, so every joiner's first heartbeat is
    // queued too.
    tokio::time::sleep(StdDuration::from_millis(100)).await;
    let held_for = held_at.elapsed();
    assert!(
        held_for < MAX_HOLD,
        "the burst joined within {MAX_HOLD:?} (took {held_for:?}): the host is too loaded for \
         the seed's suspicion timeout"
    );

    // Released: the burst reaches the leader's node at once.
    let rounds_before = history.joint_announced.len();
    let released_at = StdInstant::now();
    let (committed_after, traffic, claimed) = leader
        .drive_while(
            &mut history,
            |node| leads_committed(node, BURST + 2),
            async {
                let (committed_after, traffic) = (released_at.elapsed(), leader_net.traffic());
                let mut claimed = BTreeSet::new();
                for (joiner, claim) in joiners.iter_mut().zip(claims) {
                    joiner
                        .wait_until(|seen| {
                            seen.configuration.as_ref().is_some_and(|configuration| {
                                is_committed_with(configuration, BURST + 2)
                                    && seen.is_voter_of(configuration)
                            })
                        })
                        .await;
                    let tasks = claimed_tasks(claim.await.expect("the claim task ran"));
                    assert_eq!(tasks.len(), 1, "each pending member's claim got one task");
                    claimed.extend(tasks);
                }
                (committed_after, traffic - traffic_before, claimed)
            },
        )
        .await;
    assert_eq!(claimed.len(), BURST, "no task was claimed twice");
    leaders.assert_one_leader_per_term();

    let rounds = &history.joint_announced[rounds_before..];
    let starts: Vec<StdDuration> = rounds.iter().map(|(at, _)| *at - released_at).collect();
    eprintln!(
        "admission burst of {BURST}: held {held_for:?}; batches started at {starts:?} after \
         release (each commit starts the next batch), the last committed {committed_after:?} \
         after release; through the leader from the hold to that commit: {traffic:?} ({} in all, {:.1} per joiner)",
        traffic.total(),
        traffic.total() as f64 / BURST as f64
    );

    assert_eq!(
        rounds.len(),
        2,
        "the burst was admitted in two batches: {:?}",
        rounds
            .iter()
            .map(|(at, configuration)| (*at - released_at, configuration))
            .collect::<Vec<_>>()
    );
}
