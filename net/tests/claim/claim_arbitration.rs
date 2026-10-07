//! Claim arbitration (`/kabudachi/claim/1`) end to end over
//! real sockets: a genesis leader and two joiners, every one a driven
//! `WorkerNode` with its own `Net` and `Scheduler`.
//!
//! The joiners are pending members: they claim work
//! as soon as JOIN completes, before any admission makes them voters. Each
//! asks the leader its own node names: the test passes on what the node
//! reports, and never picks a leader for a claimant. The leader decides from the leadership grant its election handed
//! its scheduler, so a worker holding no grant refuses every claim.
//!
//! The genesis leader is driven until it leads, and only then is its scheduler
//! given its tasks: only a leader records a submission.


use std::time::Duration as StdDuration;

use crate::support::deadline::within_deadline;
use crate::support::election::{drive_until_leading, due_now};
use kabudachi_core::coordination_authority::RecoveryEpoch;
use kabudachi_core::election::{ElectionTimings, Entry, Identity, WorkerNode};
use kabudachi_core::protocol::ids::{
    IncarnationId, ShardId, TaskDefinitionId, TaskId, Uuid7Ids, WorkerId,
};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::messages::{ClaimRejectReason, ClaimResponse, claim_response};
use kabudachi_core::scheduler::{MAX_SUBMISSION_BYTES, Scheduler, Submission};
use kabudachi_core::task_record::RecordOutbox;
use kabudachi_core::time::{Duration, RealClock};
use kabudachi_net::driver::{DriverConfig, run_driver};
use kabudachi_net::claim::ClaimFailure;
use kabudachi_net::messenger::Net;
use libp2p::Multiaddr;
use tokio::sync::watch;
use tokio::time::timeout;

use crate::support::net::{ask_until_pointed_at_a_leader, driven_scheduler};

const SHARD: &str = "shard-1";

/// How long a node goes without leader contact before it suspects its
/// leader; the genesis leader waits this out before its lone roll call. Long
/// enough that a leader whose threads a loaded host starves for a moment
/// keeps its lease, which lasts a suspicion timeout less its drift share.
const SUSPECT_TIMEOUT_MS: u64 = 2000;

/// How often a follower heartbeats its leader: well inside the suspicion
/// timeout.
const HEARTBEAT_INTERVAL_MS: u64 = 10;

/// How long a roll call runs: well above a loopback round trip.
const ROLL_CALL_DEADLINE_MS: u64 = 100;

const JOIN_TIMEOUT: StdDuration = StdDuration::from_secs(10);

const TEST_TIMEOUT: StdDuration = StdDuration::from_secs(20);

fn timings() -> ElectionTimings {
    ElectionTimings::new(
        Duration::from_millis(SUSPECT_TIMEOUT_MS),
        Duration::from_millis(HEARTBEAT_INTERVAL_MS),
    )
    .with_roll_call_deadline(Duration::from_millis(ROLL_CALL_DEADLINE_MS))
}

type TestNode = WorkerNode<RealClock>;

fn submit(scheduler: &mut Scheduler<RealClock, Uuid7Ids, RecordOutbox>, payload: Vec<u8>) -> TaskId {
    scheduler
        .submit(Submission::new(
            TaskDefinitionId::new("demo.task"),
            1,
            payload,
            "default",
        ))
        .expect("submitting with no memory limits configured never fails")
}

/// Joins the shard through `seed`, then drives the joiner's node for ever,
/// reporting the leader it names to `known_leader`.
async fn join_and_drive(
    net: &Net,
    seed: &Multiaddr,
    clock: RealClock,
    known_leader: &watch::Sender<Option<WorkerId>>,
) {
    let pointer = ask_until_pointed_at_a_leader(net, std::slice::from_ref(seed), JOIN_TIMEOUT).await;
    let (mut node, first): (TestNode, _) = WorkerNode::start(
        Identity {
            id: net.local_worker_id(),
            incarnation: IncarnationId::new(format!("{}-incarnation-0", net.local_worker_id().as_str())),
            shard: ShardId::new(SHARD),
            timings: timings(),
        },
        Entry::Joining(pointer),
        clock,
        None,
    );
    let mut scheduler = driven_scheduler(clock);
    run_driver(&mut node, first, net, &mut scheduler, clock, None, DriverConfig::default(), |node, _, _| {
        let _ = known_leader.send(node.known_leader().map(|(leader, _)| leader));
    })
    .await;
}

/// Asks until the leader's roster holds the asker. A joiner learns its leader
/// from the join answer, before the leader has heard its first heartbeat, so
/// a claim made at once can be refused as `NOT_MEMBER` for a moment. That
/// refusal changes nothing on the leader, so asking again is safe.
async fn until_a_member<F, Fut>(mut ask: F) -> Result<ClaimResponse, ClaimFailure>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<ClaimResponse, ClaimFailure>>,
{
    loop {
        let response = ask().await;
        let not_member = matches!(
            &response,
            Ok(ClaimResponse { result: Some(claim_response::Result::Reject(reject)) })
                if reject.reason == ClaimRejectReason::ClaimRejectNotMember as i32
        );
        if !not_member {
            return response;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_members_claim_from_the_leader_their_nodes_name() {
    within_deadline(async {
        let net_a = Net::for_shard(ShardId::new(SHARD), None);
        let net_b = Net::for_shard(ShardId::new(SHARD), None);
        let net_c = Net::for_shard(ShardId::new(SHARD), None);
        let worker_a = net_a.local_worker_id();
        let seed = timeout(
            TEST_TIMEOUT,
            net_a.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
        )
        .await
        .expect("net_a produced a listen address within the timeout");
        let clock = RealClock::new();
        let mut node_a: TestNode = WorkerNode::start(
            Identity {
                id: worker_a.clone(),
                incarnation: IncarnationId::new("a-incarnation-0"),
                shard: ShardId::new(SHARD),
                timings: timings(),
            },
            Entry::Founding {
                recovery_epoch: RecoveryEpoch::new(0, 0),
                registered_at: None,
            },
            clock,
            None,
        )
        .0;
        let mut scheduler_a = driven_scheduler(clock);
        timeout(
            TEST_TIMEOUT,
            drive_until_leading(&mut node_a, due_now(&clock), &net_a, &mut scheduler_a, clock),
        )
        .await
        .expect("the genesis leader led within the timeout");
        let small = || b"payload".to_vec();
        let taken = submit(&mut scheduler_a, small());
        let oldest = [
            submit(&mut scheduler_a, small()),
            submit(&mut scheduler_a, small()),
        ];
        // Two of the largest task the scheduler accepts, which cannot share a
        // claim message: each goes out in a batch of its own.
        let largest = MAX_SUBMISSION_BYTES as usize - "demo.task".len() - "default".len();
        let huge = [
            submit(&mut scheduler_a, vec![7; largest]),
            submit(&mut scheduler_a, vec![7; largest]),
        ];

        let (b_leader, mut b_knows) = watch::channel(None);
        let (c_leader, mut c_knows) = watch::channel(None);

        let (claimed, refused, batch, huge_batches) = timeout(TEST_TIMEOUT, async {
            tokio::select! {
                _ = run_driver(&mut node_a, due_now(&clock), &net_a, &mut scheduler_a, clock, None, DriverConfig::default(), |_, _, _| {}) => {
                    unreachable!("this test never drains a node, so its driver never returns")
                }
                () = join_and_drive(&net_b, &seed, clock, &b_leader) => {
                    unreachable!("this test never drains a node, so its driver never returns")
                }
                () = join_and_drive(&net_c, &seed, clock, &c_leader) => {
                    unreachable!("this test never drains a node, so its driver never returns")
                }
                claims = async {
                    let mut named = Vec::new();
                    for knows in [&mut b_knows, &mut c_knows] {
                        let leader = knows
                            .wait_for(|leader| leader.as_ref() == Some(&worker_a))
                            .await
                            .expect("the joiner's driver is still running")
                            .clone()
                            .expect("the joiner's node names a leader");
                        named.push(leader);
                    }
                    let (b_names, c_names) = (named[0].clone(), named[1].clone());
                    let claimed = until_a_member(|| net_b.request_claim(b_names.clone(), taken.clone())).await;
                    let refused = until_a_member(|| net_c.request_claim(c_names.clone(), taken.clone())).await;
                    let batch = until_a_member(|| net_c.claim_oldest(c_names.clone(), 2)).await;
                    let first_huge = until_a_member(|| net_c.claim_oldest(c_names.clone(), 2)).await;
                    let second_huge = until_a_member(|| net_c.claim_oldest(c_names.clone(), 2)).await;
                    (claimed, refused, batch, [first_huge, second_huge])
                } => claims,
            }
        })
        .await
        .expect("every claim was answered within the timeout");

        match claimed.expect("the leader answered").result {
            Some(claim_response::Result::Accept(claim)) => {
                let task = claim.task.expect("an accepted claim carries its Task");
                assert_eq!(task.task_id(), taken);
                assert_eq!(claim.attempt_number, 1);
            }
            other => panic!("expected the first claim to be accepted, got {other:?}"),
        }
        match refused.expect("the leader answered").result {
            Some(claim_response::Result::Reject(reject)) => assert_eq!(
                reject.reason(),
                ClaimRejectReason::ClaimRejectAlreadySelected,
                "a second claimant for the taken task is refused"
            ),
            other => panic!("expected the second claim to be rejected, got {other:?}"),
        }
        assert_eq!(
            claimed_tasks(batch),
            oldest,
            "the oldest pending tasks, oldest first, the leader the joiners' nodes name"
        );
        assert_eq!(
            huge_batches.map(claimed_tasks),
            huge.map(|task| vec![task]),
            "tasks too big to share a message go out one to a batch, and none is lost"
        );
        // The leader counts each ask as a claim arrival, a refused one too.
        assert!(net_a.diagnostics().await.traffic.claim_requests_received >= 5);
    })
    .await
}
