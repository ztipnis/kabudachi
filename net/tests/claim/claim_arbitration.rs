//! Claim arbitration (`/kabudachi/claim/1`, README §8.2) end to end over
//! real sockets: a genesis leader and two joiners, every one a driven
//! `WorkerNode` with its own `Net` and `Scheduler`.
//!
//! The joiners are pending members (ADR-0001 decision 9.1): they claim work
//! as soon as JOIN completes, before any admission makes them voters. Each
//! asks the leader its own node names: the test passes on what the node
//! reports, and never picks a leader for a claimant. The leader decides from the leadership grant its election handed
//! its scheduler, so a worker holding no grant refuses every claim.
//!
//! The tasks are submitted before any node is driven: submission needs no
//! leadership, and a scheduler that leads later finds them queued.


use std::time::Duration as StdDuration;

use crate::support::election::due_now;
use kabudachi_core::coordination_authority::RecoveryEpoch;
use kabudachi_core::election::{ElectionTimings, Entry, Identity, WorkerNode};
use kabudachi_core::protocol::ids::{
    IncarnationId, ShardId, TaskDefinitionId, TaskId, Uuid7Ids, WorkerId,
};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::messages::{ClaimRejectReason, ClaimResponse, claim_response};
use kabudachi_core::scheduler::{Scheduler, Submission};
use kabudachi_core::time::{Duration, RealClock};
use kabudachi_net::driver::run_driver;
use kabudachi_net::messenger::{ClaimFailure, Net};
use kabudachi_net::swarm::build_swarm;
use libp2p::{Multiaddr, identity};
use tokio::sync::watch;
use tokio::time::timeout;

use crate::support::net::{ask_until_pointed_at_a_leader, connect_to};

const SHARD: &str = "shard-1";

/// How long a node goes without leader contact before it suspects its
/// leader; the genesis leader waits this out before its lone roll call.
const SUSPECT_TIMEOUT_MS: u64 = 300;

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

/// Big enough that two of them do not fit in one claim message
/// (`/kabudachi/claim/1` caps a message at 1 MiB), small enough that one does.
const LARGE_PAYLOAD_BYTES: usize = 600 * 1024;

fn submit(scheduler: &mut Scheduler<RealClock, Uuid7Ids>, payload: Vec<u8>) -> TaskId {
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
    let mut scheduler = Scheduler::new(clock, Uuid7Ids);
    run_driver(&mut node, first, net, &mut scheduler, clock, None, |node, _, _| {
        let _ = known_leader.send(node.known_leader().map(|(leader, _)| leader));
    })
    .await;
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

fn rejection(response: Result<ClaimResponse, ClaimFailure>) -> ClaimRejectReason {
    match response.expect("the leader answered").result {
        Some(claim_response::Result::Reject(reject)) => ClaimRejectReason::try_from(reject.reason)
            .expect("the leader only ever sends a reason this build knows about"),
        other => panic!("expected a rejection, got {other:?}"),
    }
}

#[tokio::test]
async fn pending_members_claim_from_the_leader_their_nodes_name() {
    let net_a = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
    let net_b = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
    let net_c = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
    let worker_a = net_a.local_worker_id();
    let worker_b = net_b.local_worker_id();
    let seed = timeout(
        TEST_TIMEOUT,
        net_a.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
    )
    .await
    .expect("net_a produced a listen address within the timeout");
    let b_addr = timeout(
        TEST_TIMEOUT,
        net_b.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
    )
    .await
    .expect("net_b produced a listen address within the timeout");

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
    let mut scheduler_a = Scheduler::new(clock, Uuid7Ids);
    let small = || b"payload".to_vec();
    let taken = submit(&mut scheduler_a, small());
    let oldest = [
        submit(&mut scheduler_a, small()),
        submit(&mut scheduler_a, small()),
        submit(&mut scheduler_a, vec![0; LARGE_PAYLOAD_BYTES]),
    ];
    // The first that does not fit ends the batch: the small task behind it
    // waits too, so the oldest go out first.
    let left_for_the_next_batch = [
        submit(&mut scheduler_a, vec![1; LARGE_PAYLOAD_BYTES]),
        submit(&mut scheduler_a, small()),
    ];

    // A bare claimant that still names a worker holding no grant, as one
    // whose node has not yet heard of a newer leader would.
    let stale = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
    connect_to(&net_b, &b_addr, &stale).await;

    let (b_leader, mut b_knows) = watch::channel(None);
    let (c_leader, mut c_knows) = watch::channel(None);

    let (claimed, taken_again, batch, next_batch, refused, own) = timeout(TEST_TIMEOUT, async {
        tokio::select! {
            _ = run_driver(&mut node_a, due_now(&clock), &net_a, &mut scheduler_a, clock, None, |_, _, _| {}) => {
                unreachable!("run_driver never returns")
            }
            () = join_and_drive(&net_b, &seed, clock, &b_leader) => {
                unreachable!("run_driver never returns")
            }
            () = join_and_drive(&net_c, &seed, clock, &c_leader) => {
                unreachable!("run_driver never returns")
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
                let claimed = net_b.request_claim(b_names, taken.clone()).await;
                let taken_again = net_c.request_claim(c_names.clone(), taken.clone()).await;
                let batch = net_c.claim_oldest(c_names.clone(), 5).await;
                let next_batch = net_c.claim_oldest(c_names, 5).await;
                let refused = stale.request_claim(worker_b, oldest[0].clone()).await;
                // A leader naming itself is told to decide its own claims.
                let own = net_a.request_claim(worker_a.clone(), oldest[0].clone()).await;
                (claimed, taken_again, batch, next_batch, refused, own)
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
    assert_eq!(
        rejection(taken_again),
        ClaimRejectReason::ClaimRejectAlreadySelected
    );
    assert_eq!(
        claimed_tasks(batch),
        oldest,
        "the oldest pending tasks, oldest first, as many as fit in one message"
    );
    assert_eq!(claimed_tasks(next_batch), left_for_the_next_batch);
    assert_eq!(rejection(refused), ClaimRejectReason::ClaimRejectNotLeader);
    assert_eq!(own, Err(ClaimFailure::ThisWorkerLeads));
}
