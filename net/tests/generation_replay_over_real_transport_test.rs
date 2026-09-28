//! Chunk C9: README §25.4.5 — "A lost generation that was the newest for its
//! key is replayed; a lost stale generation is not." — proven end-to-end over
//! a real libp2p swarm, real loopback TCP sockets, and a real disconnect, not
//! the in-memory simulator.
//!
//! `core::scheduler::Scheduler::lose_worker` already implements this exact
//! semantics (Phase 0, proven by `core/tests/scheduler_loss_test.rs`'s
//! `a_lost_coalescing_generation_that_is_the_newest_is_replayed` and
//! `a_lost_coalescing_generation_with_a_newer_one_waiting_is_not_replayed`).
//! Per the Global Constraint carried through this whole plan ("the
//! election/scheduler logic in `core` is not being redesigned — only given a
//! real transport"), this file does not re-derive that logic; it drives the
//! exact same `Scheduler` behavior through a real leader `WorkerNode` and
//! real `kabudachi_net::messenger::Net` claim wire protocol
//! (`/kabudachi/claim/1`, chunk C6), with the "worker becomes unreachable"
//! precondition being a real `Net::disconnect` severing a real TCP
//! connection — not a synchronous, in-process `lose_worker` call standing in
//! for the whole scenario.
//!
//! ## The leader detects the loss itself
//!
//! A leader tracks when it last heard a `WorkerHeartbeat` from each worker
//! (any worker that heartbeats it, not only its roster: see
//! `core::election::WorkerNode`'s "lost workers" doc) and reports
//! `Output::WorkerLost(worker)`, once, after a suspicion timeout and then a
//! reconnect timeout of silence (README §8.3). `net::driver::run_driver`
//! carries that output out by calling `core::election::apply_to_scheduler`,
//! which calls `Scheduler::lose_worker`. So `net_w1` heartbeats the leader
//! before it is disconnected, the leader node has a short
//! `with_reconnect_timeout`, and the leader's own driver decides the loss
//! and replays the run.
//!
//! `net_w1`/`net_w2` stay bare `Net`s, not full `WorkerNode`s (see
//! "Topology" below): a bare `Net` sending one `WorkerHeartbeat` message
//! needs no election of its own, exactly like `claim_arbitration_test.rs`'s
//! bare `Net`s sending `REQUEST_CLAIM`.
//!
//! ## Topology
//!
//! `node_a` self-elects alone (single-member electorate `{a}`): a lone node
//! still waits out its configured `suspect_timeout`, then converges with no
//! race against any peer. `net_w1`/`net_w2` are bare `Net`s (not full
//! `WorkerNode`s), pointed at `node_a` with `Net::set_leader` since no driver
//! runs on them: `Scheduler::request_claim` only ever sees a bare `WorkerId`,
//! and how a claimant learns its leader from its node is
//! `claim_arbitration_test.rs`'s concern, not this test's. Since
//! `net_w1`/`net_w2` are never part of `node_a`'s electorate, disconnecting
//! `net_w1` never touches `node_a`'s own quorum math (a leader's quorum
//! counts only electorate members' confirmations) — this test is entirely about the
//! `Scheduler`'s claim/loss/replay behavior, not the election layer C7/C8
//! already covered.

mod support;

use std::time::Duration as StdDuration;

use kabudachi_core::election::{ElectionTimings, WorkerNode};
use kabudachi_core::protocol::ids::{IncarnationId, ShardId, TaskDefinitionId, Uuid7Ids, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::messages::{
    ClaimRejectReason, ElectionMessage, WorkerHeartbeat, claim_response, election_message,
};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::{Scheduler, Submission};
use kabudachi_core::time::{Duration, RealClock};
use kabudachi_net::driver::run_driver;
use kabudachi_net::messenger::Net;
use kabudachi_net::swarm::build_swarm;
use libp2p::identity;
use tokio::sync::watch;
use tokio::time::timeout;

use support::net::{connect_to, heartbeat_until_acked, wait_until_disconnected};

const SHARD: &str = "shard-1";
/// Matches `claim_arbitration_test.rs`'s own constants — same reasoning.
const SUSPECT_TIMEOUT_MS: u64 = 300;

/// How often a follower heartbeats its leader: well inside every suspicion
/// timeout this file uses.
const HEARTBEAT_INTERVAL_MS: u64 = 10;

/// How long a roll call runs: well above the time a roll call takes to
/// reach a loopback peer and its reply to come back.
const ROLL_CALL_DEADLINE_MS: u64 = 100;

/// `node_a`'s `with_reconnect_timeout` (README §8.3): short, so the leader's
/// own `Output::WorkerLost` detection (suspect timeout, then this) finishes
/// well inside `TEST_TIMEOUT`.
const RECONNECT_TIMEOUT_MS: u64 = 100;

/// The leader's real detection window (`core::election::WorkerNode`'s "lost
/// workers" doc): `suspect_timeout`, then `reconnect_timeout`, of silence
/// since the worker's last heartbeat.
fn detection_window() -> StdDuration {
    StdDuration::from_millis(SUSPECT_TIMEOUT_MS) + StdDuration::from_millis(RECONNECT_TIMEOUT_MS)
}

/// Extra slack past [`detection_window`] this file waits before assuming the
/// leader has detected and replayed the loss, to absorb scheduling jitter on
/// a loaded host.
const DETECTION_SLACK: StdDuration = StdDuration::from_millis(200);

const TEST_TIMEOUT: StdDuration = StdDuration::from_secs(20);

fn generation(payload: &str, key: &str) -> Submission {
    Submission::new(
        TaskDefinitionId::new("index.refresh"),
        0,
        payload.as_bytes().to_vec(),
        "default",
    )
    .with_coalescing_key(key)
}

/// A `WorkerHeartbeat` from `worker`, at recovery epoch 0, so `net_w1`/
/// `net_w2` (bare `Net`s, never full `WorkerNode`s — this file's module doc)
/// can make the leader track them without running an election of their own.
fn heartbeat_from(worker: &WorkerId) -> ElectionMessage {
    ElectionMessage {
        payload: Some(election_message::Payload::Heartbeat(WorkerHeartbeat {
            worker_id: Some(worker.clone().into()),
            incarnation_id: Some(
                IncarnationId::new(format!("{}-incarnation-0", worker.as_str())).into(),
            ),
            recovery_epoch_seen: 0,
            term_seen: 0,
            available_capacity: 0,
            active_task_runs_digest: Vec::new(),
            shard_id: Some(ShardId::new(SHARD).into()),
            newest_accepted_ack: None,
            configuration_generation: None,
            send_token: 0,
        })),
    }
}

/// Runs `node_a`'s driver until [`detection_window`] plus [`DETECTION_SLACK`]
/// has passed since `since`, letting the leader's own `Tick`s report
/// `Output::WorkerLost` and apply it to `scheduler_a` (see
/// `net::driver::run_driver`'s doc) once its `suspect_timeout` and
/// `reconnect_timeout` have elapsed since the worker's last heartbeat.
async fn wait_for_the_leader_to_detect_the_loss(
    node_a: &mut WorkerNode<RealClock>,
    net_a: &Net,
    scheduler_a: &mut Scheduler<RealClock, Uuid7Ids>,
    clock: RealClock,
    since: std::time::Instant,
) {
    let wait_until = detection_window() + DETECTION_SLACK;
    tokio::select! {
        _ = run_driver(node_a, net_a, scheduler_a, clock, None, |_, _| {}) => {
            unreachable!("run_driver never returns")
        }
        () = async {
            let elapsed = since.elapsed();
            if elapsed < wait_until {
                tokio::time::sleep(wait_until - elapsed).await;
            }
        } => {}
    }
}

#[tokio::test]
async fn a_lost_generation_that_was_the_newest_for_its_key_is_replayed_and_claimable_over_the_real_transport()
 {
    let net_a = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
    let leader_id = net_a.local_worker_id();
    let listen_addr = timeout(
        TEST_TIMEOUT,
        net_a.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
    )
    .await
    .expect("net_a produced a listen address within the timeout");

    let clock = RealClock::new();
    let mut node_a = WorkerNode::genesis(
        leader_id.clone(),
        IncarnationId::new("a-incarnation-0"),
        ShardId::new(SHARD),
        clock,
        0,
        None,
        ElectionTimings::new(
            Duration::from_millis(SUSPECT_TIMEOUT_MS),
            Duration::from_millis(HEARTBEAT_INTERVAL_MS),
        )
        .with_roll_call_deadline(Duration::from_millis(ROLL_CALL_DEADLINE_MS)),
    )
    .with_reconnect_timeout(Duration::from_millis(RECONNECT_TIMEOUT_MS));
    let mut scheduler_a = Scheduler::new(clock, Uuid7Ids);

    // Phase 1: node_a self-elects alone.
    let (tx_a, mut rx_a) = watch::channel(node_a.state());
    timeout(TEST_TIMEOUT, async {
        tokio::select! {
            _ = run_driver(&mut node_a, &net_a, &mut scheduler_a, clock, None, |node, _| { let _ = tx_a.send(node.state()); }) => {
                unreachable!("run_driver never returns")
            }
            _ = async {
                loop {
                    if *rx_a.borrow() == WorkerState::Leader {
                        return;
                    }
                    rx_a.changed().await.expect("driver task is still running");
                }
            } => {}
        }
    })
    .await
    .expect("node_a self-elected Leader within the timeout");

    // Between phases (select! dropped run_driver's borrow of scheduler_a —
    // see this file's module doc): seed the newest, single generation for
    // key "k".
    let task = scheduler_a
        .submit(generation("a", "k"))
        .expect("submitting with no memory limits configured never fails");

    let net_w1 = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
    let w1_id = net_w1.local_worker_id();
    connect_to(&net_a, &listen_addr, &net_w1).await;
    net_w1.set_leader(Some(leader_id.clone()));

    // Phase 2: w1 claims the only generation over the real wire.
    let claim = timeout(TEST_TIMEOUT, async {
        tokio::select! {
            _ = run_driver(&mut node_a, &net_a, &mut scheduler_a, clock, None, |_, _| {}) => {
                unreachable!("run_driver never returns")
            }
            response = net_w1.request_claim(task.clone()) => response,
        }
    })
    .await
    .expect("w1's claim request completed within the timeout")
    .expect("the leader answered w1's claim request");
    match claim.result {
        Some(claim_response::Result::Accept(claim)) => {
            assert_eq!(claim.attempt_number, 1);
        }
        other => panic!("expected w1's claim to be accepted, got {other:?}"),
    }

    // w1 heartbeats the leader once, over the real wire, so the leader's
    // driver starts tracking it before the disconnect (see this file's
    // module doc): a worker the leader has never heard from is never "lost".
    let heartbeat_sent_at = heartbeat_until_acked(
        &mut node_a,
        &net_a,
        &mut scheduler_a,
        clock,
        &net_w1,
        heartbeat_from(&w1_id),
    )
    .await;

    // Between phases: sever w1's real TCP connection to the leader (a real
    // partition/unreachability, not a bookkeeping fake) and wait until w1
    // sees it closed.
    net_a.disconnect(w1_id.clone());
    wait_until_disconnected(&net_w1, &leader_id).await;

    // The leader's own driver decides w1 is lost, once its suspect_timeout
    // and reconnect_timeout have passed since the heartbeat above, and
    // applies that to scheduler_a itself (see this file's module doc). Since
    // no newer generation of "k" is pending, this must be replayed — proven
    // below by w2's claim of it, attempt 2.
    wait_for_the_leader_to_detect_the_loss(
        &mut node_a,
        &net_a,
        &mut scheduler_a,
        clock,
        heartbeat_sent_at,
    )
    .await;

    // A second, real worker claims the replay over the real wire.
    let net_w2 = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
    connect_to(&net_a, &listen_addr, &net_w2).await;
    net_w2.set_leader(Some(leader_id.clone()));

    let replacement_claim = timeout(TEST_TIMEOUT, async {
        tokio::select! {
            _ = run_driver(&mut node_a, &net_a, &mut scheduler_a, clock, None, |_, _| {}) => {
                unreachable!("run_driver never returns")
            }
            response = net_w2.request_claim(task.clone()) => response,
        }
    })
    .await
    .expect("w2's claim request completed within the timeout")
    .expect("the leader answered w2's claim request");
    match replacement_claim.result {
        Some(claim_response::Result::Accept(claim)) => {
            assert_eq!(
                claim
                    .task
                    .expect("an accepted claim carries its Task")
                    .task_id(),
                task,
                "the replayed claim must be for the same task the lost generation belonged to"
            );
            assert_eq!(
                claim.attempt_number, 2,
                "a replay of a lost generation is its second attempt"
            );
        }
        other => panic!("expected w2's claim of the replay to be accepted, got {other:?}"),
    }
}

#[tokio::test]
async fn a_lost_stale_generation_with_a_newer_one_waiting_is_not_replayed_over_the_real_transport()
{
    let net_a = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
    let leader_id = net_a.local_worker_id();
    let listen_addr = timeout(
        TEST_TIMEOUT,
        net_a.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
    )
    .await
    .expect("net_a produced a listen address within the timeout");

    let clock = RealClock::new();
    let mut node_a = WorkerNode::genesis(
        leader_id.clone(),
        IncarnationId::new("a-incarnation-0"),
        ShardId::new(SHARD),
        clock,
        0,
        None,
        ElectionTimings::new(
            Duration::from_millis(SUSPECT_TIMEOUT_MS),
            Duration::from_millis(HEARTBEAT_INTERVAL_MS),
        )
        .with_roll_call_deadline(Duration::from_millis(ROLL_CALL_DEADLINE_MS)),
    )
    .with_reconnect_timeout(Duration::from_millis(RECONNECT_TIMEOUT_MS));
    let mut scheduler_a = Scheduler::new(clock, Uuid7Ids);

    let (tx_a, mut rx_a) = watch::channel(node_a.state());
    timeout(TEST_TIMEOUT, async {
        tokio::select! {
            _ = run_driver(&mut node_a, &net_a, &mut scheduler_a, clock, None, |node, _| { let _ = tx_a.send(node.state()); }) => {
                unreachable!("run_driver never returns")
            }
            _ = async {
                loop {
                    if *rx_a.borrow() == WorkerState::Leader {
                        return;
                    }
                    rx_a.changed().await.expect("driver task is still running");
                }
            } => {}
        }
    })
    .await
    .expect("node_a self-elected Leader within the timeout");

    // Seed the older generation of key "k" — this is the one w1 will hold.
    let stale_task = scheduler_a
        .submit(generation("a", "k"))
        .expect("submitting with no memory limits configured never fails");

    let net_w1 = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
    let w1_id = net_w1.local_worker_id();
    connect_to(&net_a, &listen_addr, &net_w1).await;
    net_w1.set_leader(Some(leader_id.clone()));

    let claim = timeout(TEST_TIMEOUT, async {
        tokio::select! {
            _ = run_driver(&mut node_a, &net_a, &mut scheduler_a, clock, None, |_, _| {}) => {
                unreachable!("run_driver never returns")
            }
            response = net_w1.request_claim(stale_task.clone()) => response,
        }
    })
    .await
    .expect("w1's claim request completed within the timeout")
    .expect("the leader answered w1's claim request");
    assert!(
        matches!(claim.result, Some(claim_response::Result::Accept(_))),
        "expected w1's claim of the older generation to be accepted, got {:?}",
        claim.result
    );

    // Between phases: a newer generation of the same key "k" is submitted
    // while w1 still holds the older one — README §3.2.1's supersession, the
    // precondition `Scheduler::lose_worker`'s `newer_waits` branch checks.
    let newer_task = scheduler_a
        .submit(generation("b", "k"))
        .expect("submitting with no memory limits configured never fails");

    // w1 heartbeats the leader once, over the real wire, so the leader's
    // driver starts tracking it before the disconnect (this file's module
    // doc).
    let heartbeat_sent_at = heartbeat_until_acked(
        &mut node_a,
        &net_a,
        &mut scheduler_a,
        clock,
        &net_w1,
        heartbeat_from(&w1_id),
    )
    .await;

    net_a.disconnect(w1_id.clone());
    wait_until_disconnected(&net_w1, &leader_id).await;

    // The leader's own driver decides w1 is lost and applies that to
    // scheduler_a itself (this file's module doc). A newer generation of "k"
    // is pending, so the stale one held by w1 must not be replayed — proven
    // below by the stale task's claim being rejected as FINISHED, never
    // becoming claimable again.
    wait_for_the_leader_to_detect_the_loss(
        &mut node_a,
        &net_a,
        &mut scheduler_a,
        clock,
        heartbeat_sent_at,
    )
    .await;

    // The newer generation is what a real replacement worker claims...
    let net_w2 = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
    connect_to(&net_a, &listen_addr, &net_w2).await;
    net_w2.set_leader(Some(leader_id.clone()));

    let newer_claim = timeout(TEST_TIMEOUT, async {
        tokio::select! {
            _ = run_driver(&mut node_a, &net_a, &mut scheduler_a, clock, None, |_, _| {}) => {
                unreachable!("run_driver never returns")
            }
            response = net_w2.request_claim(newer_task.clone()) => response,
        }
    })
    .await
    .expect("w2's claim request for the newer generation completed within the timeout")
    .expect("the leader answered w2's claim request");
    match newer_claim.result {
        Some(claim_response::Result::Accept(claim)) => {
            assert!(
                claim.chain.is_empty(),
                "the stale, lost generation's payload must not be folded into the newer one"
            );
        }
        other => {
            panic!("expected w2's claim of the newer generation to be accepted, got {other:?}")
        }
    }

    // ...and the stale, lost generation is permanently unclaimable: it was
    // finished, not merely superseded-and-waiting.
    let stale_claim = timeout(TEST_TIMEOUT, async {
        tokio::select! {
            _ = run_driver(&mut node_a, &net_a, &mut scheduler_a, clock, None, |_, _| {}) => {
                unreachable!("run_driver never returns")
            }
            response = net_w2.request_claim(stale_task.clone()) => response,
        }
    })
    .await
    .expect("w2's claim request for the stale generation completed within the timeout")
    .expect("the leader answered w2's claim request for the stale generation");
    match stale_claim.result {
        Some(claim_response::Result::Reject(reject)) => {
            assert_eq!(
                ClaimRejectReason::try_from(reject.reason)
                    .expect("the leader only ever sends a reason this build knows about"),
                ClaimRejectReason::ClaimRejectFinished,
                "the lost, non-replayed stale generation must never become claimable again"
            );
        }
        other => panic!(
            "expected the stale generation's claim to be rejected as FINISHED, got {other:?}"
        ),
    }
}
