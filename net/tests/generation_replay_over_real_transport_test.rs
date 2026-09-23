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
//! ## Why `lose_worker` itself is still called directly by this test
//!
//! Nothing in `core` or `net` yet decides *when* a leader should call
//! `lose_worker` in response to a real disconnect. A `WorkerHeartbeat`/
//! `LeaderHeartbeatAck` message pair is defined in the proto
//! (`proto/election.proto`, re-exported via
//! `core/src/protocol/messages.rs`) and both halves encode/decode through
//! `net/src/codec.rs` — but only the leader->follower `LeaderHeartbeatAck`
//! direction is actually live. The worker->leader `WorkerHeartbeat`
//! direction is never sent, received or consumed by any production code
//! path: `core/src/election.rs` only ever emits `LeaderHeartbeatAck`, and
//! every `WorkerHeartbeat` value in the tree is constructed inside a
//! `#[cfg(test)]` module (`net/src/codec.rs`, `net/src/messenger.rs`,
//! `core/src/protocol/messages.rs`). So there is no leader-side
//! liveness/timeout tracking that would call `lose_worker` automatically,
//! and no live message stream one could be built on today.
//! This is a genuinely unassigned gap, not a deferral to any named phase —
//! in particular it is *not* Phase 5, which (README §27.1) is entirely about
//! Python subprocess execution (process limits, cooperative cancellation,
//! SIGTERM/SIGKILL) and has nothing to do with leader-side worker-loss
//! detection. README §27's own Phase 2 bullet list even names "direct leader
//! heartbeat" as in scope for this phase, which is exactly why this gap is
//! open rather than assigned elsewhere. The actual reason this test calls
//! `lose_worker` directly is this chunk's own Global Constraint: C9 adds no
//! new `core`/`net` production behavior — it is acceptance tests only,
//! proving the existing `Scheduler` semantics hold over the real transport.
//! This mirrors chunk C6's `claim_arbitration_test.rs`, which calls
//! `Scheduler::submit` directly for the analogous reason (spec decision 9: no
//! wire submission this phase) — here, the test plays the role of "the
//! leader, having decided (by whatever not-yet-assigned detection policy
//! eventually wires this up) that this worker is lost", exactly the same way
//! that file's `scheduler_a.submit(..)` plays the role of "however a task
//! gets submitted, not modeled by this phase". Everything else — the
//! original claim, the disconnect, and the replacement's claim of the
//! replayed generation — goes over the real wire.
//!
//! ## Topology
//!
//! `node_a` self-elects alone (single-member electorate `{a}`), exactly
//! `claim_arbitration_test.rs`'s proven pattern — a lone node still waits out
//! its configured `suspect_timeout`, then converges with no race against any
//! peer. `net_w1`/`net_w2` are bare `Net`s (not full `WorkerNode`s), also
//! matching `claim_arbitration_test.rs`'s module doc for why that's already
//! "a real follower over the real network" for claim-protocol purposes:
//! `Scheduler::request_claim` only ever sees a bare `WorkerId`, and giving
//! the askers full elections would reintroduce `two_node_election_test.rs`'s
//! documented multi-node convergence race for no added coverage here. Since
//! `net_w1`/`net_w2` are never part of `node_a`'s electorate, disconnecting
//! `net_w1` never touches `node_a`'s own quorum math (`tick_as_leader` only
//! looks at electorate members) — this test is entirely about the
//! `Scheduler`'s claim/loss/replay behavior, not the election layer C7/C8
//! already covered.

mod support;

use std::collections::BTreeSet;
use std::time::Duration as StdDuration;

use kabudachi_core::election::WorkerNode;
use kabudachi_core::membership::RingMembership;
use kabudachi_core::protocol::ids::{IncarnationId, ShardId, TaskDefinitionId, Uuid7Ids};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::messages::{ClaimRejectReason, claim_response};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::{Scheduler, Submission};
use kabudachi_core::time::Duration;
use kabudachi_net::driver::run_driver;
use kabudachi_net::messenger::Net;
use kabudachi_net::swarm::build_swarm;
use libp2p::identity;
use tokio::sync::watch;
use tokio::time::timeout;

use support::authority::AlwaysUnavailableAuthority;
use support::clock::RealClock;
use support::net::{connect_to, wait_until_unreachable};

const SHARD: &str = "shard-1";
/// Matches `claim_arbitration_test.rs`'s own constants — same reasoning.
const SUSPECT_TIMEOUT_MS: u64 = 300;
const TICK_INTERVAL_MS: u64 = 30;

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

    let mut node_a = WorkerNode::new(
        leader_id.clone(),
        IncarnationId::new("a-incarnation-0"),
        ShardId::new(SHARD),
        RealClock::new(),
        &net_a,
        RingMembership::new(BTreeSet::from([leader_id.clone()])),
        AlwaysUnavailableAuthority,
        Duration::from_ticks(SUSPECT_TIMEOUT_MS),
    );
    let mut scheduler_a = Scheduler::new(RealClock::new(), Uuid7Ids);
    let tick_interval = StdDuration::from_millis(TICK_INTERVAL_MS);

    // Phase 1: node_a self-elects alone.
    let (tx_a, mut rx_a) = watch::channel(node_a.state());
    timeout(TEST_TIMEOUT, async {
        tokio::select! {
            _ = run_driver(&mut node_a, &net_a, tick_interval, |s| { let _ = tx_a.send(s); }, |_| {}, &mut scheduler_a) => {
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

    // Phase 2: w1 claims the only generation over the real wire.
    let claim = timeout(TEST_TIMEOUT, async {
        tokio::select! {
            _ = run_driver(&mut node_a, &net_a, tick_interval, |_| {}, |_| {}, &mut scheduler_a) => {
                unreachable!("run_driver never returns")
            }
            response = net_w1.request_claim(leader_id.clone(), task.clone()) => response,
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

    // Between phases: sever w1's real TCP connection to the leader (a real
    // partition/unreachability, not a bookkeeping fake) and wait for the
    // leader's own view to reflect it.
    net_a.disconnect(w1_id.clone());
    wait_until_unreachable(&net_a, &leader_id, &w1_id).await;

    // The leader (per README §8.3's policy, played by this test — see module
    // doc) decides w1 is lost. Since no newer generation of "k" is pending,
    // this must be replayed.
    let lost = scheduler_a
        .lose_worker(&w1_id)
        .expect("node_a is still Leader");
    assert_eq!(lost.len(), 1, "w1 held exactly one run");
    assert!(
        lost[0].replayed.is_some(),
        "the newest generation for its key must be replayed when its worker is lost"
    );

    // A second, real worker claims the replay over the real wire.
    let net_w2 = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
    connect_to(&net_a, &listen_addr, &net_w2).await;

    let replacement_claim = timeout(TEST_TIMEOUT, async {
        tokio::select! {
            _ = run_driver(&mut node_a, &net_a, tick_interval, |_| {}, |_| {}, &mut scheduler_a) => {
                unreachable!("run_driver never returns")
            }
            response = net_w2.request_claim(leader_id.clone(), task.clone()) => response,
        }
    })
    .await
    .expect("w2's claim request completed within the timeout")
    .expect("the leader answered w2's claim request");
    match replacement_claim.result {
        Some(claim_response::Result::Accept(claim)) => {
            assert_eq!(
                claim.task.expect("an accepted claim carries its Task").task_id(),
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

    let mut node_a = WorkerNode::new(
        leader_id.clone(),
        IncarnationId::new("a-incarnation-0"),
        ShardId::new(SHARD),
        RealClock::new(),
        &net_a,
        RingMembership::new(BTreeSet::from([leader_id.clone()])),
        AlwaysUnavailableAuthority,
        Duration::from_ticks(SUSPECT_TIMEOUT_MS),
    );
    let mut scheduler_a = Scheduler::new(RealClock::new(), Uuid7Ids);
    let tick_interval = StdDuration::from_millis(TICK_INTERVAL_MS);

    let (tx_a, mut rx_a) = watch::channel(node_a.state());
    timeout(TEST_TIMEOUT, async {
        tokio::select! {
            _ = run_driver(&mut node_a, &net_a, tick_interval, |s| { let _ = tx_a.send(s); }, |_| {}, &mut scheduler_a) => {
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

    let claim = timeout(TEST_TIMEOUT, async {
        tokio::select! {
            _ = run_driver(&mut node_a, &net_a, tick_interval, |_| {}, |_| {}, &mut scheduler_a) => {
                unreachable!("run_driver never returns")
            }
            response = net_w1.request_claim(leader_id.clone(), stale_task.clone()) => response,
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

    net_a.disconnect(w1_id.clone());
    wait_until_unreachable(&net_a, &leader_id, &w1_id).await;

    let lost = scheduler_a
        .lose_worker(&w1_id)
        .expect("node_a is still Leader");
    assert_eq!(lost.len(), 1, "w1 held exactly one run");
    assert_eq!(
        lost[0].replayed, None,
        "a lost stale generation with a newer one waiting must not be replayed"
    );

    // The newer generation is what a real replacement worker claims...
    let net_w2 = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
    connect_to(&net_a, &listen_addr, &net_w2).await;

    let newer_claim = timeout(TEST_TIMEOUT, async {
        tokio::select! {
            _ = run_driver(&mut node_a, &net_a, tick_interval, |_| {}, |_| {}, &mut scheduler_a) => {
                unreachable!("run_driver never returns")
            }
            response = net_w2.request_claim(leader_id.clone(), newer_task.clone()) => response,
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
        other => panic!(
            "expected w2's claim of the newer generation to be accepted, got {other:?}"
        ),
    }

    // ...and the stale, lost generation is permanently unclaimable: it was
    // finished, not merely superseded-and-waiting.
    let stale_claim = timeout(TEST_TIMEOUT, async {
        tokio::select! {
            _ = run_driver(&mut node_a, &net_a, tick_interval, |_| {}, |_| {}, &mut scheduler_a) => {
                unreachable!("run_driver never returns")
            }
            response = net_w2.request_claim(leader_id.clone(), stale_task.clone()) => response,
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
