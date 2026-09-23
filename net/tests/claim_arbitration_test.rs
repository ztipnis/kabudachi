//! Chunk C6: the claim arbitration protocol (`/kabudachi/claim/1`) end to
//! end over a real libp2p swarm — a real leader `WorkerNode`, a real
//! `kabudachi_net::messenger::Net` per node, real loopback TCP sockets, no
//! simulator, no fakes. Mirrors chunk C4's `three_node_join_test.rs` in
//! spirit (prove the wire protocol against `core`'s existing, unmodified
//! logic — here, `core::scheduler::Scheduler::request_claim` — not a new
//! one written for this chunk).
//!
//! ## Topology: why only the leader is a `WorkerNode`
//!
//! Chunk C6's worker-side "who do I ask" mechanism (peeking inbound
//! `LeaderHeartbeatAck`s to remember a believed leader — see
//! `kabudachi_net::driver`'s module doc) is covered by a dedicated unit test
//! of `driver::observed_leader` in `net/src/driver.rs` itself, which needs no
//! networking to exercise. This test's job is the other half: prove
//! `Scheduler::request_claim` actually decides correctly over the real wire
//! protocol, for both the accepted and the raced-and-rejected case. Nothing
//! about that needs the *askers* to be full `WorkerNode`s running their own
//! election — a bare `Net` sending `REQUEST_CLAIM` to a peer it already
//! knows the `WorkerId` of (exactly like `join_via_seeds`'s test coverage
//! doesn't require the *seed* to be anything more than a `Net` either) is
//! already "a real follower over the real network", per this chunk's brief.
//!
//! Making the askers full `WorkerNode`s instead would reintroduce
//! `two_node_election_test.rs`'s documented multi-node convergence race
//! (which candidate wins is not under this test's control) for no added
//! coverage — the claim arbitration path under test does not care whether
//! the asker is `Active`, `Leader`-suspect, or anything else; `Scheduler`
//! only sees a bare `WorkerId`.
//!
//! `node_a`'s own convergence to `Leader` is instead made fully
//! deterministic by giving it a *single-member* electorate (`{a}`) —
//! `bootstrap_self_elect_test.rs`'s proven pattern (README §27 Phase 2 step
//! (c)): a lone node still waits out its configured `suspect_timeout`, then
//! self-elects with no race against any peer.
//!
//! ## Two phases, one `Scheduler`
//!
//! `Scheduler::submit` (spec decision 9: no wire submission this phase) needs
//! exclusive `&mut` access to `scheduler_a`, but `run_driver` also needs
//! exclusive `&mut` access to it for as long as it runs. So this test drives
//! `node_a` in two separate `tokio::select!` blocks: phase 1 races
//! `run_driver` against "watch for `Leader`" (mirrors
//! `bootstrap_self_elect_test.rs` exactly) — once the watch arm resolves,
//! `tokio::select!` drops the *other*, still-pending `run_driver` future,
//! releasing its borrow of `scheduler_a` back to this function. Only then
//! does `scheduler_a.submit(..)` run. Phase 2 starts a fresh `run_driver`
//! race for `node_a`/`scheduler_a`, this time against the two followers'
//! `Net::request_claim` calls.

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
use support::net::connect_to;

const SHARD: &str = "shard-1";

/// Matches `bootstrap_self_elect_test.rs`'s own constants — same reasoning:
/// a normal, nonzero suspect_timeout, comfortably exceeded by this test's
/// overall timeout.
const SUSPECT_TIMEOUT_MS: u64 = 300;
const TICK_INTERVAL_MS: u64 = 30;

const TEST_TIMEOUT: StdDuration = StdDuration::from_secs(20);

#[tokio::test]
async fn a_follower_claims_a_seeded_task_and_a_racing_follower_is_rejected() {
    let net_a = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
    let worker_a = net_a.local_worker_id();
    let listen_addr = timeout(
        TEST_TIMEOUT,
        net_a.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
    )
    .await
    .expect("net_a produced a listen address within the timeout");

    let mut node_a = WorkerNode::new(
        worker_a.clone(),
        IncarnationId::new("a-incarnation-0"),
        ShardId::new(SHARD),
        RealClock::new(),
        &net_a,
        RingMembership::new(BTreeSet::from([worker_a.clone()])),
        AlwaysUnavailableAuthority,
        Duration::from_ticks(SUSPECT_TIMEOUT_MS),
    );
    let mut scheduler_a = Scheduler::new(RealClock::new(), Uuid7Ids);
    let tick_interval = StdDuration::from_millis(TICK_INTERVAL_MS);

    // Phase 1: node_a self-elects alone (bootstrap_self_elect_test.rs's
    // proven pattern — a single-member electorate races against no one).
    let (tx_a, mut rx_a) = watch::channel(node_a.state());
    timeout(TEST_TIMEOUT, async {
        tokio::select! {
            _ = run_driver(
                &mut node_a,
                &net_a,
                tick_interval,
                |s| { let _ = tx_a.send(s); },
                |_| {},
                &mut scheduler_a,
            ) => {
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
    assert_eq!(node_a.state(), WorkerState::Leader);

    // Between phases: select! above dropped its run_driver future once the
    // watch arm resolved, releasing scheduler_a's borrow — see this file's
    // module doc. Seed the leader's Scheduler directly (spec decision 9: no
    // wire submission this phase).
    let task_id = scheduler_a
        .submit(Submission::new(
            TaskDefinitionId::new("demo.task"),
            1,
            b"payload".to_vec(),
            "default",
        ))
        .expect("submitting with no memory limits configured never fails");

    // Two bare Nets — real swarms, real sockets, connected directly to
    // node_a's — stand in for two followers racing for the same task. See
    // this file's module doc for why they need not be full WorkerNodes.
    let net_b = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
    let net_c = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
    connect_to(&net_a, &listen_addr, &net_b).await;
    connect_to(&net_a, &listen_addr, &net_c).await;

    // Phase 2: keep driving node_a (so it keeps answering claim requests)
    // while both followers ask for the same task, one after the other so the
    // outcome is deterministic: the first REQUEST_CLAIM is accepted, and the
    // second — racing for a task that's now Claimed — is rejected. (The
    // Scheduler's own contract already guarantees "first accepted, rest
    // refused" regardless of arrival order; running them sequentially avoids
    // this test depending on which concurrent request happens to arrive
    // first, which nothing here controls.)
    let (accept_response, reject_response) = timeout(TEST_TIMEOUT, async {
        tokio::select! {
            _ = run_driver(
                &mut node_a,
                &net_a,
                tick_interval,
                |_| {},
                |_| {},
                &mut scheduler_a,
            ) => {
                unreachable!("run_driver never returns")
            }
            responses = async {
                let accept = net_b.request_claim(worker_a.clone(), task_id.clone()).await;
                let reject = net_c.request_claim(worker_a.clone(), task_id.clone()).await;
                (accept, reject)
            } => responses,
        }
    })
    .await
    .expect("both claim requests completed within the test timeout");

    let accept_response =
        accept_response.expect("the leader answered the first follower's claim request");
    match accept_response.result {
        Some(claim_response::Result::Accept(claim)) => {
            assert_eq!(
                claim.task.expect("an accepted claim carries its Task").task_id(),
                task_id,
                "the accepted claim must be for the task seeded on the leader's Scheduler"
            );
            assert_eq!(
                claim.attempt_number, 1,
                "the first claim of a freshly submitted task is attempt 1"
            );
        }
        other => panic!("expected the first follower's claim to be accepted, got {other:?}"),
    }

    let reject_response =
        reject_response.expect("the leader answered the second follower's claim request");
    match reject_response.result {
        Some(claim_response::Result::Reject(reject)) => {
            assert_eq!(
                ClaimRejectReason::try_from(reject.reason)
                    .expect("the leader only ever sends a reason this build knows about"),
                ClaimRejectReason::ClaimRejectAlreadySelected,
                "a second follower racing for an already-claimed task must be rejected as such"
            );
        }
        other => panic!(
            "expected the second, racing follower's claim to be rejected as ALREADY_SELECTED, got {other:?}"
        ),
    }
}
