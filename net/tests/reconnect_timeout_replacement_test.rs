//! Chunk C9: README §8.3's `heartbeat_timeout`/`reconnect_timeout` policy and
//! README §25.1.9 — "A worker aborts a TaskRun by the reconnect timeout when
//! it cannot reach its leader, and no replacement TaskRun starts before the
//! reconnect timeout has elapsed (§8.3)." — proven over a real libp2p swarm
//! and a real TCP disconnect standing in for a partition, with a real
//! wall-clock stopwatch across the window. Read the "What is and is not
//! actually measured" section below before trusting the headline number:
//! most of the measured window is this test waiting out `RECONNECT_TIMEOUT`
//! by construction, not an independently-timed detection mechanism — only
//! the claim round trip on top of that is a genuine empirical measurement.
//!
//! ## Scope: what this file does and does not prove
//!
//! README §27.1's phase table scopes worker-side hard-kill/subprocess-abort
//! enforcement to Phase 5 ("Hard-timeout subprocess kill; heartbeats
//! unaffected by a CPU-bound task subprocess") — that is specifically about
//! killing the task subprocess itself, and neither it nor any
//! `abort`/`heartbeat_timeout`/`reconnect_timeout` handling exists in `core`
//! or `net` yet. So this file cannot exercise "a worker aborts a TaskRun" —
//! there is no such code path to drive.
//!
//! Separately — and *not* a Phase 5 matter — the leader-side detection that
//! would decide a worker is unreachable and call `lose_worker` automatically
//! is not implemented anywhere in this codebase, and is not deferred to any
//! named phase; it is a genuinely unassigned gap. A `WorkerHeartbeat`/
//! `LeaderHeartbeatAck` message pair is defined in the proto
//! (`proto/election.proto`, re-exported via
//! `core/src/protocol/messages.rs`) and both halves encode/decode through
//! `net/src/codec.rs` — but only the leader->follower `LeaderHeartbeatAck`
//! direction is actually live. The worker->leader `WorkerHeartbeat`
//! direction is never sent, received or consumed by any production code
//! path: `core/src/election.rs` only ever emits `LeaderHeartbeatAck`, and
//! every `WorkerHeartbeat` value in the tree is constructed inside a
//! `#[cfg(test)]` module (`net/src/codec.rs`, `net/src/messenger.rs`,
//! `core/src/protocol/messages.rs`). README §27's own Phase 2 bullet list names
//! "direct leader heartbeat" as in scope for this phase, which is exactly
//! why this gap is open rather than assigned to a later phase. What this
//! file proves instead is the leader/`Scheduler`-side half of the invariant,
//! which *is* implemented today (`core::scheduler::Scheduler::lose_worker`,
//! Phase 0): a replacement `TaskRun` is created only by an explicit
//! `lose_worker` call, never merely by a connection dropping. This file's
//! tests wait out a real `reconnect_timeout`-shaped grace period, timed with
//! a real wall clock against a real severed connection, before making that
//! call — standing in for whatever detection policy eventually closes the
//! gap above.
//!
//! This is also this chunk's documented scope boundary (spec decision 8,
//! carried from the plan): a real TCP disconnect (`Net::disconnect`) proves
//! *connection loss*, not OS-level `SIGKILL`/process death — a real
//! multi-process harness would be needed for the latter, and doesn't exist
//! yet. README §27.1's Phase 2 row is corrected accordingly in this same
//! chunk (see the commit touching that table).
//!
//! `lose_worker` is called directly by these tests rather than by any new
//! `net` production wiring, for the same reason
//! `generation_replay_over_real_transport_test.rs` (this chunk) and chunk
//! C6's `claim_arbitration_test.rs` (`Scheduler::submit`) call `Scheduler`
//! methods directly: this chunk's Global Constraint forbids adding new
//! `core`/`net` production behavior in C9 — acceptance tests only, proving
//! existing `Scheduler` semantics over the real transport.
//!
//! ## What is and is not actually measured
//!
//! [`measured_time_from_worker_unreachable_to_replacement_claim_is_bounded_by_the_reconnect_timeout`]
//! stopwatches from the real disconnect being observed to the real
//! replacement claim being accepted. But the dominant term in that window is
//! produced by the test's own `tokio::time::sleep(RECONNECT_TIMEOUT)`,
//! executed immediately before the test calls `lose_worker` (see the comment
//! at that call site) — so `measured >= RECONNECT_TIMEOUT` is satisfied *by
//! construction* (the test waited exactly that long), not because an
//! independent detection mechanism was timed; there is no such mechanism yet
//! (see "Scope" above). The genuinely non-trivial, empirical measurement
//! this test contributes is only the incremental claim round trip *on top*
//! of `RECONNECT_TIMEOUT` — the real election/claim latency over the real
//! wire, which the test also logs and sanity-bounds. Read the headline
//! number accordingly: it demonstrates the lower-bound invariant holds and
//! measures the real round-trip cost added on top, not an end-to-end
//! empirically-timed detection-to-replacement latency.
//!
//! ## Topology
//!
//! Identical to `generation_replay_over_real_transport_test.rs`: `node_a`
//! self-elects alone (single-member electorate), `net_w1`/`net_w2` are bare
//! `Net`s standing in for real followers over the real network (per
//! `claim_arbitration_test.rs`'s established precedent), both connected
//! *before* the disconnect/timing window starts so the measured window in
//! [`measured_time_from_worker_unreachable_to_replacement_claim_is_bounded_by_the_reconnect_timeout`]
//! isolates the invariant's own latency rather than an unrelated peer's
//! connection-establishment time.

mod support;

use std::collections::BTreeSet;
use std::time::{Duration as StdDuration, Instant as StdInstant};

use kabudachi_core::election::WorkerNode;
use kabudachi_core::membership::RingMembership;
use kabudachi_core::protocol::ids::{IncarnationId, ShardId, TaskDefinitionId, Uuid7Ids};
use kabudachi_core::protocol::messages::{ClaimRejectReason, claim_response};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::Scheduler;
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
/// Matches this chunk's sibling file and `claim_arbitration_test.rs`.
const SUSPECT_TIMEOUT_MS: u64 = 300;
const TICK_INTERVAL_MS: u64 = 30;

/// The `reconnect_timeout`-shaped grace period these tests hold the leader
/// to (README §8.3). Comfortably larger than `TICK_INTERVAL_MS` (about 13
/// ticks) so "just before it elapses" and "well after it elapses" are
/// unambiguous real-time checkpoints, and comfortably smaller than
/// `TEST_TIMEOUT` and every redial backoff used anywhere in `net/tests`
/// (chunk C8's `initial_backoff` is 10s) so no auto-redial can interfere.
const RECONNECT_TIMEOUT: StdDuration = StdDuration::from_millis(400);

const TEST_TIMEOUT: StdDuration = StdDuration::from_secs(20);

#[tokio::test]
async fn measured_time_from_worker_unreachable_to_replacement_claim_is_bounded_by_the_reconnect_timeout()
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

    let task = scheduler_a
        .submit(kabudachi_core::scheduler::Submission::new(
            TaskDefinitionId::new("demo.task"),
            1,
            b"payload".to_vec(),
            "default",
        ))
        .expect("submitting with no memory limits configured never fails");

    // Both followers connect up front, before the timed window starts, so
    // connection establishment never counts toward the measured latency.
    let net_w1 = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
    let w1_id = net_w1.local_worker_id();
    let net_w2 = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
    connect_to(&net_a, &listen_addr, &net_w1).await;
    connect_to(&net_a, &listen_addr, &net_w2).await;

    // w1 claims the task over the real wire.
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
    assert!(
        matches!(claim.result, Some(claim_response::Result::Accept(_))),
        "expected w1's claim to be accepted, got {:?}",
        claim.result
    );

    // ---- The measured window starts here: real disconnect, real timer ----
    net_a.disconnect(w1_id.clone());
    let unreachable_at = wait_until_unreachable(&net_a, &leader_id, &w1_id).await;

    // The reconnect_timeout grace period (README §8.3): the leader does not
    // decide w1 is lost until this elapses. NOTE: this `sleep` is what makes
    // the `measured >= RECONNECT_TIMEOUT` assertion below true by
    // construction, not because an independent detection timer was
    // observed — see the module doc's "What is and is not actually
    // measured" section. There is no real leader-side detection mechanism
    // yet to time; this stands in for it.
    tokio::time::sleep(RECONNECT_TIMEOUT).await;
    let lost = scheduler_a
        .lose_worker(&w1_id)
        .expect("node_a is still Leader");
    assert_eq!(lost.len(), 1);
    assert!(lost[0].replayed.is_some());

    // w2's claim of the replayed run, over the real wire, is what actually
    // starts the replacement.
    let replacement = timeout(TEST_TIMEOUT, async {
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
    let claim_accepted_at = StdInstant::now();
    assert!(
        matches!(replacement.result, Some(claim_response::Result::Accept(_))),
        "expected w2's replacement claim to be accepted, got {:?}",
        replacement.result
    );
    // ---- The measured window ends here ----

    let measured = claim_accepted_at.duration_since(unreachable_at);
    eprintln!(
        "measured_time_from_worker_unreachable_to_replacement_claim: {measured:?} \
         (reconnect_timeout = {RECONNECT_TIMEOUT:?})"
    );

    // This lower bound holds by construction (see the `sleep(RECONNECT_TIMEOUT)`
    // above and the module doc): it is not evidence of an independently-timed
    // detection mechanism, only that the test waited the grace period out
    // before deciding loss, matching README §25.1.9's no-early-replacement
    // invariant.
    assert!(
        measured >= RECONNECT_TIMEOUT,
        "a replacement claim must never be accepted before the reconnect_timeout grace period \
         has elapsed (measured {measured:?} < reconnect_timeout {RECONNECT_TIMEOUT:?})"
    );
    // Sanity upper bound, not a strict SLA: on real loopback sockets the
    // election/claim round trip this adds on top of reconnect_timeout is
    // expected to be on the order of one or two tick intervals plus network
    // stack overhead (low tens of milliseconds), not seconds. A generous
    // margin absorbs CI jitter while still catching a real regression (e.g.
    // a stuck driver loop) that would otherwise silently pass an
    // unbounded-time assertion. This incremental delta — `measured -
    // RECONNECT_TIMEOUT` — is the one genuinely empirical measurement this
    // test contributes (see the module doc).
    let upper_bound = RECONNECT_TIMEOUT + StdDuration::from_secs(2);
    assert!(
        measured < upper_bound,
        "the replacement claim took implausibly long after the reconnect_timeout elapsed: \
         measured {measured:?}, expected well under {upper_bound:?}"
    );
}

#[tokio::test]
async fn no_replacement_task_run_is_claimable_before_the_reconnect_timeout_has_elapsed_under_a_partition()
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

    let task = scheduler_a
        .submit(kabudachi_core::scheduler::Submission::new(
            TaskDefinitionId::new("demo.task"),
            1,
            b"payload".to_vec(),
            "default",
        ))
        .expect("submitting with no memory limits configured never fails");

    let net_w1 = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
    let w1_id = net_w1.local_worker_id();
    let net_w2 = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
    connect_to(&net_a, &listen_addr, &net_w1).await;
    connect_to(&net_a, &listen_addr, &net_w2).await;

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
    assert!(matches!(
        claim.result,
        Some(claim_response::Result::Accept(_))
    ));

    // Simulate the partition: sever w1's real connection to the leader.
    net_a.disconnect(w1_id.clone());
    let unreachable_at = wait_until_unreachable(&net_a, &leader_id, &w1_id).await;

    // While the leader is still (correctly) withholding judgment on w1 —
    // i.e. for the entire reconnect_timeout grace period — a second real
    // worker asking for the same task must be told it is already selected,
    // never that a replacement exists. Checked twice: immediately after the
    // partition is observed, and again right up against the boundary,
    // without ever calling `lose_worker`.
    let assert_still_selected = |resp: kabudachi_core::protocol::messages::ClaimResponse| {
        match resp.result {
            Some(claim_response::Result::Reject(reject)) => {
                assert_eq!(
                    ClaimRejectReason::try_from(reject.reason)
                        .expect("the leader only ever sends a reason this build knows about"),
                    ClaimRejectReason::ClaimRejectAlreadySelected,
                    "before the reconnect_timeout elapses, the original claim must still stand"
                );
            }
            other => panic!(
                "expected the claim to still be rejected as ALREADY_SELECTED (no replacement \
                 should exist yet), got {other:?}"
            ),
        }
    };

    timeout(TEST_TIMEOUT, async {
        tokio::select! {
            _ = run_driver(&mut node_a, &net_a, tick_interval, |_| {}, |_| {}, &mut scheduler_a) => {
                unreachable!("run_driver never returns")
            }
            _ = async {
                // Checkpoint 1: immediately after the partition is observed.
                let early = net_w2.request_claim(leader_id.clone(), task.clone()).await
                    .expect("the leader answered w2's early claim attempt");
                assert_still_selected(early);

                // Checkpoint 2: as close to the reconnect_timeout boundary
                // as this test gets without crossing it. Opportunistic: under
                // scheduling delay (a slow or contended host), real
                // wall-clock time can advance past the reconnect_timeout
                // boundary between the sleep below and this check even
                // though the reconnect-timeout behavior itself is correct —
                // when that happens, skip the late-claim assertion instead
                // of treating a scheduler hiccup as a test failure.
                // Checkpoint 1 above and the post-timeout assertions further
                // down are unaffected; only this checkpoint tolerates the
                // delay.
                let margin = StdDuration::from_millis(50);
                let just_before = unreachable_at + RECONNECT_TIMEOUT - margin;
                let now = StdInstant::now();
                if just_before > now {
                    tokio::time::sleep(just_before - now).await;
                }
                if StdInstant::now() < unreachable_at + RECONNECT_TIMEOUT {
                    let late = net_w2.request_claim(leader_id.clone(), task.clone()).await
                        .expect("the leader answered w2's late-but-still-in-window claim attempt");
                    assert_still_selected(late);
                } else {
                    eprintln!(
                        "checkpoint 2 skipped: scheduling delay pushed wall-clock time past the \
                         reconnect_timeout boundary before the late-claim assertion could run"
                    );
                }
            } => {}
        }
    })
    .await
    .expect("both pre-timeout checkpoints completed within the test timeout");

    // Now let the grace period actually elapse, and only then does the
    // leader decide w1 is lost — the real reconnect_timeout policy, timed
    // against the real disconnect observed above.
    let now = StdInstant::now();
    let deadline = unreachable_at + RECONNECT_TIMEOUT;
    if deadline > now {
        tokio::time::sleep(deadline - now).await;
    }
    let after_timeout = StdInstant::now();
    assert!(
        after_timeout.duration_since(unreachable_at) >= RECONNECT_TIMEOUT,
        "no replacement TaskRun may be created before the reconnect_timeout has elapsed"
    );
    let lost = scheduler_a
        .lose_worker(&w1_id)
        .expect("node_a is still Leader");
    assert_eq!(lost.len(), 1);
    assert!(
        lost[0].replayed.is_some(),
        "the task's only generation, held by the now-lost worker, must be replayed"
    );

    // Only now does a replacement actually become claimable.
    let replacement = timeout(TEST_TIMEOUT, async {
        tokio::select! {
            _ = run_driver(&mut node_a, &net_a, tick_interval, |_| {}, |_| {}, &mut scheduler_a) => {
                unreachable!("run_driver never returns")
            }
            response = net_w2.request_claim(leader_id.clone(), task.clone()) => response,
        }
    })
    .await
    .expect("w2's replacement claim completed within the timeout")
    .expect("the leader answered w2's replacement claim");
    match replacement.result {
        Some(claim_response::Result::Accept(claim)) => {
            assert_eq!(
                claim.attempt_number, 2,
                "the replacement is the task's second attempt"
            );
        }
        other => panic!(
            "expected w2's replacement claim to be accepted only after the reconnect_timeout, \
             got {other:?}"
        ),
    }
}
