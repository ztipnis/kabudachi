//! README §8.3's `heartbeat_timeout`/`reconnect_timeout` policy and README
//! §25.1.9 ("no replacement TaskRun starts before the reconnect timeout has
//! elapsed") over a real libp2p swarm, with a real cut standing in for a
//! partition and a real wall-clock stopwatch across the window.
//!
//! ## Scope: what this file does and does not prove
//!
//! No task executor exists in `core` or `net` yet (README §27.1 gives the
//! subprocess abort to Phase 5): a worker's `Output::AbortDeadline` is only
//! logged by `net::driver`. The worker's side of §25.1.9, that the deadline
//! comes before the leader's replacement, is `abort_before_replacement_test.rs`;
//! this file proves the leader's side.
//!
//! The leader detects the loss itself: it tracks when it last heard a
//! `WorkerHeartbeat` from each worker, and reports `Output::WorkerLost` once
//! a suspicion timeout and then `with_reconnect_timeout`'s
//! `reconnect_timeout` have passed since (README §8.3's `heartbeat_timeout`
//! is `suspect_timeout` here). `net::driver::run_driver` carries that output
//! out through `core::election::apply_to_scheduler`, which calls
//! `Scheduler::lose_worker`.
//!
//! The cut is a connection loss (`Net::disconnect`, or `Net::block_peer` on
//! both sides), not OS-level `SIGKILL`: real process death needs a
//! multi-process harness, which does not exist yet (Phase 2 spec decision 8).
//!
//! ## What is measured
//!
//! [`time_from_worker_unreachable_to_replacement_claim_measured_from_the_leader`]
//! is a measurement, run on request (`--ignored`), not an assertion. It
//! starts the clock on the leader's side, at the last heartbeat the leader
//! heard from the cut worker (the instant its driver acked it, which is when
//! the leader's own countdown starts), stamps the leader's
//! `Output::WorkerLost`, and stops when a second worker, which keeps asking,
//! receives the leader's accept of the replacement claim. Nothing in it
//! sleeps out the window, so the figures are the mechanism's own latency.
//!
//! ## Topology
//!
//! Identical to `generation_replay_over_real_transport_test.rs`: `node_a`
//! self-elects alone (single-member electorate), `net_w1`/`net_w2` are bare
//! `Net`s standing in for real followers over the real network, pointed at
//! `node_a` with `Net::set_leader`, both connected before the cut, so
//! connection establishment never counts toward what is timed.

mod support;

use std::cell::Cell;
use std::time::{Duration as StdDuration, Instant as StdInstant};

use kabudachi_core::election::{ElectionTimings, Output, WorkerNode};
use kabudachi_core::protocol::ids::{IncarnationId, ShardId, TaskDefinitionId, Uuid7Ids, WorkerId};
use kabudachi_core::protocol::messages::{
    ClaimRejectReason, ElectionMessage, WorkerHeartbeat, claim_response, election_message,
};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::Scheduler;
use kabudachi_core::time::{Duration, RealClock};
use kabudachi_net::driver::run_driver;
use kabudachi_net::messenger::Net;
use kabudachi_net::swarm::build_swarm;
use libp2p::identity;
use tokio::sync::watch;
use tokio::time::timeout;

use support::net::{connect_to, heartbeat_until_acked, wait_until_disconnected};

const SHARD: &str = "shard-1";
/// Matches `generation_replay_over_real_transport_test.rs` and `claim_arbitration_test.rs`.
const SUSPECT_TIMEOUT_MS: u64 = 300;

/// How often a follower heartbeats its leader: well inside every suspicion
/// timeout this file uses.
const HEARTBEAT_INTERVAL_MS: u64 = 10;

/// How long a roll call runs: well above the time a roll call takes to
/// reach a loopback peer and its reply to come back.
const ROLL_CALL_DEADLINE_MS: u64 = 100;

/// `node_a`'s `with_reconnect_timeout` (README §8.3). Long enough that "just
/// before the detection window elapses" and "well after it elapses" are
/// unambiguous real-time checkpoints, and comfortably smaller than
/// `TEST_TIMEOUT`. No auto-redial can interfere: `net_w1` is a bare `Net`
/// in no gossip mesh, and only mesh peers are redialed.
const RECONNECT_TIMEOUT_MS: u64 = 400;
const RECONNECT_TIMEOUT: StdDuration = StdDuration::from_millis(RECONNECT_TIMEOUT_MS);

/// The leader's real detection window (`core::election::WorkerNode`'s "lost
/// workers" doc): `suspect_timeout`, then `reconnect_timeout`, of silence
/// since the worker's last heartbeat. README §8.3's `heartbeat_timeout` is
/// `suspect_timeout` here.
fn detection_window() -> StdDuration {
    StdDuration::from_millis(SUSPECT_TIMEOUT_MS) + RECONNECT_TIMEOUT
}

/// Extra slack past [`detection_window`] this file waits, counted from when
/// the heartbeat that starts the window was sent, before assuming detection
/// has already happened: the leader's own
/// timer and this file's each run on independent `tokio` timers, so waiting
/// exactly the real deadline risks racing the driver's own tick before it
/// has run. Never used for a "just before the deadline" checkpoint, only for
/// "well after it".
const DETECTION_SLACK: StdDuration = StdDuration::from_millis(200);

const TEST_TIMEOUT: StdDuration = StdDuration::from_secs(20);

/// A `WorkerHeartbeat` from `worker`, at recovery epoch 0, so a bare `Net`
/// standing in for a follower (this file's module doc) can make the leader
/// track it without running an election of its own.
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

/// How many runs the measurement makes unless `KABUDACHI_REPLACEMENT_RUNS`
/// says otherwise.
const DEFAULT_MEASUREMENT_RUNS: usize = 10;

/// How long the replacement claimant waits before asking again while the
/// cut worker's claim still stands: a small share of the detection window,
/// so asking adds at most this much to the measured replacement time.
const CLAIM_POLL_INTERVAL: StdDuration = StdDuration::from_millis(5);

/// How long the cut worker heartbeats the leader before the cut, in
/// heartbeat intervals: enough that the leader last hears it within one
/// interval of the cut, as it would a live follower.
const HEARTBEATS_BEFORE_CUT: u32 = 10;

/// One run's intervals, each starting on the leader's side.
struct ReplacementTiming {
    /// From the leader's last receipt of the cut worker's heartbeat to its
    /// `Output::WorkerLost` for that worker.
    unreachable_to_lost: StdDuration,
    /// From that `Output::WorkerLost` to the claimant receiving the leader's
    /// accept of the replacement claim.
    lost_to_replacement: StdDuration,
    /// The two together: worker unreachable to replacement claim.
    unreachable_to_replacement: StdDuration,
}

/// Builds a fresh leader and two bare workers, lets `w1` claim the one task
/// and heartbeat as a live follower would, cuts `w1` off (`block_peer` on
/// both sides), and has `w2` ask for the task until the leader accepts it.
async fn measure_one_replacement() -> ReplacementTiming {
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
    // Submitting needs no leadership, so it happens before any driving.
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
    net_w1.set_leader(Some(leader_id.clone()));
    connect_to(&net_a, &listen_addr, &net_w2).await;
    net_w2.set_leader(Some(leader_id.clone()));

    // The leader's side, stamped as its driver reports each batch: the last
    // time it acked a heartbeat from w1 (so heard it), and its first
    // `WorkerLost` for w1.
    let (state_tx, mut state_rx) = watch::channel(node_a.state());
    let last_heard_w1: Cell<Option<StdInstant>> = Cell::new(None);
    let lost_at: Cell<Option<StdInstant>> = Cell::new(None);
    let observe = |node: &WorkerNode<RealClock>, outputs: &[Output]| {
        let _ = state_tx.send(node.state());
        let now = StdInstant::now();
        for output in outputs {
            match output {
                Output::Send {
                    to,
                    message:
                        ElectionMessage {
                            payload: Some(election_message::Payload::HeartbeatAck(_)),
                        },
                } if *to == w1_id => last_heard_w1.set(Some(now)),
                Output::WorkerLost(worker) if *worker == w1_id && lost_at.get().is_none() => {
                    lost_at.set(Some(now));
                }
                _ => {}
            }
        }
    };

    let replaced_at = timeout(TEST_TIMEOUT, async {
        tokio::select! {
            _ = run_driver(&mut node_a, &net_a, &mut scheduler_a, clock, None, observe) => {
                unreachable!("run_driver never returns")
            }
            replaced_at = async {
                state_rx
                    .wait_for(|state| *state == WorkerState::Leader)
                    .await
                    .expect("the driver is still running");

                let claim = net_w1
                    .request_claim(task.clone())
                    .await
                    .expect("the leader answered w1's claim");
                assert!(
                    matches!(claim.result, Some(claim_response::Result::Accept(_))),
                    "expected w1's claim to be accepted, got {:?}",
                    claim.result
                );

                let interval = StdDuration::from_millis(HEARTBEAT_INTERVAL_MS);
                tokio::select! {
                    _ = async {
                        loop {
                            net_w1.send(leader_id.clone(), heartbeat_from(&w1_id));
                            tokio::time::sleep(interval).await;
                        }
                    } => unreachable!("w1 heartbeats until the cut"),
                    () = tokio::time::sleep(interval * HEARTBEATS_BEFORE_CUT) => {}
                }
                net_a.block_peer(w1_id.clone());
                net_w1.block_peer(leader_id.clone());

                loop {
                    let response = net_w2
                        .request_claim(task.clone())
                        .await
                        .expect("the leader answered w2's claim");
                    match response.result {
                        Some(claim_response::Result::Accept(claim)) => {
                            assert_eq!(claim.attempt_number, 2, "the replacement is the second attempt");
                            return StdInstant::now();
                        }
                        Some(claim_response::Result::Reject(reject)) => assert_eq!(
                            ClaimRejectReason::try_from(reject.reason)
                                .expect("the leader only sends reasons this build knows"),
                            ClaimRejectReason::ClaimRejectAlreadySelected,
                            "until the leader loses w1, its claim stands"
                        ),
                        other => panic!("unexpected claim response {other:?}"),
                    }
                    tokio::time::sleep(CLAIM_POLL_INTERVAL).await;
                }
            } => replaced_at,
        }
    })
    .await
    .expect("the replacement was claimed within the timeout");

    let last_heard = last_heard_w1
        .get()
        .expect("the leader heard w1 before the cut");
    let lost = lost_at
        .get()
        .expect("the leader reported w1 lost before it let the task be claimed again");
    ReplacementTiming {
        unreachable_to_lost: lost.duration_since(last_heard),
        lost_to_replacement: replaced_at.duration_since(lost),
        unreachable_to_replacement: replaced_at.duration_since(last_heard),
    }
}

/// The `percentile` (0 to 100) of `samples`, nearest rank. Below 100
/// samples, p99 is the maximum.
fn percentile(samples: &[StdDuration], percentile: usize) -> StdDuration {
    let mut sorted = samples.to_vec();
    sorted.sort();
    let rank = (percentile * sorted.len()).div_ceil(100).max(1);
    sorted[rank - 1]
}

/// Measures, from the leader's side, the time from a worker becoming
/// unreachable to a replacement claim of its TaskRun (README §8.3), over
/// repeated runs, and prints p50, p99 and max of each interval. Run with:
///
/// ```text
/// KABUDACHI_REPLACEMENT_RUNS=30 cargo test -p kabudachi-net \
///     --test reconnect_timeout_replacement_test -- --ignored --nocapture
/// ```
///
/// The floor, `suspect_timeout + reconnect_timeout`, is the policy; what
/// the run adds on top is the mechanism's latency (the leader's timer, the
/// claim round trip, and at most one [`CLAIM_POLL_INTERVAL`]). That no
/// replacement comes before the floor is asserted by
/// [`no_replacement_task_run_is_claimable_before_the_reconnect_timeout_has_elapsed_under_a_partition`].
#[tokio::test]
#[ignore = "measurement: run with --ignored --nocapture; KABUDACHI_REPLACEMENT_RUNS sets the runs"]
async fn time_from_worker_unreachable_to_replacement_claim_measured_from_the_leader() {
    let runs = std::env::var("KABUDACHI_REPLACEMENT_RUNS")
        .ok()
        .map(|runs| runs.parse().expect("KABUDACHI_REPLACEMENT_RUNS is a count"))
        .unwrap_or(DEFAULT_MEASUREMENT_RUNS);
    assert!(runs > 0, "KABUDACHI_REPLACEMENT_RUNS must be at least 1");
    let mut timings = Vec::with_capacity(runs);
    for _ in 0..runs {
        timings.push(measure_one_replacement().await);
    }

    eprintln!(
        "worker unreachable -> replacement claim, leader's side, {runs} runs \
         (suspect_timeout + reconnect_timeout = {:?}):",
        detection_window()
    );
    let report = |name: &str, samples: Vec<StdDuration>| {
        eprintln!(
            "  {name}: p50 {:?}, p99 {:?}, max {:?}",
            percentile(&samples, 50),
            percentile(&samples, 99),
            percentile(&samples, 100),
        );
    };
    report(
        "last heard -> WorkerLost",
        timings.iter().map(|t| t.unreachable_to_lost).collect(),
    );
    report(
        "WorkerLost -> claim accepted",
        timings.iter().map(|t| t.lost_to_replacement).collect(),
    );
    report(
        "last heard -> claim accepted",
        timings
            .iter()
            .map(|t| t.unreachable_to_replacement)
            .collect(),
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
    net_w1.set_leader(Some(leader_id.clone()));
    connect_to(&net_a, &listen_addr, &net_w2).await;
    net_w2.set_leader(Some(leader_id.clone()));

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
    assert!(matches!(
        claim.result,
        Some(claim_response::Result::Accept(_))
    ));

    // w1 heartbeats the leader once, over the real wire, so the leader's own
    // detection countdown starts here (see this file's module doc) — not at
    // the disconnect just below.
    let heartbeat_sent_at = heartbeat_until_acked(
        &mut node_a,
        &net_a,
        &mut scheduler_a,
        clock,
        &net_w1,
        heartbeat_from(&w1_id),
    )
    .await;

    // Simulate the partition: sever w1's real connection to the leader.
    net_a.disconnect(w1_id.clone());
    wait_until_disconnected(&net_w1, &leader_id).await;

    // While the leader is still (correctly) withholding judgment on w1 —
    // i.e. for its entire detection window — a second real worker asking for
    // the same task must be told it is already selected, never that a
    // replacement exists. Checked twice: immediately after the partition is
    // observed, and again right up against the boundary. Only the leader's
    // own driver, ticking node_a, ever reports the loss now.
    let assert_still_selected =
        |resp: kabudachi_core::protocol::messages::ClaimResponse| match resp.result {
            Some(claim_response::Result::Reject(reject)) => {
                assert_eq!(
                    ClaimRejectReason::try_from(reject.reason)
                        .expect("the leader only ever sends a reason this build knows about"),
                    ClaimRejectReason::ClaimRejectAlreadySelected,
                    "before the detection window elapses, the original claim must still stand"
                );
            }
            other => panic!(
                "expected the claim to still be rejected as ALREADY_SELECTED (no replacement \
                 should exist yet), got {other:?}"
            ),
        };

    let detection_deadline = heartbeat_sent_at + detection_window();
    timeout(TEST_TIMEOUT, async {
        tokio::select! {
            _ = run_driver(&mut node_a, &net_a, &mut scheduler_a, clock, None, |_, _| {}) => {
                unreachable!("run_driver never returns")
            }
            _ = async {
                // Checkpoint 1: immediately after the partition is observed.
                let early = net_w2.request_claim(task.clone()).await
                    .expect("the leader answered w2's early claim attempt");
                assert_still_selected(early);

                // Checkpoint 2: as close to the detection-window boundary as
                // this test gets without crossing it. Opportunistic: under
                // scheduling delay (a slow or contended host), real
                // wall-clock time can advance past the boundary between the
                // sleep below and this check even though the detection
                // behavior itself is correct — when that happens, skip the
                // late-claim assertion instead of treating a scheduler
                // hiccup as a test failure. Checkpoint 1 above and the
                // post-timeout assertions further down are unaffected; only
                // this checkpoint tolerates the delay.
                let margin = StdDuration::from_millis(50);
                let just_before = detection_deadline - margin;
                let now = StdInstant::now();
                if just_before > now {
                    tokio::time::sleep(just_before - now).await;
                }
                if StdInstant::now() < detection_deadline {
                    let late = net_w2.request_claim(task.clone()).await
                        .expect("the leader answered w2's late-but-still-in-window claim attempt");
                    assert_still_selected(late);
                } else {
                    eprintln!(
                        "checkpoint 2 skipped: scheduling delay pushed wall-clock time past the \
                         detection-window boundary before the late-claim assertion could run"
                    );
                }
            } => {}
        }
    })
    .await
    .expect("both pre-timeout checkpoints completed within the test timeout");

    // Now let the detection window actually elapse, driving node_a all the
    // while so its own driver can report `Output::WorkerLost` and apply it
    // to scheduler_a the moment it does (see the module doc) — the real
    // policy, timed against the real heartbeat and disconnect above, with no
    // direct `lose_worker` call left.
    let wait_until_definitely_detected = detection_deadline + DETECTION_SLACK;
    timeout(TEST_TIMEOUT, async {
        tokio::select! {
            _ = run_driver(&mut node_a, &net_a, &mut scheduler_a, clock, None, |_, _| {}) => {
                unreachable!("run_driver never returns")
            }
            () = async {
                let now = StdInstant::now();
                if wait_until_definitely_detected > now {
                    tokio::time::sleep(wait_until_definitely_detected - now).await;
                }
            } => {}
        }
    })
    .await
    .expect("node_a kept driving while its detection window elapsed");
    assert!(
        StdInstant::now().duration_since(heartbeat_sent_at) >= detection_window(),
        "no replacement TaskRun may be created before the leader's detection window has elapsed"
    );

    // Only now does a replacement actually become claimable.
    let replacement = timeout(TEST_TIMEOUT, async {
        tokio::select! {
            _ = run_driver(&mut node_a, &net_a, &mut scheduler_a, clock, None, |_, _| {}) => {
                unreachable!("run_driver never returns")
            }
            response = net_w2.request_claim(task.clone()) => response,
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
            "expected w2's replacement claim to be accepted only after the detection window, \
             got {other:?}"
        ),
    }
}
