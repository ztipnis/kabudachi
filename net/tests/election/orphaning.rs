//! Orphaning over real sockets (ADR-0001 decision 12): a worker that loses
//! the coordination authority, and only the authority, fences itself before
//! its registration lapses, is told to abort its TaskRuns within the
//! reconnect timeout, and resumes where it was once it reaches the authority
//! again and finds the shard's recovery epoch unchanged.
//!
//! Three voters over real loopback TCP share one `FaultingAuthority`, each
//! through a handle of its own, and each registers with it from its first
//! step. A leader is elected and, once the authority has warmed up, takes
//! the recovery fence and holds a grant. One follower's handle is then cut
//! (`set_reachable(false)`) while its sockets stay up. It renews its
//! registration every third of the TTL, so its last successful renewal came
//! at most a third of a TTL before the cut, and it fences itself nine tenths
//! of a TTL after that renewal, before the authority's TTL runs out.
//!
//! The leader keeps leading throughout: the other follower and the leader
//! are a quorum, and both still reach the authority. The rejoin case, where
//! the epoch has moved on meanwhile, is left to the split-brain test.
//!
//! All nodes, schedulers and drivers share one `RealClock` (see
//! `run_driver`), and the authority reads it too.


use std::sync::{Arc, Mutex};
use std::time::Duration as StdDuration;

use kabudachi_core::configuration::{Configuration, Generation, Single};
use kabudachi_core::coordination_authority::{CoordinationAuthority, RecoveryEpoch};
use kabudachi_net::driver::SharedAuthority;
use kabudachi_core::election::{
    AuthorityTimings, ElectionTimings, Entry, Identity, Input, KnownConfiguration, Output, Step,
    WorkerNode,
};
use kabudachi_core::protocol::ids::{IncarnationId, ShardId, Uuid7Ids, WorkerId};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::Scheduler;
use kabudachi_core::time::{Clock, Duration, Instant, RealClock};
use kabudachi_net::messenger::Net;
use kabudachi_testkit::{FaultingAuthority, StepRecord};
use tokio::sync::watch;
use tokio::time::timeout;

use crate::support::election::{
    abort_deadline_as_of, built_on_one_tick, drive_three_until, heard_by_granted_leader,
    leads_with_grant, recorder, wait_until,
};
use crate::support::net::connect_full_mesh;

const SHARD: &str = "shard-1";

/// The authority's registration, fence and warm-up TTL, and every node's.
/// The leader holds no grant until the authority has warmed up, one TTL
/// after it was built. A node fences itself a tenth of a TTL before the
/// authority lets its registration lapse, and on a loaded host its driver
/// can tick it late: a tenth of this is the lateness the "still registered"
/// check tolerates.
const AUTHORITY_TTL_MS: u64 = 1_500;

const SUSPECT_TIMEOUT_MS: u64 = 500;

/// Every node's reconnect timeout (see `ElectionTimings::reconnect_timeout`).
const RECONNECT_TIMEOUT_MS: u64 = 500;

const HEARTBEAT_INTERVAL_MS: u64 = 20;

/// Well above the time a roll call takes to reach a loopback peer and its
/// reply to come back.
const ROLL_CALL_DEADLINE_MS: u64 = 100;

const TEST_TIMEOUT: StdDuration = StdDuration::from_secs(20);

/// `my_id`'s node, one of the three voters of the shard's configuration at
/// recovery epoch 0, registering with an authority whose TTL is
/// `AUTHORITY_TTL_MS`.
fn make_node(clock: RealClock, my_id: WorkerId) -> WorkerNode<RealClock> {
    WorkerNode::start(
        Identity {
            id: my_id.clone(),
            incarnation: IncarnationId::new(format!("{}-incarnation-0", my_id.as_str())),
            shard: ShardId::new(SHARD),
            timings: ElectionTimings::new(
                Duration::from_millis(SUSPECT_TIMEOUT_MS),
                Duration::from_millis(HEARTBEAT_INTERVAL_MS),
            )
            .with_roll_call_deadline(Duration::from_millis(ROLL_CALL_DEADLINE_MS))
            .with_reconnect_timeout(Duration::from_millis(RECONNECT_TIMEOUT_MS)),
        },
        Entry::Known(KnownConfiguration {
            configuration: Configuration::single(Single {
                generation: Generation::genesis(0),
                base: Generation::genesis(0),
                voter_count: 3,
            }).expect("valid"),
            admission: Some(Generation::genesis(0)),
        }),
        clock,
        Some(AuthorityTimings {
            ttl: Duration::from_millis(AUTHORITY_TTL_MS),
        }),
    )
    .0
}

/// What a node's `observe` saw the first time it found the node `Fenced`:
/// when, on the node's clock, and whether the authority still listed the
/// node as live at that instant.
#[derive(Debug, Clone, Copy)]
struct FirstFenced {
    at: Instant,
    still_registered: bool,
}

/// `duration` less its tenth, rounded up, for clock drift: how much of it a
/// node counts on (see `Output::AbortDeadline`).
fn less_drift(duration: Duration) -> Duration {
    let ticks = duration.as_ticks();
    Duration::from_ticks(ticks - ticks.div_ceil(10))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_follower_that_loses_the_authority_fences_itself_in_time_and_resumes_on_reconnect() {
    let net_a = Net::new();
    let net_b = Net::new();
    let net_c = Net::new();
    let nets = [&net_a, &net_b, &net_c];
    let ids = connect_full_mesh(&nets).await;

    let clock = RealClock::new();
    let shard = ShardId::new(SHARD);
    let authority = FaultingAuthority::new(clock, Duration::from_millis(AUTHORITY_TTL_MS));
    authority
        .compare_and_swap_recovery_epoch(&shard, None, RecoveryEpoch::new(0, 0))
        .expect("the shard has no epoch yet, so create-if-absent succeeds");
    let handles = [(); 3].map(|()| authority.for_another_worker());

    let mut nodes = built_on_one_tick(&clock, || {
        [0, 1, 2].map(|i| make_node(clock, ids[i].clone()))
    });
    let mut schedulers = [(); 3].map(|()| Scheduler::new(clock, Uuid7Ids));
    let (txs, rxs): (Vec<_>, Vec<_>) = (0..3).map(|_| watch::channel(Vec::<StepRecord>::new())).unzip();
    let first_fenced: [Arc<Mutex<Option<FirstFenced>>>; 3] = Default::default();
    let observers = [0, 1, 2].map(|i| {
        let mut record = recorder(clock, txs[i].clone());
        let first_fenced = Arc::clone(&first_fenced[i]);
        let me = ids[i].clone();
        let shard = shard.clone();
        // A handle of its own, so reading the registrations here never
        // goes through the one the test cuts.
        let authority = authority.for_another_worker();
        move |node: &WorkerNode<RealClock>, input: Option<&Input>, step: &Step| {
            record(node, input, step);
            let mut first_fenced = first_fenced.lock().unwrap();
            if node.state() == WorkerState::Fenced && first_fenced.is_none() {
                let live = authority
                    .live_registrations(&shard)
                    .expect("the observer's handle is never cut");
                *first_fenced = Some(FirstFenced {
                    at: clock.now(),
                    still_registered: live.addresses().contains_key(&me),
                });
            }
        }
    });
    let timeline = |i: usize| rxs[i].borrow().clone();
    let authorities = [0, 1, 2].map(|i| Some(Arc::new(handles[i].clone()) as SharedAuthority));

    timeout(
        TEST_TIMEOUT,
        drive_three_until(
            &mut nodes,
            nets,
            &mut schedulers,
            clock,
            authorities,
            observers,
            async {
                // ---- A leader holding a grant, which it gets once the
                // ---- authority has warmed up and granted it the fence.
                let mut leader = 0;
                wait_until(|| match (0..3).find(|i| leads_with_grant(&timeline(*i))) {
                    Some(i) => {
                        leader = i;
                        true
                    }
                    None => false,
                })
                .await;
                let orphan = (0..3).find(|i| *i != leader).expect("three nodes");

                // ---- Cut the follower off from the authority alone.
                let cut_at = clock.now();
                handles[orphan].set_reachable(false);
                wait_until(|| first_fenced[orphan].lock().unwrap().is_some()).await;
                let fenced = (*first_fenced[orphan].lock().unwrap()).expect("waited for above");
                assert!(
                    fenced.still_registered,
                    "the follower fenced itself only after the authority had let its \
                     registration lapse"
                );
                let reconnect = Duration::from_millis(RECONNECT_TIMEOUT_MS);
                match abort_deadline_as_of(&timeline(orphan), fenced.at) {
                    Some(Some(by)) => {
                        eprintln!(
                            "authority cut at {cut_at:?}; fenced at {:?}; aborts by {by:?}",
                            fenced.at
                        );
                        assert!(
                            by <= fenced.at + less_drift(reconnect),
                            "fenced at {:?}, the follower must abort by nine tenths of a \
                             reconnect timeout later, not at {by:?}",
                            fenced.at
                        );
                    }
                    reported => panic!(
                        "fenced at {:?}, the follower's last reported abort deadline was \
                         {reported:?}, not a deadline",
                        fenced.at
                    ),
                }

                // ---- Restore its authority: it resumes at the same epoch.
                handles[orphan].set_reachable(true);
                let resumed_at = |orphan_timeline: &[StepRecord]| {
                    orphan_timeline
                        .iter()
                        .find(|record| {
                            record.at >= fenced.at
                                && record
                                    .outputs
                                    .contains(&Output::StateChanged(WorkerState::Active))
                        })
                        .map(|record| record.at)
                };
                wait_until(|| resumed_at(&timeline(orphan)).is_some()).await;
                let orphan_timeline = timeline(orphan);
                let resumed = resumed_at(&orphan_timeline).expect("waited for above");
                eprintln!("resumed at {resumed:?}");
                let since_fenced: Vec<&StepRecord> = orphan_timeline
                    .iter()
                    .filter(|record| record.at >= fenced.at && record.at <= resumed)
                    .collect();
                assert!(
                    since_fenced.iter().all(|record| record.recovery_epoch == 0),
                    "the epoch never moved, so the follower resumes at it"
                );
                assert!(
                    since_fenced.iter().all(|record| {
                        !record.outputs.iter().any(|output| {
                            matches!(output, Output::StateChanged(state) if !matches!(
                                state,
                                WorkerState::Fenced | WorkerState::Active
                            ))
                        })
                    }),
                    "it went straight from Fenced back to Active, not through a rejoin"
                );

                // ---- The leader hears it again, and it withdraws the deadline.
                wait_until(|| {
                    let orphan_timeline = timeline(orphan);
                    heard_by_granted_leader(
                        &timeline(leader),
                        &ids[leader],
                        &orphan_timeline,
                        &ids[orphan],
                        resumed,
                    ) && abort_deadline_as_of(&orphan_timeline, clock.now()) == Some(None)
                })
                .await;

                assert!(
                    timeline(leader)
                        .iter()
                        .filter(|record| record.at >= cut_at)
                        .all(|record| record.state == WorkerState::Leader),
                    "the leader kept leading while one follower lost the authority"
                );
            },
        ),
    )
    .await
    .expect("the scenario ran to its end within the timeout");
}
