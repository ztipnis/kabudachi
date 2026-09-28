//! README §8.3 and §25.1.9 over real sockets: a worker cut off from its
//! leader is told to abort its TaskRuns before that leader can replay them,
//! and a worker its leader hears again withdraws that order.
//!
//! Three voters elect a leader over real loopback TCP. One follower is then
//! partitioned from both others with `Net::block_peer` on both sides of each
//! pair (see its doc), while it keeps running. The leader, still a quorum
//! with the healthy follower, reports the cut-off follower lost
//! (`Output::WorkerLost`, which replays its runs) a suspicion timeout and a
//! reconnect timeout after it last heard it. The follower reports an
//! `Output::AbortDeadline` well before that, since it counts from the last
//! heartbeat its leader provably heard (ruling E13-R1 in the E13 notes).
//!
//! All nodes, schedulers and drivers share one `RealClock`, so an instant
//! one node reports means the same moment on another's. The test asserts
//! only the order of those instants, which load on the host can delay but
//! not reorder: the follower's deadline comes from its last echoed
//! heartbeat, no later than the leader's last receipt of one, and the
//! leader's report can only run late.

mod support;

use std::time::Duration as StdDuration;

use kabudachi_core::configuration::{Configuration, Generation, Single};
use kabudachi_core::election::{ElectionTimings, KnownConfiguration, Output, WorkerNode};
use kabudachi_core::protocol::ids::{IncarnationId, ShardId, Uuid7Ids, WorkerId};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::Scheduler;
use kabudachi_core::time::{Clock, Duration, RealClock};
use kabudachi_net::messenger::Net;
use kabudachi_net::swarm::build_swarm;
use libp2p::identity::Keypair;
use tokio::sync::watch;
use tokio::time::timeout;

use support::election::{
    Batch, abort_deadline_as_of, built_on_one_tick, drive_three_until, heard_by_granted_leader,
    leads_with_grant, recorder, wait_until,
};
use support::net::connect_full_mesh;

const SHARD: &str = "shard-1";

/// Every node's suspicion timeout. Long enough that a healthy follower,
/// heartbeating every `HEARTBEAT_INTERVAL_MS`, is never nine tenths of one
/// short of an echoed ack even when a loaded host delays its driver.
const SUSPECT_TIMEOUT_MS: u64 = 500;

/// Every node's reconnect timeout: every worker of a shard must use the
/// same one (see `WorkerNode::with_reconnect_timeout`). The cut-off
/// follower reports its deadline nine tenths of a suspicion timeout after
/// its last echoed heartbeat, and the leader replays a suspicion timeout and
/// this long after its last receipt of one, so this is the slack a late
/// follower driver has before the test's order no longer holds.
const RECONNECT_TIMEOUT_MS: u64 = 500;

const HEARTBEAT_INTERVAL_MS: u64 = 20;

/// Well above the time a roll call takes to reach a loopback peer and its
/// reply to come back.
const ROLL_CALL_DEADLINE_MS: u64 = 100;

const TEST_TIMEOUT: StdDuration = StdDuration::from_secs(20);

/// `my_id`'s node, one of the three voters of the shard's configuration.
fn make_node(clock: RealClock, my_id: WorkerId) -> WorkerNode<RealClock> {
    WorkerNode::new(
        my_id.clone(),
        IncarnationId::new(format!("{}-incarnation-0", my_id.as_str())),
        ShardId::new(SHARD),
        clock,
        KnownConfiguration {
            configuration: Configuration::single(Single {
                generation: Generation::genesis(0),
                base: Generation::genesis(0),
                voter_count: 3,
            }),
            admission: Some(Generation::genesis(0)),
        },
        None,
        ElectionTimings::new(
            Duration::from_millis(SUSPECT_TIMEOUT_MS),
            Duration::from_millis(HEARTBEAT_INTERVAL_MS),
        )
        .with_roll_call_deadline(Duration::from_millis(ROLL_CALL_DEADLINE_MS)),
    )
    .with_reconnect_timeout(Duration::from_millis(RECONNECT_TIMEOUT_MS))
}

/// Blocks or unblocks every pair of `nets` across the cut between the one
/// at `cut_off` and the rest, on both sides of each pair.
fn set_partitioned(nets: [&Net; 3], ids: &[WorkerId], cut_off: usize, partitioned: bool) {
    for other in (0..3).filter(|i| *i != cut_off) {
        for (net, peer) in [(nets[cut_off], &ids[other]), (nets[other], &ids[cut_off])] {
            if partitioned {
                net.block_peer(peer.clone());
            } else {
                net.unblock_peer(peer.clone());
            }
        }
    }
}

#[tokio::test]
async fn a_follower_cut_off_from_its_leader_must_abort_before_the_leader_replays_its_runs() {
    let net_a = Net::new(build_swarm(Keypair::generate_ed25519()));
    let net_b = Net::new(build_swarm(Keypair::generate_ed25519()));
    let net_c = Net::new(build_swarm(Keypair::generate_ed25519()));
    let nets = [&net_a, &net_b, &net_c];
    let ids = connect_full_mesh(&nets).await;

    let clock = RealClock::new();
    let mut nodes = built_on_one_tick(&clock, || {
        [0, 1, 2].map(|i| make_node(clock, ids[i].clone()))
    });
    let mut schedulers = [(); 3].map(|()| Scheduler::new(clock, Uuid7Ids));
    let (txs, rxs): (Vec<_>, Vec<_>) = (0..3).map(|_| watch::channel(Vec::<Batch>::new())).unzip();
    let observers = [0, 1, 2].map(|i| recorder(clock, txs[i].clone()));
    let timeline = |i: usize| rxs[i].borrow().clone();

    timeout(
        TEST_TIMEOUT,
        drive_three_until(
            &mut nodes,
            nets,
            &mut schedulers,
            clock,
            [None, None, None],
            observers,
            async {
                // ---- A leader with a grant, heard provably by both followers.
                let mut leader = 0;
                wait_until(|| match (0..3).find(|i| leads_with_grant(&timeline(*i))) {
                    Some(i) => {
                        leader = i;
                        true
                    }
                    None => false,
                })
                .await;
                let followers: Vec<usize> = (0..3).filter(|i| *i != leader).collect();
                let (cut_off, healthy) = (followers[0], followers[1]);
                let since = clock.now();
                wait_until(|| {
                    followers.iter().all(|f| {
                        heard_by_granted_leader(
                            &timeline(leader),
                            &ids[leader],
                            &timeline(*f),
                            &ids[*f],
                            since,
                        )
                    })
                })
                .await;

                // ---- Cut one follower off from both others; it keeps running.
                let cut_at = clock.now();
                set_partitioned(nets, &ids, cut_off, true);
                let lost_at = |leader_timeline: &[Batch]| {
                    leader_timeline
                        .iter()
                        .find(|batch| {
                            batch.at >= cut_at
                                && batch
                                    .outputs
                                    .contains(&Output::WorkerLost(ids[cut_off].clone()))
                        })
                        .map(|batch| batch.at)
                };
                wait_until(|| lost_at(&timeline(leader)).is_some()).await;
                let leader_timeline = timeline(leader);
                let t = lost_at(&leader_timeline).expect("waited for above");

                assert!(
                    leader_timeline
                        .iter()
                        .filter(|batch| batch.at >= cut_at && batch.at <= t)
                        .all(|batch| batch.state == WorkerState::Leader),
                    "the leader kept leading with the healthy follower as its quorum"
                );
                // Its deadline's value is what bounds the abort; on a loaded
                // host its driver may report it after the leader's batch, so
                // wait for the report rather than require it by then.
                let first_deadline_since_cut = |follower_timeline: &[Batch]| {
                    follower_timeline
                        .iter()
                        .filter(|batch| batch.at >= cut_at)
                        .flat_map(|batch| &batch.outputs)
                        .find_map(|output| match output {
                            Output::AbortDeadline(Some(by)) => Some(*by),
                            _ => None,
                        })
                };
                wait_until(|| first_deadline_since_cut(&timeline(cut_off)).is_some()).await;
                let by = first_deadline_since_cut(&timeline(cut_off)).expect("waited for above");
                assert!(
                    by < t,
                    "the cut-off follower must abort by {by:?}, before the leader replayed its \
                     runs at {t:?}"
                );
                // A follower its leader keeps hearing is never made to abort:
                // any deadline it reports (a slow driver can miss an echo for a
                // while) is withdrawn or still ahead when the leader replays.
                let healthy_deadline = abort_deadline_as_of(&timeline(healthy), t).flatten();
                assert!(
                    healthy_deadline.is_none_or(|by| by > t),
                    "a follower its leader keeps hearing had to abort by {healthy_deadline:?}"
                );

                // ---- Heal: once the leader hears the follower again and it
                // ---- accepts that leader's echo, it withdraws the deadline.
                set_partitioned(nets, &ids, cut_off, false);
                wait_until(|| {
                    let cut_off_timeline = timeline(cut_off);
                    abort_deadline_as_of(&cut_off_timeline, clock.now()) == Some(None)
                        && heard_by_granted_leader(
                            &timeline(leader),
                            &ids[leader],
                            &cut_off_timeline,
                            &ids[cut_off],
                            t,
                        )
                })
                .await;
            },
        ),
    )
    .await
    .expect("the scenario ran to its end within the timeout");
}
