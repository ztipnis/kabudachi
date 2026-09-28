//! E5's authority split-brain scenarios (ADR-0001 decisions 11 and 12,
//! README §15), run again over real sockets: three voters, each on its own
//! libp2p swarm over loopback TCP and driven by its own `run_driver`, share
//! one coordination authority (a `FaultingAuthority`) through a handle each.
//! `core/tests/scenario_catastrophic_authority_test.rs` proves the same
//! scenarios in the simulator; this file proves that the driver performs the
//! authority calls, rejoins a fenced worker through the workers the
//! authority lists, and keeps grants exclusive when the partition is a real
//! one (`Net::block_peer`) and the timers are real ones.
//!
//! Every scenario checks the property the simulator's `first_grant_overlap`
//! checks: no two nodes ever hold an unexpired grant at the same instant.
//! Each `Output::Grant` a node reports is recorded with the instant its
//! driver's batch was observed, on the one `RealClock` every node,
//! scheduler, driver and the authority read, so instants are comparable
//! across nodes. A grant reported at `r` with lease end `v` covers `[r, v)`,
//! cut short by the node's next grant report; an unbounded one lasts until
//! that next report.
//!
//! The authority's TTL is 1 s, so a registration lapses, a fence ends and a
//! warm-up passes within about a second, and the suspicion timeout (300 ms)
//! is well inside it, as ADR-0001's defaults (30 s against 2 s) are. The
//! authority starts warming up as it is built, so the first leader has its
//! first grant only once that TTL has passed.
//!
//! Not repeated here: E5's "a worker with no authority never orphans
//! itself". It asks nothing of the driver beyond performing no authority
//! call, which a node built with no authority timings never asks for; the
//! core simulator proves it, and real sockets would add nothing to it.

mod support;

use std::sync::Arc;
use std::time::Duration as StdDuration;

use kabudachi_core::configuration::{Configuration, Generation, Single};
use kabudachi_core::coordination_authority::{CoordinationAuthority, RecoveryEpoch};
use kabudachi_core::election::{
    AuthorityTimings, ElectionTimings, KnownConfiguration, Output, WorkerNode,
};
use kabudachi_core::protocol::ids::{IncarnationId, ShardId, Uuid7Ids, WorkerId};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::{LeadershipGrant, LeaseEnd, Scheduler};
use kabudachi_core::time::{Clock, Duration, Instant, RealClock};
use kabudachi_net::driver::{SharedAuthority, run_driver};
use kabudachi_net::messenger::Net;
use kabudachi_net::swarm::build_swarm;
use kabudachi_testkit::FaultingAuthority;
use libp2p::identity::Keypair;
use tokio::sync::watch;
use tokio::time::timeout;

use support::election::built_on_one_tick;
use support::net::connect_full_mesh;

const SHARD: &str = "shard-1";
const VOTERS: usize = 3;

/// The authority's TTL, and the one every node expects (see
/// `AuthorityTimings::ttl`).
const TTL_MS: u64 = 1_000;

/// Every node's suspicion timeout: well inside the TTL, and well above what
/// a roll call takes over loopback.
const SUSPECT_TIMEOUT_MS: u64 = 300;

/// How often a follower heartbeats its leader: two intervals and a round
/// trip stay well inside nine tenths of the suspicion timeout, so a leader
/// whose followers are alive keeps its quorum-contact lease on a loaded
/// host too.
const HEARTBEAT_INTERVAL_MS: u64 = 20;

/// How long a roll call runs: well above a loopback round trip.
const ROLL_CALL_DEADLINE_MS: u64 = 100;

/// How long a scenario waits for any one thing it expects. Each is expected
/// within a few TTLs; this is the "something is broken" backstop, generous
/// for a host shared with parallel builds.
const WAIT_TIMEOUT: StdDuration = StdDuration::from_secs(20);

type Node = WorkerNode<RealClock>;

/// One grant report: `node` reported `grant` in the batch observed at `at`.
#[derive(Debug, Clone, Copy)]
struct GrantReport {
    node: usize,
    at: Instant,
    grant: Option<LeadershipGrant>,
}

/// What a node looked like after the last batch its driver ran.
#[derive(Debug, Clone, PartialEq, Eq)]
struct NodeView {
    state: WorkerState,
    recovery_epoch: u64,
    known_leader: Option<WorkerId>,
    pending: bool,
    term: u64,
    highest_term_seen: u64,
}

impl NodeView {
    fn of(node: &Node) -> Self {
        NodeView {
            state: node.state(),
            recovery_epoch: node.recovery_epoch(),
            known_leader: node.known_leader().map(|(leader, _)| leader),
            pending: node.is_pending_member(),
            term: node.term(),
            highest_term_seen: node.highest_term_seen(),
        }
    }
}

/// Everything the drivers' observers have seen, indexed by node.
#[derive(Debug)]
struct Observed {
    views: Vec<NodeView>,
    /// Every grant report, in the order observed.
    grants: Vec<GrantReport>,
    /// Every state change, with the node and the instant it was observed.
    states: Vec<(usize, Instant, WorkerState)>,
}

impl Observed {
    fn record(&mut self, node_index: usize, at: Instant, node: &Node, outputs: &[Output]) {
        self.views[node_index] = NodeView::of(node);
        for output in outputs {
            match output {
                Output::Grant(grant) => self.grants.push(GrantReport {
                    node: node_index,
                    at,
                    grant: *grant,
                }),
                Output::StateChanged(state) => self.states.push((node_index, at, *state)),
                _ => {}
            }
        }
    }

    /// The grant `node` holds at `now`: the last one it reported, if its
    /// lease has not ended by `now`.
    fn grant_at(&self, node: usize, now: Instant) -> Option<LeadershipGrant> {
        let last = self
            .grants
            .iter()
            .rev()
            .find(|report| report.node == node)?;
        last.grant.filter(|grant| match grant.valid_until {
            LeaseEnd::Unbounded => true,
            LeaseEnd::At(end) => now < end,
        })
    }

    /// Whether `node` is `Leader` and holds a grant at `now`.
    fn leads_with_grant(&self, node: usize, now: Instant) -> bool {
        self.views[node].state == WorkerState::Leader && self.grant_at(node, now).is_some()
    }

    /// Every stretch of time some node held a grant, as `(node, start,
    /// end)`: from the report to the earlier of its lease end and the
    /// node's next report. `end` is `None` for a grant nothing has ended
    /// yet.
    fn grant_intervals(&self) -> Vec<(usize, Instant, Option<Instant>)> {
        let mut intervals = Vec::new();
        for (i, report) in self.grants.iter().enumerate() {
            let Some(grant) = report.grant else {
                continue;
            };
            let next_report = self.grants[i + 1..]
                .iter()
                .find(|later| later.node == report.node)
                .map(|later| later.at);
            let lease_end = match grant.valid_until {
                LeaseEnd::Unbounded => None,
                LeaseEnd::At(end) => Some(end),
            };
            let end = match (lease_end, next_report) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            };
            if end.is_none_or(|end| report.at < end) {
                intervals.push((report.node, report.at, end));
            }
        }
        intervals
    }

    /// Two grants of different nodes that covered one instant, if any.
    fn first_grant_overlap(&self) -> Option<[(usize, Instant, Option<Instant>); 2]> {
        let intervals = self.grant_intervals();
        for (i, a) in intervals.iter().enumerate() {
            for b in &intervals[i + 1..] {
                let a_before_b_ends = b.2.is_none_or(|b_end| a.1 < b_end);
                let b_before_a_ends = a.2.is_none_or(|a_end| b.1 < a_end);
                if a.0 != b.0 && a_before_b_ends && b_before_a_ends {
                    return Some([*a, *b]);
                }
            }
        }
        None
    }
}

/// The three workers under test and the handles a scenario acts through.
/// The nodes and schedulers themselves are held by their drivers.
struct Cluster {
    clock: RealClock,
    ids: Vec<WorkerId>,
    nets: Vec<Net>,
    /// Each node's own connection to the authority, as its driver uses it.
    handles: Vec<FaultingAuthority<RealClock>>,
    /// A connection to the authority that no node uses, for a scenario to
    /// read it, and to take the whole authority down or flush it.
    authority: FaultingAuthority<RealClock>,
    observed: watch::Receiver<Observed>,
}

impl Cluster {
    /// Waits until `holds` is true of what the drivers have observed (and
    /// the clock's reading), checked after every batch any driver runs.
    async fn wait_until(&self, what: &str, mut holds: impl FnMut(&Observed, Instant) -> bool) {
        let mut observed = self.observed.clone();
        let clock = self.clock;
        let waited = timeout(
            WAIT_TIMEOUT,
            observed.wait_for(|observed| holds(observed, clock.now())),
        )
        .await;
        if !matches!(waited, Ok(Ok(_))) {
            let observed = self.observed.borrow();
            panic!(
                "{what} within the timeout; the nodes were {:#?}, after these state changes \
                 (node, instant, state): {:?}",
                observed.views, observed.states
            );
        }
    }

    /// Cuts `a` and `b` off from each other, blocking each on the other's
    /// side (see `Net::block_peer` for why both).
    fn block(&self, a: usize, b: usize) {
        self.nets[a].block_peer(self.ids[b].clone());
        self.nets[b].block_peer(self.ids[a].clone());
    }

    fn unblock(&self, a: usize, b: usize) {
        self.nets[a].unblock_peer(self.ids[b].clone());
        self.nets[b].unblock_peer(self.ids[a].clone());
    }

    /// Asserts that no two nodes have held an unexpired grant at once so
    /// far.
    fn assert_no_grant_overlap(&self) {
        let observed = self.observed.borrow();
        assert_eq!(
            observed.first_grant_overlap(),
            None,
            "two nodes held an unexpired grant at once; every grant report: {:#?}",
            observed.grants
        );
    }

    fn authority_epoch(&self) -> Option<u64> {
        self.authority
            .read_recovery_epoch(&ShardId::new(SHARD))
            .expect("the test's own handle reaches the authority while it is up")
            .map(|epoch| epoch.number)
    }
}

/// `id`'s node: one of the shard's three voters, at recovery epoch 0, with
/// the authority.
fn voter(clock: RealClock, id: &WorkerId) -> Node {
    WorkerNode::new(
        id.clone(),
        IncarnationId::new(format!("{}-incarnation-0", id.as_str())),
        ShardId::new(SHARD),
        clock,
        KnownConfiguration {
            configuration: Configuration::single(Single {
                generation: Generation::genesis(0),
                base: Generation::genesis(0),
                voter_count: VOTERS,
            }),
            admission: Some(Generation::genesis(0)),
        },
        Some(AuthorityTimings {
            ttl: Duration::from_millis(TTL_MS),
        }),
        ElectionTimings::new(
            Duration::from_millis(SUSPECT_TIMEOUT_MS),
            Duration::from_millis(HEARTBEAT_INTERVAL_MS),
        )
        .with_roll_call_deadline(Duration::from_millis(ROLL_CALL_DEADLINE_MS)),
    )
}

/// Builds the three voters over a full mesh of real sockets, with the shard
/// at epoch 0 at the authority, drives them until one leads with a grant
/// and the other two follow it, and then runs `scenario` with the cluster
/// and the leader's index while the drivers keep running. Once `scenario`
/// returns, checks that no two nodes ever held a grant at once (a scenario
/// may also check it on the way, see `Cluster::assert_no_grant_overlap`).
async fn with_elected_cluster(scenario: impl AsyncFnOnce(&Cluster, usize)) {
    let clock = RealClock::new();
    let authority = FaultingAuthority::new(clock, Duration::from_millis(TTL_MS));
    authority
        .compare_and_swap_recovery_epoch(&ShardId::new(SHARD), None, RecoveryEpoch::new(0, 0))
        .expect("a fresh authority holds no epoch, so create-if-absent succeeds");

    let nets: Vec<Net> = (0..VOTERS)
        .map(|_| Net::new(build_swarm(Keypair::generate_ed25519())))
        .collect();
    let ids = connect_full_mesh(&nets.iter().collect::<Vec<_>>()).await;
    let handles: Vec<_> = ids.iter().map(|_| authority.for_another_worker()).collect();

    let mut nodes: [Node; VOTERS] =
        built_on_one_tick(&clock, || std::array::from_fn(|i| voter(clock, &ids[i])));
    let mut schedulers: [_; VOTERS] = std::array::from_fn(|_| Scheduler::new(clock, Uuid7Ids));
    let (observations, observed) = watch::channel(Observed {
        views: nodes.iter().map(NodeView::of).collect(),
        grants: Vec::new(),
        states: Vec::new(),
    });
    let cluster = Cluster {
        clock,
        ids,
        nets,
        handles,
        authority,
        observed,
    };

    let observer = |node_index: usize| {
        let observations = &observations;
        move |node: &Node, outputs: &[Output]| {
            let at = clock.now();
            observations.send_modify(|observed| observed.record(node_index, at, node, outputs));
        }
    };
    let authority_of = |node_index: usize| -> Option<SharedAuthority> {
        Some(Arc::new(cluster.handles[node_index].clone()))
    };
    let [node_0, node_1, node_2] = &mut nodes;
    let [scheduler_0, scheduler_1, scheduler_2] = &mut schedulers;

    tokio::select! {
        _ = run_driver(node_0, &cluster.nets[0], scheduler_0, clock, authority_of(0), observer(0)) => {
            unreachable!("run_driver never returns")
        }
        _ = run_driver(node_1, &cluster.nets[1], scheduler_1, clock, authority_of(1), observer(1)) => {
            unreachable!("run_driver never returns")
        }
        _ = run_driver(node_2, &cluster.nets[2], scheduler_2, clock, authority_of(2), observer(2)) => {
            unreachable!("run_driver never returns")
        }
        () = async {
            let leader = elected_leader(&cluster).await;
            scenario(&cluster, leader).await;
        } => {}
    }

    cluster.assert_no_grant_overlap();
}

/// Waits until one node leads with a grant and the other two are `Active`
/// under it, and returns the leader's index.
async fn elected_leader(cluster: &Cluster) -> usize {
    let mut leader = None;
    cluster
        .wait_until(
            "one node leads with a grant and the others follow it",
            |observed, now| {
                leader = (0..VOTERS).find(|&node| observed.leads_with_grant(node, now));
                leader.is_some_and(|leader| {
                    (0..VOTERS).filter(|&node| node != leader).all(|node| {
                        let view = &observed.views[node];
                        view.state == WorkerState::Active
                            && view.known_leader.as_ref() == Some(&cluster.ids[leader])
                    })
                })
            },
        )
        .await;
    leader.expect("the wait ended only once a leader was found")
}

// ADR-0001 decisions 11 and 12, E5-R5 and E5-R9: the leader and one
// follower are cut off from the third worker and from the authority. They
// still reach each other, a majority of the voters, so only their
// registrations stop them: both fence themselves before those lapse. The
// third, alone but still reaching the authority, counts itself a majority
// of the live registrations once theirs have lapsed, swaps the epoch to 1
// and leads once the old leader's fence has ended. Healed, the two find the
// epoch moved on and rejoin under the new leader, through the driver.
#[tokio::test]
async fn a_majority_without_the_authority_fences_while_the_minority_with_it_recovers_then_rejoins()
{
    with_elected_cluster(async |cluster: &Cluster, leader: usize| {
        let away = [leader, (leader + 1) % VOTERS];
        let third = (leader + 2) % VOTERS;
        for &node in &away {
            cluster.handles[node].set_reachable(false);
            cluster.block(node, third);
        }

        cluster
            .wait_until(
                "the two cut off from the authority fence themselves",
                |observed, _| {
                    away.iter()
                        .all(|&node| observed.views[node].state == WorkerState::Fenced)
                },
            )
            .await;
        {
            let observed = cluster.observed.borrow();
            let now = cluster.clock.now();
            for node in away {
                assert_eq!(
                    observed.grant_at(node, now),
                    None,
                    "node {node} fenced itself but still holds a grant"
                );
            }
        }

        cluster
            .wait_until(
                "the third recovers the shard to epoch 1 and leads with a grant",
                |observed, now| {
                    observed.views[third].recovery_epoch == 1
                        && observed.leads_with_grant(third, now)
                },
            )
            .await;
        assert_eq!(cluster.authority_epoch(), Some(1));
        cluster.assert_no_grant_overlap();
        {
            let observed = cluster.observed.borrow();
            for node in away {
                let view = &observed.views[node];
                assert_eq!(
                    (view.state, view.recovery_epoch),
                    (WorkerState::Fenced, 0),
                    "node {node} cannot reach the authority, so it cannot have left its fence"
                );
            }
        }

        for &node in &away {
            cluster.handles[node].set_reachable(true);
            cluster.unblock(node, third);
        }
        cluster
            .wait_until(
                "the two rejoin as pending members at epoch 1 under the new leader",
                |observed, now| {
                    observed.leads_with_grant(third, now)
                        && away.iter().all(|&node| {
                            let view = &observed.views[node];
                            view.state == WorkerState::Active
                                && view.recovery_epoch == 1
                                && view.pending
                                && view.known_leader.as_ref() == Some(&cluster.ids[third])
                        })
                },
            )
            .await;
        let rejoined_through_bootstrap = away.iter().all(|&node| {
            cluster
                .observed
                .borrow()
                .states
                .iter()
                .any(|&(changed, _, state)| changed == node && state == WorkerState::Bootstrapping)
        });
        assert!(
            rejoined_through_bootstrap,
            "each of the two went back to Bootstrapping to rejoin"
        );
    })
    .await;
}

// E5's whole-authority outage: the authority keeps its data but no worker
// can renew, so every worker fences itself, the leader included, whose
// grant goes with it. Back up, each registers again, finds the epoch it
// fenced at unchanged and resumes; the shard elects a leader that holds a
// grant again, at the same epoch.
#[tokio::test]
async fn a_whole_authority_outage_fences_every_worker_and_they_resume_at_the_same_epoch() {
    with_elected_cluster(async |cluster: &Cluster, _leader: usize| {
        cluster.authority.set_available(false);
        cluster
            .wait_until("every worker fences itself", |observed, _| {
                observed
                    .views
                    .iter()
                    .all(|view| view.state == WorkerState::Fenced)
            })
            .await;
        {
            let observed = cluster.observed.borrow();
            let now = cluster.clock.now();
            for node in 0..VOTERS {
                assert_eq!(
                    observed.grant_at(node, now),
                    None,
                    "node {node} fenced itself but still holds a grant"
                );
            }
        }

        cluster.authority.set_available(true);
        cluster
            .wait_until(
                "every worker resumes and a leader holds a grant again",
                |observed, now| {
                    observed
                        .views
                        .iter()
                        .all(|view| matches!(view.state, WorkerState::Active | WorkerState::Leader))
                        && (0..VOTERS).any(|node| observed.leads_with_grant(node, now))
                },
            )
            .await;

        let observed = cluster.observed.borrow();
        for (node, view) in observed.views.iter().enumerate() {
            assert_eq!(view.recovery_epoch, 0, "node {node} resumed at its epoch");
        }
        assert!(
            observed
                .states
                .iter()
                .all(|&(_, _, state)| state != WorkerState::Bootstrapping),
            "a worker that finds its epoch unchanged resumes rather than rejoins"
        );
        drop(observed);
        assert_eq!(cluster.authority_epoch(), Some(0));
    })
    .await;
}

// README §15.3, E5-R4 and E5-R3: the authority loses its data while the
// shard has its quorum. Nobody fences: every worker registers again at its
// next renewal. The leader, refused its fence because the epoch is gone,
// republishes the epoch it leads. The authority grants no fence until its
// warm-up ends, by when every fence taken before the flush has ended, so
// the leader holds no grant across the warm-up's end that it did not
// acquire after it.
#[tokio::test]
async fn a_flush_while_the_shard_has_its_quorum_is_repaired_by_its_leader() {
    with_elected_cluster(async |cluster: &Cluster, _leader: usize| {
        let flushed_at = cluster.clock.now();
        cluster.authority.flush();
        let warm_at = flushed_at + Duration::from_millis(TTL_MS);

        cluster
            .wait_until("the leader republishes its epoch", |_, _| {
                cluster.authority_epoch() == Some(0)
            })
            .await;
        cluster
            .wait_until(
                "a leader holds a grant acquired after the warm-up",
                |observed, now| {
                    (0..VOTERS).any(|node| {
                        observed.leads_with_grant(node, now)
                            && observed
                                .grants
                                .iter()
                                .any(|report| report.node == node && report.at >= warm_at)
                    })
                },
            )
            .await;

        let observed = cluster.observed.borrow();
        for (node, start, end) in observed.grant_intervals() {
            if start < warm_at {
                assert!(
                    end.is_some_and(|end| end <= warm_at),
                    "node {node}'s grant from {start:?} outlasted the warm-up ending at \
                     {warm_at:?} (it ends at {end:?})"
                );
            }
        }
        let fenced_or_rejoined = observed.states.iter().find(|&&(_, at, state)| {
            at >= flushed_at && matches!(state, WorkerState::Fenced | WorkerState::Bootstrapping)
        });
        assert_eq!(
            fenced_or_rejoined, None,
            "a flush under a live quorum fences no one"
        );
        for (node, view) in observed.views.iter().enumerate() {
            assert_eq!(
                view.recovery_epoch, 0,
                "node {node} stayed at the shard's epoch"
            );
        }
        drop(observed);
        assert_eq!(cluster.authority_epoch(), Some(0));
    })
    .await;
}
