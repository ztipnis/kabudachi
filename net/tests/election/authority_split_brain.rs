//! E5's authority split-brain scenario (ADR-0001 decisions 11 and 12,
//! README §15), run again over real sockets: three voters, each on its own
//! libp2p swarm over loopback TCP and driven by its own `run_driver`, share
//! one coordination authority (a `FaultingAuthority`) through a handle each.
//! `core/tests/scenario/scenario_catastrophic_authority.rs` proves it, with
//! the whole-authority outage and the flush, in the simulator; this file
//! proves that the driver performs the authority calls, rejoins a fenced
//! worker through the workers the authority lists, and keeps grants
//! exclusive when the partition is a real one (`Net::block_peer`) and the
//! timers are real ones.
//!
//! Every scenario checks the property the simulator's `first_grant_overlap`
//! checks, through the same `kabudachi_testkit::first_grant_overlap`: no two
//! nodes ever hold an unexpired grant at the same instant. Each step that
//! reports an `Output::Grant` is recorded as a `StepRecord` stamped with the
//! instant its driver observed it, on the one `RealClock` every node,
//! scheduler, driver and the authority read, so instants and lease ends are
//! comparable across nodes. A grant reported at `r` with lease end `v`
//! covers `[r, v)`, cut short by the node's next grant report; an unbounded
//! one lasts until that next report.
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

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration as StdDuration;

use kabudachi_core::configuration::{Configuration, Generation, Single};
use kabudachi_core::coordination_authority::{CoordinationAuthority, RecoveryEpoch};
use kabudachi_core::election::{
    AuthorityTimings, ElectionTimings, Entry, Identity, Input, KnownConfiguration, Output, Step,
    WorkerNode,
};
use kabudachi_core::protocol::ids::{IncarnationId, ShardId, Uuid7Ids, WorkerId};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::{LeadershipGrant, LeaseEnd, Scheduler};
use kabudachi_core::time::{Clock, Duration, Instant, RealClock};
use kabudachi_net::driver::{DriverConfig, SharedAuthority, run_driver};
use kabudachi_net::messenger::Net;
use kabudachi_net::swarm::build_swarm;
use kabudachi_testkit::{FaultingAuthority, StepRecord, first_grant_overlap};
use libp2p::identity::Keypair;
use tokio::sync::watch;
use tokio::time::timeout;

use crate::support::election::{built_on_one_tick, due_now};
use crate::support::net::connect_full_mesh;

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

/// What a node looked like after the last step its driver observed.
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
    ids: Vec<WorkerId>,
    views: Vec<NodeView>,
    /// Every step that reported a grant or its withdrawal, in the order
    /// observed.
    grant_steps: Vec<StepRecord>,
    /// Every state change, with the node and the instant it was observed.
    states: Vec<(usize, Instant, WorkerState)>,
    /// Every `(node, recovery epoch, known leader)` a node was seen a
    /// pending member at, after any step: a pending stretch may end before
    /// a wait next looks, so a wait asks whether it was ever seen.
    pending: BTreeSet<(usize, u64, Option<WorkerId>)>,
}

impl Observed {
    fn record(&mut self, node_index: usize, record: StepRecord, node: &Node) {
        let view = NodeView::of(node);
        if view.pending {
            self.pending
                .insert((node_index, view.recovery_epoch, view.known_leader.clone()));
        }
        self.views[node_index] = view;
        for output in &record.outputs {
            if let Output::StateChanged(state) = output {
                self.states.push((node_index, record.at, *state));
            }
        }
        if record.reports_grant() {
            self.grant_steps.push(record);
        }
    }

    /// The grant `node` holds at `now`: the last one it reported, if its
    /// lease has not ended by `now`.
    fn grant_at(&self, node: usize, now: Instant) -> Option<LeadershipGrant> {
        let last = self
            .grant_steps
            .iter()
            .flat_map(|record| {
                record
                    .outputs
                    .iter()
                    .filter_map(move |output| match output {
                        Output::Grant(grant) if record.node == self.ids[node] => Some(*grant),
                        _ => None,
                    })
            })
            .next_back()?;
        last.filter(|grant| match grant.valid_until {
            LeaseEnd::Unbounded => true,
            LeaseEnd::At(end) => now < end,
        })
    }

    /// Whether `node` is `Leader` and holds a grant at `now`.
    fn leads_with_grant(&self, node: usize, now: Instant) -> bool {
        self.views[node].state == WorkerState::Leader && self.grant_at(node, now).is_some()
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
    /// the clock's reading), checked after every step any driver observes.
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
                 (node, instant, state): {:?}, and were seen pending at (node, epoch, leader) \
                 {:?}",
                observed.views, observed.states, observed.pending
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
            first_grant_overlap(&observed.grant_steps),
            None,
            "two nodes held an unexpired grant at once; every grant report: {:#?}",
            observed.grant_steps
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
    WorkerNode::start(
        Identity {
            id: id.clone(),
            incarnation: IncarnationId::new(format!("{}-incarnation-0", id.as_str())),
            shard: ShardId::new(SHARD),
            timings: ElectionTimings::new(
                Duration::from_millis(SUSPECT_TIMEOUT_MS),
                Duration::from_millis(HEARTBEAT_INTERVAL_MS),
            )
            .with_roll_call_deadline(Duration::from_millis(ROLL_CALL_DEADLINE_MS)),
        },
        Entry::Known(KnownConfiguration {
            configuration: Configuration::single(Single {
                generation: Generation::genesis(0),
                base: Generation::genesis(0),
                voter_count: VOTERS,
            }),
            admission: Some(Generation::genesis(0)),
        }),
        clock,
        Some(AuthorityTimings {
            ttl: Duration::from_millis(TTL_MS),
        }),
    )
    .0
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
        ids: ids.clone(),
        views: nodes.iter().map(NodeView::of).collect(),
        grant_steps: Vec::new(),
        states: Vec::new(),
        pending: BTreeSet::new(),
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
        move |node: &Node, input: Option<&Input>, step: &Step| {
            let record = StepRecord::of(node, input, step, clock.now());
            observations.send_modify(|observed| observed.record(node_index, record, node));
        }
    };
    let authority_of = |node_index: usize| -> Option<SharedAuthority> {
        Some(Arc::new(cluster.handles[node_index].clone()))
    };
    let [node_0, node_1, node_2] = &mut nodes;
    let [scheduler_0, scheduler_1, scheduler_2] = &mut schedulers;

    tokio::select! {
        _ = run_driver(node_0, due_now(&clock), &cluster.nets[0], scheduler_0, clock, authority_of(0), DriverConfig::default(), observer(0)) => {
            unreachable!("run_driver never returns")
        }
        _ = run_driver(node_1, due_now(&clock), &cluster.nets[1], scheduler_1, clock, authority_of(1), DriverConfig::default(), observer(1)) => {
            unreachable!("run_driver never returns")
        }
        _ = run_driver(node_2, due_now(&clock), &cluster.nets[2], scheduler_2, clock, authority_of(2), DriverConfig::default(), observer(2)) => {
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
// epoch moved on and rejoin under the new leader, through the driver, as
// pending members it then admits.
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
        let new_leader = cluster.ids[third].clone();
        cluster
            .wait_until(
                "the two rejoin as pending members at epoch 1 under the new leader, which \
                 admits them",
                |observed, now| {
                    observed.leads_with_grant(third, now)
                        && away.iter().all(|&node| {
                            let view = &observed.views[node];
                            observed
                                .pending
                                .contains(&(node, 1, Some(new_leader.clone())))
                                && view.state == WorkerState::Active
                                && view.recovery_epoch == 1
                                && !view.pending
                                && view.known_leader.as_ref() == Some(&new_leader)
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
