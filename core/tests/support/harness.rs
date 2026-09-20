//! The `Cluster` simulation harness (test-only): N real `WorkerNode`s wired to
//! one shared `FakeClock`/`FakeNetwork`/`FakeCoordinationAuthority` and driven
//! through simulated time by `advance()`. Nothing here is production code:
//! `RingMembership` is the one real, non-fake collaborator.
//!
//! `bootstrap(n, ..)` names nodes `worker-0` .. `worker-{n-1}`, each with an
//! `IncarnationId` derived from its `WorkerId`, all in shard `"shard-1"`.

use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use kabudachi_core::election::WorkerNode;
use kabudachi_core::membership::RingMembership;
use kabudachi_core::protocol::ids::{IncarnationId, ShardId, WorkerId};
use kabudachi_core::protocol::messages::election_message;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::Duration;
use kabudachi_core::transport::PeerMessenger;

use crate::support::clock::FakeClock;
use crate::support::coordination_authority::FakeCoordinationAuthority;
use crate::support::network::FakeNetwork;

const SHARD_ID: &str = "shard-1";

/// A simulated cluster of `n` real `WorkerNode`s sharing one clock, network and authority.
pub struct Cluster {
    clock: Rc<FakeClock>,
    network: FakeNetwork,
    // Exposed through `authority()` so scenarios can inject faults or seed shard state.
    authority: FakeCoordinationAuthority,
    shard_id: ShardId,
    nodes: BTreeMap<
        WorkerId,
        WorkerNode<FakeClock, FakeNetwork, RingMembership, FakeCoordinationAuthority>,
    >,
    /// The electorate `bootstrap` seeded every node with, so `restart_node` can
    /// reseed a fresh node the same way.
    initial_membership: BTreeSet<WorkerId>,
    /// The suspicion timeout every node was built with, reused by `restart_node`.
    suspect_timeout: Duration,
}

impl Cluster {
    pub fn bootstrap(n: usize, suspect_timeout: Duration) -> Self {
        let clock = Rc::new(FakeClock::new());
        let network = FakeNetwork::new(Rc::clone(&clock));
        let authority = FakeCoordinationAuthority::new();
        let shard_id = ShardId::new(SHARD_ID);

        let worker_ids: Vec<WorkerId> = (0..n)
            .map(|i| WorkerId::new(format!("worker-{i}")))
            .collect();
        for id in &worker_ids {
            network.register(id.clone());
        }
        let initial_membership: BTreeSet<WorkerId> = worker_ids.iter().cloned().collect();

        let mut nodes = BTreeMap::new();
        for id in worker_ids {
            let incarnation_id = IncarnationId::new(format!("{}-incarnation-0", id.as_str()));
            let membership = RingMembership::new(initial_membership.clone());
            let node = WorkerNode::new(
                id.clone(),
                incarnation_id,
                shard_id.clone(),
                (*clock).clone(),
                network.clone(),
                membership,
                authority.clone(),
                suspect_timeout,
            );
            nodes.insert(id, node);
        }

        Cluster {
            clock,
            network,
            authority,
            shard_id,
            nodes,
            initial_membership,
            suspect_timeout,
        }
    }

    /// Advances simulated time by `dt` and runs one step: the network delivers
    /// what became due, then each node (in sorted `WorkerId` order, for
    /// determinism) handles its inbox before its `tick()`.
    pub fn advance(&mut self, dt: Duration) {
        self.advance_impl(dt);
    }

    /// `advance`, also reporting whether a non-heartbeat message was delivered.
    fn advance_impl(&mut self, dt: Duration) -> bool {
        self.clock.advance(dt);
        self.network.pump();

        let mut non_heartbeat_delivered = false;
        for (id, node) in self.nodes.iter_mut() {
            for (from, msg) in self.network.poll_inbox(id.clone()) {
                if !matches!(
                    msg.payload,
                    Some(election_message::Payload::HeartbeatAck(_))
                ) {
                    non_heartbeat_delivered = true;
                }
                node.on_message(from, msg);
            }
            node.tick();
        }

        non_heartbeat_delivered
    }

    /// Repeatedly calls `advance(tick_size)` until `max_ticks` iterations have
    /// run or the cluster reaches a fixed point, and returns the number of
    /// iterations run (including the one that found the fixed point).
    ///
    /// A fixed point is an iteration in which no non-heartbeat message was
    /// delivered, nothing that could still change a node's state is in
    /// flight, and no node's `state()` changed. In-flight messages count
    /// because a delivery like a `VoteRequest` changes no state yet leaves the
    /// election running. Leader heartbeat acks are ignored unless addressed to
    /// a node in `RollCall`, the only state one can change: a converged
    /// cluster otherwise delivers a fresh ack every iteration forever.
    ///
    /// Reaching `max_ticks` is not an error here; the caller decides whether
    /// that fails the test.
    pub fn run_until_quiescent(&mut self, tick_size: Duration, max_ticks: usize) -> usize {
        for i in 0..max_ticks {
            let states_before = self.states();
            let non_heartbeat_delivered = self.advance_impl(tick_size);
            let states_after = self.states();
            let in_flight =
                self.network
                    .pending()
                    .iter()
                    .any(|(to, message)| match message.payload {
                        Some(election_message::Payload::HeartbeatAck(_)) => {
                            states_after[to] == WorkerState::RollCall
                        }
                        _ => true,
                    });

            if !non_heartbeat_delivered && !in_flight && states_before == states_after {
                return i + 1;
            }
        }
        max_ticks
    }

    pub fn partition(&self, group_a: BTreeSet<WorkerId>, group_b: BTreeSet<WorkerId>) {
        self.network.partition(group_a, group_b);
    }

    pub fn heal(&self) {
        self.network.heal_partition();
    }

    /// Direct access to the shared network, for tests that check delivery and drops at message level.
    pub fn network(&self) -> &FakeNetwork {
        &self.network
    }

    /// Direct access to the shared authority, to inject faults or seed shard state.
    pub fn authority(&self) -> &FakeCoordinationAuthority {
        &self.authority
    }

    /// Drains the named node; panics on an unknown ID, which is a test bug.
    pub fn drain(&mut self, worker: &WorkerId) {
        let node = self.nodes.get_mut(worker).unwrap_or_else(|| {
            panic!(
                "Cluster::drain: worker {worker:?} is not a known node ID — every ID passed to \
                 Cluster methods must come from Cluster::node_ids()"
            )
        });
        node.begin_drain();
    }

    /// Replaces the node under `id` with a fresh `WorkerNode` (same `WorkerId`,
    /// the given `incarnation_id`), simulating a crash and restart. The
    /// replacement starts `Active` with default state, reseeded with the
    /// original electorate, and is wired into the shared clock, network and
    /// authority at once. Panics on an unknown ID.
    pub fn restart_node(&mut self, id: &WorkerId, incarnation_id: IncarnationId) {
        if !self.nodes.contains_key(id) {
            panic!(
                "Cluster::restart_node: worker {id:?} is not a known node ID — every ID passed \
                 to Cluster methods must come from Cluster::node_ids()"
            );
        }

        let membership = RingMembership::new(self.initial_membership.clone());
        let node = WorkerNode::new(
            id.clone(),
            incarnation_id,
            self.shard_id.clone(),
            (*self.clock).clone(),
            self.network.clone(),
            membership,
            self.authority.clone(),
            self.suspect_timeout,
        );
        self.nodes.insert(id.clone(), node);
    }

    /// The first node (in sorted-ID order) that is `Leader`, if any. It does not check there is at most one.
    pub fn leader(&self) -> Option<WorkerId> {
        self.nodes
            .iter()
            .find(|(_, node)| node.state() == WorkerState::Leader)
            .map(|(id, _)| id.clone())
    }

    pub fn states(&self) -> BTreeMap<WorkerId, WorkerState> {
        self.nodes
            .iter()
            .map(|(id, node)| (id.clone(), node.state()))
            .collect()
    }

    /// Panics, naming the offenders, if more than one node is `Leader`.
    pub fn assert_at_most_one_leader(&self) {
        let leaders: Vec<&WorkerId> = self
            .nodes
            .iter()
            .filter(|(_, node)| node.state() == WorkerState::Leader)
            .map(|(id, _)| id)
            .collect();

        assert!(
            leaders.len() <= 1,
            "expected at most one node in WorkerState::Leader, found {}: {leaders:?}",
            leaders.len(),
        );
    }

    pub fn node_ids(&self) -> BTreeSet<WorkerId> {
        self.nodes.keys().cloned().collect()
    }

    /// Direct per-node access (e.g. for `attempt_forced_recovery()`); panics on an unknown ID.
    pub fn node(
        &mut self,
        id: &WorkerId,
    ) -> &mut WorkerNode<FakeClock, FakeNetwork, RingMembership, FakeCoordinationAuthority> {
        self.nodes.get_mut(id).unwrap_or_else(|| {
            panic!(
                "Cluster::node: worker {id:?} is not a known node ID — every ID passed to \
                 Cluster methods must come from Cluster::node_ids()"
            )
        })
    }
}
