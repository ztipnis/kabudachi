//! The `Cluster` simulation harness (test-only): N real `WorkerNode`s wired to
//! one shared `FakeClock`/`FakeNetwork` and one shared coordination authority,
//! and driven through simulated time by `advance()`. Nothing here is
//! production code: the in-memory authority behind the `FaultingAuthority`
//! handles is the real, non-fake collaborator.
//!
//! `bootstrap(n, ..)` names nodes `worker-0` .. `worker-{n-1}`, each with an
//! `IncarnationId` derived from its `WorkerId`, all in shard `"shard-1"` and
//! all voters, admitted at the genesis generation, of one configuration of
//! `n` voters. `bootstrap_with_pending(n, p, ..)` adds `p` more nodes,
//! `worker-{n}` onward, as pending members: each holds that configuration
//! with no admission generation, as a worker that has joined but that no
//! election has admitted yet. Either builds the authority first and advances
//! the clock one TTL, to the end of the authority's warm-up, so the nodes
//! start at that later instant. Their nodes have no authority configured:
//! they never call it, never register and never fence themselves.
//! `bootstrap_with_authority(n, p, ..)` builds the same cluster with every
//! node configured with the authority, at the tests' TTL (see
//! `authority::authority_ttl`), and the shard's recovery epoch seeded at 0;
//! each node registers itself at its first step.
//!
//! The harness is the nodes' driver: it sends and publishes each node's
//! messages through the network, hands each node its messages as they fall
//! due, ticks each node at the deadline the node last reported, tells every
//! node which peers it is connected to (every other node, less those across
//! a partition), and hands every grant a node reports, and every worker it
//! reports lost, to that node's own `Scheduler`, all through
//! `election::carry_out`. Each scheduler reads the shared clock, and the
//! harness also catches each one up at its `next_deadline`, as its timer loop
//! would, so a scheduler notices at its lease end that it stopped leading,
//! even while its node is stalled (see `stall`). It makes each authority call
//! a node asks for at once, through that node's own handle (see
//! `node_authority`), and hands the node the reply at the same instant, or
//! holds it while the node is stalled. It also answers JOIN for a node that
//! goes back to `Bootstrapping` to rejoin its shard (see `run_pass`).

use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use kabudachi_core::coordination_authority::CoordinationAuthority;
use kabudachi_core::election::{
    AuthorityCall, AuthorityPerformer, AuthorityReply, AuthorityTimings, Entry, Identity, Input,
    KnownConfiguration, MessageSink, Output, Step, WorkerNode, carry_out,
};
use kabudachi_core::protocol::ids::{IncarnationId, ShardId, WorkerId};
use kabudachi_core::protocol::messages::{ElectionMessage, election_message};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::Scheduler;
use kabudachi_core::time::{Clock, Duration, Instant};
pub use kabudachi_testkit::StepRecord;
use kabudachi_testkit::FaultingAuthority;

use crate::support::authority::{authority_ttl, epoch, warmed_up_authority};
use crate::support::builders::{timings, voter_of};
use crate::support::clock::FakeClock;
use crate::support::ids::SequentialIds;
use crate::support::network::FakeNetwork;
use crate::support::spy::Spy;

const SHARD_ID: &str = "shard-1";

/// More passes than any real exchange needs at one instant: each pass
/// carries a message one hop further, and roll calls, votes and acks all
/// end within a few hops.
const MAX_PASSES_PER_INSTANT: usize = 1_000;

pub type ClusterNode = WorkerNode<FakeClock>;

pub type ClusterScheduler = Scheduler<FakeClock, SequentialIds, Spy>;

/// A node whose driver hands it nothing until `until`.
struct Stall {
    until: Instant,
    /// The messages and connection changes that reached it meanwhile, in
    /// order, to hand it once the stall ends.
    held: Vec<Input>,
}

/// A simulated cluster of `n` real `WorkerNode`s sharing one clock, network and authority.
pub struct Cluster {
    clock: Rc<FakeClock>,
    network: FakeNetwork,
    /// A handle of its own, exposed through `authority()` for seeding shard state.
    authority: FaultingAuthority<FakeClock>,
    /// Each node's own handle to the same authority, exposed through
    /// `node_authority()` so a scenario can inject faults for one node.
    node_authorities: BTreeMap<WorkerId, FaultingAuthority<FakeClock>>,
    shard_id: ShardId,
    nodes: BTreeMap<WorkerId, ClusterNode>,
    /// The next deadline each node reported in its latest step.
    deadlines: BTreeMap<WorkerId, Option<Instant>>,
    /// The voter count of the configuration `bootstrap` seeded every node
    /// with, so `restart_node` can reseed a fresh node the same way.
    voter_count: usize,
    /// The nodes seeded as pending members: `bootstrap_with_pending`'s, and
    /// every node `restart_node` started.
    pending_members: BTreeSet<WorkerId>,
    /// How many nodes `restart_node` has restarted, to name each afresh.
    restarts: usize,
    /// The suspicion timeout every node was built with, reused by `restart_node`.
    suspect_timeout: Duration,
    /// The authority timings every node was built with, reused by
    /// `restart_node`; `None` for a cluster whose nodes have no authority.
    authority_timings: Option<AuthorityTimings>,
    /// Each node's scheduler, handed every grant its node reports.
    schedulers: BTreeMap<WorkerId, ClusterScheduler>,
    /// The spy on each node's scheduler, which hears what it decides.
    spies: BTreeMap<WorkerId, Spy>,
    /// Every step that reported a grant or its withdrawal, in order, for
    /// `first_grant_overlap`.
    grant_steps: Vec<StepRecord>,
    stalls: BTreeMap<WorkerId, Stall>,
    /// The steps taken since `record_steps` or the last `take_steps`;
    /// `None` while not recording.
    recorded_steps: Option<Vec<StepRecord>>,
}

/// What the harness did while running its nodes.
#[derive(Default)]
struct Activity {
    /// The nodes that were handed a message or ticked.
    busy: BTreeSet<WorkerId>,
    /// Delivered a message other than a heartbeat or a leader's ack of one,
    /// or moved a node to another state.
    progressed: bool,
}

impl Cluster {
    /// Every node suspects its leader after `suspect_timeout`, and a follower
    /// heartbeats its leader four times per `suspect_timeout` (see
    /// `builders::timings`).
    pub fn bootstrap(n: usize, suspect_timeout: Duration) -> Self {
        Cluster::bootstrap_with_pending(n, 0, suspect_timeout)
    }

    /// `bootstrap(voters, ..)`, plus `pending` pending members named after
    /// the voters (see the module docs).
    pub fn bootstrap_with_pending(
        voters: usize,
        pending: usize,
        suspect_timeout: Duration,
    ) -> Self {
        Cluster::build(voters, pending, suspect_timeout, None)
    }

    /// `bootstrap_with_pending(voters, pending, ..)`, with every node
    /// configured with the authority at the tests' TTL and the shard's
    /// recovery epoch seeded at 0 (see the module docs). Each node registers
    /// itself at its first step, so every node is live at the authority
    /// from the start.
    pub fn bootstrap_with_authority(
        voters: usize,
        pending: usize,
        suspect_timeout: Duration,
    ) -> Self {
        Cluster::build(
            voters,
            pending,
            suspect_timeout,
            Some(AuthorityTimings {
                ttl: authority_ttl(),
            }),
        )
    }

    fn build(
        voters: usize,
        pending: usize,
        suspect_timeout: Duration,
        authority_timings: Option<AuthorityTimings>,
    ) -> Self {
        let clock = Rc::new(FakeClock::new());
        let network = FakeNetwork::new(Rc::clone(&clock));
        let authority = warmed_up_authority(&clock);
        let shard_id = ShardId::new(SHARD_ID);
        if authority_timings.is_some() {
            authority
                .compare_and_swap_recovery_epoch(&shard_id, None, epoch(0))
                .expect("a fresh authority holds no epoch, so create-if-absent succeeds");
        }

        let worker_ids: Vec<WorkerId> = (0..voters + pending)
            .map(|i| WorkerId::new(format!("worker-{i}")))
            .collect();
        for id in &worker_ids {
            network.register(id.clone());
        }

        let mut cluster = Cluster {
            clock,
            network,
            authority,
            node_authorities: BTreeMap::new(),
            shard_id,
            nodes: BTreeMap::new(),
            deadlines: BTreeMap::new(),
            voter_count: voters,
            pending_members: worker_ids[voters..].iter().cloned().collect(),
            restarts: 0,
            suspect_timeout,
            authority_timings,
            schedulers: BTreeMap::new(),
            spies: BTreeMap::new(),
            grant_steps: Vec::new(),
            stalls: BTreeMap::new(),
            recorded_steps: None,
        };
        for id in &worker_ids {
            let node_authority = cluster.authority.for_another_worker();
            cluster.node_authorities.insert(id.clone(), node_authority);
            let incarnation_id = IncarnationId::new(format!("{}-incarnation-0", id.as_str()));
            cluster.insert_node(id, incarnation_id);
        }
        for id in &worker_ids {
            cluster.connect_to_reachable_peers(id);
        }
        cluster
    }

    /// Builds a fresh `Active` node under `id`, a voter or a pending member
    /// as `bootstrap_with_pending` seeded it, with a fresh scheduler that
    /// holds no grant, replacing any node there, and records its first
    /// deadline.
    fn insert_node(&mut self, id: &WorkerId, incarnation_id: IncarnationId) {
        let known_configuration = if self.pending_members.contains(id) {
            KnownConfiguration {
                admission: None,
                ..voter_of(self.voter_count)
            }
        } else {
            voter_of(self.voter_count)
        };
        // A node that starts inside a known configuration asks for nothing
        // first; the `Tick` below stands in for its first step.
        let (node, _) = WorkerNode::start(
            Identity {
                id: id.clone(),
                incarnation: incarnation_id,
                shard: self.shard_id.clone(),
                timings: timings(self.suspect_timeout),
            },
            Entry::Known(known_configuration),
            (*self.clock).clone(),
            self.authority_timings,
        );
        self.nodes.insert(id.clone(), node);
        let spy = Spy::default();
        self.schedulers.insert(
            id.clone(),
            Scheduler::with_observer((*self.clock).clone(), SequentialIds::new(), spy.clone()),
        );
        self.spies.insert(id.clone(), spy);
        self.deadlines.insert(id.clone(), None);
        // A node reports its deadline only when stepped. A `Tick` changes
        // nothing on a node this fresh, but a node no connection event ever
        // reaches would otherwise never be ticked.
        self.step(id, Input::Tick);
    }

    /// Reports to `id` every other node the network lets it reach.
    fn connect_to_reachable_peers(&mut self, id: &WorkerId) {
        let peers: Vec<WorkerId> = self
            .connections()
            .into_iter()
            .filter(|(me, _)| me == id)
            .map(|(_, peer)| peer)
            .collect();
        for peer in peers {
            self.step(id, Input::PeerConnected(peer));
        }
    }

    /// Every ordered pair of distinct nodes the network currently lets talk.
    fn connections(&self) -> BTreeSet<(WorkerId, WorkerId)> {
        let mut pairs = BTreeSet::new();
        for me in self.nodes.keys() {
            for peer in self.nodes.keys() {
                if me != peer && !self.network.is_partitioned(me, peer) {
                    pairs.insert((me.clone(), peer.clone()));
                }
            }
        }
        pairs
    }

    /// Tells each node about every connection that opened or closed since
    /// `before` was taken.
    fn report_connection_changes(&mut self, before: BTreeSet<(WorkerId, WorkerId)>) {
        let after = self.connections();
        for (me, peer) in before.difference(&after) {
            self.hand_over(me, Input::PeerDisconnected(peer.clone()));
        }
        for (me, peer) in after.difference(&before) {
            self.hand_over(me, Input::PeerConnected(peer.clone()));
        }
    }

    /// Runs the cluster through the next `dt` of simulated time. The clock
    /// jumps from event to event — the next message delivery, node deadline
    /// or scheduler deadline, whichever is earlier — and everything due at
    /// an instant runs there before the clock moves on: the schedulers whose
    /// deadline has come catch up first, then the passes run. The clock ends
    /// at `now + dt`.
    ///
    /// Within one instant the harness works in passes, as `run_passes`
    /// describes, so a message sent at an instant with no network delay is
    /// delivered at that same instant, one pass later.
    pub fn advance(&mut self, dt: Duration) {
        self.advance_impl(dt);
    }

    fn advance_impl(&mut self, dt: Duration) -> Activity {
        let end = self.clock.now() + dt;
        let mut activity = Activity::default();
        while let Some(at) = self.next_event_at().filter(|at| *at <= end) {
            self.clock.advance(at - self.clock.now());
            self.catch_up_due_schedulers();
            activity.progressed |= self.run_passes(true).progressed;
        }
        self.clock.advance(end - self.clock.now());
        activity
    }

    /// The earliest message delivery, node deadline, scheduler deadline or
    /// end of a stall still to come. A stalled node's deadline waits for the
    /// end of its stall; its scheduler's deadline does not, because a stall
    /// holds a node's driver, not its scheduler's timer. A stall, or a
    /// scheduler deadline, that `advance_clock_only` carried the clock past
    /// is due now.
    fn next_event_at(&self) -> Option<Instant> {
        let now = self.clock.now();
        let next_delivery = self.network.next_delivery_at();
        let next_deadline = self
            .deadlines
            .iter()
            .filter(|(id, _)| !self.is_stalled(id, now))
            .filter_map(|(_, deadline)| *deadline)
            .min();
        let next_scheduler_deadline = self
            .schedulers
            .values()
            .filter_map(|scheduler| scheduler.next_deadline())
            .map(|deadline| deadline.max(now))
            .min();
        let next_stall_end = self.stalls.values().map(|stall| stall.until.max(now)).min();
        next_delivery
            .into_iter()
            .chain(next_deadline)
            .chain(next_scheduler_deadline)
            .chain(next_stall_end)
            .min()
    }

    /// Catches up every scheduler whose `next_deadline` has come, as its
    /// timer loop would. A stall holds a node's driver, not its scheduler's
    /// timer, so a stalled node's scheduler is caught up too. A catch-up is
    /// not node activity. Panics if a scheduler is still due afterwards,
    /// because the clock would never move on.
    fn catch_up_due_schedulers(&mut self) {
        let now = self.clock.now();
        for (id, scheduler) in &mut self.schedulers {
            if scheduler
                .next_deadline()
                .is_some_and(|deadline| deadline <= now)
            {
                scheduler.catch_up();
                assert!(
                    scheduler.next_deadline().is_none_or(|deadline| deadline > now),
                    "Cluster: node {id:?}'s scheduler is still due at {now:?} after catch_up, \
                     so the clock would never move on"
                );
            }
        }
    }

    /// Runs passes at the current instant until nothing more is due at it.
    /// In each pass every node, in sorted `WorkerId` order for determinism,
    /// handles the messages that were due when the pass began, then (with
    /// `with_ticks`) its `Tick` if its deadline has come. A stalled node
    /// handles nothing: its messages are held for it, and once its stall has
    /// ended it first handles everything held, in order.
    ///
    /// Panics, naming the nodes still busy, if the passes never end: the
    /// nodes would keep the clock from ever moving on.
    fn run_passes(&mut self, with_ticks: bool) -> Activity {
        let mut activity = Activity::default();
        for _ in 0..MAX_PASSES_PER_INSTANT {
            let pass = self.run_pass(with_ticks);
            if pass.busy.is_empty() {
                return activity;
            }
            activity.progressed |= pass.progressed;
            activity.busy = pass.busy;
        }
        panic!(
            "Cluster: nodes {:?} were still being handed messages or ticks after \
             {MAX_PASSES_PER_INSTANT} passes at {:?}, so the clock would never move on",
            activity.busy,
            self.clock.now()
        );
    }

    /// One pass at the current instant (see `run_passes`); `with_ticks`
    /// false delivers messages only. Each pass first answers JOIN for any
    /// node rejoining its shard (see `join_rejoining_nodes`).
    fn run_pass(&mut self, with_ticks: bool) -> Activity {
        let now = self.clock.now();
        let mut activity = Activity::default();
        let joined = self.join_rejoining_nodes();
        activity.progressed |= !joined.is_empty();
        activity.busy.extend(joined);
        let mut inboxes: BTreeMap<WorkerId, Vec<(WorkerId, _)>> = BTreeMap::new();
        for due in self.network.take_due() {
            inboxes
                .entry(due.to)
                .or_default()
                .push((due.from, due.message));
        }

        let ids: Vec<WorkerId> = self.nodes.keys().cloned().collect();
        for id in ids {
            let mut inputs = self.end_stall_if_over(&id, now);
            inputs.extend(
                inboxes
                    .remove(&id)
                    .unwrap_or_default()
                    .into_iter()
                    .map(|(from, message)| Input::Message { from, message }),
            );
            if self.is_stalled(&id, now) {
                self.hold(&id, inputs);
                continue;
            }
            for input in inputs {
                activity.busy.insert(id.clone());
                if !matches!(&input, Input::Message { message, .. } if is_liveness_traffic(message))
                {
                    activity.progressed = true;
                }
                let outputs = self.step(&id, input);
                activity.progressed |= changes_state(&outputs);
            }

            if with_ticks && self.is_due(&id, now) {
                activity.busy.insert(id.clone());
                let state_before = self.nodes[&id].state();
                let outputs = self.step(&id, Input::Tick);
                activity.progressed |= changes_state(&outputs);
                assert!(
                    !self.is_due(&id, now) || self.nodes[&id].state() != state_before,
                    "Cluster: node {id:?} is still due at {now:?} after a Tick that left it in \
                     {state_before:?}, so its timers would never let the clock move on"
                );
            }
        }
        activity
    }

    fn is_due(&self, id: &WorkerId, now: Instant) -> bool {
        self.deadlines[id].is_some_and(|deadline| deadline <= now)
    }

    fn is_stalled(&self, id: &WorkerId, now: Instant) -> bool {
        self.stalls.get(id).is_some_and(|stall| now < stall.until)
    }

    /// Ends `id`'s stall if it is over by `now`, and returns what was held
    /// for it.
    fn end_stall_if_over(&mut self, id: &WorkerId, now: Instant) -> Vec<Input> {
        match self.stalls.get(id) {
            Some(stall) if stall.until <= now => self
                .stalls
                .remove(id)
                .map(|stall| stall.held)
                .unwrap_or_default(),
            _ => Vec::new(),
        }
    }

    fn hold(&mut self, id: &WorkerId, inputs: Vec<Input>) {
        if let Some(stall) = self.stalls.get_mut(id) {
            stall.held.extend(inputs);
        }
    }

    /// Hands `input` to the named node at once, after anything still held
    /// from a stall that has ended, or holds it for the node while it is
    /// stalled.
    fn hand_over(&mut self, id: &WorkerId, input: Input) {
        let now = self.clock.now();
        if self.is_stalled(id, now) {
            self.hold(id, vec![input]);
            return;
        }
        for held in self.end_stall_if_over(id, now) {
            self.step(id, held);
        }
        self.step(id, input);
    }

    /// Stalls the named node's driver for `dt`, as a paused process would
    /// be: until then the harness hands the node no message and no
    /// connection change, holding them for it, and does not tick it, while
    /// the clock and every other node move on. Its scheduler's timer is not
    /// its driver: the harness still catches the scheduler up at its
    /// deadlines, so a grant it holds lapses at its lease end all the same.
    /// Once the stall ends the node handles everything held, in order, and
    /// then its `Tick` if its deadline has passed. Stalling a stalled node
    /// again ends its stall at whichever end is later. A `drain` is held
    /// like a message; only `step` still reaches a stalled node at once.
    /// Panics on an unknown ID.
    pub fn stall(&mut self, id: &WorkerId, dt: Duration) {
        if !self.nodes.contains_key(id) {
            unknown_node("stall", id);
        }
        let until = self.clock.now() + dt;
        let stall = self.stalls.entry(id.clone()).or_insert(Stall {
            until,
            held: Vec::new(),
        });
        stall.until = stall.until.max(until);
    }

    /// Repeatedly calls `advance(tick_size)` until `max_ticks` iterations have
    /// run or the cluster reaches a fixed point, and returns the number of
    /// iterations run (including the one that found the fixed point).
    ///
    /// A fixed point is an iteration in which no message other than a
    /// heartbeat or an ack was delivered, no node changed state, nothing
    /// that could still change a node's state is in flight, and no node is
    /// in the middle of an election. In-flight messages count because a
    /// delivery like a `VoteRequest` changes no state yet leaves the
    /// election running. Heartbeats and acks are ignored, except an ack
    /// addressed to a node in `LeaderSuspect`, `RollCall` or `NoQuorum`,
    /// which it returns to `Active`: a converged cluster otherwise exchanges
    /// a heartbeat and an ack per follower every heartbeat interval forever.
    /// Anything held for a stalled node counts as in flight, whatever it is.
    ///
    /// A node in `RollCall` or `Candidate` is mid-election: its roll call or
    /// vote closes at a deadline still to come, though nothing may be
    /// delivered until then. Other timers that have not fired are not
    /// activity: a freshly bootstrapped cluster is already a fixed point,
    /// and so is a partitioned one whose `NoQuorum` side retries a roll call
    /// every suspicion timeout, as long as none is running at the check.
    ///
    /// Reaching `max_ticks` is not an error here; the caller decides whether
    /// that fails the test.
    pub fn run_until_quiescent(&mut self, tick_size: Duration, max_ticks: usize) -> usize {
        for i in 0..max_ticks {
            let activity = self.advance_impl(tick_size);
            let states = self.states();
            let in_flight =
                self.network
                    .pending()
                    .iter()
                    .any(|(to, message)| match message.payload {
                        // A node `restart_node` replaced is gone: an ack
                        // still in flight to it changes nothing.
                        Some(election_message::Payload::HeartbeatAck(_)) => matches!(
                            states.get(to),
                            Some(
                                WorkerState::LeaderSuspect
                                    | WorkerState::RollCall
                                    | WorkerState::NoQuorum
                            )
                        ),
                        _ => !is_liveness_traffic(message),
                    })
                    || self.stalls.values().any(|stall| !stall.held.is_empty());

            let electing = states
                .values()
                .any(|state| matches!(state, WorkerState::RollCall | WorkerState::Candidate));

            if !activity.progressed && !in_flight && !electing {
                return i + 1;
            }
        }
        max_ticks
    }

    /// Blocks delivery between the two groups (replacing any earlier
    /// partition) and tells every node which of its connections closed.
    pub fn partition(&mut self, group_a: BTreeSet<WorkerId>, group_b: BTreeSet<WorkerId>) {
        let before = self.connections();
        self.network.partition(group_a, group_b);
        self.report_connection_changes(before);
    }

    /// Lifts the partition and tells every node which connections reopened.
    pub fn heal(&mut self) {
        let before = self.connections();
        self.network.heal_partition();
        self.report_connection_changes(before);
    }

    /// Direct access to the shared network, for tests that check delivery and drops at message level.
    pub fn network(&self) -> &FakeNetwork {
        &self.network
    }

    /// A handle to the shared authority that belongs to no node, for seeding
    /// shard state. A fault set on it affects no node.
    pub fn authority(&self) -> &FaultingAuthority<FakeClock> {
        &self.authority
    }

    /// The named node's own handle to the shared authority, to inject faults
    /// for that node alone; panics on an unknown ID.
    pub fn node_authority(&self, id: &WorkerId) -> &FaultingAuthority<FakeClock> {
        self.node_authorities.get(id).unwrap_or_else(|| {
            panic!(
                "Cluster::node_authority: worker {id:?} is not a known node ID — every ID \
                 passed to Cluster methods must come from Cluster::node_ids()"
            )
        })
    }

    /// Asks the named node to drain, as its driver would: at once, or, while
    /// the node is stalled, once its stall ends. Panics on an unknown ID,
    /// which is a test bug.
    pub fn drain(&mut self, worker: &WorkerId) {
        if !self.nodes.contains_key(worker) {
            unknown_node("drain", worker);
        }
        self.hand_over(worker, Input::Drain);
    }

    /// Feeds one input to the named node, sends its messages through the
    /// network (delivered by a later `advance` or `deliver_messages`), and
    /// returns its outputs. Panics on an unknown ID.
    ///
    /// Every authority call the step asks for is made at once, through the
    /// node's own handle, and its reply handed to the node in turn, as are
    /// the calls that reply leads to; the outputs of those steps are
    /// returned too, in order. While the node is stalled a reply is held
    /// for it like a message.
    pub fn step(&mut self, id: &WorkerId, input: Input) -> Vec<Output> {
        let step = self.node_mut(id, "step").step(input.clone());
        self.drive(id, Some(input), step)
    }

    /// Carries out `first`, a step the named node took on `input`, and every
    /// step it leads to, through `election::carry_out`: each step's grant
    /// and lost workers go to the node's scheduler, its messages into the
    /// network, and its authority calls through the node's own handle, each
    /// reply handed straight back unless the node is stalled, when it is
    /// held for it like a message. Records each step (see `record_steps`)
    /// and returns every output, in order.
    fn drive(&mut self, id: &WorkerId, input: Option<Input>, first: Step) -> Vec<Output> {
        let mut node = self
            .nodes
            .remove(id)
            .unwrap_or_else(|| unknown_node("step", id));
        let mut scheduler = self
            .schedulers
            .remove(id)
            .unwrap_or_else(|| unknown_node("step", id));
        let now = self.clock.now();
        let mut sink = FromNode {
            network: &self.network,
            from: id.clone(),
        };
        let mut performer = HarnessPerformer {
            authority: self
                .node_authorities
                .get(id)
                .unwrap_or_else(|| unknown_node("step", id)),
            shard_id: &self.shard_id,
            me: id,
            stall: self.stalls.get_mut(id).filter(|stall| now < stall.until),
        };
        let deadlines = &mut self.deadlines;
        let grant_steps = &mut self.grant_steps;
        let recorded_steps = &mut self.recorded_steps;
        let mut all = Vec::new();
        let mut first_input = input;
        let _ = carry_out(
            &mut node,
            first,
            &mut scheduler,
            &mut sink,
            &mut performer,
            |node, _, reply, step| {
                deadlines.insert(id.clone(), step.next_deadline);
                let taken = first_input.take();
                let input = reply.or(taken.as_ref());
                let record = StepRecord::of(node, input, step, now);
                if record.reports_grant() {
                    grant_steps.push(record.clone());
                }
                if let Some(recorded) = recorded_steps.as_mut() {
                    recorded.push(record);
                }
                all.extend(step.outputs.iter().cloned());
            },
        );
        self.nodes.insert(id.clone(), node);
        self.schedulers.insert(id.clone(), scheduler);
        all
    }

    /// Answers JOIN for every node back in `Bootstrapping` to rejoin its
    /// shard (a fenced node that found the recovery epoch moved on), as a
    /// seed would: with the leader the network lets it reach, of its epoch
    /// or a later one (the latest epoch and term, if several lead), that is
    /// not stalled. A node no such leader leads for stays `Bootstrapping`
    /// until one does. Returns the nodes it joined.
    fn join_rejoining_nodes(&mut self) -> BTreeSet<WorkerId> {
        let now = self.clock.now();
        let rejoining: Vec<WorkerId> = self
            .nodes
            .iter()
            .filter(|(id, node)| {
                node.state() == WorkerState::Bootstrapping && !self.is_stalled(id, now)
            })
            .map(|(id, _)| id.clone())
            .collect();
        let mut joined = BTreeSet::new();
        for id in rejoining {
            let own_epoch = self.nodes[&id].recovery_epoch();
            // A leader names itself (see `WorkerNode::known_leader`), and
            // the harness addresses each node by its id.
            let pointer = self
                .nodes
                .iter()
                .filter(|(leader, node)| {
                    node.state() == WorkerState::Leader
                        && node.recovery_epoch() >= own_epoch
                        && !self.network.is_partitioned(&id, leader)
                        && !self.is_stalled(leader, now)
                })
                .filter_map(|(leader, node)| {
                    let pointer = node.join_response(leader.as_str().to_string())?;
                    Some(((pointer.recovery_epoch, pointer.term), pointer))
                })
                .max_by_key(|(latest, _)| *latest)
                .map(|(_, pointer)| pointer);
            if let Some(pointer) = pointer {
                let step = self.node_mut(&id, "join").finish_joining(&pointer);
                self.drive(&id, None, step);
                joined.insert(id);
            }
        }
        joined
    }

    /// Whether the named node's scheduler leads now, as its spy last heard.
    /// The harness catches every scheduler up at its deadlines, a bounded
    /// lease's end among them, so after any `advance` or `run_until_quiescent`
    /// the spy has heard every lapse up to the shared clock's now. After
    /// `advance_clock_only`, which moves the clock alone, it may not have
    /// yet. Panics on an unknown ID.
    pub fn holds_valid_grant(&self, id: &WorkerId) -> bool {
        self.scheduler_spy(id).leading()
    }

    /// The spy on the named node's scheduler. Panics on an unknown ID.
    pub fn scheduler_spy(&self, id: &WorkerId) -> &Spy {
        self.spies
            .get(id)
            .unwrap_or_else(|| unknown_node("scheduler_spy", id))
    }

    /// The named node's scheduler, which the harness hands every grant and
    /// lost worker its node reports, for a scenario to submit and claim
    /// through. Panics on an unknown ID.
    pub fn scheduler_mut(&mut self, id: &WorkerId) -> &mut ClusterScheduler {
        self.schedulers
            .get_mut(id)
            .unwrap_or_else(|| unknown_node("scheduler_mut", id))
    }

    /// Every node whose scheduler leads now, as its spy last heard.
    pub fn valid_grant_holders(&self) -> BTreeSet<WorkerId> {
        self.spies
            .iter()
            .filter(|(_, spy)| spy.leading())
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// The steps that reported the first two grants of different nodes to
    /// overlap so far (see `kabudachi_testkit::first_grant_overlap`); `None`
    /// if none did. Each scheduler leads exactly while such a grant holds:
    /// from the step that hands it the grant, all steps at one instant in the
    /// order the harness ran them, until the grant's lease ends by the
    /// shared clock or the node's next grant report, whichever is first.
    /// A node `restart_node` removed never reports again, so its last grant
    /// counts until its lease ends, and for ever if it was `Unbounded` (a
    /// leader with no authority that alone is a quorum): the restarted node
    /// comes back under a new id, so its reports never end the old one's.
    pub fn first_grant_overlap(&self) -> Option<(StepRecord, StepRecord)> {
        kabudachi_testkit::first_grant_overlap(&self.grant_steps)
    }

    /// Starts keeping a record of every step any node takes from now on,
    /// for `take_steps` to hand back.
    pub fn record_steps(&mut self) {
        self.recorded_steps.get_or_insert_with(Vec::new);
    }

    /// Every step taken since `record_steps` or the last call, in order;
    /// empty while not recording.
    pub fn take_steps(&mut self) -> Vec<StepRecord> {
        self.recorded_steps
            .as_mut()
            .map(std::mem::take)
            .unwrap_or_default()
    }

    /// Delivers every message due now, and every message those deliveries
    /// cause that is due now too, without ticking any node — so no node's
    /// timer can start anything meanwhile.
    pub fn deliver_messages(&mut self) {
        self.run_passes(false);
    }

    /// Moves the clock forward by `dt` without delivering a message or
    /// ticking a node: a scenario that steps chosen nodes by hand uses it to
    /// let their timers run out first. The next `advance` catches up on
    /// everything that fell due meanwhile.
    pub fn advance_clock_only(&mut self, dt: Duration) {
        self.clock.advance(dt);
    }

    /// Crashes the node under `id` and starts its process again, returning
    /// the new node's `WorkerId`. A `WorkerId` names one process incarnation
    /// (ADR-0001, amended 2026-09-27), so the restarted process comes back
    /// under a fresh one, `id` with a restart count appended, as a pending
    /// member seeded with the original configuration and no admission. The
    /// old `id` leaves the cluster: every peer that reached it is told its
    /// connection closed, and nothing is delivered to it any more (see
    /// `run_pass`). The new node starts `Active` with a fresh scheduler, is
    /// wired into the shared clock and network at once, connected to the
    /// peers the network lets it reach, and, in a cluster whose nodes have
    /// an authority, to the authority through the old node's handle (so any
    /// fault set on it still applies). A stall of the old node ends with it,
    /// dropping what was held. Panics on an unknown ID.
    pub fn restart_node(&mut self, id: &WorkerId) -> WorkerId {
        if !self.nodes.contains_key(id) {
            unknown_node("restart_node", id);
        }
        self.stalls.remove(id);
        let peers: Vec<WorkerId> = self
            .connections()
            .into_iter()
            .filter(|(_, peer)| peer == id)
            .map(|(me, _)| me)
            .collect();
        for peer in &peers {
            self.hand_over(peer, Input::PeerDisconnected(id.clone()));
        }
        self.nodes.remove(id);
        self.schedulers.remove(id);
        self.spies.remove(id);
        self.deadlines.remove(id);

        self.restarts += 1;
        let restarted = WorkerId::new(format!("{}-restart-{}", id.as_str(), self.restarts));
        self.network.register(restarted.clone());
        self.network.take_place_in_partition(id, &restarted);
        let authority = self
            .node_authorities
            .remove(id)
            .unwrap_or_else(|| self.authority.for_another_worker());
        self.node_authorities.insert(restarted.clone(), authority);
        self.pending_members.insert(restarted.clone());
        let incarnation_id = IncarnationId::new(format!("{}-incarnation-0", restarted.as_str()));
        self.insert_node(&restarted, incarnation_id);
        self.connect_to_reachable_peers(&restarted);
        for peer in self
            .connections()
            .into_iter()
            .filter(|(_, peer)| *peer == restarted)
            .map(|(me, _)| me)
            .collect::<Vec<_>>()
        {
            self.hand_over(&peer, Input::PeerConnected(restarted.clone()));
        }
        restarted
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

    /// Panics, naming the offenders, if more than one node is in
    /// `WorkerState::Leader` now. Whether their grants overlapped is
    /// `first_grant_overlap`'s question.
    pub fn assert_at_most_one_in_leader_state(&self) {
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

    /// The shared clock's current reading.
    pub fn now(&self) -> Instant {
        self.clock.now()
    }

    /// The suspicion timeout every node was built with.
    pub fn suspect_timeout(&self) -> Duration {
        self.suspect_timeout
    }

    /// The nodes `bootstrap_with_pending` started as pending members.
    pub fn pending_members(&self) -> &BTreeSet<WorkerId> {
        &self.pending_members
    }

    pub fn node_ids(&self) -> BTreeSet<WorkerId> {
        self.nodes.keys().cloned().collect()
    }

    /// Read access to one node (its state, term, recovery epoch); panics on
    /// an unknown ID. Inputs go through `step` and friends, so the harness
    /// sees every output and deadline.
    pub fn node(&self, id: &WorkerId) -> &ClusterNode {
        self.nodes
            .get(id)
            .unwrap_or_else(|| unknown_node("node", id))
    }

    fn node_mut(&mut self, id: &WorkerId, context: &str) -> &mut ClusterNode {
        self.nodes
            .get_mut(id)
            .unwrap_or_else(|| unknown_node(context, id))
    }
}

/// Where one node's messages go: into the shared network, from that node.
struct FromNode<'a> {
    network: &'a FakeNetwork,
    from: WorkerId,
}

impl MessageSink for FromNode<'_> {
    fn send(&mut self, to: WorkerId, message: ElectionMessage) {
        self.network.send(self.from.clone(), to, message);
    }

    fn publish(&mut self, message: ElectionMessage) {
        self.network.publish(self.from.clone(), message);
    }
}

/// Performs one node's authority calls at once, through its own handle. A
/// stalled node's replies are held for it with its messages, for it to
/// handle once the stall ends.
struct HarnessPerformer<'a> {
    authority: &'a FaultingAuthority<FakeClock>,
    shard_id: &'a ShardId,
    me: &'a WorkerId,
    stall: Option<&'a mut Stall>,
}

impl AuthorityPerformer for HarnessPerformer<'_> {
    fn perform(&mut self, call: AuthorityCall) -> Option<AuthorityReply> {
        let reply = call.perform(self.authority, self.shard_id, self.me, self.me.as_str());
        match self.stall.as_mut() {
            Some(stall) => {
                stall.held.push(Input::Authority(reply));
                None
            }
            None => Some(reply),
        }
    }
}

fn unknown_node(context: &str, id: &WorkerId) -> ! {
    panic!(
        "Cluster::{context}: worker {id:?} is not a known node ID — every ID passed to Cluster \
         methods must come from Cluster::node_ids()"
    )
}

/// A follower's heartbeat or its leader's ack: steady-state traffic that
/// a converged cluster keeps exchanging.
fn is_liveness_traffic(message: &ElectionMessage) -> bool {
    matches!(
        message.payload,
        Some(election_message::Payload::Heartbeat(_) | election_message::Payload::HeartbeatAck(_))
    )
}

fn changes_state(outputs: &[Output]) -> bool {
    outputs
        .iter()
        .any(|output| matches!(output, Output::StateChanged(_)))
}
