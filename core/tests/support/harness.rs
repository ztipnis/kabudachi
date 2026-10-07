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
//!
//! The cluster also keeps the shard's Task records (see `records`). After
//! each step and each scheduler catch-up, the revisions a node's scheduler
//! published are placed on `replication_factor` of the voters that node
//! leads, nearest by a fixed hash, and written to the shared `RecordSpace`.
//! `submit`, `claim`, `start`, `complete`, `fail` and `cancel` call a node's
//! scheduler and hold its answer, as the net driver does, until every write
//! the call made is acknowledged within the lease (see `answer`): a write the
//! space could not store at a quorum, or a lease that ended first, answers
//! `NotLeader`.

use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use kabudachi_core::coordination_authority::CoordinationAuthority;
use kabudachi_core::election::{
    AuthorityCall, AuthorityPerformer, AuthorityReply, AuthorityTimings, CallKind, Entry, Identity,
    Input, Issuer, KnownConfiguration, MessageSink, Output, ReplyToken, Step, WorkerNode, carry_out,
};
use kabudachi_core::protocol::generated::TaskRecord;
use kabudachi_core::protocol::ids::{IncarnationId, ShardId, TaskId, WorkerId};
use kabudachi_core::protocol::messages::{ElectionMessage, election_message};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::ids::TaskRunId;
use kabudachi_core::protocol::records::TaskRunRecord;
use kabudachi_core::reconcile::{
    ReconcileRound, ReconcileTerm, Republish, ReportPage, ReportedRun, ReportedState, wire,
};
use kabudachi_core::scheduler::{
    Certification, Claim, Completion, Scheduler, Submission,
};
use kabudachi_core::task_record::{
    EffectGate, PlacedWrite, Settled, Settlement, Waits, Write, WriteLedger, WriteOrder, WriteOutcome,
};
use kabudachi_core::time::{Clock, Duration, Instant};
use kabudachi_testkit::FaultingAuthority;
use kabudachi_testkit::RecordSpace;
pub use kabudachi_testkit::StepRecord;

use crate::support::authority::{authority_ttl, epoch};
use crate::support::builders::{message_input, timings, voter_of};
use crate::support::clock::FakeClock;
use crate::support::ids::SequentialIds;
use crate::support::network::FakeNetwork;
use crate::support::spy::Spy;

const SHARD_ID: &str = "shard-1";

/// How many of its voters a leader places each record on.
const REPLICATION_FACTOR: usize = 3;

/// How many ticks in a row a node may be given at one instant before it must
/// have moved on or reported a later deadline.
const TICKS_AT_ONE_INSTANT: usize = 3;

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

/// What a call on the cluster asked of a node, to read its answer with
/// `answer`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Ticket(u64);

/// What a node's scheduler answered a call, once the writes it made were
/// acknowledged.
#[derive(Debug, Clone, PartialEq)]
pub enum Answer {
    Submitted(TaskId),
    Claimed(Claim),
    Started,
    Certified(Certification),
    /// The rejection's `Display`, for a call the scheduler refused outright.
    Refused(String),
    /// A write was refused or the lease ended first.
    NotLeader,
}

/// A leader's held answers and the writes still bearing on what it may tell.
#[derive(Default)]
struct Gating {
    gate: EffectGate<Ticket>,
    unsettled: WriteLedger,
    order: WriteOrder,
}

/// Places `record` on `voters` and writes it to the shared record space as
/// `writer`.
fn store_revision(
    space: &RecordSpace,
    writer: &WorkerId,
    voters: &[WorkerId],
    mut record: TaskRecord,
    now: Instant,
) {
    let write = Write::of(&record);
    let (holders, quorum) = RecordSpace::placement(&write.task_id, voters, REPLICATION_FACTOR);
    record.placement = holders.into_iter().map(Into::into).collect();
    space.write(writer, record, quorum, now);
}

/// `voters`, in a fixed order.
fn sorted(mut voters: Vec<WorkerId>) -> Vec<WorkerId> {
    voters.sort();
    voters
}

/// `record` placed on `voters`, with the quorum that must store it.
fn place(mut record: TaskRecord, voters: &[WorkerId]) -> PlacedWrite {
    let (holders, quorum) =
        RecordSpace::placement(&Write::of(&record).task_id, voters, REPLICATION_FACTOR);
    record.placement = holders.into_iter().map(Into::into).collect();
    PlacedWrite { record, quorum }
}

/// A leader's reconciliation as the harness drives it: it asks every
/// reconcilee it can reach, each answering at once from the record space and
/// from the runs it holds, and stops as its round allows (all answered, or a
/// quorum once the grace has passed). Its scheduler then rebuilds and
/// republishes, and the node is told it has reconciled once every
/// republished write is stored.
struct Reconciliation {
    round: ReconcileRound,
    /// Set once the scheduler has rebuilt: its republished records, written
    /// until every one is stored.
    republish: Option<Republish>,
    /// The voters, sorted, the republish was last placed on.
    placed_on: Vec<WorkerId>,
}

/// Who a held call's answer tells, and which run it is about.
struct Caller {
    claimant: WorkerId,
    run: Option<TaskRunId>,
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
    /// How many reads of the authority's epoch rejoining nodes have asked, to
    /// number each afresh.
    rejoin_reads: u64,
    /// The suspicion timeout every node was built with, reused by `restart_node`.
    suspect_timeout: Duration,
    /// The reconnect timeout every node was built with, if not the default.
    reconnect_timeout: Option<Duration>,
    /// The authority timings every node was built with, reused by
    /// `restart_node`; `None` for a cluster whose nodes have no authority.
    authority_timings: Option<AuthorityTimings>,
    /// Each node's scheduler, handed every grant its node reports.
    schedulers: BTreeMap<WorkerId, ClusterScheduler>,
    /// The spy on each node's scheduler, which keeps the revisions it publishes.
    spies: BTreeMap<WorkerId, Spy>,
    /// Every step that reported a grant or its withdrawal, in order, for
    /// `first_grant_overlap`.
    grant_steps: Vec<StepRecord>,
    stalls: BTreeMap<WorkerId, Stall>,
    /// The steps taken since `record_steps` or the last `take_steps`;
    /// `None` while not recording.
    recorded_steps: Option<Vec<StepRecord>>,
    /// Whether an admitted node is left to report its routing crawl itself
    /// (`routing_crawled`) rather than crawling at once.
    routing_crawls_held: bool,
    /// The shard's Task records, written by each node's leader.
    records: RecordSpace,
    /// Each node's held answers.
    gating: BTreeMap<WorkerId, Gating>,
    /// What a held call will answer once released.
    drafts: BTreeMap<Ticket, Answer>,
    answers: BTreeMap<Ticket, Answer>,
    tickets: u64,
    /// The runs each worker holds as a claimant, as its ledger would: a claim
    /// once the answer granting it was released, running once its start was
    /// answered, succeeded from the moment it reports completion until the
    /// leader's certification is released.
    claimed: BTreeMap<WorkerId, BTreeMap<TaskRunId, ReportedRun>>,
    callers: BTreeMap<Ticket, Caller>,
    /// Each node that took office and has not yet finished reconciling.
    reconciling: BTreeMap<WorkerId, Reconciliation>,
    /// The round of each leader that finished reconciling, kept while it
    /// still has someone to ask or a task to settle: what it learns late is
    /// adopted through `Scheduler::adopt`.
    adopting: BTreeMap<WorkerId, ReconcileRound>,
    /// The ids every scheduler mints from: one sequence for the whole
    /// cluster, as real ids never repeat across leaders.
    ids: SequentialIds,
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
        Cluster::build(voters, pending, suspect_timeout, None, None)
    }

    /// `bootstrap_with_pending(voters, pending, ..)` with every node
    /// reporting a worker lost after `reconnect_timeout` past its suspicion
    /// timeout rather than the default 30 s, for a scenario about losses
    /// that should not simulate that long.
    pub fn bootstrap_with_reconnect_timeout(
        voters: usize,
        pending: usize,
        suspect_timeout: Duration,
        reconnect_timeout: Duration,
    ) -> Self {
        Cluster::build(voters, pending, suspect_timeout, None, Some(reconnect_timeout))
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
        Cluster::bootstrap_with_authority_ttl(voters, pending, suspect_timeout, authority_ttl())
    }

    /// `bootstrap_with_authority(voters, pending, ..)` with the authority's
    /// registrations, fences and warm-up lasting `ttl` instead of the tests'
    /// default, for a simulation whose timescale is far shorter than 30 s.
    pub fn bootstrap_with_authority_ttl(
        voters: usize,
        pending: usize,
        suspect_timeout: Duration,
        ttl: Duration,
    ) -> Self {
        Cluster::build(
            voters,
            pending,
            suspect_timeout,
            Some(AuthorityTimings { ttl }),
            None,
        )
    }

    fn build(
        voters: usize,
        pending: usize,
        suspect_timeout: Duration,
        authority_timings: Option<AuthorityTimings>,
        reconnect_timeout: Option<Duration>,
    ) -> Self {
        let clock = Rc::new(FakeClock::new());
        let network = FakeNetwork::new(Rc::clone(&clock));
        let ttl = authority_timings.map_or_else(authority_ttl, |timings| timings.ttl);
        let authority = FaultingAuthority::new(clock.as_ref().clone(), ttl);
        clock.advance(ttl);
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
            rejoin_reads: 0,
            suspect_timeout,
            reconnect_timeout,
            authority_timings,
            schedulers: BTreeMap::new(),
            spies: BTreeMap::new(),
            grant_steps: Vec::new(),
            stalls: BTreeMap::new(),
            recorded_steps: None,
            routing_crawls_held: false,
            records: RecordSpace::default(),
            gating: BTreeMap::new(),
            drafts: BTreeMap::new(),
            answers: BTreeMap::new(),
            tickets: 0,
            claimed: BTreeMap::new(),
            callers: BTreeMap::new(),
            reconciling: BTreeMap::new(),
            adopting: BTreeMap::new(),
            ids: SequentialIds::new(),
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
                timings: match self.reconnect_timeout {
                    Some(reconnect) => timings(self.suspect_timeout).with_reconnect_timeout(reconnect),
                    None => timings(self.suspect_timeout),
                },
            },
            Entry::Known(known_configuration),
            (*self.clock).clone(),
            self.authority_timings,
        );
        self.nodes.insert(id.clone(), node);
        let spy = Spy::default();
        self.schedulers.insert(
            id.clone(),
            Scheduler::with_observer((*self.clock).clone(), self.ids.clone(), spy.clone()),
        );
        self.spies.insert(id.clone(), spy);
        self.gating.insert(id.clone(), Gating::default());
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
        self.reconcile_due();
        while let Some(at) = self.next_event_at().filter(|at| *at <= end) {
            self.clock.advance(at - self.clock.now());
            self.catch_up_due_schedulers();
            // Outcomes settle before held calls of a node that stopped
            // leading are answered, as the net driver does, so an
            // acknowledgement due at the instant a lease ends is judged by
            // the lease check at that instant.
            self.deliver_acknowledgements();
            self.reconcile_due();
            activity.progressed |= self.run_passes(true).progressed;
            self.reconcile_due();
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
        let next_acknowledgement = self.records.next_due().map(|due| due.max(now));
        let next_stall_end = self.stalls.values().map(|stall| stall.until.max(now)).min();
        let next_reconciliation = self
            .reconciling
            .values()
            .filter_map(|reconciliation| match &reconciliation.republish {
                None => Some(reconciliation.round.grace_ends_at()).filter(|at| *at > now),
                Some(republish) => republish.wake_at().map(|at| at.max(now)),
            })
            .min();
        next_delivery
            .into_iter()
            .chain(next_reconciliation)
            .chain(next_deadline)
            .chain(next_scheduler_deadline)
            .chain(next_acknowledgement)
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
        let mut caught_up = Vec::new();
        for (id, scheduler) in &mut self.schedulers {
            if scheduler
                .next_deadline()
                .is_some_and(|deadline| deadline <= now)
            {
                scheduler.catch_up();
                caught_up.push(id.clone());
                assert!(
                    scheduler.next_deadline().is_none_or(|deadline| deadline > now),
                    "Cluster: node {id:?}'s scheduler is still due at {now:?} after catch_up, \
                     so the clock would never move on"
                );
            }
        }
        for id in &caught_up {
            self.write_revisions(id);
        }
    }

    /// Places on the node's voters and writes every revision its scheduler
    /// published since the last call, and returns their writes.
    fn write_revisions(&mut self, id: &WorkerId) -> Vec<Write> {
        let (Some(spy), Some(node), Some(gating)) = (
            self.spies.get(id),
            self.nodes.get(id),
            self.gating.get_mut(id),
        ) else {
            return Vec::new();
        };
        let voters = node.voters();
        let now = self.clock.now();
        let revisions = spy.take_revisions();
        let writes: Vec<Write> = revisions.iter().map(Write::of).collect();
        for record in gating.order.admit(revisions) {
            store_revision(&self.records, id, &voters, record, now);
        }
        gating.unsettled.made(&writes);
        writes
    }

    /// Hands every write outcome now due to its writer's held answers.
    fn deliver_acknowledgements(&mut self) {
        let mut resolved = Vec::new();
        for outcome in self.records.take_due(self.clock.now()) {
            let (Some(gating), Some(scheduler)) = (
                self.gating.get_mut(&outcome.writer),
                self.schedulers.get(&outcome.writer),
            ) else {
                continue;
            };
            // The republish of a leader that is still reconciling settles its
            // own writes; no held answer waits on them.
            if let Some(republish) = self
                .reconciling
                .get_mut(&outcome.writer)
                .and_then(|reconciliation| reconciliation.republish.as_mut())
            {
                let settled = WriteOutcome {
                    write: outcome.write.clone(),
                    stored: outcome.stored,
                };
                if republish.settled(&settled, self.clock.now()) {
                    continue;
                }
            }
            // A leader that is still reconciling writes without a grant: its
            // writes count as long as its office lasts.
            let leading = scheduler.is_leader() || self.reconciling.contains_key(&outcome.writer);
            // A superseded generation's revision is written only after its
            // successor's is stored, and while the writer still leads.
            let released = gating.order.settled(&outcome.write, outcome.stored && leading);
            gating
                .unsettled
                .settled(&outcome.write, outcome.stored, leading);
            let mut refused = Vec::new();
            if outcome.stored {
                for settled in gating.gate.acknowledged(&outcome.write, leading) {
                    resolved.push(settled);
                }
            } else {
                refused.push(outcome.write.clone());
            }
            match released {
                Settlement::Release(records) => {
                    if let Some(node) = self.nodes.get(&outcome.writer) {
                        let voters = node.voters();
                        let now = self.clock.now();
                        for record in records {
                            store_revision(&self.records, &outcome.writer, &voters, record, now);
                        }
                    }
                }
                Settlement::Refuse(writes) => {
                    for write in writes {
                        gating.unsettled.settled(&write, false, leading);
                        refused.push(write);
                    }
                }
            }
            for write in &refused {
                for ticket in gating.gate.refused(write) {
                    resolved.push(Settled::NotLeader(ticket));
                }
            }
        }
        for settled in resolved {
            self.resolve(settled);
        }
        self.answer_held_calls_of_nodes_not_leading();
    }

    /// A node whose scheduler no longer leads answers every call it holds
    /// `NotLeader` and forgets the writes of the term that ended.
    fn answer_held_calls_of_nodes_not_leading(&mut self) {
        let mut ended: Vec<Ticket> = Vec::new();
        for (id, gating) in &mut self.gating {
            if self.schedulers.get(id).is_some_and(|s| !s.is_leader())
                && !self.reconciling.contains_key(id)
            {
                ended.extend(gating.gate.lease_ended());
                gating.unsettled.clear();
                gating.order.clear();
            }
        }
        for ticket in ended {
            self.resolve(Settled::NotLeader(ticket));
        }
    }

    fn resolve(&mut self, settled: Settled<Ticket>) {
        let (ticket, answer) = match settled {
            Settled::Released(ticket) => {
                let draft = self.drafts.remove(&ticket);
                let answer = draft.expect("a held call has its draft answer");
                self.learn(ticket, &answer);
                (ticket, answer)
            }
            Settled::NotLeader(ticket) => {
                self.drafts.remove(&ticket);
                self.callers.remove(&ticket);
                (ticket, Answer::NotLeader)
            }
        };
        self.answers.insert(ticket, answer);
    }

    /// What a claimant learns from the answer released to it.
    fn learn(&mut self, ticket: Ticket, answer: &Answer) {
        let Some(caller) = self.callers.remove(&ticket) else {
            return;
        };
        let ledger = self.claimed.entry(caller.claimant).or_default();
        match answer {
            Answer::Claimed(claim) => {
                ledger.insert(
                    claim.task_run_id.clone(),
                    ReportedRun {
                        claim: Claim {
                            chain: Vec::new(),
                            ..claim.clone()
                        },
                        state: ReportedState::Claimed,
                    },
                );
            }
            Answer::Started => {
                if let Some(held) = caller.run.and_then(|run| ledger.get_mut(&run))
                    && matches!(held.state, ReportedState::Claimed)
                {
                    held.state = ReportedState::Running;
                }
            }
            Answer::Certified(_) => {
                if let Some(run) = caller.run {
                    ledger.remove(&run);
                }
            }
            Answer::Submitted(_) | Answer::Refused(_) | Answer::NotLeader => {}
        }
    }

    /// Holds `answer` for `ticket` until every write in `writes` is
    /// acknowledged within `at`'s lease.
    fn hold_answer(&mut self, at: &WorkerId, ticket: Ticket, answer: Answer, writes: Vec<Write>) {
        self.drafts.insert(ticket, answer);
        let held = self
            .gating
            .get_mut(at)
            .unwrap_or_else(|| unknown_node("hold_answer", at))
            .gate
            .hold(ticket, writes);
        if let Some(settled) = held {
            self.resolve(settled);
        }
    }

    fn next_ticket(&mut self) -> Ticket {
        self.tickets += 1;
        Ticket(self.tickets)
    }

    /// Submits `submission` to the named node's scheduler. The task's answer
    /// is held until the writes the call made are acknowledged while the node
    /// still leads; a call the scheduler refuses is answered at once. Panics
    /// on an unknown ID.
    pub fn submit(&mut self, at: &WorkerId, submission: Submission) -> Ticket {
        let ticket = self.next_ticket();
        match self.scheduler_mut(at).submit(submission) {
            Err(rejection) => {
                self.answers
                    .insert(ticket, Answer::Refused(rejection.to_string()));
            }
            Ok(task) => {
                let writes = self.write_revisions(at);
                self.hold_answer(at, ticket, Answer::Submitted(task), writes);
            }
        }
        ticket
    }

    /// Asks the named node's scheduler to claim `task` for `claimant`, and
    /// holds the answer as `submit` does. A claim that wrote nothing, because
    /// the task was already decided, waits for that task's writes still
    /// unsettled, or is answered `NotLeader` at once if one was refused.
    /// Panics on an unknown ID.
    pub fn claim(&mut self, at: &WorkerId, claimant: &WorkerId, task: &TaskId) -> Ticket {
        let answer = match self.scheduler_mut(at).request_claim(claimant, task) {
            Ok(claim) => Answer::Claimed(claim),
            Err(rejection) => Answer::Refused(rejection.to_string()),
        };
        let run = match &answer {
            Answer::Claimed(claim) => Some(claim.task_run_id.clone()),
            _ => None,
        };
        let ticket = self.hold_call_on(at, task, answer);
        self.note_caller(ticket, claimant, run);
        ticket
    }

    /// Remembers whom `ticket`'s answer is for, if it is still held.
    fn note_caller(&mut self, ticket: Ticket, claimant: &WorkerId, run: Option<TaskRunId>) {
        if self.answers.contains_key(&ticket) {
            return;
        }
        self.callers.insert(
            ticket,
            Caller {
                claimant: claimant.clone(),
                run,
            },
        );
    }

    /// `claimant` reports to the named node's scheduler that it started
    /// `run`, and the answer is held as `claim`'s is.
    pub fn start(&mut self, at: &WorkerId, claimant: &WorkerId, run: &TaskRunId) -> Ticket {
        let task = self.task_of(at, run);
        let answer = match self.scheduler_mut(at).report_started(claimant, run) {
            Ok(()) => Answer::Started,
            Err(rejection) => Answer::Refused(rejection.to_string()),
        };
        let ticket = self.hold_call_on(at, &task, answer);
        self.note_caller(ticket, claimant, Some(run.clone()));
        ticket
    }

    /// `claimant` reports that it completed `run` with a result of `digest`,
    /// ending the task; the answer is held as `claim`'s is.
    pub fn complete(
        &mut self,
        at: &WorkerId,
        claimant: &WorkerId,
        run: &TaskRunId,
        digest: Digest,
    ) -> Ticket {
        let task = self.task_of(at, run);
        // The claimant holds its result as completed from the moment it
        // reports it, whatever the leader answers, until certified.
        if let Some(held) = self
            .claimed
            .get_mut(claimant)
            .and_then(|ledger| ledger.get_mut(run))
        {
            held.state = ReportedState::Succeeded {
                result_digest: digest.clone(),
            };
        }
        let answer = match self
            .scheduler_mut(at)
            .complete(claimant, run, digest, Completion::Final)
        {
            Ok(certification) => Answer::Certified(certification),
            Err(rejection) => Answer::Refused(rejection.to_string()),
        };
        let ticket = self.hold_call_on(at, &task, answer);
        self.note_caller(ticket, claimant, Some(run.clone()));
        ticket
    }

    /// The task that `run` belongs to, as the named node's scheduler knows it.
    fn task_of(&mut self, at: &WorkerId, run: &TaskRunId) -> TaskId {
        self.scheduler_mut(at)
            .task_run(run)
            .map(|record| record.task_id())
            .expect("the node's scheduler knows the run")
    }

    /// Holds `answer` to a call about `task` until the writes the call made
    /// are acknowledged within the lease. A call that wrote nothing, because
    /// the task was already decided, waits for that task's writes still
    /// unsettled, or is answered `NotLeader` at once if one was refused.
    fn hold_call_on(&mut self, at: &WorkerId, task: &TaskId, answer: Answer) -> Ticket {
        let ticket = self.next_ticket();
        let mut writes = self.write_revisions(at);
        if writes.is_empty() {
            match self.gating[at].unsettled.waits_on(task) {
                Waits::Refused => {
                    self.answers.insert(ticket, Answer::NotLeader);
                    return ticket;
                }
                Waits::Writes(pending) => writes.extend(pending),
            }
        }
        self.hold_answer(at, ticket, answer, writes);
        ticket
    }

    /// What `ticket`'s call was answered; `None` while it is held.
    pub fn answer(&self, ticket: Ticket) -> Option<&Answer> {
        self.answers.get(&ticket)
    }

    /// The shard's Task records, as each node holds them: for tests to set
    /// the acknowledgement delay and read what holders stored.
    pub fn records(&self) -> &RecordSpace {
        &self.records
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
                    .map(|(from, message)| message_input(&from, message)),
            );
            if self.is_stalled(&id, now) {
                self.hold(&id, inputs);
                continue;
            }
            for input in inputs {
                activity.busy.insert(id.clone());
                if !matches!(&input, Input::Message { message, .. } if is_liveness_traffic(message.message()))
                {
                    activity.progressed = true;
                }
                let outputs = self.step(&id, input);
                activity.progressed |= changes_state(&outputs);
            }

            if with_ticks && self.is_due(&id, now) {
                activity.busy.insert(id.clone());
                let state_before = self.nodes[&id].state();
                // A tick can ask the authority a call whose reply, handed
                // back in the same step, makes the next tick due (a member
                // may stand once its read of the authority's epoch is
                // answered), so a node may be due again at once. It must not
                // stay due: a few ticks at one instant are enough to tell.
                for tick in 1..=TICKS_AT_ONE_INSTANT {
                    let outputs = self.step(&id, Input::Tick);
                    activity.progressed |= changes_state(&outputs);
                    if !self.is_due(&id, now) || self.nodes[&id].state() != state_before {
                        break;
                    }
                    assert!(
                        tick < TICKS_AT_ONE_INSTANT,
                        "Cluster: node {id:?} is still due at {now:?} after {tick} ticks that left \
                         it in {state_before:?}, so its timers would never let the clock move on"
                    );
                }
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
                .any(|state| {
                    matches!(
                        state,
                        WorkerState::RollCall
                            | WorkerState::Candidate
                            | WorkerState::LeaderReconciling
                    )
                });

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
        self.records.partition(&group_a, &group_b);
        self.network.partition(group_a, group_b);
        self.report_connection_changes(before);
    }

    /// Lifts the partition and tells every node which connections reopened.
    pub fn heal(&mut self) {
        let before = self.connections();
        self.records.heal();
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

    /// Stops nodes crawling their routing on admission: a node reports a
    /// crawl only once `routing_crawled` tells it one completed.
    pub fn hold_routing_crawls(&mut self) {
        self.routing_crawls_held = true;
    }

    /// Tells the named node that its routing crawl completed, as its driver
    /// would, and returns its outputs. Panics on an unknown ID.
    pub fn routing_crawled(&mut self, id: &WorkerId) -> Vec<Output> {
        if !self.nodes.contains_key(id) {
            unknown_node("routing_crawled", id);
        }
        self.step(id, Input::RoutingCrawled)
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
        let admitted_before = self.node_mut(id, "step").admission();
        let step = self.node_mut(id, "step").step(input.clone());
        let mut outputs = self.drive(id, Some(input), step);
        // The network reaches every node, so a node that was just admitted
        // has crawled to its shard's other workers by its next heartbeat.
        if !self.routing_crawls_held && self.node_mut(id, "step").admission() != admitted_before {
            outputs.extend(self.step(id, Input::RoutingCrawled));
        }
        outputs
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
        self.write_revisions(id);
        self.answer_held_calls_of_nodes_not_leading();
        for output in &all {
            if let Output::Reconcile(term) = output {
                self.begin_reconciliation(id, *term);
            }
        }
        self.reconcile_due();
        all
    }

    fn begin_reconciliation(&mut self, id: &WorkerId, term: ReconcileTerm) {
        let reconcilees = self.nodes[id].reconcilees();
        let round = ReconcileRound::new(term, reconcilees, self.clock.now(), self.suspect_timeout);
        self.reconciling.insert(
            id.clone(),
            Reconciliation {
                round,
                republish: None,
                placed_on: Vec::new(),
            },
        );
    }

    /// Takes every reconciliation as far as it can go now.
    fn reconcile_due(&mut self) {
        let ids: Vec<WorkerId> = self.reconciling.keys().cloned().collect();
        for id in ids {
            self.reconcile(&id);
        }
        let ids: Vec<WorkerId> = self.adopting.keys().cloned().collect();
        for id in ids {
            self.adopt_late(&id);
        }
    }

    /// What a leader that already leads learns late: it asks the reconcilees
    /// that have not answered, as often as the harness runs, and hands what
    /// it learns to its scheduler, until nothing is left to learn or its
    /// office ends. The node checks its lease before the scheduler is handed
    /// anything, as it does on every input, so a leader whose lease ended
    /// since its last step is out of office first, and the round with it.
    fn adopt_late(&mut self, id: &WorkerId) {
        let now = self.clock.now();
        let Some(mut round) = self.adopting.remove(id) else {
            return;
        };
        if self.is_stalled(id, now) {
            self.adopting.insert(id.clone(), round);
            return;
        }
        self.step(id, Input::Tick);
        let leading = self.nodes.get(id).is_some_and(|node| {
            node.state() == WorkerState::Leader && node.office_term() == Some(round.term())
        });
        if !leading {
            return;
        }
        self.collect_answers(id, &mut round, now);
        let node = &self.nodes[id];
        let learnt = round.take_settled(|worker| node.is_member(worker));
        let complete = round.is_complete(|worker| node.is_member(worker));
        let adopted = match self.scheduler_mut(id).adopt(learnt) {
            Ok(adopted) => adopted,
            // Holding office is not leading: no quorum has confirmed the
            // office yet, or the recovery fence lapsed. The round takes back
            // what it learnt and offers it again.
            Err(learnt) => {
                round.give_back(learnt);
                self.adopting.insert(id.clone(), round);
                return;
            }
        };
        self.write_revisions(id);
        if !adopted.silent_holders.is_empty() {
            self.step(id, Input::WatchWorkers(adopted.silent_holders));
        }
        if !complete {
            self.adopting.insert(id.clone(), round);
        }
    }

    /// One reconciliation's next steps: ask, rebuild when its round allows,
    /// write again what was refused, and tell the node once all is stored.
    fn reconcile(&mut self, id: &WorkerId) {
        let now = self.clock.now();
        let Some(mut reconciliation) = self.reconciling.remove(id) else {
            return;
        };
        let term = reconciliation.round.term();
        let reconciling_still = self.nodes.get(id).is_some_and(|node| {
            node.state() == WorkerState::LeaderReconciling && node.office_term() == Some(term)
        });
        if !reconciling_still {
            return;
        }
        if self.is_stalled(id, now) {
            self.reconciling.insert(id.clone(), reconciliation);
            return;
        }
        if reconciliation.republish.is_none() {
            // Workers that joined the roster since the round began are asked
            // too, as the leader's own driver does.
            for worker in self.nodes[id].reconcilees() {
                reconciliation.round.ask_also(worker);
            }
            self.collect_answers(id, &mut reconciliation.round, now);
            let node = &self.nodes[id];
            let answered = node.voters_answered(&reconciliation.round.answered());
            if !reconciliation.round.may_finish(answered, now) {
                self.reconciling.insert(id.clone(), reconciliation);
                return;
            }
            let rebuild = reconciliation
                .round
                .take_settled(|worker| node.is_member(worker));
            let rebuilt = match self.scheduler_mut(id).reconcile(rebuild) {
                Ok(rebuilt) => rebuilt,
                Err(error) => panic!("the scheduler of {id:?} refused its reconciliation: {error:?}"),
            };
            let voters = sorted(self.nodes[id].voters());
            let placed = self
                .spies
                .get(id)
                .map(Spy::take_revisions)
                .unwrap_or_default()
                .into_iter()
                .map(|record| place(record, &voters))
                .collect();
            let retry_after = timings(self.suspect_timeout).heartbeat_interval;
            reconciliation.republish = Some(Republish::new(placed, retry_after));
            reconciliation.placed_on = voters;
            if !rebuilt.silent_holders.is_empty() {
                self.step(id, Input::WatchWorkers(rebuilt.silent_holders));
            }
        }
        if let Some(republish) = reconciliation.republish.as_mut() {
            let voters = sorted(self.nodes[id].voters());
            if !republish.is_done() && voters != reconciliation.placed_on {
                republish.re_place(|write| *write = place(write.record.clone(), &voters), now);
                reconciliation.placed_on = voters;
            }
            for write in republish.due(now) {
                self.records.write(id, write.record, write.quorum, now);
            }
        }
        let stored = reconciliation
            .republish
            .as_ref()
            .is_some_and(Republish::is_done);
        if stored {
            self.adopting.insert(id.clone(), reconciliation.round);
            self.step(id, Input::Reconciled(term));
        } else {
            self.reconciling.insert(id.clone(), reconciliation);
        }
    }

    /// Asks every reconcilee `id` can reach that has not answered, and
    /// fetches the full records its answers name from those that did.
    fn collect_answers(&self, id: &WorkerId, round: &mut ReconcileRound, now: Instant) {
        let proof = self.nodes[id].reconcile_proof();
        for worker in round.unanswered() {
            // A worker answers only the leader it follows, or one whose
            // certificate proves its office, as the driver decides.
            let answers = worker == *id
                || (self.nodes.contains_key(&worker)
                    && !self.is_stalled(&worker, now)
                    && self.records.can_reach(id, &worker)
                    && self.nodes[&worker].may_answer_reconcile(id, proof.as_ref()));
            if answers {
                let _ = round.page(&worker, self.report_of(&worker), now);
            }
        }
        let is_member = |worker: &WorkerId| self.nodes.get(id).is_some_and(|node| node.is_member(worker));
        for task in round.missing_records(is_member) {
            for holder in round.answered() {
                if let Some(record) = self.records.held_by(&holder, &task) {
                    round.fetched(record);
                }
            }
        }
    }

    /// What `worker` reports of what it holds: the records its store holds
    /// and the runs it holds as a claimant, in one page.
    fn report_of(&self, worker: &WorkerId) -> ReportPage {
        ReportPage {
            runs: self
                .claimed
                .get(worker)
                .map(|ledger| ledger.values().cloned().collect())
                .unwrap_or_default(),
            keys: self
                .records
                .held_records(worker)
                .iter()
                .filter_map(|record| wire::held_key(record).ok())
                .collect(),
            last: true,
        }
    }

    /// Answers JOIN for every node back in `Bootstrapping` to rejoin its
    /// shard (a fenced node that found the recovery epoch moved on), as a
    /// seed would: with the newest leader its floor accepts that the network
    /// lets it reach and that is not stalled. A node no such leader leads for
    /// stays `Bootstrapping` until one does. Every node `Joining` on a pointer
    /// it took then reads the authority's epoch, as a driver does, and is told
    /// the answer. Returns the nodes it joined.
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
            let floor = self.nodes[&id].join_floor();
            // A leader names itself (see `WorkerNode::known_leader`), and
            // the harness addresses each node by its id.
            let pointers: Vec<_> = self
                .nodes
                .iter()
                .filter(|(leader, node)| {
                    node.state() == WorkerState::Leader
                        && !self.network.is_partitioned(&id, leader)
                        && !self.is_stalled(leader, now)
                })
                .filter_map(|(leader, node)| node.join_response(leader.as_str().to_string()))
                .collect();
            if let Some(pointer) = floor.newest(&pointers).cloned() {
                let input = Input::JoinAnswer(pointer);
                let step = self.node_mut(&id, "join").step(input.clone());
                self.drive(&id, Some(input), step);
                joined.insert(id);
            }
        }
        self.validate_held_pointers();
        joined
    }

    /// Reads the authority's epoch for every node `Joining` on a pointer it
    /// took and not stalled, and tells the node the answer, as a driver does.
    fn validate_held_pointers(&mut self) {
        let now = self.clock.now();
        let validating: Vec<WorkerId> = self
            .nodes
            .iter()
            .filter(|(id, node)| {
                node.state() == WorkerState::Joining && !self.is_stalled(id, now)
            })
            .map(|(id, _)| id.clone())
            .collect();
        for id in validating {
            let Ok(Some(held)) = self.node_authorities[&id].read_recovery_epoch(&self.shard_id)
            else {
                continue;
            };
            let token = ReplyToken {
                issuer: Issuer::Cascade,
                kind: CallKind::ReadRecoveryEpoch,
                number: self.rejoin_reads,
            };
            self.rejoin_reads += 1;
            for input in [
                Input::AuthorityEpochAsked(token),
                Input::AuthorityEpochRead { token, held },
            ] {
                let step = self.node_mut(&id, "validate").step(input.clone());
                self.drive(&id, Some(input), step);
            }
        }
    }

    /// Whether the named node's scheduler leads now: it holds a grant whose
    /// lease has not ended by the shared clock's now, whether or not the
    /// scheduler has been called since the lease ended. Panics on an unknown
    /// ID.
    pub fn holds_valid_grant(&self, id: &WorkerId) -> bool {
        self.schedulers
            .get(id)
            .unwrap_or_else(|| unknown_node("holds_valid_grant", id))
            .is_leader()
    }

    /// The named node's scheduler, which the harness hands every grant and
    /// lost worker its node reports, for a scenario to submit and claim
    /// through. Panics on an unknown ID.
    pub fn scheduler_mut(&mut self, id: &WorkerId) -> &mut ClusterScheduler {
        self.schedulers
            .get_mut(id)
            .unwrap_or_else(|| unknown_node("scheduler_mut", id))
    }

    /// Every node whose scheduler leads now.
    pub fn valid_grant_holders(&self) -> BTreeSet<WorkerId> {
        self.schedulers
            .iter()
            .filter(|(_, scheduler)| scheduler.is_leader())
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

    /// Crashes the node under `id` and starts its process again, returning the
    /// new node's `WorkerId`. A `WorkerId` names one process incarnation, so
    /// the restarted process comes back under a fresh one, `id` with a restart
    /// count appended, as a pending member seeded with the original
    /// configuration and no admission. The old `id` leaves the cluster: every
    /// peer that reached it is told its connection closed, and nothing is
    /// delivered to it any more (see `run_pass`). The new node starts `Active`
    /// with a fresh scheduler, is wired into the shared clock and network at
    /// once, connected to the peers the network lets it reach, and, in a
    /// cluster whose nodes have an authority, to the authority through the old
    /// node's handle (so any fault set on it still applies). A stall of the
    /// old node ends with it, dropping what was held. Panics on an unknown ID.
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
        self.gating.remove(id);
        self.deadlines.remove(id);
        self.claimed.remove(id);
        self.reconciling.remove(id);
        self.adopting.remove(id);

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
