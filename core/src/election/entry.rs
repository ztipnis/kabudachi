//! How a [`WorkerNode`] comes to be: [`WorkerNode::start`], the one
//! constructor, from the worker's [`Identity`] and the [`Entry`] by which it
//! enters its shard.

use std::collections::BTreeSet;

use crate::configuration::{Configuration, Generation, Tally};
use crate::coordination_authority::RecoveryEpoch;
use crate::hashing::HashFunction;
use crate::protocol::ids::{IncarnationId, ShardId, WorkerId};
use crate::protocol::messages::{JoinResponse, JoinResponseIds};
use crate::protocol::worker_state::WorkerState;
use crate::time::{Clock, Instant};

use super::authority_standing::AuthorityStanding;
use super::cancelled_runs::CancelledRunsLedger;
use super::drain::DrainRequest;
use super::election_round::ElectionRound;
use super::lease::Lease;
use super::self_report::SelfReport;
use super::standing::ShardStanding;
use super::{
    AuthorityTimings, ElectionTimings, Input, KnownConfiguration, RejoinCheck, Step, WorkerNode,
};

/// Who a node is: its worker, that worker's process incarnation, the shard
/// it belongs to, and the timers it runs its election on. Every worker in a
/// shard runs the same timings (see [`ElectionTimings::suspect_timeout`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    pub id: WorkerId,
    pub incarnation: IncarnationId,
    pub shard: ShardId,
    pub timings: ElectionTimings,
}

/// How a node enters its shard. The bootstrap cascade ends by choosing one.
#[derive(Debug, Clone, PartialEq)]
pub enum Entry {
    /// Found a new shard as its only member, at `recovery_epoch`: the node
    /// starts `Active` as the only voter of the genesis configuration,
    /// admitted at the genesis generation, and leads once its suspicion
    /// timeout has passed and its own roll call, of one voter, has elected it.
    /// The epoch's lineage is the one the founder drew (see
    /// [`RecoveryEpoch::founding`]), which a node with an authority must know
    /// to recognise its own epoch there.
    ///
    /// `shard_id` is the incarnation it founds, minted by the founder, or kept
    /// from the record it re-founds.
    ///
    /// `registered_at` is when the founder asked the authority to register
    /// it, before it took ownership of the shard; `None` with no authority.
    /// Its orphan deadline counts from then, not from when the node is
    /// built: a founder that counted from later could still lead after its
    /// registration had lapsed, and after another worker had found the
    /// shard with no one registered and re-founded it.
    Founding {
        shard_id: ShardId,
        recovery_epoch: RecoveryEpoch,
        registered_at: Option<Instant>,
    },
    /// Join the leader this pointer names, as a pending member: one no
    /// quorum counts until an election admits it, which learns the shard's
    /// configuration from its leader's first ack. The pointer must name a
    /// leader of the node's shard (see [`Input::JoinAnswer`]); one
    /// that names none leaves the node `Bootstrapping`.
    Joining(JoinResponse),
    /// Start `Active` inside a configuration already known, at that
    /// configuration's recovery epoch, of that configuration's lineage.
    Known(KnownConfiguration),
}

impl Entry {
    /// The incarnation of the shard this entry enters: the founded one, or
    /// the one the JOIN pointer's leader leads. `None` for `Known`, whose
    /// shard is the identity's.
    pub fn shard_id(&self) -> Option<ShardId> {
        match self {
            Entry::Founding { shard_id, .. } => Some(shard_id.clone()),
            Entry::Joining(pointer) => pointer.shard_id(),
            Entry::Known(_) => None,
        }
    }
}

impl<C: Clock> WorkerNode<C> {
    /// Builds the node `identity` names, entering its shard by `entry`, and
    /// returns it with its first step, which the caller carries out like any
    /// other (see [`super::carry_out`]). For a joiner that step heartbeats
    /// the leader it joined at once; otherwise it asks for nothing and is
    /// due now, so the node is ticked at once to learn its first deadline.
    ///
    /// `authority` is the coordination authority's timings, or `None` for a
    /// node with no authority, which never registers, fences itself or
    /// needs a recovery fence to lead. The node starts connected to no one:
    /// its driver reports the connections it holds as
    /// [`super::Input::PeerConnected`]. Its leader-contact timer starts now,
    /// so a new node is not immediately suspicious.
    ///
    /// # Panics
    ///
    /// Panics if a founding entry names another shard than `identity.shard`.
    /// Panics if `identity.timings.heartbeat_interval`,
    /// `roll_call_deadline` or `clock_drift_divisor` is zero, or if the node
    /// is not alone a quorum of its shard and either twice its heartbeat
    /// interval is not shorter than its lease length or its roll-call
    /// deadline is not shorter than its suspicion timeout: a caller bug. A
    /// lone voter never needs a lease or a suspicion, so it may run with any
    /// suspicion timeout, zero among them, and any roll-call deadline.
    pub fn start(
        identity: Identity,
        entry: Entry,
        clock: C,
        authority: Option<AuthorityTimings>,
    ) -> (Self, Step) {
        let Identity {
            id,
            incarnation,
            shard,
            timings,
        } = identity;
        match entry {
            Entry::Founding {
                shard_id,
                recovery_epoch,
                registered_at,
            } => {
                assert_eq!(
                    shard_id, shard,
                    "a node founds the shard incarnation its identity names"
                );
                let mut node = Self::genesis(
                    id,
                    incarnation,
                    shard,
                    clock,
                    recovery_epoch,
                    authority,
                    timings,
                );
                if let Some(sent_at) = registered_at {
                    node = node.registered_at(sent_at);
                }
                let first = node.due_now();
                (node, first)
            }
            Entry::Joining(pointer) => {
                let mut node =
                    Self::bootstrapping(id, incarnation, shard, clock, authority, timings);
                let first = node.step(Input::JoinAnswer(pointer));
                (node, first)
            }
            Entry::Known(known) => {
                let node = Self::new(id, incarnation, shard, clock, known, authority, timings);
                let first = node.due_now();
                (node, first)
            }
        }
    }

    /// A step that asks for nothing and is due now.
    fn due_now(&self) -> Step {
        Step {
            outputs: Vec::new(),
            next_deadline: Some(self.clock.now()),
        }
    }

    /// Constructs a node in `WorkerState::Active` that already knows its
    /// shard's configuration and its own admission generation in it, at the
    /// configuration's recovery epoch. For a node that must join its shard
    /// first, see [`Self::bootstrapping`]; for the worker that creates a
    /// shard, [`Self::genesis`]. The leader-contact timer starts now so a
    /// new node isn't immediately suspicious. The node starts connected to
    /// no one: its driver reports the connections it holds as
    /// [`Input::PeerConnected`].
    ///
    /// # Panics
    ///
    /// Panics if `timings.heartbeat_interval`, `timings.roll_call_deadline`
    /// or `timings.clock_drift_divisor` is zero, or if the node is not alone
    /// a quorum of `known.configuration` and twice `timings.heartbeat_interval`
    /// is not shorter than `timings.lease_length()`, or `timings.roll_call_deadline`
    /// is not shorter than `timings.suspect_timeout`: a caller bug. A lone
    /// voter never needs a lease or a suspicion, so it may run with any
    /// suspicion timeout, zero among them, and any roll-call deadline.
    fn new(
        my_id: WorkerId,
        incarnation_id: IncarnationId,
        shard_id: ShardId,
        clock: C,
        known: KnownConfiguration,
        authority: Option<AuthorityTimings>,
        timings: ElectionTimings,
    ) -> Self {
        let mut node = Self::with_initial_state(
            WorkerState::Active,
            my_id,
            incarnation_id,
            shard_id,
            clock,
            authority,
            timings,
        );
        let mut alone = Tally::against(&known.configuration);
        alone.record(node.my_id.clone(), known.admission);
        if !alone.has_quorum() {
            assert_heartbeats_keep_a_lease(&timings);
            assert_roll_call_deadline_leaves_room_to_widen(&timings);
        }
        node.standing = ShardStanding::known(known);
        node
    }

    /// Constructs the node of the worker that creates a shard at
    /// `recovery_epoch`, of the lineage the founder drew: it starts `Active` as the only
    /// voter of the genesis configuration, admitted at the genesis
    /// generation. Like any other node it leads once its suspicion timeout
    /// has passed and its own roll call, of one voter, has elected it at the
    /// call's deadline.
    ///
    /// # Panics
    ///
    /// Panics as [`Self::new`] does. The node starts alone a quorum, so its
    /// heartbeat interval is not checked against its lease: a lone node may
    /// run with any suspicion timeout, zero among them. Every worker that
    /// joins its shard later is checked when it is built, and every worker
    /// in a shard runs the same timings (see
    /// [`ElectionTimings::suspect_timeout`]).
    fn genesis(
        my_id: WorkerId,
        incarnation_id: IncarnationId,
        shard_id: ShardId,
        clock: C,
        recovery_epoch: RecoveryEpoch,
        authority: Option<AuthorityTimings>,
        timings: ElectionTimings,
    ) -> Self {
        Self::new(
            my_id,
            incarnation_id,
            shard_id,
            clock,
            KnownConfiguration {
                configuration: Configuration::genesis(recovery_epoch),
                admission: Some(Generation::genesis(recovery_epoch)),
            },
            authority,
            timings,
        )
    }

    /// Constructs a node in `WorkerState::Bootstrapping`: for a fresh node
    /// joining a shard that already exists. It knows no configuration and has
    /// no admission generation; step [`Input::JoinAnswer`] once something
    /// outside `core` (`net`'s `/kabudachi/join/1` handshake) has learned who
    /// leads the shard, to drive `Bootstrapping -> Joining -> Active`. It
    /// learns the shard's configuration from its leader's first ack.
    ///
    /// A `Tick` does nothing in `Bootstrapping` or `Joining`, and neither
    /// state has a deadline: nothing here times out a stalled join, by
    /// design — that is left to whatever drives the join handshake itself,
    /// not this state machine.
    ///
    /// # Panics
    ///
    /// Panics if `timings.heartbeat_interval`, `timings.roll_call_deadline`
    /// or `timings.clock_drift_divisor` is zero, or if twice
    /// `timings.heartbeat_interval` is not shorter than
    /// `timings.lease_length()`, or `timings.roll_call_deadline` is not
    /// shorter than `timings.suspect_timeout`: a caller bug. A joining node's
    /// electorate is never itself alone.
    fn bootstrapping(
        my_id: WorkerId,
        incarnation_id: IncarnationId,
        shard_id: ShardId,
        clock: C,
        authority: Option<AuthorityTimings>,
        timings: ElectionTimings,
    ) -> Self {
        let node = Self::with_initial_state(
            WorkerState::Bootstrapping,
            my_id,
            incarnation_id,
            shard_id,
            clock,
            authority,
            timings,
        );
        assert_heartbeats_keep_a_lease(&timings);
        assert_roll_call_deadline_leaves_room_to_widen(&timings);
        node
    }

    fn with_initial_state(
        state: WorkerState,
        my_id: WorkerId,
        incarnation_id: IncarnationId,
        shard_id: ShardId,
        clock: C,
        authority: Option<AuthorityTimings>,
        timings: ElectionTimings,
    ) -> Self {
        // A zero interval would make a follower's next heartbeat due the
        // instant it sent the last one, so it would never stop heartbeating.
        assert!(
            timings.heartbeat_interval.as_ticks() > 0,
            "ElectionTimings::heartbeat_interval must not be zero"
        );
        // A zero deadline would close a roll call the instant it started,
        // before any other worker could answer it.
        assert!(
            timings.roll_call_deadline.as_ticks() > 0,
            "ElectionTimings::roll_call_deadline must not be zero"
        );
        // A zero divisor has no drift share to compute.
        assert!(
            timings.clock_drift_divisor > 0,
            "ElectionTimings::clock_drift_divisor must not be zero"
        );
        let now = clock.now();
        WorkerNode {
            my_id,
            incarnation_id,
            shard_id,
            state,
            standing: ShardStanding::unjoined(),
            last_leader_contact: now,
            timings,
            clock,
            hash_function: HashFunction::default(),
            authority: authority.map(|authority_timings| {
                AuthorityStanding::starting_at(authority_timings, timings.clock_drift_divisor, now)
            }),
            stop_reason: None,
            term: 0,
            round: ElectionRound::new(now),
            office: None,
            leader: None,
            newest_accepted_ack: None,
            next_heartbeat: None,
            connected: BTreeSet::new(),
            lease: Lease::new(now),
            drain_request: DrainRequest::default(),
            crawled_at_admission: None,
            report: SelfReport::default(),
            cancelled_runs: CancelledRunsLedger::default(),
            rejoin: RejoinCheck::default(),
            outputs: Vec::new(),
        }
    }

    /// Replaces the hash function this node derives the jitter on its
    /// suspicion timeout from (see [`ElectionTimings::suspect_timeout`]).
    /// Workers need not agree on it: the jitter only has to differ between
    /// them.
    pub fn with_hash_function(mut self, hash_function: HashFunction) -> Self {
        self.hash_function = hash_function;
        self
    }

    /// The node's registration was last asked for at `sent_at`, before the
    /// node was built: its orphan deadline counts from then, not from its
    /// construction. The bootstrap cascade registers a founder before it
    /// takes ownership of the shard, and a founder that counted its
    /// registration from later could still lead after it had lapsed, and
    /// after another worker had found the shard with no one registered and
    /// re-founded it. No effect on a node with no authority.
    fn registered_at(mut self, sent_at: Instant) -> Self {
        if let Some(authority) = self.authority.as_mut() {
            authority.restart_at(sent_at);
        }
        self
    }
}

/// Panics unless `timings.roll_call_deadline` is shorter than
/// `timings.suspect_timeout`: roll-call backoff caps a widened deadline at
/// the suspicion timeout and the docs keep the base below it, so a base at
/// or above it leaves no room to widen. Called only for a node that is not
/// alone a quorum, which never suspects anyone.
fn assert_roll_call_deadline_leaves_room_to_widen(timings: &ElectionTimings) {
    assert!(
        timings.roll_call_deadline < timings.suspect_timeout,
        "ElectionTimings::roll_call_deadline ({:?}) must be shorter than \
         ElectionTimings::suspect_timeout ({:?})",
        timings.roll_call_deadline,
        timings.suspect_timeout,
    );
}

/// Panics unless a follower heartbeating every `timings.heartbeat_interval`
/// keeps its leader's lease alive: just before a confirmation arrives, the
/// newest one a leader holds can be two intervals old (and a round trip), so
/// two intervals must fit inside the lease.
fn assert_heartbeats_keep_a_lease(timings: &ElectionTimings) {
    let two_intervals = timings.heartbeat_interval.as_ticks().saturating_mul(2);
    assert!(
        two_intervals < timings.lease_length().as_ticks(),
        "twice ElectionTimings::heartbeat_interval ({:?}) must be shorter than the lease length \
         ({:?}, ElectionTimings::lease_length)",
        timings.heartbeat_interval,
        timings.lease_length(),
    );
}
