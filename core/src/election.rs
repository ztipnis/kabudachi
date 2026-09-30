//! The worker-side election state machine (README §10-§14, ADR-0001): leader
//! liveness, the roll call and the vote, graceful draining, and the
//! coordination authority's registration, fence and recovery path.
//!
//! A [`WorkerNode`] does no I/O of its own. Its driver feeds it [`Input`]s
//! through [`WorkerNode::step`] and carries out the [`Output`]s each returned
//! [`Step`] lists: it sends and publishes the messages, reports the state
//! changes and hands the leadership grant to the worker's scheduler. The node
//! keeps its timers against its own clock, and each step names the next
//! instant at which a [`Input::Tick`] can change anything, so the driver can
//! sleep until then.
//!
//! Followers never hold the member list. A node knows its shard's
//! configuration only as a generation and a voter count (two, for a joint
//! configuration), plus its own admission generation and, while a founding
//! is uncommitted, the one it held before (see [`crate::configuration`]);
//! only the leader holds a [`Roster`] of the members.
//!
//! Liveness comes from heartbeats alone (README §12.1, ADR-0001 decision 16),
//! never from the connections the driver reports. Every follower heartbeats
//! its leader, and the leader answers each heartbeat with an ack, which also
//! carries the leader's configuration and the follower's admission
//! generations. A follower suspects a leader whose acks stop. A leader keeps
//! its quorum only while a quorum of its configuration (of both sides, for
//! a joint one) keeps confirming its acks, which its followers do by echoing
//! the newest one they accepted: it goes `NoQuorum` when its quorum-contact
//! lease runs out, and its leadership grant ends where that lease does.
//!
//! A follower that suspects its leader, after a suspicion timeout jittered
//! per worker and term, publishes a roll call to its shard (see the
//! `roll_call` module); every worker that takes part answers it, or refuses
//! it and says why (see the `ballot` module). If, at the call's deadline, the
//! voters of its configuration among the respondents are a quorum, the
//! initiator stands as the candidate, asks its respondents for their votes
//! (see the `vote_round` module), and wins once those that grant it are a
//! majority of its respondents and the voters among them a quorum of the
//! configuration too. The winning roll call's respondents found the next
//! configuration (ADR-0001 decision 8): a joint one, whose new side has one
//! voter per respondent, every one admitted at its new generation, and
//! whose old side is the configuration the roll call ran under, where each
//! respondent still counts by the admission it held before. Every roll
//! call, win and lease under it needs a majority of both sides, so no
//! election under the old configuration alone can win beside it. The winner
//! leads it, certifies it to every respondent, and commits it to the new
//! side alone once a majority of each side says, in a heartbeat confirming
//! one of its acks, that it holds exactly it; a worker that missed the call
//! is no voter of the new side until the next election it answers. An
//! election won under a joint configuration not yet committed founds
//! nothing new: its winner re-stamps that one at a generation of its own
//! term and commits it. Each such change, and a removal, re-bases the
//! configuration at its new generation and re-admits there the members the
//! leader counts on the new side (see the `configuration` module), so a
//! member that misses the ack of a change is no voter of it until a later
//! ack repairs its admission. A call short of a quorum at its
//! deadline leaves the initiator `NoQuorum`, and a candidacy
//! not won within as long again leaves it `LeaderSuspect`; either way it
//! tries again later, at a later term, after a fresh jittered suspicion
//! timeout. A `NoQuorum` node keeps taking part in other nodes' elections,
//! and an ack from a leader returns it to `Active`.
//!
//! A leader also changes its configuration while it lives (ADR-0001
//! decisions 9 and 10). It admits pending joiners in batches, one change at
//! a time: a joint configuration at its next generation, whose new side
//! takes in each joiner that has confirmed one of its acks, committed like a
//! founding. It applies a departing worker's `SelfRemove`, which a follower
//! sends to its leader alone, at once and with no commit round, unless the
//! worker has seen a later term than the leader's (the term guard). A
//! draining leader announces its own departure on final acks.
//!
//! A node that holds or contests a term steps down once it sees a later one
//! (ADR-0001 decision 14): to `Active` under that term's leader when its ack
//! is what told it, and otherwise to `LeaderSuspect`. Only a vote granted, an
//! accepted ack, a refusal or an accepted election certificate raises the
//! highest term a node has seen, never a roll call it answers, so a follower
//! with a flaky link cannot depose a healthy leader by calling a roll.
//!
//! A node configured with a coordination authority (ADR-0001 decisions 11
//! and 12) also keeps its registration there, through calls it asks its
//! driver to make (see the `authority` module): it fences itself once it
//! has failed to renew for a TTL less drift, and on reconnecting resumes if
//! the shard's recovery epoch there is still its own, lineage included (see
//! `RecoveryEpoch`), and otherwise rejoins. Its leader acts only
//! while it holds the recovery fence, so its grant ends at the earlier of
//! the fence and the quorum-contact lease. A roll call of its own that falls
//! short of its returning quorum takes the authority path (see the
//! `forced_recovery` module): with a majority of the authority's live
//! registrations among its respondents it swaps the recovery epoch, waits
//! out the fence, and leads a configuration founded at the new epoch; if
//! the epoch is missing, the shard is abandoned and the node stops. A node
//! that hears a leader of a later recovery epoch adopts that epoch, and
//! that leader's configuration, from its ack. A leader also reports each
//! worker it has not heard from for a suspicion timeout and a reconnect
//! timeout as lost (README §8.3).
//!
//! Known gaps (see README §27 for the phase plan):
//! - With no authority, no removal reaches a leaderless `NoQuorum` shard:
//!   only a leader applies a departing worker's `SelfRemove`, so such a
//!   shard leaves `NoQuorum` only once enough of its peers return. With an
//!   authority, a departed worker's registration lapses and the authority
//!   path counts it out.
//! - A worker learns of a new leader from the ack that leader sends to every
//!   connected peer when it wins or when a connection to the worker opens, or
//!   from a roll-call refusal naming it. A worker whose ack is lost on a
//!   connection that stays up, or that holds no connection to the new leader,
//!   keeps following its old leader until one of those reaches it: for a
//!   voter with no connection to the new leader, once it suspects its old
//!   leader and a worker that follows the new one refuses its roll call.

mod authority;
mod authority_lease;
mod carry_out;
mod election_round;
mod entry;
mod forced_recovery;
mod lease;

use std::collections::{BTreeMap, BTreeSet};

pub use authority::{
    AuthorityCall, AuthorityReply, AuthorityRequest, AuthorityTimings, CallKind, Issuer,
    ReplyToken, ReplyTokens,
};
pub use carry_out::{AuthorityPerformer, DropMessages, MessageSink, NoAuthority, carry_out};
pub use entry::{Entry, Identity};

use crate::configuration::{Admission, Configuration, Generation, Roster, Tally};
use crate::coordination_authority::{AuthorityError, LiveRegistrations, RecoveryEpoch};
use crate::hashing::{Field, HashFunction};
use crate::protocol::ids::{IdGenerator, IncarnationId, ShardId, WorkerId};
use crate::protocol::messages::prelude::*;
use crate::protocol::messages::{
    AckEcho, ElectionCertificate, ElectionMessage, ElectionReject, ElectionRejectReason,
    JoinResponse, KnownLeader, LeaderHeartbeatAck, SelfRemove, WorkerHeartbeat, election_message,
};
use crate::protocol::worker_state::WorkerState;
use crate::scheduler::{LeadershipGrant, LeaseEnd, Observer, Scheduler};
use crate::time::{Clock, Duration, Instant};

use authority_lease::{AuthorityLease, Reconnect};
use election_round::{ElectionRound, Verdict, View};
use forced_recovery::{ForcedRecovery, Next, cannot_recover_from};
use lease::{Lease, LeaseChange, Office};

/// How long a leader waits, after it would first suspect a silent worker,
/// before it reports that worker lost and its TaskRuns are replayed; and,
/// less drift, how long a worker cut off from its leader or orphaned has
/// to abort its own (README §8.3). See
/// [`WorkerNode::with_reconnect_timeout`].
pub const DEFAULT_RECONNECT_TIMEOUT: Duration = Duration::from_secs(30);

pub struct WorkerNode<C>
where
    C: Clock,
{
    my_id: WorkerId,
    incarnation_id: IncarnationId,
    shard_id: ShardId,
    state: WorkerState,
    recovery_epoch: u64,
    /// The lineage of this node's recovery epoch (see
    /// [`RecoveryEpoch`]); `None` for a node that has not yet joined a
    /// shard.
    recovery_lineage: Option<u64>,
    highest_term_seen: u64,
    last_leader_contact: Instant,
    timings: ElectionTimings,
    clock: C,
    /// What the jitter on this node's suspicion timeout is derived from.
    hash_function: HashFunction,
    /// This node's standing with its coordination authority; `None` for a
    /// node with no authority configured.
    authority: Option<AuthorityLease>,
    /// The authority-path attempt in progress, from the census of the roll
    /// call that fell short, while `NoQuorum` or, waiting out the fence,
    /// `Candidate`.
    recovery: Option<ForcedRecovery>,
    /// The token of the one authority read or swap this node now waits on:
    /// its forced recovery's current step, or, while `Fenced`, its read of
    /// the recovery epoch. Replies arrive whenever the driver gets them,
    /// possibly out of order, so a reply to any earlier call is stale and
    /// ignored.
    awaited_reply: Option<ReplyToken>,
    /// Mints the token of every authority call this node asks. It lives as
    /// long as the node, a rejoin included, so the node never repeats a
    /// number.
    tokens: ReplyTokens,
    /// Why this node stopped; `None` until it is `Stopped`.
    stop_reason: Option<StopReason>,
    /// See [`Self::with_reconnect_timeout`].
    reconnect_timeout: Duration,
    /// While `Leader`: when it last heard from each worker it has not yet
    /// reported lost.
    last_heard: BTreeMap<WorkerId, Instant>,
    /// The configuration this node knows. `None` for a joiner until it
    /// accepts its first leader ack.
    configuration: Option<Configuration>,
    /// The generation at which this node became a voter. `None` for a
    /// pending member.
    admission: Option<Generation>,
    /// While this node holds a joint configuration an election founded,
    /// the admission generation it held before that election admitted it;
    /// `None` otherwise.
    prior_admission: Option<Generation>,
    /// The term this node is contesting or holds. Meaningful from `Candidate`
    /// onward.
    term: u64,
    /// The roll calls this node answered, the votes it granted, and the
    /// roll call or candidacy it runs.
    round: ElectionRound,
    /// The members and pending joiners this node leads. Set when it wins;
    /// meaningful only while `Leader`.
    roster: Option<Roster>,
    /// The leader whose heartbeat ack this node last accepted, or that a JOIN
    /// pointed it at, or that a roll-call refusal named, or this node itself
    /// once it wins, with the term that leader was elected in.
    leader: Option<(WorkerId, u64)>,
    /// The term and send token of the ack this node accepted last, which its
    /// heartbeats echo so the leader learns the ack arrived.
    newest_accepted_ack: Option<AckEcho>,
    /// The leader this node is heartbeating and when its next heartbeat is
    /// due. `None` while it heartbeats no one.
    next_heartbeat: Option<(WorkerId, Instant)>,
    /// The peers the driver reports this node connected to.
    connected: BTreeSet<WorkerId>,
    /// Its grant while `Leader`, and its abort deadline (see
    /// [`Output::Grant`] and [`Output::AbortDeadline`]).
    lease: Lease,
    /// A drain was asked for in a state that cannot drain yet.
    drain_requested: bool,
    /// While `Leader`: the workers whose SELF_REMOVE it has accepted since
    /// it last changed its configuration, to take out together (see
    /// [`Self::apply_pending_removals`]).
    pending_removals: BTreeSet<WorkerId>,
    /// What the step in progress has produced so far.
    outputs: Vec<Output>,
}

/// The timers a [`WorkerNode`] runs its election on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ElectionTimings {
    /// The shortest time a node goes without an accepted leader ack before
    /// it suspects its leader. Each node waits longer, by less than half of
    /// this, by a share hashed from its `WorkerId` and the latest term it
    /// knows of, so workers rarely suspect at the same instant (ADR-0001
    /// decision 15). A leader's lease lasts at most its own
    /// `suspect_timeout` less the drift margin (see
    /// [`Self::clock_drift_divisor`]), which is safe only if no follower
    /// suspects it sooner: every worker in the shard must use the same
    /// value, or at least no follower a shorter one than its leader. A node
    /// also refuses roll calls and votes for exactly this long after it last
    /// heard from its leader (or, before it has one, after it was built or
    /// joined).
    pub suspect_timeout: Duration,
    /// How often a follower heartbeats its leader, which answers each
    /// heartbeat with an ack. A heartbeat confirms the ack that answered the
    /// follower's previous one, so just before a confirmation arrives the
    /// newest one a leader holds can be two intervals and a round trip old:
    /// keep that well inside the lease length (see [`Self::lease_length`]),
    /// or a leader whose followers are all alive runs out of lease. Must not
    /// be zero, and twice it must be shorter than the lease length unless
    /// the node starts alone a quorum (see [`WorkerNode::start`]).
    pub heartbeat_interval: Duration,
    /// How long a roll call runs before its initiator decides on it: it
    /// stands as the candidate if the voters among its respondents are a
    /// quorum by then, and goes `NoQuorum` otherwise (ADR-0001 decisions 13
    /// and 15). A candidate then has as long again to win its vote. A node
    /// that answered another worker's roll call starts none of its own until
    /// twice this long after it answered, the longest that call's census and
    /// vote can take while messages take less than this to arrive: that
    /// worker is being elected meanwhile (ADR-0001 decision 5). An initiator
    /// that keeps failing to find a quorum calls again every roll-call
    /// deadline and suspicion timeout, so with a suspicion timeout no longer
    /// than this it can hold one answerer back for as long as it keeps
    /// calling (others may still call). Keep it well above the time a roll
    /// call takes to reach the shard and its replies to come back, and below
    /// `suspect_timeout`. Usually [`Self::DEFAULT_ROLL_CALL_DEADLINE`]. Must
    /// not be zero (see [`WorkerNode::start`]).
    pub roll_call_deadline: Duration,
    /// How far apart the rates of two workers' clocks, or of a worker's
    /// and its coordination authority's, may be, as a divisor: every
    /// deadline a node keeps on its own clock for something another clock
    /// times gives up `1 / clock_drift_divisor` of its length, rounded up to
    /// a whole tick. Usually [`Self::DEFAULT_CLOCK_DRIFT_DIVISOR`].
    ///
    /// The clocks need not agree on the time, only on its rate. With a
    /// divisor of 10, over one suspicion timeout on a follower's clock the
    /// leader's clock advances at least nine tenths of it, so a leader whose
    /// lease is a suspicion timeout less a tenth (see [`Self::lease_length`])
    /// stops acting before any follower that received its last confirmed
    /// ack can suspect it (ADR-0001 decision 16). The same share comes off a
    /// registration or fence TTL (the node gives up before the authority
    /// does), off the time a worker takes to fence itself and abort its
    /// runs (ADR-0001 decision 12), and off the time before which no leader
    /// can replay a cut-off worker's runs (see [`Output::AbortDeadline`]).
    /// Lower it on hosts whose clock rates can differ more. Every worker in
    /// the shard must use the same value. Must not be zero.
    pub clock_drift_divisor: u64,
}

impl ElectionTimings {
    /// The default `roll_call_deadline`, set from the census latency
    /// measured in Phase 2 (ADR-0001 decision 15): over 30 leader losses in
    /// a fully connected shard of five, all in one process on one loopback
    /// host (a debug build), each of the 90 replies to the winning roll call
    /// reached its initiator within 11 ms of the call (p50 6 ms; the p99 of
    /// 90 is their maximum). 250 ms leaves over twenty times that for
    /// replies crossing hosts, a publish relayed through the gossip mesh
    /// rather than sent to a direct peer (not exercised there), and a loaded
    /// host, and adds a quarter of a second to each election. A deadline
    /// too short costs a `NoQuorum` and a retry, never safety. Deployments
    /// whose round trips run to tens of milliseconds, or whose shards are
    /// much larger, should measure their own and raise it.
    pub const DEFAULT_ROLL_CALL_DEADLINE: Duration = Duration::from_millis(250);

    /// The default `clock_drift_divisor`: clock rates within a tenth of
    /// each other, so a lease lasts at most nine tenths of the suspicion
    /// timeout.
    pub const DEFAULT_CLOCK_DRIFT_DIVISOR: u64 = 10;

    /// Timings with this suspicion timeout and heartbeat interval, which
    /// have no default (they depend on the deployment's network), and every
    /// other setting at its default: [`Self::DEFAULT_ROLL_CALL_DEADLINE`]
    /// and [`Self::DEFAULT_CLOCK_DRIFT_DIVISOR`].
    pub fn new(suspect_timeout: Duration, heartbeat_interval: Duration) -> Self {
        ElectionTimings {
            suspect_timeout,
            heartbeat_interval,
            roll_call_deadline: Self::DEFAULT_ROLL_CALL_DEADLINE,
            clock_drift_divisor: Self::DEFAULT_CLOCK_DRIFT_DIVISOR,
        }
    }

    /// Replaces [`Self::DEFAULT_ROLL_CALL_DEADLINE`] (see
    /// [`Self::roll_call_deadline`]).
    pub fn with_roll_call_deadline(mut self, roll_call_deadline: Duration) -> Self {
        self.roll_call_deadline = roll_call_deadline;
        self
    }

    /// Replaces [`Self::DEFAULT_CLOCK_DRIFT_DIVISOR`] (see
    /// [`Self::clock_drift_divisor`]).
    pub fn with_clock_drift_divisor(mut self, clock_drift_divisor: u64) -> Self {
        self.clock_drift_divisor = clock_drift_divisor;
        self
    }

    /// How long a leader's quorum-contact lease lasts after the
    /// quorum-contact time: `suspect_timeout` less its drift share, a
    /// `1 / clock_drift_divisor` of it rounded up to a whole tick (rounding
    /// down would let the lease outlast the share it promises to keep).
    pub fn lease_length(&self) -> Duration {
        self.less_drift(self.suspect_timeout)
    }

    /// `duration` less its share for clock drift (see
    /// [`Self::clock_drift_divisor`]).
    pub(crate) fn less_drift(&self, duration: Duration) -> Duration {
        authority::less_drift(duration, self.clock_drift_divisor)
    }
}

/// The configuration a [`WorkerNode`] started on [`Entry::Known`] starts
/// with, and its own place in it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnownConfiguration {
    pub configuration: Configuration,
    /// The node's admission generation; `None` for a pending member.
    pub admission: Option<Generation>,
}

/// Something that happens to a [`WorkerNode`], fed in through
/// [`WorkerNode::step`].
// A message carrying a configuration makes `Message` far larger than the
// other variants. An input is built and consumed within one step, never
// stored in bulk, so boxing it would buy nothing but indirection at every
// call site.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq)]
pub enum Input {
    /// Time has passed: the node checks its timers against its clock.
    Tick,
    /// An election message from another worker, sent to this node or
    /// published to its shard.
    Message {
        from: WorkerId,
        message: ElectionMessage,
    },
    /// This node now holds a connection to the peer. A leader acks any peer
    /// it newly connects to, so the peer learns who leads. Reporting a peer
    /// that is already connected, or this node itself, changes nothing.
    PeerConnected(WorkerId),
    /// This node no longer holds a connection to the peer. Reporting a peer
    /// that is not connected changes nothing.
    PeerDisconnected(WorkerId),
    /// The coordination authority answered a call this node asked for (see
    /// [`Output::Authority`]). A node with no authority ignores it.
    Authority(AuthorityReply),
    /// Leave the shard gracefully (README §12.3, §18, ADR-0001 decision
    /// 10): send `SelfRemove` to the leader this node follows, if any, or as
    /// the leader announce the configuration without itself on a final ack
    /// to every connected peer, and end `Stopped`. From `Active` or `Leader`
    /// this happens at once; from `Draining` or `Stopped` the request does
    /// nothing; from any other state, `Fenced` among them, the node keeps it and
    /// drains as soon as it reaches `Active` or `Leader`, in the same step. A
    /// driver asks once.
    ///
    /// A drain kept in a state the node never leaves for `Active` or
    /// `Leader` waits for good: a node whose peers never return, which keeps
    /// retrying roll calls from `NoQuorum`, or a `Bootstrapping` node that
    /// never joins. A driver that waits for `Stopped` must not rely on it
    /// there.
    Drain,
}

/// Something a [`WorkerNode`] asks its driver to do.
#[derive(Debug, Clone, PartialEq)]
pub enum Output {
    /// Send `message` to `to`. Delivery need not be immediate or reliable:
    /// the election tolerates delayed, dropped, duplicated and reordered
    /// messages.
    Send {
        to: WorkerId,
        message: ElectionMessage,
    },
    /// Publish `message` to every worker subscribed to the node's shard.
    /// Delivery is not guaranteed: the election tolerates a publish that
    /// reaches some workers and not others, late, twice or out of order.
    Publish { message: ElectionMessage },
    /// The node moved into this state. A step that moves it more than once
    /// reports each move, in order.
    StateChanged(WorkerState),
    /// The node's leadership grant, reported whenever it changes and whenever
    /// the node leaves `Leader`, for its driver to hand to the node's
    /// scheduler (see [`carry_out`]): `Some` while the node is
    /// `Leader` with a lease, ending where the lease does; `None` otherwise.
    ///
    /// A leader that is not alone a quorum first holds a lease once a quorum
    /// has confirmed one of its acks, and its lease end moves as more
    /// confirmations arrive. A node that leaves `Leader` reports `None` just
    /// before that state change, so nothing it asks for after leaving can
    /// let another leader act while its own grant stands.
    Grant(Option<LeadershipGrant>),
    /// Make this call on the node's coordination authority, and hand the
    /// node the reply as [`Input::Authority`] (see [`AuthorityCall::perform`]).
    /// Only a node with an authority asks.
    Authority(AuthorityCall),
    /// While `Leader`: the worker has not been heard from for a suspicion
    /// timeout and then a reconnect timeout (README §8.3), so every TaskRun
    /// it holds is lost and may be replayed (see [`carry_out`]).
    /// Reported once; a worker heard from again is watched afresh.
    WorkerLost(WorkerId),
    /// By when, on the node's clock, this worker must have aborted every
    /// TaskRun it is running (README §8.3, §25.1.9): `Some` once it has gone
    /// a suspicion timeout, less drift, without evidence that its leader
    /// still hears it, or once it has fenced itself (ADR-0001 decision 12);
    /// `None` while it has nothing to abort. Reported whenever it changes,
    /// like [`Output::Grant`]: a later report replaces an earlier one, so a
    /// worker that its leader hears again before the deadline withdraws it
    /// and keeps its runs. For the worker's task executor; the scheduler has
    /// nothing to do with it.
    ///
    /// The deadline comes before any leader can replay those runs. A leader
    /// replays a worker's runs (see [`Output::WorkerLost`]) a suspicion
    /// timeout and a reconnect timeout after it last heard the worker, or
    /// after it won if it has not heard it since. An ack from a leader that
    /// holds a grant echoes the send instant of the heartbeat it answers, so
    /// the follower knows that leader heard it no earlier than that, and no
    /// rival can win before that leader's grant ends, after it sent the ack;
    /// a leader that stops leading knows no rival won before its own grant
    /// ended. From
    /// the latest such instant the deadline is the suspicion timeout plus the
    /// reconnect timeout, less a tenth for clock drift (the same rate bound
    /// the leader lease assumes). Every worker in the shard must use the same
    /// `suspect_timeout` and reconnect timeout (see
    /// [`WorkerNode::with_reconnect_timeout`]).
    AbortDeadline(Option<Instant>),
    /// An alert: the authority path found the shard's recovery epoch gone,
    /// so the shard is abandoned (README §15.5) and the node has stopped
    /// (see [`StopReason::Abandoned`]). A restart re-enters the bootstrap
    /// cascade.
    ShardAbandoned,
}

/// Why a node is `Stopped`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    /// It drained, leaving the shard gracefully.
    Drained,
    /// Its shard was abandoned (ADR-0001 decision 11.5): neither a quorum of
    /// its configuration nor the authority's recovery epoch was left to
    /// prove the shard's continuity.
    Abandoned,
}

/// What one call into a [`WorkerNode`] produced. A driver that drops one
/// loses the messages it asks to send and the deadline it reports, and can
/// leave the node's scheduler with a stale grant. The node reports a grant
/// only when it changes, so after a dropped `Grant(None)` an unbounded grant
/// keeps the scheduler leading for ever: unlike a lost message, nothing in
/// the election makes up for it.
#[derive(Debug, Clone, PartialEq)]
#[must_use = "a Step carries messages to send, the node's next deadline and \
              any change to its leadership grant"]
pub struct Step {
    /// In the order the node produced them.
    pub outputs: Vec<Output>,
    /// The earliest instant at which an [`Input::Tick`] can change anything.
    /// `None` when only another input can. A `Tick` at this instant always
    /// moves the node into another state or reports a later deadline (or
    /// none), so a driver may tick the node again at once while its deadline
    /// has come.
    pub next_deadline: Option<Instant>,
}

/// Applies to `scheduler` what `outputs`, one step of a worker's election,
/// ask of it, in order: each leadership grant the step reports, and each
/// worker it reports lost, whose TaskRuns `Scheduler::lose_worker` replays.
/// A lost worker reported to a scheduler that no longer leads changes
/// nothing. Messages, authority calls, state changes, the abort deadline and
/// alerts are the driver's to carry out and leave it alone.
///
/// `scheduler` must read the clock the node reads: a grant's lease ends at
/// an instant of the node's clock, and the scheduler compares it with its
/// own.
pub(crate) fn apply_to_scheduler<C: Clock, I: IdGenerator, O: Observer>(
    outputs: &[Output],
    scheduler: &mut Scheduler<C, I, O>,
) {
    for output in outputs {
        match output {
            Output::Grant(grant) => scheduler.set_leadership_grant(*grant),
            Output::WorkerLost(worker) => {
                // Refused only when this scheduler no longer leads, and then
                // the next leader decides what the worker held.
                let _ = scheduler.lose_worker(worker);
            }
            Output::Send { .. }
            | Output::Publish { .. }
            | Output::StateChanged(_)
            | Output::Authority(_)
            | Output::AbortDeadline(_)
            | Output::ShardAbandoned => {}
        }
    }
}

impl<C> WorkerNode<C>
where
    C: Clock,
{
    /// Constructs a node in `WorkerState::Active` that already knows its
    /// shard's configuration and its own admission generation in it, at the
    /// configuration's recovery epoch. For a node that must join its shard
    /// first, see [`Self::bootstrapping`]; for the worker that creates a
    /// shard, [`Self::genesis`]. The leader-contact timer starts now so a
    /// new node isn't immediately suspicious. The node starts connected to
    /// no one: its driver reports the connections it holds as
    /// [`Input::PeerConnected`]. Its recovery epoch is of lineage 0 unless
    /// [`Self::with_recovery_lineage`] names another.
    ///
    /// # Panics
    ///
    /// Panics if `timings.heartbeat_interval`, `timings.roll_call_deadline`
    /// or `timings.clock_drift_divisor` is zero, or if the node is not alone
    /// a quorum of `known.configuration` and twice `timings.heartbeat_interval`
    /// is not shorter than `timings.lease_length()`: a caller bug. A lone
    /// voter never needs a lease, so it may run with any suspicion timeout,
    /// zero among them.
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
        }
        node.recovery_epoch = known.configuration.generation().recovery_epoch();
        node.recovery_lineage = Some(0);
        node.configuration = Some(known.configuration);
        node.admission = known.admission;
        node
    }

    /// Constructs the node of the worker that creates a shard at
    /// `recovery_epoch` (ADR-0001 decision 1): it starts `Active` as the only
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
        recovery_epoch: u64,
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

    /// Constructs a node in `WorkerState::Bootstrapping` (README §27 Phase 2
    /// bootstrap join protocol): for a fresh node joining a shard that already
    /// exists. It knows no configuration and has no admission generation;
    /// call [`Self::finish_joining`] once something outside `core` (`net`'s
    /// `/kabudachi/join/1` handshake) has learned who leads the shard, to
    /// drive `Bootstrapping -> Joining -> Active`. It learns the shard's
    /// configuration from its leader's first ack.
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
    /// `timings.lease_length()`: a caller bug. A joining node's electorate is
    /// never itself alone.
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
            recovery_epoch: 0,
            recovery_lineage: None,
            highest_term_seen: 0,
            last_leader_contact: now,
            timings,
            clock,
            hash_function: HashFunction::default(),
            authority: authority.map(|authority_timings| {
                AuthorityLease::starting_at(authority_timings, timings.clock_drift_divisor, now)
            }),
            recovery: None,
            awaited_reply: None,
            tokens: ReplyTokens::new(Issuer::Node),
            stop_reason: None,
            reconnect_timeout: DEFAULT_RECONNECT_TIMEOUT,
            last_heard: BTreeMap::new(),
            configuration: None,
            admission: None,
            prior_admission: None,
            term: 0,
            round: ElectionRound::new(now),
            roster: None,
            leader: None,
            newest_accepted_ack: None,
            next_heartbeat: None,
            connected: BTreeSet::new(),
            lease: Lease::new(now),
            drain_requested: false,
            pending_removals: BTreeSet::new(),
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
        if let Some(lease) = self.authority.as_mut() {
            lease.restart_at(sent_at);
        }
        self
    }

    /// Makes `lineage` the lineage of this node's recovery epoch (see
    /// [`RecoveryEpoch`]): the one the bootstrap cascade drew when it
    /// founded the shard, which a node with an authority must know to
    /// recognise its own epoch there. Built by [`Self::new`] or
    /// [`Self::genesis`], a node's epoch is otherwise of lineage 0. No
    /// effect on a node that has not joined a shard yet.
    fn with_recovery_lineage(mut self, lineage: u64) -> Self {
        if self.recovery_lineage.is_some() {
            self.recovery_lineage = Some(lineage);
        }
        self
    }

    /// Replaces [`DEFAULT_RECONNECT_TIMEOUT`] (README §8.3). A leader reports
    /// a worker lost once it has not heard from it for its `suspect_timeout`
    /// and then this long; a worker must abort its TaskRuns within nine
    /// tenths of that span after its leader last provably heard it, and
    /// within nine tenths of this after it fences itself (see
    /// [`Output::AbortDeadline`]). Every worker in the shard must use the
    /// same value, or a leader could replay the work of a worker that is
    /// still running it.
    pub fn with_reconnect_timeout(mut self, reconnect_timeout: Duration) -> Self {
        self.reconnect_timeout = reconnect_timeout;
        self
    }

    pub fn state(&self) -> WorkerState {
        self.state
    }

    pub fn term(&self) -> u64 {
        self.term
    }

    /// This node's clock's reading now: the time base of every instant it
    /// reports, such as a grant's lease end.
    pub fn now(&self) -> Instant {
        self.clock.now()
    }

    /// The timers this node runs its election on.
    pub fn timings(&self) -> ElectionTimings {
        self.timings
    }

    /// The id this node was started under (see [`Identity::id`]).
    pub fn id(&self) -> &WorkerId {
        &self.my_id
    }

    pub fn shard_id(&self) -> &ShardId {
        &self.shard_id
    }

    /// The highest term this node has seen: raised by a vote it granted, an
    /// ack it accepted, a refusal, a certificate or a heartbeat naming a
    /// later term, the leader a JOIN pointed it at, winning an election, and
    /// standing through the authority path. Never by a roll call it
    /// answered, nor by standing as a candidate in an election it has not
    /// won (ADR-0001 decision 14 as amended 2026-09-28).
    pub fn highest_term_seen(&self) -> u64 {
        self.highest_term_seen
    }

    /// This node's recovery epoch: its configuration's when built with one
    /// ([`Self::start`]), taken from the leader a JOIN pointed it at
    /// ([`Self::finish_joining`]), moved on by an authority-path recovery of
    /// its own, and adopted from the ack of a leader of a later epoch.
    pub fn recovery_epoch(&self) -> u64 {
        self.recovery_epoch
    }

    /// The lineage of this node's recovery epoch (see [`RecoveryEpoch`]),
    /// learned with the epoch; `None` until the node has joined a shard.
    pub fn recovery_lineage(&self) -> Option<u64> {
        self.recovery_lineage
    }

    /// This node's recovery epoch with its lineage, as the authority would
    /// hold it; `None` until the node has joined a shard.
    fn own_recovery_epoch(&self) -> Option<RecoveryEpoch> {
        self.recovery_lineage
            .map(|lineage| RecoveryEpoch::new(self.recovery_epoch, lineage))
    }

    /// Why this node stopped; `None` unless it is `Stopped`.
    pub fn stop_reason(&self) -> Option<StopReason> {
        self.stop_reason
    }

    /// The configuration this node knows: the one it was built with or the
    /// newest a leader's ack or election certificate has carried since.
    /// `None` for a joiner that has accepted neither yet.
    pub fn configuration(&self) -> Option<&Configuration> {
        self.configuration.as_ref()
    }

    /// The generation at which this node became a voter; `None` for a
    /// pending member.
    pub fn admission(&self) -> Option<Generation> {
        self.admission
    }

    /// The admission generation this node held before the election that
    /// founded the joint configuration it holds admitted it; `None` once
    /// that configuration is committed, and for any other configuration.
    pub fn prior_admission(&self) -> Option<Generation> {
        self.prior_admission
    }

    /// Both admission generations a quorum counts this node by.
    fn counted_admission(&self) -> Admission {
        Admission {
            current: self.admission,
            prior: self.prior_admission,
        }
    }

    /// Whether this node has no admission generation: it joined through a
    /// JOIN and neither a leader's ack nor an election certificate has
    /// admitted it since. It claims work, and it answers roll calls and
    /// grants votes as a new voter, but no quorum counts it.
    pub fn is_pending_member(&self) -> bool {
        self.admission.is_none()
    }

    /// The workers that have answered the roll call this node is running,
    /// itself included: none unless it is `RollCall` with a call it has not
    /// given up for a better one. For observing how long a census takes to
    /// come back (the roll-call deadline must outlast it: see
    /// [`ElectionTimings::roll_call_deadline`]).
    pub fn roll_call_respondents(&self) -> impl Iterator<Item = &WorkerId> {
        self.round.respondents()
    }

    /// The leader this node would point a joining worker at, with the term
    /// that leader was elected in: itself while `Leader`, and while `Active`
    /// the leader whose heartbeat ack it last accepted, or that a JOIN
    /// pointed it at if no ack has arrived since.
    ///
    /// A node with no configuration (a joiner that has accepted no ack yet)
    /// also keeps naming that leader while `LeaderSuspect`: it cannot elect a
    /// replacement (it starts no roll call), it keeps heartbeating that
    /// leader until an ack returns it to `Active`, and dropping its leader
    /// would leave every joiner it answers with nowhere to go. If that leader
    /// has since gone, the pointer costs a joiner time but nothing worse: the
    /// joiner cannot connect to the named leader, so it passes the answer
    /// over and keeps asking its seeds.
    ///
    /// `None` in every other state: a node that suspects its leader or is
    /// electing a new one has no leader it can vouch for. `None` too once
    /// this node has seen a term later than the named leader's (by granting
    /// a vote in it, say): a newer leader may lead by then.
    pub fn known_leader(&self) -> Option<(WorkerId, u64)> {
        let named = match self.state {
            WorkerState::Leader => Some((self.my_id.clone(), self.term)),
            WorkerState::Active => self.leader.clone(),
            WorkerState::LeaderSuspect if self.configuration.is_none() => self.leader.clone(),
            _ => None,
        };
        named.filter(|(_, term)| *term >= self.highest_term_seen)
    }

    /// The `JOIN_RESPONSE` this node hands a joiner right now: the leader
    /// [`Self::known_leader`] names (itself while `Leader`), with that
    /// leader's term, at `leader_addr`, and this node's recovery epoch and
    /// its lineage. `None` when it knows no leader.
    ///
    /// `leader_addr` is the named leader's address, which the node does not
    /// know: its driver resolves it (see `known_leader` for whom to resolve).
    pub fn join_response(&self, leader_addr: String) -> Option<JoinResponse> {
        let (leader_id, term) = self.known_leader()?;
        Some(JoinResponse {
            leader_id: Some(leader_id.into()),
            leader_multiaddr: leader_addr,
            term,
            recovery_epoch: self.recovery_epoch,
            recovery_epoch_lineage: self.recovery_lineage.unwrap_or_default(),
        })
    }

    /// Handles one input and returns what the node asks its driver to do,
    /// with the next instant at which a `Tick` can change anything.
    ///
    /// Whatever the input, a node with an authority whose registration has
    /// lapsed first fences itself (see [`Self::orphan_if_unregistered`]),
    /// and a leader whose quorum-contact lease has run out first gives up
    /// leading (see [`Self::lose_quorum_if_its_lease_ended`]): a node paused
    /// past either must not act on the inputs held while it was paused
    /// before its next `Tick`. A paused leader would otherwise still lead,
    /// and, finding the authority flushed, republish its old epoch over one
    /// the shard recovered to meanwhile.
    pub fn step(&mut self, input: Input) -> Step {
        let orphaned = self.orphan_if_unregistered();
        self.lose_quorum_if_its_lease_ended();
        match input {
            Input::Tick if orphaned => {}
            Input::Tick => self.tick(),
            Input::Message { from, message } => self.on_message(from, message),
            // A connection to itself is no peer: a leader would ack itself.
            Input::PeerConnected(peer) if peer != self.my_id => self.on_peer_connected(peer),
            Input::PeerConnected(_) => {}
            Input::PeerDisconnected(peer) => {
                self.connected.remove(&peer);
            }
            Input::Authority(reply) => self.on_authority_reply(reply),
            Input::Drain => self.request_drain(),
        }
        self.finish_step()
    }

    /// Completes the bootstrap join handshake: records the leader `pointer`
    /// names, with its term and recovery epoch, and drives `Bootstrapping ->
    /// Joining -> Active` as a pending member, with no configuration until
    /// its leader's first ack carries one. There is no direct `Bootstrapping
    /// -> Active` edge in [`WorkerState::can_transition_to`], so this goes
    /// through `Joining` explicitly.
    ///
    /// A no-op outside `Bootstrapping` — a node started as a founder or
    /// inside a known configuration (already `Active`) or one that already
    /// finished joining has nothing left to join — and for a pointer that names no leader ("no leader
    /// known") or a leader of a recovery epoch older than the node's own,
    /// which leaves the node `Bootstrapping` so its driver can ask again.
    ///
    /// The wire handshake that produces `pointer` — dialing seed addresses,
    /// sending `JOIN_REQUEST`, taking the first `JOIN_RESPONSE` that names a
    /// leader — is entirely `net`'s concern (a separate `/kabudachi/join/1`
    /// request_response protocol, not an `ElectionMessage`); this method only
    /// performs the resulting state transition. Joining publishes nothing.
    pub fn finish_joining(&mut self, pointer: &JoinResponse) -> Step {
        self.join(pointer);
        self.finish_step()
    }

    /// Hands back everything this step produced, with the next deadline.
    /// Whatever the input, a node with an authority first asks for any
    /// registration or fence that has come due (see
    /// [`Self::ask_authority_if_due`]), a follower heartbeats its leader if
    /// that has come due (see [`Self::heartbeat_leader_if_due`]), and a
    /// leader reports its grant if the step changed it.
    fn finish_step(&mut self) -> Step {
        self.ask_authority_if_due();
        self.heartbeat_leader_if_due();
        self.report_lease_changes();
        Step {
            outputs: std::mem::take(&mut self.outputs),
            next_deadline: self.next_deadline(),
        }
    }

    /// The earliest instant at which a `Tick` can change anything.
    ///
    /// A `Tick` at the instant this returns must move the node into another
    /// state or make this return a later instant or `None`: a driver ticks
    /// the node while its deadline has come, without letting time pass in
    /// between (as `net`'s `run_driver` does), and would otherwise never
    /// stop. `election_step_test`'s
    /// `a_tick_at_the_deadline_moves_a_node_on_in_every_state_that_reports_one`
    /// checks it.
    ///
    /// - `Active`: the first instant past its jittered suspicion timeout
    ///   (see [`Self::jittered_suspect_timeout`]), or its next heartbeat to
    ///   its leader if that comes first.
    /// - `LeaderSuspect` and `NoQuorum`: when it may start a roll call (see
    ///   [`Self::next_roll_call_due`]), or its next heartbeat to its leader
    ///   if that comes first; `None` for a node with neither, a joiner with
    ///   no configuration and no leader to heartbeat.
    /// - `RollCall`: its roll call's deadline, or its next heartbeat to its
    ///   leader if that comes first.
    /// - `Candidate`: its vote's deadline.
    /// - `Leader`: when it goes `NoQuorum` unless more confirmations arrive
    ///   (never, for a leader that alone is a quorum), or when it next has a
    ///   worker to report lost, whichever comes first.
    /// - Every other state: `None`.
    ///
    /// With an authority, also the lease's next registration or fence
    /// attempt and, until it is `Fenced`, when it fences itself, in every
    /// state in which it keeps a registration. In every state, also when its
    /// contact floor goes stale, while that is still ahead (see
    /// [`Output::AbortDeadline`]): a `Tick` then reports the deadline.
    fn next_deadline(&self) -> Option<Instant> {
        let lease_deadline = self
            .authority
            .as_ref()
            .filter(|_| self.registers_with_authority())
            .map(|lease| lease.next_deadline(self.state == WorkerState::Fenced));
        earliest(
            earliest(self.election_deadline(), lease_deadline),
            self.lease.next_deadline(&self.timings, self.clock.now()),
        )
    }

    /// What [`Self::next_deadline`] reports, leaving the authority lease
    /// aside.
    fn election_deadline(&self) -> Option<Instant> {
        let next_heartbeat = self.next_heartbeat.as_ref().map(|(_, at)| *at);
        match self.state {
            WorkerState::Active => {
                let suspicion = self.last_leader_contact
                    + self.jittered_suspect_timeout()
                    + Duration::from_ticks(1);
                Some(next_heartbeat.map_or(suspicion, |at| at.min(suspicion)))
            }
            WorkerState::LeaderSuspect | WorkerState::NoQuorum => {
                earliest(next_heartbeat, self.next_roll_call_due())
            }
            WorkerState::RollCall => earliest(next_heartbeat, self.round.next_deadline()),
            WorkerState::Candidate => self.round.next_deadline(),
            WorkerState::Leader => earliest(
                self.roster
                    .as_ref()
                    .and_then(|roster| self.lease.no_quorum_at(&self.my_id, roster, &self.timings)),
                self.next_worker_lost_at(),
            ),
            _ => None,
        }
    }

    /// Moves to `next` and reports the move. Leaving `Leader` first reports
    /// that the node holds no grant (see [`Output::Grant`]), and leaving the
    /// states that lead or stand to lead gives up any fence. Reaching
    /// `Active` or `Leader` applies a drain kept from an earlier request at
    /// once (see [`Input::Drain`]), so the node can come out of this
    /// `Stopped`.
    fn transition_to(&mut self, next: WorkerState) {
        // Every caller moves along an edge of the transition table, checked
        // by the state it guards on first; no input can reach an illegal
        // edge, so one here is a bug in this module.
        assert!(
            self.state.can_transition_to(next),
            "illegal election state transition {:?} -> {next:?}",
            self.state
        );
        // No edge leads from `Leader` back to itself.
        if self.state == WorkerState::Leader {
            self.lease.withdraw_grant(self.clock.now());
            self.outputs.push(Output::Grant(None));
            self.last_heard.clear();
        }
        if !matches!(
            next,
            WorkerState::Candidate | WorkerState::LeaderReconciling | WorkerState::Leader
        ) && let Some(lease) = self.authority.as_mut()
        {
            lease.drop_fence();
        }
        self.state = next;
        self.outputs.push(Output::StateChanged(next));

        if self.drain_requested && matches!(next, WorkerState::Active | WorkerState::Leader) {
            self.drain_requested = false;
            self.drain();
        }
    }

    fn send(&mut self, to: WorkerId, payload: election_message::Payload) {
        self.outputs.push(Output::Send {
            to,
            message: ElectionMessage {
                payload: Some(payload),
            },
        });
    }

    fn publish(&mut self, payload: election_message::Payload) {
        self.outputs.push(Output::Publish {
            message: ElectionMessage {
                payload: Some(payload),
            },
        });
    }

    /// Processes a leader heartbeat acknowledgement (README §12.2
    /// `on_leader_ack`). Acks for another shard, an older recovery epoch, or
    /// a term of this node's own epoch below its floor (see
    /// [`Self::ack_floor`]) are ignored without touching any state.
    ///
    /// An ack from a later recovery epoch means the shard was recovered
    /// through the authority: the node adopts that epoch (see
    /// [`Self::adopt_recovery_epoch`]) and steps down from any term it holds
    /// or contests, whatever the terms, which two epochs do not order. A
    /// pending joiner that a stale JOIN pointer left on the old epoch finds
    /// its way the same way. An accepted ack
    /// refreshes leader contact, records its leader as [`Self::known_leader`]
    /// and the ack itself for this node's heartbeats to echo, and returns a
    /// node in `LeaderSuspect`, `RollCall` or `NoQuorum` to `Active` because
    /// a leader is reachable again, whatever term its roll call contests. A
    /// `Candidate` or `Leader` of a term earlier than the ack's steps down
    /// to `Active` under the ack's leader (ADR-0001 decision 14); one of the
    /// ack's own term keeps it.
    ///
    /// It also carries the leader's configuration and this node's admission
    /// generations in the leader's roster, which this node adopts as
    /// [`Self::adopt_configuration`] says. An ack that names no admission
    /// generation (the leader holds this node as pending, or not at all)
    /// leaves this node's own as they were. A node that adopts a newer
    /// configuration heartbeats its leader at once, so its echo of it
    /// reaches the leader without waiting out a heartbeat interval.
    fn on_leader_ack(&mut self, ack: &LeaderHeartbeatAck) {
        // A node back in `Bootstrapping` rejoins through JOIN alone: an ack
        // from a leader of the epoch it left would take it back past the
        // floor it rejoins at.
        if self.state == WorkerState::Bootstrapping
            || ack.shard_id() != self.shard_id
            || ack.recovery_epoch < self.recovery_epoch
        {
            return;
        }
        let later_epoch = ack.recovery_epoch > self.recovery_epoch;
        // Its own epoch number in another lineage is another shard's.
        let foreign = self
            .recovery_lineage
            .zip(ack.recovery_epoch_lineage)
            .is_some_and(|(own, acked)| own != acked);
        if !later_epoch && foreign {
            return;
        }
        if !later_epoch && ack.term < self.ack_floor() {
            return;
        }
        let outpaced = self
            .term_in_play()
            .is_some_and(|term| later_epoch || ack.term > term);
        if later_epoch {
            self.adopt_recovery_epoch(ack.recovery_epoch, ack.recovery_epoch_lineage, ack.term);
        }

        self.highest_term_seen = self.highest_term_seen.max(ack.term);
        let now = self.clock.now();
        self.last_leader_contact = now;
        // A token from a later instant than this node's own clock reads was
        // never sent by this node, and proves nothing.
        self.lease.acked(
            ack.heartbeat_token
                .filter(|token| *token <= now.as_ticks())
                .map(Instant::at),
        );
        self.leader = Some((ack.leader_id(), ack.term));
        self.newest_accepted_ack = Some(AckEcho {
            term: ack.term,
            send_token: ack.send_token,
        });
        let held = self.configuration.as_ref().map(Configuration::generation);
        self.adopt_configuration(
            ack.configuration(),
            ack.recipient_admission(),
            ack.recipient_prior_admission(),
        );
        if self.configuration.as_ref().map(Configuration::generation) != held {
            // Heartbeat at once: the echo of the new generation is what
            // commits it (see `Roster::commit_if_confirmed`).
            self.next_heartbeat = None;
        }

        if outpaced
            || matches!(
                self.state,
                WorkerState::LeaderSuspect | WorkerState::RollCall | WorkerState::NoQuorum
            )
        {
            self.round.stop();
            self.recovery = None;
            self.transition_to(WorkerState::Active);
        }
    }

    /// Adopts `offered`, a configuration a leader announced, when this node
    /// has none or `offered` is newer than its own, and with it `admission`
    /// and `prior_admission`, this node's admission generations there, when
    /// an admission is given. When this node already holds `offered`, it
    /// adopts only the admission generations, which repairs ones it missed.
    /// Admission generations offered with an older configuration than its
    /// own are ignored: they belong to a configuration this node has moved
    /// past, where they would make it no voter of its own.
    fn adopt_configuration(
        &mut self,
        offered: Configuration,
        admission: Option<Generation>,
        prior_admission: Option<Generation>,
    ) {
        let is_newer = self
            .configuration
            .as_ref()
            .is_none_or(|own| offered.generation() > own.generation());
        if !is_newer && self.configuration.as_ref() != Some(&offered) {
            return;
        }
        if let Some(admission) = admission {
            self.admission = Some(admission);
            self.prior_admission = prior_admission;
        }
        self.configuration = Some(offered);
    }

    /// Sends this node's leader a heartbeat (README §12.1) once a heartbeat
    /// interval has passed since the last one. A leader this node was not
    /// already heartbeating hears from it at once, so a leader it has just
    /// learned of does not wait a whole interval for its first heartbeat.
    fn heartbeat_leader_if_due(&mut self) {
        let Some(leader) = self.leader_to_heartbeat() else {
            self.next_heartbeat = None;
            return;
        };
        let now = self.clock.now();
        let due = match &self.next_heartbeat {
            Some((heartbeating, at)) => *heartbeating != leader || *at <= now,
            None => true,
        };
        if !due {
            return;
        }

        let heartbeat = WorkerHeartbeat {
            worker_id: Some(self.my_id.clone().into()),
            incarnation_id: Some(self.incarnation_id.clone().into()),
            recovery_epoch_seen: self.recovery_epoch,
            term_seen: self.highest_term_seen,
            // Nothing reports this node's capacity or running work yet.
            available_capacity: 0,
            active_task_runs_digest: Vec::new(),
            shard_id: Some(self.shard_id.clone().into()),
            newest_accepted_ack: self.newest_accepted_ack,
            configuration_generation: self
                .configuration
                .as_ref()
                .map(|configuration| configuration.generation().into()),
            send_token: now.as_ticks(),
        };
        self.send(
            leader.clone(),
            election_message::Payload::Heartbeat(heartbeat),
        );
        self.next_heartbeat = Some((leader, now + self.timings.heartbeat_interval));
    }

    /// The leader this node heartbeats: the one it records, while it is
    /// `Active`, `LeaderSuspect`, `RollCall` or `NoQuorum`, unless that is
    /// itself.
    fn leader_to_heartbeat(&self) -> Option<WorkerId> {
        if !matches!(
            self.state,
            WorkerState::Active
                | WorkerState::LeaderSuspect
                | WorkerState::RollCall
                | WorkerState::NoQuorum
        ) {
            return None;
        }
        let (leader, _) = self.leader.as_ref()?;
        (*leader != self.my_id).then(|| leader.clone())
    }

    /// Checks this node's timers against its clock.
    ///
    /// - `Active`: moves to `LeaderSuspect` once no leader ack has arrived
    ///   within its jittered suspicion timeout (README §12.2).
    /// - `LeaderSuspect` and `NoQuorum`: starts a roll call, if it can (see
    ///   [`Self::can_start_roll_call`]). Each transition takes its own
    ///   `Tick`, so a single one never goes from `Active` to `RollCall`.
    /// - `RollCall`: at its roll call's deadline, stands as the candidate or
    ///   goes `NoQuorum` (see [`ElectionRound::on_deadline`]).
    /// - `Candidate`: at its vote's deadline, not having won, suspects its
    ///   leader again (see [`Self::suspect_again`]).
    /// - `Leader`: reports every worker it has not heard from for too long
    ///   as lost (see [`Self::report_lost_workers`]). Moving to `NoQuorum`
    ///   once its quorum-contact lease has run out happens on every step,
    ///   before the input (see [`Self::step`]).
    ///
    /// Every other state is a no-op — deliberately so for `Bootstrapping` and
    /// `Joining` (README §27 Phase 2 bootstrap join): nothing times out a
    /// stalled join here, since the transition out of those states happens
    /// once via [`Self::finish_joining`], driven by something outside `core`
    /// that learns who leads the shard, not by a timer.
    fn tick(&mut self) {
        match self.state {
            WorkerState::Active => {
                if self.clock.now() - self.last_leader_contact > self.jittered_suspect_timeout() {
                    self.round.may_call_from(self.clock.now());
                    self.transition_to(WorkerState::LeaderSuspect);
                }
            }
            WorkerState::LeaderSuspect | WorkerState::NoQuorum => {
                if self.can_start_roll_call() {
                    let now = self.clock.now();
                    let timestamp_millis = self.clock.wall_clock_millis();
                    self.decide(|round, view| round.begin_roll_call(view, timestamp_millis, now));
                }
            }
            WorkerState::RollCall | WorkerState::Candidate => {
                let now = self.clock.now();
                self.decide(|round, view| round.on_deadline(view, now));
            }
            WorkerState::Leader => self.report_lost_workers(),
            _ => {}
        }
    }

    /// Records a connection to `peer`. A leader also acks any peer it has
    /// newly connected to, as it did every peer connected when it won (see
    /// [`Self::announce_leadership`]): one that missed that announcement,
    /// cut off at the win or joining since (a restarted process joins under
    /// a new `WorkerId`), would otherwise never learn whom to heartbeat.
    fn on_peer_connected(&mut self, peer: WorkerId) {
        let newly_connected = self.connected.insert(peer.clone());
        if newly_connected && self.state == WorkerState::Leader {
            self.send_ack(peer, None);
        }
    }

    /// Answers a follower's heartbeat (README §12.1) with one ack to its
    /// sender, and records which of this leader's acks the heartbeat
    /// confirms. Only a `Leader` answers, and only a heartbeat from its own
    /// shard at its own recovery epoch or an earlier one: a worker left on an
    /// earlier epoch adopts this leader's from the ack, but what it echoes
    /// confirms nothing here. Any heartbeat also tells the leader its sender
    /// is alive (see [`Self::report_lost_workers`]). A sender its roster does not hold is
    /// added as a pending joiner, so its acks name it pending, until a batch
    /// admits it (see [`Self::admit_waiting_joiners`]), which the ack
    /// answering this heartbeat already carries. Every sender gets an ack,
    /// though only members' confirmations count towards the lease, except
    /// one whose heartbeat names a later term of this leader's epoch: this
    /// leader steps down instead (ADR-0001 decision 14), and neither acks
    /// it nor records it as heard.
    ///
    /// A confirmation counts only for an ack of this leader's own term, sent
    /// no later than now: anything else names no ack this leader has sent in
    /// this term. A heartbeat that confirms one also says which
    /// configuration its sender holds, which counts toward committing a
    /// joint configuration this leader leads when it is exactly that one
    /// (see [`Roster::commit_if_confirmed`]). Granting a vote in a later term
    /// clears neither `leader` nor the newest accepted ack, so a worker that
    /// went on to vote there can still echo this leader's term-matching ack
    /// and help commit its configuration; safety rests on the exact-
    /// generation rule, not on that worker's vote. The term fence above only
    /// keeps echoes of other leaderships' acks from counting here.
    fn on_heartbeat(&mut self, from: WorkerId, heartbeat: &WorkerHeartbeat) {
        if self.state != WorkerState::Leader
            || heartbeat.shard_id() != self.shard_id
            || heartbeat.recovery_epoch_seen > self.recovery_epoch
        {
            return;
        }
        // Decision 14 on the heartbeat's own term: its sender voted, or
        // heard of a vote, in a later term. Some roll call of that term found
        // a returning quorum of stale voters, so this leader is all but
        // deposed, and the sender, whose floor is above this term, can never
        // follow it. A heartbeat from an earlier epoch names a term of
        // another count, and deposes no one.
        if heartbeat.recovery_epoch_seen == self.recovery_epoch
            && heartbeat.term_seen > self.term
        {
            self.highest_term_seen = self.highest_term_seen.max(heartbeat.term_seen);
            self.step_down_if_outpaced();
            return;
        }

        let now = self.clock.now();
        self.last_heard.insert(from.clone(), now);
        if heartbeat.recovery_epoch_seen == self.recovery_epoch
            && let Some(echo) = heartbeat.newest_accepted_ack
            && echo.term == self.term
            && echo.send_token <= now.as_ticks()
        {
            self.lease
                .confirm(from.clone(), Instant::at(echo.send_token));
            if let Some(held) = heartbeat.configuration_generation()
                && let Some(roster) = self.roster.as_mut()
            {
                roster.record_held_generation(&from, held);
            }
            self.commit_if_confirmed();
        }
        if let Some(roster) = self.roster.as_mut() {
            roster.add_pending(from.clone());
        }
        self.admit_waiting_joiners();
        // Only a leader that holds a grant vouches for when it heard the
        // sender: no rival can win until that grant ends (see
        // `Output::AbortDeadline`). Removals pending take effect first, as
        // they can end the grant.
        self.apply_pending_removals();
        let heartbeat_token = self
            .lease
            .holds_grant_at(self.office().as_ref(), &self.timings, now)
            .then_some(heartbeat.send_token);
        self.send_ack(from, heartbeat_token);
    }

    /// Sends `to` an ack naming this leader, its term, recovery epoch and
    /// configuration, and `to`'s admission generation in its roster (none
    /// for a pending joiner or a worker the roster does not hold), with the
    /// instant it is sent as the token a heartbeat echoes back, and, when it
    /// answers a heartbeat, that heartbeat's own token echoed. Removals
    /// pending take effect first (see [`Self::apply_pending_removals`]); a
    /// caller that vouches with `heartbeat_token` applies them before it
    /// decides whether it still holds a grant.
    fn send_ack(&mut self, to: WorkerId, heartbeat_token: Option<u64>) {
        self.apply_pending_removals();
        let Some(roster) = &self.roster else {
            return;
        };
        let ack = LeaderHeartbeatAck {
            shard_id: Some(self.shard_id.clone().into()),
            leader_id: Some(self.my_id.clone().into()),
            recovery_epoch: self.recovery_epoch,
            term: self.term,
            configuration: Some(roster.configuration().into()),
            recipient_admission: roster.admission_of(&to).map(Into::into),
            recipient_prior_admission: roster.prior_admission_of(&to).map(Into::into),
            send_token: self.clock.now().as_ticks(),
            heartbeat_token,
            recovery_epoch_lineage: self.recovery_lineage,
        };
        self.send(to, election_message::Payload::HeartbeatAck(ack));
    }

    /// Acks every connected peer once, unasked, so each learns that this
    /// node now leads. A follower heartbeats only a leader it knows of, and
    /// the election certificate does not name it one: it reaches only the
    /// roll call's respondents, and tells them what they founded. A peer
    /// outside this leader's roster, a voter that missed the winning roll
    /// call say, is acked too: its ack names no admission generation, so it
    /// keeps its own, older than the founded configuration's base, and is
    /// no voter of the new side (it can still be a voter of the old side,
    /// under the founded joint configuration, until the commit). A worker
    /// this misses hears from the leader once a
    /// connection to it opens (see [`Self::on_peer_connected`]).
    fn announce_leadership(&mut self) {
        for peer in self.connected.clone() {
            self.send_ack(peer, None);
        }
    }

    /// What the lease reads of this node while it leads: `None` unless it
    /// is `Leader`, and, with an authority, holds the recovery fence (design
    /// 4.5), whose end also ends its grant.
    fn office(&self) -> Option<Office<'_>> {
        if self.state != WorkerState::Leader {
            return None;
        }
        let fence_end = match &self.authority {
            None => LeaseEnd::Unbounded,
            Some(lease) => LeaseEnd::At(lease.fence_valid_until()?),
        };
        Some(Office {
            me: &self.my_id,
            roster: self.roster.as_ref()?,
            term: self.term,
            recovery_epoch: self.recovery_epoch,
            fence_end,
        })
    }

    /// Reports this node's grant and abort deadline where they differ from
    /// the ones last reported.
    fn report_lease_changes(&mut self) {
        let grant = self.lease.grant(self.office().as_ref(), &self.timings);
        let changes = self.lease.report(
            grant,
            &self.timings,
            self.lost_after(),
            self.clock.now(),
        );
        for change in changes {
            self.outputs.push(match change {
                LeaseChange::Grant(grant) => Output::Grant(grant),
                LeaseChange::AbortDeadline(deadline) => Output::AbortDeadline(deadline),
            });
        }
    }

    /// Whether a `LeaderSuspect` or `NoQuorum` node starts a roll call on
    /// its next `Tick`: once [`Self::next_roll_call_due`] has come.
    fn can_start_roll_call(&self) -> bool {
        self.next_roll_call_due()
            .is_some_and(|due| self.clock.now() >= due)
    }

    /// When a `LeaderSuspect` or `NoQuorum` node may start a roll call (see
    /// [`ElectionRound::roll_call_due`]).
    ///
    /// `None` for a node with no configuration: it has nothing to count a
    /// quorum against. It stays `LeaderSuspect`, still heartbeating its
    /// leader, until an ack from a leader returns it to `Active`.
    fn next_roll_call_due(&self) -> Option<Instant> {
        self.configuration
            .as_ref()
            .map(|_| self.round.roll_call_due(self.clock.now()))
    }

    /// The earliest term whose leader's acks this node accepts: the highest
    /// term it has seen, or, while it stands or leads, its own term if
    /// later. A candidate never follows an earlier term's leader while its
    /// candidacy can still win; once that lapses unwon, it can (ADR-0001
    /// decision 14 as amended 2026-09-28).
    fn ack_floor(&self) -> u64 {
        // A leader's own term is already its term seen (see
        // `Self::take_office`); the arm only keeps that from resting on it.
        match self.state {
            WorkerState::Candidate | WorkerState::Leader => self.highest_term_seen.max(self.term),
            _ => self.highest_term_seen,
        }
    }

    /// The term this node holds or contests: its roll call's while
    /// `RollCall`, its candidacy's while `Candidate` and its leadership's
    /// while `Leader`. `None` in every other state.
    fn term_in_play(&self) -> Option<u64> {
        match self.state {
            WorkerState::RollCall => self.round.roll_call_term(),
            WorkerState::Candidate | WorkerState::Leader => Some(self.term),
            _ => None,
        }
    }

    /// Steps down (ADR-0001 decision 14) once this node has seen a term
    /// later than the one it holds or contests: another worker is electing,
    /// or has elected, a leader for it. An ack from that term's leader
    /// returns the node to `Active` (see [`Self::on_leader_ack`]); anything
    /// else leaves it suspecting its leader again (see
    /// [`Self::suspect_again`]).
    fn step_down_if_outpaced(&mut self) {
        if self
            .term_in_play()
            .is_some_and(|term| self.highest_term_seen > term)
        {
            self.suspect_again();
        }
    }

    /// Gives up the roll call or candidacy this node runs, if any, and moves
    /// to `LeaderSuspect`, to start its next roll call only after a fresh
    /// suspicion timeout, jittered for the latest term it knows of: the
    /// wait gives the election that outpaced it time to finish, and the
    /// jitter keeps rivals that failed together from retrying together.
    fn suspect_again(&mut self) {
        self.retry_after_a_fresh_suspicion_timeout();
        self.transition_to(WorkerState::LeaderSuspect);
    }

    /// Moves to `NoQuorum` (ADR-0001 decision 13): the node's quorum is out
    /// of reach, so it waits for its peers to return. Every jittered
    /// suspicion timeout it tries a roll call again; meanwhile it answers
    /// the roll calls and grants the votes of others, and an ack from a
    /// leader returns it to `Active`. With an authority, the authority path
    /// can take it out too (see [`Self::begin_forced_recovery`]).
    /// A leader that has not heard from a quorum within its quorum-contact
    /// lease gives up leading, to `NoQuorum`.
    fn lose_quorum_if_its_lease_ended(&mut self) {
        if self.state != WorkerState::Leader {
            return;
        }
        let quorum_lost = |node: &Self| {
            node.roster.as_ref().is_some_and(|roster| {
                node.lease
                    .no_quorum_at(&node.my_id, roster, &node.timings)
                    .is_some_and(|at| node.clock.now() >= at)
            })
        };
        if quorum_lost(self) {
            // The departed no longer confirm anything: the configuration
            // without them may still have its quorum.
            self.apply_pending_removals();
            if quorum_lost(self) {
                self.lose_quorum();
            }
        }
    }

    fn lose_quorum(&mut self) {
        self.retry_after_a_fresh_suspicion_timeout();
        self.transition_to(WorkerState::NoQuorum);
    }

    /// Gives up the roll call, candidacy or recovery this node runs, if any,
    /// and puts its next roll call a fresh suspicion timeout away.
    fn retry_after_a_fresh_suspicion_timeout(&mut self) {
        let retry_at = self.clock.now() + self.jittered_suspect_timeout();
        self.round.retry_at(retry_at);
        self.recovery = None;
    }

    /// The latest term this node knows of: the highest it has seen, or that
    /// of the latest roll call it accepted, its own included, if later.
    fn latest_term(&self) -> u64 {
        self.round.latest_term(self.highest_term_seen)
    }

    /// Records the leader `pointer` names and drives `Bootstrapping ->
    /// Joining -> Active` (see [`Self::finish_joining`]).
    fn join(&mut self, pointer: &JoinResponse) {
        if !self.state.can_transition_to(WorkerState::Joining) {
            return;
        }
        let Some(leader_id) = pointer.leader_id() else {
            return;
        };
        // A leader of an epoch older than this node's own (one it rejoins
        // after a recovery without it), or of another lineage whatever its
        // number (one the authority lost), no longer leads the shard.
        let stale_lineage = self
            .recovery_lineage
            .is_some_and(|lineage| lineage != pointer.recovery_epoch_lineage);
        if pointer.recovery_epoch < self.recovery_epoch || stale_lineage {
            return;
        }

        self.transition_to(WorkerState::Joining);
        self.recovery_epoch = pointer.recovery_epoch;
        self.recovery_lineage = Some(pointer.recovery_epoch_lineage);
        self.highest_term_seen = self.highest_term_seen.max(pointer.term);
        self.leader = Some((leader_id, pointer.term));
        // A freshly joined node hasn't heard from its leader yet; start the
        // suspicion clock now so it isn't judged suspect the instant it
        // ticks — the same reasoning `Self::new`'s doc gives for a freshly
        // constructed node.
        self.last_leader_contact = self.clock.now();
        // Its registration starts with its membership: until now it kept
        // none, so its lease counts from the join.
        let now = self.clock.now();
        if let Some(lease) = self.authority.as_mut() {
            lease.restart_at(now);
        }
        self.transition_to(WorkerState::Active);
    }

    /// Dispatches an inbound message from `from` to its handler.
    ///
    /// Nodes that are `Draining`, `Stopped` or `Fenced` take no part in
    /// elections and drop everything. Every message is honoured only when
    /// its sender is the worker it names as sending it: the leader of a
    /// heartbeat ack, the follower of a heartbeat, the initiator of a roll
    /// call, the responder of a reply, the candidate of a vote request, the
    /// voter of a vote grant, the rejecter of a refusal, the departing
    /// worker of a self-remove and the leader of an election certificate.
    /// Anything else is dropped.
    fn on_message(&mut self, from: WorkerId, msg: ElectionMessage) {
        if matches!(
            self.state,
            WorkerState::Draining | WorkerState::Stopped | WorkerState::Fenced
        ) {
            return;
        }

        use election_message::Payload;
        match msg.payload {
            Some(Payload::Heartbeat(heartbeat)) if heartbeat.worker_id() == from => {
                self.on_heartbeat(from, &heartbeat);
            }
            Some(Payload::HeartbeatAck(ack)) if ack.leader_id() == from => {
                self.on_leader_ack(&ack);
            }
            Some(Payload::RollCall(call)) if call.initiator_id() == from => {
                let now = self.clock.now();
                self.decide(|round, view| round.on_roll_call(view, from, &call, now));
            }
            Some(Payload::RollCallReply(reply)) if reply.responder_id() == from => {
                self.decide(|round, view| round.on_roll_call_reply(view, from, &reply));
            }
            Some(Payload::VoteRequest(req)) if req.candidate_id() == from => {
                self.decide(|round, view| round.on_vote_request(view, from, &req));
            }
            Some(Payload::VoteGrant(grant)) if grant.voter_id() == from => {
                self.decide(|round, view| round.on_vote_grant(view, from, &grant));
            }
            Some(Payload::ElectionReject(reject)) if reject.rejecter_id() == from => {
                self.on_election_reject(&reject);
            }
            Some(Payload::SelfRemove(msg)) if msg.worker_id() == from => {
                self.on_self_remove(&from, &msg);
            }
            Some(Payload::ElectionCertificate(certificate)) if certificate.leader_id() == from => {
                self.on_election_certificate(&from, &certificate);
            }
            _ => {}
        }
    }

    /// Drains at once from `Active` or `Leader`. From a state that will
    /// reach one of them later it keeps the request for
    /// [`Self::transition_to`] to apply; a node already draining, or one
    /// that can never drain again, ignores it.
    fn request_drain(&mut self) {
        match self.state {
            WorkerState::Active | WorkerState::Leader => self.drain(),
            WorkerState::Draining | WorkerState::Stopped => {}
            _ => self.drain_requested = true,
        }
    }

    /// Gracefully shuts an `Active` or `Leader` node down (README §12.3,
    /// §18, ADR-0001 decision 10) and ends in `Stopped`.
    ///
    /// A follower sends `SelfRemove` to its leader alone, carrying the
    /// highest term it has seen, for that leader's term guard (see
    /// [`Self::on_self_remove`]).
    /// Followers learn of the removal from the generation the leader then
    /// announces. A node that knows no leader tells no one: the next
    /// founding leaves it out, or the authority path counts it out.
    ///
    /// A leader applies its own removal, which no other leader can (ADR-0001
    /// decision 10, amended 2026-09-27),
    /// and sends every connected peer a final ack announcing the
    /// configuration without it, together with every removal it has
    /// accepted and not yet applied, so the survivors elect under the
    /// shrunk one. A leader whose removal, with those, would leave no voter
    /// announces nothing (see [`Roster::remove_all`]).
    fn drain(&mut self) {
        // Leaving `Leader` withdraws the grant first, so no departure
        // message goes out while this node still holds one.
        let was_leader = self.state == WorkerState::Leader;
        self.transition_to(WorkerState::Draining);
        if was_leader {
            self.announce_own_departure();
        } else {
            self.tell_leader_of_departure();
        }

        // There is no outstanding work to wait for yet, so draining finishes
        // immediately. The transition table has no direct `Active -> Stopped`
        // edge, hence the two transitions.
        self.stop_reason = Some(StopReason::Drained);
        self.transition_to(WorkerState::Stopped);
    }

    /// The draining follower's half of [`Self::drain`]: one `SelfRemove`
    /// to the leader it follows, if it knows one other than itself.
    fn tell_leader_of_departure(&mut self) {
        let Some((leader, leader_term)) = self.leader.clone() else {
            return;
        };
        if leader == self.my_id {
            return;
        }
        let msg = SelfRemove {
            worker_id: Some(self.my_id.clone().into()),
            incarnation_id: Some(self.incarnation_id.clone().into()),
            shard_id: Some(self.shard_id.clone().into()),
            configuration_generation: self
                .configuration
                .as_ref()
                .map(|configuration| configuration.generation().into()),
            term_seen: self.highest_term_seen,
            leader_term,
        };
        self.send(leader, election_message::Payload::SelfRemove(msg));
    }

    /// The draining leader's half of [`Self::drain`]: takes itself out of
    /// its roster and, if that announced a change, acks every connected
    /// peer with it.
    fn announce_own_departure(&mut self) {
        let Some(roster) = self.roster.as_mut() else {
            return;
        };
        let before = roster.configuration().generation();
        self.pending_removals.insert(self.my_id.clone());
        self.apply_pending_removals();
        if self
            .roster
            .as_ref()
            .is_none_or(|roster| roster.configuration().generation() == before)
        {
            return;
        }
        for peer in self.connected.clone() {
            // Unasked, and sent after the grant is withdrawn: no heartbeat
            // token to vouch for.
            self.send_ack(peer, None);
        }
    }

    /// Accepts a departing worker's SELF_REMOVE (README §12.3, ADR-0001
    /// decision 10), to take it out of this leader's roster with every
    /// other one accepted since the last change, in one next generation and
    /// with no commit round (see [`Self::apply_pending_removals`]).
    ///
    /// It accepts only a removal addressed to this leadership: to this
    /// node, as the leader of this term. And the term guard (ADR-0001
    /// decision 10, amended 2026-09-27): it accepts the removal only if the
    /// worker has seen no term later than this leader's. A worker that has may
    /// have voted in an election of that later term, whose quorum was
    /// counted with it; shrinking N here as well could let two quorums of
    /// one term miss each other. Granting a vote raises the voter's highest
    /// term seen before the grant goes out, and a worker grants nothing
    /// once it has stopped, so the term it sends covers every vote it can
    /// have cast; a roll call it only answered counts for no quorum, so it
    /// need not (and does not) raise it. Such a worker stays in the roster
    /// until the next founding leaves it out, or the authority path counts
    /// it out.
    ///
    /// Only a `Leader` honours it: every other node ignores it, and a
    /// candidate keeps counting against its roll call's configuration.
    fn on_self_remove(&mut self, departing: &WorkerId, msg: &SelfRemove) {
        if self.state != WorkerState::Leader
            || msg.shard_id() != self.shard_id
            || msg.term_seen > self.term
            || msg.leader_term != self.term
        {
            return;
        }
        self.pending_removals.insert(departing.clone());
    }

    /// Takes every worker whose SELF_REMOVE this leader has accepted since
    /// its last change out of its roster together (ADR-0001 decision 10:
    /// every pending SELF_REMOVE in the next generation; see
    /// [`Roster::remove_all`]): a voter leaves a configuration shrunk at the
    /// next generation, re-based there with every remaining voter
    /// re-admitted, this leader included; during a founding or a batch the
    /// joint configuration is re-announced with shrunk counts. A pending
    /// joiner, or a member that is no voter, is only forgotten.
    ///
    /// It runs before anything the configuration changes or shows: before
    /// an ack announces it, a commit or a batch changes it, the leader's
    /// own drain, and before the leader would give up for want of a quorum
    /// of it. Until then the leader counts the departing workers as it did,
    /// their last confirmations included, which can hold its lease a little
    /// longer than the shrunk configuration's would. That is safe: a
    /// departed worker has stopped, grants no vote to anyone, and its leader
    /// contact was fresh when it confirmed (the TLA+ model keeps a stopped
    /// worker's confirmation until the lease runs past it). The same holds
    /// of a removal the leader simply announces late.
    fn apply_pending_removals(&mut self) {
        if self.pending_removals.is_empty() {
            return;
        }
        let departing = std::mem::take(&mut self.pending_removals);
        let Some(roster) = self.roster.as_mut() else {
            return;
        };
        roster.remove_all(&departing, self.term);
        self.take_on_roster_configuration();
    }

    /// Takes on, as this leader's own, the configuration its roster leads
    /// and its admission generations there.
    fn take_on_roster_configuration(&mut self) {
        let Some(roster) = &self.roster else {
            return;
        };
        self.configuration = Some(roster.configuration().clone());
        self.admission = roster.admission_of(&self.my_id);
        self.prior_admission = roster.prior_admission_of(&self.my_id);
    }

    /// Starts an admission batch (ADR-0001 decision 9, see
    /// [`Roster::begin_batch`]) of every worker waiting to join that has
    /// confirmed one of this leader's acks recently enough to leave it a
    /// lease worth having (see [`Lease::admissible`]): sent
    /// within the last two heartbeat intervals, or no earlier than the
    /// lease's quorum-contact time. A worker that drained after its
    /// last confirmation, its SELF_REMOVE not yet here, may be taken too: the
    /// same as one that is admitted and then drains, which the removal
    /// handles in turn. This leader itself, if its
    /// configuration does not count it, joins too. Nothing starts while its
    /// configuration is joint: joiners wait for the commit, which calls
    /// this again.
    fn admit_waiting_joiners(&mut self) {
        if self.state != WorkerState::Leader {
            return;
        }
        self.apply_pending_removals();
        let Some(roster) = self.roster.as_ref() else {
            return;
        };
        if roster.configuration().is_joint() {
            return;
        }
        // A joiner heartbeats every heartbeat interval, echoing the ack that
        // answered its previous heartbeat: two intervals cover that ack's
        // age, and network delays within one.
        let recent_since = Instant::at(
            self.clock
                .now()
                .as_ticks()
                .saturating_sub(2 * self.timings.heartbeat_interval.as_ticks()),
        );
        let mut waiting: BTreeSet<WorkerId> = self
            .lease
            .admissible(
                roster
                    .pending()
                    .iter()
                    .chain(roster.members().keys())
                    .filter(|worker| roster.is_admissible(worker)),
                &self.my_id,
                roster,
                &self.timings,
                recent_since,
            )
            .into_iter()
            .cloned()
            .collect();
        if roster.is_admissible(&self.my_id) {
            waiting.insert(self.my_id.clone());
        }
        let Some(roster) = self.roster.as_mut() else {
            return;
        };
        if roster.begin_batch(&waiting, self.term) {
            self.take_on_roster_configuration();
        }
    }

    /// Whether this node's state takes part in elections: only `Active`,
    /// `LeaderSuspect`, `RollCall` and `NoQuorum` answer roll calls and
    /// grant votes, a pending member among them.
    fn takes_part_in_elections(&self) -> bool {
        matches!(
            self.state,
            WorkerState::Active
                | WorkerState::LeaderSuspect
                | WorkerState::RollCall
                | WorkerState::NoQuorum
        )
    }

    /// Has this node's election round decide, through `decide`, with a view
    /// of this node as it is now, and carries out what it decided (see
    /// [`Self::apply`]).
    fn decide(&mut self, decide: impl FnOnce(&mut ElectionRound, &View<'_>) -> Vec<Verdict>) {
        let admission = self.counted_admission();
        let takes_part = self.takes_part_in_elections();
        let leader_contact_is_fresh = self.current_leader_still_valid();
        let view = View {
            me: &self.my_id,
            shard: &self.shard_id,
            recovery_epoch: self.recovery_epoch,
            highest_term_seen: self.highest_term_seen,
            configuration: self.configuration.as_ref(),
            admission,
            takes_part,
            leader_contact_is_fresh,
            roll_call_deadline: self.timings.roll_call_deadline,
        };
        let verdicts = decide(&mut self.round, &view);
        self.apply(verdicts);
    }

    /// Carries out, in order, what this node's election round decided:
    /// sends and publishes its messages, and moves this node's state, term
    /// and highest term seen as each verdict says.
    fn apply(&mut self, verdicts: Vec<Verdict>) {
        use election_message::Payload;
        for verdict in verdicts {
            match verdict {
                Verdict::Publish(call) => {
                    self.recovery = None;
                    self.transition_to(WorkerState::RollCall);
                    self.publish(Payload::RollCall(call));
                }
                Verdict::Answer { initiator, reply } => {
                    self.send(initiator, Payload::RollCallReply(reply));
                }
                Verdict::Reject {
                    to,
                    term,
                    reason,
                    name_leader,
                } => self.send_reject(to, term, reason, name_leader),
                Verdict::Stand { term } => {
                    self.transition_to(WorkerState::Candidate);
                    self.term = term;
                }
                Verdict::AskVotes { voters, request } => {
                    for voter in voters {
                        self.send(voter, Payload::VoteRequest(request.clone()));
                    }
                }
                Verdict::Grant { candidate, grant } => {
                    // Granting makes an initiator contesting an earlier term
                    // step down.
                    self.highest_term_seen = self.highest_term_seen.max(grant.term);
                    self.step_down_if_outpaced();
                    self.send(candidate, Payload::VoteGrant(grant));
                }
                Verdict::Certify {
                    respondent,
                    certificate,
                } => self.send(respondent, Payload::ElectionCertificate(certificate)),
                Verdict::Won { term, roster } => {
                    self.term = term;
                    self.take_office(roster);
                }
                Verdict::NoQuorum {
                    term,
                    configuration,
                    respondents,
                } => {
                    self.lose_quorum();
                    if self.authority.is_some() {
                        self.begin_forced_recovery(term, configuration, respondents);
                    }
                }
                Verdict::SuspectAgain => self.suspect_again(),
            }
            // The round's roll call and vote stand in for these states'
            // checks (see `ElectionRound`), so they must move together.
            debug_assert_eq!(
                self.round.roll_call_term().is_some(),
                self.state == WorkerState::RollCall,
                "a roll call runs exactly while the node is RollCall"
            );
            debug_assert!(
                !self.round.is_standing() || self.state == WorkerState::Candidate,
                "a vote runs only while the node is Candidate"
            );
        }
    }

    /// Refuses `initiator`'s roll call or vote request for `term`, naming
    /// this node's highest term seen, its configuration, and,
    /// with `name_leader`, the leader it follows, if any.
    fn send_reject(
        &mut self,
        initiator: WorkerId,
        term: u64,
        reason: ElectionRejectReason,
        name_leader: bool,
    ) {
        let leader = self
            .leader
            .as_ref()
            .filter(|_| name_leader)
            .map(|(leader, term)| KnownLeader {
                leader_id: Some(leader.clone().into()),
                term: *term,
            });
        let reject = ElectionReject {
            shard_id: Some(self.shard_id.clone().into()),
            term,
            initiator_id: Some(initiator.clone().into()),
            rejecter_id: Some(self.my_id.clone().into()),
            reason: reason as i32,
            // Not the vote floor the ballot refused against: a term this node
            // only stood in, unwon, would lock the initiator out of the
            // leader that outlasted it, as it once locked this node out.
            highest_term_seen: self.highest_term_seen,
            leader,
            configuration: self.configuration.as_ref().map(Into::into),
        };
        self.send(initiator, election_message::Payload::ElectionReject(reject));
    }

    /// How long this node, while `Active`, goes without an accepted leader
    /// ack before it suspects its leader (ADR-0001 decision 15):
    /// `suspect_timeout` lengthened by less than a half, by a share hashed
    /// from this node's `WorkerId` and the latest term it knows of. Workers
    /// thus rarely suspect at the same instant, and a worker waits a new
    /// time once the term has moved on. Leader stickiness and a leader's
    /// lease keep the unjittered `suspect_timeout`, the shortest this can
    /// be.
    fn jittered_suspect_timeout(&self) -> Duration {
        let share = self.hash_function.hash_to_u64(&[
            Field::Text(self.my_id.as_str()),
            Field::Number(self.latest_term()),
        ]);
        lengthen_by_less_than_half(self.timings.suspect_timeout, share)
    }

    fn current_leader_still_valid(&self) -> bool {
        self.clock.now() - self.last_leader_contact <= self.timings.suspect_timeout
    }

    /// Learns what a refusal of this node's roll call or vote request tells
    /// it (ADR-0001 decision 4): a higher term raises its highest term seen,
    /// and while `RollCall` a leader named at a term no older than that, or
    /// named as still valid at any term, becomes its leader, which it
    /// heartbeats until that leader's ack returns it to `Active` (or, if
    /// that leader's term is below its own term seen, until the heartbeat
    /// makes that leader step down). A refusal carrying the commit of the
    /// joint configuration this node holds hands it that commit (see
    /// [`Self::adopt_relayed_commit`]). A term later than the one it holds
    /// or contests makes it step down (see [`Self::step_down_if_outpaced`]).
    /// A refusal from a node at a later recovery epoch raises nothing (the
    /// two epochs' terms do not compare) but, while `RollCall`, makes the
    /// leader it names this node's, whose ack then moves it to that epoch.
    /// The refusal itself is not counted.
    fn on_election_reject(&mut self, reject: &ElectionReject) {
        if reject.shard_id() != self.shard_id || reject.initiator_id() != self.my_id {
            return;
        }
        let offered = reject.configuration();
        let later_epoch = offered
            .as_ref()
            .is_some_and(|offered| offered.generation().recovery_epoch() > self.recovery_epoch);
        if later_epoch {
            // Its terms are not this epoch's, so they neither raise this
            // node's nor outpace its roll call: the named leader's ack will.
            if self.state == WorkerState::RollCall
                && let Some(leader) = &reject.leader
                && leader.leader_id() != self.my_id
            {
                self.leader = Some((leader.leader_id(), leader.term));
            }
            return;
        }
        self.highest_term_seen = self.highest_term_seen.max(reject.highest_term_seen);
        // A leader named as still valid is heartbeated whatever its term:
        // if this node's term seen is above that leader's, its acks stay
        // ignored, but the heartbeat tells it of the later term, and it
        // steps down (see `Self::on_heartbeat`).
        if self.state == WorkerState::RollCall
            && let Some(leader) = &reject.leader
            && (leader.term >= self.highest_term_seen
                || reject.reason() == ElectionRejectReason::LeaderStillValid)
            && leader.leader_id() != self.my_id
        {
            self.leader = Some((leader.leader_id(), leader.term));
        }
        if let Some(offered) = offered {
            self.adopt_relayed_commit(offered);
        }
        self.step_down_if_outpaced();
    }

    /// Adopts `offered`, a refuser's configuration, when it is the commit of
    /// the joint configuration this node holds and this node is on that
    /// one's new side: the commit's ack never reached it, say because its
    /// leader stopped just after committing. It is admitted at the commit's
    /// generation, as that ack would have admitted it (see
    /// [`Configuration::admission_after_commit`]). Without this, a survivor
    /// holding the commit and one holding the joint configuration refuse
    /// each other's roll calls term after term (ADR-0001 decision 4 as
    /// amended 2026-09-28). Only a node that takes part in elections without
    /// standing or leading adopts it.
    fn adopt_relayed_commit(&mut self, offered: Configuration) {
        if !self.takes_part_in_elections() {
            return;
        }
        let Some(admission) = self
            .configuration
            .as_ref()
            .and_then(|own| own.admission_after_commit(&offered, self.admission))
        else {
            return;
        };
        self.configuration = Some(offered);
        self.admission = Some(admission);
        self.prior_admission = None;
    }

    /// Commits the joint configuration this leader leads once a majority of
    /// each side holds it (see [`Roster::commit_if_confirmed`]): it then
    /// leads the new side alone, which its acks carry from then on, with
    /// each member's admission generation there, its own included, and
    /// holds no prior admission generation any more. Joiners that waited
    /// out the change are then admitted in the next batch (see
    /// [`Self::admit_waiting_joiners`]).
    fn commit_if_confirmed(&mut self) {
        self.apply_pending_removals();
        let Some(roster) = self.roster.as_mut() else {
            return;
        };
        if roster.commit_if_confirmed(&self.my_id, self.term) {
            self.take_on_roster_configuration();
            self.admit_waiting_joiners();
        }
    }

    /// Accepts `leader`'s certificate of what its election's winner leads
    /// (ADR-0001 decision 8), sent to every respondent of its winning roll
    /// call: this node adopts that configuration and its admission
    /// generations there, as [`Self::adopt_configuration`] allows, and the
    /// certificate's term raises its highest term seen, which makes a node
    /// holding or contesting an earlier one step down (see
    /// [`Self::step_down_if_outpaced`]).
    ///
    /// It accepts a certificate for its own shard and recovery epoch that is
    /// from the leader it granted its vote in that term, or for a term no
    /// earlier than the highest it has seen: a respondent whose vote request
    /// never came granted nothing but is admitted too. A certificate for an
    /// earlier term from a leader it did not vote for is ignored, as is one
    /// reaching a node still joining, which answered no roll call.
    fn on_election_certificate(&mut self, leader: &WorkerId, certificate: &ElectionCertificate) {
        if matches!(
            self.state,
            WorkerState::Bootstrapping | WorkerState::Joining
        ) || certificate.shard_id() != self.shard_id
            || certificate.recovery_epoch != self.recovery_epoch
        {
            return;
        }
        let voted_for_it = self.round.granted_in(certificate.term) == Some(leader);
        if certificate.term < self.ack_floor() && !voted_for_it {
            return;
        }
        self.highest_term_seen = self.highest_term_seen.max(certificate.term);
        self.adopt_configuration(
            certificate.configuration(),
            certificate.recipient_admission(),
            certificate.recipient_prior_admission(),
        );
        self.step_down_if_outpaced();
    }

    /// Asks the authority for what this node's lease has come due for: a
    /// registration, from every state but `Bootstrapping`, `Joining`,
    /// `Draining` and `Stopped` (a fenced node keeps registering, to
    /// reconnect), and, while it needs one, its recovery fence.
    fn ask_authority_if_due(&mut self) {
        let now = self.clock.now();
        let registers = self.registers_with_authority();
        let epoch = self.own_recovery_epoch();
        let Some(lease) = self.authority.as_mut() else {
            return;
        };
        let register = registers && lease.registration_due(now);
        if register {
            lease.registration_asked(now);
        }
        // A node that has joined no shard has no epoch to hold a fence at,
        // and never needs one.
        let fence = epoch.filter(|_| lease.fence_due(now));
        if fence.is_some() {
            lease.fence_asked(now);
        }
        if register {
            self.ask_authority(AuthorityRequest::Register, now);
        }
        if let Some(recovery_epoch) = fence {
            self.ask_authority(AuthorityRequest::AcquireFence { recovery_epoch }, now);
        }
    }

    /// Whether this node keeps a registration with its authority in its
    /// current state.
    fn registers_with_authority(&self) -> bool {
        !matches!(
            self.state,
            WorkerState::Bootstrapping
                | WorkerState::Joining
                | WorkerState::Draining
                | WorkerState::Stopped
        )
    }

    /// Asks for `request` as the one read or swap this node now waits on
    /// (see `awaited_reply`).
    fn await_authority(&mut self, request: AuthorityRequest) {
        let now = self.clock.now();
        self.awaited_reply = Some(self.ask_authority(request, now));
    }

    /// Whether `token` is the one this node waits on, and if it is, stops
    /// waiting. Whole-token equality: a reply of another issuer, kind or
    /// number never empties the slot.
    fn take_awaited(&mut self, token: ReplyToken) -> bool {
        self.awaited_reply.take_if(|awaited| *awaited == token).is_some()
    }

    /// Asks for `request` at `now`, and returns the token its reply carries.
    fn ask_authority(&mut self, request: AuthorityRequest, now: Instant) -> ReplyToken {
        let call = AuthorityCall::new(request, &mut self.tokens, now);
        self.outputs.push(Output::Authority(call));
        call.token
    }

    /// Handles what the authority answered to a call this node asked for.
    fn on_authority_reply(&mut self, reply: AuthorityReply) {
        if self.authority.is_none() {
            return;
        }
        match reply {
            AuthorityReply::Registered {
                sent_at, result, ..
            } => {
                if let (Ok(granted), Some(lease)) = (result, self.authority.as_mut()) {
                    lease.registered(sent_at, granted);
                    // A fenced node that can register again reads the epoch
                    // to learn whether it may resume (ADR-0001 decision 12).
                    if self.state == WorkerState::Fenced {
                        self.await_authority(AuthorityRequest::ReadRecoveryEpoch);
                    }
                }
            }
            AuthorityReply::LiveRegistrations { token, result, .. } => {
                if self.take_awaited(token) {
                    self.on_live_registrations(result);
                }
            }
            AuthorityReply::RecoveryEpoch { token, result, .. } => {
                if !self.take_awaited(token) {
                    return;
                }
                if self.state == WorkerState::Fenced {
                    if let Ok(epoch) = result {
                        self.reconnect(epoch);
                    }
                } else {
                    self.on_recovery_epoch(result);
                }
            }
            AuthorityReply::RecoveryEpochSwapped {
                token,
                expected,
                new,
                result,
                ..
            } => {
                let awaited = self.take_awaited(token);
                self.on_recovery_epoch_swapped(expected, new, awaited, result);
            }
            AuthorityReply::Fence {
                recovery_epoch,
                sent_at,
                result,
                ..
            } => self.on_fence(recovery_epoch, sent_at, result),
        }
    }

    /// Fences this node (ADR-0001 decision 12) if its registration has
    /// lapsed, on its own clock, in a state that takes part in elections:
    /// it drops any roll call, vote or recovery it runs and any grant it
    /// holds, and asks its executor to abort its TaskRuns within the
    /// reconnect timeout, less drift. It keeps its configuration and
    /// admission generation. Returns whether it did.
    fn orphan_if_unregistered(&mut self) -> bool {
        let now = self.clock.now();
        let lapsed = self
            .authority
            .as_ref()
            .is_some_and(|lease| !lease.is_registered(now));
        let takes_part = matches!(
            self.state,
            WorkerState::Active
                | WorkerState::LeaderSuspect
                | WorkerState::RollCall
                | WorkerState::Candidate
                | WorkerState::Leader
                | WorkerState::NoQuorum
        );
        if !lapsed || !takes_part {
            return false;
        }
        self.round.stop();
        self.recovery = None;
        self.transition_to(WorkerState::Fenced);
        self.lease
            .orphaned(now, &self.timings, self.reconnect_timeout);
        true
    }

    /// A fenced node that can reach its authority again, which reports the
    /// shard's recovery epoch as `authority_epoch`, resumes, rejoins or stays
    /// fenced (see [`Reconnect`]). Resuming restarts its leader contact, so it
    /// does not at once suspect a leader it could not hear from while fenced.
    /// Rejoining discards everything it knew of the shard, takes the
    /// authority's epoch and returns it to `Bootstrapping`, for its driver to
    /// join it again.
    fn reconnect(&mut self, authority_epoch: Option<RecoveryEpoch>) {
        let now = self.clock.now();
        if !self
            .authority
            .as_ref()
            .is_some_and(|lease| lease.is_registered(now))
        {
            return;
        }
        match Reconnect::decide(self.own_recovery_epoch(), authority_epoch) {
            Reconnect::Resume => {
                self.lease.resumed();
                self.last_leader_contact = now;
                self.transition_to(WorkerState::Active);
            }
            Reconnect::Rejoin(epoch) => self.rejoin_at(epoch),
            Reconnect::StayFenced => {}
        }
    }

    /// Forgets the configuration, admissions and election state this node
    /// held under its recovery epoch, as it leaves that epoch behind.
    fn forget_shard(&mut self) {
        self.configuration = None;
        self.admission = None;
        self.prior_admission = None;
        self.newest_accepted_ack = None;
        self.round.forget();
        self.recovery = None;
        self.roster = None;
    }

    /// Adopts `epoch`, a later recovery epoch than this node's, whose
    /// leader, elected in `term`, has acked it: the shard was recovered
    /// through the authority. Terms of the two epochs are not comparable,
    /// so the node takes `term` as the highest it has seen and starts a
    /// fresh ballot, and it forgets what it held at the old epoch; the ack
    /// then gives it the new configuration and its admission there. The
    /// node takes the epoch's `lineage` with it when the ack names one: the
    /// epoch is usually of its own lineage, as a recovery keeps it, but one
    /// recovered from a shard founded afresh is not, and a node that kept
    /// its old lineage would not recognise the epoch as its own when it
    /// next reconnected, and would rejoin rather than resume.
    fn adopt_recovery_epoch(&mut self, epoch: u64, lineage: Option<u64>, term: u64) {
        self.forget_shard();
        self.recovery_epoch = epoch;
        if lineage.is_some() {
            self.recovery_lineage = lineage;
        }
        self.highest_term_seen = term;
    }

    /// Takes the authority path once this node's roll call for `term` under
    /// `configuration` has fallen short of its returning quorum, with these
    /// `respondents` (see the `forced_recovery` module): asks for the
    /// shard's live registrations. The node is already `NoQuorum`.
    fn begin_forced_recovery(
        &mut self,
        term: u64,
        configuration: Configuration,
        respondents: BTreeMap<WorkerId, Admission>,
    ) {
        self.recovery = Some(ForcedRecovery::start(term, configuration, respondents));
        self.await_authority(AuthorityRequest::ReadLiveRegistrations);
    }

    /// Carries a recovery on as `next` says.
    fn follow_recovery(&mut self, next: Next) {
        match next {
            Next::ReadEpoch => self.await_authority(AuthorityRequest::ReadRecoveryEpoch),
            Next::Swap { from, to } => self.await_authority(AuthorityRequest::SwapRecoveryEpoch {
                expected: Some(from),
                new: to,
            }),
            Next::AwaitFence { epoch } => self.stand_through_authority(epoch),
            Next::Abandon => self.abandon_shard(),
            Next::Rejoin(epoch) => self.rejoin_at(epoch),
            Next::GiveUp => self.recovery = None,
        }
    }

    /// Leaves the shard this node held for the one the authority holds at
    /// `epoch`, which it cannot resume or recover into: discards everything
    /// it knew of its own and goes back to `Bootstrapping`, for its driver to
    /// join it again (ADR-0001 decision 12). The epoch it rejoins is a
    /// floor: a JOIN pointer to a leader left on an older one, or on another
    /// lineage, must not take it back there.
    fn rejoin_at(&mut self, epoch: RecoveryEpoch) {
        self.forget_shard();
        self.awaited_reply = None;
        self.recovery_epoch = epoch.number;
        self.recovery_lineage = Some(epoch.lineage);
        self.highest_term_seen = 0;
        self.leader = None;
        self.transition_to(WorkerState::Bootstrapping);
    }

    fn on_live_registrations(&mut self, result: Result<LiveRegistrations, AuthorityError>) {
        if self.state != WorkerState::NoQuorum {
            return;
        }
        let Some(recovery) = self.recovery.as_mut() else {
            return;
        };
        let next = match result {
            // A node the authority does not list as live has no standing
            // to recover the shard on the authority's count.
            Ok(live) if live.addresses().contains_key(&self.my_id) => {
                recovery.on_live_registrations(&live)
            }
            _ => Next::GiveUp,
        };
        self.follow_recovery(next);
    }

    fn on_recovery_epoch(&mut self, result: Result<Option<RecoveryEpoch>, AuthorityError>) {
        if self.state != WorkerState::NoQuorum {
            return;
        }
        let own_epoch = self.own_recovery_epoch();
        let Some(recovery) = self.recovery.as_mut() else {
            return;
        };
        let next = match result {
            Ok(epoch) => recovery.on_recovery_epoch(epoch, own_epoch),
            Err(_) => Next::GiveUp,
        };
        self.follow_recovery(next);
    }

    /// A compare-and-swap of the recovery epoch came back: either this
    /// node's authority path, or a leader republishing its epoch after the
    /// authority lost it (README §15.3), which then asks for its fence
    /// again at once.
    fn on_recovery_epoch_swapped(
        &mut self,
        expected: Option<RecoveryEpoch>,
        new: RecoveryEpoch,
        awaited: bool,
        result: Result<(), AuthorityError>,
    ) {
        if self.state == WorkerState::NoQuorum
            && let Some(recovery) = self.recovery.as_mut()
        {
            if !awaited {
                return;
            }
            let next = recovery.on_swapped(expected, new, result.is_ok());
            self.follow_recovery(next);
            return;
        }
        // The leader's republish (README §15.3): once the epoch is back, by
        // this swap or another worker's, it asks for its fence at once.
        // Otherwise it asks when the fence is next due, so an authority that
        // keeps failing is not asked again within the same instant.
        let republished = match &result {
            Ok(()) => true,
            Err(AuthorityError::EpochConflict { current }) => *current == Some(new),
            Err(_) => false,
        };
        if self.state == WorkerState::Leader
            && expected.is_none()
            && Some(new) == self.own_recovery_epoch()
            && republished
        {
            let now = self.clock.now();
            if let Some(lease) = self.authority.as_mut() {
                lease.retry_fence_at(now);
            }
        }
    }

    /// The authority path swapped the recovery epoch to `epoch`: this node
    /// adopts it, and the configuration its recovery founds there, stands
    /// as `Candidate` for its roll call's term, and asks for the fence, which
    /// it must hold before it leads (ADR-0001 decision 11.4).
    fn stand_through_authority(&mut self, epoch: RecoveryEpoch) {
        let Some(recovery) = self.recovery.take() else {
            return;
        };
        self.newest_accepted_ack = None;
        self.recovery_epoch = epoch.number;
        self.recovery_lineage = Some(epoch.lineage);
        self.term = recovery.term();
        self.highest_term_seen = self.highest_term_seen.max(recovery.term());
        if let Some(roster) = recovery.founded_roster() {
            self.configuration = Some(roster.configuration().clone());
            self.admission = roster.admission_of(&self.my_id);
            self.prior_admission = None;
        }
        self.recovery = Some(recovery);
        self.transition_to(WorkerState::Candidate);
        let now = self.clock.now();
        if let Some(lease) = self.authority.as_mut() {
            lease.need_fence(now);
        }
    }

    /// The authority path found the recovery epoch missing: the shard is
    /// abandoned (ADR-0001 decision 11.5), and this node stops for good,
    /// raising an alert. A restart re-enters the bootstrap cascade.
    fn abandon_shard(&mut self) {
        self.recovery = None;
        self.stop_reason = Some(StopReason::Abandoned);
        self.transition_to(WorkerState::Stopped);
        self.outputs.push(Output::ShardAbandoned);
    }

    /// Handles the authority's answer to this node's request for the
    /// recovery fence at `epoch`, asked at `sent_at`. Only a `Leader`, or a
    /// `Candidate` waiting out the fence after its authority path, holds or
    /// seeks one; an answer for another epoch than its own is stale.
    ///
    /// - Granted: the fence lets it act until a TTL, less drift, after it
    ///   asked. A waiting candidate now leads (see [`Self::lead_recovered`]).
    /// - Held by another worker: it asks again once that fence has run out.
    /// - The epoch is missing (the authority lost its data): a leader
    ///   republishes it (README §15.3) and asks again; a waiting candidate
    ///   gives up, its swap lost with the data.
    /// - The epoch has moved on: the shard was recovered without it. A
    ///   leader steps down, and a waiting candidate gives up; either
    ///   adopts the new epoch from its leader's ack.
    /// - Unavailable: it asks again at its next renewal.
    fn on_fence(
        &mut self,
        epoch: RecoveryEpoch,
        sent_at: Instant,
        result: Result<Duration, AuthorityError>,
    ) {
        let seeking = match self.state {
            WorkerState::Leader => true,
            WorkerState::Candidate => self
                .recovery
                .as_ref()
                .is_some_and(ForcedRecovery::is_awaiting_fence),
            _ => false,
        };
        if !seeking || Some(epoch) != self.own_recovery_epoch() {
            return;
        }
        let now = self.clock.now();
        match result {
            Ok(granted) => {
                if let Some(lease) = self.authority.as_mut() {
                    lease.fence_acquired(sent_at, granted);
                }
                if self.state == WorkerState::Candidate {
                    self.lead_recovered();
                }
            }
            Err(AuthorityError::FenceHeld { remaining }) => {
                if let Some(lease) = self.authority.as_mut() {
                    lease.retry_fence_at(now + remaining + Duration::from_ticks(1));
                }
            }
            Err(AuthorityError::EpochConflict { current: None })
                if self.state == WorkerState::Leader =>
            {
                self.ask_authority(
                    AuthorityRequest::SwapRecoveryEpoch {
                        expected: None,
                        new: epoch,
                    },
                    now,
                );
            }
            // An epoch this node cannot recover from: it rejoins the shard
            // at it, as a reconnecting fenced node and a `NoQuorum` node's
            // recovery do, rather than win again and meet it again.
            Err(AuthorityError::EpochConflict {
                current: Some(held),
            }) if cannot_recover_from(self.own_recovery_epoch(), held) => {
                self.lose_quorum();
                self.rejoin_at(held);
            }
            Err(AuthorityError::EpochConflict { .. }) => {
                if self.state == WorkerState::Leader {
                    self.suspect_again();
                } else {
                    self.lose_quorum();
                }
            }
            Err(AuthorityError::Unavailable) => {}
        }
    }

    /// Leads the configuration this node's authority path founded, now that
    /// it holds the fence: becomes `Leader` of a roster of its counted
    /// respondents, each admitted at the founded generation, and acks every
    /// connected peer, which adopts the new epoch from that ack.
    fn lead_recovered(&mut self) {
        let Some(roster) = self
            .recovery
            .take()
            .and_then(|recovery| recovery.founded_roster())
        else {
            return;
        };
        self.take_office(roster);
    }

    /// Becomes `Leader` of `roster` in this node's current term: holds its
    /// configuration and its own admission generations there, commits it
    /// at once if it alone is a majority of each side, starts its quorum-
    /// contact lease and its watch over the workers it leads, records itself
    /// as its own leader in place of any it followed before, and announces
    /// itself to every connected peer (see [`Self::announce_leadership`]),
    /// unless a drain kept from before stops it first. With an authority it
    /// needs a fence to act; one it already holds is kept. Leader
    /// reconciliation (README §13) needs task data that does not exist yet,
    /// so `LeaderReconciling` is passed through immediately.
    fn take_office(&mut self, roster: Roster) {
        let now = self.clock.now();
        // A leader never acks itself, so nothing else raises its own
        // `highest_term_seen` to the term it won.
        self.highest_term_seen = self.highest_term_seen.max(self.term);
        self.pending_removals.clear();
        self.configuration = Some(roster.configuration().clone());
        self.admission = roster.admission_of(&self.my_id);
        self.prior_admission = roster.prior_admission_of(&self.my_id);
        self.last_heard = roster
            .members()
            .keys()
            .chain(roster.pending())
            .filter(|worker| **worker != self.my_id)
            .map(|worker| (worker.clone(), now))
            .collect();
        self.roster = Some(roster);
        self.commit_if_confirmed();

        self.lease.won(now);
        if let Some(lease) = self.authority.as_mut()
            && lease.fence_valid_until().is_none()
        {
            lease.need_fence(now);
        }
        // The leader it followed before is replaced. Should this node lose
        // its quorum and go back to electing, it must not heartbeat that
        // leader again.
        self.leader = Some((self.my_id.clone(), self.term));
        self.newest_accepted_ack = None;
        self.next_heartbeat = None;
        self.transition_to(WorkerState::LeaderReconciling);
        self.transition_to(WorkerState::Leader);
        if self.state == WorkerState::Leader {
            self.announce_leadership();
        }
    }

    /// When the leader next has a worker to report lost: a suspicion timeout
    /// and a reconnect timeout after it last heard from the one it heard
    /// from longest ago.
    fn next_worker_lost_at(&self) -> Option<Instant> {
        self.last_heard
            .values()
            .min()
            .map(|heard| *heard + self.lost_after())
    }

    /// How long a leader goes without hearing from a worker before it
    /// reports that worker lost: a suspicion timeout and then a reconnect
    /// timeout. The worker's own abort deadline counts from the same span
    /// (see [`Lease::report`]), so it aborts before any leader replays it.
    fn lost_after(&self) -> Duration {
        Duration::from_ticks(
            self.timings
                .suspect_timeout
                .as_ticks()
                .saturating_add(self.reconnect_timeout.as_ticks()),
        )
    }

    /// Reports every worker this leader has not heard from for a suspicion
    /// timeout and a reconnect timeout as lost (README §8.3), once each.
    fn report_lost_workers(&mut self) {
        let now = self.clock.now();
        let lost_after = self.lost_after();
        let lost: Vec<WorkerId> = self
            .last_heard
            .iter()
            .filter(|(_, heard)| now >= **heard + lost_after)
            .map(|(worker, _)| worker.clone())
            .collect();
        for worker in lost {
            self.last_heard.remove(&worker);
            self.outputs.push(Output::WorkerLost(worker));
        }
    }
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

/// The earlier of two optional instants; `None` only when both are.
fn earliest(a: Option<Instant>, b: Option<Instant>) -> Option<Instant> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

/// `timeout` × (1 + u/2), rounded down to a whole tick, where u is `share`
/// read as a fraction of 2^64, so 0 ≤ u < 1.
fn lengthen_by_less_than_half(timeout: Duration, share: u64) -> Duration {
    let ticks = timeout.as_ticks();
    // ticks × share / 2^65 is less than half of `ticks`, so it fits a u64.
    let extra = ((u128::from(ticks) * u128::from(share)) >> 65) as u64;
    Duration::from_ticks(ticks.saturating_add(extra))
}
