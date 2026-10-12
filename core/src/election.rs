//! The worker-side election state machine: leader
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
//! Liveness comes from heartbeats alone,
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
//! configuration: a joint one, whose new side has one
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
//! A leader also changes its configuration while it lives. It admits pending
//! joiners in batches, one change at a time: a joint configuration at its next
//! generation, whose new side takes in each joiner that has confirmed one of
//! its acks, committed like a founding. It applies a departing worker's
//! `SelfRemove`, which a follower sends to its leader alone, at once and with
//! no commit round, unless the worker has seen a later term than the leader's
//! (the term guard). A draining leader announces its own departure on final
//! acks. A draining node also reports who is to choose where the Task records
//! it holds go (see [`Output::HandOff`]), and a leader reads, from its roster
//! alone, the voters it places records on: those it has not reported lost
//! and not heard since (see [`WorkerNode::placeable_voters`]).
//!
//! A winner holds office from its win, in `LeaderReconciling`, and performs
//! every election duty of a leader there: it announces itself, acks
//! heartbeats, keeps its lease and watches the workers it leads. Only its
//! scheduler's grant waits: the node asks its driver to reconcile (see
//! [`Output::Reconcile`]) and moves to `Leader`, and reports a grant, once
//! handed [`Input::Reconciled`] for the same office; one for any other office
//! changes nothing. The roster says whom it asks ([`WorkerNode::reconcilees`]:
//! its voters and pending members) and how much an answer counts
//! ([`WorkerNode::voters_answered`]: only voters, and a worker the roster does
//! not hold counts for nothing). A worker it reports lost while it reconciles
//! is reported like any other, for the scheduler to keep until it leads. It
//! leaves office from either state by the same edges, and then reports no grant.
//!
//! A node that holds or contests a term steps down once it sees a later one:
//! to `Active` under that term's leader when its ack
//! is what told it, and otherwise to `LeaderSuspect`. Only a vote granted, an
//! accepted ack, a refusal or an accepted election certificate raises the
//! highest term a node has seen, never a roll call it answers, so a follower
//! with a flaky link cannot depose a healthy leader by calling a roll.
//!
//! A node configured with a coordination authority also keeps its registration
//! there, through calls it asks its driver to make (see the `authority`
//! module): it fences itself once it has failed to renew for a TTL less drift,
//! and on reconnecting resumes if the shard's recovery epoch there is still
//! its own, lineage included (see `RecoveryEpoch`), and otherwise rejoins. Its
//! leader acts only while it holds the recovery fence, so its grant ends at
//! the earlier of the fence and the quorum-contact lease. A roll call of its
//! own that falls short of its returning quorum takes the authority path (see
//! the `authority_standing` module): with a majority of the authority's live
//! registrations among its respondents it swaps the recovery epoch, waits out
//! the fence, and leads a configuration founded at the new epoch; if the epoch
//! is missing, the shard is abandoned and the node stops. A node that hears a
//! leader of a later recovery epoch adopts that epoch, and that leader's
//! configuration, from its ack. A member with an authority stands for
//! election only at the epoch the authority holds: once it suspects its
//! leader it reads the authority's epoch, stands while that read names its own
//! (or the authority holds none), and otherwise rejoins at the epoch the read
//! names (see the `authority_standing` module). A leader also reports each
//! worker it has not heard from for a suspicion timeout as silent, which
//! times the loss of each run it holds, and after a reconnect timeout more as
//! lost.
//!
//! Known gaps:
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
mod authority_standing;
mod cancelled_runs;
mod carry_out;
mod drain;
mod election_round;
mod entry;
mod leader_office;
mod lease;
mod node;
mod self_report;
mod standing;

use std::collections::{BTreeMap, BTreeSet};

pub use authority::{
    AuthorityCall, AuthorityReply, AuthorityRequest, AuthorityTimings, CallKind, Issuer,
    ReplyToken, ReplyTokens,
};
pub use carry_out::{AuthorityPerformer, DropMessages, MessageSink, NoAuthority, carry_out};
pub use entry::{Entry, Identity};
pub use lease::AbortBy;

use crate::configuration::{Admission, Configuration, Generation};
use crate::coordination_authority::{RecoveryEpoch, ShardRecord};
use crate::hashing::{Field, HashFunction};
use crate::protocol::checked::{Checked, CheckedMessage};
use crate::protocol::ids::{IdGenerator, IncarnationId, ShardId, TaskRunId, WorkerId};
use crate::protocol::messages::prelude::*;
use crate::protocol::messages::{
    AckEcho, ElectionCertificate, ElectionMessage, ElectionReject, ElectionRejectReason,
    JoinResponse, KnownLeader, RollCall, SelfRemove, VoteGrant, election_message,
};
use crate::protocol::worker_state::WorkerState;
use crate::reconcile::ReconcileTerm;
use crate::scheduler::{LeadershipGrant, Observer, Scheduler};
use crate::time::{Clock, Duration, Instant};

use authority_standing::{AuthorityStanding, AuthorityVerdict, AuthorityView};
use cancelled_runs::CancelledRunsLedger;
use drain::{Asked, DrainRequest};
use election_round::{ElectionRound, Verdict, View};
use leader_office::{Departure, LeaderOffice};
use lease::Lease;
use self_report::SelfReport;
pub use standing::JoinFloor;
use standing::ShardStanding;
pub(crate) use standing::EpochOrder;

pub struct WorkerNode<C>
where
    C: Clock,
{
    my_id: WorkerId,
    incarnation_id: IncarnationId,
    shard_id: ShardId,
    state: WorkerState,
    /// What this node knows of its shard: its recovery epoch, the highest
    /// term it has seen, and the configuration and admissions it follows.
    standing: ShardStanding,
    last_leader_contact: Instant,
    timings: ElectionTimings,
    clock: C,
    /// What the jitter on this node's suspicion timeout is derived from.
    hash_function: HashFunction,
    /// This node's standing with its coordination authority; `None` for a
    /// node with no authority configured.
    authority: Option<AuthorityStanding>,
    /// Why this node stopped; `None` until it is `Stopped`.
    stop_reason: Option<StopReason>,
    /// The term this node is contesting or holds. Meaningful from `Candidate`
    /// onward.
    term: u64,
    /// The roll calls this node answered, the votes it granted, and the
    /// roll call or candidacy it runs.
    round: ElectionRound,
    /// What this node holds only while `Leader`: the roster it leads, the
    /// removals it has accepted and when it last heard from each worker.
    /// While it is held it, not `standing`, holds the configuration and
    /// admissions this node leads (see [`Self::led_or_followed_configuration`]).
    office: Option<LeaderOffice>,
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
    /// A drain asked for and not yet carried out: kept until the node can
    /// drain, or, while it leads, waiting for its voters' routing crawls.
    drain_request: DrainRequest,
    /// The admission generation this node held when its last routing crawl
    /// completed.
    crawled_at_admission: Option<Generation>,
    /// What this worker's heartbeats say of it.
    report: SelfReport,
    /// While it holds office: per worker, the runs that worker holds which
    /// this leader cancelled and stored, to list in its acks to it.
    cancelled_runs: CancelledRunsLedger,
    /// What a node bootstrapping again holds while it checks a JOIN pointer
    /// against the authority.
    rejoin: RejoinCheck,
    /// What the step in progress has produced so far.
    outputs: Vec<Output>,
}

/// The state a rejoining node keeps beside its floor: the authority read it
/// awaits, and the pointer it holds until a read validates it.
#[derive(Debug, Default)]
struct RejoinCheck {
    /// The latest read of the authority's epoch the driver told this node it
    /// asked, and has not been answered; an answer to any other is dropped.
    awaited_read: Option<ReplyToken>,
    /// The JOIN pointer taken, held while the node is `Joining`.
    held: Option<JoinResponse>,
}

/// The timers a [`WorkerNode`] runs its election on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ElectionTimings {
    /// The shortest time a node goes without an accepted leader ack before it
    /// suspects its leader. Each node waits longer, by less than half of this,
    /// by a share hashed from its `WorkerId` and the latest term it knows of,
    /// so workers rarely suspect at the same instant. A leader's lease lasts
    /// at most its own `suspect_timeout` less the drift margin (see
    /// [`Self::clock_drift_divisor`]), which is safe only if no follower
    /// suspects it sooner: every worker in the shard must use the same value,
    /// or at least no follower a shorter one than its leader. A node also
    /// refuses roll calls and votes for exactly this long after it last heard
    /// from its leader (or, before it has one, after it was built or joined).
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
    /// How long a roll call runs before its initiator decides on it: it stands
    /// as the candidate if the voters among its respondents are a quorum by
    /// then, and goes `NoQuorum` otherwise. A candidate then has as long again
    /// to win its vote. This is the base: a reply that misses its call's
    /// deadline never counts, and each retry is a new term, so a deadline
    /// shorter than the shard's round trip would fail forever. Each of a
    /// node's own roll calls in a row that closes `NoQuorum` therefore doubles
    /// the next one's deadline (and its vote's), up to `suspect_timeout`;
    /// winning an election, accepting a leader's ack or leaving the recovery
    /// epoch resets it to this base. A node that answered another worker's
    /// roll call starts none of its own until that call resolves: until the
    /// leader's ack of that term or a later one is accepted, or, if none
    /// comes, until twice this base plus `suspect_timeout` after it answered
    /// (the call's census and vote, and the time for its caller to go quiet).
    /// A caller whose calls have widened can outlast that, and be contested
    /// early, as can a caller answered late in another caller's episode,
    /// which costs extra calls, never safety. Further roll calls the
    /// node answers while that hold lasts, from the same initiator or
    /// another, do not extend it past twice that span from the first
    /// answer (the hold episode); a later call of the same initiator does
    /// not extend it even within that. So one initiator that keeps failing
    /// to find a quorum costs an answerer at most one span, and several
    /// taking turns at most two, after which the node may call (calls of
    /// others still go on). A hold that ran out short of that cap ends the
    /// episode, and the next call the node answers starts a new one; one the
    /// cap ended leaves the node owed a call, and until it starts one of its
    /// own or follows a leader, calls it answers hold it no more. Keep it
    /// well above the time a roll call takes to reach the shard and its
    /// replies to come back, and below `suspect_timeout`; backoff only
    /// rescues a deadline set too low, at the cost of failed calls first.
    /// Usually [`Self::DEFAULT_ROLL_CALL_DEADLINE`]. Must not be zero (see
    /// [`WorkerNode::start`]).
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
    /// ack can suspect it. The same share comes off a
    /// registration or fence TTL (the node gives up before the authority
    /// does), off the time a worker takes to fence itself and abort its
    /// runs, and off the time before which no leader
    /// can replay a cut-off worker's runs (see [`Output::AbortDeadline`]).
    /// Lower it on hosts whose clock rates can differ more. Every worker in
    /// the shard must use the same value. Must not be zero.
    pub clock_drift_divisor: u64,
    /// The shard's default reconnect timeout. For a TaskRun of a task
    /// submitted without one of its own, it is how long a leader waits,
    /// after the run's worker has gone a suspicion timeout unheard, before
    /// the run is lost and may be replayed. Less drift, it is how long a
    /// worker cut off from its leader, or fenced, has to abort the run. A
    /// task with its own carries it in its Task record, so every leader and
    /// its worker time that run alike. It is also how long, past that
    /// suspicion timeout, a leader waits before it reports a silent worker
    /// lost (see [`Output::WorkerLost`]). Every worker in the shard must use
    /// the same value, or a leader could replay a run of the default timeout
    /// while its worker still runs it. Usually
    /// [`Self::DEFAULT_RECONNECT_TIMEOUT`].
    pub reconnect_timeout: Duration,
    /// The longest a leader asked to drain keeps leading while it waits for
    /// every other voter to report a routing crawl since its admission (see
    /// `WorkerHeartbeat::routing_crawled`). Leaving earlier can strand
    /// workers that know no one but this leader; past this it leaves anyway,
    /// so a voter that never reports cannot hold a shutdown up for ever.
    /// Usually [`Self::DEFAULT_DRAIN_WAIT_SUSPICIONS`] suspicion timeouts.
    pub drain_wait_limit: Duration,
}

impl ElectionTimings {
    /// The default `drain_wait_limit`, in suspicion timeouts: as many as
    /// separate a worker's unprompted routing crawls, so every voter crawls
    /// at least once within it.
    pub const DEFAULT_DRAIN_WAIT_SUSPICIONS: u64 = 10;

    /// The default `roll_call_deadline`, set from the census latency
    /// measured during the networked-election work: over 30 leader losses in
    /// a fully connected shard of five, all in one process on one loopback
    /// host (a debug build), each of the 90 replies to the winning roll call
    /// reached its initiator within 11 ms of the call (p50 6 ms; the p99 of
    /// 90 is their maximum). 250 ms leaves over twenty times that for
    /// replies crossing hosts, a publish relayed through the gossip mesh
    /// rather than sent to a direct peer (not exercised there), and a loaded
    /// host, and adds a quarter of a second to each election. A deadline
    /// too short costs `NoQuorum`s and retries, never safety. It costs
    /// liveness only if the round trip outgrows the suspicion timeout: each
    /// `NoQuorum` widens the next roll call's deadline (see
    /// [`Self::roll_call_deadline`]), up to that timeout, and no further.
    /// Deployments whose round trips run to tens of milliseconds, or whose
    /// shards are much larger, should measure their own and raise it.
    pub const DEFAULT_ROLL_CALL_DEADLINE: Duration = Duration::from_millis(250);

    /// The default `clock_drift_divisor`: clock rates within a tenth of
    /// each other, so a lease lasts at most nine tenths of the suspicion
    /// timeout.
    pub const DEFAULT_CLOCK_DRIFT_DIVISOR: u64 = 10;

    /// The default `reconnect_timeout`.
    pub const DEFAULT_RECONNECT_TIMEOUT: Duration = Duration::from_secs(30);

    /// Timings with this suspicion timeout and heartbeat interval, which
    /// have no default (they depend on the deployment's network), and every
    /// other setting at its default: [`Self::DEFAULT_ROLL_CALL_DEADLINE`],
    /// [`Self::DEFAULT_CLOCK_DRIFT_DIVISOR`] and
    /// [`Self::DEFAULT_RECONNECT_TIMEOUT`].
    pub fn new(suspect_timeout: Duration, heartbeat_interval: Duration) -> Self {
        ElectionTimings {
            suspect_timeout,
            heartbeat_interval,
            roll_call_deadline: Self::DEFAULT_ROLL_CALL_DEADLINE,
            clock_drift_divisor: Self::DEFAULT_CLOCK_DRIFT_DIVISOR,
            reconnect_timeout: Self::DEFAULT_RECONNECT_TIMEOUT,
            drain_wait_limit: Duration::from_ticks(
                suspect_timeout
                    .as_ticks()
                    .saturating_mul(Self::DEFAULT_DRAIN_WAIT_SUSPICIONS),
            ),
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

    /// Replaces [`Self::DEFAULT_RECONNECT_TIMEOUT`] (see
    /// [`Self::reconnect_timeout`]).
    pub fn with_reconnect_timeout(mut self, reconnect_timeout: Duration) -> Self {
        self.reconnect_timeout = reconnect_timeout;
        self
    }

    /// Replaces the default `drain_wait_limit` (see
    /// [`Self::drain_wait_limit`]).
    pub fn with_drain_wait_limit(mut self, drain_wait_limit: Duration) -> Self {
        self.drain_wait_limit = drain_wait_limit;
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
        message: CheckedMessage,
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
    /// Leave the shard gracefully: send `SelfRemove` to the leader this node follows, if any, or as
    /// the leader announce the configuration without itself on a final ack
    /// to every connected peer, and end `Stopped`. From `Active` this happens
    /// at once. A `Leader` keeps leading until every other voter has reported
    /// a routing crawl since its admission, or `drain_wait_limit` has passed
    /// (see [`ElectionTimings::drain_wait_limit`]), and then leaves; a leader
    /// with no other voter leaves at once. From `Draining` or `Stopped` the request does
    /// nothing; from any other state, `Fenced` among them, the node keeps it and
    /// drains as soon as it reaches `Active`, `LeaderReconciling` or `Leader`,
    /// in the same step. A leader that loses office while it waits keeps the
    /// request, and the drain wait, its limit included, starts over when it
    /// regains leadership. A driver asks once.
    ///
    /// A drain kept in a state the node never leaves for `Active`,
    /// `LeaderReconciling` or `Leader` waits for good: a node whose peers never return, which keeps
    /// retrying roll calls from `NoQuorum`, or a `Bootstrapping` node that
    /// never joins. A driver that waits for `Stopped` must not rely on it
    /// there.
    Drain,
    /// The answer to this node's JOIN:
    /// the leader it names, with its term and recovery epoch. A node in
    /// `Bootstrapping` records that leader and moves through `Joining` to
    /// `Active` as a pending member. It learns the configuration from its
    /// leader's first ack. It stays `Bootstrapping`, so its driver can ask
    /// again, for an answer that names no leader, for one that names a leader
    /// of another incarnation of the node's shard, and for one the node's
    /// floor does not accept (see [`JoinFloor::accepts`]). Every other state
    /// ignores it.
    ///
    /// A node that rejoins, one with a floor, does not become a member on the
    /// pointer alone: it holds the pointer's epoch in `Joining`, ignoring
    /// acks as a bootstrapping node does, until a read of the authority
    /// answers (see [`Self::AuthorityEpochRead`]). A node with no floor, a
    /// first join, becomes a member at once.
    ///
    /// The wire handshake that produces the answer, dialing seed addresses,
    /// sending `JOIN_REQUEST`, taking the newest leader pointer among the
    /// `JOIN_RESPONSE`s of one pass, is entirely `net`'s concern (a separate
    /// `/kabudachi/join/1` request_response protocol, not an
    /// `ElectionMessage`); this input only
    /// performs the resulting state transition. Joining publishes nothing.
    JoinAnswer(JoinResponse),
    /// The driver asked the coordination authority for the recovery epoch it
    /// holds for this node's shard, as a read named by `token`, for a node
    /// rejoining (`Bootstrapping`, or `Joining` while it checks a JOIN
    /// pointer). The node keeps the latest such token: only the answer to that
    /// read is applied (see [`Self::AuthorityEpochRead`]), so an older read
    /// answered late is dropped. Every other state ignores it.
    AuthorityEpochAsked(ReplyToken),
    /// The answer to the read named by `token`: the record `held` the
    /// coordination authority held when it answered. It is applied only when
    /// `token` is the latest read the node was told of, and once. A record of
    /// another incarnation of the node's shard shows the node's is gone: it
    /// stops, abandoned, from `Bootstrapping` or `Joining`.
    ///
    /// In `Bootstrapping`, a held epoch of another lineage than the floor's
    /// becomes the floor, so that a leader of that epoch is one the node can
    /// join even at or below the old floor's number; one of the floor's own
    /// lineage changes nothing. In `Joining`, where the node holds the epoch
    /// of a JOIN pointer it took (see [`Self::JoinAnswer`]), a held epoch
    /// equal to that one, number and lineage, makes the node a member, and any
    /// other drops the pointer: the node is `Bootstrapping` again with the held
    /// epoch as its floor. Every other state ignores it. An authority that
    /// holds no epoch answers nothing here, and a node validating a pointer
    /// waits.
    AuthorityEpochRead { token: ReplyToken, held: ShardRecord },
    /// The node's driver completed a routing crawl: it asked the peers it
    /// knows for the peers closest to it and connected to those it found.
    /// The node's heartbeats then say so until its admission generation
    /// next changes (see [`WorkerNode::routing_crawled`]).
    RoutingCrawled,
    /// The node's scheduler has rebuilt the shard's tasks for this office and
    /// every record it republished is stored: a node still reconciling for
    /// that office moves to `Leader`, and its grant follows. Ignored for any
    /// other office, or in any other state.
    Reconciled(ReconcileTerm),
    /// The node's scheduler rebuilt, or adopted late answers. It found the
    /// workers in `silent_holders` holding runs and not answering: they may
    /// have died with the old leader, and never be heard from. A node holding
    /// an office reports each of them not yet heard from silent a suspicion
    /// timeout after this input (see [`Output::WorkerSilence`]), and lost a
    /// reconnect timeout later (see [`Output::WorkerLost`]); a worker it
    /// already tracks keeps the time it was last heard. The workers in
    /// `answered` answered, and are heard now, as by a heartbeat. Ignored
    /// without an office.
    WatchWorkers {
        silent_holders: BTreeSet<WorkerId>,
        answered: BTreeSet<WorkerId>,
    },
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
    /// the node leaves office, for its driver to hand to the node's
    /// scheduler (see [`carry_out`]): `Some` while the node is
    /// `Leader` with a lease, ending where the lease does; `None` otherwise,
    /// and so while it is still `LeaderReconciling`.
    ///
    /// A leader that is not alone a quorum first holds a lease once a quorum
    /// has confirmed one of its acks, and its lease end moves as more
    /// confirmations arrive. A node that leaves office reports `None` just
    /// before that state change, so nothing it asks for after leaving can
    /// let another leader act while its own grant stands.
    Grant(Option<LeadershipGrant>),
    /// The node took office as this term's leader and reconciles before it
    /// leads: its scheduler starts rebuilding (see
    /// `Scheduler::begin_reconcile`), and the node moves to `Leader`, and
    /// reports a grant, only once handed `Input::Reconciled` for the same
    /// office.
    Reconcile(ReconcileTerm),
    /// Make this call on the node's coordination authority, and hand the
    /// node the reply as [`Input::Authority`] (see [`AuthorityCall::perform`]).
    /// Only a node with an authority asks.
    Authority(AuthorityCall),
    /// While the node holds office: the worker has not been heard from for
    /// a suspicion timeout and then a reconnect timeout, so it no longer
    /// counts as placeable. Its TaskRuns are not decided by this report but
    /// by its silence, each at its own reconnect timeout (see
    /// [`Output::WorkerSilence`]). A counted
    /// member whose heartbeats keep arriving but which confirms none of the
    /// leader's acks for that long is lost too, and the leader also removes
    /// it from its configuration, one at a time; a silent worker is only
    /// reported. Reported once; a worker heard from again is watched afresh.
    WorkerLost(WorkerId),
    /// While the node holds office: `worker` has gone a suspicion timeout
    /// unheard (or, a counted member whose heartbeats arrive, as long
    /// without confirming an ack), so each TaskRun it holds is lost, and may
    /// be replayed (see [`carry_out`]), once that run's own reconnect
    /// timeout has passed since `reconnect_from`. `None`: it was heard
    /// again, and its runs are kept. Reported whenever it changes. The
    /// worker's abort deadline for each run comes first (see
    /// [`Output::AbortDeadline`]).
    WorkerSilence {
        worker: WorkerId,
        reconnect_from: Option<Instant>,
    },
    /// While in office: a heartbeat from `worker` said the runs it holds have
    /// this digest, empty if it sent none. For the driver to compare with
    /// what the scheduler believes.
    RunsHeard { worker: WorkerId, digest: Vec<u8> },
    /// The leader this node follows says it cancelled these runs, which this
    /// worker holds: the worker's executor stops their bodies. Reported with
    /// every ack that lists them, so the same run can be reported again.
    RunsCancelled(Vec<TaskRunId>),
    /// By when, on the node's clock, this worker must have aborted each
    /// TaskRun it is running, as an [`AbortBy`] that gives each run's
    /// deadline from that run's reconnect timeout: `Some` once it has gone
    /// a suspicion timeout, less drift, without evidence that its leader
    /// still hears it, or once it has fenced itself;
    /// `None` while it has nothing to abort. Reported whenever it changes,
    /// like [`Output::Grant`]: a later report replaces an earlier one, so a
    /// worker that its leader hears again before the deadline withdraws it
    /// and keeps its runs. For the worker's task executor; the scheduler has
    /// nothing to do with it.
    ///
    /// The deadline comes before any leader can replay those runs. A leader
    /// replays a worker's runs (see [`Output::WorkerSilence`]) a suspicion
    /// timeout and the run's own reconnect timeout after it last heard the
    /// worker, or
    /// after it won if it has not heard it since. An ack from a leader that
    /// holds a grant echoes the send instant of the heartbeat it answers, so
    /// the follower knows that leader heard it no earlier than that, and no
    /// rival can win before that leader's grant ends, after it sent the ack;
    /// a leader that stops leading knows no rival won before its own grant
    /// ended. From
    /// the latest such instant the deadline is the suspicion timeout plus the
    /// run's reconnect timeout, less a tenth for clock drift (the same rate
    /// bound the leader lease assumes). Every worker in the shard must use
    /// the same `suspect_timeout`, and the same default reconnect timeout
    /// (see [`ElectionTimings::reconnect_timeout`]); a run's own timeout
    /// travels with its claim.
    AbortDeadline(Option<AbortBy>),
    /// An alert: the node found the shard gone (no record, or a record of
    /// another incarnation of it), so the shard is abandoned and the node has stopped
    /// (see [`StopReason::Abandoned`]). A restart re-enters the bootstrap
    /// cascade.
    ShardAbandoned,
    /// The node is leaving the shard: its driver hands every Task record the
    /// worker holds to where [`HandOffTo`] says, and waits for them to be
    /// stored (no longer than the drain wait limit) before it lets the worker
    /// exit. Reported once, as the node drains, before it reports `Stopped`.
    HandOff(HandOffTo),
}

/// To whom a draining node hands the Task records it holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandOffTo {
    /// The leader this follower followed: it chooses where each record goes,
    /// for a follower knows no voters.
    Leader(WorkerId),
    /// This node led: these are the voters it could still place records on,
    /// itself left out, as its configuration named them when it left office.
    Voters(Vec<WorkerId>),
    /// It knows no leader: no one can choose where its records go.
    Nobody,
}

/// Why a node is `Stopped`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    /// It drained, leaving the shard gracefully.
    Drained,
    /// Its shard was abandoned: neither a quorum of
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
/// worker's silence, which `Scheduler::note_silence` turns into the loss of
/// each run the worker holds at that run's own reconnect timeout. Messages,
/// authority calls, state changes, the abort deadline and alerts are the
/// driver's to carry out and leave it alone.
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
            Output::Reconcile(term) => scheduler.begin_reconcile(*term),
            Output::WorkerSilence {
                worker,
                reconnect_from,
            } => scheduler.note_silence(worker, *reconnect_from),
            Output::WorkerLost(_)
            | Output::Send { .. }
            | Output::Publish { .. }
            | Output::StateChanged(_)
            | Output::Authority(_)
            | Output::AbortDeadline(_)
            | Output::RunsHeard { .. }
            | Output::RunsCancelled(_)
            | Output::HandOff(_)
            | Output::ShardAbandoned => {}
        }
    }
}

impl<C> WorkerNode<C>
where
    C: Clock,
{
    /// Checks this node's timers against its clock.
    ///
    /// - `Active`: moves to `LeaderSuspect` once no leader ack has arrived
    ///   within its jittered suspicion timeout.
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
    /// `Joining`: nothing times out a
    /// stalled join here, since the transition out of those states happens
    /// once via [`Input::JoinAnswer`], driven by something outside `core`
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
            WorkerState::LeaderReconciling | WorkerState::Leader => {
                self.drain_once_free();
                if self.holds_office() {
                    self.report_lost_workers();
                }
            }
            _ => {}
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
    ///
    /// `None` too for a member that may not stand yet: with an authority, it
    /// stands only once its latest read of the authority's epoch confirms its
    /// own (see [`Self::may_stand`]). Its deadline is then its next read, so
    /// a `Tick` is never due that moves nothing.
    fn next_roll_call_due(&self) -> Option<Instant> {
        self.standing
            .configuration()
            .filter(|_| self.may_stand())
            .map(|_| self.round.roll_call_due(self.clock.now()))
    }

    /// Whether this node may stand for election. With no authority, always.
    /// With one, only while its latest read of the authority's recovery epoch
    /// confirms this node's own epoch, or found the authority holding none:
    /// a node at an epoch the authority does not hold must rejoin, not elect
    /// a leader there that would pull the others off the authority's epoch.
    fn may_stand(&self) -> bool {
        match (self.authority.as_ref(), self.standing.epoch()) {
            (Some(authority), Some(own)) => authority.permits_standing(own),
            _ => true,
        }
    }

    /// The term this node holds or contests: its roll call's while
    /// `RollCall`, its candidacy's while `Candidate` and its leadership's
    /// while `Leader`. `None` in every other state.
    fn term_in_play(&self) -> Option<u64> {
        match self.state {
            WorkerState::RollCall => self.round.roll_call_term(),
            WorkerState::Candidate | WorkerState::LeaderReconciling | WorkerState::Leader => {
                Some(self.term)
            }
            _ => None,
        }
    }

    /// Steps down once this node has seen a term
    /// later than the one it holds or contests: another worker is electing,
    /// or has elected, a leader for it. An ack from that term's leader
    /// returns the node to `Active` (see [`Self::on_leader_ack`]); anything
    /// else leaves it suspecting its leader again (see
    /// [`Self::suspect_again`]).
    fn step_down_if_outpaced(&mut self) {
        if self
            .term_in_play()
            .is_some_and(|term| self.standing.highest_term_seen() > term)
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

    fn lose_quorum(&mut self) {
        self.retry_after_a_fresh_suspicion_timeout();
        self.transition_to(WorkerState::NoQuorum);
    }

    /// Gives up the roll call, candidacy or recovery this node runs, if any,
    /// and puts its next roll call a fresh suspicion timeout away.
    fn retry_after_a_fresh_suspicion_timeout(&mut self) {
        let retry_at = self.clock.now() + self.jittered_suspect_timeout();
        self.round.retry_at(retry_at);
        self.drop_recovery();
    }

    /// The latest term this node knows of: the highest it has seen, or that
    /// of the latest roll call it accepted, its own included, if later.
    fn latest_term(&self) -> u64 {
        self.round.latest_term(self.standing.highest_term_seen())
    }

    /// Takes the leader `pointer` names and drives `Bootstrapping ->
    /// Joining -> Active` (see [`Input::JoinAnswer`]).
    fn join(&mut self, pointer: &JoinResponse) {
        if !self.state.can_transition_to(WorkerState::Joining) {
            return;
        }
        let Some(leader_id) = pointer.leader_id() else {
            return;
        };
        // A leader of another incarnation of the shard leads nothing this node
        // belongs to.
        if pointer.shard_id().as_ref() != Some(&self.shard_id) {
            return;
        }
        // A first join takes any pointer. After that, a pointer the floor the
        // node rejoins at does not accept leads nothing the node can return
        // to.
        let floor = self.standing.join_floor();
        if !floor.accepts(pointer) {
            return;
        }
        let named = RecoveryEpoch::new(pointer.recovery_epoch, pointer.recovery_epoch_lineage);

        self.transition_to(WorkerState::Joining);
        // A node with a floor is rejoining, and the pointer was taken from a
        // search that may lag the authority: it holds the pointer until a
        // read of the authority names its epoch. A read asked before this
        // says nothing of it.
        self.rejoin.awaited_read = None;
        if floor.epoch().is_some() {
            self.rejoin.held = Some(pointer.clone());
            return;
        }
        self.become_member_of(leader_id, pointer.term, named);
    }

    /// Becomes the pending member of `leader`, elected in `term` at
    /// `epoch`, that a validated JOIN pointer named: `Joining -> Active`.
    fn become_member_of(&mut self, leader: WorkerId, term: u64, epoch: RecoveryEpoch) {
        self.standing.joined(epoch, term);
        self.leader = Some((leader, term));
        // A freshly joined node hasn't heard from its leader yet; start the
        // suspicion clock now so it isn't judged suspect the instant it
        // ticks — the same reasoning `Self::new`'s doc gives for a freshly
        // constructed node.
        self.last_leader_contact = self.clock.now();
        // Its registration starts with its membership: until now it kept
        // none, so its lease counts from the join.
        let now = self.clock.now();
        if let Some(authority) = self.authority.as_mut() {
            authority.restart_at(now);
        }
        self.transition_to(WorkerState::Active);
    }

    /// Records `token` as the read of the authority's epoch that the next
    /// answer must name (see [`Input::AuthorityEpochAsked`]).
    fn note_epoch_read_asked(&mut self, token: ReplyToken) {
        if matches!(
            self.state,
            WorkerState::Bootstrapping | WorkerState::Joining
        ) {
            self.rejoin.awaited_read = Some(token);
        }
    }

    /// Applies the answer `held` to the read `token` when it is the latest
    /// asked (see [`Input::AuthorityEpochRead`]): a bootstrapping node
    /// refreshes its floor, and one validating a pointer becomes a member or
    /// drops the pointer.
    fn on_epoch_read(&mut self, token: ReplyToken, held: ShardRecord) {
        if self.rejoin.awaited_read != Some(token) {
            return;
        }
        self.rejoin.awaited_read = None;
        if held.shard_id != self.shard_id {
            self.abandon_shard();
            return;
        }
        let held = held.recovery_epoch;
        match self.state {
            WorkerState::Bootstrapping => {
                let mut floor = self.standing.join_floor();
                let before = floor;
                floor.refresh(held);
                if floor != before {
                    self.standing.rejoin_at(held);
                }
            }
            WorkerState::Joining => {
                let Some(pointer) = self.rejoin.held.take() else {
                    return;
                };
                let named =
                    RecoveryEpoch::new(pointer.recovery_epoch, pointer.recovery_epoch_lineage);
                match pointer.leader_id() {
                    Some(leader) if named == held => {
                        self.become_member_of(leader, pointer.term, named);
                    }
                    _ => {
                        self.standing.rejoin_at(held);
                        self.transition_to(WorkerState::Bootstrapping);
                    }
                }
            }
            _ => {}
        }
    }

    /// Drains at once from `Active`, and from `LeaderReconciling` or `Leader`
    /// once every other voter has crawled or the wait limit has passed. From a
    /// state that will reach one of them later it keeps the request for
    /// [`Self::transition_to`] to apply; a node already draining, or one
    /// that can never drain again, ignores it.
    fn request_drain(&mut self) {
        let asked =
            self.drain_request
                .ask(self.state, &self.clock, self.timings.drain_wait_limit);
        match asked {
            Asked::DrainNow => self.drain(),
            Asked::WaitForCrawls => self.drain_once_free(),
            Asked::Nothing => {}
        }
    }

    /// A leader asked to drain leaves once every other voter has reported a
    /// routing crawl since its admission, so no worker is left knowing only
    /// this leader, or once its drain wait has run out. Until then it keeps
    /// leading.
    fn drain_once_free(&mut self) {
        let leaves = self.drain_request.leave_if_free(&self.clock, || {
            self.office
                .as_mut()
                .is_some_and(|office| office.remaining_voters_have_crawled(&self.my_id))
        });
        if leaves {
            self.drain();
        }
    }

    /// Gracefully shuts an `Active` or `Leader` node down and ends in `Stopped`.
    ///
    /// A follower sends `SelfRemove` to its leader alone, carrying the
    /// highest term it has seen, for that leader's term guard (see
    /// [`Self::on_self_remove`]).
    /// Followers learn of the removal from the generation the leader then
    /// announces. A node that knows no leader tells no one: the next
    /// founding leaves it out, or the authority path counts it out.
    ///
    /// A leader applies its own removal, which no other leader can,
    /// and sends every connected peer a final ack announcing the
    /// configuration without it, together with every removal it has
    /// accepted and not yet applied, so the survivors elect under the
    /// shrunk one. A leader whose removal, with those, would leave no voter
    /// announces nothing (see [`Roster::remove_all`]).
    fn drain(&mut self) {
        // Leaving `Leader` withdraws the grant first, so no departure
        // message goes out while this node still holds one.
        let was_leader = self.holds_office();
        let hand_off = if was_leader {
            HandOffTo::Voters(
                self.placeable_voters()
                    .into_iter()
                    .filter(|voter| *voter != self.my_id)
                    .collect(),
            )
        } else {
            match &self.leader {
                Some((leader, _)) if *leader != self.my_id => HandOffTo::Leader(leader.clone()),
                _ => HandOffTo::Nobody,
            }
        };
        let departure = self
            .office
            .take()
            .and_then(|office| office.depart(&self.my_id, &mut self.standing));
        self.transition_to(WorkerState::Draining);
        if was_leader {
            self.announce_own_departure(departure);
        } else {
            self.tell_leader_of_departure();
        }
        self.outputs.push(Output::HandOff(hand_off));

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
                .standing
                .configuration()
                .map(|configuration| configuration.generation().into()),
            term_seen: self.standing.highest_term_seen(),
            leader_term,
        };
        self.send(leader, election_message::Payload::SelfRemove(msg));
    }

    /// The draining leader's half of [`Self::drain`]: if taking itself out
    /// of its roster announced a change (`departure`), acks every connected
    /// peer with it.
    fn announce_own_departure(&mut self, departure: Option<Departure>) {
        let Some(departure) = departure else {
            return;
        };
        for peer in self.connected.clone() {
            // Unasked, and sent after the grant is withdrawn: no heartbeat
            // token to vouch for.
            let content = departure.ack_for(&peer);
            self.send_ack_with(peer, content, None);
        }
    }

    /// Accepts a departing worker's SELF_REMOVE, to take it out of this leader's roster with every
    /// other one accepted since the last change, in one next generation and
    /// with no commit round (see [`LeaderOffice::take_removal`]).
    ///
    /// It accepts only a removal addressed to this leadership: to this
    /// node, as the leader of this term. And the term guard: it accepts the removal only if the
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
    fn on_self_remove(&mut self, departing: &WorkerId, msg: &Checked<SelfRemove>) {
        if !self.accepts_removal(msg) {
            return;
        }
        if let Some(office) = self.office.as_mut() {
            office.take_removal(departing.clone());
            self.cancelled_runs.forget(departing);
        }
        self.drain_once_free();
    }

    /// Whether this leader accepts `msg`: it holds office, `msg` is for its
    /// shard, and it is addressed to this leadership by a worker that has seen
    /// no later term (the term guard; see [`Self::on_self_remove`]).
    fn accepts_removal(&self, msg: &Checked<SelfRemove>) -> bool {
        let to_this_leadership = msg.term_seen <= self.term && msg.leader_term == self.term;
        self.holds_office() && msg.shard_id() == self.shard_id && to_this_leadership
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
            recovery_epoch: self.standing.epoch(),
            highest_term_seen: self.standing.highest_term_seen(),
            configuration: led_or_followed(&self.office, &self.standing),
            admission,
            takes_part,
            roll_call_is_census: self
                .authority
                .as_ref()
                .is_some_and(AuthorityStanding::roll_call_is_census),
            leader_contact_is_fresh,
            roll_call_deadline: self.timings.roll_call_deadline,
            suspect_timeout: self.timings.suspect_timeout,
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
                Verdict::Publish(call) => self.publish_roll_call(call),
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
                Verdict::Grant { candidate, grant } => self.grant_vote(candidate, grant),
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
                } => self.close_short(term, configuration, respondents),
                Verdict::SuspectAgain => self.suspect_again(),
            }
            self.debug_assert_round_matches_state();
        }
    }

    /// Starts this node's roll call `call`: drops any recovery, moves to
    /// `RollCall` and publishes it.
    fn publish_roll_call(&mut self, call: RollCall) {
        self.drop_recovery();
        self.transition_to(WorkerState::RollCall);
        self.publish(election_message::Payload::RollCall(call));
    }

    /// Grants `candidate` this vote, having raised the highest term seen to
    /// its term, which makes an initiator contesting an earlier term step down.
    fn grant_vote(&mut self, candidate: WorkerId, grant: VoteGrant) {
        self.standing.saw_term(grant.term);
        self.step_down_if_outpaced();
        self.send(candidate, election_message::Payload::VoteGrant(grant));
    }

    /// This node's roll call for `term` under `configuration` closed short
    /// with `respondents`: it goes `NoQuorum` and, with an authority, takes
    /// the authority path from that census, which ends where the roll call's
    /// retry would have come.
    fn close_short(
        &mut self,
        term: u64,
        configuration: Configuration,
        respondents: BTreeMap<WorkerId, Admission>,
    ) {
        self.lose_quorum();
        let now = self.clock.now();
        let retry_at = self.round.roll_call_due(now);
        if let Some(authority) = self.authority.as_mut() {
            let call = authority.begin_recovery(term, configuration, respondents, now, retry_at);
            self.outputs.push(Output::Authority(call));
        }
    }

    /// The round's roll call and vote stand in for the `RollCall` and
    /// `Candidate` states' checks (see `ElectionRound`), so they move together.
    fn debug_assert_round_matches_state(&self) {
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

    /// Refuses `initiator`'s roll call or vote request for `term`, naming
    /// this node's highest term seen, its configuration, and the leader it
    /// follows, if any, when `name_leader` asks for it, when this node leads,
    /// or when its contact with that leader is fresh: a node refused a roll
    /// call for any reason learns who leads, or one cut off from its leader's
    /// acks while another is elected never finds it again. A candidate refused
    /// a vote ignores the name.
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
            .filter(|_| {
                name_leader || self.holds_office() || self.current_leader_still_valid()
            })
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
            highest_term_seen: self.standing.highest_term_seen(),
            leader,
            configuration: self.led_or_followed_configuration().map(Into::into),
            recovery_epoch: self.standing.epoch().map(|epoch| epoch.number),
            recovery_epoch_lineage: self.standing.epoch().map_or(0, |epoch| epoch.lineage),
        };
        self.send(initiator, election_message::Payload::ElectionReject(reject));
    }

    /// How long this node, while `Active`, goes without an accepted leader
    /// ack before it suspects its leader:
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
    /// it: a higher term raises its highest term seen,
    /// and while `RollCall` a leader named at a term no older than that, or
    /// named as still valid at any term, becomes its leader, which it
    /// heartbeats until that leader's ack returns it to `Active` (or, if
    /// that leader's term is below its own term seen, until the heartbeat
    /// makes that leader step down). A refusal carrying the commit of the
    /// joint configuration this node holds, or a batch started on its single
    /// one, hands it that configuration (see
    /// [`Self::adopt_relayed_configuration`]). A term later than the one it
    /// holds or contests makes it step down (see
    /// [`Self::step_down_if_outpaced`]).
    /// A refusal from a newer epoch raises nothing (the two epochs' terms do
    /// not compare) but, while `RollCall`, makes the leader it names this
    /// node's, whose ack then moves it to that epoch. A refusal from an older
    /// epoch is dropped (see [`EpochOrder`]). A refusal names its refuser's
    /// epoch and lineage itself; one that names no epoch (its refuser has
    /// joined no shard) is read as this node's own epoch's. The refusal
    /// itself is not counted.
    fn on_election_reject(&mut self, reject: &Checked<ElectionReject>) {
        if reject.shard_id() != self.shard_id || reject.initiator_id() != self.my_id {
            return;
        }
        let offered = reject.configuration();
        let refuser_epoch = reject
            .recovery_epoch
            .map(|number| RecoveryEpoch::new(number, reject.recovery_epoch_lineage));
        match refuser_epoch.and_then(|heard| self.standing.order(heard)) {
            // A newer epoch's terms are not this epoch's, so they raise
            // nothing here; the named leader's ack moves this node.
            Some(EpochOrder::Later) => {
                if let Some(leader) = self.leader_named_by(reject, true) {
                    self.leader = Some(leader);
                }
                return;
            }
            // An older epoch's terms, leader and configuration mean nothing
            // here, and its configuration must not be relayed.
            Some(EpochOrder::Stale) => return,
            Some(EpochOrder::Mine) | None => {}
        }
        self.standing.saw_term(reject.highest_term_seen);
        // A leader named as still valid is heartbeated whatever its term:
        // if this node's term seen is above that leader's, its acks stay
        // ignored, but the heartbeat tells it of the later term, and it
        // steps down (see `Self::on_heartbeat`).
        if let Some(leader) = self.leader_named_by(reject, false) {
            self.leader = Some(leader);
        }
        if let Some(offered) = offered {
            self.adopt_relayed_configuration(offered);
        }
        self.step_down_if_outpaced();
    }

    /// The leader `reject` names for this node to follow while it is
    /// `RollCall`: never itself, and, unless `any_term`, only one named at a
    /// term no older than the highest this node has seen, or named as still
    /// valid.
    fn leader_named_by(
        &self,
        reject: &Checked<ElectionReject>,
        any_term: bool,
    ) -> Option<(WorkerId, u64)> {
        if self.state != WorkerState::RollCall {
            return None;
        }
        let (leader, term) = reject.named_leader()?;
        let current = any_term
            || term >= self.standing.highest_term_seen()
            || reject.reason() == ElectionRejectReason::LeaderStillValid;
        (current && leader != self.my_id).then_some((leader, term))
    }

    /// Adopts `offered`, a refuser's configuration, when it is the commit of
    /// the joint configuration this node holds, or the batch its leader
    /// started on the single one it holds, and this node is a voter there, or
    /// the batch it was promised admission in, which it is no voter of in what
    /// it holds, only in the batch itself: the ack that carried it never reached this node, say because its
    /// leader stopped soon after (see
    /// [`ShardStanding::adopt_relayed_configuration`]). Without this, a
    /// survivor holding the older configuration and one holding the newer
    /// refuse each other's roll calls term after term. Only a node that takes
    /// part in elections without standing or leading adopts it.
    fn adopt_relayed_configuration(&mut self, offered: Configuration) {
        if self.takes_part_in_elections() {
            self.standing.adopt_relayed_configuration(offered);
        }
    }

    /// Accepts `leader`'s certificate of what its election's winner leads,
    /// sent to every respondent of its winning roll
    /// call: this node adopts that configuration and its admission
    /// generations there, as [`ShardStanding::adopt_certificate`] allows, and the
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
    fn on_election_certificate(
        &mut self,
        leader: &WorkerId,
        certificate: &Checked<ElectionCertificate>,
    ) {
        if !self.is_joined_to(&certificate.shard_id())
            || self.standing.order(RecoveryEpoch::new(
                certificate.recovery_epoch,
                certificate.recovery_epoch_lineage,
            )) != Some(EpochOrder::Mine)
        {
            return;
        }
        let voted_for_it = self.round.granted_in(certificate.term) == Some(leader);
        if certificate.term < self.ack_floor() && !voted_for_it {
            return;
        }
        if self.holds_office() && certificate.term > self.term {
            self.leave_office();
        }
        self.standing.adopt_certificate(
            certificate.term,
            certificate.configuration(),
            certificate.recipient_admission(),
            certificate.recipient_prior_admission(),
        );
        self.step_down_if_outpaced();
    }

    /// Asks the authority for what this node's standing has come due for
    /// (see [`AuthorityStanding::calls_due`]).
    fn ask_authority_if_due(&mut self) {
        let now = self.clock.now();
        let Some(authority) = self.authority.as_mut() else {
            return;
        };
        let view = AuthorityView {
            me: &self.my_id,
            shard: &self.shard_id,
            state: self.state,
            term: self.term,
            own_epoch: self.standing.epoch(),
        };
        self.outputs
            .extend(authority.calls_due(&view, now).into_iter().map(Output::Authority));
    }

    /// Handles what the authority answered to a call this node asked for.
    fn on_authority_reply(&mut self, reply: AuthorityReply) {
        let now = self.clock.now();
        let Some(authority) = self.authority.as_mut() else {
            return;
        };
        let view = AuthorityView {
            me: &self.my_id,
            shard: &self.shard_id,
            state: self.state,
            term: self.term,
            own_epoch: self.standing.epoch(),
        };
        let verdicts = authority.on_reply(reply, &view, now);
        self.apply_authority(verdicts);
    }

    /// Carries out, in order, what this node's authority standing decided.
    fn apply_authority(&mut self, verdicts: Vec<AuthorityVerdict>) {
        for verdict in verdicts {
            match verdict {
                AuthorityVerdict::Ask(call) => self.outputs.push(Output::Authority(call)),
                AuthorityVerdict::Resume => {
                    self.lease.resumed();
                    self.last_leader_contact = self.clock.now();
                    self.transition_to(WorkerState::Active);
                }
                AuthorityVerdict::RejoinAt(epoch) => self.rejoin_at(epoch),
                AuthorityVerdict::StandAt {
                    epoch,
                    term,
                    roster,
                } => {
                    self.newest_accepted_ack = None;
                    if let Some(authority) = self.authority.as_mut() {
                        authority.epoch_left();
                    }
                    self.standing
                        .recovered_to(epoch, term, Some(&roster), &self.my_id);
                    self.term = term;
                    self.transition_to(WorkerState::Candidate);
                }
                AuthorityVerdict::Lead(roster) => self.take_office(roster),
                AuthorityVerdict::Abandon => self.abandon_shard(),
                AuthorityVerdict::LoseQuorum => self.lose_quorum(),
                AuthorityVerdict::SuspectAgain => self.suspect_again(),
            }
        }
    }

    /// Gives up the authority path this node runs, if any.
    fn drop_recovery(&mut self) {
        if let Some(authority) = self.authority.as_mut() {
            authority.drop_recovery();
        }
    }

    /// Fences this node if its registration has
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
            .is_some_and(|authority| !authority.is_registered(now));
        let takes_part = matches!(
            self.state,
            WorkerState::Active
                | WorkerState::LeaderSuspect
                | WorkerState::RollCall
                | WorkerState::Candidate
                | WorkerState::LeaderReconciling
                | WorkerState::Leader
                | WorkerState::NoQuorum
        );
        if !lapsed || !takes_part {
            return false;
        }
        self.round.stop();
        self.drop_recovery();
        self.transition_to(WorkerState::Fenced);
        self.lease.orphaned(now);
        true
    }

    /// Forgets the election state this node held under its recovery epoch,
    /// as it leaves that epoch behind. What its standing forgets goes with
    /// the transition that moves it (see [`ShardStanding::accept_ack`] and
    /// [`ShardStanding::rejoin_at`]).
    fn forget_election_state(&mut self) {
        if let Some(authority) = self.authority.as_mut() {
            authority.epoch_left();
        }
        self.newest_accepted_ack = None;
        self.round.forget();
        self.drop_recovery();
        self.office = None;
    }

    /// Leaves the shard this node held for the one the authority holds at
    /// `epoch`, which it cannot resume or recover into: discards everything
    /// it knew of its own and goes back to `Bootstrapping`, for its driver to
    /// join it again. The epoch it rejoins is a
    /// floor: a JOIN pointer to a leader left on an older one (see
    /// [`EpochOrder::Stale`]) must not take it back there.
    fn rejoin_at(&mut self, epoch: RecoveryEpoch) {
        self.forget_election_state();
        self.standing.rejoin_at(epoch);
        if let Some(authority) = self.authority.as_mut() {
            authority.rejoined();
        }
        self.leader = None;
        self.transition_to(WorkerState::Bootstrapping);
    }

    /// The node found the shard gone: no record under its name, or a record
    /// of another incarnation of it. The shard is abandoned, and this node
    /// stops for good,
    /// raising an alert. A restart re-enters the bootstrap cascade.
    fn abandon_shard(&mut self) {
        self.stop_reason = Some(StopReason::Abandoned);
        self.transition_to(WorkerState::Stopped);
        self.outputs.push(Output::ShardAbandoned);
    }
}

/// The configuration a node leads through its `office`, if it holds one, and
/// otherwise the one its `standing` follows. Takes the two fields, not the
/// node, so a caller can hold it while borrowing another field of the node.
fn led_or_followed<'a>(
    office: &'a Option<LeaderOffice>,
    standing: &'a ShardStanding,
) -> Option<&'a Configuration> {
    match office {
        Some(office) => Some(office.configuration()),
        None => standing.configuration(),
    }
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
