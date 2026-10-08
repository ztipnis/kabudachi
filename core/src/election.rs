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
//! worker it has not heard from for a suspicion timeout and a reconnect
//! timeout as lost.
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
mod carry_out;
mod drain;
mod election_round;
mod entry;
mod leader_office;
mod lease;
mod standing;

use std::collections::BTreeSet;

pub use authority::{
    AuthorityCall, AuthorityReply, AuthorityRequest, AuthorityTimings, CallKind, Issuer,
    ReplyToken, ReplyTokens,
};
pub use carry_out::{AuthorityPerformer, DropMessages, MessageSink, NoAuthority, carry_out};
pub use entry::{Entry, Identity};

use crate::configuration::{Admission, Configuration, Generation, Roster, Tally};
use crate::coordination_authority::{RecoveryEpoch, ShardRecord};
use crate::hashing::{Field, HashFunction};
use crate::protocol::checked::{Checked, CheckedMessage, CheckedPayload, decode};
use crate::protocol::digest::Digest;
use crate::protocol::ids::{IdGenerator, IncarnationId, ShardId, WorkerId};
use crate::protocol::messages::prelude::*;
use crate::protocol::messages::{
    AckEcho, ElectionCertificate, ElectionMessage, ElectionReject, ElectionRejectReason,
    JoinResponse, KnownLeader, LeaderHeartbeatAck, SelfRemove, WorkerHeartbeat, election_message,
};
use crate::protocol::worker_state::WorkerState;
use crate::reconcile::{Answered, ReconcileTerm};
use crate::scheduler::{LeadershipGrant, LeaseEnd, Observer, Scheduler};
use crate::time::{Clock, Duration, Instant};

use authority_standing::{AuthorityStanding, AuthorityVerdict, AuthorityView};
use drain::{Asked, DrainRequest};
use election_round::{ElectionRound, Verdict, View};
use leader_office::{AckContent, Departure, Duties, Heard, LeaderOffice};
use lease::{Lease, LeaseChange, Office};
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
    /// The workers reported lost while this node reconciled, which its
    /// scheduler keeps rather than applies. Watched again once the node leads
    /// (see [`Self::on_reconciled`]).
    lost_while_reconciling: BTreeSet<WorkerId>,
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
    /// What this node's heartbeats say of the runs it holds (see
    /// [`Self::set_active_runs_digest`]); empty until set.
    active_runs_digest: Vec<u8>,
    /// Whether this worker runs compaction, which its heartbeats say.
    runs_compaction: bool,
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
    /// How long a leader waits, after it would first suspect a silent
    /// worker, before it reports that worker lost and its TaskRuns are
    /// replayed; and, less drift, how long a worker cut off from its leader
    /// or fenced has to abort its own.
    /// Every worker in the shard must use the same value, or a leader could
    /// replay the work of a worker still running it. Usually
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
    /// The node's scheduler found these workers holding runs it rebuilt or
    /// adopted, and none of them answered: they may have died with the old
    /// leader, and never be heard from. A node holding an office reports each
    /// not yet heard from lost a suspicion timeout and a reconnect timeout
    /// after this input (see [`Output::WorkerLost`]); a worker it already
    /// tracks keeps the time it was last heard. Ignored without an office.
    WatchWorkers(BTreeSet<WorkerId>),
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
    /// a suspicion timeout and then a reconnect timeout, so every TaskRun it
    /// holds is lost and may be replayed (see [`carry_out`]). A counted
    /// member whose heartbeats keep arriving but which confirms none of the
    /// leader's acks for that long is lost too, and the leader also removes
    /// it from its configuration, one at a time; a silent worker is only
    /// reported. Reported once; a worker heard from again is watched afresh.
    WorkerLost(WorkerId),
    /// While in office: a heartbeat from `worker` said the runs it holds have
    /// this digest, empty if it sent none. For the driver to compare with
    /// what the scheduler believes.
    RunsHeard { worker: WorkerId, digest: Vec<u8> },
    /// By when, on the node's clock, this worker must have aborted every
    /// TaskRun it is running: `Some` once it has gone
    /// a suspicion timeout, less drift, without evidence that its leader
    /// still hears it, or once it has fenced itself;
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
    /// [`ElectionTimings::reconnect_timeout`]).
    AbortDeadline(Option<Instant>),
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
            Output::Reconcile(term) => scheduler.begin_reconcile(*term),
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
            | Output::RunsHeard { .. }
            | Output::HandOff(_)
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
            lost_while_reconciling: BTreeSet::new(),
            active_runs_digest: Vec::new(),
            runs_compaction: false,
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

    /// The voters of the configuration this node leads, by id, itself
    /// included; empty unless it holds office, reconciling or leading.
    /// Count-based configurations may hold fewer ids than voters (a voter the
    /// roster does not know is not named); only the leader's own roster is
    /// read, never the routing table.
    pub fn voters(&self) -> Vec<WorkerId> {
        match (&self.office, self.holds_office()) {
            (Some(office), true) => office.voter_ids(&self.my_id),
            _ => Vec::new(),
        }
    }

    /// The voters this leader places Task records on: those of its
    /// configuration, minus every one it has reported lost and not heard
    /// from since (one reported for confirming no ack, until it confirms
    /// one). Empty unless it holds office. Read from its roster alone.
    pub fn placeable_voters(&self) -> Vec<WorkerId> {
        match (&self.office, self.holds_office()) {
            (Some(office), true) => office.placeable_voter_ids(&self.my_id),
            _ => Vec::new(),
        }
    }

    /// Whether this node holds office and its roster holds `worker` as a
    /// voter of the configuration it leads or as a pending member. `false`
    /// for every worker while this node holds none. Only the leader's own
    /// roster is read, never the routing table.
    pub fn is_voter_or_pending(&self, worker: &WorkerId) -> bool {
        match (&self.office, self.holds_office()) {
            (Some(office), true) => office.is_voter_or_pending(worker),
            _ => false,
        }
    }

    /// The office this node holds, reconciling or leading: its recovery epoch
    /// and term. `None` while it holds none.
    pub fn office_term(&self) -> Option<ReconcileTerm> {
        if !self.holds_office() {
            return None;
        }
        Some(ReconcileTerm {
            recovery_epoch: self.standing.epoch()?,
            term: self.term,
        })
    }

    /// The certificate this leader presents to a worker it asks what it holds
    /// (see [`Self::may_answer_reconcile`]): for the term and recovery epoch
    /// it took office in, the configuration it leads. `None` unless it holds
    /// office.
    pub fn reconcile_proof(&self) -> Option<ElectionCertificate> {
        let office = self.office.as_ref().filter(|_| self.holds_office())?;
        Some(ElectionCertificate {
            shard_id: Some(self.shard_id.clone().into()),
            recovery_epoch: self.standing.epoch_number(),
            recovery_epoch_lineage: self.standing.epoch().map_or(0, |epoch| epoch.lineage),
            term: self.term,
            leader_id: Some(self.my_id.clone().into()),
            configuration: Some(office.configuration().into()),
            recipient_admission: None,
            recipient_prior_admission: None,
        })
    }

    /// Whether this worker may answer `from`, which asks it what it holds
    /// before it schedules, presenting `proof`. It answers the leader it
    /// follows, and a requester that proves an office: a certificate it
    /// names itself as the leader of, for this node's shard, no earlier in
    /// term than the highest this node has seen and not stamped before the
    /// configuration this node holds (a later recovery epoch's terms do not
    /// compare with this node's, so one is accepted as a newer office). A
    /// requester that is neither, such as a deposed leader, is refused, so
    /// what a worker holds is told only to the office that may act on it.
    /// The certificate is checked as one received over the wire is. A node
    /// that never joined a shard answers no one.
    pub fn may_answer_reconcile(
        &self,
        from: &WorkerId,
        proof: Option<&ElectionCertificate>,
    ) -> bool {
        if self
            .known_leader()
            .is_some_and(|(leader, _)| &leader == from)
        {
            return true;
        }
        let Some(proof) = proof else {
            return false;
        };
        let wrapped = ElectionMessage {
            payload: Some(election_message::Payload::ElectionCertificate(
                proof.clone(),
            )),
        };
        let Some(CheckedPayload::ElectionCertificate(certificate)) =
            decode(wrapped).ok().and_then(CheckedMessage::into_payload)
        else {
            return false;
        };
        if certificate.leader_id() != *from || certificate.shard_id() != self.shard_id {
            return false;
        }
        let named =
            RecoveryEpoch::new(certificate.recovery_epoch, certificate.recovery_epoch_lineage);
        match self.standing.order(named) {
            Some(EpochOrder::Later) => true,
            Some(EpochOrder::Stale) | None => false,
            Some(EpochOrder::Mine) => {
                certificate.term >= self.standing.highest_term_seen()
                    && self
                        .led_or_followed_configuration()
                        .is_none_or(|held| held.generation().term() <= certificate.term)
            }
        }
    }

    /// Whom its reconciliation asks: the voters of the configuration it leads
    /// and its pending members, by id, itself included. Empty unless it holds
    /// office. Read from its roster alone, never the routing table.
    pub fn reconcilees(&self) -> Vec<WorkerId> {
        match (&self.office, self.holds_office()) {
            (Some(office), true) => office.reconcilees(&self.my_id),
            _ => Vec::new(),
        }
    }

    /// What `answered`, the workers that answered its reconciliation, amount
    /// to among its voters (both sides of a joint configuration): all, a
    /// quorum, or short of one. Pending members count for nothing. `Short`
    /// unless it holds office.
    pub fn voters_answered(&self, answered: &BTreeSet<WorkerId>) -> Answered {
        match (&self.office, self.holds_office()) {
            (Some(office), true) => office.voters_answered(&self.my_id, answered),
            _ => Answered::Short,
        }
    }

    /// Whether its roster holds `worker`, admitted or pending. `false`
    /// unless it holds office.
    pub fn is_member(&self, worker: &WorkerId) -> bool {
        match (&self.office, self.holds_office()) {
            (Some(office), true) => office.is_member(worker),
            _ => false,
        }
    }

    /// Whether it holds office: reconciling or leading.
    fn holds_office(&self) -> bool {
        matches!(
            self.state,
            WorkerState::LeaderReconciling | WorkerState::Leader
        )
    }

    /// Its scheduler reconciled for `office`: the node leads. A worker lost
    /// while it reconciled may have answered the reconciliation first and died
    /// after, which the scheduler cannot tell from one that is alive; watching
    /// each again from now loses a dead one once more, this time applied.
    fn on_reconciled(&mut self, office: ReconcileTerm) {
        if self.state == WorkerState::LeaderReconciling && self.office_term() == Some(office) {
            let now = self.clock.now();
            let lost = std::mem::take(&mut self.lost_while_reconciling);
            if let Some(held) = self.office.as_mut() {
                held.watch(lost, &self.my_id, now);
            }
            self.transition_to(WorkerState::Leader);
        }
    }

    pub fn shard_id(&self) -> &ShardId {
        &self.shard_id
    }

    /// The highest term this node has seen: raised by a vote it granted, an
    /// ack it accepted, a refusal, a certificate or a heartbeat naming a
    /// later term, the leader a JOIN pointed it at, winning an election, and
    /// standing through the authority path. Never by a roll call it
    /// answered, nor by standing as a candidate in an election it has not
    /// won.
    pub fn highest_term_seen(&self) -> u64 {
        self.standing.highest_term_seen()
    }

    /// This node's recovery epoch: its configuration's when built with one
    /// ([`Self::start`]), taken from the leader a JOIN pointed it at
    /// ([`Input::JoinAnswer`]), moved on by an authority-path recovery of
    /// its own, and adopted from the ack of a leader of a later epoch.
    pub fn recovery_epoch(&self) -> u64 {
        self.standing.epoch_number()
    }

    /// The floor this node takes JOIN pointers against: its recovery epoch,
    /// or none before it has joined a shard. A driver searching for a leader
    /// for it takes the node's current floor for each pass, as the node's
    /// floor moves with the epoch reads it is told of.
    pub fn join_floor(&self) -> JoinFloor {
        self.standing.join_floor()
    }

    /// The lineage of this node's recovery epoch (see [`RecoveryEpoch`]),
    /// learned with the epoch; `None` until the node has joined a shard.
    pub fn recovery_lineage(&self) -> Option<u64> {
        self.standing.epoch().map(|epoch| epoch.lineage)
    }

    /// Why this node stopped; `None` unless it is `Stopped`.
    pub fn stop_reason(&self) -> Option<StopReason> {
        self.stop_reason
    }

    /// The configuration this node knows: the one it was built with or the
    /// newest a leader's ack or election certificate has carried since.
    /// `None` for a joiner that has accepted neither yet.
    pub fn configuration(&self) -> Option<&Configuration> {
        self.led_or_followed_configuration()
    }

    /// The digest of the runs this worker holds, which every heartbeat it
    /// sends from now on carries (see `reconcile::active_runs_digest`).
    pub fn set_active_runs_digest(&mut self, digest: Digest) {
        self.active_runs_digest = digest.value().to_vec();
    }

    /// Whether this worker runs compaction, which every heartbeat it sends
    /// from now on says.
    pub fn set_runs_compaction(&mut self, runs: bool) {
        self.runs_compaction = runs;
    }

    /// Whether this worker may start a TaskRun now: only once it could abort
    /// the run in time if it lost its leader's ear, which needs a leader to
    /// have vouched for hearing it (an ack echoing one of its heartbeats), or
    /// a grant of its own that ends, or a grant no rival can outlast. Before
    /// then no abort deadline could be named for the run (see
    /// [`Output::AbortDeadline`]).
    pub fn has_contact_floor(&self) -> bool {
        self.lease.has_contact_floor()
    }

    /// The members that said in their latest heartbeat that they run
    /// compaction, this node itself if it does. Empty unless it holds office:
    /// only the leader's own roster is read.
    pub fn compaction_runners(&self) -> BTreeSet<WorkerId> {
        match (&self.office, self.holds_office()) {
            (Some(office), true) => {
                let mut runners = office.compaction_runners();
                if self.runs_compaction {
                    runners.insert(self.my_id.clone());
                }
                runners
            }
            _ => BTreeSet::new(),
        }
    }

    /// Whether this node has completed a routing crawl since it was admitted
    /// at the admission generation it holds now. Its heartbeats carry it.
    pub fn routing_crawled(&self) -> bool {
        self.admission().is_some() && self.crawled_at_admission == self.admission()
    }

    /// The generation at which this node was admitted or promised admission
    /// as a voter; `None` for a pending member. A promise outruns the
    /// configuration that will count it, so this alone does not make the
    /// node a voter of the configuration it holds.
    pub fn admission(&self) -> Option<Generation> {
        self.counted_admission().current
    }

    /// The admission generation this node held before the election that
    /// founded the joint configuration it holds admitted it; `None` once
    /// that configuration is committed, and for any other configuration.
    pub fn prior_admission(&self) -> Option<Generation> {
        self.counted_admission().prior
    }

    /// Whether this node has no admission generation: it joined through a
    /// JOIN and neither a leader's ack nor an election certificate has
    /// admitted or promised it admission since. It claims work, and it answers roll calls and
    /// grants votes as a new voter, but no quorum counts it.
    pub fn is_pending_member(&self) -> bool {
        self.counted_admission().current.is_none()
    }

    /// The configuration this node leads while it leads, and the one it
    /// follows otherwise.
    fn led_or_followed_configuration(&self) -> Option<&Configuration> {
        led_or_followed(&self.office, &self.standing)
    }

    /// The admission generations a quorum counts this node by: those of the
    /// roster it leads while it leads, and those it follows otherwise.
    fn counted_admission(&self) -> Admission {
        match &self.office {
            Some(office) => Admission {
                current: office.admission_of(&self.my_id),
                prior: office.prior_admission_of(&self.my_id),
            },
            None => self.standing.counted_admission(),
        }
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
            WorkerState::LeaderReconciling | WorkerState::Leader => {
                Some((self.my_id.clone(), self.term))
            }
            WorkerState::Active => self.leader.clone(),
            WorkerState::LeaderSuspect if self.led_or_followed_configuration().is_none() => {
                self.leader.clone()
            }
            _ => None,
        };
        named.filter(|(_, term)| *term >= self.standing.highest_term_seen())
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
            recovery_epoch: self.standing.epoch_number(),
            recovery_epoch_lineage: self.recovery_lineage().unwrap_or_default(),
            shard_id: Some(self.shard_id.clone().into()),
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
            Input::JoinAnswer(pointer) => self.join(&pointer),
            Input::AuthorityEpochAsked(token) => self.note_epoch_read_asked(token),
            Input::AuthorityEpochRead { token, held } => self.on_epoch_read(token, held),
            Input::Drain => self.request_drain(),
            Input::RoutingCrawled => self.crawled_at_admission = self.admission(),
            Input::Reconciled(office) => self.on_reconciled(office),
            Input::WatchWorkers(workers) => {
                let now = self.clock.now();
                if let Some(office) = self.office.as_mut() {
                    office.watch(workers, &self.my_id, now);
                }
            }
        }
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
    /// - `LeaderReconciling` and `Leader`: when it goes `NoQuorum` unless
    ///   more confirmations arrive (never, for a leader that alone is a
    ///   quorum), or when it next has a worker to report lost, whichever
    ///   comes first.
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
            .and_then(|authority| authority.next_deadline(self.state));
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
            WorkerState::LeaderReconciling | WorkerState::Leader => earliest(
                earliest(
                    self.office.as_ref().and_then(|office| {
                        self.lease
                            .no_quorum_at(&self.my_id, office.roster(), &self.timings)
                    }),
                    self.office
                        .as_ref()
                        .and_then(|office| {
                            office.next_lost_at(
                                self.clock.now(),
                                self.lost_after(),
                                self.timings.suspect_timeout,
                            )
                        }),
                ),
                self.drain_request.wakes_at(),
            ),
            _ => None,
        }
    }

    /// Moves to `next` and reports the move. Leaving `Leader` first reports
    /// that the node holds no grant (see [`Output::Grant`]), and leaving the
    /// states that lead or stand to lead gives up any fence. Reaching
    /// `Active`, `LeaderReconciling` or `Leader` applies a drain kept from an
    /// earlier request at once (see [`Input::Drain`]).
    fn transition_to(&mut self, next: WorkerState) {
        // Every caller moves along an edge of the transition table, checked
        // by the state it guards on first; no input can reach an illegal
        // edge, so one here is a bug in this module.
        assert!(
            self.state.can_transition_to(next),
            "illegal election state transition {:?} -> {next:?}",
            self.state
        );
        // Reconciling to leading keeps the office; every other edge out of
        // either state gives it up.
        if self.holds_office() && next != WorkerState::Leader {
            // Losing office mid-wait keeps the request: the node drains
            // when it next reaches `Active`, `LeaderReconciling` or `Leader`.
            self.drain_request.office_lost();
            self.lease.withdraw_grant(self.clock.now());
            self.outputs.push(Output::Grant(None));
            self.leave_office();
        }
        if let Some(authority) = self.authority.as_mut() {
            authority.state_changed(next);
        }
        // Only a rejoining node keeps a read it awaits or a pointer it holds.
        if !matches!(next, WorkerState::Bootstrapping | WorkerState::Joining) {
            self.rejoin = RejoinCheck::default();
        }
        self.state = next;
        self.outputs.push(Output::StateChanged(next));

        if self.drain_request.take_kept_for(next) {
            self.request_drain();
        }
    }

    /// Hands the office back to the standing, if this node holds one: the
    /// standing then follows the configuration the roster led, as it stands
    /// (see [`LeaderOffice::hand_back`]). Call it before anything that writes
    /// the standing while this node still leads, so what it writes is not
    /// overwritten by the configuration this node led.
    fn leave_office(&mut self) {
        if let Some(office) = self.office.take() {
            office.hand_back(&mut self.standing, &self.my_id);
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

    /// Processes a leader heartbeat acknowledgement. Acks for another shard,
    /// an older recovery epoch, or a term of this node's own epoch below its
    /// floor (see [`Self::ack_floor`]) are ignored without touching any state.
    ///
    /// An ack from a later recovery epoch means the shard was recovered
    /// through the authority: the node adopts that epoch (see
    /// [`ShardStanding::accept_ack`]) and steps down from any term it holds
    /// or contests, whatever the terms, which two epochs do not order. A
    /// pending joiner that a stale JOIN pointer left on the old epoch finds
    /// its way the same way. A leader of the authority's epoch (see
    /// [`Self::leads_its_office_epoch`]) ignores an ack of another
    /// lineage's epoch whatever its number, even after its fence lapsed. An
    /// accepted ack refreshes leader contact, records its leader as
    /// [`Self::known_leader`] and the ack itself for this node's heartbeats
    /// to echo, and returns a node in `LeaderSuspect`, `RollCall` or
    /// `NoQuorum` to `Active` because a leader is reachable again, whatever
    /// term its roll call contests. A `Candidate` or `Leader` of a term
    /// earlier than the ack's steps down to `Active` under the ack's leader;
    /// one of the ack's own term keeps it.
    ///
    /// It also carries the leader's configuration and this node's admission
    /// generations in the leader's roster, which this node adopts as
    /// [`ShardStanding::accept_ack`] says. An ack that names no admission
    /// generation (the leader holds this node as pending, or not at all)
    /// leaves this node's own as they were. A node that adopts a newer
    /// configuration heartbeats its leader at once, so its echo of it
    /// reaches the leader without waiting out a heartbeat interval. A node
    /// that accepts an ack, in whatever state, forgets the roll calls it
    /// answered or made above the ack's term, short of those it voted in (see
    /// `ElectionRound::follow_leader_of`): the live leader disproved the
    /// suspicion they rested on, and that includes a node still `Active` whose
    /// stale contact no tick had yet acted on.
    fn on_leader_ack(&mut self, ack: &Checked<LeaderHeartbeatAck>) {
        // A node back in `Bootstrapping` rejoins through JOIN alone: an ack
        // from a leader of the epoch it left would take it back past the
        // floor it rejoins at, and one validating the pointer it took is no
        // member yet.
        if matches!(
            self.state,
            WorkerState::Bootstrapping | WorkerState::Joining
        ) || ack.shard_id() != self.shard_id
        {
            return;
        }
        let heard = RecoveryEpoch::new(ack.recovery_epoch, ack.recovery_epoch_lineage);
        // Never having joined a shard is `Bootstrapping`, returned above.
        let Some(order) = self.standing.order(heard) else {
            return;
        };
        let later_epoch = match order {
            EpochOrder::Stale => return,
            EpochOrder::Later => true,
            EpochOrder::Mine => false,
        };
        // A leader with an authority leads the epoch it took office at, which
        // no other lineage's epoch displaces, a higher number or lineage included.
        if later_epoch && self.leads_its_office_epoch(heard.lineage) {
            return;
        }
        if !later_epoch && ack.term < self.ack_floor() {
            return;
        }
        let outpaced = self
            .term_in_play()
            .is_some_and(|term| later_epoch || ack.term > term);
        if outpaced {
            self.leave_office();
        }
        let change = self.standing.accept_ack(
            heard,
            ack.term,
            ack.configuration(),
            ack.recipient_admission(),
            ack.recipient_prior_admission(),
        );
        if change.epoch_moved {
            self.forget_election_state();
        }

        let now = self.clock.now();
        self.last_leader_contact = now;
        self.round.end_no_quorum_streak();
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
        if change.generation_changed || change.admission_changed {
            // Heartbeat at once: the echo of the new generation is what
            // commits it (see `Roster::commit_if_confirmed`), and the
            // admission held is what confirms a promise (see
            // `Roster::promise_admission`).
            self.next_heartbeat = None;
        }

        if outpaced
            || matches!(
                self.state,
                WorkerState::LeaderSuspect | WorkerState::RollCall | WorkerState::NoQuorum
            )
        {
            self.round.stop();
            self.drop_recovery();
            self.transition_to(WorkerState::Active);
        }
        // Whatever this node's state: see the forgetting described above.
        self.round.follow_leader_of(ack.term);
    }

    /// Whether this node leads an epoch of a lineage other than `lineage`.
    /// It leads the epoch it took office at from the moment it took office,
    /// before any grant. Once the authority grants the fence, that epoch is the
    /// one the authority holds (it grants only at the epoch it holds). The node
    /// keeps that standing after the fence lapses, by time or by a flush of the
    /// authority, until it leaves leadership. An ack of a later epoch of its
    /// own lineage ends it too, by taking the node out of office. A node with
    /// no authority has none.
    fn leads_its_office_epoch(&self, lineage: u64) -> bool {
        self.holds_office()
            && self
                .authority
                .as_ref()
                .and_then(AuthorityStanding::office_epoch)
                .is_some_and(|led| led.lineage != lineage)
    }

    /// Sends this node's leader a heartbeat once a heartbeat
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
            recovery_epoch_seen: self.standing.epoch_number(),
            recovery_epoch_lineage: self.standing.epoch().map_or(0, |epoch| epoch.lineage),
            term_seen: self.standing.highest_term_seen(),
            // Nothing reports this node's capacity yet.
            available_capacity: 0,
            active_task_runs_digest: self.active_runs_digest.clone(),
            shard_id: Some(self.shard_id.clone().into()),
            newest_accepted_ack: self.newest_accepted_ack,
            configuration_generation: self
                .standing
                .configuration()
                .map(|configuration| configuration.generation().into()),
            send_token: now.as_ticks(),
            routing_crawled: self.routing_crawled(),
            admission_generation: self.standing.admission().map(Into::into),
            runs_compaction: self.runs_compaction,
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

    /// Records a connection to `peer`. A leader also acks any peer it has
    /// newly connected to, as it did every peer connected when it won (see
    /// [`Self::announce_leadership`]): one that missed that announcement,
    /// cut off at the win or joining since (a restarted process joins under
    /// a new `WorkerId`), would otherwise never learn whom to heartbeat.
    fn on_peer_connected(&mut self, peer: WorkerId) {
        let newly_connected = self.connected.insert(peer.clone());
        if newly_connected && self.holds_office() {
            self.send_ack(peer, None);
        }
    }

    /// Answers a follower's heartbeat with one ack to its
    /// sender, and records which of this leader's acks the heartbeat
    /// confirms. Only a `Leader` answers, and only a heartbeat from its own
    /// shard at its own recovery epoch or an earlier one: a worker left on an
    /// earlier epoch adopts this leader's from the ack, but what it echoes
    /// confirms nothing here. Any heartbeat also tells the leader its sender
    /// is alive (see [`Self::report_lost_workers`]). A sender its roster does not hold is
    /// added as a pending joiner, so its acks name it pending, until the
    /// leader promises it admission and then a batch admits it (see
    /// [`LeaderOffice::take_heartbeat`]); the ack answering this heartbeat
    /// already carries either. Every sender gets an ack,
    /// though only members' confirmations count towards the lease, except
    /// one whose heartbeat names a later term of this leader's epoch: this
    /// leader steps down instead, and neither acks
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
    fn on_heartbeat(&mut self, from: WorkerId, heartbeat: &Checked<WorkerHeartbeat>) {
        let seen =
            RecoveryEpoch::new(heartbeat.recovery_epoch_seen, heartbeat.recovery_epoch_lineage);
        let epoch = self.standing.order(seen);
        if !self.holds_office()
            || heartbeat.shard_id() != self.shard_id
            || matches!(epoch, None | Some(EpochOrder::Later))
        {
            return;
        }
        let same_epoch = epoch == Some(EpochOrder::Mine);
        // A heartbeat naming a later term than this leader's: its sender
        // voted, or heard of a vote, in that term. Some roll call of that term found
        // a returning quorum of stale voters, so this leader is all but
        // deposed, and the sender, whose floor is above this term, can never
        // follow it. A heartbeat from an earlier epoch names a term of
        // another count, and deposes no one.
        if same_epoch && heartbeat.term_seen > self.term {
            self.standing.saw_term(heartbeat.term_seen);
            self.step_down_if_outpaced();
            return;
        }

        let now = self.clock.now();
        let mut heard = Heard::Alive;
        if same_epoch
            && let Some(echo) = heartbeat.newest_accepted_ack
            && echo.term == self.term
            && echo.send_token <= now.as_ticks()
        {
            self.lease
                .confirm(from.clone(), Instant::at(echo.send_token));
            heard = Heard::Confirmed {
                sent_at: Instant::at(echo.send_token),
                held: heartbeat.configuration_generation(),
            };
        }
        if let Some(office) = self.office.as_mut() {
            let duties = Duties {
                me: &self.my_id,
                lease: &self.lease,
                timings: &self.timings,
                now,
            };
            office.take_heartbeat(
                from.clone(),
                heard,
                heartbeat.routing_crawled,
                heartbeat.admission_generation(),
                heartbeat.runs_compaction,
                &duties,
            );
        }
        self.drain_once_free();
        if !self.holds_office() {
            return;
        }
        self.outputs.push(Output::RunsHeard {
            worker: from.clone(),
            digest: heartbeat.active_task_runs_digest.clone(),
        });
        // Only a leader that holds a grant vouches for when it heard the
        // sender: no rival can win until that grant ends (see
        // `Output::AbortDeadline`). The office applied removals pending, as
        // they can end the grant.
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
    /// pending take effect first (see [`LeaderOffice::ack_for`]); a
    /// caller that vouches with `heartbeat_token` applies them before it
    /// decides whether it still holds a grant.
    fn send_ack(&mut self, to: WorkerId, heartbeat_token: Option<u64>) {
        let Some(office) = self.office.as_mut() else {
            return;
        };
        let content = office.ack_for(&to);
        self.send_ack_with(to, content, heartbeat_token);
    }

    /// Sends `to` an ack carrying `content` from this leader.
    fn send_ack_with(&mut self, to: WorkerId, content: AckContent, heartbeat_token: Option<u64>) {
        let ack = LeaderHeartbeatAck {
            shard_id: Some(self.shard_id.clone().into()),
            leader_id: Some(self.my_id.clone().into()),
            recovery_epoch: self.standing.epoch_number(),
            term: self.term,
            configuration: Some((&content.configuration).into()),
            recipient_admission: content.recipient_admission.map(Into::into),
            recipient_prior_admission: content.recipient_prior_admission.map(Into::into),
            send_token: self.clock.now().as_ticks(),
            heartbeat_token,
            recovery_epoch_lineage: self.standing.epoch().map_or(0, |epoch| epoch.lineage),
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
        let Some(office) = self.office.as_mut() else {
            return;
        };
        for (peer, content) in office.announce(&self.connected) {
            self.send_ack_with(peer, content, None);
        }
    }

    /// What the lease reads of this node while it leads: `None` unless it
    /// is `Leader`, and, with an authority, holds the recovery fence (design
    /// 4.5), whose end also ends its grant.
    fn office(&self) -> Option<Office<'_>> {
        if !self.holds_office() {
            return None;
        }
        let fence_end = match &self.authority {
            None => LeaseEnd::Unbounded,
            Some(authority) => LeaseEnd::At(authority.fence_valid_until()?),
        };
        Some(Office {
            me: &self.my_id,
            roster: self.office.as_ref()?.roster(),
            term: self.term,
            recovery_epoch: self.standing.epoch()?,
            fence_end,
        })
    }

    /// Reports this node's grant and abort deadline where they differ from
    /// the ones last reported.
    fn report_lease_changes(&mut self) {
        // The lease runs from the win, exactly as a leader's, but the
        // scheduler is handed a grant only once the node leads.
        let grant = self.lease.grant(self.office().as_ref(), &self.timings);
        let changes = self.lease.report(
            grant,
            self.state == WorkerState::Leader,
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

    /// The earliest term whose leader's acks this node accepts: the highest
    /// term it has seen, or, while it stands or leads, its own term if
    /// later. A candidate never follows an earlier term's leader while its
    /// candidacy can still win; once that lapses unwon, it can.
    fn ack_floor(&self) -> u64 {
        // A leader's own term is already its term seen (see
        // `Self::take_office`); the arm only keeps that from resting on it.
        match self.state {
            WorkerState::Candidate | WorkerState::LeaderReconciling | WorkerState::Leader => {
                self.standing.highest_term_seen().max(self.term)
            }
            _ => self.standing.highest_term_seen(),
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

    /// Moves to `NoQuorum`: the node's quorum is out
    /// of reach, so it waits for its peers to return. Every jittered
    /// suspicion timeout it tries a roll call again; meanwhile it answers
    /// the roll calls and grants the votes of others, and an ack from a
    /// leader returns it to `Active`. With an authority, the authority path
    /// can take it out too (see [`AuthorityStanding::begin_recovery`]).
    /// A leader that has not heard from a quorum within its quorum-contact
    /// lease gives up leading, to `NoQuorum`.
    fn lose_quorum_if_its_lease_ended(&mut self) {
        if !self.holds_office() {
            return;
        }
        let now = self.clock.now();
        let Some(office) = self.office.as_mut() else {
            return;
        };
        let duties = Duties {
            me: &self.my_id,
            lease: &self.lease,
            timings: &self.timings,
            now,
        };
        if office.has_lost_quorum(&duties) {
            self.lose_quorum();
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
    fn on_message(&mut self, from: WorkerId, message: CheckedMessage) {
        if matches!(
            self.state,
            WorkerState::Draining | WorkerState::Stopped | WorkerState::Fenced
        ) {
            return;
        }

        match message.into_payload() {
            Some(CheckedPayload::Heartbeat(heartbeat)) if heartbeat.worker_id() == from => {
                self.on_heartbeat(from, &heartbeat);
            }
            Some(CheckedPayload::HeartbeatAck(ack)) if ack.leader_id() == from => {
                self.on_leader_ack(&ack);
            }
            Some(CheckedPayload::RollCall(call)) if call.initiator_id() == from => {
                let now = self.clock.now();
                self.decide(|round, view| round.on_roll_call(view, from, &call, now));
            }
            Some(CheckedPayload::RollCallReply(reply)) if reply.responder_id() == from => {
                self.decide(|round, view| round.on_roll_call_reply(view, from, &reply));
            }
            Some(CheckedPayload::VoteRequest(req)) if req.candidate_id() == from => {
                self.decide(|round, view| round.on_vote_request(view, from, &req));
            }
            Some(CheckedPayload::VoteGrant(grant)) if grant.voter_id() == from => {
                self.decide(|round, view| round.on_vote_grant(view, from, &grant));
            }
            Some(CheckedPayload::ElectionReject(reject)) if reject.rejecter_id() == from => {
                self.on_election_reject(&reject);
            }
            Some(CheckedPayload::SelfRemove(msg)) if msg.worker_id() == from => {
                self.on_self_remove(&from, &msg);
            }
            Some(CheckedPayload::ElectionCertificate(certificate))
                if certificate.leader_id() == from =>
            {
                self.on_election_certificate(&from, &certificate);
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
        if !self.holds_office()
            || msg.shard_id() != self.shard_id
            || msg.term_seen > self.term
            || msg.leader_term != self.term
        {
            return;
        }
        if let Some(office) = self.office.as_mut() {
            office.take_removal(departing.clone());
        }
        self.drain_once_free();
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
                Verdict::Publish(call) => {
                    self.drop_recovery();
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
                    self.standing.saw_term(grant.term);
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
                    let now = self.clock.now();
                    // The authority path ends where the roll call's retry
                    // would have come.
                    let retry_at = self.round.roll_call_due(now);
                    if let Some(authority) = self.authority.as_mut() {
                        let call = authority.begin_recovery(
                            term,
                            configuration,
                            respondents,
                            now,
                            retry_at,
                        );
                        self.outputs.push(Output::Authority(call));
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
                if self.state == WorkerState::RollCall
                    && let Some((leader, term)) = reject.named_leader()
                    && leader != self.my_id
                {
                    self.leader = Some((leader, term));
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
        if self.state == WorkerState::RollCall
            && let Some((leader, term)) = reject.named_leader()
            && (term >= self.standing.highest_term_seen()
                || reject.reason() == ElectionRejectReason::LeaderStillValid)
            && leader != self.my_id
        {
            self.leader = Some((leader, term));
        }
        if let Some(offered) = offered {
            self.adopt_relayed_configuration(offered);
        }
        self.step_down_if_outpaced();
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
        if matches!(
            self.state,
            WorkerState::Bootstrapping | WorkerState::Joining
        ) || certificate.shard_id() != self.shard_id
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
        self.lease.orphaned(now, &self.timings);
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

    /// Takes office as leader of `roster` in this node's current term, in
    /// `LeaderReconciling`: holds its configuration and its own admission
    /// generations there, commits it at once if it alone is a majority of
    /// each side, starts its quorum-contact lease and its watch over the
    /// workers it leads, records itself as its own leader in place of any it
    /// followed before, asks its driver to reconcile (see
    /// [`Output::Reconcile`]), and announces itself to every connected peer
    /// (see [`Self::announce_leadership`]), unless a drain kept from before
    /// stops it first. With an authority it needs a fence to act; one it
    /// already holds is kept. Every election duty runs from here; only the
    /// scheduler's grant waits for [`Input::Reconciled`].
    fn take_office(&mut self, roster: Roster) {
        let now = self.clock.now();
        // A leader never acks itself, so nothing else raises its own
        // `highest_term_seen` to the term it won.
        self.standing.saw_term(self.term);
        let duties = Duties {
            me: &self.my_id,
            lease: &self.lease,
            timings: &self.timings,
            now,
        };
        self.office = Some(LeaderOffice::take(roster, self.term, &duties));
        self.lost_while_reconciling.clear();

        self.lease.won(now);
        if let Some(authority) = self.authority.as_mut() {
            authority.took_office(self.standing.epoch(), now);
        }
        // The leader it followed before is replaced. Should this node lose
        // its quorum and go back to electing, it must not heartbeat that
        // leader again.
        self.leader = Some((self.my_id.clone(), self.term));
        self.newest_accepted_ack = None;
        self.next_heartbeat = None;
        self.transition_to(WorkerState::LeaderReconciling);
        // A drain kept from before may have drained the node already.
        if self.state == WorkerState::LeaderReconciling {
            if let Some(office) = self.office_term() {
                self.outputs.push(Output::Reconcile(office));
            }
            self.announce_leadership();
        }
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
                .saturating_add(self.timings.reconnect_timeout.as_ticks()),
        )
    }

    /// Reports every worker this leader has not heard from for a suspicion
    /// timeout and a reconnect timeout as lost, once each.
    fn report_lost_workers(&mut self) {
        let now = self.clock.now();
        let lost_after = self.lost_after();
        let suspect_timeout = self.timings.suspect_timeout;
        let Some(office) = self.office.as_mut() else {
            return;
        };
        for worker in office.lost_by(
            now,
            lost_after,
            suspect_timeout,
        ) {
            if self.state == WorkerState::LeaderReconciling {
                self.lost_while_reconciling.insert(worker.clone());
            }
            self.outputs.push(Output::WorkerLost(worker));
        }
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
