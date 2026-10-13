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
//! only the leader holds a [`Roster`](crate::configuration::Roster) of the members.
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
//! A leader can also hand its leadership over without leaving (see
//! [`Input::TransferLeadership`]): it stops granting and vouching, tells every
//! peer so on its acks, and asks the voter that confirmed its acks last to
//! stand for election at once. Meanwhile it takes part in elections like a
//! follower, so a shard whose other voters are few can still elect its
//! successor, and granting a later term deposes it. A follower that accepts
//! such an ack keeps its old leader-contact time and any roll call it runs,
//! and refuses no roll call for a leader that is valid. If no leader of a
//! later term is heard within one suspicion timeout, the hand-over ends and
//! the leader leads again.
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
mod transfer;

use std::collections::BTreeSet;

pub use authority::{
    AuthorityCall, AuthorityReply, AuthorityRequest, AuthorityTimings, CallKind, Issuer,
    ReplyToken, ReplyTokens,
};
pub use carry_out::{AuthorityPerformer, DropMessages, MessageSink, NoAuthority, carry_out};
pub use entry::{Entry, Identity};
pub use lease::AbortBy;

use crate::configuration::{Configuration, Generation};
use crate::coordination_authority::ShardRecord;
use crate::hashing::HashFunction;
use crate::protocol::checked::CheckedMessage;
use crate::protocol::ids::{IdGenerator, IncarnationId, ShardId, TaskRunId, WorkerId};
use crate::protocol::messages::{AckEcho, ElectionMessage, JoinResponse};
use crate::protocol::worker_state::WorkerState;
use crate::reconcile::ReconcileTerm;
use crate::scheduler::{LeadershipGrant, Observer, Scheduler};
use crate::time::{Clock, Duration, Instant};

use authority_standing::AuthorityStanding;
use cancelled_runs::CancelledRunsLedger;
use drain::DrainRequest;
use election_round::ElectionRound;
use leader_office::LeaderOffice;
use lease::Lease;
use self_report::SelfReport;
pub use standing::JoinFloor;
use standing::ShardStanding;
use transfer::Transfer;
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
    /// The hand-over this leader has under way, if any (see
    /// [`Input::TransferLeadership`]).
    transfer: Option<Transfer>,
    /// Whether the newest ack this node accepted said its leader is handing
    /// over: that leader no longer counts as valid against a roll call.
    leader_draining: bool,
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
    /// (see [`ElectionTimings::drain_wait_limit`]), and then hands its
    /// leadership over to the voter that confirmed its acks last (see
    /// [`Input::TransferLeadership`]). It leaves through the leader that
    /// takes over once that leader vouches for hearing it, or, if none does
    /// within a suspicion timeout of the hand-over, as it would alone. A
    /// leader with no other voter to hand over to keeps leading, so a pending
    /// member can be admitted and become one, until `drain_wait_limit` has
    /// passed, and then leaves alone. From `Draining` or `Stopped` the request does
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
    /// Hand leadership over without leaving: a `Leader` stops granting, acks
    /// every connected peer saying so, and asks the voter of its committed
    /// configuration that confirmed its acks last to stand for election at
    /// once (see [`Output::TransferLeadership`]). It keeps every other duty
    /// of its office, answers roll calls and grants votes, and follows the
    /// winner once it hears its ack; if no leader of a later term is heard
    /// within one suspicion timeout it leads again. Ignored in any other
    /// state, while a hand-over is under way, under a joint configuration, and
    /// by a leader with no other voter.
    TransferLeadership,
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
    /// This leader stopped granting and asked `to` to stand for election at
    /// once (see [`Input::TransferLeadership`]). Reported in the step that
    /// sends `to` the request, once per hand-over.
    TransferLeadership { to: WorkerId },
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
            | Output::TransferLeadership { .. }
            | Output::ShardAbandoned => {}
        }
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
