//! `Net`: `core::transport::PeerMessenger` implemented on top of the swarm
//! built by `crate::swarm`.
//!
//! ## `WorkerId` <-> `PeerId` mapping
//!
//! A `WorkerId` *is* the base58 string form of the node's libp2p `PeerId`
//! (`PeerId::to_string()` / `PeerId::from_str()`, which round-trip via
//! `bs58`/multihash — see `libp2p_identity::PeerId`'s `Display`/`FromStr`
//! impls). This avoids standing up a second identity system: whatever
//! process holds the keypair that produced a `PeerId` is, by construction,
//! the only process that can act as that `WorkerId`.
//!
//! ## Synchronization: a background driver task, not a locked `Swarm`
//!
//! `PeerMessenger`'s methods take `&self`, but driving a libp2p `Swarm`
//! needs `&mut self` (`swarm.select_next_some()`). `Net::new` hands the
//! `Swarm` to a dedicated tokio task (`drive`) that owns it exclusively and
//! polls it continuously; `Net` itself holds only a command channel (for
//! `send`, `dial`, `listen_on`) and two small `Mutex`-guarded snapshots
//! (`inbox`, `connected`) that the driver task keeps current as it observes
//! swarm events. Trait methods either push a fire-and-forget command onto
//! the channel (`send`) or take a quick, non-blocking lock on a snapshot
//! (`poll_inbox`, `reachable_peers`) — neither ever waits on the driver task,
//! so there is no risk of a `&self` caller deadlocking against it.
//!
//! This is simpler than a `Mutex<Swarm>`: the driver never awaits while
//! holding a lock (because it never takes one over the swarm at all), and a
//! `&self` caller never blocks waiting for the swarm to become available.
//! It also leaves room for chunk C3: the driver task is where more command
//! variants and more swarm-event handling naturally go as the real
//! `WorkerNode` driver loop needs more from the network (dialing discovered
//! peers, reacting to disconnects, and so on), without changing
//! `PeerMessenger`'s synchronous, non-blocking shape.
//!
//! ## Response shape
//!
//! `send`/`poll_inbox` never correlate a request with its response —
//! `PeerMessenger::send` documents delivery as unreliable and
//! fire-and-forget. The driver answers every inbound request with
//! `codec::Ack` immediately after queuing it, purely so the substream closes
//! cleanly on the sender's side; nothing here ever reads that ack back.
//!
//! ## The bootstrap join protocol (chunk C4)
//!
//! `/kabudachi/join/1` (see `crate::join_codec`) is a genuine correlated
//! request/response, unlike the election protocol above, so it needs its own
//! machinery: [`Net::join_via_seeds`] (the joining side — dial each seed in
//! order, first `JOIN_RESPONSE` wins) and [`Net::poll_join_requests`] /
//! [`Net::respond_join`] (the answering side, mirroring `poll_inbox`'s
//! drain-then-caller-decides shape, since unlike an election heartbeat ack a
//! join response needs information — the current electorate — that only
//! `core::election::WorkerNode` has; see `crate::driver::run_driver`, which
//! calls both). [`Net::peer_addresses`] and [`Net::local_multiaddr`] expose
//! what `Net` knows about addresses, the other half of what a join response
//! needs.
//!
//! ## The claim arbitration protocol (chunk C6)
//!
//! `/kabudachi/claim/1` (see `crate::claim_codec`) is the same shape of
//! genuine correlated request/response as join, so it gets the same
//! machinery: [`Net::request_claim`] (the asking side — a follower already
//! knows exactly which single peer it wants to ask, unlike join's
//! seed-cascade, so there's no `*_via_*` wrapper above it) and
//! [`Net::poll_claim_requests`] / [`Net::respond_claim`] (the answering side;
//! see `crate::driver::respond_to_claim_requests`, which calls both and, like
//! the join responder, needs information — here, `core::scheduler::Scheduler`'s
//! decision — that `Net` alone doesn't have).

//! ## Reconnect/backoff (chunk C8)
//!
//! Spec decision 2.b's other half (`reachable_peers` promptly reflecting a
//! drop was already true since C2 -- see `Self::reachable_peers`'s doc):
//! nothing before this chunk ever automatically retried a connection that
//! dropped. `RedialPolicy` (see its own doc) governs a bounded,
//! exponential-backoff redial the `drive` task now runs on its own, using the
//! address `Self::peer_addresses` already retains for a disconnected peer.
//!
//! **libp2p has no built-in retry to defer to here** -- checked against the
//! actual pinned `libp2p-swarm` 0.48.0 source (the same discipline C4's and
//! C7's fix rounds established): `libp2p_swarm::dial_opts::DialOpts` (and its
//! `PeerCondition`) only configure a *single* dial attempt's addresses,
//! concurrency and precondition-to-dial-at-all; nothing in `libp2p-swarm`
//! 0.48.0 schedules a *retry over time* after a dial or an established
//! connection fails (`grep -rniE "backoff|retry|redial"` across that crate's
//! entire `src/` tree returns zero matches). So this chunk's redial loop is
//! entirely `net`-side, built the same way `drive`'s existing command/event
//! loop already is.
//!
//! **Which drops are redial-eligible.** A peer whose connection this `Net`
//! itself asked to close (`Self::disconnect`) is *not* auto-redialed: an
//! explicit local hangup is a decision, not a failure to recover from, and
//! -- checked empirically, not assumed -- the *active* closing side is the
//! only side that can reliably tell the difference at all. A quick swarm
//! -level probe during this chunk's development (two real swarms, one calls
//! `Swarm::disconnect_peer_id` on the other) showed the active closer's own
//! `SwarmEvent::ConnectionClosed` reports `cause: None`, but the *passive*
//! side being disconnected sees `cause: Some(IO(..Closed..))` -- indistinguishable
//! from a genuine transport failure at that event alone. So only the side
//! that actually issued the local `Command::Disconnect` excludes that peer
//! (tracked in `drive`'s own `locally_disconnected` set, cleared once the
//! peer reconnects by any means); the passive side of an explicit disconnect,
//! like any other organic drop, is redial-eligible by design -- this is what
//! keeps the feature real (a production driver benefits from it without
//! opting in) rather than a no-op default.
//!
//! **Why that doesn't destabilize `ring_roll_call_leader_loss_test.rs`
//! (chunk C7).** That test's *followers* are exactly the passive side of the
//! leader's own `Net::disconnect` calls, so they are redial-eligible under
//! this design — and an early draft of `RedialPolicy::default` (a 2s
//! `initial_backoff`, reasoned as "well past that test's typically-sub-2s
//! real convergence") was empirically *wrong*: repeated real runs against
//! that draft did occasionally reconnect a surviving follower to the
//! isolated ex-leader while phase 2's election was still in flight, tripping
//! that test's `"the disconnected old leader must never appear as a hop
//! recipient"` assertion (absorbed by that test's own retry loop, so it
//! never outright failed the suite, but it was a real, new failure mode —
//! see task-C8-report.md for the actual failing output this caught). The
//! fix is not a special case for that one test: `initial_backoff` (10s) is
//! chosen to exceed every `suspect_timeout` tier any `net/tests/*.rs` file
//! configures (longest: `BYSTANDER_SUSPECT_TIMEOUT_MS` = 8s), on the general
//! principle that redial is a slow, best-effort *background* repair
//! mechanism and must never race or preempt the primary, much faster
//! election/quorum recovery path — a redial that could plausibly land
//! mid-election defeats that purpose regardless of which specific test
//! happens to notice it. (An earlier version of this doc also claimed
//! `initial_backoff` exceeds every `net/tests/*.rs` file's `TEST_TIMEOUT`
//! constant, "all 10s or less" — that was false (`bootstrap_self_elect_test.rs`,
//! `claim_arbitration_test.rs`, and `two_node_election_test.rs` use 20s, and
//! `three_node_join_test.rs` uses 30s) and has been removed. It was also
//! never the right thing to compare against in the first place:
//! `TEST_TIMEOUT` is an assertion-patience ceiling on a `timeout()`-wrapped
//! wait, not a bound on how long a real election/convergence phase takes, so
//! matching it establishes nothing about redial-vs-election-timing races —
//! unlike the `suspect_timeout`-tier comparison above, which does.)
//! `ring_roll_call_survives_real_leader_loss_across_five_nodes` was re-run
//! repeatedly against both the broken 2s draft (to confirm the failure mode
//! really is caused by this chunk, not pre-existing) and the fixed 10s
//! default (to confirm it's gone) — see task-C8-report.md for both sets of
//! results. `Self::new_with_redial_policy` exists so this chunk's own tests
//! (and any future caller with different needs) can use much shorter,
//! test-scale parameters without touching that default.
//!
//! ## Where a peer's address comes from (final-review finding I1)
//!
//! `peer_addresses` is the address of record this node hands a joining node
//! in a `JOIN_RESPONSE` (`crate::driver::respond_to_join_requests`), so every
//! entry in it has to be an address something can actually *dial*. Not every
//! address a swarm event carries is: `ConnectedPoint::get_remote_address`
//! (libp2p-core 0.44.0, `src/connection.rs`) returns the address this node
//! dialed for `ConnectedPoint::Dialer`, but `send_back_addr` for
//! `ConnectedPoint::Listener` — and `send_back_addr` is the *dialer's
//! ephemeral source address*, which is generally not an address anything can
//! connect back to. Until the final whole-branch review, every connection's
//! address was recorded from `get_remote_address` regardless of direction, so
//! a member this node only ever learned about through an *inbound* connection
//! was advertised in `JOIN_RESPONSE` at an un-dialable address.
//!
//! `identify::Behaviour` has been in the swarm since chunk C1 (spec decision
//! 2: peer address exchange) but no `handle_event` arm read its events.
//! `identify::Event::Received`'s `info.listen_addrs` is exactly the peer's own
//! advertised listen addresses, so it is now the preferred source, ranked
//! above both endpoint-derived ones by [`AddressSource`]: `Identify` >
//! `DialedAddress` (a `ConnectedPoint::Dialer` address, dialable by
//! construction, since this node just dialed it) > `InboundRemote` (a
//! `send_back_addr`, kept only as the pre-existing fallback for a peer whose
//! Identify exchange has not completed yet). A new observation replaces the
//! stored one whenever its source ranks at least as high, so a fresher
//! Identify or a fresher successful dial still wins over a stale one of the
//! same kind.
//!
//! Best-effort, deliberately: `listen_addrs` entries with an unspecified IP
//! (`0.0.0.0`/`::`) are skipped as undialable, and the first remaining one is
//! taken. On a multi-homed host that may be an address the particular asking
//! peer cannot route to — a general address-selection problem this phase does
//! not try to solve, consistent with `Self::peer_addresses`'s existing
//! "best-effort and address-of-record only" contract.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::str::FromStr;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration as StdDuration;

use kabudachi_core::protocol::ids::{TaskId, WorkerId};
use kabudachi_core::protocol::messages::{
    ClaimRequest, ClaimResponse, ElectionMessage, JoinRequest, JoinResponse,
};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::transport::PeerMessenger;
use libp2p::core::ConnectedPoint;
use libp2p::core::transport::ListenerId;
use libp2p::futures::StreamExt;
use libp2p::multiaddr::Protocol;
use libp2p::request_response::{self, OutboundRequestId, ResponseChannel};
use libp2p::swarm::dial_opts::DialOpts;
use libp2p::swarm::{ConnectionId, SwarmEvent};
use libp2p::{Multiaddr, PeerId, Swarm, identify};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::codec::Ack;
use crate::swarm::{Behaviour, BehaviourEvent};

/// How long [`Net::join_via_seeds`] waits, per seed, for a connection and
/// then a `JOIN_RESPONSE` before moving on to the next seed.
pub const DEFAULT_JOIN_SEED_TIMEOUT: StdDuration = StdDuration::from_secs(10);

/// Where a `peer_addresses` entry came from, and so how far it can be trusted
/// to be an address anything can dial. Ordered worst to best: `Ord` *is* the
/// precedence rule — see the module doc's "Where a peer's address comes from".
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum AddressSource {
    /// A `ConnectedPoint::Listener`'s `send_back_addr`: the remote's ephemeral
    /// source address, usually *not* dialable. Kept only as a fallback for a
    /// peer whose Identify exchange has not completed yet.
    InboundRemote,
    /// A `ConnectedPoint::Dialer`'s address: one this node itself successfully
    /// dialed, so dialable by construction.
    DialedAddress,
    /// One of `identify::Event::Received`'s `info.listen_addrs`: the peer's own
    /// advertised listen address, the only source that is right by definition
    /// rather than by circumstance.
    Identify,
}

/// One peer's address of record plus the [`AddressSource`] it came from.
#[derive(Clone, Debug)]
struct KnownAddress {
    addr: Multiaddr,
    source: AddressSource,
}

/// Records `addr` for `peer` unless a strictly better-ranked source is already
/// on file. An equal rank *does* overwrite, so a fresher observation of the
/// same kind (a re-dial to a new address, a later Identify) still wins.
fn record_peer_address(
    peer_addresses: &Arc<Mutex<BTreeMap<PeerId, KnownAddress>>>,
    peer: PeerId,
    addr: Multiaddr,
    source: AddressSource,
) {
    let mut addresses = peer_addresses
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    if addresses
        .get(&peer)
        .is_some_and(|known| known.source > source)
    {
        return;
    }
    addresses.insert(peer, KnownAddress { addr, source });
}

/// Whether an address advertised by Identify is worth recording: an
/// unspecified IP (`0.0.0.0`/`::`) is a wildcard bind, never a destination.
fn is_dialable_listen_addr(addr: &Multiaddr) -> bool {
    addr.iter().all(|protocol| match protocol {
        Protocol::Ip4(ip) => !ip.is_unspecified(),
        Protocol::Ip6(ip) => !ip.is_unspecified(),
        _ => true,
    })
}

/// Chunk C8: bounded, exponential-backoff redial policy for a peer that was
/// reachable and dropped without this `Net` itself asking to disconnect it
/// (see the module doc's "Reconnect/backoff" section for why libp2p's own
/// `DialOpts`/`PeerCondition` don't already do this, and which drops are
/// eligible at all).
///
/// On each eligible drop, `drive` schedules a first redial attempt after
/// `initial_backoff`; each subsequent attempt (up to `max_attempts` total)
/// doubles the wait, capped at `max_backoff`. `check_interval` is how often
/// `drive` polls for a due attempt — coarser than `initial_backoff` wastes
/// time before the first attempt actually fires, so a caller using a short
/// `initial_backoff` (chunk C8's own tests) should also shrink this.
///
/// `Default` picks conservative, production-shaped values, chosen so redial
/// — a slow, best-effort background repair — never races the much faster
/// primary failure-recovery path (election/quorum): `initial_backoff` (10s)
/// exceeds every `suspect_timeout` tier used anywhere in `net/tests/*.rs`
/// (longest: `BYSTANDER_SUSPECT_TIMEOUT_MS` = 8s) — this is the comparison
/// that actually bears on redial-vs-election-timing races (see the module
/// doc's "Why that doesn't destabilize `ring_roll_call_leader_loss_test.rs`"
/// section for the empirical failure this value's predecessor actually
/// caused, and why 10s specifically was chosen instead of guessed, and for
/// why a `TEST_TIMEOUT` comparison — an earlier version of this doc made one
/// — isn't the right argument here). A caller that wants fast
/// test-scale retries (chunk C8's own new tests) should use
/// [`Net::new_with_redial_policy`] instead of [`Net::new`].
#[derive(Debug, Clone, Copy)]
pub struct RedialPolicy {
    /// Delay before the first redial attempt after an eligible drop.
    pub initial_backoff: StdDuration,
    /// Ceiling the doubling backoff never exceeds.
    pub max_backoff: StdDuration,
    /// Total attempts made before giving up on a peer for good (the
    /// "bounded" half of "bounded redial policy" — spec decision 2.b).
    pub max_attempts: u32,
    /// How often `drive` checks for a due attempt.
    pub check_interval: StdDuration,
}

impl Default for RedialPolicy {
    fn default() -> Self {
        Self {
            initial_backoff: StdDuration::from_secs(10),
            max_backoff: StdDuration::from_secs(60),
            max_attempts: 5,
            check_interval: StdDuration::from_millis(500),
        }
    }
}

/// One peer's in-progress redial schedule (`drive`'s own bookkeeping, not
/// exposed outside this module).
struct RedialAttempt {
    attempts_made: u32,
    next_backoff: StdDuration,
    next_attempt_at: tokio::time::Instant,
    addr: Multiaddr,
}

/// Instructions for the driver task. `Net`'s trait methods and setup
/// helpers only ever push onto this channel; they never touch the `Swarm`
/// directly.
enum Command {
    Send {
        to: PeerId,
        message: ElectionMessage,
    },
    Dial {
        addr: Multiaddr,
    },
    /// See `Net::disconnect`. Fire-and-forget, same shape as `Dial`: the
    /// caller learns the outcome (if it cares) by polling `reachable_peers`
    /// afterward, the same way the rest of `Net`'s connection-state surface
    /// works — there is no dedicated "disconnect completed" signal.
    Disconnect {
        peer: PeerId,
    },
    /// Like `Dial`, but the caller wants to know *which* connection this
    /// specific dial produces — see `Net::dial_for_connection`, the only
    /// caller. `opts` carries a `ConnectionId` (`DialOpts::connection_id`)
    /// that libp2p attaches to every `SwarmEvent::ConnectionEstablished` /
    /// `SwarmEvent::OutgoingConnectionError` this dial attempt produces, so
    /// the driver can correlate the outcome to this exact call instead of
    /// guessing from the connected-peer set.
    DialForConnection {
        opts: DialOpts,
        respond_to: oneshot::Sender<Option<PeerId>>,
    },
    ListenOn {
        addr: Multiaddr,
        respond_to: oneshot::Sender<Multiaddr>,
    },
    SendJoinRequest {
        to: PeerId,
        respond_to: oneshot::Sender<Option<JoinResponse>>,
    },
    RespondJoin {
        channel: ResponseChannel<JoinResponse>,
        response: JoinResponse,
    },
    SendClaimRequest {
        to: PeerId,
        request: ClaimRequest,
        respond_to: oneshot::Sender<Option<ClaimResponse>>,
    },
    RespondClaim {
        channel: ResponseChannel<ClaimResponse>,
        response: ClaimResponse,
    },
}

/// An unanswered inbound `/kabudachi/join/1` request, returned by
/// [`Net::poll_join_requests`]. Answer it with [`Net::respond_join`];
/// dropping it unanswered just lets the requester's substream eventually
/// fail with `OutboundFailure` on their side (nothing here relies on that
/// happening).
pub struct JoinRequestHandle {
    from: WorkerId,
    channel: ResponseChannel<JoinResponse>,
}

impl JoinRequestHandle {
    /// The `WorkerId` of whoever sent this join request.
    pub fn from(&self) -> WorkerId {
        self.from.clone()
    }
}

/// An unanswered inbound `/kabudachi/claim/1` request, returned by
/// [`Net::poll_claim_requests`]. Answer it with [`Net::respond_claim`];
/// dropping it unanswered just lets the requester's substream eventually fail
/// with `OutboundFailure` on their side (nothing here relies on that
/// happening) — same contract as [`JoinRequestHandle`].
pub struct ClaimRequestHandle {
    from: WorkerId,
    request: ClaimRequest,
    channel: ResponseChannel<ClaimResponse>,
}

impl ClaimRequestHandle {
    /// The `WorkerId` of whoever sent this claim request.
    pub fn from(&self) -> WorkerId {
        self.from.clone()
    }

    /// The task this request asks permission to run.
    pub fn task_id(&self) -> TaskId {
        self.request.task_id()
    }
}

/// `core::transport::PeerMessenger` on top of a real libp2p swarm. One `Net`
/// represents exactly one worker/node — see the module doc for the
/// `WorkerId <-> PeerId` mapping and the actor pattern behind `&self`.
pub struct Net {
    local_worker_id: WorkerId,
    commands: mpsc::UnboundedSender<Command>,
    inbox: Arc<Mutex<VecDeque<(WorkerId, ElectionMessage)>>>,
    connected: Arc<Mutex<BTreeSet<PeerId>>>,
    /// The best dialable address known for each peer, kept current the same
    /// way `connected` is (see `drive` below) and ranked by where it came from
    /// (see [`AddressSource`] and the module doc's "Where a peer's address
    /// comes from"). Used to compose `JOIN_RESPONSE`s
    /// (`Self::peer_addresses`) and to redial a dropped peer — not otherwise
    /// load-bearing for `PeerMessenger`.
    peer_addresses: Arc<Mutex<BTreeMap<PeerId, KnownAddress>>>,
    /// The most recent address this `Net` was told it is listening on (see
    /// `Self::local_multiaddr`), for including this node's own address in a
    /// `JOIN_RESPONSE` it composes.
    local_addr: Arc<Mutex<Option<Multiaddr>>>,
    /// Inbound `/kabudachi/join/1` requests not yet answered via
    /// `Self::respond_join`.
    join_inbox: Arc<Mutex<VecDeque<JoinRequestHandle>>>,
    /// Inbound `/kabudachi/claim/1` requests not yet answered via
    /// `Self::respond_claim`.
    claim_inbox: Arc<Mutex<VecDeque<ClaimRequestHandle>>>,
    /// Chunk C8: every peer `drive` is currently mid-redial for, and how many
    /// attempts have been made so far — see `Self::redial_attempts`.
    redial_attempts: Arc<Mutex<BTreeMap<PeerId, u32>>>,
    driver: JoinHandle<()>,
}

impl Net {
    /// Takes ownership of `swarm` and spawns the task that drives it. Does
    /// not listen or dial; use `listen_on`/`dial` for that.
    ///
    /// Uses [`RedialPolicy::default`] (chunk C8) for the bounded redial
    /// -with-backoff policy applied to a peer that drops unexpectedly — see
    /// [`Self::new_with_redial_policy`] to use different parameters (e.g.
    /// short, test-scale ones).
    pub fn new(swarm: Swarm<Behaviour>) -> Self {
        Self::new_with_redial_policy(swarm, RedialPolicy::default())
    }

    /// Like [`Self::new`], but with an explicit [`RedialPolicy`] instead of
    /// its default. Exists so a caller with different needs — chiefly, this
    /// crate's own tests, which need much shorter backoff/check intervals
    /// than the conservative production default to stay fast and
    /// deterministic — doesn't have to change what every other `Net::new`
    /// caller gets.
    pub fn new_with_redial_policy(swarm: Swarm<Behaviour>, redial_policy: RedialPolicy) -> Self {
        let local_worker_id = WorkerId::new(swarm.local_peer_id().to_string());
        let (commands, command_rx) = mpsc::unbounded_channel();
        let inbox = Arc::new(Mutex::new(VecDeque::new()));
        let connected = Arc::new(Mutex::new(BTreeSet::new()));
        let peer_addresses = Arc::new(Mutex::new(BTreeMap::new()));
        let local_addr = Arc::new(Mutex::new(None));
        let join_inbox = Arc::new(Mutex::new(VecDeque::new()));
        let claim_inbox = Arc::new(Mutex::new(VecDeque::new()));
        let redial_attempts = Arc::new(Mutex::new(BTreeMap::new()));

        let driver = tokio::spawn(drive(
            swarm,
            command_rx,
            inbox.clone(),
            connected.clone(),
            peer_addresses.clone(),
            local_addr.clone(),
            join_inbox.clone(),
            claim_inbox.clone(),
            redial_attempts.clone(),
            redial_policy,
        ));

        Self {
            local_worker_id,
            commands,
            inbox,
            connected,
            peer_addresses,
            local_addr,
            join_inbox,
            claim_inbox,
            redial_attempts,
            driver,
        }
    }

    /// This `Net`'s own `WorkerId`, per the `WorkerId <-> PeerId` mapping
    /// documented on this module.
    pub fn local_worker_id(&self) -> WorkerId {
        self.local_worker_id.clone()
    }

    /// Starts listening on `addr` and resolves once the driver task
    /// observes the concrete address the OS actually bound (relevant for
    /// `.../tcp/0` ephemeral-port addresses).
    ///
    /// # Panics
    ///
    /// Panics if the driver task has already stopped (i.e. this `Net` was
    /// already dropped and called through anyway), or if it drops the
    /// responder without resolving an address (which only happens if
    /// `Swarm::listen_on` itself rejects `addr`).
    pub async fn listen_on(&self, addr: Multiaddr) -> Multiaddr {
        let (respond_to, response) = oneshot::channel();
        self.commands
            .send(Command::ListenOn { addr, respond_to })
            .expect("the driver task outlives every Net handle that can call listen_on");
        response
            .await
            .expect("Swarm::listen_on rejected the address given to Net::listen_on")
    }

    /// Fire-and-forget dial: initiates a connection attempt and returns
    /// immediately. Failures surface only as a lack of a connection later
    /// (nothing in this chunk retries or reports them) — this mirrors
    /// `PeerMessenger::send`'s own best-effort delivery contract.
    pub fn dial(&self, addr: Multiaddr) {
        let _ = self.commands.send(Command::Dial { addr });
    }

    /// Forcibly closes every connection this `Net` currently has to `peer`,
    /// without touching the driver task itself (chunk C7: "drop its swarm's
    /// connections, not the process" — see `net/src/messenger.rs`'s module
    /// doc and `task-C7-brief.md`). Fire-and-forget, like `dial`: a caller
    /// observes the effect by polling `reachable_peers`, not by awaiting
    /// this call.
    ///
    /// Backed by `libp2p_swarm::Swarm::disconnect_peer_id` (verified against
    /// `libp2p-swarm` 0.48.0's actual source, the version this workspace's
    /// `Cargo.lock` pins — see that method's doc: it closes every established
    /// connection to `peer` by sending each one a graceful `Close` command,
    /// which tears down the real transport connection (the underlying TCP
    /// socket), so the *remote* side observes a genuine disconnect too, not
    /// just a local bookkeeping change. `Swarm::connected_peers()` (which
    /// `Net`'s `connected` snapshot is refreshed from every driver loop
    /// iteration — see `drive` below) does not drop the peer until the pool
    /// actually processes the resulting close, so `reachable_peers` reflects
    /// this asynchronously, the same way it already does for an ordinary
    /// peer-initiated disconnect (see
    /// `reachable_peers_drops_a_peer_once_it_disconnects` below).
    ///
    /// A no-op (silently) if `peer` is not a `WorkerId` this mapping ever
    /// produced, the driver task is gone, or there was no connection to
    /// close — the same "nothing to do" shape as `send`'s `PeerId::from_str`
    /// guard.
    pub fn disconnect(&self, peer: WorkerId) {
        let Ok(peer) = PeerId::from_str(peer.as_str()) else {
            return;
        };
        let _ = self.commands.send(Command::Disconnect { peer });
    }

    /// Dials `addr` and resolves to the `PeerId` of the connection *this
    /// specific dial* establishes — never a connection some other dial (or
    /// an unrelated inbound connection) produced. `None` if the driver task
    /// is gone or this dial attempt fails (`SwarmEvent::OutgoingConnectionError`,
    /// or `Swarm::dial` rejecting it outright).
    ///
    /// Correlation is by `ConnectionId` (`DialOpts::connection_id`), not by
    /// diffing the connected-peer set before/after — see
    /// `Self::try_join_via_seed`, the only caller, and the module doc's
    /// "bootstrap join protocol" section for why that distinction matters:
    /// a late `ConnectionEstablished` for an abandoned dial carries *that*
    /// dial's `ConnectionId`, so it can only ever resolve (or fail to
    /// resolve, if the caller already stopped awaiting it) that dial's own
    /// response channel — it can never be mistaken for a different, later
    /// dial's result.
    async fn dial_for_connection(&self, addr: Multiaddr) -> Option<PeerId> {
        let opts = DialOpts::unknown_peer_id().address(addr).build();
        let (respond_to, response) = oneshot::channel();
        self.commands
            .send(Command::DialForConnection { opts, respond_to })
            .ok()?;
        response.await.ok()?
    }

    /// The most recent address `listen_on` resolved for this `Net`, if any.
    /// Used to include this node's own address in a `JOIN_RESPONSE` it
    /// composes (`crate::driver::run_driver`'s join-request responder).
    pub fn local_multiaddr(&self) -> Option<Multiaddr> {
        self.local_addr
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// The remote address last observed for each currently-known connection,
    /// keyed by the peer's `WorkerId` (see the module doc's `WorkerId <->
    /// PeerId` mapping). Best-effort and address-of-record only: a peer that
    /// disconnects keeps its last-known entry here rather than being removed
    /// (unlike `reachable_peers`, nothing needs this to reflect only *live*
    /// connections — a stale-but-still-valid address is still useful to hand
    /// a joining node to dial).
    pub fn peer_addresses(&self) -> BTreeMap<WorkerId, Multiaddr> {
        self.peer_addresses
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(|(peer, known)| (WorkerId::new(peer.to_string()), known.addr.clone()))
            .collect()
    }

    /// Chunk C8: every peer this `Net` is currently mid-redial for (see
    /// `RedialPolicy`), and how many redial attempts have been made for each
    /// so far. A peer disappears from this map once it either reconnects
    /// (redial succeeded) or exhausts `RedialPolicy::max_attempts` (redial
    /// gave up — the "bounded" half of "bounded redial policy"). Not
    /// load-bearing for `PeerMessenger`; exposed for observability and to
    /// let tests assert on the redial policy directly rather than only
    /// inferring it from `reachable_peers` timing.
    pub fn redial_attempts(&self) -> BTreeMap<WorkerId, u32> {
        self.redial_attempts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(|(peer, attempts)| (WorkerId::new(peer.to_string()), *attempts))
            .collect()
    }

    /// Drains every inbound `/kabudachi/join/1` request not yet answered.
    /// Answer each with `Self::respond_join`.
    pub fn poll_join_requests(&self) -> Vec<JoinRequestHandle> {
        self.join_inbox
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .drain(..)
            .collect()
    }

    /// Answers a join request obtained from `Self::poll_join_requests`.
    /// Fire-and-forget like `send`: if the driver task has already stopped,
    /// there's nowhere for the answer to go, and that's fine to drop.
    pub fn respond_join(&self, handle: JoinRequestHandle, response: JoinResponse) {
        let _ = self.commands.send(Command::RespondJoin {
            channel: handle.channel,
            response,
        });
    }

    /// Sends a `JOIN_REQUEST` to `to` (which must already be connected — see
    /// `Self::try_join_via_seed`, the only caller) and awaits its
    /// `JOIN_RESPONSE`. `None` if the driver task is gone, the request fails
    /// outright (`OutboundFailure`), or the peer disconnects before
    /// answering.
    async fn send_join_request(&self, to: PeerId) -> Option<JoinResponse> {
        let (respond_to, response) = oneshot::channel();
        self.commands
            .send(Command::SendJoinRequest { to, respond_to })
            .ok()?;
        response.await.ok()?
    }

    /// Drains every inbound `/kabudachi/claim/1` request not yet answered.
    /// Answer each with `Self::respond_claim`.
    pub fn poll_claim_requests(&self) -> Vec<ClaimRequestHandle> {
        self.claim_inbox
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .drain(..)
            .collect()
    }

    /// Answers a claim request obtained from `Self::poll_claim_requests`.
    /// Fire-and-forget like `respond_join`: if the driver task has already
    /// stopped, there's nowhere for the answer to go, and that's fine to
    /// drop.
    pub fn respond_claim(&self, handle: ClaimRequestHandle, response: ClaimResponse) {
        let _ = self.commands.send(Command::RespondClaim {
            channel: handle.channel,
            response,
        });
    }

    /// Sends a `REQUEST_CLAIM(task_id)` to `leader` (which must already be
    /// connected, same requirement as `Self::send_join_request`) and awaits
    /// its `ClaimResponse` (an accepted `Claim` or a `ClaimReject`). `None`
    /// if the driver task is gone, the request fails outright
    /// (`OutboundFailure`), or `leader` disconnects before answering.
    ///
    /// Unlike `join_via_seeds`, there is no cascading `*_via_*` wrapper above
    /// this: a caller asking to claim a task already knows exactly which
    /// single peer it believes is leader (see `crate::driver`'s leader
    /// tracking, chunk C6), so there is nothing here to cascade over.
    pub async fn request_claim(&self, leader: WorkerId, task_id: TaskId) -> Option<ClaimResponse> {
        let to = PeerId::from_str(leader.as_str()).ok()?;
        let (respond_to, response) = oneshot::channel();
        self.commands
            .send(Command::SendClaimRequest {
                to,
                request: ClaimRequest {
                    task_id: Some(task_id.into()),
                },
                respond_to,
            })
            .ok()?;
        response.await.ok()?
    }

    /// Bootstrap join (README §27 Phase 2, spec decision 5 step (a)): dials
    /// each of `seeds` in order and, for the first one that yields a
    /// connection and then a `JOIN_RESPONSE` within `per_seed_timeout`,
    /// returns its membership as a plain `WorkerId` set — the caller hands
    /// this straight to `core::election::WorkerNode::finish_joining`. `None`
    /// if every seed fails or times out.
    ///
    /// Along the way, best-effort dials every member address the response
    /// names (besides the seed itself, already connected) — the practical
    /// reason `JOIN_RESPONSE` carries addresses at all: without this, a
    /// joining node would learn *of* the rest of the electorate but gain no
    /// actual connectivity to it.
    pub async fn join_via_seeds(
        &self,
        seeds: &[Multiaddr],
        per_seed_timeout: StdDuration,
    ) -> Option<BTreeSet<WorkerId>> {
        for seed in seeds {
            if let Some(members) = self.try_join_via_seed(seed.clone(), per_seed_timeout).await {
                return Some(members);
            }
        }
        None
    }

    /// One seed of `Self::join_via_seeds`'s cascade: dial `seed`, wait for
    /// *that dial's own* connection (see `Self::dial_for_connection` — the
    /// resulting peer is identified by the dial's `ConnectionId`, not by
    /// diffing the connected-peer set, so a late connection from an already
    /// -abandoned earlier seed attempt can never be misattributed here),
    /// then send `JOIN_REQUEST` and await the response — both steps bounded
    /// by `per_seed_timeout`.
    async fn try_join_via_seed(
        &self,
        seed: Multiaddr,
        per_seed_timeout: StdDuration,
    ) -> Option<BTreeSet<WorkerId>> {
        let peer = tokio::time::timeout(per_seed_timeout, self.dial_for_connection(seed))
            .await
            .ok()??;

        let response = tokio::time::timeout(per_seed_timeout, self.send_join_request(peer))
            .await
            .ok()??;

        let mut members = BTreeSet::new();
        for member in response.members {
            members.insert(member.worker_id());
            if let Some(addr) = dialable_member_addr(&member.multiaddr) {
                self.dial(addr);
            }
        }
        Some(members)
    }
}

/// The address to dial for a `JOIN_RESPONSE` member, if there is one. The
/// responder sends an empty `multiaddr` for a member it has no address for
/// (see `driver::respond_to_join_requests`), and an empty string parses as
/// an empty `Multiaddr`, so it is skipped explicitly instead of dialed.
fn dialable_member_addr(multiaddr: &str) -> Option<Multiaddr> {
    if multiaddr.is_empty() {
        return None;
    }
    multiaddr.parse().ok()
}

impl Drop for Net {
    fn drop(&mut self) {
        // The driver task holds no state worth flushing on shutdown (an
        // in-flight send is already best-effort); abort it outright rather
        // than negotiating a graceful stop.
        self.driver.abort();
    }
}

impl PeerMessenger for Net {
    fn send(&self, from: WorkerId, to: WorkerId, message: ElectionMessage) {
        debug_assert_eq!(
            from, self.local_worker_id,
            "Net::send called with a `from` other than this Net's own WorkerId — \
             one Net represents exactly one worker"
        );
        let Ok(peer) = PeerId::from_str(to.as_str()) else {
            // Not a WorkerId this mapping ever produced: nowhere to send.
            return;
        };
        // Fire-and-forget, per PeerMessenger::send's contract: if the driver
        // task has already stopped (this Net is concurrently being dropped),
        // there is nowhere for the command to go, and that is fine to drop.
        let _ = self.commands.send(Command::Send {
            to: peer,
            message,
        });
    }

    fn poll_inbox(&self, me: WorkerId) -> Vec<(WorkerId, ElectionMessage)> {
        debug_assert_eq!(
            me, self.local_worker_id,
            "Net::poll_inbox called with a WorkerId other than this Net's own — \
             one Net represents exactly one worker"
        );
        self.inbox
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .drain(..)
            .collect()
    }

    fn reachable_peers(&self, me: WorkerId) -> BTreeSet<WorkerId> {
        debug_assert_eq!(
            me, self.local_worker_id,
            "Net::reachable_peers called with a WorkerId other than this Net's own — \
             one Net represents exactly one worker"
        );
        // Ruling (task-C2-brief.md, "Ruling carried forward from preflight"):
        // this must reflect the swarm's live connected-peer set, not a stale
        // snapshot. `connected` is overwritten from `Swarm::connected_peers()`
        // after every event the driver task observes (see `drive` below), so
        // a disconnect is reflected here as soon as the driver processes it.
        self.connected
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(|peer_id| WorkerId::new(peer_id.to_string()))
            .collect()
    }
}

/// `Net` is not `Clone` (it uniquely owns the driver task's `JoinHandle`, and
/// `Drop` aborts it — see the module doc), but `WorkerNode::new`'s
/// `transport: M` parameter takes its transport by value. Chunk C3's driver
/// loop needs its own handle to poll `poll_inbox` from outside the
/// `WorkerNode` (nothing inside `WorkerNode` ever polls its own inbox — see
/// `core::election`), so a caller needs to keep a `Net` alive outside the
/// node while also giving the node something transport-shaped. Implementing
/// `PeerMessenger` for `&Net` (all of `Net`'s trait methods already take
/// `&self`, so this is a pure forward) lets `WorkerNode<C, &Net, V, A>`
/// borrow one `Net` that the caller keeps, instead of making `Net` itself
/// `Clone` (which would need to decide which handle's `Drop` aborts the
/// shared driver task — a real semantic question this chunk doesn't need to
/// answer).
impl PeerMessenger for &Net {
    fn send(&self, from: WorkerId, to: WorkerId, message: ElectionMessage) {
        Net::send(self, from, to, message)
    }

    fn poll_inbox(&self, me: WorkerId) -> Vec<(WorkerId, ElectionMessage)> {
        Net::poll_inbox(self, me)
    }

    fn reachable_peers(&self, me: WorkerId) -> BTreeSet<WorkerId> {
        Net::reachable_peers(self, me)
    }
}

/// Owns `swarm` exclusively and polls it forever, applying commands and
/// keeping `inbox`/`connected`/`peer_addresses`/`local_addr`/`join_inbox`
/// current for `Net`'s trait/inherent methods to read.
#[allow(clippy::too_many_arguments)]
async fn drive(
    mut swarm: Swarm<Behaviour>,
    mut commands: mpsc::UnboundedReceiver<Command>,
    inbox: Arc<Mutex<VecDeque<(WorkerId, ElectionMessage)>>>,
    connected: Arc<Mutex<BTreeSet<PeerId>>>,
    peer_addresses: Arc<Mutex<BTreeMap<PeerId, KnownAddress>>>,
    local_addr: Arc<Mutex<Option<Multiaddr>>>,
    join_inbox: Arc<Mutex<VecDeque<JoinRequestHandle>>>,
    claim_inbox: Arc<Mutex<VecDeque<ClaimRequestHandle>>>,
    redial_attempts_snapshot: Arc<Mutex<BTreeMap<PeerId, u32>>>,
    redial_policy: RedialPolicy,
) {
    let mut pending_listens: HashMap<ListenerId, oneshot::Sender<Multiaddr>> = HashMap::new();
    let mut pending_join_requests: HashMap<OutboundRequestId, oneshot::Sender<Option<JoinResponse>>> =
        HashMap::new();
    let mut pending_claim_requests: HashMap<
        OutboundRequestId,
        oneshot::Sender<Option<ClaimResponse>>,
    > = HashMap::new();
    let mut pending_dials: HashMap<ConnectionId, oneshot::Sender<Option<PeerId>>> = HashMap::new();

    // Chunk C8: bounded redial-with-backoff bookkeeping (see RedialPolicy's
    // doc and the module doc's "Reconnect/backoff" section).
    //
    // `locally_disconnected` holds peers this Net itself asked to drop via
    // Command::Disconnect — never auto-redialed, since that was a local
    // decision, not a failure. Cleared once the peer reconnects by any
    // means, so a later *organic* drop of the same peer starts fresh.
    let mut locally_disconnected: HashSet<PeerId> = HashSet::new();
    // Every peer currently mid-backoff, working toward RedialPolicy's cap.
    let mut redial_state: HashMap<PeerId, RedialAttempt> = HashMap::new();
    // Tracks the connected set as of the *previous* loop iteration, so a
    // drop can be detected as a diff against the freshly recomputed set
    // below, without needing a dedicated SwarmEvent match arm for it (the
    // driver already recomputes `connected` from swarm.connected_peers()
    // every iteration — see the end of this loop).
    let mut previously_connected: BTreeSet<PeerId> = BTreeSet::new();
    let mut redial_ticker = tokio::time::interval(redial_policy.check_interval);
    redial_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            command = commands.recv() => {
                match command {
                    Some(Command::Send { to, message }) => {
                        swarm.behaviour_mut().request_response.send_request(&to, message);
                    }
                    Some(Command::Dial { addr }) => {
                        let _ = swarm.dial(addr);
                    }
                    Some(Command::Disconnect { peer }) => {
                        let _ = swarm.disconnect_peer_id(peer);
                        // See "Which drops are redial-eligible" in the
                        // module doc: this Net asked for this specific
                        // disconnect, so it must never auto-redial it, and
                        // any already-scheduled attempt (e.g. a previous
                        // organic drop's backoff still pending) is moot now.
                        locally_disconnected.insert(peer);
                        redial_state.remove(&peer);
                    }
                    Some(Command::DialForConnection { opts, respond_to }) => {
                        let connection_id = opts.connection_id();
                        match swarm.dial(opts) {
                            Ok(()) => {
                                pending_dials.insert(connection_id, respond_to);
                            }
                            // Swarm::dial can fail synchronously (e.g. no
                            // addresses survive filtering) without ever
                            // producing a SwarmEvent for this connection_id
                            // — answer inline rather than leaving the
                            // caller to wait out the full timeout.
                            Err(_) => {
                                let _ = respond_to.send(None);
                            }
                        }
                    }
                    Some(Command::ListenOn { addr, respond_to }) => {
                        if let Ok(listener_id) = swarm.listen_on(addr) {
                            pending_listens.insert(listener_id, respond_to);
                        }
                        // On error, dropping `respond_to` closes the channel;
                        // `Net::listen_on`'s awaiter observes that as a panic
                        // with a message pointing at the cause.
                    }
                    Some(Command::SendJoinRequest { to, respond_to }) => {
                        let request_id = swarm.behaviour_mut().join.send_request(&to, JoinRequest {});
                        pending_join_requests.insert(request_id, respond_to);
                    }
                    Some(Command::RespondJoin { channel, response }) => {
                        // Best-effort, like the election Ack below: a channel
                        // that already closed just means the requester
                        // stopped waiting.
                        let _ = swarm.behaviour_mut().join.send_response(channel, response);
                    }
                    Some(Command::SendClaimRequest { to, request, respond_to }) => {
                        let request_id = swarm.behaviour_mut().claim.send_request(&to, request);
                        pending_claim_requests.insert(request_id, respond_to);
                    }
                    Some(Command::RespondClaim { channel, response }) => {
                        // Best-effort, same reasoning as RespondJoin above.
                        let _ = swarm.behaviour_mut().claim.send_response(channel, response);
                    }
                    None => return, // Every Net handle for this swarm was dropped.
                }
            }
            event = swarm.select_next_some() => {
                handle_event(
                    &mut swarm,
                    event,
                    &mut pending_listens,
                    &mut pending_join_requests,
                    &mut pending_claim_requests,
                    &mut pending_dials,
                    &inbox,
                    &peer_addresses,
                    &local_addr,
                    &join_inbox,
                    &claim_inbox,
                );
            }
            _ = redial_ticker.tick() => {
                // Chunk C8: issue whichever scheduled redial(s) are due.
                // Success/failure of any given dial is observed the same
                // way any other dial's is — via the connected-set diff
                // below on a later loop iteration — rather than tracked by
                // ConnectionId: unlike Self::dial_for_connection's
                // unknown-peer-identity case (see that method's doc), a
                // redial always already knows its target PeerId, so a
                // plain PeerId-keyed check against the freshly recomputed
                // connected set (below) is sufficient correlation on its
                // own — no separate pending-dial bookkeeping needed here.
                let now = tokio::time::Instant::now();
                let due: Vec<PeerId> = redial_state
                    .iter()
                    .filter(|(_, attempt)| attempt.next_attempt_at <= now)
                    .map(|(peer, _)| *peer)
                    .collect();
                for peer in due {
                    let Some(attempt) = redial_state.get_mut(&peer) else { continue };
                    if attempt.attempts_made >= redial_policy.max_attempts {
                        // Bounded: this peer's redial budget is spent.
                        redial_state.remove(&peer);
                        continue;
                    }
                    let _ = swarm.dial(
                        DialOpts::peer_id(peer)
                            .addresses(vec![attempt.addr.clone()])
                            .build(),
                    );
                    attempt.attempts_made += 1;
                    attempt.next_backoff =
                        std::cmp::min(attempt.next_backoff * 2, redial_policy.max_backoff);
                    attempt.next_attempt_at = now + attempt.next_backoff;
                }
            }
        }

        let newly_connected: BTreeSet<PeerId> = swarm.connected_peers().copied().collect();

        // Chunk C8: anyone reconnected (by their own redial, a manual
        // Net::dial, or any other means) is done retrying, and no longer
        // counts as "locally disconnected" — a later organic drop of the
        // same peer is judged fresh, not against a stale exclusion.
        redial_state.retain(|peer, _| !newly_connected.contains(peer));
        //
        // Fix round (review finding 1): lifting the `locally_disconnected`
        // exclusion must require an *observed* reconnect — a genuine
        // transition from absent to present across consecutive iterations
        // (`newly_connected.difference(&previously_connected)`) — not merely
        // "not currently in `newly_connected`'s complement" (i.e. "peer is
        // connected"). The latter looked equivalent but wasn't:
        // `Swarm::disconnect_peer_id` (called just above, on
        // `Command::Disconnect`, in this same loop iteration with no `.await`
        // in between) only starts an async close by sending a `Close` command
        // to the connection's task — `swarm.connected_peers()` (what
        // `newly_connected` is read from, right here) doesn't reflect that
        // until the pool processes the resulting event on a *later* poll. So
        // on the very same iteration the disconnect command is handled, the
        // peer is still in `newly_connected`, and the old
        // `retain(|peer| !newly_connected.contains(peer))` evicted it from
        // `locally_disconnected` immediately — before the connection had
        // actually closed — defeating the exclusion for every
        // `Net::disconnect()` call. Requiring the peer to have been absent
        // from the *previous* snapshot first (i.e. a real drop already
        // happened, then a real reconnect) closes that gap while still
        // lifting the exclusion once the peer legitimately reconnects later.
        for peer in newly_connected.difference(&previously_connected) {
            locally_disconnected.remove(peer);
        }

        // Anyone who just dropped (connected last iteration, not now) and
        // wasn't a local Command::Disconnect becomes redial-eligible, using
        // whatever address Self::peer_addresses still has on file for them
        // (see that method's doc: a disconnected peer's last-known address
        // is deliberately retained, exactly for this). No address on file
        // (never connected, or somehow never recorded) means nothing to
        // redial with — skipped, not an error.
        for peer in previously_connected.difference(&newly_connected) {
            if locally_disconnected.contains(peer) {
                continue;
            }
            let Some(addr) = peer_addresses
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get(peer)
                .map(|known| known.addr.clone())
            else {
                continue;
            };
            redial_state.entry(*peer).or_insert_with(|| RedialAttempt {
                attempts_made: 0,
                next_backoff: redial_policy.initial_backoff,
                next_attempt_at: tokio::time::Instant::now() + redial_policy.initial_backoff,
                addr,
            });
        }
        previously_connected = newly_connected.clone();

        // Chunk C8: resync the externally-readable snapshot from
        // redial_state (Self::redial_attempts), the same pattern `connected`
        // itself already uses below.
        *redial_attempts_snapshot
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = redial_state
            .iter()
            .map(|(peer, attempt)| (*peer, attempt.attempts_made))
            .collect();

        *connected.lock().unwrap_or_else(PoisonError::into_inner) = newly_connected;
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_event(
    swarm: &mut Swarm<Behaviour>,
    event: SwarmEvent<BehaviourEvent>,
    pending_listens: &mut HashMap<ListenerId, oneshot::Sender<Multiaddr>>,
    pending_join_requests: &mut HashMap<OutboundRequestId, oneshot::Sender<Option<JoinResponse>>>,
    pending_claim_requests: &mut HashMap<OutboundRequestId, oneshot::Sender<Option<ClaimResponse>>>,
    pending_dials: &mut HashMap<ConnectionId, oneshot::Sender<Option<PeerId>>>,
    inbox: &Arc<Mutex<VecDeque<(WorkerId, ElectionMessage)>>>,
    peer_addresses: &Arc<Mutex<BTreeMap<PeerId, KnownAddress>>>,
    local_addr: &Arc<Mutex<Option<Multiaddr>>>,
    join_inbox: &Arc<Mutex<VecDeque<JoinRequestHandle>>>,
    claim_inbox: &Arc<Mutex<VecDeque<ClaimRequestHandle>>>,
) {
    match event {
        SwarmEvent::NewListenAddr {
            listener_id,
            address,
        } => {
            *local_addr.lock().unwrap_or_else(PoisonError::into_inner) = Some(address.clone());
            if let Some(respond_to) = pending_listens.remove(&listener_id) {
                let _ = respond_to.send(address);
            }
        }
        SwarmEvent::ConnectionEstablished {
            peer_id,
            connection_id,
            endpoint,
            ..
        } => {
            // The two sides of a connection observe different addresses here
            // (see the module doc's "Where a peer's address comes from"):
            // only the dialing side's is dialable. Both are recorded, ranked,
            // so an Identify exchange on this same connection can supersede
            // either — but neither can silently displace a better one.
            let source = match endpoint {
                ConnectedPoint::Dialer { .. } => AddressSource::DialedAddress,
                ConnectedPoint::Listener { .. } => AddressSource::InboundRemote,
            };
            record_peer_address(
                peer_addresses,
                peer_id,
                endpoint.get_remote_address().clone(),
                source,
            );
            // Only ever resolves `Net::dial_for_connection`'s own oneshot
            // for *this* connection_id — see the module doc and that
            // method's doc comment for why matching on ConnectionId (rather
            // than diffing the connected-peer set) is what makes a late
            // connection from an abandoned dial harmless: if the caller
            // already stopped awaiting this entry (timeout elapsed, cascade
            // moved on), `send` below just fails silently instead of
            // resolving some other, unrelated await.
            if let Some(respond_to) = pending_dials.remove(&connection_id) {
                let _ = respond_to.send(Some(peer_id));
            }
        }
        SwarmEvent::Behaviour(BehaviourEvent::Identify(identify::Event::Received {
            peer_id,
            info,
            ..
        })) => {
            // The whole point of having `identify` in the swarm (spec
            // decision 2): `info.listen_addrs` is the peer's own advertised
            // listen address, which is what a joining node needs to dial —
            // unlike either endpoint-derived address above. See the module
            // doc's "Where a peer's address comes from" for the ranking and
            // for why the first non-wildcard entry is taken.
            if let Some(addr) = info
                .listen_addrs
                .into_iter()
                .find(is_dialable_listen_addr)
            {
                record_peer_address(peer_addresses, peer_id, addr, AddressSource::Identify);
            }
        }
        SwarmEvent::OutgoingConnectionError { connection_id, .. } => {
            if let Some(respond_to) = pending_dials.remove(&connection_id) {
                let _ = respond_to.send(None);
            }
        }
        SwarmEvent::Behaviour(BehaviourEvent::RequestResponse(request_response::Event::Message {
            peer,
            message:
                request_response::Message::Request {
                    request, channel, ..
                },
            ..
        })) => {
            inbox
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push_back((WorkerId::new(peer.to_string()), request));
            // Best-effort: nothing reads this ack back (see module doc), and
            // a channel that is already closed just means the peer stopped
            // waiting on it.
            let _ = swarm
                .behaviour_mut()
                .request_response
                .send_response(channel, Ack);
        }
        SwarmEvent::Behaviour(BehaviourEvent::Join(request_response::Event::Message {
            peer,
            message:
                request_response::Message::Request {
                    request: JoinRequest {},
                    channel,
                    ..
                },
            ..
        })) => {
            join_inbox
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push_back(JoinRequestHandle {
                    from: WorkerId::new(peer.to_string()),
                    channel,
                });
        }
        SwarmEvent::Behaviour(BehaviourEvent::Join(request_response::Event::Message {
            message: request_response::Message::Response { request_id, response },
            ..
        })) => {
            if let Some(respond_to) = pending_join_requests.remove(&request_id) {
                let _ = respond_to.send(Some(response));
            }
        }
        SwarmEvent::Behaviour(BehaviourEvent::Join(request_response::Event::OutboundFailure {
            request_id,
            ..
        })) => {
            if let Some(respond_to) = pending_join_requests.remove(&request_id) {
                let _ = respond_to.send(None);
            }
        }
        SwarmEvent::Behaviour(BehaviourEvent::Claim(request_response::Event::Message {
            peer,
            message:
                request_response::Message::Request {
                    request, channel, ..
                },
            ..
        })) => {
            claim_inbox
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push_back(ClaimRequestHandle {
                    from: WorkerId::new(peer.to_string()),
                    request,
                    channel,
                });
        }
        SwarmEvent::Behaviour(BehaviourEvent::Claim(request_response::Event::Message {
            message: request_response::Message::Response { request_id, response },
            ..
        })) => {
            if let Some(respond_to) = pending_claim_requests.remove(&request_id) {
                let _ = respond_to.send(Some(response));
            }
        }
        SwarmEvent::Behaviour(BehaviourEvent::Claim(request_response::Event::OutboundFailure {
            request_id,
            ..
        })) => {
            if let Some(respond_to) = pending_claim_requests.remove(&request_id) {
                let _ = respond_to.send(None);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use kabudachi_core::protocol::ids::{IncarnationId, TaskRunId};
    use kabudachi_core::protocol::messages::{
        Claim, ClaimReject, ClaimRejectReason, JoinMember, WorkerHeartbeat, claim_response,
        election_message,
    };
    use libp2p::identity;
    use tokio::time::timeout;

    use super::*;
    use crate::swarm::build_swarm;

    #[test]
    fn dialable_member_addr_skips_an_empty_address() {
        assert_eq!(dialable_member_addr(""), None);
        assert_eq!(dialable_member_addr("not a multiaddr"), None);
        assert_eq!(
            dialable_member_addr("/ip4/127.0.0.1/tcp/1"),
            Some("/ip4/127.0.0.1/tcp/1".parse().unwrap())
        );
    }

    const TEST_TIMEOUT: Duration = Duration::from_secs(10);

    fn heartbeat_message(worker: &WorkerId) -> ElectionMessage {
        ElectionMessage {
            payload: Some(election_message::Payload::Heartbeat(WorkerHeartbeat {
                worker_id: Some(worker.clone().into()),
                incarnation_id: Some(IncarnationId::new("incarnation-1").into()),
                recovery_epoch_seen: 0,
                term_seen: 0,
                available_capacity: 4,
                active_task_runs_digest: vec![],
            })),
        }
    }

    /// Polls `condition` until it's true. Every call site wraps this in
    /// `tokio::time::timeout` — this alone would spin forever on a bug.
    async fn wait_until(mut condition: impl FnMut() -> bool) {
        loop {
            if condition() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Connects `net_a` and `net_b` over a real loopback TCP socket and
    /// returns each side's `WorkerId`.
    async fn connected_pair(net_a: &Net, net_b: &Net) -> (WorkerId, WorkerId) {
        let listen_addr = timeout(
            TEST_TIMEOUT,
            net_a.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
        )
        .await
        .expect("net_a produced a listen address within the timeout");

        net_b.dial(listen_addr);

        let worker_a = net_a.local_worker_id();
        let worker_b = net_b.local_worker_id();

        timeout(
            TEST_TIMEOUT,
            wait_until(|| net_a.reachable_peers(worker_a.clone()).contains(&worker_b)),
        )
        .await
        .expect("net_a saw net_b as reachable within the timeout");
        timeout(
            TEST_TIMEOUT,
            wait_until(|| net_b.reachable_peers(worker_b.clone()).contains(&worker_a)),
        )
        .await
        .expect("net_b saw net_a as reachable within the timeout");

        (worker_a, worker_b)
    }

    #[tokio::test]
    async fn send_then_poll_inbox_round_trips_a_heartbeat_over_real_sockets() {
        let net_a = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let net_b = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let (worker_a, worker_b) = connected_pair(&net_a, &net_b).await;

        let heartbeat = heartbeat_message(&worker_a);
        net_a.send(worker_a.clone(), worker_b.clone(), heartbeat.clone());

        let received = timeout(TEST_TIMEOUT, async {
            loop {
                let inbox = net_b.poll_inbox(worker_b.clone());
                if !inbox.is_empty() {
                    return inbox;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("net_b's inbox received the heartbeat within the timeout");

        assert_eq!(received, vec![(worker_a, heartbeat)]);
    }

    #[tokio::test]
    async fn reachable_peers_drops_a_peer_once_it_disconnects() {
        // The ruling this proves: reachable_peers must track the swarm's
        // *live* connected-peer set, not a snapshot that's only ever added
        // to. See the "Ruling carried forward from preflight" section of
        // task-C2-brief.md.
        let net_a = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let net_b = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let (worker_a, worker_b) = connected_pair(&net_a, &net_b).await;

        drop(net_b); // aborts net_b's driver task, closing its connection

        timeout(
            TEST_TIMEOUT,
            wait_until(|| !net_a.reachable_peers(worker_a.clone()).contains(&worker_b)),
        )
        .await
        .expect("net_a's reachable_peers dropped net_b after disconnect within the timeout");
    }

    #[tokio::test]
    async fn disconnect_drops_the_peer_from_reachable_peers_on_both_sides() {
        // Unlike `reachable_peers_drops_a_peer_once_it_disconnects` above
        // (which simulates the *other* side going away entirely — process
        // death), this proves the new, genuinely-different chunk-C7
        // capability: closing a connection while both `Net`s, and both
        // driver tasks, stay alive. Checked on both sides (the C2 ruling —
        // "reachable_peers must reflect the swarm's live connected-peer set"
        // — applies symmetrically, and chunk C7's brief calls this out
        // explicitly: the disconnecting side's own view must update too, not
        // just the side that got disconnected).
        let net_a = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let net_b = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let (worker_a, worker_b) = connected_pair(&net_a, &net_b).await;

        net_a.disconnect(worker_b.clone());

        timeout(
            TEST_TIMEOUT,
            wait_until(|| !net_a.reachable_peers(worker_a.clone()).contains(&worker_b)),
        )
        .await
        .expect(
            "net_a's own reachable_peers dropped net_b after net_a.disconnect(worker_b) \
             within the timeout",
        );
        timeout(
            TEST_TIMEOUT,
            wait_until(|| !net_b.reachable_peers(worker_b.clone()).contains(&worker_a)),
        )
        .await
        .expect(
            "net_b also observed the connection close — disconnect_peer_id tears down the \
             real transport connection, not just net_a's local bookkeeping — within the timeout",
        );

        // Both driver tasks are still alive and usable: proves this is a
        // connection-level disconnect, not the process-death simulation
        // above. A fresh heartbeat round-trips fine once reconnected.
        let listen_addr = timeout(
            TEST_TIMEOUT,
            net_a.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
        )
        .await
        .expect("net_a's driver task is still alive and can listen again after disconnecting");
        net_b.dial(listen_addr);
        timeout(
            TEST_TIMEOUT,
            wait_until(|| net_a.reachable_peers(worker_a.clone()).contains(&worker_b)),
        )
        .await
        .expect("net_a's driver task can still accept a fresh connection from net_b");
    }

    /// Chunk C8's short, test-scale [`RedialPolicy`] — orders of magnitude
    /// faster than [`RedialPolicy::default`]'s conservative production
    /// values (see that default's own doc for why it must be slow), so
    /// these tests stay fast without needing that default's safety margin.
    fn short_redial_policy() -> RedialPolicy {
        RedialPolicy {
            initial_backoff: Duration::from_millis(20),
            max_backoff: Duration::from_millis(80),
            max_attempts: 3,
            check_interval: Duration::from_millis(5),
        }
    }

    #[tokio::test]
    async fn auto_redial_reconnects_a_passively_dropped_peer_within_the_bounded_policy() {
        // Chunk C8, "What to build" item 3: a peer that drops gets
        // automatically redialed and reconnects once it's reachable again.
        //
        // net_b is the *active* closer here (it calls disconnect, not
        // net_a), so from net_a's side this is a passive/organic drop — the
        // module doc's "Which drops are redial-eligible" section is exactly
        // why that distinction matters: only net_a (the passive side) is
        // eligible to auto-redial. net_b never stops listening (`disconnect`
        // only closes the connection, not the listener — see that method's
        // own doc), so net_a's redial dialing net_b's cached address (from
        // `peer_addresses`) is expected to actually succeed, simulating "the
        // follower's swarm is reachable again" without needing to tear down
        // and rebuild a whole second swarm.
        //
        // net_a must be the *dialer* here (not the listener, unlike
        // `connected_pair`'s default direction): `peer_addresses` records
        // whatever `ConnectedPoint::get_remote_address` reports for the
        // connection, which for the dialing side is the address it actually
        // dialed (net_b's real, redialable listen address) — for the
        // *listening* side it would instead be the dialer's ephemeral source
        // port, which is never something the other side is listening on and
        // so could never be successfully redialed.
        //
        // The first redial waits a full second, not `short_redial_policy`'s
        // 20ms. The test must observe net_b as unreachable before the redial
        // brings it back, and `wait_until` samples every 10ms; under a loaded
        // test runner a 20ms window was missed about 2 runs in 10, leaving
        // the drop wait below to time out on a drop that had already healed.
        let net_a = Net::new_with_redial_policy(
            build_swarm(identity::Keypair::generate_ed25519()),
            RedialPolicy {
                initial_backoff: Duration::from_secs(1),
                max_backoff: Duration::from_secs(2),
                ..short_redial_policy()
            },
        );
        let net_b = Net::new(build_swarm(identity::Keypair::generate_ed25519()));

        let listen_addr_b = timeout(
            TEST_TIMEOUT,
            net_b.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
        )
        .await
        .expect("net_b produced a listen address within the timeout");
        net_a.dial(listen_addr_b);
        let worker_a = net_a.local_worker_id();
        let worker_b = net_b.local_worker_id();
        timeout(
            TEST_TIMEOUT,
            wait_until(|| net_a.reachable_peers(worker_a.clone()).contains(&worker_b)),
        )
        .await
        .expect("net_a saw net_b as reachable within the timeout");
        // net_b must see the connection too. `disconnect` is a silent no-op
        // on a side that has not registered it yet, and then no drop ever
        // happens (about 2 runs in 10 under Bazel).
        timeout(
            TEST_TIMEOUT,
            wait_until(|| net_b.reachable_peers(worker_b.clone()).contains(&worker_a)),
        )
        .await
        .expect("net_b saw net_a as reachable within the timeout");

        net_b.disconnect(worker_a.clone());

        timeout(
            TEST_TIMEOUT,
            wait_until(|| !net_a.reachable_peers(worker_a.clone()).contains(&worker_b)),
        )
        .await
        .expect("net_a observed the drop within the timeout");

        timeout(
            TEST_TIMEOUT,
            wait_until(|| net_a.reachable_peers(worker_a.clone()).contains(&worker_b)),
        )
        .await
        .expect(
            "net_a's own bounded redial policy reconnected to net_b within the timeout, \
             without any manual dial from the test itself",
        );
    }

    #[tokio::test]
    async fn auto_redial_gives_up_after_max_attempts_against_a_permanently_gone_peer() {
        // Chunk C8, "What to build" item 4: the bounded cap actually bounds
        // — a permanently-gone peer's redial attempts eventually stop,
        // rather than continuing forever.
        let net_a = Net::new_with_redial_policy(
            build_swarm(identity::Keypair::generate_ed25519()),
            short_redial_policy(),
        );
        let net_b = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let (worker_a, worker_b) = connected_pair(&net_a, &net_b).await;

        // Process death, not a local disconnect: net_b is gone for good, and
        // net_a never asked for this drop, so it's organic/redial-eligible
        // (same simulation as reachable_peers_drops_a_peer_once_it_disconnects
        // above) — nothing will ever again answer at net_b's last-known
        // address, so every redial attempt against it must fail.
        drop(net_b);

        timeout(
            TEST_TIMEOUT,
            wait_until(|| net_a.redial_attempts().contains_key(&worker_b)),
        )
        .await
        .expect("net_a started redialing the permanently-gone peer within the timeout");

        timeout(
            TEST_TIMEOUT,
            wait_until(|| !net_a.redial_attempts().contains_key(&worker_b)),
        )
        .await
        .expect(
            "net_a gave up redialing (its RedialPolicy::max_attempts budget was spent) within \
             the timeout, rather than retrying forever",
        );

        assert!(
            !net_a.reachable_peers(worker_a).contains(&worker_b),
            "a permanently-gone peer must never actually reconnect"
        );
    }

    /// Regression test for review finding 1 on this chunk:
    /// `locally_disconnected`'s exclusion wasn't actually enforced.
    /// `swarm.disconnect_peer_id` (called from `Command::Disconnect`) only
    /// starts an async close by sending a `Close` command to the connection's
    /// own task -- it has no synchronous effect on `Swarm::connected_peers()`
    /// -- so on the *same* `drive()` loop iteration `Command::Disconnect` was
    /// processed, the freshly-recomputed `newly_connected` snapshot still
    /// showed the peer connected. The old
    /// `locally_disconnected.retain(|peer| !newly_connected.contains(peer))`
    /// read that stale "still connected" snapshot and evicted the peer's
    /// exclusion entry immediately -- before the connection had actually
    /// closed -- so by the time the real drop was observed on a later
    /// iteration, `locally_disconnected` no longer contained the peer, and it
    /// got scheduled for auto-redial like any organic drop. This proves
    /// `net_a.disconnect(worker_b)` genuinely and durably excludes `worker_b`
    /// from auto-redial: `net_a.redial_attempts()` must never gain an entry
    /// for it, and `net_a` must never reconnect to it on its own.
    #[tokio::test]
    async fn locally_disconnected_peer_is_never_auto_redialed() {
        // net_a needs a short RedialPolicy so a broken exclusion would fire
        // (and this test would fail) well within the assertion window below
        // -- see auto_redial_reconnects_a_passively_dropped_peer_within_the_bounded_policy
        // above, which proves this same policy *does* redial a genuinely
        // organic drop fast, so a locally-disconnected drop surviving the
        // same window is a real property, not just "didn't happen to fire
        // yet".
        let net_a = Net::new_with_redial_policy(
            build_swarm(identity::Keypair::generate_ed25519()),
            short_redial_policy(),
        );
        let net_b = Net::new(build_swarm(identity::Keypair::generate_ed25519()));

        // net_a must be the dialer (same reasoning as
        // auto_redial_reconnects_a_passively_dropped_peer_within_the_bounded_policy
        // above): peer_addresses records the address net_a actually dialed
        // (net_b's real, redialable listen address), so if the exclusion
        // were broken, net_a's redial would have a real address to
        // (wrongly) succeed against -- not fail for an unrelated reason.
        let listen_addr_b = timeout(
            TEST_TIMEOUT,
            net_b.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
        )
        .await
        .expect("net_b produced a listen address within the timeout");
        net_a.dial(listen_addr_b);
        let worker_a = net_a.local_worker_id();
        let worker_b = net_b.local_worker_id();
        timeout(
            TEST_TIMEOUT,
            wait_until(|| net_a.reachable_peers(worker_a.clone()).contains(&worker_b)),
        )
        .await
        .expect("net_a saw net_b as reachable within the timeout");

        net_a.disconnect(worker_b.clone());

        timeout(
            TEST_TIMEOUT,
            wait_until(|| !net_a.reachable_peers(worker_a.clone()).contains(&worker_b)),
        )
        .await
        .expect("net_a observed its own disconnect within the timeout");

        // net_b never stops listening (`disconnect` only closes the
        // connection, not the listener), so nothing but the exclusion itself
        // stops net_a's short_redial_policy from reconnecting here.
        let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
        while tokio::time::Instant::now() < deadline {
            assert!(
                !net_a.redial_attempts().contains_key(&worker_b),
                "net_a must never auto-redial a peer it locally disconnected"
            );
            assert!(
                !net_a.reachable_peers(worker_a.clone()).contains(&worker_b),
                "net_a must never auto-reconnect to a peer it locally disconnected"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Proves `&Net: PeerMessenger` behaves exactly like `Net: PeerMessenger`
    /// (same round trip as `send_then_poll_inbox_round_trips_a_heartbeat_over_real_sockets`,
    /// but every call goes through a `&Net` borrow) — this is what lets a
    /// caller hand `&net` to `WorkerNode::new` as its transport while still
    /// holding `net` itself to drive the inbox (see the impl's doc comment).
    #[tokio::test]
    async fn borrowed_net_round_trips_a_heartbeat_over_real_sockets() {
        fn assert_peer_messenger<M: PeerMessenger>(messenger: M) -> M {
            messenger
        }

        let net_a = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let net_b = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let (worker_a, worker_b) = connected_pair(&net_a, &net_b).await;

        let borrowed_a = assert_peer_messenger(&net_a);
        let borrowed_b = assert_peer_messenger(&net_b);

        let heartbeat = heartbeat_message(&worker_a);
        borrowed_a.send(worker_a.clone(), worker_b.clone(), heartbeat.clone());

        let received = timeout(TEST_TIMEOUT, async {
            loop {
                let inbox = borrowed_b.poll_inbox(worker_b.clone());
                if !inbox.is_empty() {
                    return inbox;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("net_b's inbox received the heartbeat within the timeout, via a &Net borrow");

        assert_eq!(received, vec![(worker_a, heartbeat)]);
    }

    #[tokio::test]
    async fn local_multiaddr_and_peer_addresses_are_populated_after_connecting() {
        let net_a = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let net_b = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let (_worker_a, worker_b) = connected_pair(&net_a, &net_b).await;

        assert!(
            net_a.local_multiaddr().is_some(),
            "local_multiaddr should be populated once listen_on resolves"
        );
        timeout(
            TEST_TIMEOUT,
            wait_until(|| net_a.peer_addresses().contains_key(&worker_b)),
        )
        .await
        .expect("net_a recorded net_b's address within the timeout");
    }

    /// Spawns a background task that answers every `/kabudachi/join/1`
    /// request `net` receives with `response`, mirroring (at the messenger
    /// level, not through `core::election::WorkerNode`) what
    /// `crate::driver::run_driver`'s join responder does in production.
    fn spawn_join_responder(net: Net, response: JoinResponse) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                for handle in net.poll_join_requests() {
                    net.respond_join(handle, response.clone());
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    }

    #[tokio::test]
    async fn join_via_seeds_returns_the_seeds_membership() {
        let net_a = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let net_c = Net::new(build_swarm(identity::Keypair::generate_ed25519()));

        let listen_addr = timeout(
            TEST_TIMEOUT,
            net_a.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
        )
        .await
        .expect("net_a produced a listen address within the timeout");
        let worker_a = net_a.local_worker_id();

        let response = JoinResponse {
            members: vec![JoinMember {
                worker_id: Some(worker_a.clone().into()),
                multiaddr: listen_addr.to_string(),
            }],
        };
        let _responder = spawn_join_responder(net_a, response);

        let members = timeout(
            TEST_TIMEOUT,
            net_c.join_via_seeds(&[listen_addr], Duration::from_secs(5)),
        )
        .await
        .expect("join_via_seeds completed within the test timeout");

        assert_eq!(members, Some([worker_a].into_iter().collect()));
    }

    #[tokio::test]
    async fn join_via_seeds_returns_none_when_the_only_seed_never_responds() {
        let net_c = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        // Nothing listens here, so dialing it fails to connect.
        let unreachable_seed: Multiaddr = "/ip4/127.0.0.1/tcp/1".parse().unwrap();

        let members = timeout(
            TEST_TIMEOUT,
            net_c.join_via_seeds(&[unreachable_seed], Duration::from_secs(2)),
        )
        .await
        .expect("join_via_seeds completed within the test timeout");

        assert_eq!(members, None);
    }

    #[tokio::test]
    async fn join_via_seeds_falls_through_a_non_responding_seed_to_the_next() {
        // Proves spec decision 5 step (a)'s cascade: seeds are dialed in
        // order, and a seed that doesn't answer doesn't stop the join —
        // the next seed in the list still gets a chance.
        let net_a = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let net_c = Net::new(build_swarm(identity::Keypair::generate_ed25519()));

        let listen_addr = timeout(
            TEST_TIMEOUT,
            net_a.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
        )
        .await
        .expect("net_a produced a listen address within the timeout");
        let worker_a = net_a.local_worker_id();

        let response = JoinResponse {
            members: vec![JoinMember {
                worker_id: Some(worker_a.clone().into()),
                multiaddr: listen_addr.to_string(),
            }],
        };
        let _responder = spawn_join_responder(net_a, response);

        let unreachable_seed: Multiaddr = "/ip4/127.0.0.1/tcp/1".parse().unwrap();
        let seeds = vec![unreachable_seed, listen_addr];

        let members = timeout(
            TEST_TIMEOUT,
            net_c.join_via_seeds(&seeds, Duration::from_secs(5)),
        )
        .await
        .expect("join_via_seeds completed within the test timeout");

        assert_eq!(members, Some([worker_a].into_iter().collect()));
    }

    /// Regression test for the bug this fix addresses: `try_join_via_seed`
    /// used to identify "the newly connected peer" for a seed by diffing
    /// the connected-peer set before/after the dial, with no correlation to
    /// the specific dial in progress. If a seed's dial connects *late* —
    /// after its per-seed timeout has elapsed and the cascade has moved on
    /// to the next seed — that late `ConnectionEstablished` could be
    /// misattributed as the *next* seed's peer, and `JOIN_REQUEST` would go
    /// to the wrong node.
    /// `join_via_seeds_falls_through_a_non_responding_seed_to_the_next`
    /// (above) doesn't catch this: its non-responding seed is genuinely
    /// unreachable (nothing listens on that port), so it fails fast
    /// (`OutgoingConnectionError`) rather than ever connecting late.
    ///
    /// This test forces the hazard deterministically where it can be forced
    /// deterministically (seed A's dial is abandoned — its response channel
    /// dropped — exactly as a timed-out `tokio::time::timeout` would do it,
    /// *before* seed B's dial is even issued, so the two are genuinely
    /// in flight concurrently) and documents the one piece that's left to
    /// real, uncontrolled scheduling: whether seed A's real connection
    /// happens to land while seed B's own dial is still pending in
    /// `pending_dials` (likely, since both are driven by the same
    /// single-threaded driver task polling both dials concurrently, but not
    /// something this test can force to happen on every run). The fix makes
    /// the outcome correct either way — seed B's slot always resolves to
    /// seed B's own connection, correlated by that dial's own
    /// `ConnectionId` — which is what this test asserts.
    #[tokio::test]
    async fn join_via_seeds_ignores_a_late_connection_from_an_abandoned_seed() {
        let net_a = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let net_b = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let net_c = Net::new(build_swarm(identity::Keypair::generate_ed25519()));

        let addr_a = timeout(
            TEST_TIMEOUT,
            net_a.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
        )
        .await
        .expect("net_a produced a listen address within the timeout");
        let addr_b = timeout(
            TEST_TIMEOUT,
            net_b.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
        )
        .await
        .expect("net_b produced a listen address within the timeout");

        let worker_a = net_a.local_worker_id();
        let worker_b = net_b.local_worker_id();
        let worker_c = net_c.local_worker_id();

        // Seed A would answer with a membership that's obviously wrong for
        // this test (just itself), so a misattribution is easy to detect.
        let response_a = JoinResponse {
            members: vec![JoinMember {
                worker_id: Some(worker_a.clone().into()),
                multiaddr: addr_a.to_string(),
            }],
        };
        let _responder_a = spawn_join_responder(net_a, response_a);

        // Seed B answers with its own, distinct membership.
        let response_b = JoinResponse {
            members: vec![JoinMember {
                worker_id: Some(worker_b.clone().into()),
                multiaddr: addr_b.to_string(),
            }],
        };
        let _responder_b = spawn_join_responder(net_b, response_b);

        // Simulate `try_join_via_seed` abandoning seed A's dial once its
        // per-seed timeout elapses: issue exactly the command it would
        // (`Net::dial_for_connection`'s body, inlined here since it's
        // private and this test needs to drop the response channel instead
        // of awaiting it), *before* seed B's dial is issued, so seed A's
        // real connection is free to keep completing in the background
        // while seed B's dial is in flight.
        let opts_a = DialOpts::unknown_peer_id().address(addr_a.clone()).build();
        let (respond_to_a, response_rx_a) = oneshot::channel::<Option<PeerId>>();
        net_c
            .commands
            .send(Command::DialForConnection {
                opts: opts_a,
                respond_to: respond_to_a,
            })
            .expect("net_c's driver task is still running");
        drop(response_rx_a); // abandoned, as a timed-out try_join_via_seed would drop it

        // Run the real cascade against seed B only. If seed A's abandoned
        // dial connects while this is in flight, the fix must not let that
        // leak into seed B's result.
        let members = timeout(
            TEST_TIMEOUT,
            net_c.join_via_seeds(&[addr_b], Duration::from_secs(5)),
        )
        .await
        .expect("join_via_seeds completed within the test timeout");

        assert_eq!(
            members,
            Some([worker_b.clone()].into_iter().collect()),
            "seed B's join must resolve to seed B's own membership, never seed A's, \
             even though seed A's abandoned dial may still be completing concurrently"
        );

        // Confirm seed A's dial really did complete in the background
        // (proving this test actually exercised a live late connection, not
        // a dial that simply never connected) — its already-abandoned
        // response channel makes that harmless, which is exactly the
        // property under test.
        timeout(
            TEST_TIMEOUT,
            wait_until(|| net_c.reachable_peers(worker_c.clone()).contains(&worker_a)),
        )
        .await
        .expect("seed A's abandoned dial still connected in the background");
    }

    /// Chunk C6: proves `request_claim`/`poll_claim_requests`/`respond_claim`
    /// round-trip over real sockets at the `Net` layer alone, independent of
    /// `core::scheduler::Scheduler` or `crate::driver` — the same "prove the
    /// wire machinery in isolation" role `join_via_seeds_returns_the_seeds_membership`
    /// plays for the join protocol above.
    #[tokio::test]
    async fn request_claim_round_trips_an_accept_over_real_sockets() {
        let net_a = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let net_b = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let (worker_a, worker_b) = connected_pair(&net_a, &net_b).await;
        let task_id = TaskId::new("task-1");

        let responder = tokio::spawn({
            let task_id = task_id.clone();
            async move {
                loop {
                    for handle in net_b.poll_claim_requests() {
                        assert_eq!(handle.from(), worker_a);
                        assert_eq!(handle.task_id(), task_id);
                        net_b.respond_claim(
                            handle,
                            ClaimResponse {
                                result: Some(claim_response::Result::Accept(Claim {
                                    task: None,
                                    task_run_id: Some(TaskRunId::new("run-1").into()),
                                    attempt_number: 1,
                                    chain: vec![],
                                })),
                            },
                        );
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
        });

        let response = timeout(
            TEST_TIMEOUT,
            net_a.request_claim(worker_b, task_id),
        )
        .await
        .expect("request_claim completed within the test timeout");

        responder.abort();
        assert_eq!(
            response,
            Some(ClaimResponse {
                result: Some(claim_response::Result::Accept(Claim {
                    task: None,
                    task_run_id: Some(TaskRunId::new("run-1").into()),
                    attempt_number: 1,
                    chain: vec![],
                })),
            })
        );
    }

    /// Same round trip, but the answering side rejects — proves `ClaimReject`
    /// (not just `Claim`) survives the wire intact.
    #[tokio::test]
    async fn request_claim_round_trips_a_reject_over_real_sockets() {
        let net_a = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let net_b = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let (_worker_a, worker_b) = connected_pair(&net_a, &net_b).await;
        let task_id = TaskId::new("task-1");

        let responder = tokio::spawn(async move {
            loop {
                for handle in net_b.poll_claim_requests() {
                    net_b.respond_claim(
                        handle,
                        ClaimResponse {
                            result: Some(claim_response::Result::Reject(ClaimReject {
                                reason: ClaimRejectReason::ClaimRejectAlreadySelected as i32,
                            })),
                        },
                    );
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });

        let response = timeout(
            TEST_TIMEOUT,
            net_a.request_claim(worker_b, task_id),
        )
        .await
        .expect("request_claim completed within the test timeout");

        responder.abort();
        assert_eq!(
            response,
            Some(ClaimResponse {
                result: Some(claim_response::Result::Reject(ClaimReject {
                    reason: ClaimRejectReason::ClaimRejectAlreadySelected as i32,
                })),
            })
        );
    }

    /// The ranking rule behind final-review finding I1, on its own: a better
    /// -ranked source always wins regardless of arrival order, and an equally
    /// -ranked one is treated as a fresher observation and overwrites. See the
    /// module doc's "Where a peer's address comes from"; the end-to-end
    /// consequence is asserted in `net/tests/three_node_join_test.rs`.
    #[test]
    fn a_peer_address_is_only_replaced_by_an_equal_or_better_ranked_source() {
        let peer = PeerId::random();
        let send_back: Multiaddr = "/ip4/127.0.0.1/tcp/54321".parse().unwrap();
        let dialed: Multiaddr = "/ip4/127.0.0.1/tcp/4001".parse().unwrap();
        let advertised: Multiaddr = "/ip4/127.0.0.1/tcp/4002".parse().unwrap();
        let fresher_advertised: Multiaddr = "/ip4/127.0.0.1/tcp/4003".parse().unwrap();

        let addresses = Arc::new(Mutex::new(BTreeMap::new()));
        let stored = |addresses: &Arc<Mutex<BTreeMap<PeerId, KnownAddress>>>| {
            addresses
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get(&peer)
                .expect("an address was recorded for this peer")
                .addr
                .clone()
        };

        record_peer_address(
            &addresses,
            peer,
            send_back.clone(),
            AddressSource::InboundRemote,
        );
        assert_eq!(stored(&addresses), send_back);

        record_peer_address(&addresses, peer, dialed.clone(), AddressSource::DialedAddress);
        assert_eq!(stored(&addresses), dialed, "a dialed address outranks a send_back_addr");

        record_peer_address(&addresses, peer, advertised.clone(), AddressSource::Identify);
        assert_eq!(stored(&addresses), advertised, "Identify outranks both");

        record_peer_address(
            &addresses,
            peer,
            send_back.clone(),
            AddressSource::InboundRemote,
        );
        assert_eq!(
            stored(&addresses),
            advertised,
            "a later inbound connection must not displace an Identify-advertised address"
        );

        record_peer_address(
            &addresses,
            peer,
            fresher_advertised.clone(),
            AddressSource::Identify,
        );
        assert_eq!(
            stored(&addresses),
            fresher_advertised,
            "an equally-ranked observation is a fresher one and does replace"
        );
    }

    #[test]
    fn a_wildcard_bind_address_is_not_treated_as_dialable() {
        assert!(is_dialable_listen_addr(
            &"/ip4/127.0.0.1/tcp/4001".parse().unwrap()
        ));
        assert!(!is_dialable_listen_addr(
            &"/ip4/0.0.0.0/tcp/4001".parse().unwrap()
        ));
        assert!(!is_dialable_listen_addr(&"/ip6/::/tcp/4001".parse().unwrap()));
    }
}
