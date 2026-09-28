//! `Net`: the network a driver runs a `core::election::WorkerNode` over, on
//! top of the swarm built by `crate::swarm`. It sends the election messages
//! the node asks to send, publishes the ones it asks to publish to its whole
//! shard (see "Gossip" below), and queues the node's inputs as they happen:
//! every election message received, and every connection to a peer opening
//! or closing. It also carries the join and claim protocols, whose requests
//! only the driver can answer (see `crate::driver::run_driver`).
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
//! ## Synchronization: a background task, not a locked `Swarm`
//!
//! `Net`'s methods take `&self`, but driving a libp2p `Swarm` needs `&mut
//! self` (`swarm.select_next_some()`). `Net::new` hands the `Swarm` to a
//! dedicated tokio task (`drive`) that owns it exclusively and polls it
//! continuously. `Net` itself holds a command channel to that task and a few
//! small `Mutex`-guarded values the task keeps current as it observes swarm
//! events: the queues of inputs and requests for the driver, the peers the
//! swarm is connected to, the peers subscribed to its shard, and the
//! addresses it knows. A method either pushes a command and returns, awaits
//! the task's answer to a command, or takes a short lock; nothing holds a
//! lock across an await, so a `&self` caller never deadlocks against the
//! task.
//!
//! ## Inputs for the node
//!
//! `drive` turns swarm events into `core::election::Input`s and queues them
//! from the moment the `Net` is created ([`Net::take_inputs`]), so a node
//! built later (after the bootstrap cascade, say), or driven again after a
//! pause, is told of everything that happened in between:
//!
//! - an inbound election message, sent or published, becomes
//!   `Input::Message`;
//! - a peer's first connection opening becomes `Input::PeerConnected`;
//! - its last connection closing becomes `Input::PeerDisconnected`, and so
//!   does a failed dial that names a peer this `Net` holds no connection to.
//!
//! The queue stays bounded however long no driver takes from it (the
//! bootstrap cascade can wait indefinitely, say). A connection event that
//! repeats the peer's last queued one is not queued again, since the node
//! would ignore it; a disconnect that follows a still-queued connect removes
//! that connect and is not queued either, since together they change nothing
//! the node needs to hear, so at most two connection events per peer are
//! ever queued. Election messages, unreliable anyway, are capped instead:
//! past [`Net::with_input_limit`]'s limit, the oldest queued one is dropped.
//! Each input, like each join or claim request, also wakes whoever waits in
//! [`Net::wait_for_arrival`], so a driver can sleep until something arrives
//! instead of polling.
//!
//! ## Response shape
//!
//! Election messages are never correlated with a response: [`Net::send`] is
//! fire-and-forget and delivery is unreliable. `drive` answers every inbound
//! election message with `codec::Ack` as soon as it has queued it, purely so
//! the substream closes cleanly on the sender's side; nothing here ever reads
//! that ack back.
//!
//! ## Gossip
//!
//! A `Net` serves one shard. [`Net::subscribe_to_shard`] subscribes it to
//! that shard's gossipsub topic, `/kabudachi/<shard>/election/2`, and
//! [`Net::publish`] publishes on it, fire-and-forget like `send`. A publish
//! gossipsub refuses (no subscribed peer yet, say) is dropped like a lost
//! message, logged at `debug`. Every gossip message is signed by its author
//! (see `crate::swarm`), so an arriving one becomes `Input::Message` from the
//! author, not from whichever peer relayed it. One with no author, or whose
//! body is not a well-formed `ElectionMessage`, is dropped.
//!
//! A relayed roll call can reach a worker that holds no connection to its
//! initiator, and that worker answers the initiator directly. So a roll call
//! and a reply to one carry their sender's address (see "Where a peer's
//! address comes from" below), and [`Net::send`] dials a peer it holds no
//! connection to at the address it has on file.
//!
//! ## The bootstrap join protocol
//!
//! `/kabudachi/join/1` (see `crate::join_codec`) is a genuine correlated
//! request/response, unlike the election protocol above, so it needs its own
//! machinery: [`Net::ask_for_leader`] (the joining side — ask each seed or
//! registered peer in order, first `JOIN_RESPONSE` that points at a leader
//! this node reaches wins) and
//! [`Net::poll_join_requests`] / [`Net::respond_join`] (the answering side).
//! A join response names the shard's leader, which only
//! `core::election::WorkerNode` knows, so the driver answers it.
//! [`Net::dialable_address`] and [`Net::local_multiaddr`] expose what `Net`
//! knows about addresses, the other half of what a join response needs.
//!
//! ## The claim arbitration protocol
//!
//! `/kabudachi/claim/1` (see `crate::claim_codec`) is the same shape of
//! genuine correlated request/response as join, so it gets the same
//! machinery: [`Net::request_claim`] and [`Net::claim_oldest`] (the asking
//! side, sent to the leader the worker's node names; see
//! [`Net::set_leader`]) and [`Net::poll_claim_requests`] /
//! [`Net::respond_claim`] (the answering side). Whether to grant a claim is
//! `core::scheduler::Scheduler`'s decision, so the driver answers it too.
//!
//! ## Reconnect/backoff
//!
//! A connection that drops is redialed: [`RedialPolicy`] governs a bounded,
//! exponential-backoff redial that `drive` runs on its own, at the address
//! [`Net::peer_addresses`] keeps for a disconnected peer. libp2p has no
//! retry to defer to: in the pinned `libp2p-swarm` 0.48.0,
//! `libp2p_swarm::dial_opts::DialOpts` (and its `PeerCondition`) configure
//! only a single dial attempt, and nothing schedules a retry after a dial or
//! an established connection fails.
//!
//! **Which drops are redial-eligible.** Only a peer that this node still
//! needs a connection to, and would not otherwise reach again, is redialed:
//! one in its gossip mesh for its shard ([`Net::shard_mesh`]) when the
//! connection dropped. The node's election needs that mesh, since a roll
//! call is published and a peer cut off from the mesh never hears it, and
//! nothing else reconnects it: gossipsub never dials. Every other
//! connection either repairs itself or was never needed: a follower's
//! heartbeat, a claim or any other [`Net::send`] dials its peer as it goes,
//! and a connection that only a JOIN, a `kad` crawl or a peer's own dial
//! opened has no reason to be reopened just because an address is on file.
//! A peer in the mesh is never closed for being idle (gossipsub keeps it
//! alive; see `crate::swarm`'s `IDLE_CONNECTION_TIMEOUT`), so an idle
//! connection that times out is never redialed. Only the driver's routing
//! crawls (see [`Net::refresh_peer_routing`]) open such a connection again,
//! at most once per peer per crawl.
//!
//! A peer whose connection this `Net` itself asked to close
//! ([`Net::disconnect`]) is not redialed either: a local hangup is a
//! decision, not a failure to recover from. Only the closing side can tell
//! the difference: its own `SwarmEvent::ConnectionClosed` reports `cause:
//! None`, while the side being disconnected sees `cause:
//! Some(IO(..Closed..))`, like any transport failure. So only the side that
//! issued the disconnect excludes that peer (`drive`'s
//! `locally_disconnected` set, cleared once the peer reconnects by any
//! means); the other side, like any other dropped peer, redials.
//!
//! This exemption covers only the redial policy, not the peer itself: any
//! later [`Net::send`] to it, a follower's heartbeat for example, dials it
//! again like any other unreachable peer. `disconnect` is not a way to
//! isolate a peer; [`Net::block_peer`] is. A blocked peer stays
//! redial-eligible, like one across a real partition: each attempt fails
//! until the block lifts, and counts against the bounded budget.
//!
//! **Why the default is short.** A redial restores the gossip mesh after a
//! transient drop, so it should land well within a suspicion timeout: a
//! follower that misses a roll call because its mesh has not come back is a
//! follower the election cannot count. Nothing in the election needs a
//! redial to stay away, either. The ring roll call did (a redial that landed
//! mid-election reconnected survivors to the ex-leader they were replacing,
//! which is why the default was once 10 s); the gossip roll call, leader
//! stickiness and terms make a reconnected ex-leader harmless. So
//! `RedialPolicy::default` tries after one second, and backs off from there.
//! [`Net::new_with_redial_policy`] lets a caller, such as this crate's own
//! tests, use other parameters.
//!
//! ## Where a peer's address comes from
//!
//! `peer_addresses` holds the leader address this node hands a joining node
//! in a `JOIN_RESPONSE`, and the address [`Net::send`] dials a peer at when
//! it holds no connection to it, so it has to know which of its entries
//! something can actually *dial*. Not every address a swarm event carries
//! is:
//! `ConnectedPoint::get_remote_address` (libp2p-core 0.44.0,
//! `src/connection.rs`) returns the address this node dialed for
//! `ConnectedPoint::Dialer`, but `send_back_addr` for
//! `ConnectedPoint::Listener` — and `send_back_addr` is the *dialer's
//! ephemeral source address*, which is generally not an address anything can
//! connect back to.
//!
//! `identify::Event::Received`'s `info.listen_addrs` is exactly the peer's
//! own advertised listen addresses, so it is the preferred source, ranked
//! above both endpoint-derived ones by `AddressSource`: `Identify` >
//! `DialedAddress` (a `ConnectedPoint::Dialer` address, dialable by
//! construction, since this node just dialed it) > `SelfStamped` (below) >
//! `InboundRemote` (a `send_back_addr`, kept only as a fallback for a peer
//! whose Identify exchange has not completed yet: never handed to a joiner
//! nor offered to [`Net::send`]'s dial — see [`Net::dialable_address`] —
//! though a redial, which has nothing better, tries it). A new observation
//! replaces the stored one whenever its source ranks at least as high, so a
//! fresher Identify or a fresher successful dial still wins over a stale one
//! of the same kind.
//!
//! A worker can also learn a peer's address with no connection to it at
//! all: gossip delivers a roll call from an initiator it may never have
//! connected to, and it answers with a direct reply. So a `Net` stamps its
//! own address (chosen as described below) on every roll call it publishes
//! and every roll call reply it sends, and records the address stamped on
//! one that arrives as its sender's `SelfStamped` address — but only when the
//! stamp names the sender `Net` vouches for (a gossip message's signed
//! author, or the peer at the other end of a direct message's connection), so
//! no peer can redirect traffic meant for another. A stamp is the peer's own
//! choice among its listen addresses, not one this node has seen work, so it
//! ranks below a dialed address and, being one address where Identify
//! advertises them all, below Identify; it ranks above an inbound source
//! address, which is usually not dialable at all.
//!
//! Of a peer's `listen_addrs`, entries with an unspecified IP
//! (`0.0.0.0`/`::`) are skipped as undialable, and the first remaining one
//! that is not loopback is taken; a loopback one only when nothing else is
//! advertised. A node on another host that dialed a loopback leader address
//! would reach itself, and a joiner that a seed has answered keeps asking
//! for a leader it can reach. This node's own address, which it hands
//! joiners when it leads ([`Net::local_multiaddr`]), follows the same rule:
//! a node listening on a wildcard bind is told one listen address per
//! interface, in no set order, and a later one replaces the recorded one
//! unless it would swap a non-loopback address for loopback.
//!
//! Best-effort, deliberately: on a multi-homed host the chosen address may
//! be one the particular asking peer cannot route to — a general
//! address-selection problem this phase does not try to solve, consistent
//! with [`Net::peer_addresses`]'s "best-effort and address-of-record only"
//! contract.
//!
//! This module's own address book is not the only place a dial can find an
//! address, though: [`Net::send`] hands `request_response` an empty address
//! list whenever `dialable_address_of` has nothing, and the swarm underneath
//! that call still asks `kad`'s routing table for one regardless — fed from
//! the same Identify events, in `handle_event` below (see `crate::swarm`'s
//! "kad: peer routing, not membership"). So a peer this node never itself
//! connected to can still be reached, once some other peer's Identify has
//! given `kad` a route to it.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration as StdDuration;

use kabudachi_core::election::Input;
use kabudachi_core::protocol::ids::{ShardId, TaskId, WorkerId};
use kabudachi_core::protocol::messages::{
    ClaimOldest, ClaimRequest, ClaimResponse, ElectionMessage, JoinRequest, JoinResponse,
    claim_request, election_message,
};
use kabudachi_core::protocol::messages::prelude::*;
use libp2p::core::ConnectedPoint;
use libp2p::core::transport::ListenerId;
use libp2p::futures::StreamExt;
use libp2p::multiaddr::Protocol;
use libp2p::request_response::{self, OutboundRequestId, ResponseChannel};
use libp2p::swarm::dial_opts::DialOpts;
use libp2p::swarm::{ConnectionId, SwarmEvent};
use libp2p::{Multiaddr, PeerId, Swarm, gossipsub, identify};
use prost::Message as _;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::codec::Ack;
use crate::framing::decode_well_formed;
use crate::swarm::{Behaviour, BehaviourEvent};

/// How long [`Net::ask_for_leader`] waits, per peer, for a connection and
/// then a `JOIN_RESPONSE` before moving on to the next peer.
pub const DEFAULT_JOIN_PEER_TIMEOUT: StdDuration = StdDuration::from_secs(10);

/// Where a `peer_addresses` entry came from, and so how far it can be trusted
/// to be an address anything can dial. Ordered worst to best: `Ord` *is* the
/// precedence rule — see the module doc's "Where a peer's address comes from".
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum AddressSource {
    /// A `ConnectedPoint::Listener`'s `send_back_addr`: the remote's ephemeral
    /// source address, usually *not* dialable. Kept only as a fallback for a
    /// peer whose Identify exchange has not completed yet.
    InboundRemote,
    /// The address a peer stamped for itself on a roll call or roll call
    /// reply it sent: one it listens on, by its own choice, but one this node
    /// has not yet seen work.
    SelfStamped,
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

/// `peer`'s address of record in `peer_addresses`, unless it is only the
/// source address of `peer`'s inbound connection, which `peer` need not
/// listen on (see the module doc's "Where a peer's address comes from").
fn dialable_address_of(
    peer_addresses: &Arc<Mutex<BTreeMap<PeerId, KnownAddress>>>,
    peer: &PeerId,
) -> Option<Multiaddr> {
    peer_addresses
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(peer)
        .filter(|known| known.source > AddressSource::InboundRemote)
        .map(|known| known.addr.clone())
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

/// The address, of `candidates` in order, to give other nodes to dial: the
/// first that is neither a wildcard bind nor loopback, or else the first
/// loopback one. A loopback address (`127.0.0.0/8`, `::1`) reaches only the
/// host that dials it, so a node on another host handed one would dial
/// itself; it is used only when there is nothing else.
fn preferred_address(candidates: impl IntoIterator<Item = Multiaddr>) -> Option<Multiaddr> {
    let mut first_loopback = None;
    for candidate in candidates {
        if !is_dialable_listen_addr(&candidate) {
            continue;
        }
        if !is_loopback(&candidate) {
            return Some(candidate);
        }
        first_loopback.get_or_insert(candidate);
    }
    first_loopback
}

fn is_loopback(addr: &Multiaddr) -> bool {
    addr.iter().any(|protocol| match protocol {
        Protocol::Ip4(ip) => ip.is_loopback(),
        Protocol::Ip6(ip) => ip.is_loopback(),
        _ => false,
    })
}

/// Bounded, exponential-backoff redial policy for a peer in this node's
/// gossip mesh that dropped without this `Net` itself asking to disconnect
/// it (see the module doc's "Reconnect/backoff" section for why libp2p's own
/// `DialOpts`/`PeerCondition` don't already do this, and which drops are
/// eligible at all).
///
/// On each eligible drop, `drive` schedules a first redial attempt after
/// `initial_backoff`; each subsequent attempt (up to `max_attempts` total)
/// doubles the wait, capped at `max_backoff`. `check_interval` is how often
/// `drive` polls for a due attempt — coarser than `initial_backoff` wastes
/// time before the first attempt actually fires, so a caller using a short
/// `initial_backoff` (this crate's own tests) should also shrink this.
///
/// `Default` picks production-shaped values (see the module doc's "Why the
/// default is short"): a first attempt one second after the drop, well
/// within any suspicion timeout of seconds, doubling to at most 30 s, and
/// eight attempts in all, so a peer gone for about two minutes is given up
/// on and left to the node's own sends. A caller that wants test-scale
/// retries should use [`Net::new_with_redial_policy`] instead of
/// [`Net::new`].
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
            initial_backoff: StdDuration::from_secs(1),
            max_backoff: StdDuration::from_secs(30),
            max_attempts: 8,
            check_interval: StdDuration::from_millis(250),
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

/// Instructions for the driver task. `Net`'s methods and setup
/// helpers only ever push onto this channel; they never touch the `Swarm`
/// directly.
enum Command {
    Send {
        to: PeerId,
        message: ElectionMessage,
    },
    Subscribe {
        topic: gossipsub::IdentTopic,
    },
    Publish {
        topic: gossipsub::TopicHash,
        message: ElectionMessage,
    },
    Dial {
        addr: Multiaddr,
    },
    /// See `Net::disconnect`. Fire-and-forget, same shape as `Dial`: the
    /// caller learns the outcome (if it cares) from the
    /// `Input::PeerDisconnected` the closed connection queues, the same way
    /// the rest of `Net`'s connection state is reported — there is no
    /// dedicated "disconnect completed" signal.
    Disconnect {
        peer: PeerId,
    },
    /// See `Net::refresh_peer_routing`.
    RefreshPeerRouting,
    /// See `Net::block_peer` and `Net::unblock_peer`.
    Block {
        peer: PeerId,
        blocked: bool,
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

    /// What this request asks for: one task, or some of the oldest pending
    /// ones.
    pub fn request(&self) -> &claim_request::Request {
        self.request
            .request
            .as_ref()
            .expect("the claim codec only accepts a request that asks for something")
    }
}

/// [`Net::try_listen_on`] could not listen on this address.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListenRejected(pub Multiaddr);

impl std::fmt::Display for ListenRejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "could not listen on {}", self.0)
    }
}

impl std::error::Error for ListenRejected {}

/// What one pass of [`Net::ask_for_leader`] over its peers found.
#[derive(Debug, Clone, PartialEq)]
pub enum LeaderSearch {
    /// A peer pointed at a leader, and this node is now connected to it.
    Found(JoinResponse),
    /// Some peer answered, but none pointed at a leader this node could
    /// reach: the shard exists, and its leader may not be elected yet.
    NoReachableLeader,
    /// No peer answered at all.
    NoAnswer,
}

/// Why [`Net::request_claim`] or [`Net::claim_oldest`] got no answer from
/// a leader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimFailure {
    /// The worker's node names no leader. Ask again once it does.
    NoLeader,
    /// This worker is the leader. Its own claims are its own scheduler's to
    /// decide, not a peer's, so nothing was sent.
    ThisWorkerLeads,
    /// No answer came: the request failed outright (such as a leader that
    /// cannot be dialed), the leader disconnected before answering, or
    /// nothing could be sent (this `Net` has stopped, or the leader's id
    /// names no libp2p peer).
    Unanswered,
}

/// Running counts of what one [`Net`] has carried since it was created
/// (see [`Net::traffic`]): how much of the shard's traffic goes through a
/// worker, its leader say.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Traffic {
    /// Election messages received, sent to this worker or published to its
    /// shard, including any the input queue's bound later dropped.
    pub messages_received: u64,
    /// Election messages this worker handed to its swarm to send
    /// ([`Net::send`]), whether or not they arrived.
    pub messages_sent: u64,
    /// Election messages this worker published to its shard
    /// ([`Net::publish`]), each counted once however many workers it reaches.
    pub messages_published: u64,
    /// `/kabudachi/join/1` requests received.
    pub join_requests_received: u64,
    /// `/kabudachi/claim/1` requests received.
    pub claim_requests_received: u64,
}

impl Traffic {
    /// Everything this worker received or sent: election messages in and
    /// out, and join and claim requests in (each answered once).
    pub fn total(&self) -> u64 {
        self.messages_received
            + self.messages_sent
            + self.messages_published
            + self.join_requests_received
            + self.claim_requests_received
    }
}

impl std::ops::Sub for Traffic {
    type Output = Traffic;

    /// The traffic carried between two readings, `self` the later (each
    /// count saturates at zero if not).
    fn sub(self, earlier: Traffic) -> Traffic {
        Traffic {
            messages_received: self
                .messages_received
                .saturating_sub(earlier.messages_received),
            messages_sent: self.messages_sent.saturating_sub(earlier.messages_sent),
            messages_published: self
                .messages_published
                .saturating_sub(earlier.messages_published),
            join_requests_received: self
                .join_requests_received
                .saturating_sub(earlier.join_requests_received),
            claim_requests_received: self
                .claim_requests_received
                .saturating_sub(earlier.claim_requests_received),
        }
    }
}

/// How many of its node's suspicion timeouts `crate::driver::run_driver`
/// waits, by default, between re-crawls of peer routing that nothing else
/// prompted (see [`Net::refresh_peer_routing`]). It only backs up the
/// crawls a change to the node's view of its shard starts (a new leader, a
/// new configuration, its own admission), finding a peer those missed; a
/// crawl costs a few `kad` queries, so a crawl every few suspicion timeouts
/// is cheap.
pub const DEFAULT_ROUTING_REFRESH_SUSPICIONS: u32 = 10;

/// The shortest period between routing crawls nothing else prompted (see
/// [`Net::with_routing_refresh_period`]), whatever the suspicion timeout: a
/// lone node may run with a suspicion timeout of zero.
pub const MIN_ROUTING_REFRESH_PERIOD: StdDuration = StdDuration::from_secs(1);

/// How many inputs [`Net`] holds for its node, by default, before it drops
/// the oldest election message to make room (see [`Net::with_input_limit`]).
pub const DEFAULT_INPUT_LIMIT: usize = 1024;

/// What `drive` hands over for the driver of this `Net`'s node, each queue
/// in arrival order: the node's inputs, and the join and claim requests the
/// driver answers. `arrived` is signalled whenever any of them grows.
struct Inbound {
    inputs: Mutex<VecDeque<Input>>,
    /// See [`Net::with_input_limit`].
    input_limit: AtomicUsize,
    join_requests: Mutex<VecDeque<JoinRequestHandle>>,
    claim_requests: Mutex<VecDeque<ClaimRequestHandle>>,
    arrived: Notify,
    /// See [`Net::traffic`].
    messages_received: AtomicU64,
    join_requests_received: AtomicU64,
    claim_requests_received: AtomicU64,
}

impl Default for Inbound {
    fn default() -> Self {
        Self {
            inputs: Mutex::default(),
            input_limit: AtomicUsize::new(DEFAULT_INPUT_LIMIT),
            join_requests: Mutex::default(),
            claim_requests: Mutex::default(),
            arrived: Notify::new(),
            messages_received: AtomicU64::new(0),
            join_requests_received: AtomicU64::new(0),
            claim_requests_received: AtomicU64::new(0),
        }
    }
}

impl Inbound {
    /// Queues `input` for the node, keeping the queue bounded while no
    /// driver takes from it (see the module doc's "Inputs for the node").
    fn queue_input(&self, input: Input) {
        if matches!(input, Input::Message { .. }) {
            self.messages_received.fetch_add(1, Ordering::Relaxed);
        }
        let mut inputs = self.inputs.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(peer) = connection_event_peer(&input) {
            let latest = inputs
                .iter()
                .rposition(|queued| connection_event_peer(queued) == Some(peer));
            match latest.map(|index| (index, &inputs[index])) {
                // The node would ignore a repeat of the peer's last event.
                Some((_, queued)) if *queued == input => return,
                // The peer connected and disconnected again before the node
                // heard either: together they change nothing the node
                // needs to know.
                Some((index, Input::PeerConnected(_))) => {
                    inputs.remove(index);
                    return;
                }
                _ => {}
            }
        } else if inputs.len() >= self.input_limit.load(Ordering::Relaxed) {
            // Connection events are never dropped: at most two per peer are
            // ever queued, so only messages can fill the queue.
            if let Some(oldest) = inputs
                .iter()
                .position(|queued| matches!(queued, Input::Message { .. }))
            {
                inputs.remove(oldest);
                tracing::debug!(
                    limit = self.input_limit.load(Ordering::Relaxed),
                    "dropping the oldest queued election message: no driver has taken this \
                     node's inputs"
                );
            }
        }
        inputs.push_back(input);
        drop(inputs);
        self.arrived.notify_one();
    }

    fn queue_join_request(&self, handle: JoinRequestHandle) {
        self.join_requests_received.fetch_add(1, Ordering::Relaxed);
        push(&self.join_requests, handle);
        self.arrived.notify_one();
    }

    fn queue_claim_request(&self, handle: ClaimRequestHandle) {
        self.claim_requests_received.fetch_add(1, Ordering::Relaxed);
        push(&self.claim_requests, handle);
        self.arrived.notify_one();
    }
}

/// The peer a connection event is about; `None` for any other input.
fn connection_event_peer(input: &Input) -> Option<&WorkerId> {
    match input {
        Input::PeerConnected(peer) | Input::PeerDisconnected(peer) => Some(peer),
        _ => None,
    }
}

fn push<T>(queue: &Mutex<VecDeque<T>>, item: T) {
    queue
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .push_back(item);
}

fn drain<T>(queue: &Mutex<VecDeque<T>>) -> Vec<T> {
    queue
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .drain(..)
        .collect()
}

/// The election network of one worker on top of a real libp2p swarm. One
/// `Net` represents exactly one worker/node — see the module doc for the
/// `WorkerId <-> PeerId` mapping and the actor pattern behind `&self`.
pub struct Net {
    local_worker_id: WorkerId,
    commands: mpsc::UnboundedSender<Command>,
    inbound: Arc<Inbound>,
    /// The peers the swarm holds a connection to, refreshed by `drive` after
    /// every swarm event and command, for the join cascade to ask whether a
    /// seed or leader is still connected (`Self::is_connected`).
    connected: Arc<Mutex<BTreeSet<PeerId>>>,
    /// The best dialable address known for each peer, kept current the same
    /// way `connected` is (see `drive` below) and ranked by where it came from
    /// (see [`AddressSource`] and the module doc's "Where a peer's address
    /// comes from"). Used to compose `JOIN_RESPONSE`s
    /// (`Self::dialable_address`), to redial a dropped peer, and to dial a
    /// peer `Self::send` holds no connection to.
    peer_addresses: Arc<Mutex<BTreeMap<PeerId, KnownAddress>>>,
    /// The address this `Net` gives other nodes for itself (see
    /// `Self::local_multiaddr`), for including this node's own address in a
    /// `JOIN_RESPONSE` it composes.
    local_addr: Arc<Mutex<Option<Multiaddr>>>,
    /// Every peer `drive` is currently mid-redial for, and how many
    /// attempts have been made so far — see `Self::redial_attempts`.
    redial_attempts: Arc<Mutex<BTreeMap<PeerId, u32>>>,
    /// The shard this `Net` subscribed to (see `Self::subscribe_to_shard`).
    shard: Mutex<Option<ShardId>>,
    /// The leader this worker's claims go to (see `Self::set_leader`).
    leader: Mutex<Option<WorkerId>>,
    /// The peer each address [`Self::ask_for_leader`] asked turned out to
    /// be, so a peer asked again is asked over the connection it already
    /// has.
    asked_peers: Mutex<HashMap<Multiaddr, PeerId>>,
    /// The peers known to be subscribed to this `Net`'s shard topic, kept
    /// current the same way `connected` is (see `Self::shard_subscribers`).
    shard_subscribers: Arc<Mutex<BTreeSet<PeerId>>>,
    /// The peers in this `Net`'s gossip mesh, kept current the same way
    /// (see `Self::shard_mesh`).
    shard_mesh: Arc<Mutex<BTreeSet<PeerId>>>,
    /// See [`Self::with_routing_refresh_period`].
    routing_refresh_period: Option<StdDuration>,
    /// See [`Self::traffic`].
    messages_sent: AtomicU64,
    messages_published: AtomicU64,
    driver: JoinHandle<()>,
}

impl Net {
    /// Takes ownership of `swarm` and spawns the task that drives it. Does
    /// not listen or dial; use `listen_on`/`dial` for that.
    ///
    /// Uses [`RedialPolicy::default`] for the bounded redial
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
        let inbound = Arc::new(Inbound::default());
        let connected = Arc::new(Mutex::new(BTreeSet::new()));
        let peer_addresses = Arc::new(Mutex::new(BTreeMap::new()));
        let local_addr = Arc::new(Mutex::new(None));
        let redial_attempts = Arc::new(Mutex::new(BTreeMap::new()));
        let shard_subscribers = Arc::new(Mutex::new(BTreeSet::new()));
        let shard_mesh = Arc::new(Mutex::new(BTreeSet::new()));

        let driver = tokio::spawn(drive(
            swarm,
            command_rx,
            inbound.clone(),
            connected.clone(),
            peer_addresses.clone(),
            local_addr.clone(),
            redial_attempts.clone(),
            shard_subscribers.clone(),
            shard_mesh.clone(),
            redial_policy,
        ));

        Self {
            local_worker_id,
            commands,
            inbound,
            connected,
            peer_addresses,
            local_addr,
            redial_attempts,
            shard: Mutex::new(None),
            leader: Mutex::new(None),
            asked_peers: Mutex::new(HashMap::new()),
            shard_subscribers,
            shard_mesh,
            routing_refresh_period: None,
            messages_sent: AtomicU64::new(0),
            messages_published: AtomicU64::new(0),
            driver,
        }
    }

    /// Holds at most `limit` inputs for this `Net`'s node (default
    /// [`DEFAULT_INPUT_LIMIT`]) while no driver takes them: past it, the
    /// oldest election message queued is dropped, like any lost message, to
    /// make room. Connection events are compacted instead of dropped (see the
    /// module doc's "Inputs for the node"), so they alone can take the queue
    /// past `limit`, by at most two per peer. A `limit` below 1 counts as 1:
    /// the message just arrived is always kept.
    #[must_use]
    pub fn with_input_limit(self, limit: usize) -> Self {
        self.inbound.input_limit.store(limit.max(1), Ordering::Relaxed);
        self
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
        self.try_listen_on(addr)
            .await
            .expect("Swarm::listen_on rejected the address given to Net::listen_on")
    }

    /// [`Self::listen_on`], returning [`ListenRejected`] instead of
    /// panicking when the swarm cannot listen on `addr`, or this `Net` has
    /// stopped.
    pub async fn try_listen_on(&self, addr: Multiaddr) -> Result<Multiaddr, ListenRejected> {
        let (respond_to, response) = oneshot::channel();
        self.commands
            .send(Command::ListenOn {
                addr: addr.clone(),
                respond_to,
            })
            .map_err(|_| ListenRejected(addr.clone()))?;
        response.await.map_err(|_| ListenRejected(addr))
    }

    /// Fire-and-forget dial: initiates a connection attempt and returns
    /// immediately, mirroring `Self::send`'s own best-effort delivery
    /// contract. The outcome is reported only as an input for the node: the
    /// connection's `Input::PeerConnected`, or an `Input::PeerDisconnected`
    /// when a dial whose address names its peer (`.../p2p/<peer id>`) fails.
    /// A failed dial to an address that names no peer is not reported.
    pub fn dial(&self, addr: Multiaddr) {
        let _ = self.commands.send(Command::Dial { addr });
    }

    /// Forcibly closes every connection this `Net` currently has to `peer`,
    /// without touching the driver task itself: the swarm's connections go,
    /// not the process. Fire-and-forget, like `dial`: a caller observes the
    /// effect through the `Input::PeerDisconnected` it queues on each side,
    /// not by awaiting this call.
    ///
    /// Backed by `libp2p_swarm::Swarm::disconnect_peer_id` (verified against
    /// `libp2p-swarm` 0.48.0's actual source, the version this workspace's
    /// `Cargo.lock` pins — see that method's doc: it closes every established
    /// connection to `peer` by sending each one a graceful `Close` command,
    /// which tears down the real transport connection (the underlying TCP
    /// socket), so the *remote* side observes a genuine disconnect too, not
    /// just a local bookkeeping change. The swarm reports the connection
    /// closed only once its pool has processed the close, so the input comes
    /// later, as it does when a peer closes a connection itself.
    ///
    /// A no-op (silently) if `peer` is not a `WorkerId` this mapping ever
    /// produced, the driver task is gone, or there was no connection to
    /// close — the same "nothing to do" shape as `send`'s `PeerId::from_str`
    /// guard.
    ///
    /// Only exempts `peer` from the redial policy (module doc, "Which drops
    /// are redial-eligible"); it does not isolate it. A later `Self::send` to
    /// `peer`, a follower's heartbeat for example, dials it again like any
    /// other unreachable peer. To isolate a peer, see [`Self::block_peer`].
    pub fn disconnect(&self, peer: WorkerId) {
        let Ok(peer) = PeerId::from_str(peer.as_str()) else {
            return;
        };
        let _ = self.commands.send(Command::Disconnect { peer });
    }

    /// Cuts this node off from `peer` until [`Self::unblock_peer`], as a
    /// network partition would: every connection to `peer` closes (each side
    /// reports `Input::PeerDisconnected`, as for [`Self::disconnect`]), and
    /// no message crosses either way, whether this node dials `peer` (a
    /// send, a redial), which fails at once, or `peer` dials it, which this
    /// node refuses as soon as the handshake names `peer`. A message either
    /// way meanwhile is lost like any undeliverable one. Only this node
    /// refuses: `peer` may see its own dial connect for an instant before
    /// the refusal closes it. So a partition is blocked on both sides: a
    /// connection that opens and is refused at once leaves one end of it in
    /// TCP `TIME_WAIT`, and with the port reuse the TCP transport does, a
    /// later dial between the same two listen ports can fail with
    /// `EADDRINUSE` until that wait ends (tens of seconds).
    /// Gossip still reaches `peer` through any other peer both are
    /// connected to, as it would across a partial partition: a test that
    /// cuts one group off from another blocks every pair across the cut.
    /// Blocking changes nothing about redial eligibility: a redial of a
    /// blocked peer fails like one of an unreachable peer and counts against
    /// the redial budget. Fire-and-forget, like `disconnect`; a no-op for a
    /// `peer` this mapping never produced.
    ///
    /// Backed by `libp2p::allow_block_list`, whose `block_peer` closes the
    /// peer's connections and whose connection checks deny every later one
    /// until `unblock_peer`.
    pub fn block_peer(&self, peer: WorkerId) {
        self.set_blocked(peer, true);
    }

    /// Lifts [`Self::block_peer`] for `peer`: connections to and from it may
    /// open again. Nothing is dialed here; the next send to `peer`, a
    /// pending redial, or `peer`'s own dial reconnects them.
    pub fn unblock_peer(&self, peer: WorkerId) {
        self.set_blocked(peer, false);
    }

    fn set_blocked(&self, peer: WorkerId, blocked: bool) {
        let Ok(peer) = PeerId::from_str(peer.as_str()) else {
            return;
        };
        let _ = self.commands.send(Command::Block { peer, blocked });
    }

    /// Makes the dial `opts` describes and resolves to the `PeerId` of the
    /// connection *this specific dial* establishes — never a connection some
    /// other dial (or an unrelated inbound connection) produced. `None` if
    /// the driver task is gone or this dial attempt fails
    /// (`SwarmEvent::OutgoingConnectionError`, including libp2p's `WrongPeerId`
    /// when `opts` names a peer and another answers, or `Swarm::dial`
    /// rejecting it outright).
    ///
    /// Correlation is by `ConnectionId` (`DialOpts::connection_id`), not by
    /// diffing the connected-peer set before/after — see its two callers,
    /// `Self::ask_peer_for_leader` and `Self::connect_to_leader`, and the module
    /// doc's "bootstrap join protocol" section for why that distinction matters:
    /// a late `ConnectionEstablished` for an abandoned dial carries *that*
    /// dial's `ConnectionId`, so it can only ever resolve (or fail to
    /// resolve, if the caller already stopped awaiting it) that dial's own
    /// response channel — it can never be mistaken for a different, later
    /// dial's result.
    async fn dial_for_connection(&self, opts: DialOpts) -> Option<PeerId> {
        let (respond_to, response) = oneshot::channel();
        self.commands
            .send(Command::DialForConnection { opts, respond_to })
            .ok()?;
        response.await.ok()?
    }

    /// The address this `Net` listens on that it gives other nodes, if any:
    /// the most recent one it was told of, except that a loopback address
    /// never replaces a non-loopback one (see the module doc's "Where a
    /// peer's address comes from"). Used to include this node's own address
    /// in a `JOIN_RESPONSE` it composes (`crate::driver::run_driver`'s
    /// join-request responder).
    pub fn local_multiaddr(&self) -> Option<Multiaddr> {
        self.local_addr
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Every address of record this node holds, keyed by the peer's
    /// `WorkerId` (see the module doc's `WorkerId <-> PeerId` mapping) —
    /// including a peer's inbound source address, which may not be dialable.
    /// For an address to give anyone else, use `Self::dialable_address`.
    /// Best-effort: a peer that disconnects keeps its last-known entry here
    /// rather than being removed (nothing needs this to reflect only *live*
    /// connections), which is where the redial policy finds the address to
    /// redial it at.
    pub fn peer_addresses(&self) -> BTreeMap<WorkerId, Multiaddr> {
        self.peer_addresses
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(|(peer, known)| (WorkerId::new(peer.to_string()), known.addr.clone()))
            .collect()
    }

    /// `peer`'s address of record, but only if something can dial it: one
    /// `peer` advertised through Identify or stamped on a roll call or reply
    /// it sent, or one this node dialed itself. `None` for a peer known only
    /// from its own inbound connection, whose source address is not one
    /// `peer` listens on (see the module doc's "Where a peer's address comes
    /// from").
    pub fn dialable_address(&self, peer: &WorkerId) -> Option<Multiaddr> {
        let peer = PeerId::from_str(peer.as_str()).ok()?;
        dialable_address_of(&self.peer_addresses, &peer)
    }

    /// Every peer this `Net` is currently mid-redial for (see
    /// `RedialPolicy`), and how many redial attempts have been made for each
    /// so far. A peer disappears from this map once it either reconnects
    /// (redial succeeded) or exhausts `RedialPolicy::max_attempts` (redial
    /// gave up — the "bounded" half of "bounded redial policy"). Not
    /// load-bearing for sending election messages; exposed for observability
    /// and to let tests assert on the redial policy directly rather than
    /// only inferring it from connection events.
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
        drain(&self.inbound.join_requests)
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
    /// `Self::ask_peer_for_leader`, the only caller) and awaits its
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
        drain(&self.inbound.claim_requests)
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

    /// Names the leader this worker's claims go to, or `None` when its node
    /// knows no leader. `crate::driver::run_driver` sets it from its node's
    /// `known_leader` after every batch, so a caller of [`Self::request_claim`]
    /// or [`Self::claim_oldest`] never chooses whom to ask. Once a driver
    /// stops, the slot keeps the last leader it set. Set it yourself only on
    /// a `Net` no driver runs on.
    pub fn set_leader(&self, leader: Option<WorkerId>) {
        *self.leader.lock().unwrap_or_else(PoisonError::into_inner) = leader;
    }

    /// Asks the leader (see [`Self::set_leader`]) for permission to run
    /// `task_id` (`REQUEST_CLAIM`, README §8.2), and awaits its answer: an
    /// accepted `Claim` or a `ClaimReject`. See [`ClaimFailure`] for why
    /// there may be no answer.
    pub async fn request_claim(&self, task_id: TaskId) -> Result<ClaimResponse, ClaimFailure> {
        self.ask_leader(claim_request::Request::TaskId(task_id.into()))
            .await
    }

    /// Asks the leader (see [`Self::set_leader`]) for up to `limit` of the
    /// oldest pending tasks (`CLAIM_OLDEST`), and awaits its answer: a batch
    /// of claims, oldest task first, or a `ClaimReject`. The batch may hold
    /// fewer than `limit`, or none: the leader hands out only as many as fit
    /// in one message. See [`ClaimFailure`] for why there may be no answer.
    pub async fn claim_oldest(&self, limit: u32) -> Result<ClaimResponse, ClaimFailure> {
        self.ask_leader(claim_request::Request::Oldest(ClaimOldest { limit }))
            .await
    }

    async fn ask_leader(
        &self,
        request: claim_request::Request,
    ) -> Result<ClaimResponse, ClaimFailure> {
        let leader = self
            .leader
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
            .ok_or(ClaimFailure::NoLeader)?;
        if leader == self.local_worker_id {
            return Err(ClaimFailure::ThisWorkerLeads);
        }
        let to = PeerId::from_str(leader.as_str()).map_err(|_| ClaimFailure::Unanswered)?;
        let (respond_to, response) = oneshot::channel();
        self.commands
            .send(Command::SendClaimRequest {
                to,
                request: ClaimRequest {
                    request: Some(request),
                },
                respond_to,
            })
            .map_err(|_| ClaimFailure::Unanswered)?;
        response
            .await
            .ok()
            .flatten()
            .ok_or(ClaimFailure::Unanswered)
    }

    /// One pass of the bootstrap join (README §27 Phase 2, spec decision 5
    /// step (a)): asks each of `peers` in order who leads the shard, and
    /// returns [`LeaderSearch::Found`] with the first `JOIN_RESPONSE` that
    /// points at a leader this node is then connected to; the caller hands
    /// it straight to `core::election::WorkerNode::finish_joining`. A peer
    /// that fails to connect or answer within `per_peer_timeout`, answers "no
    /// leader known", or points at a leader this node cannot reach (see
    /// `Self::connect_to_leader`), is passed over for the next one.
    ///
    /// Otherwise the pass says whether anyone answered at all:
    /// [`LeaderSearch::NoReachableLeader`] when some peer did (even "no
    /// leader known", or a pointer that does not parse or cannot be
    /// reached), which shows the shard exists; [`LeaderSearch::NoAnswer`]
    /// when none did. Asking again is the caller's (see
    /// `crate::bootstrap`). A peer asked again is asked over the connection
    /// this node already has to it, if that is still up, rather than dialed
    /// afresh.
    pub async fn ask_for_leader(
        &self,
        peers: &[Multiaddr],
        per_peer_timeout: StdDuration,
    ) -> LeaderSearch {
        let mut a_peer_answered = false;
        for peer in peers {
            let Some(response) = self.ask_peer_for_leader(peer, per_peer_timeout).await else {
                continue;
            };
            a_peer_answered = true;
            let Some((leader, leader_addr)) = pointed_leader(&response) else {
                continue;
            };
            if self
                .connect_to_leader(&leader, leader_addr, per_peer_timeout)
                .await
            {
                return LeaderSearch::Found(response);
            }
        }
        if a_peer_answered {
            LeaderSearch::NoReachableLeader
        } else {
            LeaderSearch::NoAnswer
        }
    }

    /// One peer of `Self::ask_for_leader`'s pass: send `JOIN_REQUEST` to the
    /// peer at `address` and await the response, bounded by
    /// `per_peer_timeout`. While the peer an earlier ask found at `address`
    /// is still connected, it is asked directly. Otherwise `address` is
    /// dialed and this waits, also bounded by `per_peer_timeout`, for *that
    /// dial's own* connection (see `Self::dial_for_connection` — the
    /// resulting peer is identified by the dial's `ConnectionId`, not by
    /// diffing the connected-peer set, so a late connection from an
    /// already-abandoned earlier attempt can never be misattributed here),
    /// and records the peer it finds there.
    async fn ask_peer_for_leader(
        &self,
        address: &Multiaddr,
        per_peer_timeout: StdDuration,
    ) -> Option<JoinResponse> {
        let asked_before = self
            .asked_peers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(address)
            .copied();
        // A peer this node already holds a connection to at `address` is
        // asked over it: dialing `address` afresh would fail, since the TCP
        // transport dials from its listen port and the new connection would
        // reuse the live one's address pair (`EADDRINUSE`). A worker fenced
        // for losing the authority alone keeps its connections, and rejoins
        // through the addresses its peers registered.
        let peer = match asked_before
            .filter(|peer| self.is_connected(peer))
            .or_else(|| self.connected_peer_at(address))
        {
            Some(peer) if self.is_connected(&peer) => peer,
            _ => {
                let opts = DialOpts::unknown_peer_id().address(address.clone()).build();
                let dialed = tokio::time::timeout(per_peer_timeout, self.dial_for_connection(opts))
                    .await
                    .ok()
                    .flatten();
                let Some(peer) = dialed else {
                    // Nothing answers there now; forget whoever did.
                    self.asked_peers
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .remove(address);
                    return None;
                };
                self.asked_peers
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(address.clone(), peer);
                peer
            }
        };

        tokio::time::timeout(per_peer_timeout, self.send_join_request(peer))
            .await
            .ok()?
    }

    /// Whether this node ends up connected to `leader`: at once if it already
    /// is (the leader was the seed that answered, say); otherwise by dialing
    /// `leader` at `leader_addr`, and any address `kad`'s routing table
    /// separately knows for it (see `crate::swarm`'s "kad: peer routing, not
    /// membership" — `DialOpts::extend_addresses_through_behaviour` is what
    /// asks for that here; `WithPeerIdWithAddresses::addresses` alone
    /// defaults it off), and waiting, bounded by `per_peer_timeout`, for
    /// *that dial's own* connection (see `Self::dial_for_connection`). So a
    /// `leader_addr` that is loopback or otherwise unreachable from here is
    /// not the only way to reach `leader`: a `kad` entry for it, learned from
    /// any other peer's Identify, gives this dial a second address to try.
    ///
    /// The dial names `leader`'s peer id, so if some other peer answers at
    /// `leader_addr` (a leader restarted under a new identity, or the address
    /// reused), libp2p refuses it as `WrongPeerId` and closes that
    /// connection. A stale pointer asked about on every pass of
    /// `Self::ask_for_leader` therefore costs one failed dial per pass, never
    /// a connection left open.
    async fn connect_to_leader(
        &self,
        leader: &WorkerId,
        leader_addr: Multiaddr,
        per_peer_timeout: StdDuration,
    ) -> bool {
        let Ok(leader_peer) = PeerId::from_str(leader.as_str()) else {
            return false;
        };
        if self.is_connected(&leader_peer) {
            return true;
        }
        let opts = DialOpts::peer_id(leader_peer)
            .addresses(vec![leader_addr])
            .extend_addresses_through_behaviour()
            .build();
        let dialed = tokio::time::timeout(per_peer_timeout, self.dial_for_connection(opts))
            .await
            .ok()
            .flatten();
        dialed == Some(leader_peer)
    }

    /// A connected peer whose address of record is `address`.
    fn connected_peer_at(&self, address: &Multiaddr) -> Option<PeerId> {
        let addresses = self
            .peer_addresses
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        addresses
            .iter()
            .find(|(peer, known)| known.addr == *address && self.is_connected(peer))
            .map(|(peer, _)| *peer)
    }

    fn is_connected(&self, peer: &PeerId) -> bool {
        self.connected
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(peer)
    }
}

/// The leader a `JOIN_RESPONSE` points at and the address to dial it on.
/// `None` for "no leader known" (see join.proto), and for an address that
/// does not parse — which `Net::ask_for_leader` treats like "no leader
/// known": the peer did answer, so the shard exists. The codec has
/// already rejected a leader without an address (`WellFormed`), so a named
/// leader always comes with some string.
fn pointed_leader(response: &JoinResponse) -> Option<(WorkerId, Multiaddr)> {
    let leader = response.leader_id()?;
    let leader_addr = response.leader_multiaddr.parse().ok()?;
    Some((leader, leader_addr))
}

impl Drop for Net {
    fn drop(&mut self) {
        // The driver task holds no state worth flushing on shutdown (an
        // in-flight send is already best-effort); abort it outright rather
        // than negotiating a graceful stop.
        self.driver.abort();
    }
}

impl Net {
    /// Sends `message` to `to`, fire-and-forget: delivery need not be
    /// immediate or reliable, and nothing reports whether it arrived. A `to`
    /// that is not a `WorkerId` this `Net`'s mapping produces goes nowhere.
    ///
    /// With no connection to `to`, this dials it first, at its dialable
    /// address of record (see [`Self::dialable_address`]) and any address
    /// the swarm's behaviours know for it. A roll call reply carries this
    /// node's own address (see the module doc's "Where a peer's address comes
    /// from").
    pub fn send(&self, to: WorkerId, message: ElectionMessage) {
        let Ok(peer) = PeerId::from_str(to.as_str()) else {
            // Not a WorkerId this mapping ever produced: nowhere to send.
            return;
        };
        // If the driver task has already stopped (this Net is concurrently
        // being dropped), there is nowhere for the command to go, and that
        // is fine to drop.
        self.messages_sent.fetch_add(1, Ordering::Relaxed);
        let _ = self.commands.send(Command::Send {
            to: peer,
            message: self.with_own_address(message),
        });
    }

    /// Subscribes this `Net` to `shard`'s gossip topic, so it receives what
    /// the shard's workers publish and [`Self::publish`] reaches them.
    /// Subscribing again to the same shard changes nothing. A `Net` serves
    /// one shard: subscribing to a second, different one is a caller bug,
    /// which fails a debug assertion and is otherwise ignored.
    pub fn subscribe_to_shard(&self, shard: &ShardId) {
        let mut subscribed = self.shard.lock().unwrap_or_else(PoisonError::into_inner);
        match &*subscribed {
            Some(current) => debug_assert_eq!(
                current, shard,
                "a Net serves one shard; it cannot subscribe to a second"
            ),
            None => {
                *subscribed = Some(shard.clone());
                // Same reasoning as in `send` for a stopped driver task.
                let _ = self.commands.send(Command::Subscribe {
                    topic: shard_topic(shard),
                });
            }
        }
    }

    /// Publishes `message` to every worker subscribed to this `Net`'s shard,
    /// fire-and-forget like [`Self::send`]: it may reach some of them and
    /// not others, and nothing reports which. Before
    /// [`Self::subscribe_to_shard`] there is no shard to publish to, and the
    /// message goes nowhere.
    pub fn publish(&self, message: ElectionMessage) {
        let Some(shard) = self
            .shard
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
        else {
            return;
        };
        self.messages_published.fetch_add(1, Ordering::Relaxed);
        // Same reasoning as in `send` for a stopped driver task.
        let _ = self.commands.send(Command::Publish {
            topic: shard_topic(&shard).hash(),
            message: self.with_own_address(message),
        });
    }

    /// `message` with this node's own address (see [`Self::local_multiaddr`])
    /// stamped on it where it carries one (see [`stamp_own_address`]). Left
    /// unstamped while this `Net` listens on nothing.
    fn with_own_address(&self, mut message: ElectionMessage) -> ElectionMessage {
        if let Some(own) = self.local_multiaddr() {
            stamp_own_address(&mut message, &own);
        }
        message
    }

    /// The peers this `Net` knows to be subscribed to its shard's gossip
    /// topic: those connected peers whose subscription has reached it. Empty
    /// before [`Self::subscribe_to_shard`].
    pub fn shard_subscribers(&self) -> BTreeSet<WorkerId> {
        self.shard_subscribers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(worker_id_of)
            .collect()
    }

    /// The peers in this `Net`'s gossip mesh for its shard: the connected
    /// subscribers its shard's gossip actually runs through, whose dropped
    /// connection the redial policy repairs (see the module doc's "Which
    /// drops are redial-eligible"). Empty before
    /// [`Self::subscribe_to_shard`]. It is gossipsub's mesh over every topic
    /// this `Net` joined, which is its shard's alone.
    pub fn shard_mesh(&self) -> BTreeSet<WorkerId> {
        self.shard_mesh
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(worker_id_of)
            .collect()
    }

    /// Takes every input for this worker's node queued since the last call,
    /// oldest first: each election message received, each peer this `Net`
    /// first connected to, and each peer it last lost its connection to or
    /// failed to dial while holding none. Inputs queue from the moment this
    /// `Net` is created, so a node built later, or driven again after a
    /// pause, is told of everything that happened in between, bar what the
    /// queue's bound leaves out (see the module doc's "Inputs for the
    /// node"): connection events that changed nothing, and the oldest
    /// messages past [`Self::with_input_limit`].
    pub fn take_inputs(&self) -> Vec<Input> {
        drain(&self.inbound.inputs)
    }

    /// Re-crawls this node's peer routing: starts a `kad` bootstrap, which
    /// asks the peers this node knows for the peers closest to it and
    /// connects to those it finds (see `crate::swarm`'s "kad: peer routing,
    /// not membership"). Gossipsub then meshes with the connected peers that
    /// serve the same shard, so a roll call published after the leader is
    /// lost still reaches them. `crate::driver::run_driver` calls it when
    /// its node's view of the shard changes and periodically (see
    /// [`Self::with_routing_refresh_period`]). Fire-and-forget; with no peer
    /// known yet it does nothing.
    pub(crate) fn refresh_peer_routing(&self) {
        let _ = self.commands.send(Command::RefreshPeerRouting);
    }

    /// Sets how often `crate::driver::run_driver` re-crawls peer routing
    /// (see [`Self::refresh_peer_routing`]) while nothing else prompts it.
    /// By default it is [`DEFAULT_ROUTING_REFRESH_SUSPICIONS`] of the
    /// node's suspicion timeouts. Either way it is never less than
    /// [`MIN_ROUTING_REFRESH_PERIOD`]: a crawl every instant would only
    /// spin.
    #[must_use]
    pub fn with_routing_refresh_period(mut self, period: StdDuration) -> Self {
        self.routing_refresh_period = Some(period);
        self
    }

    /// The period set by [`Self::with_routing_refresh_period`], if any.
    pub(crate) fn routing_refresh_period(&self) -> Option<StdDuration> {
        self.routing_refresh_period
    }

    /// Asks this worker's node to leave its shard gracefully (README §12.3,
    /// ADR-0001 decision 10): queues `Input::Drain` for it, which the driver
    /// hands it in its next batch like any other input (see
    /// `kabudachi_core::election::Input::Drain` for what the node then
    /// does). This is how a worker being shut down, one replaced in a rolling
    /// deploy say, leaves without waiting to be suspected. Ask once: the
    /// node ignores a second request.
    pub fn request_drain(&self) {
        self.inbound.queue_input(Input::Drain);
    }

    /// What this `Net` has carried since it was created (see [`Traffic`]).
    /// For observability: nothing here depends on it.
    pub fn traffic(&self) -> Traffic {
        Traffic {
            messages_received: self.inbound.messages_received.load(Ordering::Relaxed),
            messages_sent: self.messages_sent.load(Ordering::Relaxed),
            messages_published: self.messages_published.load(Ordering::Relaxed),
            join_requests_received: self.inbound.join_requests_received.load(Ordering::Relaxed),
            claim_requests_received: self.inbound.claim_requests_received.load(Ordering::Relaxed),
        }
    }

    /// Waits until something has arrived for this worker's driver since this
    /// last returned: an input for its node, a join request or a claim
    /// request. An arrival while no one waits is kept, so a caller that
    /// takes everything queued after each wait misses nothing; that also
    /// means this can return when what arrived was already taken. Wait from
    /// one task at a time.
    pub async fn wait_for_arrival(&self) {
        self.inbound.arrived.notified().await;
    }
}

/// `peer`'s `WorkerId` (see the module doc's `WorkerId <-> PeerId` mapping).
fn worker_id_of(peer: &PeerId) -> WorkerId {
    WorkerId::new(peer.to_string())
}

/// The gossip topic `shard`'s workers publish election messages on. Every
/// worker in the shard must name it the same way, or they cannot hear each
/// other. It carries the same `ElectionMessage` schema as the direct
/// protocol ([`crate::codec::PROTOCOL`]), so its version moves in step with
/// that protocol's: a peer of another schema version never hears a message
/// it would misread.
fn shard_topic(shard: &ShardId) -> gossipsub::IdentTopic {
    gossipsub::IdentTopic::new(format!("/kabudachi/{}/election/2", shard.as_str()))
}

/// The input an arriving gossip `message` is for this worker's node: a
/// message from its author (see the module doc's "Gossip"). `None`, and the
/// message dropped, when it names no author or its body is not a well-formed
/// `ElectionMessage`.
fn gossip_input(message: gossipsub::Message) -> Option<Input> {
    let Some(author) = message.source else {
        tracing::debug!(topic = %message.topic, "dropping a gossip message with no author");
        return None;
    };
    match decode_well_formed::<ElectionMessage>(&message.data) {
        Ok(election_message) => Some(Input::Message {
            from: worker_id_of(&author),
            message: election_message,
        }),
        Err(error) => {
            tracing::debug!(
                %author,
                %error,
                "dropping a gossip message that is not a well-formed election message"
            );
            None
        }
    }
}

/// Writes `own` into `message` as its sender's address, if `message` is a
/// roll call or a roll call reply: the two messages whose recipient may hold
/// no connection to the sender and still has to answer it directly (see the
/// module doc's "Where a peer's address comes from"). Every other message is
/// left as it is.
fn stamp_own_address(message: &mut ElectionMessage, own: &Multiaddr) {
    match &mut message.payload {
        Some(election_message::Payload::RollCall(call)) => {
            call.initiator_address = own.to_string();
        }
        Some(election_message::Payload::RollCallReply(reply)) => {
            reply.responder_address = own.to_string();
        }
        _ => {}
    }
}

/// The address `from` stamped on `message` for itself: the initiator's
/// address on a roll call `from` initiated, or the responder's on a reply
/// `from` wrote. `None` for any other message, for a stamp that names
/// someone other than `from` (a peer relaying, or lying about, another
/// worker's message must not redirect traffic meant for it), and for a stamp
/// that is empty, does not parse, or is a wildcard bind.
fn stamped_address(from: &WorkerId, message: &ElectionMessage) -> Option<Multiaddr> {
    let (author, stamp) = match message.payload.as_ref()? {
        election_message::Payload::RollCall(call) => {
            (call.initiator_id(), call.initiator_address.as_str())
        }
        election_message::Payload::RollCallReply(reply) => {
            (reply.responder_id(), reply.responder_address.as_str())
        }
        _ => return None,
    };
    if author != *from {
        return None;
    }
    let address: Multiaddr = stamp.parse().ok()?;
    // An empty string parses, as the empty multiaddr.
    (!address.is_empty() && is_dialable_listen_addr(&address)).then_some(address)
}

/// Records, as its sender's address, the address an arriving message's
/// sender stamped on it for itself (see [`stamped_address`]). The sender is
/// the one `Net` vouches for: a gossip message's signed author, or the peer
/// at the other end of the connection a direct message came over.
fn record_stamped_address(
    peer_addresses: &Arc<Mutex<BTreeMap<PeerId, KnownAddress>>>,
    input: &Input,
) {
    let Input::Message { from, message } = input else {
        return;
    };
    let Some(address) = stamped_address(from, message) else {
        return;
    };
    let Ok(peer) = PeerId::from_str(from.as_str()) else {
        return;
    };
    record_peer_address(peer_addresses, peer, address, AddressSource::SelfStamped);
}

/// Owns `swarm` exclusively and polls it forever, applying commands and
/// keeping `inbound`/`connected`/`peer_addresses`/`local_addr`/
/// `shard_subscribers` current for `Net`'s methods to read.
#[allow(clippy::too_many_arguments)]
async fn drive(
    mut swarm: Swarm<Behaviour>,
    mut commands: mpsc::UnboundedReceiver<Command>,
    inbound: Arc<Inbound>,
    connected: Arc<Mutex<BTreeSet<PeerId>>>,
    peer_addresses: Arc<Mutex<BTreeMap<PeerId, KnownAddress>>>,
    local_addr: Arc<Mutex<Option<Multiaddr>>>,
    redial_attempts_snapshot: Arc<Mutex<BTreeMap<PeerId, u32>>>,
    shard_subscribers: Arc<Mutex<BTreeSet<PeerId>>>,
    shard_mesh: Arc<Mutex<BTreeSet<PeerId>>>,
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
    // The gossip mesh as of the previous iteration, for the same reason:
    // by the time a drop shows in the connected set, gossipsub has already
    // taken the peer out of its mesh.
    let mut previously_meshed: BTreeSet<PeerId> = BTreeSet::new();
    let mut redial_ticker = tokio::time::interval(redial_policy.check_interval);
    redial_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            command = commands.recv() => {
                match command {
                    Some(Command::Send { to, message }) => {
                        // Over a connection to `to` if there is one; else
                        // libp2p dials `to` at this address of record (one
                        // stamped on a roll call from a peer this node never
                        // connected to, say), along with any its behaviours
                        // know.
                        let address = dialable_address_of(&peer_addresses, &to);
                        swarm.behaviour_mut().request_response.send_request_with_addresses(
                            &to,
                            message,
                            address.into_iter().collect(),
                        );
                    }
                    Some(Command::Subscribe { topic }) => {
                        if let Err(error) = swarm.behaviour_mut().gossipsub.subscribe(&topic) {
                            tracing::warn!(
                                %topic,
                                %error,
                                "could not subscribe to the shard's gossip topic"
                            );
                        }
                    }
                    Some(Command::Publish { topic, message }) => {
                        // Lost like any other unreliable message (see the
                        // module doc's "Gossip").
                        if let Err(error) = swarm
                            .behaviour_mut()
                            .gossipsub
                            .publish(topic.clone(), message.encode_to_vec())
                        {
                            tracing::debug!(%topic, %error, "dropping a gossip publish");
                        }
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
                    Some(Command::RefreshPeerRouting) => {
                        // `NoKnownPeers`: nothing to crawl from yet; the
                        // next refresh tries again.
                        let _ = swarm.behaviour_mut().kad.bootstrap();
                    }
                    Some(Command::Block { peer, blocked }) => {
                        let blocklist = &mut swarm.behaviour_mut().blocked;
                        if blocked {
                            blocklist.block_peer(peer);
                        } else {
                            blocklist.unblock_peer(peer);
                        }
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
                    &inbound,
                    &peer_addresses,
                    &local_addr,
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
                    // Also asks kad's routing table for an address of its
                    // own, same reasoning as connect_to_leader: the
                    // last-known address is not the only one that might
                    // still reach peer.
                    let _ = swarm.dial(
                        DialOpts::peer_id(peer)
                            .addresses(vec![attempt.addr.clone()])
                            .extend_addresses_through_behaviour()
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

        // Anyone who just dropped (connected last iteration, not now), was
        // in this node's gossip mesh, and wasn't a local Command::Disconnect
        // becomes redial-eligible (see the module doc's "Which drops are
        // redial-eligible"), using whatever address Self::peer_addresses
        // still has on file for them (see that method's doc: a disconnected
        // peer's last-known address is deliberately retained, exactly for
        // this). No address on file (never connected, or somehow never
        // recorded) means nothing to redial with — skipped, not an error.
        for peer in previously_connected.difference(&newly_connected) {
            if locally_disconnected.contains(peer) || !previously_meshed.contains(peer) {
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

        let gossipsub = &swarm.behaviour().gossipsub;
        previously_meshed = gossipsub.all_mesh_peers().copied().collect();
        *shard_mesh.lock().unwrap_or_else(PoisonError::into_inner) = previously_meshed.clone();
        let my_topics: BTreeSet<&gossipsub::TopicHash> = gossipsub.topics().collect();
        *shard_subscribers
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = gossipsub
            .all_peers()
            .filter(|(_, topics)| topics.iter().any(|topic| my_topics.contains(topic)))
            .map(|(peer, _)| *peer)
            .collect();
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
    inbound: &Inbound,
    peer_addresses: &Arc<Mutex<BTreeMap<PeerId, KnownAddress>>>,
    local_addr: &Arc<Mutex<Option<Multiaddr>>>,
) {
    match event {
        SwarmEvent::NewListenAddr {
            listener_id,
            address,
        } => {
            // A node listening on a wildcard bind gets one of these per
            // interface, in no set order, loopback among them. A later
            // address replaces the recorded one unless that would swap a
            // non-loopback address for loopback.
            {
                let mut recorded = local_addr.lock().unwrap_or_else(PoisonError::into_inner);
                let previous = recorded.take();
                *recorded = preferred_address(std::iter::once(address.clone()).chain(previous));
            }
            if let Some(respond_to) = pending_listens.remove(&listener_id) {
                let _ = respond_to.send(address);
            }
        }
        SwarmEvent::ConnectionEstablished {
            peer_id,
            connection_id,
            endpoint,
            num_established,
            ..
        } => {
            if num_established.get() == 1 {
                inbound.queue_input(Input::PeerConnected(worker_id_of(&peer_id)));
            }
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
            // for which of the advertised addresses is taken.
            //
            // Every dialable one, not just the one taken below, feeds `kad`
            // (see `crate::swarm`'s "kad: peer routing, not membership"):
            // Kademlia keeps several addresses per peer, and it is the peer
            // routing this crate wants from it, not this node's own
            // one-address-of-record bookkeeping.
            for addr in info
                .listen_addrs
                .iter()
                .filter(|addr| is_dialable_listen_addr(addr))
            {
                swarm.behaviour_mut().kad.add_address(&peer_id, addr.clone());
            }
            if let Some(addr) = preferred_address(info.listen_addrs) {
                record_peer_address(peer_addresses, peer_id, addr, AddressSource::Identify);
            }
        }
        SwarmEvent::ConnectionClosed {
            peer_id,
            num_established: 0,
            ..
        } => {
            inbound.queue_input(Input::PeerDisconnected(worker_id_of(&peer_id)));
        }
        SwarmEvent::OutgoingConnectionError {
            connection_id,
            peer_id,
            ..
        } => {
            if let Some(respond_to) = pending_dials.remove(&connection_id) {
                let _ = respond_to.send(None);
            }
            // A dial that names its peer and fails while no other connection
            // to that peer is open: the peer cannot be reached.
            if let Some(peer) = peer_id
                && !swarm.is_connected(&peer)
            {
                inbound.queue_input(Input::PeerDisconnected(worker_id_of(&peer)));
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
            let input = Input::Message {
                from: worker_id_of(&peer),
                message: request,
            };
            // Recorded before the node is told, so a direct answer the node
            // sends finds the address on file.
            record_stamped_address(peer_addresses, &input);
            inbound.queue_input(input);
            // Best-effort: nothing reads this ack back (see module doc), and
            // a channel that is already closed just means the peer stopped
            // waiting on it.
            let _ = swarm
                .behaviour_mut()
                .request_response
                .send_response(channel, Ack);
        }
        SwarmEvent::Behaviour(BehaviourEvent::Gossipsub(gossipsub::Event::Message {
            message,
            ..
        })) => {
            if let Some(input) = gossip_input(message) {
                // Same ordering as for a direct message above.
                record_stamped_address(peer_addresses, &input);
                inbound.queue_input(input);
            }
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
            inbound.queue_join_request(JoinRequestHandle {
                from: worker_id_of(&peer),
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
            inbound.queue_claim_request(ClaimRequestHandle {
                from: worker_id_of(&peer),
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

    use kabudachi_core::configuration::Configuration;
    use kabudachi_core::protocol::ids::{IncarnationId, ShardId, TaskRunId};
    use kabudachi_core::protocol::messages::{
        Claim, ClaimReject, ClaimRejectReason, RollCall, RollCallReply, WorkerHeartbeat,
        claim_response, election_message,
    };
    use libp2p::identity;
    use tokio::time::timeout;

    use super::*;
    use crate::swarm::build_swarm;

    /// The `WorkerId` of a fresh keypair no `Net` was ever built from.
    fn worker_that_never_runs() -> WorkerId {
        let peer = identity::Keypair::generate_ed25519().public().to_peer_id();
        WorkerId::new(peer.to_string())
    }

    fn pointer_to(leader: &WorkerId, leader_addr: &Multiaddr) -> JoinResponse {
        JoinResponse {
            leader_id: Some(leader.clone().into()),
            leader_multiaddr: leader_addr.to_string(),
            term: 1,
            recovery_epoch: 0,
            recovery_epoch_lineage: 0,
        }
    }

    #[test]
    fn pointed_leader_needs_a_leader_and_an_address_that_parses() {
        let leader = WorkerId::new("leader-1");
        let leader_addr: Multiaddr = "/ip4/127.0.0.1/tcp/1".parse().unwrap();
        let pointer = pointer_to(&leader, &leader_addr);

        assert_eq!(pointed_leader(&pointer), Some((leader, leader_addr)));
        assert_eq!(pointed_leader(&JoinResponse::default()), None);
        let garbled = JoinResponse {
            leader_multiaddr: "not a multiaddr".into(),
            ..pointer
        };
        assert_eq!(pointed_leader(&garbled), None);
    }

    const TEST_TIMEOUT: Duration = Duration::from_secs(10);

    fn queued_inputs(inbound: &Inbound) -> Vec<Input> {
        drain(&inbound.inputs)
    }

    #[test]
    fn connection_events_that_change_nothing_are_not_queued() {
        let inbound = Inbound::default();
        let (p, q) = (WorkerId::new("p"), WorkerId::new("q"));
        let message = Input::Message {
            from: p.clone(),
            message: heartbeat_message(&p),
        };

        // Connected and gone again before anything was taken: nothing left
        // to hear about `p` but its message.
        inbound.queue_input(Input::PeerConnected(p.clone()));
        inbound.queue_input(message.clone());
        inbound.queue_input(Input::PeerDisconnected(p.clone()));
        // Failed dials repeat a disconnect the node would ignore.
        inbound.queue_input(Input::PeerDisconnected(q.clone()));
        inbound.queue_input(Input::PeerDisconnected(q.clone()));
        assert_eq!(
            queued_inputs(&inbound),
            vec![message, Input::PeerDisconnected(q.clone())]
        );

        // A reconnect after a disconnect the node has heard is news.
        inbound.queue_input(Input::PeerConnected(q.clone()));
        inbound.queue_input(Input::PeerDisconnected(q.clone()));
        inbound.queue_input(Input::PeerConnected(q.clone()));
        assert_eq!(queued_inputs(&inbound), vec![Input::PeerConnected(q)]);
    }

    #[test]
    fn past_its_limit_the_queue_drops_its_oldest_message_but_no_connection_event() {
        let inbound = Inbound::default();
        inbound.input_limit.store(2, Ordering::Relaxed);
        let message_from = |worker: &str| {
            let worker = WorkerId::new(worker);
            Input::Message {
                from: worker.clone(),
                message: heartbeat_message(&worker),
            }
        };
        let connected = Input::PeerConnected(WorkerId::new("p"));

        inbound.queue_input(message_from("first"));
        inbound.queue_input(connected.clone());
        inbound.queue_input(message_from("second"));
        inbound.queue_input(message_from("third"));

        assert_eq!(
            queued_inputs(&inbound),
            vec![connected, message_from("third")]
        );
    }

    fn heartbeat_message(worker: &WorkerId) -> ElectionMessage {
        ElectionMessage {
            payload: Some(election_message::Payload::Heartbeat(WorkerHeartbeat {
                worker_id: Some(worker.clone().into()),
                incarnation_id: Some(IncarnationId::new("incarnation-1").into()),
                recovery_epoch_seen: 0,
                term_seen: 0,
                available_capacity: 4,
                active_task_runs_digest: vec![],
                shard_id: Some(ShardId::new("shard-1").into()),
                newest_accepted_ack: None,
                configuration_generation: None,
                send_token: 0,
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

    /// Takes `net`'s queued inputs until they have included every one of
    /// `expected`, in that order (others may come in between), failing the
    /// test with `what` if they do not within the timeout. Inputs taken
    /// along with the last expected one are dropped.
    async fn expect_inputs(net: &Net, expected: &[Input], what: &str) {
        let mut remaining = expected.iter().peekable();
        let all_arrived = async {
            loop {
                for input in net.take_inputs() {
                    if remaining.peek() == Some(&&input) {
                        remaining.next();
                    }
                }
                if remaining.peek().is_none() {
                    return;
                }
                net.wait_for_arrival().await;
            }
        };
        timeout(TEST_TIMEOUT, all_arrived)
            .await
            .unwrap_or_else(|_| panic!("{what} within the timeout"));
    }

    async fn expect_input(net: &Net, expected: Input, what: &str) {
        expect_inputs(net, &[expected], what).await;
    }

    /// Connects `net_a` and `net_b` over a real loopback TCP socket, waits
    /// until each has reported the connection, and returns each side's
    /// `WorkerId`.
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
        expect_input(
            net_a,
            Input::PeerConnected(worker_b.clone()),
            "net_a reported its connection to net_b",
        )
        .await;
        expect_input(
            net_b,
            Input::PeerConnected(worker_a.clone()),
            "net_b reported its connection to net_a",
        )
        .await;

        (worker_a, worker_b)
    }

    #[tokio::test]
    async fn a_sent_heartbeat_arrives_as_a_message_input_over_real_sockets() {
        let net_a = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let net_b = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let (worker_a, worker_b) = connected_pair(&net_a, &net_b).await;

        let heartbeat = heartbeat_message(&worker_a);
        net_a.send(worker_b, heartbeat.clone());

        expect_input(
            &net_b,
            Input::Message {
                from: worker_a,
                message: heartbeat,
            },
            "net_b received the heartbeat",
        )
        .await;
    }

    #[tokio::test]
    async fn a_new_connection_yields_peer_connected_on_both_sides() {
        let net_a = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let net_b = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let listen_addr = timeout(
            TEST_TIMEOUT,
            net_a.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
        )
        .await
        .expect("net_a produced a listen address within the timeout");

        net_b.dial(listen_addr);

        expect_input(
            &net_a,
            Input::PeerConnected(net_b.local_worker_id()),
            "the listening side reported the new connection",
        )
        .await;
        expect_input(
            &net_b,
            Input::PeerConnected(net_a.local_worker_id()),
            "the dialing side reported the new connection",
        )
        .await;
    }

    #[tokio::test]
    async fn a_peer_that_goes_away_yields_peer_disconnected() {
        let net_a = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let net_b = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let (_, worker_b) = connected_pair(&net_a, &net_b).await;

        drop(net_b); // aborts net_b's driver task, closing its connection

        expect_input(
            &net_a,
            Input::PeerDisconnected(worker_b),
            "net_a reported the connection to the vanished net_b closed",
        )
        .await;
    }

    #[tokio::test]
    async fn disconnect_yields_peer_disconnected_on_both_sides() {
        // Unlike a peer going away entirely, both `Net`s and their driver
        // tasks stay alive here: only the connection closes. The side that
        // closes it reports that too, not only the side it was closed on.
        let net_a = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let net_b = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let (worker_a, worker_b) = connected_pair(&net_a, &net_b).await;

        net_a.disconnect(worker_b.clone());

        expect_input(
            &net_a,
            Input::PeerDisconnected(worker_b.clone()),
            "net_a reported the connection it closed",
        )
        .await;
        expect_input(
            &net_b,
            Input::PeerDisconnected(worker_a),
            "net_b reported the connection net_a closed: disconnect_peer_id tears down the \
             real transport connection, not just net_a's local bookkeeping",
        )
        .await;

        // Both driver tasks are still alive and usable: a fresh connection
        // is reported again.
        let listen_addr = timeout(
            TEST_TIMEOUT,
            net_a.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
        )
        .await
        .expect("net_a's driver task is still alive and can listen again after disconnecting");
        net_b.dial(listen_addr);
        expect_input(
            &net_a,
            Input::PeerConnected(worker_b),
            "net_a accepted and reported a fresh connection from net_b",
        )
        .await;
    }

    #[tokio::test]
    async fn a_failed_dial_to_a_named_peer_yields_peer_disconnected() {
        let net_a = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        // A port that was just bound and released, so nothing listens on it.
        let dead_port = std::net::TcpListener::bind("127.0.0.1:0")
            .and_then(|listener| listener.local_addr())
            .expect("the OS lent this test a loopback port")
            .port();
        let absent_peer = worker_that_never_runs();
        let dead_addr: Multiaddr = format!(
            "/ip4/127.0.0.1/tcp/{dead_port}/p2p/{}",
            absent_peer.as_str()
        )
        .parse()
        .unwrap();

        net_a.dial(dead_addr);

        expect_input(
            &net_a,
            Input::PeerDisconnected(absent_peer),
            "net_a reported that it could not reach the peer its dial named",
        )
        .await;
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

    /// Subscribes `nets` to one shard and waits until each has the other in
    /// its gossip mesh, which is what makes a drop between them
    /// redial-eligible.
    async fn meshed(net_a: &Net, net_b: &Net) {
        let shard = ShardId::new("shard-1");
        net_a.subscribe_to_shard(&shard);
        net_b.subscribe_to_shard(&shard);
        let (worker_a, worker_b) = (net_a.local_worker_id(), net_b.local_worker_id());
        timeout(
            TEST_TIMEOUT,
            wait_until(|| {
                net_a.shard_mesh().contains(&worker_b) && net_b.shard_mesh().contains(&worker_a)
            }),
        )
        .await
        .expect("the two Nets meshed on their shard's topic within the timeout");
    }

    #[tokio::test]
    async fn block_peer_isolates_a_peer_both_ways_until_unblocked() {
        let net_a = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let net_b = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let (worker_a, worker_b) = connected_pair(&net_a, &net_b).await;

        net_a.block_peer(worker_b.clone());
        expect_input(
            &net_b,
            Input::PeerDisconnected(worker_a.clone()),
            "net_b reported the connection net_a's block closed",
        )
        .await;

        // Neither side's sends get through while blocked:
        // unlike `disconnect`, which a later send undoes at once.
        let heartbeat = heartbeat_message(&worker_b);
        let deadline = tokio::time::Instant::now() + Duration::from_millis(300);
        while tokio::time::Instant::now() < deadline {
            net_b.send(worker_a.clone(), heartbeat.clone());
            net_a.send(worker_b.clone(), heartbeat_message(&worker_a));
            tokio::time::sleep(Duration::from_millis(20)).await;
            let arrived = net_a.take_inputs();
            assert!(
                !arrived.iter().any(|input| matches!(
                    input,
                    Input::PeerConnected(_) | Input::Message { .. }
                )),
                "a blocked peer reached net_a: {arrived:?}"
            );
            // net_b blocks nothing, so its own dial can look connected for
            // an instant before net_a refuses it, but nothing crosses.
            let arrived = net_b.take_inputs();
            assert!(
                !arrived
                    .iter()
                    .any(|input| matches!(input, Input::Message { .. })),
                "net_a's block let a message through to net_b: {arrived:?}"
            );
        }

        net_a.unblock_peer(worker_b.clone());
        // A send can race the unblock across the two Nets, so net_b keeps
        // sending until one arrives.
        let expected = Input::Message {
            from: worker_b,
            message: heartbeat.clone(),
        };
        timeout(TEST_TIMEOUT, async {
            loop {
                net_b.send(worker_a.clone(), heartbeat.clone());
                tokio::time::sleep(Duration::from_millis(20)).await;
                if net_a.take_inputs().contains(&expected) {
                    return;
                }
            }
        })
        .await
        .expect("net_a heard net_b again once it unblocked it");
    }

    #[tokio::test]
    async fn a_dropped_peer_outside_the_gossip_mesh_is_not_redialed() {
        // A JOIN seed or a kad crawl connection: this Net has an address on
        // file for the peer but no gossip runs through it, so an idle
        // timeout closing it is no failure to repair (E6-R2's churn).
        let net_a = Net::new_with_redial_policy(
            build_swarm(identity::Keypair::generate_ed25519()),
            short_redial_policy(),
        );
        let net_b = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let listen_addr_b = timeout(
            TEST_TIMEOUT,
            net_b.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
        )
        .await
        .expect("net_b produced a listen address within the timeout");
        net_a.dial(listen_addr_b);
        let (worker_a, worker_b) = (net_a.local_worker_id(), net_b.local_worker_id());
        expect_input(
            &net_a,
            Input::PeerConnected(worker_b.clone()),
            "net_a reported its connection to net_b",
        )
        .await;
        expect_input(
            &net_b,
            Input::PeerConnected(worker_a.clone()),
            "net_b reported its connection to net_a",
        )
        .await;

        net_b.disconnect(worker_a);
        expect_input(
            &net_a,
            Input::PeerDisconnected(worker_b.clone()),
            "net_a reported the drop",
        )
        .await;

        let deadline = tokio::time::Instant::now() + Duration::from_millis(300);
        while tokio::time::Instant::now() < deadline {
            assert!(
                !net_a.redial_attempts().contains_key(&worker_b),
                "net_a redialed a peer no gossip ran through"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
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
        let net_a = Net::new_with_redial_policy(
            build_swarm(identity::Keypair::generate_ed25519()),
            short_redial_policy(),
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
        expect_input(
            &net_a,
            Input::PeerConnected(worker_b.clone()),
            "net_a reported its connection to net_b",
        )
        .await;
        // net_b must hold the connection too. `disconnect` is a silent no-op
        // on a side that has not registered it yet, and then no drop ever
        // happens.
        expect_input(
            &net_b,
            Input::PeerConnected(worker_a.clone()),
            "net_b reported its connection to net_a",
        )
        .await;
        // Only a peer the shard's gossip runs through is worth redialing.
        meshed(&net_a, &net_b).await;

        net_b.disconnect(worker_a);

        // Inputs queue up rather than being sampled, so a drop that the
        // redial heals at once is still reported, ahead of the reconnection.
        expect_inputs(
            &net_a,
            &[
                Input::PeerDisconnected(worker_b.clone()),
                Input::PeerConnected(worker_b),
            ],
            "net_a reported the drop, then its own bounded redial policy reconnected to net_b \
             without any manual dial from the test itself,",
        )
        .await;
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
        let (_, worker_b) = connected_pair(&net_a, &net_b).await;
        meshed(&net_a, &net_b).await;

        // Process death, not a local disconnect: net_b is gone for good, and
        // net_a never asked for this drop, so it's organic/redial-eligible
        // (same simulation as a_peer_that_goes_away_yields_peer_disconnected
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
            !net_a
                .take_inputs()
                .contains(&Input::PeerConnected(worker_b)),
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
        let worker_b = net_b.local_worker_id();
        expect_input(
            &net_a,
            Input::PeerConnected(worker_b.clone()),
            "net_a reported its connection to net_b",
        )
        .await;
        // Meshed, so nothing but the exclusion keeps net_a from redialing.
        meshed(&net_a, &net_b).await;

        net_a.disconnect(worker_b.clone());

        expect_input(
            &net_a,
            Input::PeerDisconnected(worker_b.clone()),
            "net_a reported its own disconnect",
        )
        .await;

        // net_b never stops listening (`disconnect` only closes the
        // connection, not the listener), so nothing but the exclusion itself
        // stops net_a's short_redial_policy from scheduling a redial here.
        // Whether the two reconnect at all is not the redial policy's alone
        // to say: a `kad` query in flight may dial net_b on its own.
        let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
        while tokio::time::Instant::now() < deadline {
            assert!(
                !net_a.redial_attempts().contains_key(&worker_b),
                "net_a must never auto-redial a peer it locally disconnected"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
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

    #[tokio::test]
    async fn dialable_address_withholds_a_peer_known_only_by_its_inbound_source_address() {
        let net_a = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let net_b = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        // net_b dials net_a without listening itself, so all net_a knows of
        // net_b is the ephemeral source address of net_b's connection.
        let (worker_a, worker_b) = connected_pair(&net_a, &net_b).await;
        timeout(
            TEST_TIMEOUT,
            wait_until(|| net_a.peer_addresses().contains_key(&worker_b)),
        )
        .await
        .expect("net_a recorded net_b's source address within the timeout");

        assert_eq!(net_a.dialable_address(&worker_b), None);
        assert_eq!(net_b.dialable_address(&worker_a), net_a.local_multiaddr());

        let listen_addr_b = timeout(
            TEST_TIMEOUT,
            net_b.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
        )
        .await
        .expect("net_b produced a listen address within the timeout");
        timeout(
            TEST_TIMEOUT,
            wait_until(|| net_a.dialable_address(&worker_b) == Some(listen_addr_b.clone())),
        )
        .await
        .expect("net_a learned net_b's advertised listen address within the timeout");
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
    async fn ask_for_leader_passes_over_a_pointer_to_a_leader_it_cannot_reach() {
        let net_a = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let net_b = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let net_z = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let net_c = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let mut addrs = Vec::new();
        for net in [&net_a, &net_b, &net_z] {
            let addr = timeout(
                TEST_TIMEOUT,
                net.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
            )
            .await
            .expect("the net produced a listen address within the timeout");
            addrs.push(addr);
        }
        let (addr_a, addr_b, addr_z) = (addrs[0].clone(), addrs[1].clone(), addrs[2].clone());
        let worker_b = net_b.local_worker_id();

        // Seed A names a leader that never runs, at an address where some
        // other worker (net_z) answers: the dial connects, but not to that
        // leader.
        let absent_leader = worker_that_never_runs();
        let _responder_a = spawn_join_responder(net_a, pointer_to(&absent_leader, &addr_z));
        let response_b = pointer_to(&worker_b, &addr_b);
        let _responder_b = spawn_join_responder(net_b, response_b.clone());

        let pointer = timeout(
            TEST_TIMEOUT,
            net_c.ask_for_leader(&[addr_a, addr_b], Duration::from_secs(5)),
        )
        .await
        .expect("ask_for_leader completed within the test timeout");

        assert_eq!(pointer, LeaderSearch::Found(response_b));
        // A connection to the wrong peer is not kept: a stale pointer asked
        // about on every pass would otherwise pile up connections.
        let worker_z = net_z.local_worker_id();
        let mut still_connected_to_z = false;
        for input in net_c.take_inputs() {
            match input {
                Input::PeerConnected(peer) if peer == worker_z => still_connected_to_z = true,
                Input::PeerDisconnected(peer) if peer == worker_z => still_connected_to_z = false,
                _ => {}
            }
        }
        assert!(
            !still_connected_to_z,
            "net_c must not stay connected to net_z, which is not the leader it was pointed at"
        );
    }

    #[tokio::test]
    async fn a_pointer_only_to_an_undialable_leader_is_an_answer_with_no_reachable_leader() {
        let net_a = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let net_c = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let addr_a = timeout(
            TEST_TIMEOUT,
            net_a.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
        )
        .await
        .expect("net_a produced a listen address within the timeout");

        // Nothing listens at this address, so the pointed leader cannot be
        // reached; the seed did answer, so the shard exists.
        let unreachable_leader_addr: Multiaddr = "/ip4/127.0.0.1/tcp/1".parse().unwrap();
        let absent_leader = worker_that_never_runs();
        let _responder =
            spawn_join_responder(net_a, pointer_to(&absent_leader, &unreachable_leader_addr));

        let search = timeout(
            TEST_TIMEOUT,
            net_c.ask_for_leader(&[addr_a], Duration::from_secs(1)),
        )
        .await
        .expect("ask_for_leader completed within the test timeout");

        assert_eq!(search, LeaderSearch::NoReachableLeader);
    }

    #[tokio::test]
    async fn ask_for_leader_returns_the_seeds_leader_pointer() {
        let net_a = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let net_c = Net::new(build_swarm(identity::Keypair::generate_ed25519()));

        let listen_addr = timeout(
            TEST_TIMEOUT,
            net_a.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
        )
        .await
        .expect("net_a produced a listen address within the timeout");
        let worker_a = net_a.local_worker_id();

        let response = pointer_to(&worker_a, &listen_addr);
        let _responder = spawn_join_responder(net_a, response.clone());

        let pointer = timeout(
            TEST_TIMEOUT,
            net_c.ask_for_leader(&[listen_addr], Duration::from_secs(5)),
        )
        .await
        .expect("ask_for_leader completed within the test timeout");

        assert_eq!(pointer, LeaderSearch::Found(response));
    }

    #[tokio::test]
    async fn ask_for_leader_dials_the_leader_it_is_pointed_at() {
        let net_leader = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let net_a = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let net_c = Net::new(build_swarm(identity::Keypair::generate_ed25519()));

        let leader_addr = timeout(
            TEST_TIMEOUT,
            net_leader.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
        )
        .await
        .expect("net_leader produced a listen address within the timeout");
        let seed_addr = timeout(
            TEST_TIMEOUT,
            net_a.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
        )
        .await
        .expect("net_a produced a listen address within the timeout");
        let worker_leader = net_leader.local_worker_id();

        let _responder = spawn_join_responder(net_a, pointer_to(&worker_leader, &leader_addr));

        timeout(
            TEST_TIMEOUT,
            net_c.ask_for_leader(&[seed_addr], Duration::from_secs(5)),
        )
        .await
        .expect("ask_for_leader completed within the test timeout");

        expect_input(
            &net_c,
            Input::PeerConnected(worker_leader),
            "net_c connected to the leader it was pointed at",
        )
        .await;
    }

    #[tokio::test]
    async fn ask_for_leader_passes_over_a_seed_that_knows_no_leader() {
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
        let worker_b = net_b.local_worker_id();

        let _responder_a = spawn_join_responder(net_a, JoinResponse::default());
        let response_b = pointer_to(&worker_b, &addr_b);
        let _responder_b = spawn_join_responder(net_b, response_b.clone());

        let pointer = timeout(
            TEST_TIMEOUT,
            net_c.ask_for_leader(&[addr_a, addr_b], Duration::from_secs(5)),
        )
        .await
        .expect("ask_for_leader completed within the test timeout");

        assert_eq!(pointer, LeaderSearch::Found(response_b));
    }

    #[tokio::test]
    async fn ask_for_leader_returns_none_when_the_only_seed_never_responds() {
        let net_c = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        // Nothing listens here, so dialing it fails to connect.
        let unreachable_seed: Multiaddr = "/ip4/127.0.0.1/tcp/1".parse().unwrap();

        let pointer = timeout(
            TEST_TIMEOUT,
            net_c.ask_for_leader(&[unreachable_seed], Duration::from_secs(2)),
        )
        .await
        .expect("ask_for_leader completed within the test timeout");

        assert_eq!(pointer, LeaderSearch::NoAnswer);
    }

    #[tokio::test]
    async fn ask_for_leader_falls_through_a_non_responding_seed_to_the_next() {
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

        let response = pointer_to(&worker_a, &listen_addr);
        let _responder = spawn_join_responder(net_a, response.clone());

        let unreachable_seed: Multiaddr = "/ip4/127.0.0.1/tcp/1".parse().unwrap();
        let seeds = vec![unreachable_seed, listen_addr];

        let pointer = timeout(
            TEST_TIMEOUT,
            net_c.ask_for_leader(&seeds, Duration::from_secs(5)),
        )
        .await
        .expect("ask_for_leader completed within the test timeout");

        assert_eq!(pointer, LeaderSearch::Found(response));
    }

    /// Regression test for the bug this fix addresses: `ask_peer_for_leader`
    /// used to identify "the newly connected peer" for a seed by diffing
    /// the connected-peer set before/after the dial, with no correlation to
    /// the specific dial in progress. If a seed's dial connects *late* —
    /// after its per-seed timeout has elapsed and the cascade has moved on
    /// to the next seed — that late `ConnectionEstablished` could be
    /// misattributed as the *next* seed's peer, and `JOIN_REQUEST` would go
    /// to the wrong node.
    /// `ask_for_leader_falls_through_a_non_responding_seed_to_the_next`
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
    async fn ask_for_leader_ignores_a_late_connection_from_an_abandoned_seed() {
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

        // Seed A would point at itself, which is obviously wrong for this
        // test, so a misattribution is easy to detect.
        let _responder_a = spawn_join_responder(net_a, pointer_to(&worker_a, &addr_a));

        // Seed B points at itself too: a distinct leader.
        let response_b = pointer_to(&worker_b, &addr_b);
        let _responder_b = spawn_join_responder(net_b, response_b.clone());

        // Simulate `ask_peer_for_leader` abandoning seed A's dial once its
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
        drop(response_rx_a); // abandoned, as a timed-out ask_peer_for_leader would drop it

        // Run the real cascade against seed B only. If seed A's abandoned
        // dial connects while this is in flight, the fix must not let that
        // leak into seed B's result.
        let pointer = timeout(
            TEST_TIMEOUT,
            net_c.ask_for_leader(&[addr_b], Duration::from_secs(5)),
        )
        .await
        .expect("ask_for_leader completed within the test timeout");

        assert_eq!(
            pointer,
            LeaderSearch::Found(response_b),
            "seed B's join must resolve to seed B's own answer, never seed A's, \
             even though seed A's abandoned dial may still be completing concurrently"
        );

        // Confirm seed A's dial really did complete in the background
        // (proving this test actually exercised a live late connection, not
        // a dial that simply never connected) — its already-abandoned
        // response channel makes that harmless, which is exactly the
        // property under test.
        expect_input(
            &net_c,
            Input::PeerConnected(worker_a),
            "seed A's abandoned dial still connected in the background",
        )
        .await;
    }

    /// Chunk C6: proves `request_claim`/`poll_claim_requests`/`respond_claim`
    /// round-trip over real sockets at the `Net` layer alone, independent of
    /// `core::scheduler::Scheduler` or `crate::driver` — the same "prove the
    /// wire machinery in isolation" role `ask_for_leader_returns_the_seeds_leader_pointer`
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
                        assert_eq!(
                            handle.request(),
                            &claim_request::Request::TaskId(task_id.clone().into())
                        );
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

        net_a.set_leader(Some(worker_b));
        let response = timeout(TEST_TIMEOUT, net_a.request_claim(task_id))
            .await
            .expect("request_claim completed within the test timeout");

        responder.abort();
        assert_eq!(
            response,
            Ok(ClaimResponse {
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

        net_a.set_leader(Some(worker_b));
        let response = timeout(TEST_TIMEOUT, net_a.request_claim(task_id))
            .await
            .expect("request_claim completed within the test timeout");

        responder.abort();
        assert_eq!(
            response,
            Ok(ClaimResponse {
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

    fn addr(text: &str) -> Multiaddr {
        text.parse().expect("a well-formed multiaddr")
    }

    #[test]
    fn a_non_loopback_address_is_preferred_over_loopback_in_either_order() {
        let loopback = addr("/ip4/127.0.0.1/tcp/4001");
        let routable = addr("/ip4/192.0.2.7/tcp/4001");

        assert_eq!(
            preferred_address([loopback.clone(), routable.clone()]),
            Some(routable.clone())
        );
        assert_eq!(
            preferred_address([routable.clone(), loopback]),
            Some(routable)
        );
    }

    #[test]
    fn a_loopback_address_is_taken_only_when_it_is_the_only_choice() {
        let loopback = addr("/ip4/127.0.0.1/tcp/4001");
        let wildcard = addr("/ip4/0.0.0.0/tcp/4001");

        assert_eq!(
            preferred_address([wildcard, loopback.clone()]),
            Some(loopback)
        );
        assert_eq!(preferred_address([]), None);
    }

    #[test]
    fn a_wildcard_address_is_never_preferred() {
        assert_eq!(
            preferred_address([addr("/ip4/0.0.0.0/tcp/4001"), addr("/ip6/::/tcp/4001")]),
            None
        );
        assert_eq!(
            preferred_address([
                addr("/ip4/0.0.0.0/tcp/4001"),
                addr("/ip4/192.0.2.7/tcp/4001"),
            ]),
            Some(addr("/ip4/192.0.2.7/tcp/4001"))
        );
    }

    #[test]
    fn ipv6_loopback_counts_as_loopback() {
        let loopback = addr("/ip6/::1/tcp/4001");
        let routable = addr("/ip6/2001:db8::7/tcp/4001");

        assert_eq!(
            preferred_address([loopback.clone(), routable.clone()]),
            Some(routable)
        );
        assert_eq!(preferred_address([loopback.clone()]), Some(loopback));
    }

    /// A gossip message on `shard-1`'s topic from `source` carrying `data`.
    fn gossip_message(source: Option<PeerId>, data: Vec<u8>) -> gossipsub::Message {
        gossipsub::Message {
            source,
            data,
            sequence_number: Some(1),
            topic: shard_topic(&ShardId::new("shard-1")).hash(),
        }
    }

    #[test]
    fn a_shards_topic_is_named_after_the_shard() {
        assert_eq!(
            shard_topic(&ShardId::new("shard-1")).to_string(),
            "/kabudachi/shard-1/election/2"
        );
    }

    #[test]
    fn a_gossip_message_becomes_a_message_from_its_author() {
        let author = identity::Keypair::generate_ed25519().public().to_peer_id();
        let heartbeat = heartbeat_message(&worker_id_of(&author));

        assert_eq!(
            gossip_input(gossip_message(Some(author), heartbeat.encode_to_vec())),
            Some(Input::Message {
                from: worker_id_of(&author),
                message: heartbeat,
            })
        );
    }

    #[test]
    fn a_gossip_message_with_no_author_is_dropped() {
        let author = worker_that_never_runs();
        let heartbeat = heartbeat_message(&author);

        assert_eq!(
            gossip_input(gossip_message(None, heartbeat.encode_to_vec())),
            None
        );
    }

    #[test]
    fn a_gossip_message_that_is_no_election_message_is_dropped() {
        let author = identity::Keypair::generate_ed25519().public().to_peer_id();

        assert_eq!(
            gossip_input(gossip_message(Some(author), vec![0xff, 0xff, 0xff])),
            None
        );
    }

    #[test]
    fn a_gossip_message_that_is_not_well_formed_is_dropped() {
        let author = identity::Keypair::generate_ed25519().public().to_peer_id();
        let mut heartbeat = heartbeat_message(&worker_id_of(&author));
        let Some(election_message::Payload::Heartbeat(payload)) = &mut heartbeat.payload else {
            unreachable!("heartbeat_message builds a heartbeat");
        };
        payload.worker_id = None;

        assert_eq!(
            gossip_input(gossip_message(Some(author), heartbeat.encode_to_vec())),
            None
        );
    }

    /// A roll call `initiator` starts, stamped with `initiator_address`, with
    /// a well-formed configuration so it survives `decode_well_formed` on the
    /// wire (see `WellFormed`).
    fn roll_call(initiator: &WorkerId, initiator_address: &str) -> ElectionMessage {
        ElectionMessage {
            payload: Some(election_message::Payload::RollCall(RollCall {
                shard_id: Some(ShardId::new("shard-1").into()),
                term: 1,
                configuration: Some((&Configuration::genesis(0)).into()),
                initiator_id: Some(initiator.clone().into()),
                initiator_address: initiator_address.into(),
                ..Default::default()
            })),
        }
    }

    /// `responder`'s reply to `initiator`'s roll call, stamped with
    /// `responder_address`.
    fn roll_call_reply(
        initiator: &WorkerId,
        responder: &WorkerId,
        responder_address: &str,
    ) -> ElectionMessage {
        ElectionMessage {
            payload: Some(election_message::Payload::RollCallReply(RollCallReply {
                shard_id: Some(ShardId::new("shard-1").into()),
                term: 1,
                initiator_id: Some(initiator.clone().into()),
                responder_id: Some(responder.clone().into()),
                responder_address: responder_address.into(),
                admission: None,
                prior_admission: None,
            })),
        }
    }

    #[test]
    fn a_roll_call_and_a_reply_are_stamped_with_the_senders_own_address() {
        let (me, other) = (WorkerId::new("me"), WorkerId::new("other"));
        let own = addr("/ip4/192.0.2.7/tcp/4001");

        let mut call = roll_call(&me, "");
        stamp_own_address(&mut call, &own);
        assert_eq!(call, roll_call(&me, "/ip4/192.0.2.7/tcp/4001"));

        let mut reply = roll_call_reply(&other, &me, "");
        stamp_own_address(&mut reply, &own);
        assert_eq!(
            reply,
            roll_call_reply(&other, &me, "/ip4/192.0.2.7/tcp/4001")
        );

        let mut heartbeat = heartbeat_message(&me);
        stamp_own_address(&mut heartbeat, &own);
        assert_eq!(
            heartbeat,
            heartbeat_message(&me),
            "only roll calls and replies carry one"
        );
    }

    #[test]
    fn the_address_a_sender_stamped_for_itself_is_read_back() {
        let (initiator, responder) = (WorkerId::new("initiator"), WorkerId::new("responder"));

        assert_eq!(
            stamped_address(
                &initiator,
                &roll_call(&initiator, "/ip4/192.0.2.7/tcp/4001")
            ),
            Some(addr("/ip4/192.0.2.7/tcp/4001"))
        );
        assert_eq!(
            stamped_address(
                &responder,
                &roll_call_reply(&initiator, &responder, "/ip4/192.0.2.8/tcp/4001")
            ),
            Some(addr("/ip4/192.0.2.8/tcp/4001"))
        );
        assert_eq!(
            stamped_address(&initiator, &heartbeat_message(&initiator)),
            None
        );
    }

    #[test]
    fn a_stamp_naming_anyone_but_its_sender_is_ignored() {
        let (initiator, responder, relay) = (
            WorkerId::new("initiator"),
            WorkerId::new("responder"),
            WorkerId::new("relay"),
        );

        assert_eq!(
            stamped_address(&relay, &roll_call(&initiator, "/ip4/192.0.2.7/tcp/4001")),
            None,
            "a roll call's stamp is its initiator's, not whoever else sent it"
        );
        assert_eq!(
            stamped_address(
                &initiator,
                &roll_call_reply(&initiator, &responder, "/ip4/192.0.2.8/tcp/4001")
            ),
            None,
            "a reply's stamp is its responder's, not its initiator's"
        );
    }

    #[test]
    fn an_empty_unparsable_or_wildcard_stamp_is_no_address() {
        let initiator = WorkerId::new("initiator");

        for stamp in ["", "not a multiaddr", "/ip4/0.0.0.0/tcp/4001"] {
            assert_eq!(
                stamped_address(&initiator, &roll_call(&initiator, stamp)),
                None,
                "{stamp:?}"
            );
        }
    }

    #[test]
    fn a_stamped_address_is_recorded_below_identify_and_a_dialed_address() {
        let peer = PeerId::random();
        let sender = worker_id_of(&peer);
        let stamped = addr("/ip4/192.0.2.7/tcp/4001");
        let arrival = Input::Message {
            from: sender.clone(),
            message: roll_call(&sender, "/ip4/192.0.2.7/tcp/4001"),
        };
        let stored = |addresses: &Arc<Mutex<BTreeMap<PeerId, KnownAddress>>>| {
            addresses
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get(&peer)
                .map(|known| (known.addr.clone(), known.source))
        };

        let addresses = Arc::new(Mutex::new(BTreeMap::new()));
        record_stamped_address(&addresses, &arrival);
        assert_eq!(
            stored(&addresses),
            Some((stamped.clone(), AddressSource::SelfStamped))
        );

        for better in [AddressSource::DialedAddress, AddressSource::Identify] {
            let known = addr("/ip4/192.0.2.9/tcp/4001");
            let addresses = Arc::new(Mutex::new(BTreeMap::new()));
            record_peer_address(&addresses, peer, known.clone(), better);
            record_stamped_address(&addresses, &arrival);
            assert_eq!(stored(&addresses), Some((known, better)), "{better:?}");
        }

        let addresses = Arc::new(Mutex::new(BTreeMap::new()));
        record_peer_address(
            &addresses,
            peer,
            addr("/ip4/127.0.0.1/tcp/54321"),
            AddressSource::InboundRemote,
        );
        record_stamped_address(&addresses, &arrival);
        assert_eq!(
            stored(&addresses),
            Some((stamped, AddressSource::SelfStamped)),
            "a stamp outranks an inbound connection's source address"
        );
    }

    #[test]
    fn an_arrival_whose_stamp_names_another_peer_records_nothing() {
        let (sender, initiator) = (PeerId::random(), PeerId::random());
        let addresses = Arc::new(Mutex::new(BTreeMap::new()));

        record_stamped_address(
            &addresses,
            &Input::Message {
                from: worker_id_of(&sender),
                message: roll_call(&worker_id_of(&initiator), "/ip4/192.0.2.7/tcp/4001"),
            },
        );

        assert!(
            addresses
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .is_empty()
        );
    }

    #[tokio::test]
    async fn a_sent_reply_carries_its_senders_listen_address_and_none_without_one() {
        let net_a = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let net_b = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        // Only net_a listens.
        let (worker_a, worker_b) = connected_pair(&net_a, &net_b).await;
        let listen_addr_a = net_a.local_multiaddr().expect("net_a listens").to_string();

        net_a.send(worker_b.clone(), roll_call_reply(&worker_b, &worker_a, ""));
        expect_input(
            &net_b,
            Input::Message {
                from: worker_a.clone(),
                message: roll_call_reply(&worker_b, &worker_a, &listen_addr_a),
            },
            "net_b received net_a's reply stamped with net_a's listen address",
        )
        .await;

        net_b.send(worker_a.clone(), roll_call_reply(&worker_a, &worker_b, ""));
        expect_input(
            &net_a,
            Input::Message {
                from: worker_b.clone(),
                message: roll_call_reply(&worker_a, &worker_b, ""),
            },
            "net_a received net_b's reply with no address, as net_b listens on none",
        )
        .await;
    }

    #[tokio::test]
    async fn a_reply_arriving_over_a_connection_records_its_senders_stamp() {
        let net_a = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let net_b = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        // net_b dials net_a without listening itself, so net_a knows net_b
        // only by the source address of its connection, which is not
        // dialable.
        let (worker_a, worker_b) = connected_pair(&net_a, &net_b).await;
        assert_eq!(net_a.dialable_address(&worker_b), None);

        // net_b listens on nothing, so it sends the stamp it is given as is.
        let stamp = "/ip4/192.0.2.7/tcp/4001";
        let reply = roll_call_reply(&worker_a, &worker_b, stamp);
        net_b.send(worker_a, reply.clone());
        expect_input(
            &net_a,
            Input::Message {
                from: worker_b.clone(),
                message: reply,
            },
            "net_a received net_b's reply",
        )
        .await;

        assert_eq!(net_a.dialable_address(&worker_b), Some(addr(stamp)));
    }

    #[tokio::test]
    async fn a_published_roll_call_arrives_over_gossipsub_stamped_with_the_publishers_address() {
        // The generic gossipsub round trip (delivery, one copy per
        // subscriber, shard scoping, relaying) is proven once in
        // `gossip_publish_test.rs`, with a payload `stamp_own_address` does
        // not touch. This test instead sits with this file's other
        // address-stamping and real-socket round-trip tests, and proves the
        // one thing none of them do: that `Net::publish` stamps a roll call
        // with this node's own address exactly as `Net::send` does (see
        // `stamp_own_address`), and that the stamp survives a real gossipsub
        // encode/decode round trip, not just a direct connection.
        let net_a = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let net_b = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let (worker_a, worker_b) = connected_pair(&net_a, &net_b).await;
        let listen_addr_a = net_a.local_multiaddr().expect("net_a listens").to_string();

        let shard = ShardId::new("shard-1");
        net_a.subscribe_to_shard(&shard);
        net_b.subscribe_to_shard(&shard);
        timeout(
            TEST_TIMEOUT,
            wait_until(|| net_a.shard_subscribers().contains(&worker_b)),
        )
        .await
        .expect("net_a saw net_b's subscription within the timeout");

        net_a.publish(roll_call(&worker_a, ""));

        expect_input(
            &net_b,
            Input::Message {
                from: worker_a.clone(),
                message: roll_call(&worker_a, &listen_addr_a),
            },
            "net_b received net_a's roll call over gossipsub, stamped with net_a's listen address",
        )
        .await;
    }

    #[tokio::test]
    async fn send_dials_a_peer_it_holds_no_connection_to_at_its_stamped_address() {
        let net_a = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let net_c = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let listen_addr_c = timeout(
            TEST_TIMEOUT,
            net_c.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
        )
        .await
        .expect("net_c produced a listen address within the timeout");
        let (worker_a, worker_c) = (net_a.local_worker_id(), net_c.local_worker_id());
        record_stamped_address(
            &net_a.peer_addresses,
            &Input::Message {
                from: worker_c.clone(),
                message: roll_call(&worker_c, &listen_addr_c.to_string()),
            },
        );

        let heartbeat = heartbeat_message(&worker_a);
        net_a.send(worker_c, heartbeat.clone());

        expect_input(
            &net_c,
            Input::Message {
                from: worker_a,
                message: heartbeat,
            },
            "net_c received the heartbeat net_a sent without a prior connection",
        )
        .await;
    }
}
