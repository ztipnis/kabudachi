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
//! continuously, together with everything the task learns about its peers
//! (`crate::peers::Peers`: connections, addresses, redials, gossip mesh,
//! traffic), which no other code touches. `Net` itself holds a command
//! channel to that task, the queues of inputs and requests the task fills
//! for the driver, and this node's own address, which the task publishes on
//! a `watch` channel. A method either pushes a command and returns, awaits
//! the task's answer to a command (a read of its peers among them: see
//! [`Net::diagnostics`]), or takes a short lock on a queue; nothing holds a
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
//! machinery: the joining side sends a `JOIN_REQUEST` over a connection a
//! dial of its own opened (see `crate::join`, the JOIN client), and
//! [`Net::poll_join_requests`] / [`Net::respond_join`] serve the answering
//! side. A join response names the shard's leader, which only
//! `core::election::WorkerNode` knows, so the driver answers it (see
//! `crate::join::pointer_for`). [`Net::dialable_address`] and
//! [`Net::local_multiaddr`] expose what `Net` knows about addresses, the
//! other half of what a join response needs.
//!
//! ## The claim arbitration protocol
//!
//! `/kabudachi/claim/1` (see `crate::claim_codec`) is the same shape of
//! genuine correlated request/response as join, so it gets the same
//! machinery: [`Net::request_claim`] and [`Net::claim_oldest`] (the asking
//! side, sent to the leader the caller names: the transport keeps no leader
//! of its own) and [`Net::poll_claim_requests`] / [`Net::respond_claim`]
//! (the answering side). Whether to grant a claim is
//! `core::scheduler::Scheduler`'s decision, so the driver answers it too.
//!
//! ## Reconnect/backoff
//!
//! A connection that drops is redialed: [`RedialPolicy`] governs a bounded,
//! exponential-backoff redial that `drive` runs on its own, at the address
//! [`Diagnostics::peer_addresses`] keeps for a disconnected peer. libp2p has no
//! retry to defer to: in the pinned `libp2p-swarm` 0.48.0,
//! `libp2p_swarm::dial_opts::DialOpts` (and its `PeerCondition`) configure
//! only a single dial attempt, and nothing schedules a retry after a dial or
//! an established connection fails.
//!
//! **Which drops are redial-eligible.** Only a peer that this node still
//! needs a connection to, and would not otherwise reach again, is redialed:
//! one in its gossip mesh for its shard ([`Diagnostics::shard_mesh`]) when the
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
//! issued the disconnect excludes that peer (see `crate::peers`'s
//! `RedialTracker`, which lifts the exclusion once the peer reconnects by
//! any means); the other side, like any other dropped peer, redials.
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
//! A peer's address of record is the leader address this node hands a
//! joining node in a `JOIN_RESPONSE`, and the address [`Net::send`] dials a
//! peer at when it holds no connection to it, so this node has to know which
//! of its entries something can actually *dial*. Not every address a swarm
//! event carries is:
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
//! with [`Diagnostics::peer_addresses`]'s "best-effort and address-of-record only"
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

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
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
use tokio::sync::{Notify, mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use crate::codec::Ack;
use crate::framing::decode_well_formed;
use crate::peers::Peers;
use crate::swarm::{Behaviour, BehaviourEvent};

/// Where a `peer_addresses` entry came from, and so how far it can be trusted
/// to be an address anything can dial. Ordered worst to best: `Ord` *is* the
/// precedence rule — see the module doc's "Where a peer's address comes from".
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub(crate) enum AddressSource {
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
pub(crate) struct KnownAddress {
    pub(crate) addr: Multiaddr,
    source: AddressSource,
}

/// Records `addr` for `peer` unless a strictly better-ranked source is already
/// on file. An equal rank *does* overwrite, so a fresher observation of the
/// same kind (a re-dial to a new address, a later Identify) still wins.
fn record_peer_address(
    addresses: &mut BTreeMap<PeerId, KnownAddress>,
    peer: PeerId,
    addr: Multiaddr,
    source: AddressSource,
) {
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
    addresses: &BTreeMap<PeerId, KnownAddress>,
    peer: &PeerId,
) -> Option<Multiaddr> {
    addresses
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
    /// Runs a read or small change of the swarm task's [`Peers`] on that
    /// task, which alone owns them; see `Net::with_peers`.
    WithPeers(Box<dyn FnOnce(&mut Peers) + Send>),
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

/// Why [`Net::request_claim`] or [`Net::claim_oldest`] got no answer from
/// a leader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimFailure {
    /// The leader named is this worker. Its own claims are its own scheduler's to
    /// decide, not a peer's, so nothing was sent.
    ThisWorkerLeads,
    /// No answer came: the request failed outright (such as a leader that
    /// cannot be dialed), the leader disconnected before answering, or
    /// nothing could be sent (this `Net` has stopped, or the leader's id
    /// names no libp2p peer).
    Unanswered,
}

/// Running counts of what one [`Net`] has carried since it was created
/// (see [`Diagnostics::traffic`]): how much of the shard's traffic goes
/// through a worker, its leader say.
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

/// One read of what a [`Net`]'s transport knows, for tests and logs (see
/// [`Net::diagnostics`]). Nothing a node decides depends on it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Diagnostics {
    /// The peers the swarm holds a connection to.
    pub connected: BTreeSet<WorkerId>,
    /// Every address of record, keyed by peer, including a peer's inbound
    /// source address, which may not be dialable (see the module doc's
    /// "Where a peer's address comes from"; for an address to give anyone
    /// else, use [`Net::dialable_address`]). A peer that disconnects keeps
    /// its last entry: that is where a redial finds the address to dial.
    pub peer_addresses: BTreeMap<WorkerId, Multiaddr>,
    /// The address this node gives other nodes (see [`Net::local_multiaddr`]).
    pub local_addr: Option<Multiaddr>,
    /// Every peer being redialed (see [`RedialPolicy`]) and how many
    /// attempts have been made so far. A peer leaves once it reconnects or
    /// has used up `RedialPolicy::max_attempts`.
    pub redial_attempts: BTreeMap<WorkerId, u32>,
    /// The connected peers whose subscription to this node's shard topic
    /// has reached it. Empty before [`Net::subscribe_to_shard`].
    pub shard_subscribers: BTreeSet<WorkerId>,
    /// The peers in this node's gossip mesh for its shard: the subscribers
    /// its shard's gossip actually runs through, whose dropped connection
    /// the redial policy repairs (see the module doc's "Which drops are
    /// redial-eligible").
    pub shard_mesh: BTreeSet<WorkerId>,
    /// What this `Net` has carried since it was created.
    pub traffic: Traffic,
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
}

impl Default for Inbound {
    fn default() -> Self {
        Self {
            inputs: Mutex::default(),
            input_limit: AtomicUsize::new(DEFAULT_INPUT_LIMIT),
            join_requests: Mutex::default(),
            claim_requests: Mutex::default(),
            arrived: Notify::new(),
        }
    }
}

impl Inbound {
    /// Queues `input` for the node, keeping the queue bounded while no
    /// driver takes from it (see the module doc's "Inputs for the node").
    fn queue_input(&self, input: Input) {
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
        push(&self.join_requests, handle);
        self.arrived.notify_one();
    }

    fn queue_claim_request(&self, handle: ClaimRequestHandle) {
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
    /// The address this `Net` gives other nodes for itself (see
    /// `Self::local_multiaddr`), as the swarm task last published it.
    local_addr: watch::Receiver<Option<Multiaddr>>,
    /// The shard this `Net` subscribed to (see `Self::subscribe_to_shard`).
    shard: Mutex<Option<ShardId>>,
    /// See [`Self::with_routing_refresh_period`].
    routing_refresh_period: Option<StdDuration>,
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
        let (local_addr_tx, local_addr) = watch::channel(None);
        let peers = Peers::new(local_addr_tx, redial_policy);

        let driver = tokio::spawn(drive(swarm, command_rx, inbound.clone(), peers));

        Self {
            local_worker_id,
            commands,
            inbound,
            local_addr,
            shard: Mutex::new(None),
            routing_refresh_period: None,
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
    /// the refusal closes it. So a partition is blocked on both sides, or
    /// the unblocked side would see each of its dials connect for an
    /// instant.
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
    /// diffing the connected-peer set before/after — see its callers in
    /// `crate::join` for why that distinction matters:
    /// a late `ConnectionEstablished` for an abandoned dial carries *that*
    /// dial's `ConnectionId`, so it can only ever resolve (or fail to
    /// resolve, if the caller already stopped awaiting it) that dial's own
    /// response channel — it can never be mistaken for a different, later
    /// dial's result.
    pub(crate) async fn dial_for_connection(&self, opts: DialOpts) -> Option<PeerId> {
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
        self.local_addr.borrow().clone()
    }

    /// One read of what this `Net`'s transport knows (see [`Diagnostics`]),
    /// for tests and logs. Empty if the swarm task has stopped.
    pub async fn diagnostics(&self) -> Diagnostics {
        self.with_peers(|peers| peers.snapshot())
            .await
            .unwrap_or_default()
    }

    /// Runs `read` on the swarm task's [`Peers`], which that task alone
    /// owns, and returns what it returns: commands are handled in the order
    /// they were sent, so `read` sees the effect of every command sent
    /// before it. `None` if the swarm task has stopped.
    pub(crate) async fn with_peers<T: Send + 'static>(
        &self,
        read: impl FnOnce(&mut Peers) -> T + Send + 'static,
    ) -> Option<T> {
        let (respond_to, response) = oneshot::channel();
        self.commands
            .send(Command::WithPeers(Box::new(move |peers| {
                let _ = respond_to.send(read(peers));
            })))
            .ok()?;
        response.await.ok()
    }

    /// `peer`'s address of record, but only if something can dial it: one
    /// `peer` advertised through Identify or stamped on a roll call or reply
    /// it sent, or one this node dialed itself. `None` for a peer known only
    /// from its own inbound connection, whose source address is not one
    /// `peer` listens on (see the module doc's "Where a peer's address comes
    /// from").
    pub async fn dialable_address(&self, peer: &WorkerId) -> Option<Multiaddr> {
        let peer = PeerId::from_str(peer.as_str()).ok()?;
        self.with_peers(move |peers| dialable_address_of(&peers.addresses, &peer))
            .await
            .flatten()
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
    /// `crate::join`, the only caller) and awaits its
    /// `JOIN_RESPONSE`. `None` if the driver task is gone, the request fails
    /// outright (`OutboundFailure`), or the peer disconnects before
    /// answering.
    pub(crate) async fn send_join_request(&self, to: PeerId) -> Option<JoinResponse> {
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

    /// Asks `leader` for permission to run `task_id` (`REQUEST_CLAIM`,
    /// README §8.2), and awaits its answer: an accepted `Claim` or a
    /// `ClaimReject`. The caller names the leader, as its node knows it
    /// (`WorkerNode::known_leader`); a leader that has since lost office
    /// answers `NOT_LEADER`. See [`ClaimFailure`] for why there may be no
    /// answer.
    pub async fn request_claim(
        &self,
        leader: WorkerId,
        task_id: TaskId,
    ) -> Result<ClaimResponse, ClaimFailure> {
        self.ask_leader(leader, claim_request::Request::TaskId(task_id.into()))
            .await
    }

    /// Asks `leader` for up to `limit` of the oldest pending tasks
    /// (`CLAIM_OLDEST`), and awaits its answer: a batch of claims, oldest
    /// task first, or a `ClaimReject`. The batch may hold fewer than
    /// `limit`, or none: the leader hands out only as many as fit in one
    /// message. The caller names the leader, as for
    /// [`Self::request_claim`]. See [`ClaimFailure`] for why there may be no
    /// answer.
    pub async fn claim_oldest(
        &self,
        leader: WorkerId,
        limit: u32,
    ) -> Result<ClaimResponse, ClaimFailure> {
        self.ask_leader(leader, claim_request::Request::Oldest(ClaimOldest { limit }))
            .await
    }

    async fn ask_leader(
        &self,
        leader: WorkerId,
        request: claim_request::Request,
    ) -> Result<ClaimResponse, ClaimFailure> {
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
        let _ = self.commands.send(Command::Send { to: peer, message });
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
        // Same reasoning as in `send` for a stopped driver task.
        let _ = self.commands.send(Command::Publish {
            topic: shard_topic(&shard).hash(),
            message,
        });
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
pub(crate) fn worker_id_of(peer: &PeerId) -> WorkerId {
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
fn record_stamped_address(addresses: &mut BTreeMap<PeerId, KnownAddress>, input: &Input) {
    let Input::Message { from, message } = input else {
        return;
    };
    let Some(address) = stamped_address(from, message) else {
        return;
    };
    let Ok(peer) = PeerId::from_str(from.as_str()) else {
        return;
    };
    record_peer_address(addresses, peer, address, AddressSource::SelfStamped);
}

/// The requests `drive` has made of the swarm on a caller's behalf and not
/// yet answered, each keyed by what the swarm reports its outcome under.
#[derive(Default)]
struct Pending {
    listens: HashMap<ListenerId, oneshot::Sender<Multiaddr>>,
    join_requests: HashMap<OutboundRequestId, oneshot::Sender<Option<JoinResponse>>>,
    claim_requests: HashMap<OutboundRequestId, oneshot::Sender<Option<ClaimResponse>>>,
    dials: HashMap<ConnectionId, oneshot::Sender<Option<PeerId>>>,
}

/// Owns `swarm` and `peers` exclusively and polls the swarm for ever,
/// applying commands, queueing what arrives on `inbound`, and keeping
/// `peers` current after every event and command.
async fn drive(
    mut swarm: Swarm<Behaviour>,
    mut commands: mpsc::UnboundedReceiver<Command>,
    inbound: Arc<Inbound>,
    mut peers: Peers,
) {
    let mut pending = Pending::default();
    let mut redial_ticker = tokio::time::interval(peers.redial.check_interval());
    redial_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else {
                    return; // Every Net handle for this swarm was dropped.
                };
                handle_command(&mut swarm, command, &mut pending, &mut peers);
            }
            event = swarm.select_next_some() => {
                handle_event(&mut swarm, event, &mut pending, &inbound, &mut peers);
            }
            _ = redial_ticker.tick() => {
                // Whether a redial worked shows in a later refresh, which
                // stops redialing a peer once it is connected again.
                for (peer, addr) in peers.redial.due(tokio::time::Instant::now()) {
                    // Also asks kad's routing table for an address of its
                    // own, as `crate::join` does for a leader: the last-known
                    // address is not the only one that might still reach
                    // `peer`.
                    let _ = swarm.dial(
                        DialOpts::peer_id(peer)
                            .addresses(vec![addr])
                            .extend_addresses_through_behaviour()
                            .build(),
                    );
                }
            }
        }
        peers.refresh(&swarm);
    }
}

fn handle_command(
    swarm: &mut Swarm<Behaviour>,
    command: Command,
    pending: &mut Pending,
    peers: &mut Peers,
) {
    match command {
        Command::Send { to, mut message } => {
            if let Some(own) = &*peers.local_addr.borrow() {
                stamp_own_address(&mut message, own);
            }
            peers.traffic.messages_sent += 1;
            // Over a connection to `to` if there is one; else libp2p dials
            // `to` at this address of record (one stamped on a roll call
            // from a peer this node never connected to, say), along with any
            // its behaviours know.
            let address = dialable_address_of(&peers.addresses, &to);
            swarm
                .behaviour_mut()
                .request_response
                .send_request_with_addresses(&to, message, address.into_iter().collect());
        }
        Command::Subscribe { topic } => {
            if let Err(error) = swarm.behaviour_mut().gossipsub.subscribe(&topic) {
                tracing::warn!(
                    %topic,
                    %error,
                    "could not subscribe to the shard's gossip topic"
                );
            }
        }
        Command::Publish { topic, mut message } => {
            if let Some(own) = &*peers.local_addr.borrow() {
                stamp_own_address(&mut message, own);
            }
            peers.traffic.messages_published += 1;
            // Lost like any other unreliable message (see the module doc's
            // "Gossip").
            if let Err(error) = swarm
                .behaviour_mut()
                .gossipsub
                .publish(topic.clone(), message.encode_to_vec())
            {
                tracing::debug!(%topic, %error, "dropping a gossip publish");
            }
        }
        Command::Dial { addr } => {
            let _ = swarm.dial(addr);
        }
        Command::Disconnect { peer } => {
            let _ = swarm.disconnect_peer_id(peer);
            // See "Which drops are redial-eligible" in the module doc.
            peers.redial.disconnected_locally(peer);
        }
        Command::RefreshPeerRouting => {
            // `NoKnownPeers`: nothing to crawl from yet; the next refresh
            // tries again.
            let _ = swarm.behaviour_mut().kad.bootstrap();
        }
        Command::Block { peer, blocked } => {
            let blocklist = &mut swarm.behaviour_mut().blocked;
            if blocked {
                blocklist.block_peer(peer);
            } else {
                blocklist.unblock_peer(peer);
            }
        }
        Command::DialForConnection { opts, respond_to } => {
            let connection_id = opts.connection_id();
            match swarm.dial(opts) {
                Ok(()) => {
                    pending.dials.insert(connection_id, respond_to);
                }
                // Swarm::dial can fail synchronously (e.g. no addresses
                // survive filtering) without ever producing a SwarmEvent for
                // this connection_id — answer inline rather than leaving the
                // caller to wait out the full timeout.
                Err(_) => {
                    let _ = respond_to.send(None);
                }
            }
        }
        Command::ListenOn { addr, respond_to } => {
            if let Ok(listener_id) = swarm.listen_on(addr) {
                pending.listens.insert(listener_id, respond_to);
            }
            // On error, dropping `respond_to` closes the channel;
            // `Net::listen_on`'s awaiter observes that as a panic with a
            // message pointing at the cause.
        }
        Command::SendJoinRequest { to, respond_to } => {
            let request_id = swarm.behaviour_mut().join.send_request(&to, JoinRequest {});
            pending.join_requests.insert(request_id, respond_to);
        }
        Command::RespondJoin { channel, response } => {
            // Best-effort, like the election Ack: a channel that already
            // closed just means the requester stopped waiting.
            let _ = swarm.behaviour_mut().join.send_response(channel, response);
        }
        Command::SendClaimRequest {
            to,
            request,
            respond_to,
        } => {
            let request_id = swarm.behaviour_mut().claim.send_request(&to, request);
            pending.claim_requests.insert(request_id, respond_to);
        }
        Command::RespondClaim { channel, response } => {
            // Best-effort, same reasoning as RespondJoin above.
            let _ = swarm.behaviour_mut().claim.send_response(channel, response);
        }
        Command::WithPeers(read) => read(peers),
    }
}

fn handle_event(
    swarm: &mut Swarm<Behaviour>,
    event: SwarmEvent<BehaviourEvent>,
    pending: &mut Pending,
    inbound: &Inbound,
    peers: &mut Peers,
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
            peers.local_addr.send_modify(|recorded| {
                let previous = recorded.take();
                *recorded = preferred_address(std::iter::once(address.clone()).chain(previous));
            });
            if let Some(respond_to) = pending.listens.remove(&listener_id) {
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
                &mut peers.addresses,
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
            if let Some(respond_to) = pending.dials.remove(&connection_id) {
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
                record_peer_address(&mut peers.addresses, peer_id, addr, AddressSource::Identify);
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
            if let Some(respond_to) = pending.dials.remove(&connection_id) {
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
            record_stamped_address(&mut peers.addresses, &input);
            peers.traffic.messages_received += 1;
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
                record_stamped_address(&mut peers.addresses, &input);
                peers.traffic.messages_received += 1;
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
            peers.traffic.join_requests_received += 1;
            inbound.queue_join_request(JoinRequestHandle {
                from: worker_id_of(&peer),
                channel,
            });
        }
        SwarmEvent::Behaviour(BehaviourEvent::Join(request_response::Event::Message {
            message: request_response::Message::Response { request_id, response },
            ..
        })) => {
            if let Some(respond_to) = pending.join_requests.remove(&request_id) {
                let _ = respond_to.send(Some(response));
            }
        }
        SwarmEvent::Behaviour(BehaviourEvent::Join(request_response::Event::OutboundFailure {
            request_id,
            ..
        })) => {
            if let Some(respond_to) = pending.join_requests.remove(&request_id) {
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
            peers.traffic.claim_requests_received += 1;
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
            if let Some(respond_to) = pending.claim_requests.remove(&request_id) {
                let _ = respond_to.send(Some(response));
            }
        }
        SwarmEvent::Behaviour(BehaviourEvent::Claim(request_response::Event::OutboundFailure {
            request_id,
            ..
        })) => {
            if let Some(respond_to) = pending.claim_requests.remove(&request_id) {
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
    use kabudachi_core::protocol::ids::{IncarnationId, ShardId};
    use kabudachi_core::protocol::messages::{
        RollCall, RollCallReply, WorkerHeartbeat, election_message,
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

    /// Reads `net`'s diagnostics until `condition` holds of them. Every
    /// call site wraps this in `tokio::time::timeout` — this alone would
    /// spin forever on a bug.
    async fn wait_for_diagnostics(net: &Net, condition: impl Fn(&Diagnostics) -> bool) {
        while !condition(&net.diagnostics().await) {
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
        timeout(TEST_TIMEOUT, async {
            wait_for_diagnostics(net_a, |d| d.shard_mesh.contains(&worker_b)).await;
            wait_for_diagnostics(net_b, |d| d.shard_mesh.contains(&worker_a)).await;
        })
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
                !net_a
                    .diagnostics()
                    .await
                    .redial_attempts
                    .contains_key(&worker_b),
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
            wait_for_diagnostics(&net_a, |d| d.redial_attempts.contains_key(&worker_b)),
        )
        .await
        .expect("net_a started redialing the permanently-gone peer within the timeout");

        timeout(
            TEST_TIMEOUT,
            wait_for_diagnostics(&net_a, |d| !d.redial_attempts.contains_key(&worker_b)),
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
    /// from auto-redial: `net_a`'s diagnostics must never show a redial
    /// attempt for it, and `net_a` must never reconnect to it on its own.
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
                !net_a
                    .diagnostics()
                    .await
                    .redial_attempts
                    .contains_key(&worker_b),
                "net_a must never auto-redial a peer it locally disconnected"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
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
            wait_for_diagnostics(&net_a, |d| d.peer_addresses.contains_key(&worker_b)),
        )
        .await
        .expect("net_a recorded net_b's source address within the timeout");

        assert_eq!(net_a.dialable_address(&worker_b).await, None);
        assert_eq!(
            net_b.dialable_address(&worker_a).await,
            net_a.local_multiaddr()
        );

        let listen_addr_b = timeout(
            TEST_TIMEOUT,
            net_b.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
        )
        .await
        .expect("net_b produced a listen address within the timeout");
        timeout(TEST_TIMEOUT, async {
            while net_a.dialable_address(&worker_b).await != Some(listen_addr_b.clone()) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("net_a learned net_b's advertised listen address within the timeout");
    }

    /// The ranking rule behind final-review finding I1, on its own: a better
    /// -ranked source always wins regardless of arrival order, and an equally
    /// -ranked one is treated as a fresher observation and overwrites. See the
    /// module doc's "Where a peer's address comes from"; the end-to-end
    /// consequence is asserted in `net/tests/election/three_node_join.rs`.
    #[test]
    fn a_peer_address_is_only_replaced_by_an_equal_or_better_ranked_source() {
        let peer = PeerId::random();
        let send_back: Multiaddr = "/ip4/127.0.0.1/tcp/54321".parse().unwrap();
        let dialed: Multiaddr = "/ip4/127.0.0.1/tcp/4001".parse().unwrap();
        let advertised: Multiaddr = "/ip4/127.0.0.1/tcp/4002".parse().unwrap();
        let fresher_advertised: Multiaddr = "/ip4/127.0.0.1/tcp/4003".parse().unwrap();

        let mut addresses = BTreeMap::new();
        let stored = |addresses: &BTreeMap<PeerId, KnownAddress>| {
            addresses
                .get(&peer)
                .expect("an address was recorded for this peer")
                .addr
                .clone()
        };

        record_peer_address(
            &mut addresses,
            peer,
            send_back.clone(),
            AddressSource::InboundRemote,
        );
        assert_eq!(stored(&addresses), send_back);

        record_peer_address(&mut addresses, peer, dialed.clone(), AddressSource::DialedAddress);
        assert_eq!(stored(&addresses), dialed, "a dialed address outranks a send_back_addr");

        record_peer_address(&mut addresses, peer, advertised.clone(), AddressSource::Identify);
        assert_eq!(stored(&addresses), advertised, "Identify outranks both");

        record_peer_address(
            &mut addresses,
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
            &mut addresses,
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
    fn a_gossip_message_with_no_author_is_dropped() {
        let author = worker_that_never_runs();
        let heartbeat = heartbeat_message(&author);

        assert_eq!(
            gossip_input(gossip_message(None, heartbeat.encode_to_vec())),
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
        let stored = |addresses: &BTreeMap<PeerId, KnownAddress>| {
            addresses
                .get(&peer)
                .map(|known| (known.addr.clone(), known.source))
        };

        let mut addresses = BTreeMap::new();
        record_stamped_address(&mut addresses, &arrival);
        assert_eq!(
            stored(&addresses),
            Some((stamped.clone(), AddressSource::SelfStamped))
        );

        for better in [AddressSource::DialedAddress, AddressSource::Identify] {
            let known = addr("/ip4/192.0.2.9/tcp/4001");
            let mut addresses = BTreeMap::new();
            record_peer_address(&mut addresses, peer, known.clone(), better);
            record_stamped_address(&mut addresses, &arrival);
            assert_eq!(stored(&addresses), Some((known, better)), "{better:?}");
        }

        let mut addresses = BTreeMap::new();
        record_peer_address(
            &mut addresses,
            peer,
            addr("/ip4/127.0.0.1/tcp/54321"),
            AddressSource::InboundRemote,
        );
        record_stamped_address(&mut addresses, &arrival);
        assert_eq!(
            stored(&addresses),
            Some((stamped, AddressSource::SelfStamped)),
            "a stamp outranks an inbound connection's source address"
        );
    }

    #[test]
    fn an_arrival_whose_stamp_names_another_peer_records_nothing() {
        let (relay, initiator, responder) = (
            worker_id_of(&PeerId::random()),
            worker_id_of(&PeerId::random()),
            worker_id_of(&PeerId::random()),
        );
        let mut addresses = BTreeMap::new();

        // A roll call's stamp is its initiator's, not whoever else sent it.
        record_stamped_address(
            &mut addresses,
            &Input::Message {
                from: relay,
                message: roll_call(&initiator, "/ip4/192.0.2.7/tcp/4001"),
            },
        );
        // A reply's stamp is its responder's, not its initiator's.
        record_stamped_address(
            &mut addresses,
            &Input::Message {
                from: initiator.clone(),
                message: roll_call_reply(&initiator, &responder, "/ip4/192.0.2.8/tcp/4001"),
            },
        );

        assert!(addresses.is_empty());
    }

    #[tokio::test]
    async fn a_sent_reply_carries_its_senders_listen_address_and_none_without_one() {
        let net_a = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        let net_b = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
        // Only net_a listens.
        let (worker_a, worker_b) = connected_pair(&net_a, &net_b).await;
        let listen_addr_a = net_a.local_multiaddr().expect("net_a listens").to_string();

        // Only a roll call or a reply carries its sender's address.
        let heartbeat = heartbeat_message(&worker_a);
        net_a.send(worker_b.clone(), heartbeat.clone());
        expect_input(
            &net_b,
            Input::Message {
                from: worker_a.clone(),
                message: heartbeat,
            },
            "net_b received net_a's heartbeat unstamped",
        )
        .await;

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
        assert_eq!(net_a.dialable_address(&worker_b).await, None);

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

        assert_eq!(net_a.dialable_address(&worker_b).await, Some(addr(stamp)));
    }

    #[tokio::test]
    async fn a_published_roll_call_arrives_over_gossipsub_stamped_with_the_publishers_address() {
        // The generic gossipsub round trip (delivery, one copy per
        // subscriber, shard scoping, relaying) is proven once in
        // `net/tests/claim/gossip_publish.rs`, with a payload `stamp_own_address` does
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
            wait_for_diagnostics(&net_a, |d| d.shard_subscribers.contains(&worker_b)),
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
        let arrival = Input::Message {
            from: worker_c.clone(),
            message: roll_call(&worker_c, &listen_addr_c.to_string()),
        };
        net_a
            .with_peers(move |peers| record_stamped_address(&mut peers.addresses, &arrival))
            .await;

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
