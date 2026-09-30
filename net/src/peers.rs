//! The peer book: what `crate::messenger`'s swarm task knows about the peers
//! around it. Which are connected, the address of record for each, this
//! node's own address, which peers it is redialing, which share its shard's
//! gossip topic and mesh, and how much traffic it has carried.
//!
//! The swarm task owns [`Peers`] outright and is the only code that changes
//! it, by telling it what it saw happen ([`Observation`], through
//! [`Peers::observe`]). A `Net` reads it by asking the task (see
//! `Net::diagnostics`), except for this node's own address, which the task
//! publishes on a `watch` channel because a node's driver needs it without
//! waiting (to register itself, say). Nothing here reads the swarm or the
//! clock: every observation comes with the `now` of the swarm task's loop
//! iteration that saw it, so the redial schedule is a plain function of the
//! observations it was given.
//!
//! ## Reconnect/backoff
//!
//! A connection that drops is redialed by the swarm task on its own, at the
//! address [`Diagnostics::peer_addresses`] keeps for a disconnected peer.
//! [`RedialPolicy`] governs the schedule: a few fast attempts with
//! exponential backoff, then a slow retry that continues for as long as the
//! peer stays away.
//! libp2p has no retry to defer to: in the pinned `libp2p-swarm` 0.48.0,
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
//! heartbeat, a claim or any other `Net::send` dials its peer as it goes,
//! and a connection that only a JOIN, a `kad` crawl or a peer's own dial
//! opened has no reason to be reopened just because an address is on file.
//! A peer in the mesh is never closed for being idle (gossipsub keeps it
//! alive; see `crate::swarm`'s `IDLE_CONNECTION_TIMEOUT`), so an idle
//! connection that times out is never redialed. Only the driver's routing
//! crawls (see `Net::refresh_peer_routing`) open such a connection again, at
//! most once per peer per crawl.
//!
//! A peer whose connection this `Net` itself asked to close
//! (`Net::disconnect`, seen here as [`Observation::DisconnectedLocally`]) is
//! not redialed either: a local hangup is a decision, not a failure to
//! recover from. Only the closing side can tell the difference: its own
//! `SwarmEvent::ConnectionClosed` reports `cause: None`, while the side being
//! disconnected sees `cause: Some(IO(..Closed..))`, like any transport
//! failure. So only the side that issued the disconnect excludes that peer;
//! the exclusion lifts once the peer reconnects by any means, and the other
//! side, like any other dropped peer, redials.
//!
//! This exemption covers only the redial policy, not the peer itself: any
//! later `Net::send` to it, a follower's heartbeat for example, dials it
//! again like any other unreachable peer. `disconnect` is not a way to
//! isolate a peer; `Net::block_peer` is. A blocked peer stays redial-eligible,
//! like one across a real partition: each attempt fails until the block
//! lifts, and keeps being retried.
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
//!
//! The fast phase is bounded, roughly two minutes with the default policy,
//! and then the slow phase takes over, one dial a minute with no end. A peer
//! that is dead for good therefore costs one failed dial per minute for the
//! life of the process; that is the price of reconnecting a mesh peer that
//! returns after a long partition without waiting for this node's own sends.
//! `Net::new_with_redial_policy` lets a caller, such as this crate's own
//! tests, use other parameters.
//!
//! ## Where a peer's address comes from
//!
//! A peer's address of record is the leader address this node hands a
//! joining node in a `JOIN_RESPONSE`, and the address `Net::send` dials a
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
//! `DialedAddress` (a [`Side::Dialer`] connection's address, dialable by
//! construction, since this node just dialed it) > `SelfStamped` (below) >
//! `InboundRemote` (a [`Side::Listener`] connection's source address, kept
//! only as a fallback for a peer whose Identify exchange has not completed
//! yet: never handed to a joiner nor offered to `Net::send`'s dial — see
//! [`Peers::dialable_address`] — though a redial, which has nothing better,
//! tries it). A new observation replaces the stored one whenever its source
//! ranks at least as high, so a fresher Identify or a fresher successful dial
//! still wins over a stale one of the same kind.
//!
//! A worker can also learn a peer's address with no connection to it at
//! all: gossip delivers a roll call from an initiator it may never have
//! connected to, and it answers with a direct reply. So a `Net` stamps its
//! own address (chosen as described below) on every roll call it publishes
//! and every roll call reply it sends (that half stays in `crate::messenger`),
//! and records the address stamped on one that arrives
//! ([`Observation::MessageArrived`]) as its sender's `SelfStamped` address —
//! but only when the stamp names the sender `Net` vouches for (a gossip
//! message's signed author, or the peer at the other end of a direct
//! message's connection), so no peer can redirect traffic meant for another.
//! A stamp is the peer's own choice among its listen addresses, not one this
//! node has seen work, so it ranks below a dialed address and, being one
//! address where Identify advertises them all, below Identify; it ranks above
//! an inbound source address, which is usually not dialable at all.
//!
//! Of a peer's `listen_addrs`, entries with an unspecified IP
//! (`0.0.0.0`/`::`) are skipped as undialable, and the first remaining one
//! that is not loopback is taken; a loopback one only when nothing else is
//! advertised. A node on another host that dialed a loopback leader address
//! would reach itself, and a joiner that a seed has answered keeps asking
//! for a leader it can reach. This node's own address, which it hands
//! joiners when it leads (`Net::local_multiaddr`), follows the same rule: a
//! node listening on a wildcard bind is told one listen address per
//! interface, in no set order, and a later one replaces the recorded one
//! unless it would swap a non-loopback address for loopback.
//!
//! Best-effort, deliberately: on a multi-homed host the chosen address may
//! be one the particular asking peer cannot route to — a general
//! address-selection problem this phase does not try to solve, consistent
//! with [`Diagnostics::peer_addresses`]'s "best-effort and address-of-record
//! only" contract.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::str::FromStr;
use std::time::Duration;

use kabudachi_core::election::Input;
use kabudachi_core::protocol::checked::{CheckedMessage, CheckedPayload};
use kabudachi_core::protocol::ids::WorkerId;
use kabudachi_core::protocol::messages::prelude::*;

use libp2p::multiaddr::Protocol;
use libp2p::{Multiaddr, PeerId};
use tokio::sync::watch;
use tokio::time::Instant;

/// `peer`'s `WorkerId` (see `crate::messenger`'s `WorkerId <-> PeerId`
/// mapping).
pub(crate) fn worker_id_of(peer: &PeerId) -> WorkerId {
    WorkerId::new(peer.to_string())
}

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

/// Fast-then-slow redial policy for a peer in this node's gossip mesh that
/// dropped without this `Net` itself asking to disconnect it (see the module
/// doc's "Reconnect/backoff" section for why libp2p's own
/// `DialOpts`/`PeerCondition` don't already do this, and which drops are
/// eligible at all).
///
/// On each eligible drop, the swarm task schedules a first redial attempt
/// after `initial_backoff`; each subsequent fast attempt (`fast_attempts` in
/// all) doubles the wait, capped at `max_backoff`. After the fast attempts
/// the peer is retried once every `slow_interval`, for as long as it stays
/// away: a peer gone for good costs one failed dial per interval, and a mesh
/// peer that comes back after a long partition is reconnected without
/// waiting for the node's own sends. `check_interval` is how often the swarm
/// task polls for a due attempt; coarser than `initial_backoff` wastes time
/// before the first attempt actually fires, so a caller using a short
/// `initial_backoff` (this crate's own tests) should also shrink this.
///
/// `Default` picks production-shaped values (see the module doc's "Why the
/// default is short"): a first attempt one second after the drop, well within
/// any suspicion timeout of seconds, doubling to at most 30 s, for eight fast
/// attempts (about two minutes), then one attempt a minute. A caller that
/// wants test-scale retries should use `Net::new_with_redial_policy` instead
/// of `Net::new`.
#[derive(Debug, Clone, Copy)]
pub struct RedialPolicy {
    /// Delay before the first redial attempt after an eligible drop.
    pub initial_backoff: Duration,
    /// Ceiling the doubling backoff never exceeds.
    pub max_backoff: Duration,
    /// Attempts on the doubling schedule before the slow retry takes over.
    pub fast_attempts: u32,
    /// Wait between attempts once the fast ones are used up: the bound on how
    /// long a peer that stays away goes unretried.
    pub slow_interval: Duration,
    /// How often the swarm task checks for a due attempt.
    pub check_interval: Duration,
}

impl Default for RedialPolicy {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(30),
            fast_attempts: 8,
            slow_interval: Duration::from_secs(60),
            check_interval: Duration::from_millis(250),
        }
    }
}

/// Running counts of what one `Net` has carried since it was created
/// (see [`Diagnostics::traffic`]): how much of the shard's traffic goes
/// through a worker, its leader say.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Traffic {
    /// Election messages received, sent to this worker or published to its
    /// shard, including any the input queue's bound later dropped.
    pub messages_received: u64,
    /// Election messages this worker handed to its swarm to send
    /// (`Net::send`), whether or not they arrived.
    pub messages_sent: u64,
    /// Election messages this worker published to its shard
    /// (`Net::publish`), each counted once however many workers it reaches.
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

/// One read of what a `Net`'s transport knows, for tests and logs (see
/// `Net::diagnostics`). Nothing a node decides depends on it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Diagnostics {
    /// The peers the swarm holds a connection to.
    pub connected: BTreeSet<WorkerId>,
    /// Every address of record, keyed by peer, including a peer's inbound
    /// source address, which may not be dialable (see the module doc's
    /// "Where a peer's address comes from"; for an address to give anyone
    /// else, use `Net::dialable_address`). A peer that disconnects keeps
    /// its last entry: that is where a redial finds the address to dial.
    pub peer_addresses: BTreeMap<WorkerId, Multiaddr>,
    /// The address this node gives other nodes (see `Net::local_multiaddr`).
    pub local_addr: Option<Multiaddr>,
    /// Every peer being redialed (see [`RedialPolicy`]) and how many
    /// attempts have been made so far. A peer leaves once it reconnects or
    /// this node hangs up on it.
    pub redial_attempts: BTreeMap<WorkerId, u32>,
    /// The connected peers whose subscription to this node's shard topic
    /// has reached it. Empty before `Net::subscribe_to_shard`.
    pub shard_subscribers: BTreeSet<WorkerId>,
    /// The peers in this node's gossip mesh for its shard: the subscribers
    /// its shard's gossip actually runs through, whose dropped connection
    /// the redial policy repairs (see the module doc's "Which drops are
    /// redial-eligible").
    pub shard_mesh: BTreeSet<WorkerId>,
    /// What this `Net` has carried since it was created.
    pub traffic: Traffic,
}

/// Which end of a connection this node is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Side {
    Dialer,
    Listener,
}

/// One traffic count the peer book keeps (see [`Traffic`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Carried {
    Sent,
    Published,
    JoinRequest,
    ClaimRequest,
}

/// What the swarm task saw happen, for the peer book. Every observation comes
/// with the `now` of the swarm task's loop iteration that saw it.
pub(crate) enum Observation<'a> {
    /// The swarm reported a listen address (a wildcard bind reports one per
    /// interface).
    ListeningOn(Multiaddr),
    /// A connection to `peer` opened; `address` is
    /// `ConnectedPoint::get_remote_address`.
    ConnectionOpened {
        peer: PeerId,
        side: Side,
        address: Multiaddr,
    },
    /// `peer`'s last connection closed.
    ConnectionClosed(PeerId),
    /// Identify reported `peer`'s own listen addresses.
    Identified {
        peer: PeerId,
        listen_addresses: Vec<Multiaddr>,
    },
    /// An election message arrived, direct or gossiped, from the sender `Net`
    /// vouches for. Records its stamped address (if it is the sender's own)
    /// and counts it received.
    MessageArrived(&'a Input),
    /// A dial of whoever answers at `address` (a JOIN ask) ended: `Some(peer)`
    /// connected, `None` failed.
    Dialed {
        address: Multiaddr,
        outcome: Option<PeerId>,
    },
    /// This node asked to close every connection to `peer`.
    DisconnectedLocally(PeerId),
    /// The gossip mesh and shard subscribers as gossipsub reports them now.
    Gossip {
        mesh: BTreeSet<PeerId>,
        subscribers: BTreeSet<PeerId>,
    },
    /// One more of `Carried`.
    Carried(Carried),
}

/// The swarm task's own record of its peers (see the module doc).
pub(crate) struct Peers {
    /// The peers the swarm holds a connection to, from the connections
    /// observed opening and closing.
    connected: BTreeSet<PeerId>,
    /// The best address known for each peer, ranked by where it came from
    /// (see [`AddressSource`]). A peer that disconnects keeps its entry, which
    /// is where a redial finds the address to dial.
    addresses: BTreeMap<PeerId, KnownAddress>,
    /// The address this node gives other nodes for itself.
    local_addr: watch::Sender<Option<Multiaddr>>,
    redial: RedialTracker,
    /// The connected peers whose subscription to this node's shard topic has
    /// reached it.
    subscribers: BTreeSet<PeerId>,
    /// The peers in this node's gossip mesh for its shard.
    mesh: BTreeSet<PeerId>,
    /// The peer each address a JOIN asked turned out to be (see
    /// `crate::join`), so a peer asked again is asked over the connection it
    /// already has.
    asked: HashMap<Multiaddr, PeerId>,
    traffic: Traffic,
}

impl Peers {
    pub(crate) fn new(own_address: watch::Sender<Option<Multiaddr>>, policy: RedialPolicy) -> Self {
        Peers {
            connected: BTreeSet::new(),
            addresses: BTreeMap::new(),
            local_addr: own_address,
            redial: RedialTracker::new(policy),
            subscribers: BTreeSet::new(),
            mesh: BTreeSet::new(),
            asked: HashMap::new(),
            traffic: Traffic::default(),
        }
    }

    /// Takes one observation. The only way the book changes.
    pub(crate) fn observe(&mut self, observation: Observation<'_>, now: Instant) {
        match observation {
            Observation::ListeningOn(address) => {
                // A later address replaces the recorded one unless that would
                // swap a non-loopback address for loopback.
                self.local_addr.send_modify(|recorded| {
                    let previous = recorded.take();
                    *recorded = preferred_address(std::iter::once(address).chain(previous));
                });
            }
            Observation::ConnectionOpened {
                peer,
                side,
                address,
            } => {
                if self.connected.insert(peer) {
                    self.redial.reconnected(&peer);
                }
                // The two sides of a connection observe different addresses:
                // only the dialing side's is dialable. Both are recorded,
                // ranked, so an Identify exchange on this same connection can
                // supersede either, but neither can displace a better one.
                let source = match side {
                    Side::Dialer => AddressSource::DialedAddress,
                    Side::Listener => AddressSource::InboundRemote,
                };
                self.record_address(peer, address, source);
            }
            Observation::ConnectionClosed(peer) => {
                // Judged against the mesh as last observed: by the time gossipsub
                // reports the drop, it has already taken the peer out of it.
                if self.connected.remove(&peer)
                    && self.mesh.contains(&peer)
                    && let Some(known) = self.addresses.get(&peer)
                {
                    self.redial.dropped(peer, known.addr.clone(), now);
                }
            }
            Observation::Identified {
                peer,
                listen_addresses,
            } => {
                if let Some(address) = preferred_address(listen_addresses) {
                    self.record_address(peer, address, AddressSource::Identify);
                }
            }
            Observation::MessageArrived(input) => {
                self.record_stamped_address(input);
                self.traffic.messages_received += 1;
            }
            Observation::Dialed { address, outcome } => match outcome {
                Some(peer) => {
                    self.asked.insert(address, peer);
                }
                None => {
                    self.asked.remove(&address);
                }
            },
            Observation::DisconnectedLocally(peer) => self.redial.disconnected_locally(peer),
            Observation::Gossip { mesh, subscribers } => {
                self.mesh = mesh;
                self.subscribers = subscribers;
            }
            Observation::Carried(carried) => match carried {
                Carried::Sent => self.traffic.messages_sent += 1,
                Carried::Published => self.traffic.messages_published += 1,
                Carried::JoinRequest => self.traffic.join_requests_received += 1,
                Carried::ClaimRequest => self.traffic.claim_requests_received += 1,
            },
        }
    }

    /// The redials due at `now`, each a peer and the address to dial. A
    /// peer is redialed on the policy's fast schedule (exponential backoff for
    /// a fixed number of attempts), then once per slow interval for as long as
    /// it stays away; it is never given up on. Whether a redial worked shows
    /// as a later [`Observation::ConnectionOpened`], which ends the redial.
    pub(crate) fn redials_due(&mut self, now: Instant) -> Vec<(PeerId, Multiaddr)> {
        self.redial.due(now)
    }

    /// How often the swarm task asks for [`Self::redials_due`].
    pub(crate) fn redial_check_interval(&self) -> Duration {
        self.redial.policy.check_interval
    }

    /// `peer`'s address of record, unless it is only the source address of
    /// `peer`'s inbound connection, which `peer` need not listen on (see the
    /// module doc's "Where a peer's address comes from").
    pub(crate) fn dialable_address(&self, peer: &PeerId) -> Option<Multiaddr> {
        self.addresses
            .get(peer)
            .filter(|known| known.source > AddressSource::InboundRemote)
            .map(|known| known.addr.clone())
    }

    /// The connected peer at `address`: the one a dial there last found, while
    /// still connected, or else a connected peer whose address of record is
    /// `address`.
    pub(crate) fn peer_connected_at(&self, address: &Multiaddr) -> Option<PeerId> {
        self.asked
            .get(address)
            .copied()
            .filter(|peer| self.is_connected(peer))
            .or_else(|| {
                self.addresses
                    .iter()
                    .find(|(peer, known)| known.addr == *address && self.is_connected(peer))
                    .map(|(peer, _)| *peer)
            })
    }

    /// Whether the swarm holds a connection to `peer`.
    pub(crate) fn is_connected(&self, peer: &PeerId) -> bool {
        self.connected.contains(peer)
    }

    /// The address this node gives other nodes.
    pub(crate) fn own_address(&self) -> Option<Multiaddr> {
        self.local_addr.borrow().clone()
    }

    /// Everything a `Net` reports about its peers, as of now.
    pub(crate) fn snapshot(&self) -> Diagnostics {
        Diagnostics {
            connected: self.connected.iter().map(worker_id_of).collect(),
            peer_addresses: self
                .addresses
                .iter()
                .map(|(peer, known)| (worker_id_of(peer), known.addr.clone()))
                .collect(),
            local_addr: self.own_address(),
            redial_attempts: self
                .redial
                .pending
                .iter()
                .map(|(peer, attempt)| (worker_id_of(peer), attempt.attempts_made))
                .collect(),
            shard_subscribers: self.subscribers.iter().map(worker_id_of).collect(),
            shard_mesh: self.mesh.iter().map(worker_id_of).collect(),
            traffic: self.traffic,
        }
    }

    /// Records `addr` for `peer` unless a strictly better-ranked source is
    /// already on file. An equal rank *does* overwrite, so a fresher
    /// observation of the same kind (a re-dial to a new address, a later
    /// Identify) still wins.
    fn record_address(&mut self, peer: PeerId, addr: Multiaddr, source: AddressSource) {
        if self
            .addresses
            .get(&peer)
            .is_some_and(|known| known.source > source)
        {
            return;
        }
        self.addresses.insert(peer, KnownAddress { addr, source });
    }

    /// Records, as its sender's address, the address an arriving message's
    /// sender stamped on it for itself (see [`stamped_address`]). The sender
    /// is the one `Net` vouches for: a gossip message's signed author, or the
    /// peer at the other end of the connection a direct message came over.
    fn record_stamped_address(&mut self, input: &Input) {
        let Input::Message { from, message } = input else {
            return;
        };
        let Some(address) = stamped_address(from, message) else {
            return;
        };
        let Ok(peer) = PeerId::from_str(from.as_str()) else {
            return;
        };
        self.record_address(peer, address, AddressSource::SelfStamped);
    }
}

/// The address `from` stamped on `message` for itself: the initiator's
/// address on a roll call `from` initiated, or the responder's on a reply
/// `from` wrote. `None` for any other message, for a stamp that names
/// someone other than `from` (a peer relaying, or lying about, another
/// worker's message must not redirect traffic meant for it), and for a stamp
/// that is empty, does not parse, or is a wildcard bind.
fn stamped_address(from: &WorkerId, message: &CheckedMessage) -> Option<Multiaddr> {
    let (author, stamp) = match message.payload()? {
        CheckedPayload::RollCall(call) => {
            (call.initiator_id(), call.initiator_address.clone())
        }
        CheckedPayload::RollCallReply(reply) => {
            (reply.responder_id(), reply.responder_address.clone())
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

/// Whether an address advertised by Identify is worth recording: an
/// unspecified IP (`0.0.0.0`/`::`) is a wildcard bind, never a destination.
pub(crate) fn is_dialable_listen_addr(addr: &Multiaddr) -> bool {
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

/// The redial schedule of [`RedialPolicy`]: which dropped peers are
/// being redialed, and how far along each is (see the module doc's
/// "Reconnect/backoff" for which drops qualify).
struct RedialTracker {
    policy: RedialPolicy,
    /// Peers this node itself asked to disconnect: a local decision, not a
    /// failure, so never redialed. A peer leaves the set once it is seen to
    /// reconnect, so a later drop of it is judged afresh.
    locally_disconnected: HashSet<PeerId>,
    /// Every peer being redialed, until it reconnects or this node hangs up on it.
    pending: HashMap<PeerId, RedialAttempt>,
}

/// One peer's redial schedule.
struct RedialAttempt {
    attempts_made: u32,
    next_backoff: Duration,
    next_attempt_at: Instant,
    addr: Multiaddr,
}

impl RedialTracker {
    fn new(policy: RedialPolicy) -> Self {
        RedialTracker {
            policy,
            locally_disconnected: HashSet::new(),
            pending: HashMap::new(),
        }
    }

    /// Notes that this node asked to disconnect `peer`: it is not redialed,
    /// and a redial already scheduled for it is dropped.
    fn disconnected_locally(&mut self, peer: PeerId) {
        self.locally_disconnected.insert(peer);
        self.pending.remove(&peer);
    }

    /// Notes that `peer` connected again: it is done being redialed, and no
    /// longer excluded for having been disconnected locally. Only a peer seen
    /// closed and then opened again counts: a disconnect this node just asked
    /// for closes later, so an open seen before that close is not a
    /// reconnect.
    fn reconnected(&mut self, peer: &PeerId) {
        self.pending.remove(peer);
        self.locally_disconnected.remove(peer);
    }

    /// Notes that `peer`, in the gossip mesh, dropped: it is scheduled for its
    /// first redial at `addr` unless this node hung up on it.
    fn dropped(&mut self, peer: PeerId, addr: Multiaddr, now: Instant) {
        if self.locally_disconnected.contains(&peer) {
            return;
        }
        let first_backoff = self.policy.initial_backoff;
        self.pending.entry(peer).or_insert_with(|| RedialAttempt {
            attempts_made: 0,
            next_backoff: first_backoff,
            next_attempt_at: now + first_backoff,
            addr,
        });
    }

    fn due(&mut self, now: Instant) -> Vec<(PeerId, Multiaddr)> {
        let RedialPolicy {
            max_backoff,
            fast_attempts,
            slow_interval,
            ..
        } = self.policy;
        let mut dials = Vec::new();
        for (peer, attempt) in &mut self.pending {
            if attempt.next_attempt_at > now {
                continue;
            }
            dials.push((*peer, attempt.addr.clone()));
            attempt.attempts_made = attempt.attempts_made.saturating_add(1);
            let wait = if attempt.attempts_made < fast_attempts {
                attempt.next_backoff = std::cmp::min(attempt.next_backoff * 2, max_backoff);
                attempt.next_backoff
            } else {
                slow_interval
            };
            attempt.next_attempt_at = now + wait;
        }
        dials
    }
}

#[cfg(test)]
mod tests {
    use kabudachi_core::configuration::Configuration;
    use kabudachi_core::protocol::ids::ShardId;
    use kabudachi_core::protocol::checked;
    use kabudachi_core::protocol::messages::{
        ElectionMessage, RollCall, RollCallReply, election_message,
    };

    use super::*;

    fn ms(millis: u64) -> Duration {
        Duration::from_millis(millis)
    }

    /// Test-scale: backoffs 20, 40, then capped at 60; four fast attempts,
    /// then one every 100.
    fn policy() -> RedialPolicy {
        RedialPolicy {
            initial_backoff: ms(20),
            max_backoff: ms(60),
            fast_attempts: 4,
            slow_interval: ms(100),
            check_interval: ms(5),
        }
    }

    fn book() -> Peers {
        Peers::new(watch::channel(None).0, policy())
    }

    fn addr(text: &str) -> Multiaddr {
        text.parse().expect("a well-formed multiaddr")
    }

    fn open(book: &mut Peers, peer: PeerId, side: Side, address: &Multiaddr, at: Instant) {
        book.observe(
            Observation::ConnectionOpened {
                peer,
                side,
                address: address.clone(),
            },
            at,
        );
    }

    fn mesh_of(book: &mut Peers, peers: &[PeerId], at: Instant) {
        let mesh: BTreeSet<PeerId> = peers.iter().copied().collect();
        book.observe(
            Observation::Gossip {
                mesh: mesh.clone(),
                subscribers: mesh,
            },
            at,
        );
    }

    /// `peer` connected (this node dialed it at `at_address`), in the mesh,
    /// then dropped at `t`.
    fn meshed_then_dropped(book: &mut Peers, peer: PeerId, at_address: &Multiaddr, t: Instant) {
        open(book, peer, Side::Dialer, at_address, t);
        mesh_of(book, &[peer], t);
        book.observe(Observation::ConnectionClosed(peer), t);
    }

    /// A roll call `initiator` starts, stamped with `initiator_address`, with
    /// a well-formed configuration.
    fn roll_call(initiator: &WorkerId, initiator_address: &str) -> Input {
        Input::Message {
            from: initiator.clone(),
            message: checked::decode(ElectionMessage {
                payload: Some(election_message::Payload::RollCall(RollCall {
                    shard_id: Some(ShardId::new("shard-1").into()),
                    term: 1,
                    configuration: Some((&Configuration::genesis(0)).into()),
                    initiator_id: Some(initiator.clone().into()),
                    initiator_address: initiator_address.into(),
                    ..Default::default()
                })),
            })
            .expect("a well-formed roll call"),
        }
    }

    /// `responder`'s reply to `initiator`'s roll call, stamped with
    /// `responder_address`, as `from` sent it.
    fn roll_call_reply(
        from: &WorkerId,
        initiator: &WorkerId,
        responder: &WorkerId,
        responder_address: &str,
    ) -> Input {
        Input::Message {
            from: from.clone(),
            message: checked::decode(ElectionMessage {
                payload: Some(election_message::Payload::RollCallReply(RollCallReply {
                    shard_id: Some(ShardId::new("shard-1").into()),
                    term: 1,
                    initiator_id: Some(initiator.clone().into()),
                    responder_id: Some(responder.clone().into()),
                    responder_address: responder_address.into(),
                    admission: None,
                    prior_admission: None,
                })),
            })
            .expect("a well-formed roll call reply"),
        }
    }

    fn stored(book: &Peers, peer: &PeerId) -> Option<Multiaddr> {
        book.snapshot().peer_addresses.get(&worker_id_of(peer)).cloned()
    }

    #[test]
    fn a_dropped_mesh_peer_is_redialed_fast_then_slowly_and_never_given_up() {
        let (mut book, peer) = (book(), PeerId::random());
        let t0 = Instant::now();
        let at = addr("/ip4/192.0.2.7/tcp/4001");
        meshed_then_dropped(&mut book, peer, &at, t0);

        assert!(book.redials_due(t0 + ms(19)).is_empty());
        // 20, 60, 120, 180: the doubling waits, the third capped at 60. Then
        // 280, 380, 480: the slow interval, for as long as the peer is away.
        for due in [20, 60, 120, 180, 280, 380, 480] {
            assert_eq!(book.redials_due(t0 + ms(due)), vec![(peer, at.clone())], "{due} ms");
            assert!(book.redials_due(t0 + ms(due + 1)).is_empty(), "{due} ms, just after");
        }
        assert!(book.redials_due(t0 + ms(119)).is_empty());
        assert!(book.redials_due(t0 + ms(179)).is_empty());
        assert!(book.redials_due(t0 + ms(579)).is_empty());
        assert_eq!(book.snapshot().redial_attempts[&worker_id_of(&peer)], 7);

        // A peer that comes back ends the schedule.
        open(&mut book, peer, Side::Dialer, &at, t0 + ms(590));
        assert!(book.snapshot().redial_attempts.is_empty());
    }

    #[test]
    fn only_a_mesh_peer_this_node_did_not_hang_up_on_is_redialed() {
        type Steps = fn(&mut Peers, PeerId, &Multiaddr, Instant);
        fn row(what: &'static str, steps: Steps, redialed: bool) -> (&'static str, Steps, bool) {
            (what, steps, redialed)
        }

        let rows = [
            row("meshed then dropped", meshed_then_dropped, true),
            row(
                "dropped while outside the mesh",
                |book, peer, at, t| {
                    open(book, peer, Side::Dialer, at, t);
                    mesh_of(book, &[], t);
                    book.observe(Observation::ConnectionClosed(peer), t);
                },
                false,
            ),
            row(
                "hung up on, then closed",
                |book, peer, at, t| {
                    open(book, peer, Side::Dialer, at, t);
                    mesh_of(book, &[peer], t);
                    book.observe(Observation::DisconnectedLocally(peer), t);
                    book.observe(Observation::ConnectionClosed(peer), t);
                },
                false,
            ),
            row(
                "closed, then hung up on",
                |book, peer, at, t| {
                    meshed_then_dropped(book, peer, at, t);
                    book.observe(Observation::DisconnectedLocally(peer), t);
                },
                false,
            ),
            row(
                "hung up on, reconnected, then dropped",
                |book, peer, at, t| {
                    open(book, peer, Side::Dialer, at, t);
                    mesh_of(book, &[peer], t);
                    book.observe(Observation::DisconnectedLocally(peer), t);
                    book.observe(Observation::ConnectionClosed(peer), t);
                    open(book, peer, Side::Dialer, at, t);
                    mesh_of(book, &[peer], t);
                    book.observe(Observation::ConnectionClosed(peer), t);
                },
                true,
            ),
            row(
                "closed, then reopened before the redial is due",
                |book, peer, at, t| {
                    meshed_then_dropped(book, peer, at, t);
                    open(book, peer, Side::Dialer, at, t + ms(10));
                },
                false,
            ),
            row(
                "closed, then dropped from the mesh after the fact",
                |book, peer, at, t| {
                    meshed_then_dropped(book, peer, at, t);
                    mesh_of(book, &[], t);
                },
                true,
            ),
        ];

        for (what, steps, redialed) in rows {
            let (mut book, peer) = (book(), PeerId::random());
            let t0 = Instant::now();
            steps(&mut book, peer, &addr("/ip4/192.0.2.7/tcp/4001"), t0);
            assert_eq!(!book.redials_due(t0 + ms(20)).is_empty(), redialed, "{what}");
        }
    }

    #[test]
    fn a_peer_address_is_only_replaced_by_an_equal_or_better_ranked_source() {
        let (mut book, peer) = (book(), PeerId::random());
        let t = Instant::now();
        let send_back = addr("/ip4/127.0.0.1/tcp/54321");
        let dialed = addr("/ip4/127.0.0.1/tcp/4001");
        let advertised = addr("/ip4/127.0.0.1/tcp/4002");
        let fresher_advertised = addr("/ip4/127.0.0.1/tcp/4003");
        let identified = |listen: &Multiaddr| Observation::Identified {
            peer,
            listen_addresses: vec![listen.clone()],
        };

        open(&mut book, peer, Side::Listener, &send_back, t);
        assert_eq!(stored(&book, &peer), Some(send_back.clone()));
        assert_eq!(book.dialable_address(&peer), None, "an inbound source address is not dialable");

        open(&mut book, peer, Side::Dialer, &dialed, t);
        assert_eq!(stored(&book, &peer), Some(dialed.clone()), "a dialed address outranks a source address");
        assert_eq!(book.dialable_address(&peer), Some(dialed));

        book.observe(identified(&advertised), t);
        assert_eq!(stored(&book, &peer), Some(advertised.clone()), "Identify outranks both");

        open(&mut book, peer, Side::Listener, &send_back, t);
        assert_eq!(
            stored(&book, &peer),
            Some(advertised),
            "a later inbound connection must not displace an Identify-advertised address"
        );

        book.observe(identified(&fresher_advertised), t);
        assert_eq!(
            stored(&book, &peer),
            Some(fresher_advertised),
            "an equally-ranked observation is a fresher one and does replace"
        );
    }

    #[test]
    fn a_stamp_ranks_below_identify_and_a_dialed_address_and_above_an_inbound_source() {
        let (peer, t) = (PeerId::random(), Instant::now());
        let sender = worker_id_of(&peer);
        let stamped = addr("/ip4/192.0.2.7/tcp/4001");
        let arrival = roll_call(&sender, "/ip4/192.0.2.7/tcp/4001");

        let mut fresh = book();
        fresh.observe(Observation::MessageArrived(&arrival), t);
        assert_eq!(stored(&fresh, &peer), Some(stamped.clone()));

        let known = addr("/ip4/192.0.2.9/tcp/4001");
        for better in ["a dialed address", "an Identify address"] {
            let mut book = book();
            if better == "a dialed address" {
                open(&mut book, peer, Side::Dialer, &known, t);
            } else {
                book.observe(
                    Observation::Identified { peer, listen_addresses: vec![known.clone()] },
                    t,
                );
            }
            book.observe(Observation::MessageArrived(&arrival), t);
            assert_eq!(stored(&book, &peer), Some(known.clone()), "{better}");
        }

        let mut book = book();
        open(&mut book, peer, Side::Listener, &addr("/ip4/127.0.0.1/tcp/54321"), t);
        book.observe(Observation::MessageArrived(&arrival), t);
        assert_eq!(
            stored(&book, &peer),
            Some(stamped),
            "a stamp outranks an inbound connection's source address"
        );
    }

    #[test]
    fn a_stamp_is_recorded_only_when_it_is_its_senders_own_dialable_address() {
        let worker = || worker_id_of(&PeerId::random());
        let (relay, initiator, responder) = (worker(), worker(), worker());
        let stamp = "/ip4/192.0.2.7/tcp/4001";
        let rows = [
            ("an empty stamp", roll_call(&initiator, ""), None),
            ("an unparsable stamp", roll_call(&initiator, "not a multiaddr"), None),
            ("a wildcard stamp", roll_call(&initiator, "/ip4/0.0.0.0/tcp/4001"), None),
            (
                "a roll call's stamp is its initiator's, not its relay's",
                Input::Message { from: relay, message: message_of(&roll_call(&initiator, stamp)) },
                None,
            ),
            (
                "a reply's stamp is its responder's, not its initiator's",
                roll_call_reply(&initiator, &initiator, &responder, stamp),
                None,
            ),
            (
                "control: the sender's own dialable stamp",
                roll_call(&initiator, stamp),
                Some((&initiator, addr(stamp))),
            ),
        ];

        for (what, arrival, recorded) in rows {
            let mut book = book();
            book.observe(Observation::MessageArrived(&arrival), Instant::now());
            let expected: BTreeMap<WorkerId, Multiaddr> = recorded
                .into_iter()
                .map(|(worker, address)| (worker.clone(), address))
                .collect();
            assert_eq!(book.snapshot().peer_addresses, expected, "{what}");
        }
    }

    fn message_of(input: &Input) -> checked::CheckedMessage {
        let Input::Message { message, .. } = input else {
            unreachable!("the builders above make messages");
        };
        message.clone()
    }

    #[test]
    fn a_peer_asked_at_an_address_is_found_there_only_while_connected() {
        let (mut book, peer, t) = (book(), PeerId::random(), Instant::now());
        let at = addr("/ip4/192.0.2.7/tcp/4001");

        book.observe(Observation::Dialed { address: at.clone(), outcome: Some(peer) }, t);
        open(&mut book, peer, Side::Dialer, &addr("/ip4/192.0.2.8/tcp/4001"), t);
        assert_eq!(book.peer_connected_at(&at), Some(peer));

        book.observe(Observation::ConnectionClosed(peer), t);
        assert_eq!(book.peer_connected_at(&at), None);

        // A later dial there that fails forgets whoever answered before.
        open(&mut book, peer, Side::Dialer, &addr("/ip4/192.0.2.8/tcp/4001"), t);
        assert_eq!(book.peer_connected_at(&at), Some(peer));
        book.observe(Observation::Dialed { address: at.clone(), outcome: None }, t);
        assert_eq!(book.peer_connected_at(&at), None);

        // A connected peer whose address of record is the address needs no Dialed.
        let (other, other_at) = (PeerId::random(), addr("/ip4/192.0.2.9/tcp/4001"));
        open(&mut book, other, Side::Dialer, &other_at, t);
        assert_eq!(book.peer_connected_at(&other_at), Some(other));
    }

    #[test]
    fn own_address_never_trades_a_routable_address_for_loopback() {
        let (own, receiver) = watch::channel(None);
        let mut book = Peers::new(own, policy());
        let t = Instant::now();

        for listening in ["/ip4/127.0.0.1/tcp/4001", "/ip4/192.0.2.7/tcp/4001", "/ip4/127.0.0.2/tcp/4001"] {
            book.observe(Observation::ListeningOn(addr(listening)), t);
        }

        let routable = Some(addr("/ip4/192.0.2.7/tcp/4001"));
        assert_eq!(book.own_address(), routable);
        assert_eq!(*receiver.borrow(), routable);
        assert_eq!(book.snapshot().local_addr, routable);
    }

    #[test]
    fn the_snapshot_counts_traffic_by_kind() {
        let mut book = book();
        let t = Instant::now();
        let arrival = roll_call(&worker_id_of(&PeerId::random()), "");

        book.observe(Observation::MessageArrived(&arrival), t);
        for carried in [Carried::Sent, Carried::Published, Carried::JoinRequest, Carried::ClaimRequest] {
            book.observe(Observation::Carried(carried), t);
        }

        let traffic = book.snapshot().traffic;
        assert_eq!(
            (
                traffic.messages_received,
                traffic.messages_sent,
                traffic.messages_published,
                traffic.join_requests_received,
                traffic.claim_requests_received,
            ),
            (1, 1, 1, 1, 1)
        );
        assert_eq!(traffic.total(), 5);
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
}
