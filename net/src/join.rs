//! The JOIN protocol's two halves over [`Net`] (`/kabudachi/join/1`, see
//! `crate::join_codec`), one for the bootstrap cascade and the driver's
//! rejoin alike:
//!
//! - the client: [`ask_for_leader`] asks peers who lead the shard, all at
//!   once, and connects to the newest leader they point at (the leader
//!   search, `crate::leader_search`, decides whom to ask and when);
//! - the responder's answer: [`pointer_for`], the pointer a node hands a
//!   joiner. A pointer names the shard's leader, which only
//!   `core::election::WorkerNode` knows, and its address, which only `Net`
//!   knows, so it is put together here.
//!
//! A JOIN is a correlated request/response, unlike the election protocol:
//! each ask dials its peer and waits, bounded by a per-peer timeout, for
//! *that dial's own* connection, identified by the dial's `ConnectionId`
//! rather than by diffing the connected-peer set, so a late connection from
//! an abandoned ask is never mistaken for a later one's.

use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::time::Duration as StdDuration;

use kabudachi_core::election::{JoinFloor, WorkerNode};
use kabudachi_core::protocol::ids::WorkerId;
use kabudachi_core::protocol::messages::{JoinRequest, JoinResponse};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::time::Clock;
use libp2p::futures::StreamExt;
use libp2p::futures::stream::FuturesUnordered;
use libp2p::{Multiaddr, PeerId};
use tokio::time::Instant;

use crate::exchange::Asked;
use crate::join_codec::JoinCodec;
use crate::messenger::{DialTarget, Net};
use crate::peers::worker_id_of;

/// An unanswered inbound `/kabudachi/join/1` request, returned by
/// [`Net::poll_join_requests`]. Answer it with [`Net::respond_join`];
/// dropping it unanswered just lets the requester's substream eventually
/// fail with `OutboundFailure` on their side (nothing here relies on that
/// happening).
pub struct JoinRequestHandle(Asked<JoinCodec>);

impl JoinRequestHandle {
    /// The `WorkerId` of whoever sent this join request.
    pub fn from(&self) -> WorkerId {
        worker_id_of(&self.0.from)
    }
}

impl Net {
    /// Drains every inbound `/kabudachi/join/1` request not yet answered.
    /// Answer each with `Self::respond_join`.
    pub fn poll_join_requests(&self) -> Vec<JoinRequestHandle> {
        self.take_asked::<JoinCodec>()
            .into_iter()
            .map(JoinRequestHandle)
            .collect()
    }

    /// Answers a join request obtained from `Self::poll_join_requests`.
    /// Fire-and-forget like `send`: if the driver task has already stopped,
    /// there's nowhere for the answer to go, and that's fine to drop.
    pub fn respond_join(&self, handle: JoinRequestHandle, response: JoinResponse) {
        self.answer::<JoinCodec>(handle.0.channel, response);
    }

    /// Sends a `JOIN_REQUEST` to `to` (which must already be connected, see
    /// this module's `ask_for_leader`, the only caller) and awaits its
    /// `JOIN_RESPONSE`. `None` if the driver task is gone, the request fails
    /// outright (`OutboundFailure`), or the peer disconnects before
    /// answering.
    pub(crate) async fn send_join_request(&self, to: PeerId) -> Option<JoinResponse> {
        self.ask::<JoinCodec>(to, JoinRequest {}).await
    }
}

/// How long [`ask_for_leader`] waits, per peer, for a connection and then a
/// `JOIN_RESPONSE` before giving up on that peer.
pub const DEFAULT_JOIN_PEER_TIMEOUT: StdDuration = StdDuration::from_secs(10);

/// What one pass of [`ask_for_leader`] over its peers found.
#[derive(Debug, Clone, PartialEq)]
pub enum LeaderSearch {
    /// A peer pointed at a leader, the newest of the pass this node could
    /// reach, and it is now connected to it.
    Found(JoinResponse),
    /// Some peer answered, but none pointed at a leader this node could
    /// reach: the shard exists, and its leader may not be elected yet.
    NoReachableLeader,
    /// No peer answered at all.
    NoAnswer,
}

/// One pass of the JOIN client: asks every one of `peers` who leads the
/// shard, over `net`, all at once: every seed is dialed at the same time, so a
/// pass may leave a connection to each live seed. A peer that fails to connect or answer
/// within `per_peer_timeout`, answers "no leader known", or points at an
/// address that does not parse, contributes no pointer.
///
/// `floor` is the recovery epoch the caller rejoins at, [`JoinFloor::none`]
/// for a first join. It alone judges the pointers: one it does not accept is
/// treated as no pointer, and it never starts the grace window below.
///
/// The pass ends when every peer has answered or timed out, or `grace` after
/// the first answer that points at an acceptable leader arrived, whichever
/// comes first: a live seed's answer is not held up by a dead seed's full
/// timeout, and a seed that answers within `grace` of the first still has its
/// say. Callers pass the shard's suspicion timeout, the time after which a
/// silent leader is no longer waited on.
///
/// It then takes the acceptable pointers newest first, as
/// [`JoinFloor::newest_first`] ranks them (equally new ones in `peers`
/// order), and returns
/// [`LeaderSearch::Found`] with the first whose leader this node is then
/// connected to (see [`connect_to_leader`]); the caller enters the shard
/// with it (`core::election::Entry::Joining`). A pointer to a leader that
/// cannot be reached is passed over for the next newest. These connections
/// are made one at a time after the gathering ends, each bounded by
/// `per_peer_timeout` on its own.
///
/// Otherwise the pass says whether anyone answered at all:
/// [`LeaderSearch::NoReachableLeader`] when some peer did (even "no leader
/// known", or a pointer that does not parse or cannot be reached), which
/// shows the shard exists; [`LeaderSearch::NoAnswer`] when none did. Asking
/// again is the caller's (see `crate::bootstrap`). A peer asked again is
/// asked over the connection this node already has to it, if that is still
/// up, rather than dialed afresh.
pub async fn ask_for_leader(
    net: &Net,
    peers: &[Multiaddr],
    floor: JoinFloor,
    per_peer_timeout: StdDuration,
    grace: StdDuration,
) -> LeaderSearch {
    let asks = peers
        .iter()
        .map(|peer| {
            Box::pin(ask_peer_for_leader(net, peer, per_peer_timeout)) as PeerAsk<'_>
        })
        .collect();
    let answers = gather_answers(asks, grace, |pointer| {
        floor.accepts(pointer) && pointed_leader(pointer).is_some()
    })
    .await;
    let a_peer_answered = answers.iter().any(Option::is_some);
    let answers: Vec<_> = answers.into_iter().flatten().collect();
    let pointers: Vec<_> = floor
        .newest_first(&answers)
        .into_iter()
        .filter_map(|response| {
            let (leader, leader_addr) = pointed_leader(response)?;
            Some((response, leader, leader_addr))
        })
        .collect();
    for (response, leader, leader_addr) in pointers {
        if connect_to_leader(net, &leader, leader_addr, per_peer_timeout).await {
            return LeaderSearch::Found(response.clone());
        }
    }
    if a_peer_answered {
        LeaderSearch::NoReachableLeader
    } else {
        LeaderSearch::NoAnswer
    }
}

/// One peer's ask of a pass, in flight.
type PeerAsk<'a> = Pin<Box<dyn Future<Output = Option<JoinResponse>> + Send + 'a>>;

/// Drives every ask at once and returns what each answered, in the order
/// the asks were given (`None` for one that did not answer). It stops early
/// when `grace` has passed since the first answer `starts_grace` holds of
/// arrived, dropping the asks still in flight: their dials and requests are
/// abandoned exactly as a per-peer timeout abandons them.
async fn gather_answers(
    asks: Vec<PeerAsk<'_>>,
    grace: StdDuration,
    starts_grace: impl Fn(&JoinResponse) -> bool,
) -> Vec<Option<JoinResponse>> {
    let mut answers = vec![None; asks.len()];
    let mut in_flight: FuturesUnordered<_> = asks
        .into_iter()
        .enumerate()
        .map(|(index, ask)| async move { (index, ask.await) })
        .collect();
    let mut grace_ends = None;
    loop {
        let next = match grace_ends {
            Some(end) => match tokio::time::timeout_at(end, in_flight.next()).await {
                Ok(next) => next,
                Err(_) => break,
            },
            None => in_flight.next().await,
        };
        let Some((index, answer)) = next else { break };
        if grace_ends.is_none() && answer.as_ref().is_some_and(&starts_grace) {
            grace_ends = Some(Instant::now() + grace);
        }
        answers[index] = answer;
    }
    answers
}

/// The `JOIN_RESPONSE` `node`, running over `net`, gives a joiner right
/// now: `WorkerNode::join_response`, at the address `net` can give a joiner
/// for the leader `node.known_leader()` names.
///
/// "No leader known" (an empty response) when the node knows no leader, or
/// has no dialable address for it: this node's own address before its first
/// successful `listen_on`, or a leader known only by the source address of
/// its inbound connection (see `Net::dialable_address`). A pointer the
/// joiner cannot dial would strand it, so the joiner is sent on to its next
/// seed instead.
pub async fn pointer_for<C: Clock>(node: &WorkerNode<C>, net: &Net) -> JoinResponse {
    // The node's answer, read once, names the leader whose address it
    // still needs.
    let Some(mut pointer) = node.join_response(String::new()) else {
        return JoinResponse::default();
    };
    let Some(leader_id) = pointer.leader_id() else {
        return JoinResponse::default();
    };
    let leader_addr = if &leader_id == node.id() {
        net.local_multiaddr()
    } else {
        net.dialable_address(&leader_id).await
    };
    let Some(leader_addr) = leader_addr else {
        return JoinResponse::default();
    };
    pointer.leader_multiaddr = leader_addr.to_string();
    pointer
}

/// One peer of [`ask_for_leader`]'s pass: send `JOIN_REQUEST` to the peer at
/// `address` and await the response, bounded by `per_peer_timeout`. While
/// the peer an earlier ask found at `address` is still connected, it is
/// asked directly. Otherwise `address` is dialed and this waits, also
/// bounded by `per_peer_timeout`, for that dial's own connection (see the
/// module doc); the swarm task records the peer it finds there.
async fn ask_peer_for_leader(
    net: &Net,
    address: &Multiaddr,
    per_peer_timeout: StdDuration,
) -> Option<JoinResponse> {
    // A peer this node already holds a connection to at `address` is asked
    // over it: dialing `address` afresh would only open a second connection
    // to the same peer. A worker fenced for losing the authority alone
    // keeps its connections, and rejoins through the addresses its peers
    // registered.
    let peer = match connected_peer_at(net, address).await {
        Some(peer) => peer,
        None => {
            // The swarm task records whoever answers there (or forgets whoever
            // did before) as the dial ends.
            tokio::time::timeout(
                per_peer_timeout,
                net.dial_for_connection(DialTarget::Address(address.clone())),
            )
            .await
            .ok()
            .flatten()?
        }
    };

    tokio::time::timeout(per_peer_timeout, net.send_join_request(peer))
        .await
        .ok()?
}

/// A peer this node is connected to at `address`: the one an earlier ask
/// found there, while still connected, or else a connected peer whose
/// address of record is `address`.
async fn connected_peer_at(net: &Net, address: &Multiaddr) -> Option<PeerId> {
    let address = address.clone();
    net.with_peers(move |peers| peers.peer_connected_at(&address))
        .await
        .flatten()
}

/// Whether this node ends up connected to `leader`: at once if it already
/// is (the leader was the seed that answered, say); otherwise by dialing
/// `leader` at `leader_addr`, and any address `kad`'s routing table
/// separately knows for it (see `crate::swarm`'s "kad: peer routing, not
/// membership" — `DialOpts::extend_addresses_through_behaviour` is what asks
/// for that here; `WithPeerIdWithAddresses::addresses` alone defaults it
/// off), and waiting, bounded by `per_peer_timeout`, for that dial's own
/// connection. So a `leader_addr` that is loopback or otherwise unreachable
/// from here is not the only way to reach `leader`: a `kad` entry for it,
/// learned from any other peer's Identify, gives this dial a second address
/// to try.
///
/// The dial names `leader`'s peer id, so if some other peer answers at
/// `leader_addr` (a leader restarted under a new identity, or the address
/// reused), libp2p refuses it as `WrongPeerId` and closes that connection. A
/// stale pointer asked about on every pass of [`ask_for_leader`] therefore
/// costs one failed dial per pass, never a connection left open.
async fn connect_to_leader(
    net: &Net,
    leader: &WorkerId,
    leader_addr: Multiaddr,
    per_peer_timeout: StdDuration,
) -> bool {
    let Ok(leader_peer) = PeerId::from_str(leader.as_str()) else {
        return false;
    };
    if net
        .with_peers(move |peers| peers.is_connected(&leader_peer))
        .await
        .unwrap_or(false)
    {
        return true;
    }
    let target = DialTarget::Peer {
        peer: leader_peer,
        address: leader_addr,
    };
    let dialed = tokio::time::timeout(per_peer_timeout, net.dial_for_connection(target))
        .await
        .ok()
        .flatten();
    dialed == Some(leader_peer)
}

/// The leader a `JOIN_RESPONSE` points at and the address to dial it on.
/// `None` for "no leader known" (see join.proto), and for an address that
/// does not parse — which [`ask_for_leader`] treats like "no leader known":
/// the peer did answer, so the shard exists. The codec has already rejected
/// a leader without an address (`WellFormed`), so a named leader always
/// comes with some string.
fn pointed_leader(response: &JoinResponse) -> Option<(WorkerId, Multiaddr)> {
    let leader = response.leader_id()?;
    let leader_addr = response.leader_multiaddr.parse().ok()?;
    Some((leader, leader_addr))
}
