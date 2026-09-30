//! Swarm construction: proves a minimal libp2p node builds and links under
//! both `cargo` and Bazel, and that two such nodes complete an `identify`
//! handshake over TCP+noise+yamux.
//!
//! `Behaviour` holds `identify`, `gossipsub`, `kad` and three
//! `request_response` behaviours: one carrying the election protocol (see
//! `crate::codec`), one the bootstrap join protocol (see
//! `crate::join_codec`), and one the claim arbitration protocol (see
//! `crate::claim::codec`) — each a deliberately separate wire protocol, not a
//! variant folded into `ElectionMessage` (see `crate::join_codec`'s module
//! doc). `gossipsub` carries the election messages a worker publishes to its
//! whole shard rather than sends to one peer; every message is signed with
//! the swarm's keypair and every arriving one must carry a valid signature,
//! so its author is known. `crate::messenger`'s `Net` carries election
//! messages over this swarm, and consumes `identify::Event::Received` there
//! to learn each peer's real, dialable listen address (see that module's
//! "Where a peer's address comes from"). The struct is deliberately left
//! easy to extend with more fields rather than, say, wrapping a single
//! behaviour type.
//!
//! ## `kad`: peer routing, not membership
//!
//! `kad` runs in [`libp2p::kad::Mode::Server`] (this node answers other
//! peers' DHT queries, not just makes its own) with no record store actually
//! used — the routing table (`FIND_NODE`) is the only part of Kademlia this
//! crate wants. It needs no dedicated bootstrap peer list of its own: `kad`'s
//! own default (`libp2p_kad::Config`'s `BucketInserts::OnConnected`)
//! registers a newly connected peer as soon as this node dials it, using the
//! address it just dialed successfully; `crate::messenger`'s `handle_event`,
//! in its `identify::Event::Received` arm, also calls `kad.add_address` with
//! every listen address Identify hands it, which is what closes the one gap
//! that default leaves open — a peer that dialed *this* node, whose
//! connection-derived address (its own ephemeral source port) `kad`
//! deliberately never registers on its own (see
//! `libp2p_kad::behaviour::Behaviour::on_connection_handler_event`'s own
//! comment: only the dialing side's address is safe to put in a routing
//! table and hand to other peers). Once a node's table holds any peer,
//! [`libp2p::kad::Behaviour::bootstrap`] self-triggers, throttled, whenever
//! a new peer is added to a routing table below the `k` bucket target, and
//! every five minutes regardless. The crawl gives a node a route to a peer
//! it never dialed itself: an address for it, learned from whichever other
//! peer's routing table already had it, that this node can then dial
//! directly. That self-triggered crawl runs when a joiner first connects,
//! before its leader has learned of the workers joining alongside it, so
//! `crate::driver::run_driver` also asks for one once its node's view of
//! the shard settles, and periodically (`Net::refresh_peer_routing`).
//!
//! `libp2p-swarm`'s own dial-address aggregation
//! (`NetworkBehaviour::handle_pending_outbound_connection`) is what makes a
//! `kad` route usable: a `Swarm::dial` naming only a `PeerId` asks every
//! behaviour, `kad` included, for addresses of that peer, and a routing-table
//! entry answers even when nothing else on this node has a dialable address
//! for it on file. Two call sites in `crate::messenger` reach a peer this
//! way: `Net::send`'s dial
//! (`request_response::Behaviour::send_request_with_addresses` always
//! extends through behaviours, whether or not it was given an address of
//! its own) and the dial to the leader a `JOIN_RESPONSE` names
//! (`crate::join`'s `connect_to_leader`, which asks for that extension explicitly —
//! building a `DialOpts` with its own address list defaults it off). So a
//! leader address a `JOIN_RESPONSE` names that turns out to be unreachable
//! from where the joiner stands is not the only way it can reach that
//! leader, once some other peer's Identify has given `kad` a route to it.
//!
//! The routing table itself is never consulted for anything else: it is not
//! a membership list, a peer in it may not be part of any shard this node
//! serves, and nothing here reads it directly. No record is ever put or
//! fetched, so [`libp2p::kad::store::MemoryStore`] — required only because
//! [`libp2p::kad::Behaviour`] is generic over a `RecordStore` — never
//! actually holds one.

use std::pin::Pin;
use std::task::{Context, Poll};

use libp2p::core::transport::{
    DialOpts, ListenerId, PortUse, Transport, TransportError, TransportEvent,
};
use libp2p::core::upgrade::Version;
use libp2p::request_response::{self, ProtocolSupport};
use libp2p::swarm::NetworkBehaviour;
use libp2p::{
    Multiaddr, Swarm, allow_block_list, gossipsub, identify, kad, noise, tcp, yamux,
};

use crate::claim::codec::{ClaimCodec, PROTOCOL as CLAIM_PROTOCOL};
use crate::codec::{ElectionCodec, PROTOCOL};
use crate::join_codec::{JoinCodec, PROTOCOL as JOIN_PROTOCOL};

/// Protocol version string advertised by `identify`. Not yet load-bearing
/// (nothing checks it), but real peers should agree on it eventually.
const IDENTIFY_PROTOCOL_VERSION: &str = "/kabudachi/1.0.0";

/// How long a connection with no active streams, held open by no connection
/// handler's own keep-alive, stays open: 60 seconds. libp2p-swarm's own
/// default is 10 seconds.
///
/// The connections a worker needs keep themselves open. gossipsub's
/// connection handler reports `connection_keep_alive() == true` for as long
/// as the peer is a mesh member for some topic
/// (`libp2p_gossipsub::handler::Handler::connection_keep_alive`,
/// `matches!(self, Handler::Enabled(h) if h.in_mesh)`), whatever this
/// timeout, and `crate::driver::run_driver` subscribes every driven node to
/// its shard's topic from its first batch. A follower's connection to its
/// leader carries a heartbeat every heartbeat interval, which is seconds, so
/// it never goes idle this long.
///
/// What this closes is a connection nothing needs any more: a JOIN seed
/// dial, a `kad` crawl connection, a shard peer past gossipsub's mesh cap
/// (`mesh_n_high`, 12 by default). None of those is redialed once closed
/// (`crate::messenger`'s "Which drops are redial-eligible" redials only
/// gossip-mesh peers), so closing one costs nothing, and a node that meets
/// many peers does not hold every connection it ever made. A later routing
/// crawl (see `crate::driver::run_driver`) may open a crawl connection
/// again, at most once per peer per crawl.
///
/// The value stays well above libp2p's 10 s so a connection opened for a JOIN
/// or a claim, which no gossip mesh keeps alive, is not closed between the
/// steps that use it. No test waits out the timeout: the property is this
/// constant's size, and a test that held a connection idle past 10 s to show
/// it survives cost about 15 s of every run for it.
const IDLE_CONNECTION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

#[derive(NetworkBehaviour)]
pub struct Behaviour {
    pub identify: identify::Behaviour,
    pub gossipsub: gossipsub::Behaviour,
    pub kad: kad::Behaviour<kad::store::MemoryStore>,
    pub request_response: request_response::Behaviour<ElectionCodec>,
    pub join: request_response::Behaviour<JoinCodec>,
    pub claim: request_response::Behaviour<ClaimCodec>,
    /// The peers this node refuses to hold a connection to, either way (see
    /// `crate::messenger::Net::block_peer`).
    pub blocked: allow_block_list::Behaviour<allow_block_list::BlockedPeers>,
}

/// libp2p's TCP transport, except that every dial it makes leaves from a
/// port of its own ([`PortUse::New`]) instead of from this node's listen
/// port.
///
/// libp2p-swarm's `DialOpts` default to [`PortUse::Reuse`], and every
/// behaviour's own dials (`kad`'s crawl, `request_response`'s, a redial)
/// take that default, so libp2p-tcp binds each dial to the listen address.
/// Two listening nodes that dial each other at once, as joiners' lockstep
/// routing crawls do (see `crate::driver::run_driver`), then open one TCP
/// connection from both ends (a simultaneous open): both sides act as noise
/// initiator and the handshake fails, and every later dial between the two
/// listen ports fails with `EADDRINUSE` while that connection's 4-tuple sits
/// in `TIME_WAIT` (tens of seconds). A dial from a port of its own has a
/// 4-tuple no other connection shares. libp2p offers no setting for this:
/// `tcp::Config::port_reuse` is deprecated and does nothing.
///
/// What is given up is port-reuse NAT hole punching (DCUtR), which this
/// crate does not use. Nothing here depends on a dial's source port: a
/// peer's dialable address comes from Identify, not from the connection
/// (see `crate::messenger`'s "Where a peer's address comes from").
/// Behaviours still see `ConnectedPoint::Dialer { port_use: Reuse }` for
/// such a dial: the swarm records the dial options it asked for, not the
/// ones this wrapper passed on.
struct NewPortTcp(tcp::tokio::Transport);

impl Transport for NewPortTcp {
    type Output = <tcp::tokio::Transport as Transport>::Output;
    type Error = <tcp::tokio::Transport as Transport>::Error;
    type ListenerUpgrade = <tcp::tokio::Transport as Transport>::ListenerUpgrade;
    type Dial = <tcp::tokio::Transport as Transport>::Dial;

    fn listen_on(
        &mut self,
        id: ListenerId,
        addr: Multiaddr,
    ) -> Result<(), TransportError<Self::Error>> {
        self.0.listen_on(id, addr)
    }

    fn remove_listener(&mut self, id: ListenerId) -> bool {
        self.0.remove_listener(id)
    }

    fn dial(
        &mut self,
        addr: Multiaddr,
        opts: DialOpts,
    ) -> Result<Self::Dial, TransportError<Self::Error>> {
        self.0.dial(
            addr,
            DialOpts {
                port_use: PortUse::New,
                ..opts
            },
        )
    }

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<TransportEvent<Self::ListenerUpgrade, Self::Error>> {
        Pin::new(&mut self.0).poll(cx)
    }
}

/// Builds a `Swarm` over TCP+noise+yamux, identified by a keypair it
/// generates, with every protocol of [`Behaviour`] enabled. Does not listen,
/// dial or subscribe to any gossip topic; callers do that.
///
/// The keypair is new to this process: a worker's id is its peer id, and it
/// lives for one process incarnation (see `crate::worker`'s "One identity
/// per process"). Only `crate::messenger::Net` builds a swarm, so no caller
/// can hand one a reused identity.
pub(crate) fn build_swarm() -> Swarm<Behaviour> {
    libp2p::SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_other_transport(|key| {
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                NewPortTcp(tcp::tokio::Transport::new(tcp::Config::default()))
                    .upgrade(Version::V1Lazy)
                    .authenticate(noise::Config::new(key)?)
                    .multiplex(yamux::Config::default()),
            )
        })
        .expect("TCP transport with noise/yamux upgrades is always constructible")
        .with_behaviour(|key| Behaviour {
            // `with_push_listen_addr_updates` is off by default, which would
            // leave a peer's advertised listen addresses stale for up to the
            // identify interval (5 minutes) whenever a node starts listening
            // *after* a connection is already up — an ordering nothing in
            // this codebase prevents, since a worker's listener and its
            // outbound seed dial are unordered. `crate::messenger` now keys a
            // peer's dialable address of record off exactly these addresses
            // (see its "Where a peer's address comes from"), and that address
            // is what a `JOIN_RESPONSE` hands a joining node, so a five-minute
            // staleness window is a real correctness cost, not a cosmetic one.
            identify: identify::Behaviour::new(
                identify::Config::new(IDENTIFY_PROTOCOL_VERSION.to_string(), key.public())
                    .with_push_listen_addr_updates(true),
            ),
            gossipsub: gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(key.clone()),
                gossipsub::ConfigBuilder::default()
                    .validation_mode(gossipsub::ValidationMode::Strict)
                    .build()
                    .expect("gossipsub's default config with strict validation is valid"),
            )
            .expect("gossipsub accepts a signing keypair with a valid config"),
            // Mode::Server unconditionally: this node always answers other
            // peers' DHT queries rather than waiting for libp2p's own
            // external-address-confirmation heuristic (kad's default) to
            // decide that for it — every worker in a shard is an equally
            // good routing hop for another. See this module's "kad: peer
            // routing, not membership" doc for why no put/get record ever
            // touches the MemoryStore below.
            kad: {
                let mut kad = kad::Behaviour::new(
                    key.public().to_peer_id(),
                    kad::store::MemoryStore::new(key.public().to_peer_id()),
                );
                kad.set_mode(Some(kad::Mode::Server));
                kad
            },
            request_response: request_response::Behaviour::new(
                [(PROTOCOL, ProtocolSupport::Full)],
                request_response::Config::default(),
            ),
            // ProtocolSupport::Full on every node: any node may need to ask
            // (a fresh Bootstrapping node) or answer (an established Active
            // node) a join request — see net/src/driver.rs's
            // respond_to_join_requests for the answering side.
            join: request_response::Behaviour::new(
                [(JOIN_PROTOCOL, ProtocolSupport::Full)],
                request_response::Config::default(),
            ),
            // ProtocolSupport::Full on every node, same reasoning as `join`
            // above: any node may need to ask (a follower with a task to
            // claim) or answer (whichever node currently holds leadership) a
            // claim request — see net/src/driver.rs's
            // respond_to_claim_requests for the answering side.
            claim: request_response::Behaviour::new(
                [(CLAIM_PROTOCOL, ProtocolSupport::Full)],
                request_response::Config::default(),
            ),
            blocked: allow_block_list::Behaviour::default(),
        })
        .expect("behaviour construction never fails")
        .with_swarm_config(|config| config.with_idle_connection_timeout(IDLE_CONNECTION_TIMEOUT))
        .build()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use kabudachi_core::election::Input;
    use kabudachi_core::protocol::ids::WorkerId;
    use tokio::time::timeout;

    use super::*;
    use crate::messenger::Net;

    /// Takes `net`'s inputs until one reports a connection to `peer`.
    async fn wait_until_connected_to(net: &Net, peer: &WorkerId) {
        let connected = Input::PeerConnected(peer.clone());
        while !net.take_inputs().contains(&connected) {
            net.wait_for_arrival().await;
        }
    }

    /// Two listening nodes that dial each other's listen address at the same
    /// instant, as joiners' lockstep routing crawls do, must still connect.
    /// Each round uses fresh ports, so no round inherits another's
    /// `TIME_WAIT`.
    #[tokio::test]
    async fn two_listening_nodes_dialing_each_other_at_once_connect() {
        const ROUNDS: usize = 20;
        const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
        let loopback: Multiaddr = "/ip4/127.0.0.1/tcp/0".parse().unwrap();

        for round in 0..ROUNDS {
            let net_a = Net::new();
            let net_b = Net::new();
            let addr_a = net_a.listen_on(loopback.clone()).await;
            let addr_b = net_b.listen_on(loopback.clone()).await;

            let (worker_a, worker_b) = (net_a.local_worker_id(), net_b.local_worker_id());

            net_a.dial(addr_b);
            net_b.dial(addr_a);

            timeout(CONNECT_TIMEOUT, async {
                tokio::join!(
                    wait_until_connected_to(&net_a, &worker_b),
                    wait_until_connected_to(&net_b, &worker_a),
                )
            })
            .await
            .unwrap_or_else(|_| panic!("round {round}: both nodes connected within the timeout"));
        }
    }
}
