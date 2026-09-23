//! Swarm construction: proves a minimal libp2p node builds and links under
//! both `cargo` and Bazel, and that two such nodes complete an `identify`
//! handshake over TCP+noise+yamux.
//!
//! `Behaviour` holds `identify` plus three `request_response` behaviours: one
//! carrying the election protocol (see `crate::codec`), one the bootstrap
//! join protocol (see `crate::join_codec`), and one the claim arbitration
//! protocol (see `crate::claim_codec`) — each a deliberately separate wire
//! protocol, not a variant folded into `ElectionMessage` (see
//! `crate::join_codec`'s module doc). `crate::messenger` implements
//! `core::transport::PeerMessenger` on top of this swarm, and consumes
//! `identify::Event::Received` there to learn each peer's real, dialable
//! listen address (see that module's "Where a peer's address comes from").
//! The struct is
//! deliberately left easy to extend with more fields rather than, say,
//! wrapping a single behaviour type.

use libp2p::request_response::{self, ProtocolSupport};
use libp2p::swarm::NetworkBehaviour;
use libp2p::{Swarm, identify, identity, noise, tcp, yamux};

use crate::claim_codec::{ClaimCodec, PROTOCOL as CLAIM_PROTOCOL};
use crate::codec::{ElectionCodec, PROTOCOL};
use crate::join_codec::{JoinCodec, PROTOCOL as JOIN_PROTOCOL};

/// Protocol version string advertised by `identify`. Not yet load-bearing
/// (nothing checks it), but real peers should agree on it eventually.
const IDENTIFY_PROTOCOL_VERSION: &str = "/kabudachi/1.0.0";

/// How long a connection with no active streams stays open. libp2p-swarm's
/// default is 10 seconds, but followers exchange nothing with each other
/// while the leader is stable, and ring roll-call forwarding after leader
/// loss needs those connections still up (`PeerMessenger::reachable_peers`).
/// Finite, because libp2p adds it to an `Instant` and a huge value would
/// overflow; a connection closed at expiry is an ordinary drop that
/// `crate::messenger`'s redial policy reconnects.
const IDLE_CONNECTION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(24 * 60 * 60);

#[derive(NetworkBehaviour)]
pub struct Behaviour {
    pub identify: identify::Behaviour,
    pub request_response: request_response::Behaviour<ElectionCodec>,
    pub join: request_response::Behaviour<JoinCodec>,
    pub claim: request_response::Behaviour<ClaimCodec>,
}

/// Builds a `Swarm` over TCP+noise+yamux, identified by `keypair`, with the
/// `identify` and election `request_response` protocols enabled. Does not
/// listen or dial; callers do that.
pub fn build_swarm(keypair: identity::Keypair) -> Swarm<Behaviour> {
    libp2p::SwarmBuilder::with_existing_identity(keypair)
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )
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
        })
        .expect("behaviour construction never fails")
        .with_swarm_config(|config| config.with_idle_connection_timeout(IDLE_CONNECTION_TIMEOUT))
        .build()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use libp2p::futures::StreamExt;
    use libp2p::swarm::SwarmEvent;
    use libp2p::{Multiaddr, identity};
    use tokio::time::timeout;

    use super::*;

    const TEST_TIMEOUT: Duration = Duration::from_secs(10);

    #[tokio::test]
    async fn build_swarm_uses_the_given_keypairs_peer_id() {
        let keypair = identity::Keypair::generate_ed25519();
        let expected_peer_id = keypair.public().to_peer_id();

        let swarm = build_swarm(keypair);

        assert_eq!(*swarm.local_peer_id(), expected_peer_id);
    }

    async fn wait_for_new_listen_addr(swarm: &mut Swarm<Behaviour>) -> Multiaddr {
        loop {
            if let SwarmEvent::NewListenAddr { address, .. } = swarm.select_next_some().await {
                return address;
            }
        }
    }

    async fn wait_for_identify_received(swarm: &mut Swarm<Behaviour>) {
        loop {
            if let SwarmEvent::Behaviour(BehaviourEvent::Identify(identify::Event::Received {
                ..
            })) = swarm.select_next_some().await
            {
                return;
            }
        }
    }

    #[tokio::test]
    async fn two_nodes_complete_an_identify_handshake() {
        let mut listener = build_swarm(identity::Keypair::generate_ed25519());
        let mut dialer = build_swarm(identity::Keypair::generate_ed25519());

        listener
            .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
            .expect("listening on an ephemeral loopback port never fails");
        let listen_addr = timeout(TEST_TIMEOUT, wait_for_new_listen_addr(&mut listener))
            .await
            .expect("listener produced a listen address within the timeout");

        dialer
            .dial(listen_addr)
            .expect("dialing the listener's own address never fails");

        timeout(TEST_TIMEOUT, async {
            tokio::join!(
                wait_for_identify_received(&mut listener),
                wait_for_identify_received(&mut dialer),
            )
        })
        .await
        .expect("both sides received an identify::Event::Received within the timeout");
    }
}
