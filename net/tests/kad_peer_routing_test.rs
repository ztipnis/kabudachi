//! A peer this node never itself dialed, and was never told the address of,
//! becomes reachable once `kad`'s automatic bootstrap crawl learns a route
//! to it through a peer this node did dial — see `kabudachi_net::swarm`'s
//! "kad: peer routing, not membership" module doc. Three real swarms,
//! `net_c` dialing only `net_a`: `net_c` ends up connected to `net_b`,
//! purely from `kad`, and a gossipsub message on the shard topic still
//! reaches all three afterwards — proving the routing table changes nothing
//! about the election protocol itself, only what the swarm can dial
//! (`core::election` never sees a `kad` event; the only observable effect
//! here is an ordinary `Input::PeerConnected`, same as any other
//! connection).

mod support;

use std::time::Duration as StdDuration;

use kabudachi_core::election::Input;
use kabudachi_core::protocol::ids::{IncarnationId, ShardId, WorkerId};
use kabudachi_core::protocol::messages::{ElectionMessage, SelfRemove, election_message};
use kabudachi_net::messenger::Net;
use kabudachi_net::swarm::build_swarm;
use libp2p::identity;
use tokio::time::timeout;

use support::net::{connect_to, take_inputs_until, wait_until_subscribed};

/// Generous bound for a real DHT crawl on a machine shared with other
/// parallel builds: well above `kad`'s own 500ms automatic-bootstrap
/// throttle (`libp2p_kad::bootstrap::DEFAULT_AUTOMATIC_THROTTLE`) plus the
/// real dial and `FIND_NODE` round trip it then takes.
const WAIT_TIMEOUT: StdDuration = StdDuration::from_secs(20);

/// How long the test waits, after gossip's expected copies arrive, for an
/// unexpected one — same reasoning and value as `gossip_publish_test.rs`.
const QUIET_PERIOD: StdDuration = StdDuration::from_secs(3);

fn new_net() -> Net {
    Net::new(build_swarm(identity::Keypair::generate_ed25519()))
}

/// A well-formed message naming `worker` in `shard`; the specific payload is
/// arbitrary, matching `gossip_publish_test.rs`'s own choice of `SelfRemove`
/// as a message this crate never inspects the meaning of.
fn self_remove(worker: &WorkerId, shard: &ShardId) -> ElectionMessage {
    ElectionMessage {
        payload: Some(election_message::Payload::SelfRemove(SelfRemove {
            worker_id: Some(worker.clone().into()),
            incarnation_id: Some(IncarnationId::new("incarnation-1").into()),
            shard_id: Some(shard.clone().into()),
            configuration_generation: None,
            term_seen: 0,
            leader_term: 0,
        })),
    }
}

/// Every message among `inputs`, with its sender, in order.
fn messages(inputs: Vec<Input>) -> Vec<(WorkerId, ElectionMessage)> {
    inputs
        .into_iter()
        .filter_map(|input| match input {
            Input::Message { from, message } => Some((from, message)),
            _ => None,
        })
        .collect()
}

/// Takes `net`'s queued inputs until at least one message has arrived, and
/// returns the messages among them. Every other input is dropped: `net` must
/// be a bare `Net`.
async fn take_messages(net: &Net) -> Vec<(WorkerId, ElectionMessage)> {
    timeout(WAIT_TIMEOUT, async {
        loop {
            let arrived = messages(net.take_inputs());
            if !arrived.is_empty() {
                return arrived;
            }
            net.wait_for_arrival().await;
        }
    })
    .await
    .expect("a message arrived within the timeout")
}

#[tokio::test]
async fn a_third_swarm_reaches_a_peer_only_kad_told_it_about_and_gossip_still_reaches_all_three() {
    let net_a = new_net();
    let net_b = new_net();
    let net_c = new_net();
    let (worker_a, worker_b, worker_c) = (
        net_a.local_worker_id(),
        net_b.local_worker_id(),
        net_c.local_worker_id(),
    );

    let addr_a = timeout(
        WAIT_TIMEOUT,
        net_a.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
    )
    .await
    .expect("net_a produced a listen address within the timeout");
    let addr_b = timeout(
        WAIT_TIMEOUT,
        net_b.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
    )
    .await
    .expect("net_b produced a listen address within the timeout");

    // net_b dials net_a directly: the two peers kad is meant to bridge net_c
    // to. net_c is never told net_b's address, and never dials it itself.
    connect_to(&net_a, &addr_a, &net_b).await;

    // net_a's kad only learns net_b's address once their Identify exchange
    // completes (see kabudachi_net::messenger's handle_event, its
    // identify::Event::Received arm), which must happen before net_c's own
    // bootstrap crawl reaches net_a asking for it — otherwise the test would
    // depend on a race rather than kad's documented "bootstrap does not
    // require a manual call" behaviour. net_a.peer_addresses() only ever
    // equals net_b's real listen address once Identify supplies it (see
    // kabudachi_net::messenger's module doc, "Where a peer's address comes
    // from": net_a is the connection's Listener side here, so the
    // endpoint-derived address it would otherwise have on file is net_b's
    // ephemeral source port, not this).
    timeout(WAIT_TIMEOUT, async {
        while net_a.peer_addresses().get(&worker_b) != Some(&addr_b) {
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }
    })
    .await
    .expect("net_a received net_b's Identify within the timeout");

    // net_c dials only net_a. Its own Identify exchange with net_a feeds
    // net_c's kad the same way (see kabudachi_net::swarm's "kad: peer
    // routing, not membership"), which is the only peer it now knows of —
    // triggering kad's own automatic, throttled bootstrap (no explicit call
    // needed, per `libp2p_kad::Behaviour::bootstrap`'s doc). That self-lookup asks
    // net_a for its closest known peers, learns of net_b from net_a's own
    // kad table, and dials net_b directly to continue the lookup — the real
    // connection this test is actually after.
    connect_to(&net_a, &addr_a, &net_c).await;

    take_inputs_until(&net_c, &Input::PeerConnected(worker_b.clone())).await;

    // The routing table did its one job — nothing about the election
    // protocol changed. A shard-topic gossip message still reaches every
    // member, including net_c, whose only two connections both formed
    // without ever being told an address by the test itself for net_b.
    let shard = ShardId::new("shard-1");
    for net in [&net_a, &net_b, &net_c] {
        net.subscribe_to_shard(&shard);
    }
    wait_until_subscribed(&net_a, &[&worker_b, &worker_c]).await;
    wait_until_subscribed(&net_b, &[&worker_a, &worker_c]).await;
    wait_until_subscribed(&net_c, &[&worker_a, &worker_b]).await;

    let message = self_remove(&worker_a, &shard);
    net_a.publish(message.clone());

    let expected = vec![(worker_a.clone(), message)];
    assert_eq!(take_messages(&net_b).await, expected);
    assert_eq!(take_messages(&net_c).await, expected);
    tokio::time::sleep(QUIET_PERIOD).await;
    for (net, id) in [(&net_a, &worker_a), (&net_b, &worker_b), (&net_c, &worker_c)] {
        assert_eq!(
            messages(net.take_inputs()),
            vec![],
            "{id:?} received a message beyond one copy for each other subscriber"
        );
    }
}
