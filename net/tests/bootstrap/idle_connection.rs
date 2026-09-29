//! A connection with no shared gossip topic must still survive past
//! libp2p-swarm's own idle timeout (10s by default). A peer within this
//! node's gossip mesh doesn't depend on `swarm::IDLE_CONNECTION_TIMEOUT` at
//! all — gossipsub's own connection handler keeps that connection alive on
//! its own (see that constant's doc comment) — but neither `net_a` nor
//! `net_b` here ever subscribes to a shard, so that keep-alive never
//! engages, and this timeout is the only thing keeping their connection up.
//! That is the case of a JOIN seed dial or a claim's connection, which
//! must not close between the steps that use it. The timeout is finite
//! (60 s) so that such a connection, once nothing needs it, does close; it
//! is not redialed then, since only gossip-mesh peers are (see
//! `crate::messenger`'s "Which drops are redial-eligible").


use std::time::{Duration as StdDuration, Instant as StdInstant};

use kabudachi_net::messenger::Net;
use kabudachi_net::swarm::build_swarm;
use libp2p::identity;
use tokio::time::timeout;

use kabudachi_core::election::Input;
use crate::support::net::{connect_to, take_inputs_until, wait_for_diagnostics};

/// Longer than libp2p-swarm's 10s default idle timeout, with margin for
/// streams still closing after Identify completes.
const IDLE_PERIOD: StdDuration = StdDuration::from_secs(15);

const WAIT_TIMEOUT: StdDuration = StdDuration::from_secs(20);

#[tokio::test]
async fn an_idle_connection_stays_reachable_past_libp2ps_default_idle_timeout() {
    let net_a = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
    let net_b = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
    let listen_addr = timeout(
        WAIT_TIMEOUT,
        net_a.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
    )
    .await
    .expect("net_a produced a listen address within the timeout");
    // net_b listens too, only so that Identify is observable below.
    let listen_addr_b = timeout(
        WAIT_TIMEOUT,
        net_b.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
    )
    .await
    .expect("net_b produced a listen address within the timeout");
    connect_to(&net_a, &listen_addr, &net_b).await;
    let worker_a = net_a.local_worker_id();
    let worker_b = net_b.local_worker_id();
    take_inputs_until(&net_a, &Input::PeerConnected(worker_b.clone())).await;

    // libp2p's idle timer starts only once no stream is active, so the idle
    // period must not start before the initial Identify exchange finishes.
    // net_a first records net_b's ephemeral source port for this inbound
    // connection, and replaces it with net_b's listen address only when
    // Identify arrives (see `crate::messenger`'s "Where a peer's address
    // comes from").
    wait_for_diagnostics(&net_a, "net_a received net_b's Identify", |diagnostics| {
        diagnostics.peer_addresses.get(&worker_b) == Some(&listen_addr_b)
    })
    .await;

    // Check throughout, not just at the end, so a drop is reported when it
    // happens. An auto-redial cannot hide one: each side reports every
    // drop.
    let started = StdInstant::now();
    while started.elapsed() < IDLE_PERIOD {
        let dropped = net_a
            .take_inputs()
            .into_iter()
            .chain(net_b.take_inputs())
            .find(|input| matches!(input, Input::PeerDisconnected(_)));
        assert_eq!(
            dropped,
            None,
            "an idle connection dropped after {:?} (net_a is {worker_a:?}, net_b is {worker_b:?})",
            started.elapsed()
        );
        tokio::time::sleep(StdDuration::from_millis(50)).await;
    }
}
