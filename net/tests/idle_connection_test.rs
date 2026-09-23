//! A connection that carries no traffic must stay up. Followers exchange no
//! messages with each other while the leader is stable, yet ring roll-call
//! forwarding after leader loss needs them still connected
//! (`PeerMessenger::reachable_peers`). libp2p-swarm closes a connection
//! with no active streams after its idle timeout (10s by default), so
//! `swarm::build_swarm` must set a longer one.

mod support;

use std::time::{Duration as StdDuration, Instant as StdInstant};

use kabudachi_core::transport::PeerMessenger;
use kabudachi_net::messenger::Net;
use kabudachi_net::swarm::build_swarm;
use libp2p::identity;
use tokio::time::timeout;

use support::net::connect_to;

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

    // libp2p's idle timer starts only once no stream is active, so the idle
    // period must not start before the initial Identify exchange finishes.
    // net_a first records net_b's ephemeral source port for this inbound
    // connection, and replaces it with net_b's listen address only when
    // Identify arrives (see `crate::messenger`'s "Where a peer's address
    // comes from").
    timeout(WAIT_TIMEOUT, async {
        while net_a.peer_addresses().get(&worker_b) != Some(&listen_addr_b) {
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }
    })
    .await
    .expect("net_a received net_b's Identify within the timeout");

    // Sample throughout, not just at the end: an auto-redial could hide a
    // drop that happened in between.
    let started = StdInstant::now();
    while started.elapsed() < IDLE_PERIOD {
        assert!(
            net_a.reachable_peers(worker_a.clone()).contains(&worker_b)
                && net_b.reachable_peers(worker_b.clone()).contains(&worker_a),
            "an idle connection dropped after {:?}",
            started.elapsed()
        );
        tokio::time::sleep(StdDuration::from_millis(50)).await;
    }
}
