//! Two nodes that dial each other at the same instant connect.

use std::time::Duration;

use kabudachi_core::election::Input;
use kabudachi_core::protocol::ids::WorkerId;
use kabudachi_net::messenger::Net;
use libp2p::Multiaddr;
use tokio::time::timeout;

use crate::support::deadline::within_deadline;

/// Takes `net`'s inputs until one reports a connection to `peer`.
async fn wait_until_connected_to(net: &Net, peer: &WorkerId) {
    let connected = Input::PeerConnected(peer.clone());
    while !net.take_inputs().contains(&connected) {
        net.wait_for_arrival().await;
    }
}

/// Two listening nodes that dial each other's listen address at the same
/// instant, as joiners' lockstep routing crawls do, must still connect. Each
/// round uses fresh ports, so no round inherits another's `TIME_WAIT`.
#[tokio::test]
async fn two_listening_nodes_dialing_each_other_at_once_connect() {
    within_deadline(async {
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
    })
    .await
}
