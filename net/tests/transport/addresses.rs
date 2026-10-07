//! The address a `Net` gives a peer to be dialed on: only one the peer
//! listens on, never the source address of its connection to this node.

use std::time::Duration;

use kabudachi_core::election::Input;
use kabudachi_net::messenger::Net;
use tokio::time::timeout;

use crate::support::net::{connect_to, take_inputs_until, wait_for_diagnostics};

const TEST_TIMEOUT: Duration = Duration::from_secs(20);

#[tokio::test]
async fn a_peer_known_only_by_its_inbound_source_address_has_no_dialable_address_until_it_advertises_one() {
    let net_a = Net::new();
    let net_b = Net::new();
    // net_b dials net_a without listening itself, so all net_a knows of
    // net_b is the ephemeral source address of net_b's connection.
    let address_a = net_a.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()).await;
    let (worker_a, worker_b) = (net_a.local_worker_id(), net_b.local_worker_id());
    connect_to(&net_a, &address_a, &net_b).await;
    take_inputs_until(&net_a, &[Input::PeerConnected(worker_b.clone())]).await;
    wait_for_diagnostics(&net_a, "net_a recorded net_b's source address", |d| {
        d.peer_addresses.contains_key(&worker_b)
    })
    .await;

    assert_eq!(net_a.dialable_address(&worker_b).await, None);
    assert_eq!(net_b.dialable_address(&worker_a).await, net_a.local_multiaddr());

    let address_b = timeout(
        TEST_TIMEOUT,
        net_b.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
    )
    .await
    .expect("net_b produced a listen address within the timeout");
    timeout(TEST_TIMEOUT, async {
        while net_a.dialable_address(&worker_b).await != Some(address_b.clone()) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("net_a learned net_b's advertised listen address within the timeout");
}
