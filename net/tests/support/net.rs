//! Real-transport connection helpers shared by the `net/tests/*.rs` files.

use std::time::{Duration as StdDuration, Instant as StdInstant};

use kabudachi_core::protocol::ids::WorkerId;
use kabudachi_core::transport::PeerMessenger;
use kabudachi_net::messenger::Net;
use libp2p::Multiaddr;
use tokio::time::timeout;

/// How long each helper waits for the connection state it polls for.
const WAIT_TIMEOUT: StdDuration = StdDuration::from_secs(20);

/// Dials `net_a`'s already-resolved `listen_addr` from `net_b` and waits
/// until each side sees the other reachable. Both sides matter: a later
/// `Net::disconnect` is a silent no-op on a side that has not registered the
/// connection yet.
pub async fn connect_to(net_a: &Net, listen_addr: &Multiaddr, net_b: &Net) {
    net_b.dial(listen_addr.clone());
    let worker_a = net_a.local_worker_id();
    let worker_b = net_b.local_worker_id();
    timeout(WAIT_TIMEOUT, async {
        loop {
            if net_b.reachable_peers(worker_b.clone()).contains(&worker_a)
                && net_a.reachable_peers(worker_a.clone()).contains(&worker_b)
            {
                return;
            }
            tokio::time::sleep(StdDuration::from_millis(5)).await;
        }
    })
    .await
    .expect("net_a and net_b saw each other as reachable within the timeout");
}

/// Polls `leader_net`'s own view of `worker` until it is no longer
/// reachable, returning the wall-clock instant that first observed it gone:
/// the real-transport "worker unreachable" moment.
pub async fn wait_until_unreachable(
    leader_net: &Net,
    leader_id: &WorkerId,
    worker: &WorkerId,
) -> StdInstant {
    timeout(WAIT_TIMEOUT, async {
        loop {
            if !leader_net.reachable_peers(leader_id.clone()).contains(worker) {
                return StdInstant::now();
            }
            tokio::time::sleep(StdDuration::from_millis(5)).await;
        }
    })
    .await
    .expect("the leader's view of the disconnected worker went unreachable within the timeout")
}
