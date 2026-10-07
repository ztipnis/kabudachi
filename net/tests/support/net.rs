//! Real-transport connection helpers shared by the `net/tests/<area>/` crates.
//!
//! A `Net` queues its node's inputs, connection events among them, until its
//! driver takes them. A helper that waits for a connection event by taking
//! inputs ([`connect_to`]) takes them from any
//! node too, so it waits only on a bare `Net`, one that no node is driven
//! on. A setup that builds its nodes after connecting waits without taking
//! anything ([`wait_until_registered`], [`connect_full_mesh`]), so each node
//! is still told of its connections.

use std::time::{Duration as StdDuration, Instant as StdInstant};

use kabudachi_core::election::{Input, JoinFloor};
use kabudachi_core::protocol::ids::WorkerId;
use kabudachi_core::protocol::messages::JoinResponse;
use kabudachi_net::join::{LeaderSearch, ask_for_leader};
use kabudachi_net::messenger::{Diagnostics, Net};
use libp2p::Multiaddr;
use tokio::time::timeout;

/// How long each helper waits for the connection state it waits for.
const WAIT_TIMEOUT: StdDuration = StdDuration::from_secs(20);
/// How long a pass of `ask_for_leader` keeps listening once a pointer arrived.
const GRACE: StdDuration = StdDuration::from_secs(5);

/// Takes `net`'s queued inputs until every one of `expected` has been taken,
/// in any order, and returns when the last was. Everything taken is
/// dropped: `net` must be a bare `Net`.
pub async fn take_inputs_until(net: &Net, expected: &[Input]) -> StdInstant {
    let mut missing: Vec<&Input> = expected.iter().collect();
    timeout(WAIT_TIMEOUT, async {
        loop {
            for input in net.take_inputs() {
                missing.retain(|wanted| **wanted != input);
            }
            if missing.is_empty() {
                return StdInstant::now();
            }
            net.wait_for_arrival().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{expected:?} arrived within the timeout"))
}

/// Dials `net_a`'s already-resolved `listen_addr` from the bare `net_b` and
/// waits until `net_b` reports the connection. `net_a`'s own
/// `PeerConnected` stays queued for its node. `net_a` reports the connection
/// no later than it receives anything `net_b` sends over it, so once a
/// request from `net_b` has been answered, `net_a` holds the connection too
/// (a `Net::disconnect` is a silent no-op on a side that does not).
pub async fn connect_to(net_a: &Net, listen_addr: &Multiaddr, net_b: &Net) {
    net_b.dial(listen_addr.clone());
    take_inputs_until(net_b, &[Input::PeerConnected(net_a.local_worker_id())]).await;
}

/// Reads `net`'s diagnostics until `condition` holds of them, and returns
/// that reading; panics naming `what` if it does not within the timeout.
pub async fn wait_for_diagnostics(
    net: &Net,
    what: &str,
    condition: impl Fn(&Diagnostics) -> bool,
) -> Diagnostics {
    timeout(WAIT_TIMEOUT, async {
        loop {
            let diagnostics = net.diagnostics().await;
            if condition(&diagnostics) {
                return diagnostics;
            }
            tokio::time::sleep(StdDuration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what} within the timeout"))
}

/// Waits until `net` has registered a connection to `peer`, taking nothing
/// from it, so a node built on `net` afterwards is still fed the
/// connection's `PeerConnected`. When its first connection to a peer opens,
/// `net` queues that input and then records the peer's address, which it
/// keeps (`Diagnostics::peer_addresses`); for a peer `net` never connected
/// to before, and whose address no roll call or reply has carried to it
/// yet, a recorded address means the input is queued.
pub async fn wait_until_registered(net: &Net, peer: &WorkerId) {
    wait_for_diagnostics(net, "the net registered its connection", |diagnostics| {
        diagnostics.peer_addresses.contains_key(peer)
    })
    .await;
}

/// Asks `seeds` who leads the shard, again and again, until one points at a
/// leader `net` reaches, and returns that pointer. Each ask of one seed is
/// bounded by `per_seed_timeout`, the whole wait by [`WAIT_TIMEOUT`].
pub async fn ask_until_pointed_at_a_leader(
    net: &Net,
    seeds: &[Multiaddr],
    per_seed_timeout: StdDuration,
) -> JoinResponse {
    timeout(WAIT_TIMEOUT, async {
        loop {
            if let LeaderSearch::Found(pointer) =
                ask_for_leader(net, seeds, JoinFloor::none(), per_seed_timeout, GRACE).await
            {
                return pointer;
            }
            tokio::time::sleep(StdDuration::from_millis(50)).await;
        }
    })
    .await
    .expect("a seed pointed at a reachable leader within the timeout")
}

/// Waits until `net` has seen every one of `peers` subscribe to its shard.
pub async fn wait_until_subscribed(net: &Net, peers: &[&WorkerId]) {
    wait_for_diagnostics(net, "every peer's subscription arrived", |diagnostics| {
        peers
            .iter()
            .all(|peer| diagnostics.shard_subscribers.contains(*peer))
    })
    .await;
}

/// Connects every one of `nets` to every other one over real loopback TCP,
/// waits until each has registered each connection (see
/// [`wait_until_registered`]), and returns each one's `WorkerId` in the
/// same order as `nets`. None of `nets` may have connected before.
pub async fn connect_full_mesh(nets: &[&Net]) -> Vec<WorkerId> {
    let mut addrs = Vec::with_capacity(nets.len());
    for net in nets {
        addrs.push(
            timeout(
                WAIT_TIMEOUT,
                net.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()),
            )
            .await
            .expect("every net produced a listen address within the timeout"),
        );
    }

    for (i, addr) in addrs.iter().enumerate() {
        for net in &nets[(i + 1)..] {
            net.dial(addr.clone());
        }
    }

    let ids: Vec<WorkerId> = nets.iter().map(|net| net.local_worker_id()).collect();
    for (net, id) in nets.iter().zip(&ids) {
        for other in ids.iter().filter(|other| *other != id) {
            wait_until_registered(net, other).await;
        }
    }

    ids
}
