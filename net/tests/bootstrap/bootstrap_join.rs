//! Workers bootstrapping into one shard over real sockets, end to end,
//! through their entry point (`kabudachi_net::worker::Worker`): each starts
//! with a fresh identity, bootstraps, and is then driven, over its own `Net`
//! on loopback TCP, and every worker that has an authority shares one
//! `InMemoryAuthority`.


use std::time::Duration as StdDuration;

use kabudachi_core::coordination_authority::CoordinationAuthority;
use kabudachi_core::election::{ElectionTimings, JoinFloor};
use kabudachi_core::in_memory_authority::InMemoryAuthority;
use kabudachi_core::protocol::ids::ShardId;
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::{Duration, RealClock};
use kabudachi_net::join::{LeaderSearch, ask_for_leader};
use kabudachi_net::messenger::Net;
use libp2p::Multiaddr;
use libp2p::multiaddr::Protocol;
use tokio::time::timeout;

use crate::support::deadline::within_deadline;
use crate::support::worker::{
    PER_PEER_TIMEOUT, RunningWorker, TEST_TIMEOUT, poll_until, warmed_up_in_memory_authority,
    with_in_memory_authority, worker_config,
};

const SHARD: &str = "shard-1";

/// How long a pass of `ask_for_leader` keeps listening once a pointer arrived.
const GRACE: StdDuration = StdDuration::from_secs(5);

/// The authority's registration TTL, which is also how long it warms up.
/// A worker renews every third of it, one call at a time, and a loaded host
/// can leave a renewal's reply waiting most of a second on the blocking pool
/// and the driver's next wake; at a TTL of one second such a worker lapses
/// and fences itself, a leader included, with its authority still reachable.
const AUTHORITY_TTL_MS: u64 = 3_000;

/// Long enough that a follower whose acks a loaded host holds back keeps its
/// leader, and its leader the lease of a shard of several voters.
const SUSPECT_TIMEOUT_MS: u64 = 2_000;

/// How often a worker heartbeats its leader. A leader admits a joiner only
/// once it echoes an ack sent within the last two intervals, so the interval
/// bounds the round trip admission tolerates: at 50 ms, the acks and echoes
/// of five workers on a host running several test binaries at once fall
/// behind by more than that, and a joiner is admitted only in a lull.
const HEARTBEAT_INTERVAL_MS: u64 = 100;

/// How long a roll call runs: well above a loopback round trip.
const ROLL_CALL_DEADLINE_MS: u64 = 100;

fn shard() -> ShardId {
    ShardId::new(SHARD)
}

fn timings() -> ElectionTimings {
    ElectionTimings::new(
        Duration::from_millis(SUSPECT_TIMEOUT_MS),
        Duration::from_millis(HEARTBEAT_INTERVAL_MS),
    )
    .with_roll_call_deadline(Duration::from_millis(ROLL_CALL_DEADLINE_MS))
}

/// Starts a worker listening on `bind` that bootstraps through `seeds`, with
/// `authority` if any, and runs it on its own task (`support::worker`,
/// shared with the other worker-entry-point test files).
async fn spawn_worker(
    bind: &str,
    authority: Option<InMemoryAuthority<RealClock>>,
    seeds: Vec<Multiaddr>,
) -> RunningWorker {
    let config = worker_config(shard(), bind, timings(), seeds);
    let config = match authority {
        Some(authority) => {
            with_in_memory_authority(config, authority, Duration::from_millis(AUTHORITY_TTL_MS))
        }
        None => config,
    };
    crate::support::worker::spawn_worker(config).await
}

/// An authority that has finished warming up.
async fn warmed_up_authority() -> InMemoryAuthority<RealClock> {
    warmed_up_in_memory_authority(&shard(), Duration::from_millis(AUTHORITY_TTL_MS)).await
}

/// A shard led by a founder, a second worker that joined it, and three more
/// workers given only that second worker's address as their one seed, all
/// over one authority. Returns them once each of the three follows a
/// leader: the founder first, the seed second.
async fn three_workers_joined_through_one_seed(
    authority: &InMemoryAuthority<RealClock>,
) -> Vec<RunningWorker> {
    let mut founder = spawn_worker("/ip4/127.0.0.1/tcp/0", Some(authority.clone()), vec![]).await;
    founder
        .wait_until(|seen| seen.state == WorkerState::Leader)
        .await;

    let mut seed = spawn_worker(
        "/ip4/127.0.0.1/tcp/0",
        Some(authority.clone()),
        vec![founder.address.clone()],
    )
    .await;
    seed.wait_to_follow_a_leader().await;

    let mut workers = vec![founder, seed];
    for _ in 0..3 {
        let seeds = vec![workers[1].address.clone()];
        workers.push(spawn_worker("/ip4/127.0.0.1/tcp/0", Some(authority.clone()), seeds).await);
    }
    for joiner in &mut workers[2..] {
        joiner.wait_to_follow_a_leader().await;
    }
    workers
}

// Each of three workers learns the leader from a seed that is not the
// leader, registers with the authority at the address it listens on once it
// is driven, and, through kad's bootstrap after its join connections, comes
// to know the other two joiners, whose addresses it was never given. The
// leader then admits every worker that joined it in an admission batch:
// each becomes a voter. A drained voter then stops.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn three_workers_that_join_through_one_seed_register_and_are_admitted() {
    within_deadline(async {
        let authority = warmed_up_authority().await;
        let mut workers = three_workers_joined_through_one_seed(&authority).await;
        let joiners = &workers[2..];

        for joiner in joiners {
            poll_until("the joiner registered at its listen address", || {
                authority
                    .live_registrations(&shard())
                    .expect("the in-memory authority is always reachable")
                    .addresses()
                    .get(&joiner.id)
                    == Some(&joiner.address.to_string())
            })
            .await;
        }
        for joiner in joiners {
            for other in joiners.iter().filter(|other| other.id != joiner.id) {
                timeout(TEST_TIMEOUT, async {
                    while !joiner
                        .net
                        .diagnostics()
                        .await
                        .peer_addresses
                        .contains_key(&other.id)
                    {
                        tokio::time::sleep(StdDuration::from_millis(10)).await;
                    }
                })
                .await
                .expect("kad connected the joiner to another joiner within the timeout");
            }
        }
        for worker in &mut workers[1..] {
            worker.wait_until(|seen| seen.voter).await;
        }

        // A drained voter leaves through its own driver and node: the request
        // reaches the node, which removes itself and stops.
        let leaving = workers.last_mut().expect("the shard has workers");
        leaving.net.request_drain();
        leaving
            .wait_until(|seen| seen.state == WorkerState::Stopped)
            .await;
    })
    .await
}

/// Whether this host has an address other than loopback: the local address
/// the OS would route a packet to a public address from. Nothing is sent.
fn has_a_non_loopback_address() -> bool {
    std::net::UdpSocket::bind("0.0.0.0:0")
        .and_then(|socket| {
            socket.connect("192.0.2.1:9")?;
            socket.local_addr()
        })
        .is_ok_and(|address| !address.ip().is_loopback() && !address.ip().is_unspecified())
}

fn is_unspecified(address: &Multiaddr) -> bool {
    address.iter().any(|protocol| match protocol {
        Protocol::Ip4(ip) => ip.is_unspecified(),
        Protocol::Ip6(ip) => ip.is_unspecified(),
        _ => false,
    })
}

fn is_loopback(address: &Multiaddr) -> bool {
    address.iter().any(|protocol| match protocol {
        Protocol::Ip4(ip) => ip.is_loopback(),
        Protocol::Ip6(ip) => ip.is_loopback(),
        _ => false,
    })
}

// A leader bound to every interface must not point joiners at the address it
// is bound to, which no one can dial, nor at its loopback address: a joiner on
// another host would dial itself. It names an address another host can reach,
// and a joiner dialing that address reaches it. On a host with no
// non-loopback interface (a sandbox with networking off), where loopback is
// all a leader can offer, only the first is checked.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_leader_bound_to_every_interface_points_joiners_at_an_address_they_can_dial() {
    within_deadline(async {
        let mut leader = spawn_worker("/ip4/0.0.0.0/tcp/0", None, vec![]).await;
        leader
            .wait_until(|seen| seen.state == WorkerState::Leader)
            .await;

        let joining_net = Net::new();
        let search = timeout(
            TEST_TIMEOUT,
            ask_for_leader(
                &joining_net,
                std::slice::from_ref(&leader.address),
                JoinFloor::none(),
                PER_PEER_TIMEOUT,
                GRACE,
            ),
        )
        .await
        .expect("the leader answered within the timeout");
        let LeaderSearch::Found(pointer) = search else {
            panic!("the leader pointed at itself: {search:?}");
        };
        assert_eq!(pointer.leader_id(), Some(leader.id.clone()));
        let pointed: Multiaddr = pointer
            .leader_multiaddr
            .parse()
            .expect("the pointer names a multiaddr");
        assert!(!is_unspecified(&pointed), "the leader pointed at {pointed}");
        if has_a_non_loopback_address() {
            assert!(!is_loopback(&pointed), "the leader pointed at {pointed}");
        } else {
            eprintln!("no non-loopback interface on this host: only the bound address was checked");
        }

        // Another worker, asking at the address the pointer names,
        // reaches the leader there.
        let remote_net = Net::new();
        let search = timeout(
            TEST_TIMEOUT,
            ask_for_leader(&remote_net, &[pointed], JoinFloor::none(), PER_PEER_TIMEOUT, GRACE),
        )
        .await
        .expect("the leader answered within the timeout");
        assert_eq!(search, LeaderSearch::Found(pointer));
    })
    .await
}
