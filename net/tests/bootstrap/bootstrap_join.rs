//! Workers bootstrapping into one shard over real sockets, end to end,
//! through their entry point (`kabudachi_net::worker::Worker`): each starts
//! with a fresh identity, bootstraps, and is then driven, over its own `Net`
//! on loopback TCP, and every worker that has an authority shares one
//! `InMemoryAuthority`.


use std::sync::Arc;
use std::time::Duration as StdDuration;

use kabudachi_core::coordination_authority::CoordinationAuthority;
use kabudachi_core::election::{AuthorityTimings, ElectionTimings};
use kabudachi_core::in_memory_authority::InMemoryAuthority;
use kabudachi_core::protocol::ids::ShardId;
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::{Duration, RealClock};
use kabudachi_net::join::{LeaderSearch, ask_for_leader};
use kabudachi_net::messenger::Net;
use kabudachi_net::worker::{AuthorityConfig, WorkerConfig};
use libp2p::Multiaddr;
use libp2p::multiaddr::Protocol;
use tokio::time::timeout;

use crate::support::worker::{RunningWorker, poll_until, warmed_up_in_memory_authority};

const SHARD: &str = "shard-1";

/// Generous whole-test backstop for everything that is expected to happen.
const TEST_TIMEOUT: StdDuration = StdDuration::from_secs(30);

/// Short, so a waiting worker goes round the cascade many times per test.
const RETRY_INTERVAL: StdDuration = StdDuration::from_millis(50);

/// How long each ask of one seed or registered peer may take.
const PER_PEER_TIMEOUT: StdDuration = StdDuration::from_secs(1);

/// The authority's registration TTL, which is also how long it warms up.
/// Long enough that a driven worker on a loaded host always renews in time
/// (every third of it).
const AUTHORITY_TTL_MS: u64 = 1_000;

fn shard() -> ShardId {
    ShardId::new(SHARD)
}

fn timings() -> ElectionTimings {
    ElectionTimings::new(Duration::from_millis(300), Duration::from_millis(50))
        .with_roll_call_deadline(Duration::from_millis(100))
}

/// Starts a worker listening on `bind` that bootstraps through `seeds`, with
/// `authority` if any, and runs it on its own task (`support::worker`,
/// shared with the other worker-entry-point test files).
async fn spawn_worker(
    bind: &str,
    authority: Option<InMemoryAuthority<RealClock>>,
    seeds: Vec<Multiaddr>,
) -> RunningWorker {
    let mut config = WorkerConfig::new(shard(), bind.parse().unwrap(), timings())
        .with_seeds(seeds)
        .with_join_peer_timeout(PER_PEER_TIMEOUT)
        .with_retry_interval(RETRY_INTERVAL);
    if let Some(authority) = authority {
        config = config.with_authority(AuthorityConfig {
            authority: Arc::new(authority),
            timings: AuthorityTimings {
                ttl: Duration::from_millis(AUTHORITY_TTL_MS),
            },
        });
    }
    crate::support::worker::spawn_worker(config).await
}

/// An authority that has finished warming up.
async fn warmed_up_authority() -> InMemoryAuthority<RealClock> {
    warmed_up_in_memory_authority(&shard(), Duration::from_millis(AUTHORITY_TTL_MS)).await
}

// Seedless workers that start together over one authority race to found the
// shard. The authority lets exactly one win; each of the others finds the
// winner registered, asks it, and joins it once it leads, rather than
// founding a second shard beside it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seedless_workers_starting_together_found_one_shard_and_join_its_founder() {
    let authority = warmed_up_authority().await;
    let mut workers = Vec::new();
    for _ in 0..3 {
        workers.push(spawn_worker("/ip4/127.0.0.1/tcp/0", Some(authority.clone()), vec![]).await);
    }

    let mut leaders = Vec::new();
    for worker in &mut workers {
        let seen = worker
            .wait_until(|seen| seen.leader.is_some() && seen.state != WorkerState::Bootstrapping)
            .await;
        leaders.push(seen.leader.expect("the node follows a leader"));
    }
    let founder = leaders[0].clone();
    assert!(
        leaders.iter().all(|leader| *leader == founder),
        "every worker follows the one founder: {leaders:?}"
    );
    // The others joined it as pending members, so following it is all they
    // share: whether a batch has admitted one yet is a race (ADR-0001
    // decision 9).
    let founder_worker = workers
        .iter_mut()
        .find(|worker| worker.id == founder)
        .expect("the founder is one of the workers");
    let seen = founder_worker.wait_until(|_| true).await;
    assert!(
        !seen.pending,
        "the founder is a voter of the shard it founded"
    );
    assert_eq!(
        authority
            .read_recovery_epoch(&shard())
            .map(|epoch| epoch.map(|epoch| epoch.number)),
        Ok(Some(0))
    );
}

/// A shard led by a founder, a second worker that joined it, and three more
/// workers given only that second worker's address as their one seed, all
/// over one authority. Returns them once each of the three follows the
/// founder: the founder first, the seed second.
async fn three_workers_joined_through_one_seed(
    authority: &InMemoryAuthority<RealClock>,
) -> Vec<RunningWorker> {
    let mut founder = spawn_worker("/ip4/127.0.0.1/tcp/0", Some(authority.clone()), vec![]).await;
    founder
        .wait_until(|seen| seen.state == WorkerState::Leader)
        .await;
    let leader = founder.id.clone();

    let mut seed = spawn_worker(
        "/ip4/127.0.0.1/tcp/0",
        Some(authority.clone()),
        vec![founder.address.clone()],
    )
    .await;
    seed.wait_to_follow(&leader).await;

    let mut workers = vec![founder, seed];
    for _ in 0..3 {
        let seeds = vec![workers[1].address.clone()];
        workers.push(spawn_worker("/ip4/127.0.0.1/tcp/0", Some(authority.clone()), seeds).await);
    }
    for joiner in &mut workers[2..] {
        joiner.wait_to_follow(&leader).await;
    }
    workers
}

// Each of three workers learns the leader from a seed that is not the
// leader, registers with the authority at the address it listens on once it
// is driven, and, through kad's bootstrap after its join connections, comes
// to know the other two joiners, whose addresses it was never given. The
// leader then admits every worker that joined it in an admission batch
// (ADR-0001 decision 9): each becomes a voter.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn three_workers_that_join_through_one_seed_register_and_are_admitted() {
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
        worker.wait_until(|seen| !seen.pending).await;
    }
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

fn is_loopback(address: &Multiaddr) -> bool {
    address.iter().any(|protocol| match protocol {
        Protocol::Ip4(ip) => ip.is_loopback(),
        Protocol::Ip6(ip) => ip.is_loopback(),
        _ => false,
    })
}

// A leader bound to every interface must not point joiners at its loopback
// address: a joiner on another host would dial itself. It names an address
// another host can reach, and a joiner dialing that address reaches it.
// Skipped, saying so, on a host with no non-loopback interface (a sandbox
// with networking off), where loopback is all a leader can offer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_leader_bound_to_every_interface_points_joiners_at_a_non_loopback_address() {
    if !has_a_non_loopback_address() {
        eprintln!("skipped: this host has no non-loopback interface");
        return;
    }
    let mut leader = spawn_worker("/ip4/0.0.0.0/tcp/0", None, vec![]).await;
    leader
        .wait_until(|seen| seen.state == WorkerState::Leader)
        .await;

    let joining_net = Net::new();
    let search = timeout(
        TEST_TIMEOUT,
        ask_for_leader(&joining_net, std::slice::from_ref(&leader.address), PER_PEER_TIMEOUT),
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
    assert!(!is_loopback(&pointed), "the leader pointed at {pointed}");

    // Another worker, asking at the address the pointer names,
    // reaches the leader there.
    let remote_net = Net::new();
    let search = timeout(
        TEST_TIMEOUT,
        ask_for_leader(&remote_net, &[pointed], PER_PEER_TIMEOUT),
    )
    .await
    .expect("the leader answered within the timeout");
    assert_eq!(search, LeaderSearch::Found(pointer));
}
