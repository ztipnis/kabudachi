//! E8: election over `Net` (walking skeleton). Three swarms driven through
//! their production entry point (`kabudachi_net::worker::Worker`, E9): a
//! genesis leader, and two joiners given its address directly, admitted into
//! its configuration by the E4c admission batch (N = 3). The leader is then
//! cut — its whole process gone, not merely disconnected (see
//! `docs/superpowers/plans/notes/e8.md`, E8-R1) — and the two survivors meet
//! the returning quorum and elect one of themselves, with no coordination
//! authority anywhere.
//!
//! Two controls share the same topology but cut the leader before the
//! admission batch ever commits, so the founder's own N = 1 configuration
//! (the only one anyone knows of) names no voter but the leader now gone:
//! with no authority, the survivors can never form a returning quorum and
//! stay stuck short of one; with an `InMemoryAuthority` configured and every
//! worker registered, the authority path — triggered automatically once an
//! ordinary roll call comes up short (`core/src/election.rs`'s
//! `close_roll_call`) — recovers the shard around the two live survivors
//! instead. Building "before the batch commits" without racing needs a
//! lower-level join than `Worker`'s: see `support::worker::
//! join_without_heartbeating`'s doc and E8-R3 in
//! `docs/superpowers/plans/notes/e8.md`.

mod support;

use std::sync::Arc;
use std::time::Duration as StdDuration;

use kabudachi_core::coordination_authority::CoordinationAuthority;
use kabudachi_core::election::{AuthorityTimings, ElectionTimings, WorkerNode};
use kabudachi_core::in_memory_authority::InMemoryAuthority;
use kabudachi_core::protocol::ids::ShardId;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::{Duration, RealClock};
use kabudachi_net::messenger::Net;
use kabudachi_net::swarm::build_swarm;
use kabudachi_net::worker::{AuthorityConfig, WorkerConfig};
use libp2p::identity;
use libp2p::Multiaddr;
use tokio::sync::watch;
use tokio::time::sleep_until;

use support::worker::{
    Seen, drive_bootstrapped_node, join_without_heartbeating, poll_until, spawn_worker,
    wait_for_seen, warmed_up_in_memory_authority,
};

const SHARD: &str = "shard-1";

/// Every node's suspicion timeout.
const SUSPECT_TIMEOUT_MS: u64 = 300;

/// How often an admitted worker heartbeats its leader: well inside every
/// suspicion timeout this file uses.
const HEARTBEAT_INTERVAL_MS: u64 = 50;

/// How long a roll call runs: well above the time one takes to reach a
/// loopback peer and its reply to come back.
const ROLL_CALL_DEADLINE_MS: u64 = 100;

/// Short, so a waiting worker goes round the join cascade many times per
/// test.
const RETRY_INTERVAL: StdDuration = StdDuration::from_millis(50);

/// How long each ask of a seed may take.
const PER_PEER_TIMEOUT: StdDuration = StdDuration::from_secs(1);

/// The authority's registration TTL, which is also how long it warms up.
const AUTHORITY_TTL_MS: u64 = 500;

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

/// A `WorkerConfig` for a worker listening on `bind`, bootstrapping through
/// `seeds` on `election_timings`, with `authority` if any.
fn worker_config(
    bind: &str,
    seeds: Vec<Multiaddr>,
    election_timings: ElectionTimings,
    authority: Option<InMemoryAuthority<RealClock>>,
) -> WorkerConfig {
    let mut config = WorkerConfig::new(shard(), bind.parse().unwrap(), election_timings)
        .with_seeds(seeds)
        .with_join_peer_timeout(PER_PEER_TIMEOUT)
        .with_retry_interval(RETRY_INTERVAL);
    if let Some(authority) = authority {
        config = config.with_authority(AuthorityConfig {
            authority: Arc::new(authority),
            timings: authority_timings(),
        });
    }
    config
}

fn authority_timings() -> AuthorityTimings {
    AuthorityTimings {
        ttl: Duration::from_millis(AUTHORITY_TTL_MS),
    }
}

/// A bare, listening `Net` on loopback TCP with a fresh identity.
async fn fresh_listening_net() -> Net {
    let net = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
    net.try_listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .expect("a fresh net can always listen on an ephemeral port");
    net
}

/// Watches `seen` for `window`, failing at once if it ever shows `Leader`:
/// used to prove a survivor stays stuck short of a quorum, not merely that
/// it visited `NoQuorum` once (a node short of quorum cycles `NoQuorum ->
/// RollCall -> NoQuorum` forever, retrying every roll-call deadline).
/// Event-driven (reacts to `watch::Receiver::changed`), bounded only by
/// `window` — proving a negative needs a real bound, not a signal to wait
/// for, the same shape `leader_observes_follower_drop_test.rs` uses to
/// prove a leader keeps its quorum.
async fn assert_never_leads(seen: &mut watch::Receiver<Option<Seen>>, window: StdDuration) {
    let deadline = tokio::time::Instant::now() + window;
    loop {
        tokio::select! {
            changed = seen.changed() => {
                changed.expect("the worker's task is still running");
                if let Some(observed) = seen.borrow().as_ref() {
                    assert_ne!(
                        observed.state,
                        WorkerState::Leader,
                        "a survivor short of a quorum, with no authority, must never lead"
                    );
                }
            }
            () = sleep_until(deadline) => return,
        }
    }
}

// A genesis leader (no authority) admits two joiners, given its address
// directly, into one N = 3 configuration. Once both are voters, the leader
// is cut. The two survivors are still a majority of that configuration, so
// an ordinary roll call and vote elects one of themselves at once: no
// authority is configured anywhere, and none is needed. (No suspicion
// staggering is needed here: CallRank's earliest-timestamp tie-break
// resolves two nodes suspecting at the same instant on its own, confirmed
// by repeated runs — see docs/superpowers/plans/notes/e8.md.)
#[tokio::test]
async fn two_committed_survivors_elect_one_of_themselves_after_the_leader_is_cut() {
    let mut leader = spawn_worker(worker_config("/ip4/127.0.0.1/tcp/0", vec![], timings(), None)).await;
    leader
        .wait_until(|seen| seen.state == WorkerState::Leader)
        .await;
    let leader_id = leader.id.clone();
    let leader_address = leader.address.clone();

    let mut joiner_a = spawn_worker(worker_config(
        "/ip4/127.0.0.1/tcp/0",
        vec![leader_address.clone()],
        timings(),
        None,
    ))
    .await;
    let mut joiner_b = spawn_worker(worker_config(
        "/ip4/127.0.0.1/tcp/0",
        vec![leader_address.clone()],
        timings(),
        None,
    ))
    .await;
    joiner_a.wait_to_follow(&leader_id).await;
    joiner_b.wait_to_follow(&leader_id).await;

    // The admission batch commits: both joiners become voters of the N = 3
    // configuration before the leader is cut.
    joiner_a.wait_until(|seen| !seen.pending).await;
    joiner_b.wait_until(|seen| !seen.pending).await;

    // The two survivors must be able to reach each other's roll calls
    // (gossipsub) once the leader is gone, not just each know of the leader:
    // each was seeded with only the leader's address, so kad's post-join
    // crawl (E9-R1) is what connects them to each other directly.
    support::net::wait_until_subscribed(&joiner_a.net, &[&joiner_b.id]).await;
    support::net::wait_until_subscribed(&joiner_b.net, &[&joiner_a.id]).await;

    drop(leader);

    let mut rx_a = joiner_a.seen.clone();
    let mut rx_b = joiner_b.seen.clone();
    let winner_id = tokio::select! {
        _ = wait_for_seen(&mut rx_a, |seen| seen.state == WorkerState::Leader) => joiner_a.id.clone(),
        _ = wait_for_seen(&mut rx_b, |seen| seen.state == WorkerState::Leader) => joiner_b.id.clone(),
    };

    let loser = if winner_id == joiner_a.id {
        &mut joiner_b
    } else {
        &mut joiner_a
    };
    let loser_seen = loser
        .wait_until(|seen| seen.leader.as_ref() == Some(&winner_id) && seen.state == WorkerState::Active)
        .await;
    assert!(
        !loser_seen.pending,
        "the surviving follower stays a voter of the elected leader's configuration"
    );
}

/// Joins `net`'s worker to `leader_address` without heartbeating it (see
/// `support::worker::join_without_heartbeating`, which already guarantees
/// the returned node holds a configuration), and confirms it is a pending
/// member: real core behavior, not something the join helper's own
/// construction already guarantees the way holding a configuration is.
async fn join_pending_and_unheard_of(
    net: &Net,
    leader_address: &Multiaddr,
    authority: Option<&InMemoryAuthority<RealClock>>,
) -> WorkerNode<RealClock> {
    let my_id = net.local_worker_id();
    let node = join_without_heartbeating(
        net,
        my_id,
        shard(),
        RealClock::new(),
        authority.map(|authority| (authority, authority_timings())),
        timings(),
        leader_address,
        PER_PEER_TIMEOUT,
    )
    .await;
    assert!(
        node.is_pending_member(),
        "a joiner is a pending member of the shard it joined until an election admits it"
    );
    node
}

// Control: the leader is cut before either joiner's admission batch ever
// commits. Both joiners are built with `join_without_heartbeating` (E8-R3):
// each is pending, knows the leader and holds its N = 1 configuration, but
// has never heartbeated it — the leader is cut immediately afterwards,
// before anything drives either joiner, so no heartbeat, and so no
// admission, can ever reach it. The founder's own N = 1 configuration is
// the only one anyone knows of, and it names no voter but the leader now
// gone. With no authority to fall back on, the survivors can never form a
// returning quorum: every roll call comes up short, and they stay stuck in
// `NoQuorum` for good.
#[tokio::test]
async fn pending_survivors_with_no_authority_never_leave_no_quorum_after_the_leader_is_cut_before_admission()
 {
    let mut leader = spawn_worker(worker_config("/ip4/127.0.0.1/tcp/0", vec![], timings(), None)).await;
    leader
        .wait_until(|seen| seen.state == WorkerState::Leader)
        .await;
    let leader_address = leader.address.clone();

    let net_a = fresh_listening_net().await;
    let net_b = fresh_listening_net().await;
    let node_a = join_pending_and_unheard_of(&net_a, &leader_address, None).await;
    let node_b = join_pending_and_unheard_of(&net_b, &leader_address, None).await;
    let (id_a, address_a) = (
        net_a.local_worker_id(),
        net_a.local_multiaddr().expect("a listening net has an address"),
    );
    let (id_b, address_b) = (
        net_b.local_worker_id(),
        net_b.local_multiaddr().expect("a listening net has an address"),
    );

    // The cut, before anything has driven either joiner: neither has sent a
    // heartbeat, so the leader has never had a chance to admit either.
    drop(leader);

    let clock = RealClock::new();
    let mut joiner_a = drive_bootstrapped_node(node_a, id_a, address_a, Arc::new(net_a), clock, None);
    let mut joiner_b = drive_bootstrapped_node(node_b, id_b, address_b, Arc::new(net_b), clock, None);

    // The survivors need a direct connection to reach each other's roll
    // calls: with the leader gone, nothing else connects them.
    joiner_a.net.dial(joiner_b.address.clone());
    support::net::wait_until_subscribed(&joiner_a.net, &[&joiner_b.id]).await;
    support::net::wait_until_subscribed(&joiner_b.net, &[&joiner_a.id]).await;

    joiner_a
        .wait_until(|seen| seen.state == WorkerState::NoQuorum)
        .await;
    joiner_b
        .wait_until(|seen| seen.state == WorkerState::NoQuorum)
        .await;

    let window = StdDuration::from_millis((SUSPECT_TIMEOUT_MS + ROLL_CALL_DEADLINE_MS) * 5);
    let mut rx_a = joiner_a.seen.clone();
    let mut rx_b = joiner_b.seen.clone();
    tokio::join!(
        assert_never_leads(&mut rx_a, window),
        assert_never_leads(&mut rx_b, window),
    );
}

// Control: the same before-admission setup (E8-R3's
// `join_without_heartbeating`), but every worker (the leader included)
// shares one warmed-up `InMemoryAuthority`, and both survivors are
// registered with it directly as part of joining (matching what the
// bootstrap cascade's own registration would have done once driven, E5-R14).
// The same failed roll call this time trips the authority path
// (`core/src/election.rs`'s `close_roll_call`, `if self.authority.is_some()
// { self.begin_forced_recovery(&round); }`): floor(N_auth / 2) + 1 = 2 of
// the 3 registered workers is met by the two live survivors answering the
// one failed roll call, so it swaps the recovery epoch and founds a fresh
// configuration whose voters are exactly the two survivors — unstuck from
// the dead leader's stale N = 1 configuration the no-authority control above
// can never escape.
#[tokio::test]
async fn an_authority_lets_two_registered_survivors_elect_one_of_themselves_after_the_leader_is_cut_before_admission()
 {
    let authority = warmed_up_in_memory_authority(&shard(), Duration::from_millis(AUTHORITY_TTL_MS)).await;

    let mut leader = spawn_worker(worker_config(
        "/ip4/127.0.0.1/tcp/0",
        vec![],
        timings(),
        Some(authority.clone()),
    ))
    .await;
    leader
        .wait_until(|seen| seen.state == WorkerState::Leader)
        .await;
    let leader_address = leader.address.clone();

    let net_a = fresh_listening_net().await;
    let net_b = fresh_listening_net().await;
    let node_a = join_pending_and_unheard_of(&net_a, &leader_address, Some(&authority)).await;
    let node_b = join_pending_and_unheard_of(&net_b, &leader_address, Some(&authority)).await;
    let (id_a, address_a) = (
        net_a.local_worker_id(),
        net_a.local_multiaddr().expect("a listening net has an address"),
    );
    let (id_b, address_b) = (
        net_b.local_worker_id(),
        net_b.local_multiaddr().expect("a listening net has an address"),
    );

    // Both joiners registered with the authority directly, as part of
    // joining (`join_without_heartbeating`); the leader registers through
    // its own driven cascade, so its registration is polled for.
    poll_until("the leader registered at its listen address", || {
        authority
            .live_registrations(&shard())
            .expect("the in-memory authority is always reachable")
            .addresses()
            .get(&leader.id)
            == Some(&leader.address.to_string())
    })
    .await;

    // The cut, before anything has driven either joiner: neither has sent a
    // heartbeat, so the leader has never had a chance to admit either.
    drop(leader);

    let clock = RealClock::new();
    let joiner_authority: kabudachi_net::driver::SharedAuthority = Arc::new(authority.clone());
    let mut joiner_a = drive_bootstrapped_node(
        node_a,
        id_a,
        address_a,
        Arc::new(net_a),
        clock,
        Some(Arc::clone(&joiner_authority)),
    );
    let mut joiner_b = drive_bootstrapped_node(
        node_b,
        id_b,
        address_b,
        Arc::new(net_b),
        clock,
        Some(joiner_authority),
    );

    joiner_a.net.dial(joiner_b.address.clone());
    support::net::wait_until_subscribed(&joiner_a.net, &[&joiner_b.id]).await;
    support::net::wait_until_subscribed(&joiner_b.net, &[&joiner_a.id]).await;

    let mut rx_a = joiner_a.seen.clone();
    let mut rx_b = joiner_b.seen.clone();
    let winner_id = tokio::select! {
        _ = wait_for_seen(&mut rx_a, |seen| seen.state == WorkerState::Leader) => joiner_a.id.clone(),
        _ = wait_for_seen(&mut rx_b, |seen| seen.state == WorkerState::Leader) => joiner_b.id.clone(),
    };

    let loser = if winner_id == joiner_a.id {
        &mut joiner_b
    } else {
        &mut joiner_a
    };
    // Waits for the loser to follow the winner as a full voter (not
    // pending) of the recovered configuration.
    loser
        .wait_until(|seen| seen.leader.as_ref() == Some(&winner_id) && !seen.pending)
        .await;

    let recovered_epoch = authority
        .read_recovery_epoch(&shard())
        .expect("the in-memory authority is always reachable")
        .map(|epoch| epoch.number);
    assert!(
        recovered_epoch.is_some_and(|number| number > 0),
        "the survivors recovered through the authority, past the founder's stale N = 1 epoch 0, \
         got {recovered_epoch:?}"
    );
}
