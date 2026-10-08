//! A node cut off from its shard, driven by `run_driver` over real sockets
//! and an authority that fails: a stranded node asks the workers the
//! authority lists, and then its seeds, for a leader; a node that fenced
//! itself rejoins the shard the authority says recovered, and ends up a
//! member of the lineage the authority holds, not a stale leader's.

use std::sync::Arc;
use std::time::Duration as StdDuration;

use kabudachi_core::configuration::{Configuration, Generation, Single};
use kabudachi_core::coordination_authority::{CoordinationAuthority, RecoveryEpoch};
use kabudachi_core::election::{
    AuthorityTimings, CallKind, ElectionTimings, Entry, Identity, Input, KnownConfiguration,
    Step, WorkerNode,
};
use kabudachi_core::protocol::ids::{IncarnationId, ShardId, WorkerId};
use kabudachi_core::protocol::messages::JoinResponse;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::{Duration as TickDuration, RealClock};
use kabudachi_net::authority::AuthorityClient;
use kabudachi_net::driver::{DriverConfig, run_driver};
use kabudachi_net::messenger::Net;
use kabudachi_testkit::FaultingAuthority;
use tokio::sync::{Notify, watch};
use tokio::time::timeout;

use crate::support::deadline::within_deadline;
use crate::support::net::{
    JoinResponder, driven_scheduler, listening_net, take_inputs_until,
};

const TEST_TIMEOUT: StdDuration = StdDuration::from_secs(30);

fn shard() -> ShardId {
    ShardId::new("shard-1")
}

/// A configuration of two voters, both admitted at genesis.
fn two_voters() -> Configuration {
    Configuration::single(Single {
        generation: Generation::genesis(RecoveryEpoch::new(0, 0)),
        base: Generation::genesis(RecoveryEpoch::new(0, 0)),
        voter_count: 2,
    })
    .expect("valid")
}

/// `me`'s node, a voter of a configuration of two, suspecting a leader only
/// after `suspect_timeout`, and the first step it starts with.
fn node_of_two(
    clock: RealClock,
    me: &WorkerId,
    suspect_timeout: TickDuration,
    authority: Option<AuthorityTimings>,
) -> (WorkerNode<RealClock>, Step) {
    let identity = Identity {
        id: me.clone(),
        incarnation: IncarnationId::new("incarnation-0"),
        shard: shard(),
        // Twice 40 ms fits inside the lease of the shortest suspicion timeout
        // the tests here use (100 ms, less a tenth).
        timings: ElectionTimings::new(suspect_timeout, TickDuration::from_millis(40))
            .with_roll_call_deadline(TickDuration::from_millis(50)),
    };
    let known = KnownConfiguration {
        configuration: two_voters(),
        admission: Some(Generation::genesis(RecoveryEpoch::new(0, 0))),
    };
    WorkerNode::start(identity, Entry::Known(known), clock, authority)
}

/// A pointer at `leader`, reachable at `address`, of epoch `recovery_epoch`
/// of `lineage` and term 1.
fn pointer(
    leader: &WorkerId,
    address: &libp2p::Multiaddr,
    recovery_epoch: u64,
    lineage: u64,
) -> JoinResponse {
    JoinResponse {
        leader_id: Some(leader.clone().into()),
        leader_multiaddr: address.to_string(),
        term: 1,
        recovery_epoch,
        recovery_epoch_lineage: lineage,
    }
}

/// Registers every one of `workers` at `authority` again and again, for
/// ever: a registration lasts one `ttl`, so a test that must keep a worker
/// listed while it waits keeps registering it.
async fn keep_registered(
    authority: &FaultingAuthority<RealClock>,
    workers: &[(WorkerId, String)],
) {
    loop {
        for (id, address) in workers {
            authority
                .register(&shard(), id, address)
                .expect("the authority is reachable");
        }
        tokio::time::sleep(StdDuration::from_millis(100)).await;
    }
}

/// How the authority treats the stranded node's read of its listing.
enum Listing {
    /// It answers, naming a worker that answers JOIN.
    Answers,
    /// The first read panics; the next answers as above.
    PanicsOnce,
    /// The read hangs, and the node has a seed that answers JOIN.
    HeldWithASeed,
}

// A node that has been in `RollCall` or `NoQuorum` for a suspicion timeout is
// stranded, and searches the authority's listing, then its seeds, for a
// leader to reconnect to. The watch keys on the node's state and time alone:
// a peer it still hears, one that never answers its roll call, does not hold
// the search back.
async fn a_stranded_node_reaches_a_leader_despite(listing: Listing) {
    let clock = RealClock::new();
    let (worker_net, worker_address) = listening_net().await;
    let worker_net = Arc::new(worker_net);
    let worker = worker_net.local_worker_id();
    let _worker_answers = JoinResponder::start(
        Arc::clone(&worker_net),
        Some(pointer(&worker, &worker_address, 1, 0)),
    );
    // A peer the node hears from the start and that never answers: its roll
    // call gets no quorum.
    let (silent_peer, node_net) = {
        let (silent_peer, _) = listening_net().await;
        let node_net = Net::new();
        let node_address = node_net
            .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
            .await;
        silent_peer.dial(node_address);
        take_inputs_until(
            &silent_peer,
            &[Input::PeerConnected(node_net.local_worker_id())],
        )
        .await;
        (silent_peer, node_net)
    };
    let me = node_net.local_worker_id();

    let authority = FaultingAuthority::new(clock, TickDuration::from_millis(1_000));
    let node_handle = authority.for_another_worker();
    let listed = vec![(worker.clone(), worker_address.to_string())];
    let mut config = DriverConfig::default();
    // Keeps a seed's answers going for as long as the test runs.
    let mut _seed_answers = None;
    // The net a stranded node is expected to reach.
    let reached_net = match listing {
        Listing::Answers => Arc::clone(&worker_net),
        Listing::PanicsOnce => {
            node_handle.panic_next(CallKind::ReadLiveRegistrations);
            Arc::clone(&worker_net)
        }
        Listing::HeldWithASeed => {
            let (seed_net, seed_address) = listening_net().await;
            let seed_net = Arc::new(seed_net);
            let seed = seed_net.local_worker_id();
            _seed_answers = Some(JoinResponder::start(
                Arc::clone(&seed_net),
                Some(pointer(&seed, &seed_address, 1, 0)),
            ));
            config.seeds = vec![seed_address];
            config.retry_interval = StdDuration::from_millis(100);
            node_handle.hold_next(CallKind::ReadLiveRegistrations);
            seed_net
        }
    };
    // Long enough that the held listing read of `HeldWithASeed` is not given up.
    let timings = AuthorityTimings { ttl: TickDuration::from_secs(60) };
    let client = AuthorityClient::new(&node_net, shard(), Arc::new(node_handle.clone()), timings);
    let (mut node, first) = node_of_two(clock, &me, TickDuration::from_millis(100), None);
    let mut scheduler = driven_scheduler(clock);
    let (seen, observed) = watch::channel(node.state());

    let driven = run_driver(
        &mut node,
        first,
        &node_net,
        &mut scheduler,
        clock,
        Some(client),
        config,
        |node, _, _| {
            seen.send_replace(node.state());
        },
    );
    let reached_out = async {
        take_inputs_until(&reached_net, &[Input::PeerConnected(me.clone())]).await;
        *observed.borrow()
    };
    let state_when_it_reached_out = timeout(TEST_TIMEOUT, async {
        tokio::select! {
            _ = driven => unreachable!("this test never drains a node, so its driver never returns"),
            () = keep_registered(&authority, &listed) => unreachable!("registers for ever"),
            state = reached_out => state,
        }
    })
    .await
    .expect("the stranded node reached a leader within the timeout");
    if matches!(listing, Listing::HeldWithASeed) {
        assert!(
            node_handle.is_holding(CallKind::ReadLiveRegistrations),
            "the node reached its seed after its listing read was answered, not while it was held"
        );
    }
    // Frees a held read's thread so the runtime can shut down.
    node_handle.release(CallKind::ReadLiveRegistrations);
    drop(silent_peer);

    assert!(
        matches!(
            state_when_it_reached_out,
            WorkerState::RollCall | WorkerState::NoQuorum
        ),
        "it reached out while stranded, not after finding a leader: {state_when_it_reached_out:?}"
    );
}

#[tokio::test]
async fn a_stranded_node_asks_the_workers_the_authority_lists() {
    within_deadline(async {
        a_stranded_node_reaches_a_leader_despite(Listing::Answers).await;
    })
    .await
}

#[tokio::test]
async fn a_stranded_node_whose_listing_read_panicked_reads_it_again_and_reaches_the_worker() {
    within_deadline(async {
        a_stranded_node_reaches_a_leader_despite(Listing::PanicsOnce).await;
    })
    .await
}

#[tokio::test]
async fn a_stranded_node_whose_listing_read_hangs_asks_its_seed_meanwhile() {
    within_deadline(async {
        a_stranded_node_reaches_a_leader_despite(Listing::HeldWithASeed).await;
    })
    .await
}

// A rejoining node takes the pointer of a stale leader of the lineage its
// floor stands at, while the authority holds the shard refounded under
// another lineage at that same number. The node must not become that
// leader's member: it holds the pointer in `Joining` until a read of the
// authority validates it, finds the epoch differs, drops the pointer and
// joins the new lineage's leader, whose pointer, at no higher epoch number,
// its floor accepts only once it has the authority's epoch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_that_took_a_pointer_of_a_refounded_lineage_ends_active_in_the_new_one() {
    within_deadline(async {
        let clock = RealClock::new();
        let ttl = TickDuration::from_millis(1_000);
        let authority = FaultingAuthority::new(clock, ttl);
        let elsewhere = authority.for_another_worker();
        elsewhere
            .compare_and_swap_recovery_epoch(&shard(), None, RecoveryEpoch::new(0, 0))
            .expect("a fresh authority holds no epoch");
        // Two leaders answer JOIN, each pointing at itself: the old lineage's and
        // the refounded one's.
        let mut leaders = Vec::new();
        for lineage in [1, 2] {
            let (net, address) = listening_net().await;
            let net = Arc::new(net);
            let id = net.local_worker_id();
            let responder =
                JoinResponder::start(Arc::clone(&net), Some(pointer(&id, &address, 2, lineage)));
            leaders.push((net, id, address, responder));
        }
        let net = Net::new();
        let me = net.local_worker_id();
        let (mut node, first) = node_of_two(
            clock,
            &me,
            TickDuration::from_secs(60),
            Some(AuthorityTimings { ttl }),
        );
        let mut scheduler = driven_scheduler(clock);
        let mine = authority.for_another_worker();
        let (seen_tx, mut seen) = watch::channel((WorkerState::Active, None));
        let driven = run_driver(
            &mut node,
            first,
            &net,
            &mut scheduler,
            clock,
            Some(AuthorityClient::new(
                &net,
                shard(),
                Arc::new(mine.clone()),
                AuthorityTimings { ttl },
            )),
            DriverConfig::default(),
            |node, _, _| {
                seen_tx.send_replace((node.state(), node.recovery_lineage()));
            },
        );
        let last = seen.clone();
        let listed = Notify::new();
        let register_leaders = async {
            listed.notified().await;
            let listed: Vec<_> = leaders
                .iter()
                .map(|(_, id, address, _)| (id.clone(), address.to_string()))
                .collect();
            keep_registered(&elsewhere, &listed).await;
        };
        let scenario = async {
            let mut wait_for = async |what: &str, holds: fn(&(WorkerState, Option<u64>)) -> bool| {
                timeout(TEST_TIMEOUT, seen.wait_for(holds))
                    .await
                    .unwrap_or_else(|_| panic!("{what} within the timeout: {:?}", *last.borrow()))
                    .expect("the driver is running");
            };
            mine.set_reachable(false);
            wait_for("the node fenced itself", |seen| seen.0 == WorkerState::Fenced).await;
            elsewhere
                .compare_and_swap_recovery_epoch(
                    &shard(),
                    Some(RecoveryEpoch::new(0, 0)),
                    RecoveryEpoch::new(2, 1),
                )
                .expect("a recovery elsewhere moved the epoch on");
            mine.set_reachable(true);
            wait_for("the node rejoined at epoch 2 of lineage 1", |seen| {
                *seen == (WorkerState::Bootstrapping, Some(1))
            })
            .await;
            // The node's next read of the epoch lags, and the shard is refounded
            // under lineage 2 at the same number. Both leaders are listed only
            // now: the old one's pointer is the only one the node's floor accepts.
            // The shard is refounded only once the next read is held: a read
            // already past the hold could otherwise reach the authority after
            // the refounding, and the node would learn the new lineage early.
            // The client keeps one read in flight, so every read before the
            // held one has been answered by then.
            mine.hold_next(CallKind::ReadRecoveryEpoch);
            timeout(TEST_TIMEOUT, async {
                while !mine.is_holding(CallKind::ReadRecoveryEpoch) {
                    tokio::time::sleep(StdDuration::from_millis(5)).await;
                }
            })
            .await
            .expect("the node's next read of the epoch was held within the timeout");
            elsewhere
                .compare_and_swap_recovery_epoch(
                    &shard(),
                    Some(RecoveryEpoch::new(2, 1)),
                    RecoveryEpoch::new(2, 2),
                )
                .expect("the shard is refounded");
            listed.notify_one();
            wait_for("the node took the old lineage's pointer and awaits the authority", |seen| {
                *seen == (WorkerState::Joining, Some(1))
            })
            .await;
            mine.release(CallKind::ReadRecoveryEpoch);
            wait_for("the node is a member in the refounded lineage", |seen| {
                *seen == (WorkerState::Active, Some(2))
            })
            .await;
        };
        tokio::select! {
            _ = driven => unreachable!("this test never drains a node, so its driver never returns"),
            () = register_leaders => unreachable!("registering never ends"),
            () = scenario => {}
        }
    })
    .await
}
