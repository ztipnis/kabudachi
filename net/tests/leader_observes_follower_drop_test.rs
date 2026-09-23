//! Chunk C8, Part 1: proves the specific scenario the plan's own text names
//! for "map `ConnectionClosed` into `reachable_peers` promptly" that no
//! existing test covers — see `task-C8-brief.md`'s Part 1 section.
//!
//! `net/src/messenger.rs`'s `reachable_peers_drops_a_peer_once_it_disconnects`
//! and `disconnect_drops_the_peer_from_reachable_peers_on_both_sides` (chunks
//! C2/C7) already prove `Net::reachable_peers` tracks the swarm's live
//! connected-peer set at the transport layer alone, and
//! `net/tests/ring_roll_call_leader_loss_test.rs` (chunk C7) already proves
//! the *disconnected node's own view of itself* — a leader that severs every
//! one of its own connections observes its own `reachable_peers` go empty and
//! demotes itself to `NoQuorum`.
//!
//! Neither proves the complementary case this file covers: a **surviving**
//! leader's `core::election::WorkerNode::tick_as_leader` (unmodified since
//! Phase 0 — `core/src/election.rs:275-304`) correctly recomputing
//! `reachable_electorate` as its individual followers drop out from under it
//! one at a time, while the leader itself stays fully up and connected to
//! everyone else. This matters because `tick_as_leader`'s quorum math
//! (`electorate.len() / 2 + 1`, `visible = 1 + reachable_electorate.len()`)
//! is evaluated fresh every tick against whatever `reachable_peers` reports
//! *at that moment* — a stale view here would let a leader believe it still
//! has quorum after followers have actually gone.
//!
//! ## Topology and why the election here is safe, unlike C7's naive first
//! ## design
//!
//! Electorate `{a, b, c}` (3 members), pre-seeded via `WorkerNode::new` (not
//! the join cascade — see `ring_roll_call_leader_loss_test.rs`'s doc for why
//! that's necessary at all: `finish_joining` only updates the *joining*
//! node's own membership, so a chained join would leave the three nodes with
//! three different, incomplete electorates, which is exactly wrong for a
//! test needing every node to agree on quorum math).
//!
//! `a` and `b` are given the same short `FAST_SUSPECT_TIMEOUT_MS`, from
//! synchronous back-to-back construction — the exact engineering margin
//! `net/tests/two_node_election_test.rs` (chunk C3) established and depends
//! on, carried over unchanged. `c` is given a much longer
//! `BYSTANDER_SUSPECT_TIMEOUT_MS` so it never independently begins its own
//! roll call within this test's window — it only ever participates
//! passively, forwarding whatever call `a` or `b` originates.
//!
//! This is deliberately *not* a repeat of
//! `ring_roll_call_leader_loss_test.rs`'s 5-node race, and does not need that
//! file's precomputed-winner/relay-chain machinery: this test does not care
//! *which* of `a`/`b` wins (only that convergence to exactly one Leader and
//! two Active members happens at all), so there is no need to predict
//! `choose_candidate`'s exact output. With only 3 electorate members and
//! quorum(3) = 2, any roll call reaches quorum after exactly one forwarding
//! hop — a much smaller, much less failure-prone state space than C7's
//! 5-member/fanout-3 case, which is what made that file's naive symmetric
//! -race design "fail more often than not" in practice. A light retry
//! wrapper (`MAX_ATTEMPTS`, mirroring C7's own defensive posture, though far
//! simpler since this file tracks no hop chains) absorbs whatever residual
//! real-timing risk remains, same as C7's own justification for doing so.

mod support;

use std::collections::BTreeSet;
use std::time::Duration as StdDuration;

use kabudachi_core::election::WorkerNode;
use kabudachi_core::membership::RingMembership;
use kabudachi_core::protocol::ids::{IncarnationId, ShardId, Uuid7Ids, WorkerId};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::Scheduler;
use kabudachi_core::time::Duration;
use kabudachi_core::transport::PeerMessenger;
use kabudachi_net::driver::run_driver;
use kabudachi_net::messenger::Net;
use kabudachi_net::swarm::build_swarm;
use libp2p::identity;
use tokio::sync::watch;
use tokio::time::timeout;

use support::authority::AlwaysUnavailableAuthority;
use support::clock::RealClock;

const SHARD: &str = "shard-1";

/// Matches `two_node_election_test.rs`'s own `SUSPECT_TIMEOUT_MS` — see this
/// file's module doc for why reusing that exact margin is what makes `a`/`b`
/// racing safely at n=3 a minor generalization of an already-proven pattern,
/// not a new one.
const FAST_SUSPECT_TIMEOUT_MS: u64 = 300;
/// Comfortably longer than this test's expected runtime (a few hundred ms),
/// so `c` never independently begins its own roll call — matches
/// `ring_roll_call_leader_loss_test.rs`'s `BYSTANDER_SUSPECT_TIMEOUT_MS`.
const BYSTANDER_SUSPECT_TIMEOUT_MS: u64 = 8_000;
const TICK_INTERVAL_MS: u64 = 30;

const TEST_TIMEOUT: StdDuration = StdDuration::from_secs(10);

/// Defensive retry, mirroring `ring_roll_call_leader_loss_test.rs`'s own
/// justification (see that file's `MAX_ATTEMPTS` doc) for the same class of
/// real-timing risk, at a much smaller scale here (no hop-chain determinism
/// to preserve, only "did exactly one Leader and two Active emerge").
const MAX_ATTEMPTS: u32 = 5;

type Node<'a> = WorkerNode<RealClock, &'a Net, RingMembership, AlwaysUnavailableAuthority>;

fn make_node<'a>(
    my_id: WorkerId,
    electorate: &BTreeSet<WorkerId>,
    transport: &'a Net,
    suspect_timeout_ms: u64,
) -> Node<'a> {
    WorkerNode::new(
        my_id.clone(),
        IncarnationId::new(format!("{}-incarnation-0", my_id.as_str())),
        ShardId::new(SHARD),
        RealClock::new(),
        transport,
        RingMembership::new(electorate.clone()),
        AlwaysUnavailableAuthority,
        Duration::from_ticks(suspect_timeout_ms),
    )
}

/// Full mesh over real loopback TCP, same shape as
/// `ring_roll_call_leader_loss_test.rs`'s own `connect_full_mesh`, generalized
/// from N back down to exactly 3. Not yet moved into `support::net` with
/// `connect_to`; the two copies take different node counts.
async fn connect_full_mesh(nets: &[&Net]) -> Vec<WorkerId> {
    let mut addrs = Vec::with_capacity(nets.len());
    for net in nets {
        addrs.push(
            timeout(
                TEST_TIMEOUT,
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
    for (i, net) in nets.iter().enumerate() {
        let expected: BTreeSet<WorkerId> = ids
            .iter()
            .enumerate()
            .filter(|(j, _)| *j != i)
            .map(|(_, id)| id.clone())
            .collect();
        timeout(TEST_TIMEOUT, async {
            loop {
                if net.reachable_peers(ids[i].clone()) == expected {
                    return;
                }
                tokio::time::sleep(StdDuration::from_millis(5)).await;
            }
        })
        .await
        .expect("every net saw every other net reachable within the timeout (full mesh)");
    }

    ids
}

/// Blocks until `rxs` report exactly one `Leader` and two `Active` among the
/// three nodes — this test does not care *which* one wins (see the module
/// doc), unlike `ring_roll_call_leader_loss_test.rs`'s precomputed-winner
/// assertions.
async fn wait_for_convergence(rxs: &[watch::Receiver<WorkerState>]) -> Vec<WorkerState> {
    loop {
        let states: Vec<WorkerState> = rxs.iter().map(|rx| *rx.borrow()).collect();
        let leaders = states.iter().filter(|s| **s == WorkerState::Leader).count();
        let actives = states.iter().filter(|s| **s == WorkerState::Active).count();
        if leaders == 1 && actives == 2 {
            return states;
        }
        tokio::time::sleep(StdDuration::from_millis(5)).await;
    }
}

#[tokio::test]
async fn leader_observes_a_dropped_follower_shrink_reachable_electorate_and_lose_quorum() {
    let mut last_failure: Option<tokio::task::JoinError> = None;
    for attempt in 1..=MAX_ATTEMPTS {
        match tokio::spawn(attempt_leader_observes_a_dropped_follower()).await {
            Ok(()) => {
                if attempt > 1 {
                    eprintln!(
                        "leader_observes_a_dropped_follower_shrink_reachable_electorate_and_lose_quorum: \
                         succeeded on attempt {attempt}/{MAX_ATTEMPTS}"
                    );
                }
                return;
            }
            Err(join_error) => {
                eprintln!(
                    "leader_observes_a_dropped_follower_shrink_reachable_electorate_and_lose_quorum: \
                     attempt {attempt}/{MAX_ATTEMPTS} failed ({join_error}); retrying with fresh keys"
                );
                last_failure = Some(join_error);
            }
        }
    }
    if let Some(join_error) = last_failure {
        std::panic::resume_unwind(
            join_error
                .try_into_panic()
                .unwrap_or_else(|_| Box::new("attempt was cancelled, not panicked")),
        );
    }
}

async fn attempt_leader_observes_a_dropped_follower() {
    // ---- Setup: 3 nodes, full mesh, one shared 3-member electorate ----
    let net_a = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
    let net_b = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
    let net_c = Net::new(build_swarm(identity::Keypair::generate_ed25519()));
    let nets: Vec<&Net> = vec![&net_a, &net_b, &net_c];
    let ids = connect_full_mesh(&nets).await;
    let electorate: BTreeSet<WorkerId> = ids.iter().cloned().collect();

    // a and b race for leadership under the same proven C3 margin; c is a
    // long-timeout bystander (see module doc).
    let mut node_a = make_node(ids[0].clone(), &electorate, &net_a, FAST_SUSPECT_TIMEOUT_MS);
    let mut node_b = make_node(ids[1].clone(), &electorate, &net_b, FAST_SUSPECT_TIMEOUT_MS);
    let mut node_c = make_node(
        ids[2].clone(),
        &electorate,
        &net_c,
        BYSTANDER_SUSPECT_TIMEOUT_MS,
    );

    let (tx_a, rx_a) = watch::channel(node_a.state());
    let (tx_b, rx_b) = watch::channel(node_b.state());
    let (tx_c, rx_c) = watch::channel(node_c.state());
    let rxs = [rx_a.clone(), rx_b.clone(), rx_c.clone()];

    let tick_interval = StdDuration::from_millis(TICK_INTERVAL_MS);
    let mut scheduler_a = Scheduler::new(RealClock::new(), Uuid7Ids);
    let mut scheduler_b = Scheduler::new(RealClock::new(), Uuid7Ids);
    let mut scheduler_c = Scheduler::new(RealClock::new(), Uuid7Ids);

    // ---- Phase 1: converge to exactly one Leader, two Active ----
    let states = timeout(TEST_TIMEOUT, async {
        tokio::select! {
            _ = run_driver(&mut node_a, &net_a, tick_interval, {
                let tx_a = tx_a.clone();
                move |s| { let _ = tx_a.send(s); }
            }, |_| {}, &mut scheduler_a) => unreachable!(),
            _ = run_driver(&mut node_b, &net_b, tick_interval, {
                let tx_b = tx_b.clone();
                move |s| { let _ = tx_b.send(s); }
            }, |_| {}, &mut scheduler_b) => unreachable!(),
            _ = run_driver(&mut node_c, &net_c, tick_interval, {
                let tx_c = tx_c.clone();
                move |s| { let _ = tx_c.send(s); }
            }, |_| {}, &mut scheduler_c) => unreachable!(),
            states = wait_for_convergence(&rxs) => states,
        }
    })
    .await
    .expect("all 3 nodes converged to exactly one Leader and two Active followers");

    let leader_index = states
        .iter()
        .position(|s| *s == WorkerState::Leader)
        .expect("wait_for_convergence guarantees exactly one Leader");
    let follower_indices: Vec<usize> = (0..3).filter(|i| *i != leader_index).collect();
    let leader_id = ids[leader_index].clone();
    let leader_net = nets[leader_index];
    let follower1_id = ids[follower_indices[0]].clone();
    let follower2_id = ids[follower_indices[1]].clone();
    eprintln!(
        "phase 1: {leader_id:?} elected leader; followers = {follower1_id:?}, {follower2_id:?}"
    );

    // Sanity: the leader's reachable_electorate starts out containing both
    // followers (nothing dropped yet).
    timeout(TEST_TIMEOUT, async {
        loop {
            let reachable = leader_net.reachable_peers(leader_id.clone());
            if reachable.contains(&follower1_id) && reachable.contains(&follower2_id) {
                return;
            }
            tokio::time::sleep(StdDuration::from_millis(5)).await;
        }
    })
    .await
    .expect("the leader starts out reachable to both followers");

    // ---- Phases 2 and 3, driven inside one continuous select! so the three
    // ---- driver tasks (and thus tick_as_leader) never stop running between
    // ---- steps — dropping a follower is only observable once the leader's
    // ---- own next tick actually runs against the new reachable_peers view.
    //
    // Phase 2: drop ONE follower's connection; the leader must shrink its
    // reachable_electorate, but stay Leader (quorum(3) = 2, visible = 1 self
    // + 1 remaining follower = 2, still >= quorum).
    //
    // Phase 3: drop the SECOND follower's connection too; now nothing is
    // reachable, visible = 1 < quorum(2), so the leader must demote itself
    // to NoQuorum.
    leader_net.disconnect(follower1_id.clone());
    let mut rx_leader_state = rxs[leader_index].clone();

    timeout(TEST_TIMEOUT, async {
        tokio::select! {
            _ = run_driver(&mut node_a, &net_a, tick_interval, {
                let tx_a = tx_a.clone();
                move |s| { let _ = tx_a.send(s); }
            }, |_| {}, &mut scheduler_a) => unreachable!(),
            _ = run_driver(&mut node_b, &net_b, tick_interval, {
                let tx_b = tx_b.clone();
                move |s| { let _ = tx_b.send(s); }
            }, |_| {}, &mut scheduler_b) => unreachable!(),
            _ = run_driver(&mut node_c, &net_c, tick_interval, {
                let tx_c = tx_c.clone();
                move |s| { let _ = tx_c.send(s); }
            }, |_| {}, &mut scheduler_c) => unreachable!(),
            _ = async {
                // Wait for the shrink: follower1 gone, follower2 still there.
                loop {
                    let reachable = leader_net.reachable_peers(leader_id.clone());
                    if !reachable.contains(&follower1_id) && reachable.contains(&follower2_id) {
                        break;
                    }
                    tokio::time::sleep(StdDuration::from_millis(5)).await;
                }

                // Give tick_as_leader at least one more full tick against the
                // shrunk view, then assert it did NOT demote (quorum still
                // met by self + the one remaining follower).
                tokio::time::sleep(StdDuration::from_millis(TICK_INTERVAL_MS * 3)).await;
                assert_eq!(
                    *rx_leader_state.borrow_and_update(),
                    WorkerState::Leader,
                    "the leader must remain Leader after losing exactly one of two followers \
                     — quorum(3) = 2 is still met by itself plus the one remaining reachable \
                     follower"
                );

                // Now drop the second (last) follower too.
                leader_net.disconnect(follower2_id.clone());
                rx_leader_state
                    .wait_for(|s| *s == WorkerState::NoQuorum)
                    .await
                    .expect("the leader's driver task is still running");
            } => {},
        }
    })
    .await
    .expect(
        "the leader shrank its reachable_electorate on the first drop, stayed Leader, then \
         transitioned to NoQuorum on the second drop, all within the timeout",
    );

    assert!(
        leader_net.reachable_peers(leader_id.clone()).is_empty(),
        "the leader's reachable_peers must be empty once both followers are dropped"
    );
}
